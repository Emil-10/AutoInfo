const { Pool } = require("pg");

const DB_ENABLED = String(process.env.OPEN_DATA_DB_ENABLED || "true").toLowerCase() !== "false";
const DATABASE_URL = process.env.DATABASE_URL || "";
const ARES_CACHE_TTL_MS = Math.max(60000, Number(process.env.ARES_CACHE_TTL_MS || 604800000) || 604800000);
const ACTIVE_DATASET_VERSION_CACHE_TTL_MS = Math.max(
  1000,
  Number(process.env.ACTIVE_DATASET_VERSION_CACHE_TTL_MS || 30000) || 30000
);
const DATABASE_FAILURE_COOLDOWN_MS = Math.max(
  1000,
  Number(process.env.DATABASE_FAILURE_COOLDOWN_MS || 30000) || 30000
);

let pool = null;
const activeDatasetVersionCache = new Map();
let databaseUnavailableUntil = 0;

function isDatabaseConfigured() {
  return DB_ENABLED && Boolean(DATABASE_URL);
}

function getDatabaseRuntimeStatus() {
  return {
    configured: Boolean(DATABASE_URL),
    enabled: DB_ENABLED,
    ssl: resolveSslMode() ? "enabled" : "disabled",
    unavailableUntil: databaseUnavailableUntil || null
  };
}

function getPool() {
  if (!isDatabaseConfigured()) {
    return null;
  }

  if (!pool) {
    pool = new Pool(buildPoolConfig());
  }

  return pool;
}

function buildPoolConfig() {
  const max = Math.max(1, Number(process.env.DATABASE_POOL_MAX || 5) || 5);
  const idleTimeoutMillis = Math.max(1000, Number(process.env.DATABASE_IDLE_TIMEOUT_MS || 30000) || 30000);
  const connectionTimeoutMillis = Math.max(
    500,
    Number(process.env.DATABASE_CONNECTION_TIMEOUT_MS || 1500) || 1500
  );
  const ssl = resolveSslMode();

  return {
    connectionString: DATABASE_URL,
    max,
    idleTimeoutMillis,
    connectionTimeoutMillis,
    options: process.env.DATABASE_OPTIONS || "-c jit=off",
    ssl
  };
}

function resolveSslMode() {
  const explicit = String(process.env.DATABASE_SSL || process.env.PGSSLMODE || "").toLowerCase();
  if (["false", "disable", "disabled", "0", "off"].includes(explicit)) {
    return false;
  }

  if (["true", "require", "required", "1", "on"].includes(explicit)) {
    return { rejectUnauthorized: false };
  }

  if (/sslmode=require/i.test(DATABASE_URL)) {
    return { rejectUnauthorized: false };
  }

  return false;
}

async function closeDatabasePool() {
  if (!pool) {
    return;
  }

  const currentPool = pool;
  pool = null;
  await currentPool.end();
}

async function withOptionalClient(callback) {
  if (Date.now() < databaseUnavailableUntil) {
    return null;
  }

  const currentPool = getPool();
  if (!currentPool) {
    return null;
  }

  let client;
  try {
    client = await currentPool.connect();
    databaseUnavailableUntil = 0;
  } catch (error) {
    databaseUnavailableUntil = Date.now() + DATABASE_FAILURE_COOLDOWN_MS;
    if (String(process.env.OPEN_DATA_DB_STRICT || "false").toLowerCase() === "true") {
      throw error;
    }
    return null;
  }

  try {
    return await callback(client);
  } catch (error) {
    if (isRecoverableSchemaError(error)) {
      try {
        await applyOpenDataSchema(client);
        return await callback(client);
      } catch (retryError) {
        if (String(process.env.OPEN_DATA_DB_STRICT || "false").toLowerCase() === "true") {
          throw retryError;
        }
        return null;
      }
    }

    if (String(process.env.OPEN_DATA_DB_STRICT || "false").toLowerCase() === "true") {
      throw error;
    }
    return null;
  } finally {
    client.release();
  }
}

async function ensureOpenDataSchema() {
  const currentPool = getPool();
  if (!currentPool) {
    throw new Error("DATABASE_URL neni nastaveny.");
  }

  await applyOpenDataSchema(currentPool);
}

async function applyOpenDataSchema(client) {
  await client.query(SCHEMA_PRE_MIGRATION_SQL);
  await client.query(SCHEMA_SQL);
}

function isRecoverableSchemaError(error) {
  return ["42P01", "42703"].includes(String(error?.code || ""));
}

async function getActiveDatasetVersion(source, client = null) {
  const run = async (db) => {
    const result = await db.query(
      `
        select source, filename, dataset_date, record_count, last_checked_at, import_finished_at
        from dataset_versions
        where source = $1 and active is true and status = 'ready'
        order by import_finished_at desc nulls last, id desc
        limit 1
      `,
      [source]
    );
    return result.rows[0] ? mapDatasetVersionRow(result.rows[0]) : null;
  };

  if (client) {
    return await run(client);
  }

  const cached = activeDatasetVersionCache.get(source);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.value ? { ...cached.value } : null;
  }

  const value = await withOptionalClient(run);
  activeDatasetVersionCache.set(source, {
    value: value ? { ...value } : null,
    expiresAt: Date.now() + ACTIVE_DATASET_VERSION_CACHE_TTL_MS
  });
  return value;
}

async function getActiveDatasetVersions(sources) {
  return await withOptionalClient(async (client) => {
    const result = await client.query(
      `
        select distinct on (source)
          source, filename, dataset_date, record_count, last_checked_at, import_finished_at
        from dataset_versions
        where source = any($1::text[]) and active is true and status = 'ready'
        order by source, import_finished_at desc nulls last, id desc
      `,
      [sources]
    );
    return result.rows.reduce((map, row) => {
      map[row.source] = mapDatasetVersionRow(row);
      return map;
    }, Object.create(null));
  });
}

async function touchDatasetVersionChecks(sources) {
  return await withOptionalClient(async (client) => {
    await client.query(
      `
        update dataset_versions
        set last_checked_at = now(), updated_at = now()
        where source = any($1::text[]) and active is true
      `,
      [sources]
    );
    invalidateActiveDatasetVersionCache(sources);
    return true;
  });
}

function invalidateActiveDatasetVersionCache(sources = null) {
  if (!sources) {
    activeDatasetVersionCache.clear();
    return;
  }

  (Array.isArray(sources) ? sources : [sources]).forEach((source) => {
    activeDatasetVersionCache.delete(source);
  });
}

async function getOpenDataStatus(sources = ["ownership", "vehicles", "inspections"]) {
  const database = getDatabaseRuntimeStatus();
  if (!isDatabaseConfigured()) {
    return {
      database,
      activeVersions: null
    };
  }

  const activeVersions = await getActiveDatasetVersions(sources);
  return {
    database,
    activeVersions: activeVersions || Object.create(null)
  };
}

function vehiclePlateLateralSql(pcvExpression, vinExpression, alias = "pr") {
  const normalizedVinExpression = sqlNormalizedVin(vinExpression);
  return `
        left join lateral (
          select plate
          from (
            select vpl.plate, vpl.confidence, vpl.last_seen_at as resolved_at, vpl.expires_at
            from vehicle_plate_links vpl
            where (
              (${pcvExpression} is not null and vpl.pcv = ${pcvExpression})
              or (${vinExpression} is not null and ${sqlNormalizedVin("vpl.vin")} = ${normalizedVinExpression})
              or (${vinExpression} is not null and vpl.vehicle_key_type = 'vin' and vpl.vehicle_key = ${normalizedVinExpression})
            )
            and (vpl.expires_at is null or vpl.expires_at > now())
            union all
            select pr.plate, pr.confidence, pr.resolved_at, pr.expires_at
            from plate_resolutions pr
            where (
              (${pcvExpression} is not null and pr.pcv = ${pcvExpression})
              or (${vinExpression} is not null and ${sqlNormalizedVin("pr.vin")} = ${normalizedVinExpression})
            )
            and (pr.expires_at is null or pr.expires_at > now())
          ) plate_matches
          where plate is not null
          order by confidence desc nulls last, resolved_at desc nulls last
          limit 1
        ) ${alias} on true`;
}

function sqlNormalizedVin(expression) {
  return `upper(regexp_replace(coalesce(${expression}, ''), '[^A-Za-z0-9]', '', 'g'))`;
}

function vehicleInspectionSummarySelect(alias = "vis") {
  return `,
          ${alias}.type as inspection_type,
          ${alias}.state as inspection_state,
          ${alias}.station_code as inspection_station_code,
          ${alias}.station_name as inspection_station_name,
          ${alias}.performed_on as inspection_performed_on,
          ${alias}.valid_from as inspection_valid_from,
          ${alias}.valid_until as inspection_valid_until,
          ${alias}.protocol_number as inspection_protocol_number,
          ${alias}.odometer as inspection_odometer,
          ${alias}.odometer_unit as inspection_odometer_unit,
          ${alias}.current as inspection_current,
          ${alias}.dataset_filename as inspection_dataset_filename,
          ${alias}.dataset_date as inspection_dataset_date`;
}

function vehicleInspectionSummaryJoin(pcvExpression, alias = "vis") {
  return `left join vehicle_inspection_summaries ${alias} on ${alias}.pcv = ${pcvExpression}`;
}

function vehicleFleetFactsJoin(pcvExpression, alias = "vf") {
  return `left join vehicle_fleet_facts ${alias} on ${alias}.pcv = ${pcvExpression}`;
}

function vehicleFleetFactsSelect(alias = "vf", vinExpression = "null::text", plateExpression = "null::text") {
  return `
          coalesce(${alias}.vin, ${vinExpression}) as vin,
          ${alias}.make,
          ${alias}.model,
          ${alias}.type,
          ${alias}.variant,
          ${alias}.category,
          ${alias}.fuel,
          ${alias}.first_registration,
          ${alias}.first_registration_cz,
          ${alias}.power,
          ${alias}.color,
          ${alias}.length_mm,
          ${alias}.width_mm,
          ${alias}.height_mm,
          ${alias}.wheelbase_mm,
          ${alias}.weight_kg,
          ${alias}.vehicle_status,
          coalesce(nullif(btrim(${plateExpression}), ''), nullif(btrim(${alias}.plate), '')) as plate,
          ${alias}.inspection_type,
          ${alias}.inspection_state,
          ${alias}.inspection_station_code,
          ${alias}.inspection_station_name,
          ${alias}.inspection_performed_on,
          ${alias}.inspection_valid_from,
          ${alias}.inspection_valid_until,
          ${alias}.inspection_protocol_number,
          ${alias}.inspection_odometer,
          ${alias}.inspection_odometer_unit,
          ${alias}.inspection_current,
          ${alias}.inspection_dataset_filename,
	          ${alias}.inspection_dataset_date`;
}

function vehiclePlateSummarySourceSql() {
  return `
with candidates as (
  select
    v.pcv,
    v.vin,
    v.plate,
    'vehicles.plate' as source,
    1.00::numeric(3, 2) as confidence,
    now() as resolved_at,
    null::timestamptz as expires_at,
    now() as updated_at
  from vehicles v
  where v.pcv is not null
    and v.plate is not null
  union all
  select
    v.pcv,
    coalesce(vpl.vin, v.vin) as vin,
    vpl.plate,
    vpl.source,
    vpl.confidence,
    vpl.last_seen_at as resolved_at,
    vpl.expires_at,
    vpl.updated_at
  from vehicle_plate_links vpl
  join vehicles v on vpl.pcv is not null and v.pcv = vpl.pcv
  where vpl.plate is not null
  union all
  select
    v.pcv,
    coalesce(vpl.vin, v.vin) as vin,
    vpl.plate,
    vpl.source,
    vpl.confidence,
    vpl.last_seen_at as resolved_at,
    vpl.expires_at,
    vpl.updated_at
  from vehicle_plate_links vpl
  join vehicles v
    on vpl.vin is not null
    and v.vin is not null
    and ${sqlNormalizedVin("vpl.vin")} = ${sqlNormalizedVin("v.vin")}
  where vpl.plate is not null
  union all
  select
    v.pcv,
    coalesce(vpl.vin, v.vin) as vin,
    vpl.plate,
    vpl.source,
    vpl.confidence,
    vpl.last_seen_at as resolved_at,
    vpl.expires_at,
    vpl.updated_at
  from vehicle_plate_links vpl
  join vehicles v
    on vpl.vehicle_key_type = 'vin'
    and v.vin is not null
    and vpl.vehicle_key = ${sqlNormalizedVin("v.vin")}
  where vpl.plate is not null
  union all
  select
    v.pcv,
    coalesce(pr.vin, v.vin) as vin,
    pr.plate,
    pr.source,
    pr.confidence,
    pr.resolved_at,
    pr.expires_at,
    pr.updated_at
  from plate_resolutions pr
  join vehicles v on pr.pcv is not null and v.pcv = pr.pcv
  where pr.plate is not null
  union all
  select
    v.pcv,
    coalesce(pr.vin, v.vin) as vin,
    pr.plate,
    pr.source,
    pr.confidence,
    pr.resolved_at,
    pr.expires_at,
    pr.updated_at
  from plate_resolutions pr
  join vehicles v
    on pr.vin is not null
    and v.vin is not null
    and ${sqlNormalizedVin("pr.vin")} = ${sqlNormalizedVin("v.vin")}
  where pr.plate is not null
),
ranked as (
  select distinct on (pcv)
    pcv,
    vin,
    plate,
    source,
    confidence,
    resolved_at,
    expires_at,
    coalesce(updated_at, now()) as updated_at
  from candidates
  where pcv is not null
    and plate is not null
    and (expires_at is null or expires_at > now())
  order by pcv, confidence desc nulls last, resolved_at desc nulls last
)
select pcv, vin, plate, source, confidence, resolved_at, expires_at, updated_at
from ranked`;
}

function vehicleFleetFactsSchemaSql(tableName = "vehicle_fleet_facts", options = {}) {
  const ifNotExists = options.ifNotExists === false ? "" : "if not exists ";
  return `
create table ${ifNotExists}${tableName} (
  pcv text primary key,
  vin text,
  make text,
  model text,
  type text,
  variant text,
  category text,
  fuel text,
  first_registration date,
  first_registration_cz date,
  power text,
  color text,
  length_mm text,
  width_mm text,
  height_mm text,
  wheelbase_mm text,
  weight_kg text,
  vehicle_status text,
  dataset_filename text,
  dataset_date date,
  plate text,
  inspection_type text,
  inspection_state text,
  inspection_station_code text,
  inspection_station_name text,
  inspection_performed_on date,
  inspection_valid_from date,
  inspection_valid_until date,
  inspection_protocol_number text,
  inspection_odometer integer,
  inspection_odometer_unit text,
  inspection_current boolean,
  inspection_dataset_filename text,
  inspection_dataset_date date,
  updated_at timestamptz not null default now()
);`;
}

function vehicleFleetFactsIndexSql(tableName = "vehicle_fleet_facts", prefix = "vehicle_fleet_facts") {
  return `
create index if not exists ${prefix}_vin_norm_idx
  on ${tableName} ((upper(regexp_replace(coalesce(vin, ''), '[^A-Za-z0-9]', '', 'g'))))
  where vin is not null;
create index if not exists ${prefix}_plate_idx
  on ${tableName} (plate)
  where plate is not null;
create index if not exists ${prefix}_inspection_valid_until_idx
  on ${tableName} (inspection_valid_until desc)
  where inspection_valid_until is not null;`;
}

function vehicleFleetFactsSourceSql() {
  return `
select
  v.pcv,
  v.vin,
  v.make,
  v.model,
  v.type,
  v.variant,
  v.category,
  v.fuel,
  v.first_registration,
  v.first_registration_cz,
  v.power,
  v.color,
  v.length_mm,
  v.width_mm,
  v.height_mm,
  v.wheelbase_mm,
  v.weight_kg,
  v.status as vehicle_status,
  v.dataset_filename,
  v.dataset_date,
	  coalesce(nullif(btrim(v.plate), ''), nullif(btrim(vps.plate), '')) as plate,
  vis.type as inspection_type,
  vis.state as inspection_state,
  vis.station_code as inspection_station_code,
  vis.station_name as inspection_station_name,
  vis.performed_on as inspection_performed_on,
  vis.valid_from as inspection_valid_from,
  vis.valid_until as inspection_valid_until,
  vis.protocol_number as inspection_protocol_number,
  vis.odometer as inspection_odometer,
  vis.odometer_unit as inspection_odometer_unit,
  vis.current as inspection_current,
  vis.dataset_filename as inspection_dataset_filename,
  vis.dataset_date as inspection_dataset_date,
  now() as updated_at
		from vehicles v
	left join vehicle_plate_summaries vps
	  on vps.pcv = v.pcv
	  and (vps.expires_at is null or vps.expires_at > now())
	left join vehicle_inspection_summaries vis on vis.pcv = v.pcv`;
}

function vehicleFleetFactsTableSql() {
  return `
${dropVehicleFleetFactsViewSql()}
${vehicleFleetFactsSchemaSql("vehicle_fleet_facts")}
${vehicleFleetFactsIndexSql()}`;
}

function dropVehicleFleetFactsViewSql() {
  return `
do $$
begin
  if to_regclass('vehicle_fleet_facts') is not null
    and exists (
      select 1
      from pg_class
      where oid = 'vehicle_fleet_facts'::regclass
        and relkind = 'v'
    )
  then
    execute 'drop view vehicle_fleet_facts';
  end if;
end $$;`;
}

function companyVehicleFactsSchemaSql(tableName = "company_vehicle_facts", options = {}) {
  const ifNotExists = options.ifNotExists === false ? "" : "if not exists ";
  return `
create table ${ifNotExists}${tableName} (
  relation_source text not null,
  relation_id bigint not null,
  pcv text not null,
  relation_vin text,
  relation_plate text,
  ico text,
  name_key text,
  name text,
  address text,
  relation text,
  subject_type text,
  current boolean,
  date_from date,
  date_to date,
  dataset_filename text,
  dataset_date date,
  vin text,
  make text,
  model text,
  type text,
  variant text,
  category text,
  fuel text,
  first_registration date,
  first_registration_cz date,
  power text,
  color text,
  length_mm text,
  width_mm text,
  height_mm text,
  wheelbase_mm text,
  weight_kg text,
  vehicle_status text,
  plate text,
  inspection_type text,
  inspection_state text,
  inspection_station_code text,
  inspection_station_name text,
  inspection_performed_on date,
  inspection_valid_from date,
  inspection_valid_until date,
  inspection_protocol_number text,
  inspection_odometer integer,
  inspection_odometer_unit text,
  inspection_current boolean,
  inspection_dataset_filename text,
  inspection_dataset_date date,
  updated_at timestamptz not null default now(),
  unique (relation_source, relation_id)
);`;
}

function companyVehicleFactsIndexSql(tableName = "company_vehicle_facts", prefix = "company_vehicle_facts") {
  return `
create index if not exists ${prefix}_ico_current_idx
  on ${tableName} (ico, pcv)
  where current is true and date_to is null and relation in ('Vlastnik', 'Provozovatel');
create index if not exists ${prefix}_ico_history_idx
  on ${tableName} (ico, date_from desc, pcv)
  where relation in ('Vlastnik', 'Provozovatel');
create index if not exists ${prefix}_name_current_idx
  on ${tableName} (name_key, pcv)
  where ico is null and name_key is not null and current is true and date_to is null and relation in ('Vlastnik', 'Provozovatel');
create index if not exists ${prefix}_name_history_idx
  on ${tableName} (name_key, date_from desc, pcv)
  where ico is null and name_key is not null and relation in ('Vlastnik', 'Provozovatel');
create index if not exists ${prefix}_pcv_history_idx
  on ${tableName} (pcv, date_from desc)
  where relation in ('Vlastnik', 'Provozovatel');`;
}

function companyVehicleFactsSourceSql() {
  return `
select
  source.relation_source,
  source.relation_id,
  source.pcv,
  source.relation_vin,
  source.relation_plate,
  source.ico,
  source.name_key,
  source.name,
  source.address,
  source.relation,
  source.subject_type,
  source.current,
  source.date_from,
  source.date_to,
  source.dataset_filename,
  source.dataset_date,
  coalesce(vf.vin, source.relation_vin) as vin,
  vf.make,
  vf.model,
  vf.type,
  vf.variant,
  vf.category,
  vf.fuel,
  vf.first_registration,
  vf.first_registration_cz,
  vf.power,
  vf.color,
  vf.length_mm,
  vf.width_mm,
  vf.height_mm,
  vf.wheelbase_mm,
  vf.weight_kg,
  vf.vehicle_status,
  coalesce(nullif(btrim(source.relation_plate), ''), nullif(btrim(vf.plate), '')) as plate,
  vf.inspection_type,
  vf.inspection_state,
  vf.inspection_station_code,
  vf.inspection_station_name,
  vf.inspection_performed_on,
  vf.inspection_valid_from,
  vf.inspection_valid_until,
  vf.inspection_protocol_number,
  vf.inspection_odometer,
  vf.inspection_odometer_unit,
  vf.inspection_current,
  vf.inspection_dataset_filename,
  vf.inspection_dataset_date,
  now() as updated_at
from (
  select
    'ownership'::text as relation_source,
    id as relation_id,
    pcv,
    null::text as relation_vin,
    null::text as relation_plate,
    ico,
    lower(name) as name_key,
    name,
    address,
    relation,
    subject_type,
    current,
    date_from,
    date_to,
    dataset_filename,
    dataset_date
  from ownership_relations
  where pcv is not null
  union all
  select
    'supplemental'::text as relation_source,
    id as relation_id,
    pcv,
    vin as relation_vin,
    plate as relation_plate,
    ico,
    lower(name) as name_key,
    name,
    address,
    relation,
    'supplemental'::text as subject_type,
    current,
    date_from,
    date_to,
    source as dataset_filename,
    null::date as dataset_date
  from supplemental_ownership_relations
  where pcv is not null
) source
left join vehicle_fleet_facts vf on vf.pcv = source.pcv`;
}

function refreshVehicleFleetFactsSql() {
  return `
${dropVehicleFleetFactsViewSql()}
drop table if exists vehicle_fleet_facts_next;
${vehicleFleetFactsSchemaSql("vehicle_fleet_facts_next", { ifNotExists: false })}
insert into vehicle_fleet_facts_next (
  pcv, vin, make, model, type, variant, category, fuel, first_registration,
  first_registration_cz, power, color, length_mm, width_mm, height_mm, wheelbase_mm,
  weight_kg, vehicle_status, dataset_filename, dataset_date,
  plate, inspection_type, inspection_state, inspection_station_code, inspection_station_name,
  inspection_performed_on, inspection_valid_from, inspection_valid_until, inspection_protocol_number,
  inspection_odometer, inspection_odometer_unit, inspection_current, inspection_dataset_filename,
  inspection_dataset_date, updated_at
)
${vehicleFleetFactsSourceSql()};
${vehicleFleetFactsIndexSql("vehicle_fleet_facts_next", "vehicle_fleet_facts_next")}
analyze vehicle_fleet_facts_next;
drop table if exists vehicle_fleet_facts_old;
alter table if exists vehicle_fleet_facts rename to vehicle_fleet_facts_old;
alter table vehicle_fleet_facts_next rename to vehicle_fleet_facts;
drop table if exists vehicle_fleet_facts_old;
alter index if exists vehicle_fleet_facts_next_pkey rename to vehicle_fleet_facts_pkey;
alter index if exists vehicle_fleet_facts_next_vin_norm_idx rename to vehicle_fleet_facts_vin_norm_idx;
alter index if exists vehicle_fleet_facts_next_plate_idx rename to vehicle_fleet_facts_plate_idx;
alter index if exists vehicle_fleet_facts_next_inspection_valid_until_idx rename to vehicle_fleet_facts_inspection_valid_until_idx;`;
}

function refreshCompanyVehicleFactsSql() {
  return `
drop table if exists company_vehicle_facts_next;
${companyVehicleFactsSchemaSql("company_vehicle_facts_next", { ifNotExists: false })}
insert into company_vehicle_facts_next (
  relation_source, relation_id, pcv, relation_vin, relation_plate, ico, name_key, name, address,
  relation, subject_type, current, date_from, date_to, dataset_filename, dataset_date, vin, make,
  model, type, variant, category, fuel, first_registration, first_registration_cz, power, color,
  length_mm, width_mm, height_mm, wheelbase_mm, weight_kg, vehicle_status, plate, inspection_type, inspection_state, inspection_station_code,
  inspection_station_name, inspection_performed_on, inspection_valid_from, inspection_valid_until,
  inspection_protocol_number, inspection_odometer, inspection_odometer_unit, inspection_current,
  inspection_dataset_filename, inspection_dataset_date, updated_at
)
${companyVehicleFactsSourceSql()};
${companyVehicleFactsIndexSql("company_vehicle_facts_next", "company_vehicle_facts_next")}
analyze company_vehicle_facts_next;
drop table if exists company_vehicle_facts_old;
alter table if exists company_vehicle_facts rename to company_vehicle_facts_old;
alter table company_vehicle_facts_next rename to company_vehicle_facts;
drop table if exists company_vehicle_facts_old;
alter index if exists company_vehicle_facts_next_relation_source_relation_id_key
  rename to company_vehicle_facts_relation_source_relation_id_key;
alter index if exists company_vehicle_facts_next_ico_current_idx rename to company_vehicle_facts_ico_current_idx;
alter index if exists company_vehicle_facts_next_ico_history_idx rename to company_vehicle_facts_ico_history_idx;
alter index if exists company_vehicle_facts_next_name_current_idx rename to company_vehicle_facts_name_current_idx;
alter index if exists company_vehicle_facts_next_name_history_idx rename to company_vehicle_facts_name_history_idx;
alter index if exists company_vehicle_facts_next_pcv_history_idx rename to company_vehicle_facts_pcv_history_idx;`;
}

async function queryPcvByVin(vin) {
  const normalizedVin = normalizeVin(vin);
  if (!normalizedVin) {
    return null;
  }

  return await withOptionalClient(async (client) => {
    const result = await client.query(
      `
        select pcv
        from (
          select pcv, 1 as priority from vehicle_vins where vin = $1
          union all
          select pcv, 2 as priority from vehicles where ${sqlNormalizedVin("vin")} = $1
        ) matches
        where pcv is not null and btrim(pcv) <> ''
        order by priority
        limit 1
      `,
      [normalizedVin]
    );
    return result.rows[0]?.pcv || null;
  });
}

async function queryVehicleByPcv(pcv) {
  const normalizedPcv = normalizeText(pcv);
  if (!normalizedPcv) {
    return null;
  }

  return await withOptionalClient(async (client) => {
    const active = await getActiveDatasetVersion("vehicles", client);
    if (!active) {
      return null;
    }

    const result = await client.query(
      `
        select vf.pcv, ${vehicleFleetFactsSelect("vf")}, vf.dataset_filename, vf.dataset_date
        from vehicle_fleet_facts vf
        where vf.pcv = $1
        limit 1
      `,
      [normalizedPcv]
    );
    const summary = mapVehicleSummaryRow(result.rows[0]);
    return summary
      ? {
          sourceFile: active.filename || null,
          sourceUpdatedAt: active.datasetDate || null,
          summary
        }
      : null;
  });
}

async function queryVehicleByVin(vin) {
  const normalizedVin = normalizeVin(vin);
  if (!normalizedVin) {
    return null;
  }

  return await withOptionalClient(async (client) => {
    const active = await getActiveDatasetVersion("vehicles", client);
    if (!active) {
      return null;
    }

    const result = await client.query(
      `
        select vf.pcv, ${vehicleFleetFactsSelect("vf")}, vf.dataset_filename, vf.dataset_date
        from vehicle_vins i
        join vehicle_fleet_facts vf on vf.pcv = i.pcv
        where i.vin = $1
        limit 1
      `,
      [normalizedVin]
    );
    const summary = mapVehicleSummaryRow(result.rows[0]);
    return summary
      ? {
          sourceFile: active.filename || null,
          sourceUpdatedAt: active.datasetDate || null,
          summary
        }
      : null;
  });
}

async function queryOwnershipByPcv(pcv) {
  const normalizedPcv = normalizeText(pcv);
  if (!normalizedPcv) {
    return null;
  }

  return await withOptionalClient(async (client) => {
    const active = await getActiveDatasetVersion("ownership", client);
    if (!active) {
      return null;
    }

	    const result = await client.query(
	      `
        with official_relations as (
          select o.pcv, coalesce(nullif(o.ico, ''), s.ico) as ico, o.name,
            coalesce(nullif(o.address, ''), s.address) as address, o.relation, o.subject_type,
            o.current, o.date_from, o.date_to, o.dataset_filename, o.dataset_date, o.id::text as row_id
          from ownership_relations o
          left join lateral (
            select supplemental.ico, supplemental.address
            from supplemental_ownership_relations supplemental
            where supplemental.pcv = o.pcv
              and coalesce(supplemental.relation, '') = coalesce(o.relation, '')
              and lower(btrim(coalesce(supplemental.name, ''))) = lower(btrim(coalesce(o.name, '')))
              and coalesce(supplemental.current, false) = coalesce(o.current, false)
              and coalesce(supplemental.date_from, date '0001-01-01') = coalesce(o.date_from, date '0001-01-01')
              and coalesce(supplemental.date_to, date '9999-12-31') = coalesce(o.date_to, date '9999-12-31')
            order by supplemental.observed_at desc nulls last, supplemental.id desc
            limit 1
          ) s on true
          where o.pcv = $1
        ),
        all_relations as (
          select pcv, ico, name, address, relation, subject_type, current, date_from, date_to,
            dataset_filename, dataset_date, row_id
          from official_relations
          union all
          select supplemental.pcv, supplemental.ico, supplemental.name, supplemental.address,
            supplemental.relation, 'supplemental' as subject_type, supplemental.current,
            supplemental.date_from, supplemental.date_to, supplemental.source as dataset_filename,
            null::date as dataset_date, supplemental.id::text as row_id
          from supplemental_ownership_relations supplemental
          where supplemental.pcv = $1
            and not exists (
              select 1
              from official_relations official
              where official.pcv = supplemental.pcv
                and coalesce(official.relation, '') = coalesce(supplemental.relation, '')
                and lower(btrim(coalesce(official.name, ''))) = lower(btrim(coalesce(supplemental.name, '')))
                and coalesce(official.current, false) = coalesce(supplemental.current, false)
                and coalesce(official.date_from, date '0001-01-01') = coalesce(supplemental.date_from, date '0001-01-01')
                and coalesce(official.date_to, date '9999-12-31') = coalesce(supplemental.date_to, date '9999-12-31')
            )
        )
	        select pcv, ico, name, address, relation, subject_type, current, date_from, date_to,
	          dataset_filename, dataset_date
	        from all_relations
	        order by coalesce(current, false) desc, date_from desc nulls last, date_to desc nulls last, row_id desc
	      `,
	      [normalizedPcv]
	    );

    return {
      sourceFile: active.filename || null,
      sourceUpdatedAt: active.datasetDate || null,
      relations: result.rows.map(mapRelationRow)
    };
  });
}

async function queryVehiclesByIco(ico, options = {}) {
  const normalizedIco = sanitizeIco(ico);
  if (!normalizedIco) {
    return null;
  }
  const maxPcvs = normalizePositiveInteger(options.limit, 200);
  const companyNameKeys = normalizeCompanyNameSearchTerms(options.companyNames);

  return await withOptionalClient(async (client) => {
    const [ownershipVersion, vehicleVersion] = await Promise.all([
      getActiveDatasetVersion("ownership", client),
      getActiveDatasetVersion("vehicles", client)
    ]);
	    if (!ownershipVersion) {
	      return null;
	    }

    const factPayload = await queryVehiclesByIcoFromFacts(
      client,
      normalizedIco,
      companyNameKeys,
      maxPcvs,
      ownershipVersion,
      vehicleVersion
    );
    if (factPayload) {
      return factPayload;
    }

		    const countResult = await client.query(
	      `
		        with active_relations as (
				          select pcv
				          from ownership_relations
				          where (
                    ico = $1
                    or (
                      ico is null
                      and lower(name) = any($2::text[])
                    )
                  )
				            and pcv is not null
				            and current is true
		            and date_to is null
		            and relation in ('Vlastnik', 'Provozovatel')
	          union all
	          select pcv
	          from supplemental_ownership_relations
	          where ico = $1
	            and pcv is not null
	            and current is true
		            and date_to is null
		            and relation in ('Vlastnik', 'Provozovatel')
		        ),
		        all_ico_pcvs as (
			          select distinct pcv
			          from ownership_relations
			          where (
                  ico = $1
                  or (
                    ico is null
                    and lower(name) = any($2::text[])
                  )
                )
			            and pcv is not null
			            and relation in ('Vlastnik', 'Provozovatel')
		          union
		          select distinct pcv
		          from supplemental_ownership_relations
		          where ico = $1
		            and pcv is not null
		            and relation in ('Vlastnik', 'Provozovatel')
		        ),
			        active_pcvs as (
			          select distinct pcv from active_relations
			        ),
			        company_history_pcvs as (
			          select distinct pcv
			          from ownership_relations
			          where (
	                  ico = $1
	                  or (
	                    ico is null
	                    and lower(name) = any($2::text[])
	                  )
	                )
			            and pcv is not null
			            and relation in ('Vlastnik', 'Provozovatel')
			            and not (current is true and date_to is null)
			          union
			          select distinct pcv
			          from supplemental_ownership_relations
			          where ico = $1
			            and pcv is not null
			            and relation in ('Vlastnik', 'Provozovatel')
			            and not (current is true and date_to is null)
			        )
			        select
			          (select count(*)::bigint from active_pcvs) as candidate_count,
			          (select count(*)::bigint from active_relations) as active_relation_count,
			          (select count(*)::bigint from all_ico_pcvs) as all_vehicle_count,
			          (
			            select count(*)::bigint
			            from all_ico_pcvs h
			            where not exists (select 1 from active_pcvs a where a.pcv = h.pcv)
			          ) as historical_vehicle_count,
			          (select count(*)::bigint from company_history_pcvs) as company_history_vehicle_count
			      `,
				      [normalizedIco, companyNameKeys]
				    );
		    const candidateCount = Number(countResult.rows[0]?.candidate_count || 0);
		    const activeRelationCount = Number(countResult.rows[0]?.active_relation_count || 0);
		    const allVehicleCount = Number(countResult.rows[0]?.all_vehicle_count || candidateCount);
		    const historicalVehicleCount = Number(countResult.rows[0]?.historical_vehicle_count || 0);
		    const companyHistoryVehicleCount = Number(countResult.rows[0]?.company_history_vehicle_count || 0);
		    if (candidateCount <= 0 && historicalVehicleCount <= 0 && companyHistoryVehicleCount <= 0) {
		      return {
		        sourceUpdatedAt: ownershipVersion.datasetDate || vehicleVersion?.datasetDate || null,
		        candidateCount: 0,
		        activeRelationCount: 0,
		        allVehicleCount,
		        historicalVehicleCount,
		        companyHistoryVehicleCount,
		        limit: maxPcvs,
		        truncated: false,
		        relations: [],
		        summaries: [],
		        historicalRelations: [],
		        historicalSummaries: [],
		        companyHistoryRelations: [],
		        companyHistorySummaries: []
	      };
	    }

    const result = await client.query(
      `
			        with active_relations as (
				          select id::text as row_id, pcv, null::text as relation_vin, null::text as relation_plate,
	                  case
	                    when ico is null and lower(name) = any($3::text[]) then $1
	                    else ico
                  end as ico,
                  name, address, relation, subject_type,
			            current, date_from, date_to, dataset_filename, dataset_date
			          from ownership_relations
			          where (
                  ico = $1
                  or (
                    ico is null
                    and lower(name) = any($3::text[])
                  )
                )
			            and pcv is not null
			            and current is true
		            and date_to is null
		            and relation in ('Vlastnik', 'Provozovatel')
		          union all
			          select id::text as row_id, pcv, vin as relation_vin, plate as relation_plate, ico, name, address, relation,
			            'supplemental' as subject_type, current, date_from, date_to,
			            source as dataset_filename, null::date as dataset_date
		          from supplemental_ownership_relations
		          where ico = $1
		            and pcv is not null
		            and current is true
		            and date_to is null
		            and relation in ('Vlastnik', 'Provozovatel')
		        ),
		        candidate_pcvs as (
		          select pcv, max(date_from) as latest_date
		          from active_relations
		          group by pcv
		          order by latest_date desc nulls last, pcv asc
		          limit $2
		        ),
		        history_counts as (
		          select pcv, count(*)::bigint as history_relation_count
		          from (
			            select pcv
			            from ownership_relations
			            where (
                    ico = $1
                    or (
                      ico is null
                      and lower(name) = any($3::text[])
                    )
                  )
                  and relation in ('Vlastnik', 'Provozovatel')
		            union all
		            select pcv
		            from supplemental_ownership_relations
		            where ico = $1 and relation in ('Vlastnik', 'Provozovatel')
		          ) h
		          group by pcv
		        )
		        select
		          h.pcv, h.ico, h.name, h.address, h.relation, h.subject_type, h.current,
		          h.date_from, h.date_to, h.dataset_filename, h.dataset_date,
		          coalesce(c.history_relation_count, 0) as history_relation_count,
				          ${vehicleFleetFactsSelect("vf", "h.relation_vin", "h.relation_plate")}
						        from active_relations h
					        join candidate_pcvs a on a.pcv = h.pcv
					        left join history_counts c on c.pcv = h.pcv
						        ${vehicleFleetFactsJoin("h.pcv", "vf")}
			        where h.relation in ('Vlastnik', 'Provozovatel')
			        order by a.latest_date desc nulls last, h.pcv asc, h.date_from desc nulls last, h.row_id desc
			      `,
	      [normalizedIco, maxPcvs, companyNameKeys]
	    );
		    const historicalResult = await client.query(
		      `
	        with active_pcvs as (
	          select distinct pcv
	          from ownership_relations
	          where (
	            ico = $1
	            or (
	              ico is null
	              and lower(name) = any($3::text[])
	            )
	          )
	          and pcv is not null
	          and current is true
	          and date_to is null
	          and relation in ('Vlastnik', 'Provozovatel')
	          union
	          select distinct pcv
	          from supplemental_ownership_relations
	          where ico = $1
	            and pcv is not null
	            and current is true
	            and date_to is null
	            and relation in ('Vlastnik', 'Provozovatel')
	        ),
	        historical_relations as (
			          select id::text as row_id, pcv, null::text as relation_vin, null::text as relation_plate,
			            case
			              when ico is null and lower(name) = any($3::text[]) then $1
	              else ico
	            end as ico,
	            name, address, relation, subject_type,
	            current, date_from, date_to, dataset_filename, dataset_date
	          from ownership_relations
	          where (
	            ico = $1
	            or (
	              ico is null
	              and lower(name) = any($3::text[])
	            )
	          )
	          and pcv is not null
	          and relation in ('Vlastnik', 'Provozovatel')
	          and not (current is true and date_to is null)
	          union all
			          select id::text as row_id, pcv, vin as relation_vin, plate as relation_plate, ico, name, address, relation,
			            'supplemental' as subject_type, current, date_from, date_to,
			            source as dataset_filename, null::date as dataset_date
	          from supplemental_ownership_relations
	          where ico = $1
	            and pcv is not null
	            and relation in ('Vlastnik', 'Provozovatel')
	            and not (current is true and date_to is null)
	        ),
	        candidate_pcvs as (
	          select h.pcv, max(h.date_from) as latest_date
	          from historical_relations h
	          where not exists (select 1 from active_pcvs a where a.pcv = h.pcv)
	          group by h.pcv
	          order by latest_date desc nulls last, h.pcv asc
	          limit $2
	        ),
	        history_counts as (
	          select pcv, count(*)::bigint as history_relation_count
	          from historical_relations
	          group by pcv
	        )
	        select
	          h.pcv, h.ico, h.name, h.address, h.relation, h.subject_type, h.current,
	          h.date_from, h.date_to, h.dataset_filename, h.dataset_date,
	          coalesce(c.history_relation_count, 0) as history_relation_count,
				          ${vehicleFleetFactsSelect("vf", "h.relation_vin", "h.relation_plate")}
			        from historical_relations h
			        join candidate_pcvs a on a.pcv = h.pcv
			        left join history_counts c on c.pcv = h.pcv
				        ${vehicleFleetFactsJoin("h.pcv", "vf")}
	        order by a.latest_date desc nulls last, h.pcv asc, h.date_from desc nulls last, h.row_id desc
	      `,
		      [normalizedIco, maxPcvs, companyNameKeys]
		    );
		    const companyHistoryResult = await client.query(
		      `
		        with company_history_relations as (
		          select id::text as row_id, pcv, null::text as relation_vin, null::text as relation_plate,
		            case
		              when ico is null and lower(name) = any($3::text[]) then $1
		              else ico
		            end as ico,
		            name, address, relation, subject_type,
		            current, date_from, date_to, dataset_filename, dataset_date
		          from ownership_relations
		          where (
		            ico = $1
		            or (
		              ico is null
		              and lower(name) = any($3::text[])
		            )
		          )
		          and pcv is not null
		          and relation in ('Vlastnik', 'Provozovatel')
		          and not (current is true and date_to is null)
		          union all
		          select id::text as row_id, pcv, vin as relation_vin, plate as relation_plate, ico, name, address, relation,
		            'supplemental' as subject_type, current, date_from, date_to,
		            source as dataset_filename, null::date as dataset_date
		          from supplemental_ownership_relations
		          where ico = $1
		            and pcv is not null
		            and relation in ('Vlastnik', 'Provozovatel')
		            and not (current is true and date_to is null)
		        ),
		        candidate_pcvs as (
		          select h.pcv, max(h.date_from) as latest_date
		          from company_history_relations h
		          group by h.pcv
		          order by latest_date desc nulls last, h.pcv asc
		          limit $2
		        ),
		        history_counts as (
		          select pcv, count(*)::bigint as history_relation_count
		          from company_history_relations
		          group by pcv
		        )
		        select
		          h.pcv, h.ico, h.name, h.address, h.relation, h.subject_type, h.current,
		          h.date_from, h.date_to, h.dataset_filename, h.dataset_date,
		          coalesce(c.history_relation_count, 0) as history_relation_count,
			          ${vehicleFleetFactsSelect("vf", "h.relation_vin", "h.relation_plate")}
				        from company_history_relations h
				        join candidate_pcvs a on a.pcv = h.pcv
				        left join history_counts c on c.pcv = h.pcv
				        ${vehicleFleetFactsJoin("h.pcv", "vf")}
		        order by a.latest_date desc nulls last, h.pcv asc, h.date_from desc nulls last, h.row_id desc
		      `,
		      [normalizedIco, maxPcvs, companyNameKeys]
		    );

		    return {
			      sourceUpdatedAt: ownershipVersion.datasetDate || vehicleVersion?.datasetDate || null,
		      candidateCount,
		      activeRelationCount,
		      allVehicleCount,
		      historicalVehicleCount,
		      companyHistoryVehicleCount,
			      limit: maxPcvs,
			      truncated: candidateCount > maxPcvs,
			      relations: result.rows.map(mapRelationRow),
			      summaries: result.rows.map(mapVehicleSummaryRow).filter(Boolean),
			      historicalRelations: historicalResult.rows.map(mapRelationRow),
			      historicalSummaries: historicalResult.rows.map(mapVehicleSummaryRow).filter(Boolean),
			      companyHistoryRelations: companyHistoryResult.rows.map(mapRelationRow),
			      companyHistorySummaries: companyHistoryResult.rows.map(mapVehicleSummaryRow).filter(Boolean)
			    };
		  });
	}

async function queryVehiclesByIcoFromFacts(
  client,
  normalizedIco,
  companyNameKeys,
  maxPcvs,
  ownershipVersion,
  vehicleVersion
) {
  const readyResult = await client.query("select exists (select 1 from company_vehicle_facts limit 1) as ready");
  if (!readyResult.rows[0]?.ready) {
    return null;
  }

  const countResult = await client.query(
    `
      with matching_relations as (
        select pcv, current, date_to, relation, date_from
        from company_vehicle_facts
        where (
          ico = $1
          or (
            ico is null
            and name_key = any($2::text[])
          )
        )
        and pcv is not null
        and relation in ('Vlastnik', 'Provozovatel')
      ),
      active_relations as (
        select pcv
        from matching_relations
        where current is true and date_to is null
      ),
      all_ico_pcvs as (
        select distinct pcv from matching_relations
      ),
      active_pcvs as (
        select distinct pcv from active_relations
      ),
      company_history_pcvs as (
        select distinct pcv
        from matching_relations
        where not (current is true and date_to is null)
      )
      select
        (select count(*)::bigint from active_pcvs) as candidate_count,
        (select count(*)::bigint from active_relations) as active_relation_count,
        (select count(*)::bigint from all_ico_pcvs) as all_vehicle_count,
        (
          select count(*)::bigint
          from all_ico_pcvs h
          where not exists (select 1 from active_pcvs a where a.pcv = h.pcv)
        ) as historical_vehicle_count,
        (select count(*)::bigint from company_history_pcvs) as company_history_vehicle_count
    `,
    [normalizedIco, companyNameKeys]
  );
  const candidateCount = Number(countResult.rows[0]?.candidate_count || 0);
  const activeRelationCount = Number(countResult.rows[0]?.active_relation_count || 0);
  const allVehicleCount = Number(countResult.rows[0]?.all_vehicle_count || candidateCount);
  const historicalVehicleCount = Number(countResult.rows[0]?.historical_vehicle_count || 0);
  const companyHistoryVehicleCount = Number(countResult.rows[0]?.company_history_vehicle_count || 0);

  if (candidateCount <= 0 && historicalVehicleCount <= 0 && companyHistoryVehicleCount <= 0) {
    return {
      sourceUpdatedAt: ownershipVersion.datasetDate || vehicleVersion?.datasetDate || null,
      candidateCount: 0,
      activeRelationCount: 0,
      allVehicleCount,
      historicalVehicleCount,
      companyHistoryVehicleCount,
      limit: maxPcvs,
      truncated: false,
      relations: [],
      summaries: [],
      historicalRelations: [],
      historicalSummaries: [],
      companyHistoryRelations: [],
      companyHistorySummaries: []
    };
  }

  const result = await client.query(
    `
      with active_relations as (
        select
          relation_source || ':' || relation_id::text as row_id,
          pcv,
          relation_vin,
          relation_plate,
          case
            when ico is null and name_key = any($3::text[]) then $1
            else ico
          end as ico,
          name,
          address,
          relation,
          subject_type,
          current,
          date_from,
          date_to,
          dataset_filename,
          dataset_date,
          ${vehicleFleetFactsSelect("company_vehicle_facts", "relation_vin", "relation_plate")}
        from company_vehicle_facts
        where (
          ico = $1
          or (
            ico is null
            and name_key = any($3::text[])
          )
        )
        and pcv is not null
        and current is true
        and date_to is null
        and relation in ('Vlastnik', 'Provozovatel')
      ),
      candidate_pcvs as (
        select pcv, max(date_from) as latest_date
        from active_relations
        group by pcv
        order by latest_date desc nulls last, pcv asc
        limit $2
      ),
      history_counts as (
        select pcv, count(*)::bigint as history_relation_count
        from company_vehicle_facts
        where (
          ico = $1
          or (
            ico is null
            and name_key = any($3::text[])
          )
        )
        and relation in ('Vlastnik', 'Provozovatel')
        group by pcv
      )
      select
        h.pcv, h.ico, h.name, h.address, h.relation, h.subject_type, h.current,
        h.date_from, h.date_to, h.dataset_filename, h.dataset_date,
        coalesce(c.history_relation_count, 0) as history_relation_count,
        ${vehicleFleetFactsSelect("h", "h.relation_vin", "h.relation_plate")}
      from active_relations h
      join candidate_pcvs a on a.pcv = h.pcv
      left join history_counts c on c.pcv = h.pcv
      order by a.latest_date desc nulls last, h.pcv asc, h.date_from desc nulls last, h.row_id desc
    `,
    [normalizedIco, maxPcvs, companyNameKeys]
  );

  const historicalResult = await client.query(
    `
      with active_pcvs as (
        select distinct pcv
        from company_vehicle_facts
        where (
          ico = $1
          or (
            ico is null
            and name_key = any($3::text[])
          )
        )
        and pcv is not null
        and current is true
        and date_to is null
        and relation in ('Vlastnik', 'Provozovatel')
      ),
      historical_relations as (
        select
          relation_source || ':' || relation_id::text as row_id,
          pcv,
          relation_vin,
          relation_plate,
          case
            when ico is null and name_key = any($3::text[]) then $1
            else ico
          end as ico,
          name,
          address,
          relation,
          subject_type,
          current,
          date_from,
          date_to,
          dataset_filename,
          dataset_date,
          ${vehicleFleetFactsSelect("company_vehicle_facts", "relation_vin", "relation_plate")}
        from company_vehicle_facts
        where (
          ico = $1
          or (
            ico is null
            and name_key = any($3::text[])
          )
        )
        and pcv is not null
        and relation in ('Vlastnik', 'Provozovatel')
        and not (current is true and date_to is null)
      ),
      candidate_pcvs as (
        select h.pcv, max(h.date_from) as latest_date
        from historical_relations h
        where not exists (select 1 from active_pcvs a where a.pcv = h.pcv)
        group by h.pcv
        order by latest_date desc nulls last, h.pcv asc
        limit $2
      ),
      history_counts as (
        select pcv, count(*)::bigint as history_relation_count
        from historical_relations
        group by pcv
      )
      select
        h.pcv, h.ico, h.name, h.address, h.relation, h.subject_type, h.current,
        h.date_from, h.date_to, h.dataset_filename, h.dataset_date,
        coalesce(c.history_relation_count, 0) as history_relation_count,
        ${vehicleFleetFactsSelect("h", "h.relation_vin", "h.relation_plate")}
      from historical_relations h
      join candidate_pcvs a on a.pcv = h.pcv
      left join history_counts c on c.pcv = h.pcv
      order by a.latest_date desc nulls last, h.pcv asc, h.date_from desc nulls last, h.row_id desc
    `,
    [normalizedIco, maxPcvs, companyNameKeys]
  );

  const companyHistoryResult = await client.query(
    `
      with company_history_relations as (
        select
          relation_source || ':' || relation_id::text as row_id,
          pcv,
          relation_vin,
          relation_plate,
          case
            when ico is null and name_key = any($3::text[]) then $1
            else ico
          end as ico,
          name,
          address,
          relation,
          subject_type,
          current,
          date_from,
          date_to,
          dataset_filename,
          dataset_date,
          ${vehicleFleetFactsSelect("company_vehicle_facts", "relation_vin", "relation_plate")}
        from company_vehicle_facts
        where (
          ico = $1
          or (
            ico is null
            and name_key = any($3::text[])
          )
        )
        and pcv is not null
        and relation in ('Vlastnik', 'Provozovatel')
        and not (current is true and date_to is null)
      ),
      candidate_pcvs as (
        select h.pcv, max(h.date_from) as latest_date
        from company_history_relations h
        group by h.pcv
        order by latest_date desc nulls last, h.pcv asc
        limit $2
      ),
      history_counts as (
        select pcv, count(*)::bigint as history_relation_count
        from company_history_relations
        group by pcv
      )
      select
        h.pcv, h.ico, h.name, h.address, h.relation, h.subject_type, h.current,
        h.date_from, h.date_to, h.dataset_filename, h.dataset_date,
        coalesce(c.history_relation_count, 0) as history_relation_count,
        ${vehicleFleetFactsSelect("h", "h.relation_vin", "h.relation_plate")}
      from company_history_relations h
      join candidate_pcvs a on a.pcv = h.pcv
      left join history_counts c on c.pcv = h.pcv
      order by a.latest_date desc nulls last, h.pcv asc, h.date_from desc nulls last, h.row_id desc
    `,
    [normalizedIco, maxPcvs, companyNameKeys]
  );

  return {
    sourceUpdatedAt: ownershipVersion.datasetDate || vehicleVersion?.datasetDate || null,
    candidateCount,
    activeRelationCount,
    allVehicleCount,
    historicalVehicleCount,
    companyHistoryVehicleCount,
    limit: maxPcvs,
    truncated: candidateCount > maxPcvs,
    relations: result.rows.map(mapRelationRow),
    summaries: result.rows.map(mapVehicleSummaryRow).filter(Boolean),
    historicalRelations: historicalResult.rows.map(mapRelationRow),
    historicalSummaries: historicalResult.rows.map(mapVehicleSummaryRow).filter(Boolean),
    companyHistoryRelations: companyHistoryResult.rows.map(mapRelationRow),
    companyHistorySummaries: companyHistoryResult.rows.map(mapVehicleSummaryRow).filter(Boolean)
  };
}

async function queryCompanyVehicleHistory(ico, pcv, options = {}) {
  const normalizedIco = sanitizeIco(ico);
  const normalizedPcv = normalizeText(pcv);
  const maxRows = normalizePositiveInteger(options.limit, 500);
  const companyNameKeys = normalizeCompanyNameSearchTerms(options.companyNames);
  if (!normalizedIco || !normalizedPcv) {
    return null;
  }

  return await withOptionalClient(async (client) => {
    const [ownershipVersion, vehicleVersion] = await Promise.all([
      getActiveDatasetVersion("ownership", client),
      getActiveDatasetVersion("vehicles", client)
    ]);
    if (!ownershipVersion) {
      return null;
    }

    const result = await client.query(
      `
        with all_relations as (
	          select id::text as row_id, pcv, null::text as relation_vin, null::text as relation_plate,
	            case
	              when ico is null and lower(name) = any($4::text[]) then $1
              else ico
            end as ico,
            name, address, relation, subject_type,
            current, date_from, date_to, dataset_filename, dataset_date
          from ownership_relations
          where (
            ico = $1
            or (
              ico is null
              and lower(name) = any($4::text[])
            )
          )
          and pcv = $2
          and relation in ('Vlastnik', 'Provozovatel')
          union all
	          select id::text as row_id, pcv, vin as relation_vin, plate as relation_plate, ico, name, address, relation,
	            'supplemental' as subject_type, current, date_from, date_to,
	            source as dataset_filename, null::date as dataset_date
          from supplemental_ownership_relations
          where ico = $1 and pcv = $2 and relation in ('Vlastnik', 'Provozovatel')
        )
        select
          h.pcv, h.ico, h.name, h.address, h.relation, h.subject_type, h.current,
          h.date_from, h.date_to, h.dataset_filename, h.dataset_date,
		          ${vehicleFleetFactsSelect("vf", "h.relation_vin", "h.relation_plate")}
				        from all_relations h
				        ${vehicleFleetFactsJoin("h.pcv", "vf")}
	        order by coalesce(h.current, false) desc, h.date_from desc nulls last, h.date_to desc nulls last, h.row_id desc
        limit $3
      `,
      [normalizedIco, normalizedPcv, maxRows, companyNameKeys]
    );

    return {
      sourceUpdatedAt: ownershipVersion.datasetDate || vehicleVersion?.datasetDate || null,
      limit: maxRows,
      truncated: result.rows.length >= maxRows,
      relations: result.rows.map(mapRelationRow),
      summary: mapVehicleSummaryRow(result.rows[0])
    };
  });
}

async function queryVehicleOwnershipHistory(pcv, options = {}) {
  const normalizedPcv = normalizeText(pcv);
  const maxRows = normalizePositiveInteger(options.limit, 1000);
  if (!normalizedPcv) {
    return null;
  }

  return await withOptionalClient(async (client) => {
    const [ownershipVersion, vehicleVersion] = await Promise.all([
      getActiveDatasetVersion("ownership", client),
      getActiveDatasetVersion("vehicles", client)
    ]);
    if (!ownershipVersion) {
      return null;
    }

    const result = await client.query(
      `
        with all_relations as (
	          select id::text as row_id, pcv, null::text as relation_vin, null::text as relation_plate,
              ico, name, address, relation, subject_type,
	            current, date_from, date_to, dataset_filename, dataset_date
          from ownership_relations
          where pcv = $1 and relation in ('Vlastnik', 'Provozovatel')
          union all
	          select id::text as row_id, pcv, vin as relation_vin, plate as relation_plate, ico, name, address, relation,
	            'supplemental' as subject_type, current, date_from, date_to,
	            source as dataset_filename, null::date as dataset_date
          from supplemental_ownership_relations
          where pcv = $1 and relation in ('Vlastnik', 'Provozovatel')
        )
        select
          h.pcv, h.ico, h.name, h.address, h.relation, h.subject_type, h.current,
          h.date_from, h.date_to, h.dataset_filename, h.dataset_date,
		          ${vehicleFleetFactsSelect("vf", "h.relation_vin", "h.relation_plate")}
				        from all_relations h
				        ${vehicleFleetFactsJoin("h.pcv", "vf")}
	        order by coalesce(h.current, false) desc, h.date_from desc nulls last, h.date_to desc nulls last, h.row_id desc
        limit $2
      `,
      [normalizedPcv, maxRows]
    );

    return {
      sourceUpdatedAt: ownershipVersion.datasetDate || vehicleVersion?.datasetDate || null,
      limit: maxRows,
      truncated: result.rows.length >= maxRows,
      relations: result.rows.map(mapRelationRow),
      summary: mapVehicleSummaryRow(result.rows[0])
    };
  });
}

async function queryInspectionsByPcv(pcv) {
  const normalizedPcv = normalizeText(pcv);
  if (!normalizedPcv) {
    return null;
  }

  return await withOptionalClient(async (client) => {
    const active = await getActiveDatasetVersion("inspections", client);
    if (!active) {
      return null;
    }

    const result = await client.query(
      `
        select pcv, type, state, station_code, station_name, valid_from, valid_until,
          protocol_number, odometer, odometer_unit, current, dataset_filename, dataset_date
        from inspections
        where pcv = $1
        order by coalesce(current, false) desc, valid_from desc nulls last, valid_until desc nulls last
      `,
      [normalizedPcv]
    );

    return {
      pcv: normalizedPcv,
      sourceFile: active.filename || null,
      sourceUpdatedAt: active.datasetDate || null,
      records: result.rows.map(mapInspectionRow)
    };
	  });
	}

async function queryInspectionsByVin(vin) {
  const normalizedVin = normalizeVin(vin);
  if (!normalizedVin) {
    return null;
  }

  return await withOptionalClient(async (client) => {
    const active = await getActiveDatasetVersion("inspections", client);
    if (!active) {
      return null;
    }

    const result = await client.query(
      `
        with matched_pcvs as (
          select pcv from vehicle_vins where vin = $1
          union
          select pcv from vehicles
          where ${sqlNormalizedVin("vin")} = $1
        )
        select i.pcv, i.type, i.state, i.station_code, i.station_name, i.valid_from, i.valid_until,
          i.protocol_number, i.odometer, i.odometer_unit, i.current, i.dataset_filename, i.dataset_date
        from inspections i
        join matched_pcvs m on m.pcv = i.pcv
        order by coalesce(i.current, false) desc, i.valid_from desc nulls last, i.valid_until desc nulls last
      `,
      [normalizedVin]
    );

    return {
      pcv: normalizeText(result.rows[0]?.pcv) || null,
      sourceFile: active.filename || null,
      sourceUpdatedAt: active.datasetDate || null,
      records: result.rows.map(mapInspectionRow)
    };
  });
}

async function refreshVehicleInspectionSummaries(client = null) {
  const run = async (db) => {
    await db.query(REFRESH_VEHICLE_INSPECTION_SUMMARIES_SQL);
    return true;
  };

  if (client) {
    return await run(client);
  }

  return await withOptionalClient(run);
}

async function refreshVehiclePlateSummaries(client = null) {
  const run = async (db) => {
    await db.query(REFRESH_VEHICLE_PLATE_SUMMARIES_SQL);
    return true;
  };

  if (client) {
    return await run(client);
  }

  return await withOptionalClient(run);
}

async function refreshVehicleFleetFacts(client = null) {
  const run = async (db) => {
    await db.query(refreshVehicleFleetFactsSql());
    return true;
  };

  if (client) {
    return await run(client);
  }

  return await withOptionalClient(run);
}

async function refreshCompanyVehicleFacts(client = null) {
  const run = async (db) => {
    await db.query(refreshCompanyVehicleFactsSql());
    return true;
  };

  if (client) {
    return await run(client);
  }

  return await withOptionalClient(run);
}

async function getCachedAresCompany(ico) {
  const normalizedIco = sanitizeIco(ico);
  if (!normalizedIco) {
    return null;
  }

  return await withOptionalClient(async (client) => {
    const result = await client.query(
      `
        select ico, name, address, fetched_at, expires_at
        from ares_companies
        where ico = $1 and (expires_at is null or expires_at > now())
        limit 1
      `,
      [normalizedIco]
    );

    const row = result.rows[0];
    return row
      ? {
          ico: row.ico,
          name: row.name || null,
          address: row.address || null,
          fetchedAt: formatDateTime(row.fetched_at),
          expiresAt: formatDateTime(row.expires_at)
        }
      : null;
  });
}

async function getCachedPlateResolution(plate) {
  const normalizedPlate = normalizePlate(plate);
  if (!normalizedPlate) {
    return null;
  }

  return await withOptionalClient(async (client) => {
    const result = await client.query(
      `
        select plate, vin, pcv, source, confidence, resolved_at, expires_at
        from (
          select plate, vin, pcv, source, confidence, last_seen_at as resolved_at, expires_at
          from vehicle_plate_links
          where plate = $1 and (expires_at is null or expires_at > now())
          union all
          select plate, vin, pcv, source, confidence, resolved_at, expires_at
          from plate_resolutions
          where plate = $1 and (expires_at is null or expires_at > now())
        ) matches
        order by confidence desc nulls last, resolved_at desc nulls last
        limit 1
      `,
      [normalizedPlate]
    );

    return mapPlateResolutionRow(result.rows[0]);
  });
}

async function getCachedPlateResolutionByVehicle(input = {}) {
  const vin = normalizeVin(input.vin) || null;
  const pcv = normalizeText(input.pcv) || null;
  if (!vin && !pcv) {
    return null;
  }

  return await withOptionalClient(async (client) => {
    const result = await client.query(
      `
	        select plate, vin, pcv, source, confidence, resolved_at, expires_at
	        from (
	          select plate, vin, pcv, source, confidence, resolved_at, expires_at
	          from vehicle_plate_summaries
	          where (
	            ($1::text is not null and ${sqlNormalizedVin("vin")} = $1)
	            or ($2::text is not null and pcv = $2)
	          )
	          and plate is not null
	          and (expires_at is null or expires_at > now())
	          union all
	          select plate, vin, pcv, source, confidence, last_seen_at as resolved_at, expires_at
	          from vehicle_plate_links
	          where (
	            ($1::text is not null and ${sqlNormalizedVin("vin")} = $1)
	            or ($2::text is not null and pcv = $2)
	            or ($1::text is not null and vehicle_key_type = 'vin' and vehicle_key = $1)
	          )
	          and plate is not null
	          and (expires_at is null or expires_at > now())
          union all
	          select plate, vin, pcv, source, confidence, resolved_at, expires_at
	          from plate_resolutions
	          where (
	            ($1::text is not null and ${sqlNormalizedVin("vin")} = $1)
	            or ($2::text is not null and pcv = $2)
	          )
          and plate is not null
          and (expires_at is null or expires_at > now())
        ) matches
        order by confidence desc nulls last, resolved_at desc nulls last
        limit 1
      `,
      [vin, pcv]
    );

    return mapPlateResolutionRow(result.rows[0]);
  });
}

async function storePlateResolution(input = {}) {
  const plate = normalizePlate(input.plate);
  const vin = normalizeVin(input.vin) || null;
  const pcv = normalizeText(input.pcv) || null;
  const source = normalizeText(input.source) || "lookup";
  const confidence = normalizeConfidence(input.confidence, vin || pcv ? 0.8 : 0.3);
  const ttlMs = normalizePositiveInteger(input.ttlMs, 2592000000);

  if (!plate || (!vin && !pcv)) {
    return null;
  }

  return await withOptionalClient(async (client) => {
    const linkedPcv = await resolveExistingPcvForPlateLink(client, pcv, vin);
    const result = await client.query(
      `
        with legacy as (
          insert into plate_resolutions (
            plate, vin, pcv, source, confidence, resolved_at, expires_at, updated_at
          )
          values ($1, $2, $3, $4, $5, now(), now() + ($6::bigint * interval '1 millisecond'), now())
          on conflict (plate) do update set
            vin = coalesce(excluded.vin, plate_resolutions.vin),
            pcv = coalesce(excluded.pcv, plate_resolutions.pcv),
            source = excluded.source,
            confidence = excluded.confidence,
            resolved_at = excluded.resolved_at,
            expires_at = excluded.expires_at,
            updated_at = excluded.updated_at
          returning plate, vin, pcv, source, confidence, resolved_at, expires_at
        ),
	        pcv_link as (
	          insert into vehicle_plate_links (
	            plate, vin, pcv, vehicle_key_type, vehicle_key, source, confidence,
	            first_seen_at, last_seen_at, expires_at, updated_at
	          )
	          select $1, $2, $3, 'pcv', $3, $4, $5, now(), now(), now() + ($6::bigint * interval '1 millisecond'), now()
          where $3::text is not null
          on conflict (vehicle_key_type, vehicle_key, plate) do update set
            vin = coalesce(excluded.vin, vehicle_plate_links.vin),
            pcv = coalesce(excluded.pcv, vehicle_plate_links.pcv),
            source = excluded.source,
            confidence = greatest(excluded.confidence, vehicle_plate_links.confidence),
            last_seen_at = greatest(excluded.last_seen_at, coalesce(vehicle_plate_links.last_seen_at, excluded.last_seen_at)),
            expires_at = excluded.expires_at,
	            updated_at = excluded.updated_at
	          returning plate, vin, pcv, source, confidence, last_seen_at as resolved_at, expires_at
	        ),
		        vin_link as (
		          insert into vehicle_plate_links (
	            plate, vin, pcv, vehicle_key_type, vehicle_key, source, confidence,
	            first_seen_at, last_seen_at, expires_at, updated_at
	          )
	          select $1, $2, $3, 'vin', ${sqlNormalizedVin("$2")}, $4, $5, now(), now(), now() + ($6::bigint * interval '1 millisecond'), now()
	          where $2::text is not null
	          on conflict (vehicle_key_type, vehicle_key, plate) do update set
	            vin = coalesce(excluded.vin, vehicle_plate_links.vin),
	            pcv = coalesce(excluded.pcv, vehicle_plate_links.pcv),
	            source = excluded.source,
	            confidence = greatest(excluded.confidence, vehicle_plate_links.confidence),
            last_seen_at = greatest(excluded.last_seen_at, coalesce(vehicle_plate_links.last_seen_at, excluded.last_seen_at)),
	            expires_at = excluded.expires_at,
	            updated_at = excluded.updated_at
		          returning plate, vin, pcv, source, confidence, last_seen_at as resolved_at, expires_at
		        ),
		        summary as (
		          insert into vehicle_plate_summaries (
		            pcv, vin, plate, source, confidence, resolved_at, expires_at, updated_at
		          )
		          select $3, $2, $1, $4, $5, now(), now() + ($6::bigint * interval '1 millisecond'), now()
		          where $3::text is not null
		          on conflict (pcv) do update set
		            vin = coalesce(excluded.vin, vehicle_plate_summaries.vin),
		            plate = case
		              when excluded.confidence >= vehicle_plate_summaries.confidence then excluded.plate
		              else vehicle_plate_summaries.plate
		            end,
		            source = case
		              when excluded.confidence >= vehicle_plate_summaries.confidence then excluded.source
		              else vehicle_plate_summaries.source
		            end,
		            confidence = greatest(excluded.confidence, vehicle_plate_summaries.confidence),
			            resolved_at = greatest(excluded.resolved_at, coalesce(vehicle_plate_summaries.resolved_at, excluded.resolved_at)),
		            expires_at = excluded.expires_at,
		            updated_at = now()
		          returning plate, vin, pcv, source, confidence, resolved_at, expires_at
		        )
		        select plate, vin, pcv, source, confidence, resolved_at, expires_at from summary
		        union all
		        select plate, vin, pcv, source, confidence, resolved_at, expires_at from pcv_link
	        union all
	        select plate, vin, pcv, source, confidence, resolved_at, expires_at from vin_link
	        union all
	        select plate, vin, pcv, source, confidence, resolved_at, expires_at from legacy
	        limit 1
      `,
      [plate, vin, linkedPcv, source, confidence, ttlMs]
    );
    return mapPlateResolutionRow(result.rows[0]);
  });
}

async function resolveExistingPcvForPlateLink(client, pcv, vin) {
  const normalizedPcv = normalizeText(pcv);
  if (normalizedPcv) {
    const result = await client.query("select pcv from vehicles where pcv = $1 limit 1", [normalizedPcv]);
    if (result.rows[0]?.pcv) {
      return normalizeText(result.rows[0].pcv);
    }
  }

  return await resolvePcvForPlateLink(client, vin);
}

async function resolvePcvForPlateLink(client, vin) {
  const normalizedVin = normalizeVin(vin);
  if (!normalizedVin) {
    return null;
  }

  const result = await client.query(
    `
      select pcv
      from vehicle_vins
      where vin = $1
      union
      select pcv
      from vehicles
      where vin = $1
      limit 1
    `,
    [normalizedVin]
  );
  return normalizeText(result.rows[0]?.pcv) || null;
}

async function storeCachedAresCompany(ico, company) {
  const normalizedIco = sanitizeIco(ico);
  if (!normalizedIco || !company) {
    return null;
  }

  return await withOptionalClient(async (client) => {
    await client.query(
      `
        insert into ares_companies (ico, name, address, fetched_at, expires_at)
        values ($1, $2, $3, now(), now() + ($4::bigint * interval '1 millisecond'))
        on conflict (ico) do update set
          name = excluded.name,
          address = excluded.address,
          fetched_at = excluded.fetched_at,
          expires_at = excluded.expires_at
      `,
      [normalizedIco, company.name || null, company.address || null, ARES_CACHE_TTL_MS]
    );
    return true;
	  });
	}

async function storeSupplementalOwnershipRelations(input = {}) {
  const pcv = normalizeText(input.pcv);
  const vin = normalizeVin(input.vin) || normalizeText(input.vin) || null;
  const plate = normalizePlate(input.plate);
  const source = normalizeText(input.source) || "lookup";
  const relations = Array.isArray(input.relations) ? input.relations : [];
  const rows = relations
    .map((relation) => ({
      pcv,
      vin,
      plate,
      ico: sanitizeIco(relation.ico),
      name: normalizeText(relation.name),
      address: normalizeText(relation.address),
      relation: normalizeRelation(relation.relation || relation.role),
      current: relation.current === undefined || relation.current === null ? true : Boolean(relation.current),
      date_from: parseDateOnly(relation.dateFrom || relation.since),
      date_to: parseDateOnly(relation.dateTo || relation.until),
      source
    }))
    .filter((row) => row.pcv && row.ico && row.relation);

  if (rows.length === 0) {
    return 0;
  }

  return await withOptionalClient(async (client) => {
    let count = 0;
    for (const row of rows) {
      await client.query(
        `
          insert into supplemental_ownership_relations (
            pcv, vin, plate, ico, name, address, relation, current, date_from, date_to, source, observed_at, updated_at
          )
          values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), now())
          on conflict (pcv, ico, relation) do update set
            vin = coalesce(excluded.vin, supplemental_ownership_relations.vin),
            plate = coalesce(excluded.plate, supplemental_ownership_relations.plate),
            name = coalesce(excluded.name, supplemental_ownership_relations.name),
            address = coalesce(excluded.address, supplemental_ownership_relations.address),
            current = excluded.current,
            date_from = coalesce(excluded.date_from, supplemental_ownership_relations.date_from),
            date_to = excluded.date_to,
            source = excluded.source,
            observed_at = now(),
            updated_at = now()
        `,
        [
          row.pcv,
          row.vin,
          row.plate,
          row.ico,
          row.name || null,
          row.address || null,
          row.relation,
          row.current,
          row.date_from,
          row.date_to,
          row.source
        ]
      );
      count += 1;
    }
    return count;
  });
}

function mapDatasetVersionRow(row) {
  return {
    source: row.source,
    filename: row.filename,
    datasetDate: formatDateOnly(row.dataset_date),
    recordCount: Number(row.record_count || 0),
    lastCheckedAt: formatDateTime(row.last_checked_at),
    importFinishedAt: formatDateTime(row.import_finished_at)
  };
}

function mapRelationRow(row) {
  return {
    pcv: normalizeText(row.pcv),
    ico: sanitizeIco(row.ico),
    name: normalizeText(row.name),
    address: normalizeText(row.address),
    relation: normalizeText(row.relation),
    subjectType: normalizeText(row.subject_type),
    current: row.current === null || row.current === undefined ? null : Boolean(row.current),
    dateFrom: formatDateTime(row.date_from),
    dateTo: formatDateTime(row.date_to),
    datasetFilename: row.dataset_filename || null,
    datasetDate: formatDateOnly(row.dataset_date),
    historyRelationCount: normalizeInteger(row.history_relation_count)
  };
}

function mapPlateResolutionRow(row) {
  if (!row) {
    return null;
  }

  return {
    plate: normalizePlate(row.plate),
    vin: normalizeVin(row.vin) || normalizeText(row.vin) || null,
    pcv: normalizeText(row.pcv) || null,
    source: normalizeText(row.source) || null,
    confidence: Number.isFinite(Number(row.confidence)) ? Number(row.confidence) : null,
    resolvedAt: formatDateTime(row.resolved_at),
    expiresAt: formatDateTime(row.expires_at)
  };
}

function mapVehicleSummaryRow(row) {
  const pcv = normalizeText(row.pcv);
  if (!pcv) {
    return null;
  }

			  return {
			    pcv,
			    plate: normalizePlate(row.plate) || null,
			    vin: normalizeText(row.vin) || null,
		    make: normalizeText(row.make),
	    model: normalizeText(row.model),
	    type: normalizeText(row.type),
	    variant: normalizeText(row.variant),
	    category: normalizeText(row.category),
	    fuel: normalizeText(row.fuel),
	    firstRegistration: formatDateTime(row.first_registration),
		    firstRegistrationCz: formatDateTime(row.first_registration_cz),
		    power: normalizeText(row.power),
		    color: normalizeText(row.color),
		    lengthMm: normalizeText(row.length_mm),
		    widthMm: normalizeText(row.width_mm),
		    heightMm: normalizeText(row.height_mm),
		    wheelbaseMm: normalizeText(row.wheelbase_mm),
		    weightKg: normalizeText(row.weight_kg),
		    status: normalizeText(row.vehicle_status),
        inspection: mapInspectionSummaryRow(row)
		  };
		}

function mapInspectionSummaryRow(row) {
  const type = normalizeText(row.inspection_type) || null;
  const validFrom = formatDateTime(row.inspection_valid_from);
  const validUntil = formatDateTime(row.inspection_valid_until);
  const performedOn = formatDateTime(row.inspection_performed_on || row.inspection_valid_from);
  if (!type && !validFrom && !validUntil && !performedOn) {
    return null;
  }

  return {
    type,
    state: normalizeText(row.inspection_state) || null,
    stationCode: normalizeText(row.inspection_station_code) || null,
    stationName: normalizeText(row.inspection_station_name) || null,
    performedOn,
    validFrom,
    validUntil,
    protocolNumber: normalizeText(row.inspection_protocol_number) || null,
    odometer: normalizeInteger(row.inspection_odometer),
    odometerUnit: normalizeText(row.inspection_odometer_unit) || null,
    current: row.inspection_current === null || row.inspection_current === undefined
      ? null
      : Boolean(row.inspection_current),
    datasetFilename: normalizeText(row.inspection_dataset_filename) || null,
    datasetDate: formatDateTime(row.inspection_dataset_date)
  };
}

function mapInspectionRow(row) {
  return {
    type: normalizeText(row.type) || null,
    state: normalizeText(row.state) || null,
    stationCode: normalizeText(row.station_code) || null,
    stationName: normalizeText(row.station_name) || null,
    validFrom: formatDateTime(row.valid_from),
    validUntil: formatDateTime(row.valid_until),
    protocolNumber: normalizeText(row.protocol_number) || null,
    odometer: normalizeInteger(row.odometer),
    odometerUnit: normalizeText(row.odometer_unit) || null,
    current: row.current === null || row.current === undefined ? null : Boolean(row.current)
  };
}

function formatDateOnly(value) {
  if (!value) {
    return null;
  }

  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      return null;
    }
    return [
      value.getFullYear(),
      String(value.getMonth() + 1).padStart(2, "0"),
      String(value.getDate()).padStart(2, "0")
    ].join("-");
  }

  const raw = String(value).trim();
  if (!raw) {
    return null;
  }

  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (match) {
    return `${match[1]}-${match[2]}-${match[3]}`;
  }

  const dateTime = formatDateTime(raw);
  return dateTime ? dateTime.slice(0, 10) : null;
}

function formatDateTime(value) {
  if (!value) {
    return null;
  }

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value.toISOString();
  }

  const raw = String(value).trim();
  if (!raw) {
    return null;
  }

  const parsed = new Date(raw);
  return Number.isNaN(parsed.getTime()) ? raw : parsed.toISOString();
}

function normalizeVin(value) {
  const normalized = normalizeText(value).toUpperCase();
  return /^[A-HJ-NPR-Z0-9]{17}$/.test(normalized) ? normalized : null;
}

function normalizeText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalizeCompanyNameSearchTerms(values) {
  const rawValues = Array.isArray(values) ? values : [values];
  const names = new Set();
  rawValues.forEach((value) => {
    const normalized = normalizeText(value);
    if (normalized) {
      names.add(normalized.toLowerCase());
    }
  });
  return Array.from(names).slice(0, 8);
}

function sanitizeIco(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return /^\d{8}$/.test(digits) ? digits : null;
}

function normalizePlate(value) {
  const normalized = normalizeText(value).toUpperCase().replace(/[^A-Z0-9]/g, "");
  return /^[A-Z0-9]{5,10}$/.test(normalized) ? normalized : null;
}

function normalizeConfidence(value, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(0, Math.min(1, parsed));
}

function normalizeRelation(value) {
  const normalized = normalizeText(value);
  const lower = normalized
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
  if (normalized === "1" || lower.includes("vlast")) {
    return "Vlastnik";
  }
  if (normalized === "2" || lower.includes("provoz")) {
    return "Provozovatel";
  }
  return normalized || null;
}

function parseDateOnly(value) {
  const normalized = normalizeText(value);
  if (!normalized || normalized === "-") {
    return null;
  }

  const iso = normalized.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (iso) {
    return `${iso[1]}-${iso[2].padStart(2, "0")}-${iso[3].padStart(2, "0")}`;
  }

  const czech = normalized.match(/^(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})/);
  if (czech) {
    return `${czech[3]}-${czech[2].padStart(2, "0")}-${czech[1].padStart(2, "0")}`;
  }

  const parsed = new Date(normalized);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString().slice(0, 10);
}

function normalizeInteger(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const normalized = String(value).replace(/\s+/g, "").replace(",", ".");
  const parsed = Number.parseInt(normalized, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizePositiveInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

const SCHEMA_PRE_MIGRATION_SQL = `
do $$
begin
  if to_regclass('vehicles') is not null then
    execute 'alter table vehicles add column if not exists plate text';
    execute 'alter table vehicles add column if not exists variant text';
    execute 'alter table vehicles add column if not exists first_registration_cz date';
    execute 'alter table vehicles add column if not exists power text';
    execute 'alter table vehicles add column if not exists color text';
    execute 'alter table vehicles add column if not exists length_mm text';
    execute 'alter table vehicles add column if not exists width_mm text';
    execute 'alter table vehicles add column if not exists height_mm text';
    execute 'alter table vehicles add column if not exists wheelbase_mm text';
    execute 'alter table vehicles add column if not exists weight_kg text';
  end if;

	  if to_regclass('vehicles_staging') is not null then
    execute 'alter table vehicles_staging add column if not exists plate text';
    execute 'alter table vehicles_staging add column if not exists variant text';
    execute 'alter table vehicles_staging add column if not exists first_registration_cz date';
    execute 'alter table vehicles_staging add column if not exists power text';
	    execute 'alter table vehicles_staging add column if not exists color text';
    execute 'alter table vehicles_staging add column if not exists length_mm text';
    execute 'alter table vehicles_staging add column if not exists width_mm text';
    execute 'alter table vehicles_staging add column if not exists height_mm text';
    execute 'alter table vehicles_staging add column if not exists wheelbase_mm text';
    execute 'alter table vehicles_staging add column if not exists weight_kg text';
	  end if;

  if exists (
    select 1 from pg_class
    where oid = to_regclass('vehicle_fleet_facts')
      and relkind in ('r', 'p')
  ) then
    execute 'alter table vehicle_fleet_facts add column if not exists length_mm text';
    execute 'alter table vehicle_fleet_facts add column if not exists width_mm text';
    execute 'alter table vehicle_fleet_facts add column if not exists height_mm text';
    execute 'alter table vehicle_fleet_facts add column if not exists wheelbase_mm text';
    execute 'alter table vehicle_fleet_facts add column if not exists weight_kg text';
  end if;

  if exists (
    select 1 from pg_class
    where oid = to_regclass('company_vehicle_facts')
      and relkind in ('r', 'p')
  ) then
    execute 'alter table company_vehicle_facts add column if not exists length_mm text';
    execute 'alter table company_vehicle_facts add column if not exists width_mm text';
    execute 'alter table company_vehicle_facts add column if not exists height_mm text';
    execute 'alter table company_vehicle_facts add column if not exists wheelbase_mm text';
    execute 'alter table company_vehicle_facts add column if not exists weight_kg text';
  end if;

	  if to_regclass('ownership_relations') is not null then
	    execute 'alter table ownership_relations add column if not exists party_id bigint';
	  end if;

  if to_regclass('supplemental_ownership_relations') is not null then
    execute 'alter table supplemental_ownership_relations add column if not exists vin text';
    execute 'alter table supplemental_ownership_relations add column if not exists plate text';
    execute 'alter table supplemental_ownership_relations add column if not exists observed_at timestamptz not null default now()';
    execute 'alter table supplemental_ownership_relations add column if not exists updated_at timestamptz not null default now()';
  end if;

  if to_regclass('plate_resolutions') is not null then
    execute 'alter table plate_resolutions add column if not exists pcv text';
    execute 'alter table plate_resolutions add column if not exists confidence numeric(3, 2) not null default 0.80';
    execute 'alter table plate_resolutions add column if not exists expires_at timestamptz';
    execute 'alter table plate_resolutions add column if not exists updated_at timestamptz not null default now()';
  end if;

  if to_regclass('vehicle_plate_links') is not null then
    execute 'alter table vehicle_plate_links add column if not exists vin text';
    execute 'alter table vehicle_plate_links add column if not exists pcv text';
    execute 'alter table vehicle_plate_links add column if not exists vehicle_key_type text';
    execute 'alter table vehicle_plate_links add column if not exists vehicle_key text';
    execute 'alter table vehicle_plate_links add column if not exists confidence numeric(3, 2) not null default 0.80';
    execute 'alter table vehicle_plate_links add column if not exists first_seen_at timestamptz not null default now()';
    execute 'alter table vehicle_plate_links add column if not exists last_seen_at timestamptz not null default now()';
    execute 'alter table vehicle_plate_links add column if not exists expires_at timestamptz';
    execute 'alter table vehicle_plate_links add column if not exists updated_at timestamptz not null default now()';
  end if;
end $$;
`;

const SCHEMA_SQL = `
create table if not exists dataset_versions (
  id bigserial primary key,
  source text not null,
  filename text not null,
  dataset_date date,
  status text not null default 'pending',
  active boolean not null default false,
  record_count bigint not null default 0,
  last_checked_at timestamptz not null default now(),
  import_started_at timestamptz,
  import_finished_at timestamptz,
  error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (source, filename)
);

create table if not exists vehicles (
	  pcv text primary key,
	  plate text,
		  vin text,
	  make text,
	  model text,
	  type text,
	  variant text,
	  category text,
	  fuel text,
	  first_registration date,
	  first_registration_cz date,
	  power text,
	  color text,
	  length_mm text,
	  width_mm text,
	  height_mm text,
	  wheelbase_mm text,
	  weight_kg text,
	  status text,
  dataset_filename text,
  dataset_date date,
  imported_at timestamptz not null default now()
);

create table if not exists vehicle_vins (
  vin text primary key,
  pcv text not null,
  dataset_filename text,
  dataset_date date,
  imported_at timestamptz not null default now()
);

create table if not exists ownership_parties (
  id bigserial primary key,
  party_key text not null unique,
  ico text,
  name text,
  name_key text,
  address text,
  subject_type text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unlogged table if not exists vehicles_staging (
	  pcv text,
	  plate text,
		  vin text,
	  make text,
	  model text,
	  type text,
	  variant text,
	  category text,
	  fuel text,
	  first_registration date,
	  first_registration_cz date,
	  power text,
	  color text,
	  length_mm text,
	  width_mm text,
	  height_mm text,
	  wheelbase_mm text,
	  weight_kg text,
	  status text,
  dataset_filename text,
  dataset_date date
);

create unlogged table if not exists vehicle_vins_staging (
  vin text,
  pcv text,
  dataset_filename text,
  dataset_date date
);

create table if not exists ownership_relations (
  id bigserial primary key,
  party_id bigint,
  pcv text not null,
  ico text,
  name text,
  address text,
  relation text,
  subject_type text,
  current boolean,
  date_from date,
  date_to date,
  dataset_filename text,
  dataset_date date,
  imported_at timestamptz not null default now()
);

create unlogged table if not exists ownership_relations_staging (
  pcv text,
  ico text,
  name text,
  address text,
  relation text,
  subject_type text,
  current boolean,
  date_from date,
  date_to date,
  dataset_filename text,
  dataset_date date
);

create table if not exists inspections (
  id bigserial primary key,
  pcv text not null,
  type text,
  state text,
  station_code text,
  station_name text,
  valid_from date,
  valid_until date,
  protocol_number text,
  odometer integer,
  odometer_unit text,
  current boolean,
  dataset_filename text,
  dataset_date date,
  imported_at timestamptz not null default now()
);

create unlogged table if not exists inspections_staging (
  pcv text,
  type text,
  state text,
  station_code text,
  station_name text,
  valid_from date,
  valid_until date,
  protocol_number text,
  odometer integer,
  odometer_unit text,
  current boolean,
  dataset_filename text,
  dataset_date date
);

create table if not exists vehicle_inspection_summaries (
  pcv text primary key,
  type text,
  state text,
  station_code text,
  station_name text,
  performed_on date,
  valid_from date,
  valid_until date,
  protocol_number text,
  odometer integer,
  odometer_unit text,
  current boolean,
  dataset_filename text,
  dataset_date date,
  inspection_id bigint,
  updated_at timestamptz not null default now()
);

create table if not exists vehicle_imports (
  id bigserial primary key,
  pcv text not null,
  country text,
  imported_on date,
  dataset_filename text,
  dataset_date date,
  imported_at timestamptz not null default now()
);

create unlogged table if not exists vehicle_imports_staging (
  pcv text,
  country text,
  imported_on date,
  dataset_filename text,
  dataset_date date
);

create table if not exists vehicle_deregistrations (
  id bigserial primary key,
  pcv text not null,
  date_from date,
  date_to date,
  reason text,
  rm_code text,
  rm_name text,
  dataset_filename text,
  dataset_date date,
  imported_at timestamptz not null default now()
);

create unlogged table if not exists vehicle_deregistrations_staging (
  pcv text,
  date_from date,
  date_to date,
  reason text,
  rm_code text,
  rm_name text,
  dataset_filename text,
  dataset_date date
);

create table if not exists open_data_aux_records (
  id bigserial primary key,
  source text not null,
  pcv text,
  vin text,
  record_key text,
  payload jsonb not null default '{}'::jsonb,
  dataset_filename text,
  dataset_date date,
  imported_at timestamptz not null default now()
);

create unlogged table if not exists open_data_aux_records_staging (
  source text not null,
  pcv text,
  vin text,
  record_key text,
  payload text not null,
  dataset_filename text,
  dataset_date date
);

create table if not exists ares_companies (
  ico text primary key,
  name text,
  address text,
  fetched_at timestamptz not null default now(),
  expires_at timestamptz
);

create table if not exists supplemental_ownership_relations (
  id bigserial primary key,
  pcv text not null,
  vin text,
  plate text,
  ico text not null,
  name text,
  address text,
  relation text not null,
  current boolean not null default true,
  date_from date,
  date_to date,
  source text not null default 'lookup',
  observed_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (pcv, ico, relation)
);

create table if not exists plate_resolutions (
  plate text primary key,
  vin text,
  pcv text,
  source text not null default 'lookup',
  confidence numeric(3, 2) not null default 0.80,
  resolved_at timestamptz not null default now(),
  expires_at timestamptz,
  updated_at timestamptz not null default now()
);

create table if not exists vehicle_plate_links (
  id bigserial primary key,
  plate text not null,
  vin text,
  pcv text,
  vehicle_key_type text not null,
  vehicle_key text not null,
  source text not null default 'lookup',
  confidence numeric(3, 2) not null default 0.80,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  expires_at timestamptz,
  updated_at timestamptz not null default now(),
  check (vehicle_key_type in ('pcv', 'vin'))
);

create table if not exists vehicle_plate_summaries (
  pcv text primary key,
  vin text,
  plate text not null,
  source text not null default 'lookup',
  confidence numeric(3, 2) not null default 0.80,
  resolved_at timestamptz,
  expires_at timestamptz,
  updated_at timestamptz not null default now()
);

${vehicleFleetFactsTableSql()}
${companyVehicleFactsSchemaSql()}
${companyVehicleFactsIndexSql()}

create index if not exists dataset_versions_source_active_idx
  on dataset_versions (source, active, status);
create index if not exists vehicles_vin_idx
  on vehicles (vin) where vin is not null;
create index if not exists vehicles_vin_norm_idx
  on vehicles ((upper(regexp_replace(coalesce(vin, ''), '[^A-Za-z0-9]', '', 'g'))))
  where vin is not null;
create index if not exists vehicles_plate_idx
  on vehicles (plate) where plate is not null;
create index if not exists vehicle_vins_pcv_idx
  on vehicle_vins (pcv);
create index if not exists ownership_parties_ico_idx
  on ownership_parties (ico) where ico is not null;
create index if not exists ownership_parties_name_key_idx
  on ownership_parties (name_key) where name_key is not null;
create index if not exists ownership_relations_ico_idx
  on ownership_relations (ico);
create index if not exists ownership_relations_party_id_idx
  on ownership_relations (party_id) where party_id is not null;
create index if not exists ownership_relations_pcv_idx
  on ownership_relations (pcv);
create index if not exists ownership_relations_current_idx
  on ownership_relations (ico, pcv) where current is true and date_to is null;
create index if not exists ownership_relations_ico_current_relation_idx
  on ownership_relations (ico, pcv) where current is true and date_to is null and relation in ('Vlastnik', 'Provozovatel');
create index if not exists ownership_relations_ico_history_idx
  on ownership_relations (ico, date_from desc, pcv) where relation in ('Vlastnik', 'Provozovatel');
create index if not exists ownership_relations_pcv_history_idx
  on ownership_relations (pcv, date_from desc) where relation in ('Vlastnik', 'Provozovatel');
create index if not exists ownership_relations_missing_ico_name_current_relation_idx
  on ownership_relations (lower(name), pcv) where ico is null and name is not null and current is true and date_to is null and relation in ('Vlastnik', 'Provozovatel');
create index if not exists ownership_relations_missing_ico_name_history_idx
  on ownership_relations (lower(name), date_from desc, pcv) where ico is null and name is not null and relation in ('Vlastnik', 'Provozovatel');
create index if not exists inspections_pcv_idx
  on inspections (pcv);
create index if not exists inspections_current_idx
  on inspections (pcv) where current is true;
create index if not exists inspections_pcv_valid_until_idx
  on inspections (pcv, valid_until desc);
create index if not exists vehicle_inspection_summaries_valid_until_idx
  on vehicle_inspection_summaries (valid_until desc) where valid_until is not null;
create index if not exists vehicle_inspection_summaries_current_idx
  on vehicle_inspection_summaries (pcv) where current is true;
create index if not exists vehicle_imports_pcv_idx
  on vehicle_imports (pcv);
create index if not exists vehicle_imports_imported_on_idx
  on vehicle_imports (imported_on desc);
create index if not exists vehicle_deregistrations_pcv_idx
  on vehicle_deregistrations (pcv);
create index if not exists vehicle_deregistrations_pcv_date_idx
  on vehicle_deregistrations (pcv, date_from desc);
create index if not exists open_data_aux_source_pcv_idx
  on open_data_aux_records (source, pcv) where pcv is not null;
create index if not exists open_data_aux_source_vin_idx
  on open_data_aux_records (source, vin) where vin is not null;
create index if not exists open_data_aux_source_record_key_idx
  on open_data_aux_records (source, record_key) where record_key is not null;
create index if not exists supplemental_ownership_ico_current_idx
  on supplemental_ownership_relations (ico, pcv) where current is true and date_to is null;
create index if not exists supplemental_ownership_pcv_idx
  on supplemental_ownership_relations (pcv);
create index if not exists supplemental_ownership_vin_idx
  on supplemental_ownership_relations (vin) where vin is not null;
create index if not exists supplemental_ownership_plate_idx
  on supplemental_ownership_relations (plate) where plate is not null;
create index if not exists supplemental_ownership_ico_history_idx
  on supplemental_ownership_relations (ico, date_from desc, pcv);
create index if not exists supplemental_ownership_pcv_history_idx
  on supplemental_ownership_relations (pcv, date_from desc);
create index if not exists plate_resolutions_vin_idx
  on plate_resolutions (vin) where vin is not null;
create index if not exists plate_resolutions_vin_norm_idx
  on plate_resolutions ((upper(regexp_replace(coalesce(vin, ''), '[^A-Za-z0-9]', '', 'g'))))
  where vin is not null;
create index if not exists plate_resolutions_pcv_idx
  on plate_resolutions (pcv) where pcv is not null;
create unique index if not exists vehicle_plate_links_vehicle_plate_idx
  on vehicle_plate_links (vehicle_key_type, vehicle_key, plate);
create index if not exists vehicle_plate_links_plate_idx
  on vehicle_plate_links (plate);
create index if not exists vehicle_plate_links_vin_idx
  on vehicle_plate_links (vin) where vin is not null;
create index if not exists vehicle_plate_links_vin_norm_idx
  on vehicle_plate_links ((upper(regexp_replace(coalesce(vin, ''), '[^A-Za-z0-9]', '', 'g'))))
  where vin is not null;
create index if not exists vehicle_plate_links_vehicle_key_idx
  on vehicle_plate_links (vehicle_key_type, vehicle_key);
create index if not exists vehicle_plate_links_pcv_idx
  on vehicle_plate_links (pcv) where pcv is not null;
create index if not exists vehicle_plate_summaries_plate_idx
  on vehicle_plate_summaries (plate);
create index if not exists vehicle_plate_summaries_vin_norm_idx
  on vehicle_plate_summaries ((upper(regexp_replace(coalesce(vin, ''), '[^A-Za-z0-9]', '', 'g'))))
  where vin is not null;

alter table ownership_relations alter column ico drop not null;
alter table ownership_relations add column if not exists party_id bigint;
alter table ownership_relations_staging alter column ico drop not null;
alter table inspections add column if not exists odometer integer;
alter table inspections add column if not exists odometer_unit text;
alter table inspections_staging add column if not exists odometer integer;
alter table inspections_staging add column if not exists odometer_unit text;
alter table vehicles add column if not exists variant text;
alter table vehicles add column if not exists plate text;
alter table vehicles add column if not exists first_registration_cz date;
alter table vehicles add column if not exists power text;
alter table vehicles add column if not exists color text;
alter table vehicles add column if not exists length_mm text;
alter table vehicles add column if not exists width_mm text;
alter table vehicles add column if not exists height_mm text;
alter table vehicles add column if not exists wheelbase_mm text;
alter table vehicles add column if not exists weight_kg text;
alter table vehicles_staging add column if not exists variant text;
alter table vehicles_staging add column if not exists plate text;
alter table vehicles_staging add column if not exists first_registration_cz date;
alter table vehicles_staging add column if not exists power text;
alter table vehicles_staging add column if not exists color text;
alter table vehicles_staging add column if not exists length_mm text;
alter table vehicles_staging add column if not exists width_mm text;
alter table vehicles_staging add column if not exists height_mm text;
alter table vehicles_staging add column if not exists wheelbase_mm text;
alter table vehicles_staging add column if not exists weight_kg text;
alter table vehicle_fleet_facts add column if not exists length_mm text;
alter table vehicle_fleet_facts add column if not exists width_mm text;
alter table vehicle_fleet_facts add column if not exists height_mm text;
alter table vehicle_fleet_facts add column if not exists wheelbase_mm text;
alter table vehicle_fleet_facts add column if not exists weight_kg text;
alter table company_vehicle_facts add column if not exists length_mm text;
alter table company_vehicle_facts add column if not exists width_mm text;
alter table company_vehicle_facts add column if not exists height_mm text;
alter table company_vehicle_facts add column if not exists wheelbase_mm text;
alter table company_vehicle_facts add column if not exists weight_kg text;
alter table supplemental_ownership_relations add column if not exists vin text;
alter table supplemental_ownership_relations add column if not exists plate text;
alter table supplemental_ownership_relations add column if not exists observed_at timestamptz not null default now();
alter table supplemental_ownership_relations add column if not exists updated_at timestamptz not null default now();
alter table plate_resolutions add column if not exists pcv text;
alter table plate_resolutions add column if not exists confidence numeric(3, 2) not null default 0.80;
alter table plate_resolutions add column if not exists expires_at timestamptz;
alter table plate_resolutions add column if not exists updated_at timestamptz not null default now();
alter table vehicle_plate_links add column if not exists vin text;
alter table vehicle_plate_links add column if not exists pcv text;
alter table vehicle_plate_links add column if not exists vehicle_key_type text;
alter table vehicle_plate_links add column if not exists vehicle_key text;
alter table vehicle_plate_links add column if not exists confidence numeric(3, 2) not null default 0.80;
alter table vehicle_plate_links add column if not exists first_seen_at timestamptz not null default now();
alter table vehicle_plate_links add column if not exists last_seen_at timestamptz not null default now();
alter table vehicle_plate_links add column if not exists expires_at timestamptz;
alter table vehicle_plate_links add column if not exists updated_at timestamptz not null default now();

insert into vehicle_plate_links (
  plate, vin, pcv, vehicle_key_type, vehicle_key, source, confidence,
  first_seen_at, last_seen_at, expires_at, updated_at
)
select distinct on (vehicle_key_type, vehicle_key, plate)
  plate, vin, pcv, vehicle_key_type, vehicle_key, source, confidence,
  first_seen_at, last_seen_at, expires_at, updated_at
from (
  select
    plate,
    vin,
    pcv,
    'pcv' as vehicle_key_type,
    pcv as vehicle_key,
    'vehicles.plate' as source,
    1.00 as confidence,
    now() as first_seen_at,
    now() as last_seen_at,
    null::timestamptz as expires_at,
    now() as updated_at
  from vehicles
  where plate is not null and pcv is not null
  union all
  select
    plate,
    vin,
    pcv,
    'pcv' as vehicle_key_type,
    pcv as vehicle_key,
    source,
    confidence,
    coalesce(resolved_at, now()) as first_seen_at,
    coalesce(resolved_at, now()) as last_seen_at,
    expires_at,
    updated_at
	  from plate_resolutions
	  where plate is not null
	    and pcv is not null
	    and exists (select 1 from vehicles v where v.pcv = plate_resolutions.pcv)
	  union all
	  select
	    pr.plate,
	    coalesce(pr.vin, v.vin) as vin,
	    v.pcv,
	    'pcv' as vehicle_key_type,
	    v.pcv as vehicle_key,
	    pr.source,
	    pr.confidence,
	    coalesce(pr.resolved_at, now()) as first_seen_at,
	    coalesce(pr.resolved_at, now()) as last_seen_at,
	    pr.expires_at,
	    pr.updated_at
	  from plate_resolutions pr
	  join vehicles v
	    on pr.vin is not null
	    and v.vin is not null
	    and ${sqlNormalizedVin("pr.vin")} = ${sqlNormalizedVin("v.vin")}
	  where pr.plate is not null
	  union all
	  select
	    plate,
	    vin,
	    null::text as pcv,
	    'vin' as vehicle_key_type,
	    ${sqlNormalizedVin("vin")} as vehicle_key,
	    source,
	    confidence,
	    coalesce(resolved_at, now()) as first_seen_at,
	    coalesce(resolved_at, now()) as last_seen_at,
	    expires_at,
	    updated_at
	  from plate_resolutions
	  where plate is not null
	    and vin is not null
	) source_links
order by vehicle_key_type, vehicle_key, plate, confidence desc nulls last, last_seen_at desc nulls last
on conflict (vehicle_key_type, vehicle_key, plate) do update set
  vin = coalesce(excluded.vin, vehicle_plate_links.vin),
  pcv = coalesce(excluded.pcv, vehicle_plate_links.pcv),
  source = excluded.source,
  confidence = greatest(excluded.confidence, vehicle_plate_links.confidence),
  last_seen_at = greatest(excluded.last_seen_at, coalesce(vehicle_plate_links.last_seen_at, excluded.last_seen_at)),
  expires_at = excluded.expires_at,
  updated_at = now();

delete from vehicle_plate_links vpl
where vpl.pcv is not null
  and not exists (select 1 from vehicles v where v.pcv = vpl.pcv);

delete from supplemental_ownership_relations sor
where sor.pcv is not null
  and not exists (select 1 from vehicles v where v.pcv = sor.pcv);

do $$
begin
	  if to_regclass('vehicle_plate_links') is not null
	    and to_regclass('vehicles') is not null
	    and not exists (
      select 1
      from pg_constraint
      where conname = 'vehicle_plate_links_pcv_fkey'
        and conrelid = 'vehicle_plate_links'::regclass
    )
  then
	    alter table vehicle_plate_links
	      add constraint vehicle_plate_links_pcv_fkey
	      foreign key (pcv) references vehicles(pcv)
	      on update cascade on delete cascade;
		  end if;

	  if to_regclass('vehicle_plate_summaries') is not null
	    and to_regclass('vehicles') is not null
	    and not exists (
	      select 1
	      from pg_constraint
	      where conname = 'vehicle_plate_summaries_pcv_fkey'
	        and conrelid = 'vehicle_plate_summaries'::regclass
	    )
	  then
	    alter table vehicle_plate_summaries
	      add constraint vehicle_plate_summaries_pcv_fkey
	      foreign key (pcv) references vehicles(pcv)
	      on update cascade on delete cascade;
	  end if;

	  if to_regclass('supplemental_ownership_relations') is not null
    and to_regclass('vehicles') is not null
    and not exists (
      select 1
      from pg_constraint
      where conname = 'supplemental_ownership_relations_pcv_fkey'
        and conrelid = 'supplemental_ownership_relations'::regclass
    )
  then
    alter table supplemental_ownership_relations
      add constraint supplemental_ownership_relations_pcv_fkey
      foreign key (pcv) references vehicles(pcv)
      on update cascade on delete cascade;
  end if;
	end $$;
					`;

module.exports = {
  closeDatabasePool,
  ensureOpenDataSchema,
	  getActiveDatasetVersions,
	  getCachedPlateResolution,
	  getCachedPlateResolutionByVehicle,
	  getCachedAresCompany,
	  getDatabaseRuntimeStatus,
	  getOpenDataStatus,
	  getPool,
	  invalidateActiveDatasetVersionCache,
	  isDatabaseConfigured,
	  queryCompanyVehicleHistory,
	  queryInspectionsByPcv,
  queryInspectionsByVin,
	  queryOwnershipByPcv,
	  queryPcvByVin,
  queryVehicleByPcv,
	  queryVehicleByVin,
		  queryVehicleOwnershipHistory,
			  queryVehiclesByIco,
		  refreshCompanyVehicleFacts,
		  refreshVehicleFleetFacts,
		  refreshVehicleInspectionSummaries,
		  refreshVehiclePlateSummaries,
	  storePlateResolution,
	  storeSupplementalOwnershipRelations,
	  storeCachedAresCompany,
  touchDatasetVersionChecks
};

const REFRESH_VEHICLE_PLATE_SUMMARIES_SQL = `
drop table if exists vehicle_plate_summaries_next;
create table vehicle_plate_summaries_next (
  pcv text primary key,
  vin text,
  plate text not null,
  source text not null default 'lookup',
  confidence numeric(3, 2) not null default 0.80,
  resolved_at timestamptz,
  expires_at timestamptz,
  updated_at timestamptz not null default now()
);

insert into vehicle_plate_summaries_next (
  pcv, vin, plate, source, confidence, resolved_at, expires_at, updated_at
)
${vehiclePlateSummarySourceSql()};

create index vehicle_plate_summaries_next_plate_idx
  on vehicle_plate_summaries_next (plate);
create index vehicle_plate_summaries_next_vin_norm_idx
  on vehicle_plate_summaries_next ((upper(regexp_replace(coalesce(vin, ''), '[^A-Za-z0-9]', '', 'g'))))
  where vin is not null;

analyze vehicle_plate_summaries_next;

drop table if exists vehicle_plate_summaries_old;
alter table if exists vehicle_plate_summaries rename to vehicle_plate_summaries_old;
alter table vehicle_plate_summaries_next rename to vehicle_plate_summaries;
drop table if exists vehicle_plate_summaries_old;

alter index if exists vehicle_plate_summaries_next_pkey
  rename to vehicle_plate_summaries_pkey;
alter index if exists vehicle_plate_summaries_next_plate_idx
  rename to vehicle_plate_summaries_plate_idx;
alter index if exists vehicle_plate_summaries_next_vin_norm_idx
  rename to vehicle_plate_summaries_vin_norm_idx;
alter table vehicle_plate_summaries
  add constraint vehicle_plate_summaries_pcv_fkey
  foreign key (pcv) references vehicles(pcv)
  on update cascade on delete cascade;
${refreshVehicleFleetFactsSql()}
${refreshCompanyVehicleFactsSql()}
`;

const REFRESH_VEHICLE_INSPECTION_SUMMARIES_SQL = `
drop table if exists vehicle_inspection_summaries_next;
create table vehicle_inspection_summaries_next (
  pcv text primary key,
  type text,
  state text,
  station_code text,
  station_name text,
  performed_on date,
  valid_from date,
  valid_until date,
  protocol_number text,
  odometer integer,
  odometer_unit text,
  current boolean,
  dataset_filename text,
  dataset_date date,
  inspection_id bigint,
  updated_at timestamptz not null default now()
);

insert into vehicle_inspection_summaries_next (
  pcv, type, state, station_code, station_name, performed_on, valid_from, valid_until,
  protocol_number, odometer, odometer_unit, current, dataset_filename, dataset_date, inspection_id, updated_at
)
select distinct on (pcv)
  pcv,
  type,
  state,
  station_code,
  station_name,
  valid_from as performed_on,
  valid_from,
  valid_until,
  protocol_number,
  odometer,
  odometer_unit,
  current,
  dataset_filename,
  dataset_date,
  id as inspection_id,
  now() as updated_at
from inspections
where pcv is not null
  and (valid_from is not null or valid_until is not null or current is true)
order by
  pcv,
  case
    when current is true and valid_until is not null then 0
    when current is true then 1
    else 2
  end,
  valid_until desc nulls last,
  valid_from desc nulls last,
  id desc;

create index vehicle_inspection_summaries_next_valid_until_idx
  on vehicle_inspection_summaries_next (valid_until desc) where valid_until is not null;
create index vehicle_inspection_summaries_next_current_idx
  on vehicle_inspection_summaries_next (pcv) where current is true;

analyze vehicle_inspection_summaries_next;

drop table if exists vehicle_inspection_summaries_old;
alter table if exists vehicle_inspection_summaries rename to vehicle_inspection_summaries_old;
alter table vehicle_inspection_summaries_next rename to vehicle_inspection_summaries;
drop table if exists vehicle_inspection_summaries_old;

alter index if exists vehicle_inspection_summaries_next_pkey
  rename to vehicle_inspection_summaries_pkey;
alter index if exists vehicle_inspection_summaries_next_valid_until_idx
  rename to vehicle_inspection_summaries_valid_until_idx;
alter index if exists vehicle_inspection_summaries_next_current_idx
  rename to vehicle_inspection_summaries_current_idx;
${refreshVehicleFleetFactsSql()}
${refreshCompanyVehicleFactsSql()}
`;
