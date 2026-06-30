#!/usr/bin/env node

const fs = require("fs");
const crypto = require("crypto");
const https = require("https");
const path = require("path");
const readline = require("readline");
const zlib = require("zlib");
const { once } = require("events");
const { finished } = require("stream/promises");
const { Transform } = require("stream");
const { from: copyFrom } = require("pg-copy-streams");

const ROOT_DIR = path.join(__dirname, "..");
loadEnvFile(path.join(ROOT_DIR, ".env"));

const {
	  closeDatabasePool,
	  ensureOpenDataSchema,
		  getActiveDatasetVersions,
		  getPool,
		  invalidateActiveDatasetVersionCache,
		  refreshCompanyVehicleFacts,
		  refreshVehicleInspectionSummaries,
		  refreshVehiclePlateSummaries,
		  touchDatasetVersionChecks
} = require("../open-data-db");

const OPEN_DATA_DIR = path.resolve(process.env.OPEN_DATA_PERSIST_DIR || path.join(ROOT_DIR, ".cache", "open-data"));
const RAILWAY_DEFAULT_SOURCES = "ownership,vehicles";
const CORE_SOURCE_NAMES = [
  "ownership",
  "vehicles",
  "inspections",
  "deregistered",
  "imports"
];
const AUX_SOURCE_NAMES = [
  "equipment",
  "manufacturer_reports"
];
const ALL_SOURCE_NAMES = [...CORE_SOURCE_NAMES, ...AUX_SOURCE_NAMES];
const IMPORT_LOCK_KEY = [720191, 20260519];
const OWNERSHIP_HISTORY_SCOPE_ACTIVE_COMPANY_PCVS = "active company pcvs";
const OWNERSHIP_HISTORY_SCOPE_LEGAL_HISTORY = "legal history";
const VEHICLE_VINS_ONLY_SWITCH_SQL = `
  drop table if exists vehicle_vins_next;
  create table vehicle_vins_next (
    vin text primary key,
    pcv text not null,
    dataset_filename text,
    dataset_date date,
    imported_at timestamptz not null default now()
  );

  insert into vehicle_vins_next (
    vin, pcv, dataset_filename, dataset_date, imported_at
  )
  select distinct on (vin)
    vin, pcv, dataset_filename, dataset_date, now()
  from vehicle_vins_staging
  where vin is not null and pcv is not null
  order by vin, pcv;

  create index vehicle_vins_next_pcv_idx
    on vehicle_vins_next (pcv);
  analyze vehicle_vins_next;

  drop table if exists vehicle_vins_old;
  alter table vehicle_vins rename to vehicle_vins_old;
  alter table vehicle_vins_next rename to vehicle_vins;
  drop table vehicle_vins_old;
  alter index vehicle_vins_next_pkey rename to vehicle_vins_pkey;
  alter index vehicle_vins_next_pcv_idx rename to vehicle_vins_pcv_idx;

  truncate table vehicle_vins_staging;
  truncate table vehicles_staging;
`;
const OWNERSHIP_DESTRUCTIVE_REPLACE_SWITCH_SQL = `
  create index ownership_relations_ico_current_relation_idx
    on ownership_relations (ico, pcv)
    where ico is not null and current is true and date_to is null and relation in ('Vlastnik', 'Provozovatel');
  create index ownership_relations_ico_history_idx
    on ownership_relations (ico, date_from desc, pcv)
    where ico is not null and relation in ('Vlastnik', 'Provozovatel');
  create index ownership_relations_pcv_history_idx
    on ownership_relations (pcv, date_from desc)
    where relation in ('Vlastnik', 'Provozovatel');
  create index ownership_relations_missing_ico_name_current_relation_idx
    on ownership_relations (lower(name), pcv)
    where ico is null and name is not null and current is true and date_to is null and relation in ('Vlastnik', 'Provozovatel');
  create index ownership_relations_missing_ico_name_history_idx
    on ownership_relations (lower(name), date_from desc, pcv)
    where ico is null and name is not null and relation in ('Vlastnik', 'Provozovatel');
  analyze ownership_relations;
  truncate table ownership_relations_staging;
`;
const OWNERSHIP_DESTRUCTIVE_REPLACE_LEAN_SWITCH_SQL = `
  create index ownership_relations_pcv_history_idx
    on ownership_relations (pcv, date_from desc)
    where relation in ('Vlastnik', 'Provozovatel');
  analyze ownership_relations;
  truncate table ownership_relations_staging;
`;

const SOURCE_CONFIGS = {
  vehicles: {
    route: "/vypiszregistru/vypisvozidel",
    stagingTable: "vehicles_staging",
    mainTable: "vehicles",
    columns: [
      "pcv",
      "plate",
      "vin",
      "make",
      "model",
	      "type",
	      "variant",
	      "category",
	      "fuel",
	      "first_registration",
	      "first_registration_cz",
	      "power",
	      "color",
	      "length_mm",
	      "width_mm",
	      "height_mm",
	      "wheelbase_mm",
	      "weight_kg",
	      "status",
      "dataset_filename",
      "dataset_date"
    ],
    localPattern: /^RSV_vypis_vozidel_\d{8}\.csv(?:\.gz)?(?:\.tmp)?$/i,
    normalize: normalizeVehicleRecord,
    switchSql: `
      drop table if exists vehicle_vins_next;
      drop table if exists vehicles_next;
	      create table vehicles_next (
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

	      insert into vehicles_next (
	        pcv, plate, vin, make, model, type, variant, category, fuel, first_registration,
	        first_registration_cz, power, color, length_mm, width_mm, height_mm, wheelbase_mm,
	        weight_kg, status, dataset_filename, dataset_date, imported_at
	      )
	      select distinct on (pcv)
	        pcv, plate, vin, make, model, type, variant, category, fuel, first_registration,
	        first_registration_cz, power, color, length_mm, width_mm, height_mm, wheelbase_mm,
	        weight_kg, status, dataset_filename, dataset_date, now()
	      from vehicles_staging
      where pcv is not null
      order by pcv, vin nulls last;

      create index vehicles_next_vin_idx
        on vehicles_next (vin) where vin is not null;

      create table vehicle_vins_next (
        vin text primary key,
        pcv text not null,
        dataset_filename text,
        dataset_date date,
        imported_at timestamptz not null default now()
      );

      insert into vehicle_vins_next (
        vin, pcv, dataset_filename, dataset_date, imported_at
      )
      select distinct on (vin)
        vin, pcv, dataset_filename, dataset_date, now()
      from vehicles_next
      where vin is not null and pcv is not null
      order by vin, pcv;

      create index vehicle_vins_next_pcv_idx
        on vehicle_vins_next (pcv);

	      analyze vehicles_next;
	      analyze vehicle_vins_next;

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
			      end $$;
			      alter table if exists vehicle_plate_links drop constraint if exists vehicle_plate_links_pcv_fkey;
			      alter table if exists vehicle_plate_summaries drop constraint if exists vehicle_plate_summaries_pcv_fkey;
			      alter table if exists supplemental_ownership_relations drop constraint if exists supplemental_ownership_relations_pcv_fkey;
	      drop table if exists vehicles_old;
	      drop table if exists vehicle_vins_old;
	      alter table vehicles rename to vehicles_old;
	      alter table vehicle_vins rename to vehicle_vins_old;
	      alter table vehicles_next rename to vehicles;
      alter table vehicle_vins_next rename to vehicle_vins;
      drop table vehicles_old;
      drop table vehicle_vins_old;
      alter index vehicles_next_pkey rename to vehicles_pkey;
	      alter index vehicles_next_vin_idx rename to vehicles_vin_idx;
	      alter index vehicle_vins_next_pkey rename to vehicle_vins_pkey;
	      alter index vehicle_vins_next_pcv_idx rename to vehicle_vins_pcv_idx;

	      delete from vehicle_plate_links vpl
	      where vpl.pcv is not null
	        and not exists (select 1 from vehicles v where v.pcv = vpl.pcv);

	      do $$
	      begin
	        if to_regclass('vehicle_plate_links') is not null
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
	      end $$;

	      truncate table vehicle_vins_staging;
	      truncate table vehicles_staging;
		    `
  },
  ownership: {
    route: "/vypiszregistru/vlastnikprovozovatelvozidla",
    stagingTable: "ownership_relations_staging",
    mainTable: "ownership_relations",
    columns: [
      "pcv",
      "ico",
      "name",
      "address",
      "relation",
      "subject_type",
      "current",
      "date_from",
      "date_to",
      "dataset_filename",
      "dataset_date"
    ],
    localPattern: /^RSV_vlastnik_provozovatel_vozidla_\d{8}\.csv(?:\.gz)?(?:\.tmp)?$/i,
	    normalize: normalizeOwnershipRecord,
	    switchSql: `
	      drop table if exists ownership_relations_next;
	      create table ownership_relations_next (
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

	      insert into ownership_relations_next (
	        pcv, ico, name, address, relation, subject_type, current,
	        date_from, date_to, dataset_filename, dataset_date, imported_at
	      )
      select
        pcv, ico, name, address, relation, subject_type, current,
        date_from, date_to, dataset_filename, dataset_date, now()
	      from ownership_relations_staging
	      where pcv is not null;

	      insert into ownership_parties (
	        party_key, ico, name, name_key, address, subject_type, updated_at
	      )
	      select distinct
	        md5(
	          coalesce(ico, '') || '|' ||
	          coalesce(lower(name), '') || '|' ||
	          coalesce(address, '') || '|' ||
	          coalesce(subject_type, '')
	        ) as party_key,
	        ico,
	        name,
	        lower(name) as name_key,
	        address,
	        subject_type,
	        now()
	      from ownership_relations_next
	      where ico is not null or name is not null or address is not null
	      on conflict (party_key) do update set
	        ico = excluded.ico,
	        name = excluded.name,
	        name_key = excluded.name_key,
	        address = excluded.address,
	        subject_type = excluded.subject_type,
	        updated_at = now();

	      update ownership_relations_next r
	      set party_id = p.id
	      from ownership_parties p
	      where p.party_key = md5(
	        coalesce(r.ico, '') || '|' ||
	        coalesce(lower(r.name), '') || '|' ||
	        coalesce(r.address, '') || '|' ||
	        coalesce(r.subject_type, '')
	      );

	      create index ownership_relations_next_ico_idx
	        on ownership_relations_next (ico);
	      create index ownership_relations_next_party_id_idx
	        on ownership_relations_next (party_id) where party_id is not null;
	      create index ownership_relations_next_pcv_idx
	        on ownership_relations_next (pcv);
	      create index ownership_relations_next_current_idx
	        on ownership_relations_next (ico, pcv) where current is true and date_to is null;
	      create index ownership_relations_next_ico_current_relation_idx
	        on ownership_relations_next (ico, pcv) where current is true and date_to is null and relation in ('Vlastnik', 'Provozovatel');
	      create index ownership_relations_next_ico_history_idx
	        on ownership_relations_next (ico, date_from desc, pcv) where relation in ('Vlastnik', 'Provozovatel');
	      create index ownership_relations_next_pcv_history_idx
	        on ownership_relations_next (pcv, date_from desc) where relation in ('Vlastnik', 'Provozovatel');
	      create index ownership_relations_next_missing_ico_name_current_relation_idx
	        on ownership_relations_next (lower(name), pcv) where ico is null and name is not null and current is true and date_to is null and relation in ('Vlastnik', 'Provozovatel');
	      create index ownership_relations_next_missing_ico_name_history_idx
	        on ownership_relations_next (lower(name), date_from desc, pcv) where ico is null and name is not null and relation in ('Vlastnik', 'Provozovatel');

	      analyze ownership_relations_next;

	      drop table if exists ownership_relations_old;
	      alter table ownership_relations rename to ownership_relations_old;
	      alter table ownership_relations_next rename to ownership_relations;
	      drop table ownership_relations_old;
	      alter index ownership_relations_next_pkey rename to ownership_relations_pkey;
	      alter index ownership_relations_next_ico_idx rename to ownership_relations_ico_idx;
	      alter index ownership_relations_next_party_id_idx rename to ownership_relations_party_id_idx;
	      alter index ownership_relations_next_pcv_idx rename to ownership_relations_pcv_idx;
	      alter index ownership_relations_next_current_idx rename to ownership_relations_current_idx;
	      alter index ownership_relations_next_ico_current_relation_idx rename to ownership_relations_ico_current_relation_idx;
	      alter index ownership_relations_next_ico_history_idx rename to ownership_relations_ico_history_idx;
	      alter index ownership_relations_next_pcv_history_idx rename to ownership_relations_pcv_history_idx;
	      alter index ownership_relations_next_missing_ico_name_current_relation_idx rename to ownership_relations_missing_ico_name_current_relation_idx;
	      alter index ownership_relations_next_missing_ico_name_history_idx rename to ownership_relations_missing_ico_name_history_idx;

	      truncate table ownership_relations_staging;
	    `
  },
  inspections: {
    route: "/vypiszregistru/technickeprohlidky",
    stagingTable: "inspections_staging",
    mainTable: "inspections",
    columns: [
      "pcv",
      "type",
      "state",
      "station_code",
      "station_name",
      "valid_from",
      "valid_until",
      "protocol_number",
      "odometer",
      "odometer_unit",
      "current",
      "dataset_filename",
      "dataset_date"
    ],
    localPattern: /^RSV_technicke_prohlidky_\d{8}\.csv(?:\.gz)?(?:\.tmp)?$/i,
    normalize: normalizeInspectionRecord,
    switchSql: `
      drop table if exists inspections_next;
      create table inspections_next (
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

      insert into inspections_next (
        pcv, type, state, station_code, station_name, valid_from,
        valid_until, protocol_number, odometer, odometer_unit, current, dataset_filename, dataset_date,
        imported_at
      )
      select
        pcv, type, state, station_code, station_name, valid_from,
        valid_until, protocol_number, odometer, odometer_unit, current, dataset_filename, dataset_date,
        now()
      from inspections_staging
      where pcv is not null;

      create index inspections_next_pcv_idx
        on inspections_next (pcv);
      create index inspections_next_current_idx
        on inspections_next (pcv) where current is true;
      create index inspections_next_pcv_valid_until_idx
        on inspections_next (pcv, valid_until desc);

      analyze inspections_next;

      drop table if exists inspections_old;
      alter table inspections rename to inspections_old;
      alter table inspections_next rename to inspections;
      drop table inspections_old;
      alter index inspections_next_pkey rename to inspections_pkey;
      alter index inspections_next_pcv_idx rename to inspections_pcv_idx;
      alter index inspections_next_current_idx rename to inspections_current_idx;
      alter index inspections_next_pcv_valid_until_idx rename to inspections_pcv_valid_until_idx;

      truncate table inspections_staging;
	    `
  },
  deregistered: {
    route: "/vypiszregistru/vozidlavyrazenazprovozu",
    stagingTable: "vehicle_deregistrations_staging",
    mainTable: "vehicle_deregistrations",
    columns: [
      "pcv",
      "date_from",
      "date_to",
      "reason",
      "rm_code",
      "rm_name",
      "dataset_filename",
      "dataset_date"
    ],
    localPattern: /^RSV_vozidla_vyrazena_z_provozu_\d{8}\.csv(?:\.gz)?(?:\.tmp)?$/i,
    normalize: normalizeDeregistrationRecord,
    switchSql: `
      truncate table vehicle_deregistrations restart identity;
      insert into vehicle_deregistrations (
        pcv, date_from, date_to, reason, rm_code, rm_name,
        dataset_filename, dataset_date, imported_at
      )
      select
        pcv, date_from, date_to, reason, rm_code, rm_name,
        dataset_filename, dataset_date, now()
      from vehicle_deregistrations_staging
      where pcv is not null;

      truncate table vehicle_deregistrations_staging;
    `
  },
  imports: {
    route: "/vypiszregistru/vozidladovoz",
    stagingTable: "vehicle_imports_staging",
    mainTable: "vehicle_imports",
    columns: [
      "pcv",
      "country",
      "imported_on",
      "dataset_filename",
      "dataset_date"
    ],
    localPattern: /^RSV_vozidla_dovoz_\d{8}\.csv(?:\.gz)?(?:\.tmp)?$/i,
    normalize: normalizeImportedVehicleRecord,
    switchSql: `
      truncate table vehicle_imports restart identity;
      insert into vehicle_imports (
        pcv, country, imported_on, dataset_filename, dataset_date, imported_at
      )
      select
        pcv, country, imported_on, dataset_filename, dataset_date, now()
      from vehicle_imports_staging
      where pcv is not null;

      truncate table vehicle_imports_staging;
    `
  },
  equipment: {
    route: "/vypiszregistru/vozidladoplnkovevybaveni",
    stagingTable: "open_data_aux_records_staging",
    mainTable: "open_data_aux_records",
    columns: [
      "source",
      "pcv",
      "vin",
      "record_key",
      "payload",
      "dataset_filename",
      "dataset_date"
    ],
    localPattern: /^RSV_.*dopln.*vybav.*\d{8}\.csv(?:\.gz)?(?:\.tmp)?$/i,
    normalize: (values, headerMap, metadata) => normalizeAuxiliaryRecord("equipment", values, headerMap, metadata),
    switchSql: `
      delete from open_data_aux_records where source = 'equipment';
      insert into open_data_aux_records (
        source, pcv, vin, record_key, payload, dataset_filename, dataset_date, imported_at
      )
      select
        source, pcv, vin, record_key, payload::jsonb, dataset_filename, dataset_date, now()
      from open_data_aux_records_staging
      where source = 'equipment';

      truncate table open_data_aux_records_staging;
    `
  },
  manufacturer_reports: {
    route: "/vypiszregistru/zpravyvyrobcezastupce",
    stagingTable: "open_data_aux_records_staging",
    mainTable: "open_data_aux_records",
    columns: [
      "source",
      "pcv",
      "vin",
      "record_key",
      "payload",
      "dataset_filename",
      "dataset_date"
    ],
    localPattern: /^RSV_.*zprav.*vyrobc.*\d{8}\.csv(?:\.gz)?(?:\.tmp)?$/i,
    normalize: (values, headerMap, metadata) => normalizeAuxiliaryRecord("manufacturer_reports", values, headerMap, metadata),
    switchSql: `
      delete from open_data_aux_records where source = 'manufacturer_reports';
      insert into open_data_aux_records (
        source, pcv, vin, record_key, payload, dataset_filename, dataset_date, imported_at
      )
      select
        source, pcv, vin, record_key, payload::jsonb, dataset_filename, dataset_date, now()
      from open_data_aux_records_staging
      where source = 'manufacturer_reports';

      truncate table open_data_aux_records_staging;
    `
  }
};

if (process.argv.includes("--self-check")) {
  selfCheck();
  process.exit(0);
}

main().catch(async (error) => {
  console.error("[open-data-import] failed");
  console.error(error && error.stack ? error.stack : String(error));
  await closeDatabasePool().catch(() => {});
  process.exitCode = 1;
});

async function main() {
  const sources = parseSources();
  const force = process.argv.includes("--force") || String(process.env.OPEN_DATA_IMPORT_FORCE || "false") === "true";
  const pool = getPool();

  if (!pool) {
    throw new Error("DATABASE_URL neni nastaveny.");
  }

  await ensureOpenDataSchema();
  const localOnly = String(process.env.OPEN_DATA_IMPORT_LOCAL_ONLY || "false").toLowerCase() === "true";
  const token = localOnly ? null : await getRenToken();
  const resolvedSources = [];

  for (const sourceName of sources) {
    const config = SOURCE_CONFIGS[sourceName];
    console.log(`[open-data-import] resolving ${sourceName}`);
    const localDataset = localOnly ? await findLatestLocalDataset(config) : null;
    const metadata = localDataset
      ? {
          filename: localDataset.name,
          datasetDate: parseDatasetDateFromFilename(localDataset.name)
        }
      : await fetchDatasetMetadata(config.route, token);
    const localPath = localDataset?.localPath || await findLocalDataset(config, metadata.filename);
    resolvedSources.push({
      name: sourceName,
      config,
      metadata,
      token,
      localPath
    });
  }

  const activeVersions = (await getActiveDatasetVersions(sources)) || Object.create(null);
  const upToDate = resolvedSources.every(
    (source) => activeVersions[source.name]?.filename === source.metadata.filename
  );

  if (upToDate && !force) {
    await touchDatasetVersionChecks(sources);
    console.log(`[open-data-import] up to date (${sources.join(", ")})`);
    await closeDatabasePool();
    return;
  }

  let companyPcvs = !sources.includes("vehicles") || sources.includes("ownership") || shouldImportAllVehicleRows()
    ? null
    : await collectCurrentTableCompanyOwnershipPcvs(pool);
  const client = await pool.connect();
  let lockAcquired = false;
  try {
    await configureImportSession(client);
	    await acquireImportLock(client);
	    lockAcquired = true;
	    let lockedActiveVersions = activeVersions;

	    if (!force) {
	      lockedActiveVersions = (await getActiveDatasetVersions(sources)) || Object.create(null);
	      const lockedUpToDate = resolvedSources.every(
	        (source) => lockedActiveVersions[source.name]?.filename === source.metadata.filename
	      );

      if (lockedUpToDate) {
        await touchDatasetVersionChecks(sources);
        console.log(`[open-data-import] up to date after lock (${sources.join(", ")})`);
        return;
      }
	    }

	    for (const source of resolvedSources) {
	      if (!force && lockedActiveVersions[source.name]?.filename === source.metadata.filename) {
	        await touchDatasetVersionChecks([source.name]);
	        if (source.name === "ownership" && sources.includes("vehicles") && !shouldImportAllVehicleRows()) {
	          companyPcvs = await collectCurrentTableCompanyOwnershipPcvs(client);
	        }
	        console.log(`[open-data-import] ${source.name} already active (${source.metadata.filename})`);
	        continue;
	      }
	      await markImporting(client, source);
	      const result =
        source.name === "vehicles"
          ? await importVehiclesToStaging(client, source, companyPcvs)
          : await importSourceToStaging(client, source, {
              collectCompanyPcvs: sources.includes("vehicles") && !shouldImportAllVehicleRows()
            });
      if (result.count <= 0) {
        throw new Error(`Import source ${source.name} produced no rows.`);
      }
	      if (source.name === "ownership") {
	        companyPcvs = shouldImportAllVehicleRows() ? null : result.companyPcvs;
	      }
      await switchActiveSource(client, {
        ...source,
        count: result.count,
        replaceMain: result.replaceMain,
        vinIndexOnly: result.vinIndexOnly
      });
    }
  } catch (error) {
    await markFailed(client, resolvedSources, error).catch(() => {});
    throw error;
  } finally {
    if (lockAcquired) {
      await releaseImportLock(client).catch(() => {});
    }
    client.release();
    await closeDatabasePool();
  }
}

async function configureImportSession(client) {
  await client.query("set statement_timeout = 0");
  await client.query("set lock_timeout = 0");
  await client.query("set idle_in_transaction_session_timeout = 0");
  const maintenanceWorkMem = process.env.OPEN_DATA_IMPORT_MAINTENANCE_WORK_MEM || "512MB";
  await client.query(`set maintenance_work_mem = '${String(maintenanceWorkMem).replace(/'/g, "''")}'`);
}

async function acquireImportLock(client) {
  await client.query("select pg_advisory_lock($1, $2)", IMPORT_LOCK_KEY);
}

async function releaseImportLock(client) {
  await client.query("select pg_advisory_unlock($1, $2)", IMPORT_LOCK_KEY);
}

function parseSources() {
  const raw =
    process.argv.find((arg) => arg.startsWith("--sources="))?.slice("--sources=".length) ||
    process.env.OPEN_DATA_IMPORT_SOURCES ||
    getDefaultSources();
	  const normalizedRaw = normalizeForMatch(raw);
	  const sources = (normalizedRaw === "all" ? getAllImportSourceNames().join(",") : raw)
	    .split(",")
	    .map((source) => source.trim())
	    .filter(Boolean);

  if (sources.length === 0) {
    throw new Error("Nebyl vybran zadny zdroj pro import.");
  }

  sources.forEach((source) => {
	    if (!SOURCE_CONFIGS[source]) {
	      throw new Error(`Neznamy open-data source: ${source}`);
	    }
	    if (AUX_SOURCE_NAMES.includes(source) && !isAuxImportEnabled()) {
	      throw new Error(`Auxiliary JSONB source ${source} requires OPEN_DATA_IMPORT_AUX_ENABLED=true.`);
	    }
	  });

  const order = new Map(ALL_SOURCE_NAMES.map((source, index) => [source, index]));
  return sources.sort((left, right) => (order.get(left) ?? 999) - (order.get(right) ?? 999));
}

function getDefaultSources() {
	  return isFullLocalImportProfile() ? getAllImportSourceNames().join(",") : RAILWAY_DEFAULT_SOURCES;
	}

function getAllImportSourceNames() {
  return isAuxImportEnabled() ? ALL_SOURCE_NAMES : CORE_SOURCE_NAMES;
}

function isAuxImportEnabled() {
  return String(process.env.OPEN_DATA_IMPORT_AUX_ENABLED || "false").toLowerCase() === "true";
}

function getImportProfile() {
  return parseArgValue("--profile") || process.env.OPEN_DATA_IMPORT_PROFILE || "";
}

function isFullLocalImportProfile() {
  const normalized = normalizeForMatch(getImportProfile());
  return normalized === "full local" || normalized === "local full" || normalized === "all" || normalized === "full";
}

function parseArgValue(name) {
  const prefix = `${name}=`;
  const withEquals = process.argv.find((arg) => arg.startsWith(prefix));
  if (withEquals) {
    return withEquals.slice(prefix.length);
  }

  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : null;
}

async function markImporting(client, source) {
  await client.query(
    `
      insert into dataset_versions (
        source, filename, dataset_date, status, active, record_count,
        last_checked_at, import_started_at, import_finished_at, error, updated_at
      )
      values ($1, $2, $3, 'importing', false, 0, now(), now(), null, null, now())
      on conflict (source, filename) do update set
        status = case
          when dataset_versions.active is true and dataset_versions.status = 'ready' then dataset_versions.status
          else 'importing'
        end,
        active = dataset_versions.active,
        record_count = case
          when dataset_versions.active is true and dataset_versions.status = 'ready' then dataset_versions.record_count
          else 0
        end,
        last_checked_at = now(),
        import_started_at = now(),
        import_finished_at = case
          when dataset_versions.active is true and dataset_versions.status = 'ready' then dataset_versions.import_finished_at
          else null
        end,
        error = case
          when dataset_versions.active is true and dataset_versions.status = 'ready' then dataset_versions.error
          else null
        end,
        updated_at = now()
    `,
    [source.name, source.metadata.filename, source.metadata.datasetDate]
  );
}

async function markFailed(client, sources, error) {
  await client.query(
    `
      update dataset_versions
      set status = 'failed', error = $2, updated_at = now(), import_finished_at = now()
      where source = any($1::text[]) and status = 'importing'
    `,
    [sources.map((source) => source.name), error?.message || String(error)]
  );
}

async function importSourceToStaging(client, source, options = {}) {
  const { config } = source;
  const collectCompanyPcvs = Boolean(options.collectCompanyPcvs);
  const filterOwnershipToActiveCompanyPcvs =
    source.name === "ownership" && getOwnershipHistoryScope() === OWNERSHIP_HISTORY_SCOPE_ACTIVE_COMPANY_PCVS;
  const replaceOwnershipMain = shouldDestructivelyReplaceOwnershipMain(source);
  if (source.name === "ownership") {
    console.log(`[open-data-import] ownership options ${JSON.stringify(getOwnershipImportOptions())}`);
    if (replaceOwnershipMain) {
      console.log("[open-data-import] ownership destructive replace enabled");
    }
  }
  const ownershipPcvFilter =
    filterOwnershipToActiveCompanyPcvs
      ? await collectActiveCompanyOwnershipPcvs(client, source)
      : null;
  const copyTable = replaceOwnershipMain ? config.mainTable : config.stagingTable;
  if (replaceOwnershipMain) {
    await prepareOwnershipDestructiveReplace(client);
  } else {
    await client.query(`truncate table ${config.stagingTable}`);
  }

  const copySql = `copy ${copyTable} (${config.columns.join(", ")}) from stdin with (format text, delimiter E'\\t')`;
  const copyStream = client.query(copyFrom(copySql));
  const copyDone = finished(copyStream);
  const companyPcvs = source.name === "ownership" && collectCompanyPcvs ? ownershipPcvFilter || new Set() : null;
  let rowCount = 0;
  let processed = 0;

  try {
    await scanCsvSource(source, async ({ values, headerMap }) => {
      processed += 1;
      const shouldLogProgress = processed % 100000 === 0;
      const row = config.normalize(values, headerMap, source.metadata);
      if (!row) {
        if (shouldLogProgress) {
          console.log(`[open-data-import] ${source.name} processed=${processed} imported=${rowCount}`);
        }
        return;
      }
      if (source.name === "ownership") {
        if (!isFleetOwnershipRelation(row.relation)) {
          if (shouldLogProgress) {
            console.log(`[open-data-import] ${source.name} processed=${processed} imported=${rowCount}`);
          }
          return;
        }
        if (ownershipPcvFilter && !ownershipPcvFilter.has(row.pcv)) {
          if (shouldLogProgress) {
            console.log(`[open-data-import] ${source.name} processed=${processed} imported=${rowCount}`);
          }
          return;
        }
      }

      if (companyPcvs && !ownershipPcvFilter && row.pcv) {
        companyPcvs.add(row.pcv);
      }

      const line = config.columns.map((column) => encodeCopyValue(row[column])).join("\t") + "\n";
      if (!copyStream.write(line)) {
        await once(copyStream, "drain");
      }
      rowCount += 1;

      if (shouldLogProgress) {
        console.log(`[open-data-import] ${source.name} processed=${processed} imported=${rowCount}`);
      }
    });
    copyStream.end();
    await copyDone;
  } catch (error) {
    console.error(
      `[open-data-import] ${source.name} copy failed processed=${processed} imported=${rowCount} code=${error?.code || ""}`
    );
    copyStream.destroy(error);
    throw error;
  }

  console.log(
    `[open-data-import] ${source.name} done processed=${processed} imported=${rowCount} file=${source.metadata.filename}`
  );
  return { count: rowCount, companyPcvs, replaceMain: replaceOwnershipMain };
}

async function prepareOwnershipDestructiveReplace(client) {
  await client.query("drop table if exists ownership_relations_next");
  await client.query("drop table if exists ownership_relations_old");
  await client.query("truncate table ownership_relations_staging");
  await client.query("truncate table ownership_parties restart identity");
  await client.query("truncate table company_vehicle_facts");
  await client.query("drop table if exists ownership_relations");
  await client.query(`
    create table ownership_relations (
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
    )
  `);
}

async function collectActiveCompanyOwnershipPcvs(client, source) {
  const currentTablePcvs = await collectCurrentTableCompanyOwnershipPcvs(client, source);
  if (currentTablePcvs?.size > 0) {
    return currentTablePcvs;
  }
  if (currentTablePcvs) {
    console.log("[open-data-import] ownership active table pcvs empty; falling back to csv scan");
  }

  const pcvs = new Set();
  let processed = 0;

  await scanCsvSource(source, async ({ values, headerMap }) => {
    processed += 1;
    const row = normalizeOwnershipRecord(values, headerMap, source.metadata, {
      requireCurrent: true,
      requireIdentified: true,
      requireIco: false
    });
    if (!row || !isFleetOwnershipRelation(row.relation) || row.current !== true || row.date_to) {
      if (processed % 1000000 === 0) {
        console.log(`[open-data-import] ownership active-pcv scan processed=${processed} pcvs=${pcvs.size}`);
      }
      return;
    }

    pcvs.add(row.pcv);
    if (processed % 1000000 === 0) {
      console.log(`[open-data-import] ownership active-pcv scan processed=${processed} pcvs=${pcvs.size}`);
    }
  });

  console.log(`[open-data-import] ownership active-company pcvs=${pcvs.size}`);
  return pcvs;
}

async function collectCurrentTableCompanyOwnershipPcvs(db) {
  if (!db) {
    return null;
  }

  const activeVersion = await db.query(
    "select filename from dataset_versions where source = 'ownership' and active is true and status = 'ready' limit 1"
  );
  const activeFilename = activeVersion.rows[0]?.filename || null;
  if (!activeFilename) {
    return null;
  }

  const result = await db.query(`
    select distinct pcv
    from ownership_relations
    where pcv is not null
      and (
        ico is not null
        or (
          subject_type is not null
          and (
            lower(subject_type) = '2'
            or lower(subject_type) like '%vnick%'
            or lower(subject_type) like '%company%'
            or lower(subject_type) like '%firma%'
          )
          and (nullif(btrim(name), '') is not null or nullif(btrim(address), '') is not null)
        )
      )
      and relation in ('Vlastnik', 'Provozovatel')
  `);
  const pcvs = new Set(result.rows.map((row) => String(row.pcv)).filter(Boolean));
  console.log(`[open-data-import] ownership legal-history pcvs=${pcvs.size} source=active-table`);
  return pcvs.size > 0 ? pcvs : null;
}

async function importVehiclesToStaging(client, source, companyPcvs) {
  const { config } = source;
  if (shouldImportVehicleVinIndexOnly()) {
    return await importVehicleVinsToStaging(client, source, companyPcvs);
  }

  const pcvFilter = companyPcvs || new Set();
  const hasPcvFilter = pcvFilter.size > 0;
  await client.query("truncate table vehicles_staging");
  await client.query("truncate table vehicle_vins_staging");

  const summaryCopy = client.query(
    copyFrom(`copy vehicles_staging (${config.columns.join(", ")}) from stdin with (format text, delimiter E'\\t')`)
  );
  const summaryDone = finished(summaryCopy);
  let processed = 0;
  let summaryCount = 0;

  try {
    await scanCsvSource(source, async ({ values, headerMap }) => {
      processed += 1;
      const row = config.normalize(values, headerMap, source.metadata);
      if (!row) {
        return;
      }

      const includeVehicle = !hasPcvFilter || pcvFilter.has(row.pcv);

      if (includeVehicle) {
        const summaryLine = config.columns.map((column) => encodeCopyValue(row[column])).join("\t") + "\n";
        if (!summaryCopy.write(summaryLine)) {
          await once(summaryCopy, "drain");
        }
        summaryCount += 1;
      }

      if (processed % 100000 === 0) {
        console.log(
          `[open-data-import] vehicles processed=${processed} summaries=${summaryCount}`
        );
      }
    });
    summaryCopy.end();
    await summaryDone;
  } catch (error) {
    console.error(
      `[open-data-import] vehicles copy failed processed=${processed} summaries=${summaryCount} code=${error?.code || ""}`
    );
    summaryCopy.destroy(error);
    throw error;
  }

  console.log(
    `[open-data-import] vehicles done processed=${processed} summaries=${summaryCount} file=${source.metadata.filename}`
  );
  return { count: summaryCount, companyPcvs: null };
}

async function importVehicleVinsToStaging(client, source, companyPcvs) {
  const pcvFilter = companyPcvs || new Set();
  const hasPcvFilter = pcvFilter.size > 0;
  await client.query("truncate table vehicle_vins_staging");

  const copy = client.query(
    copyFrom("copy vehicle_vins_staging (vin, pcv, dataset_filename, dataset_date) from stdin with (format text, delimiter E'\\t')")
  );
  const done = finished(copy);
  let processed = 0;
  let vinCount = 0;

  try {
    await scanCsvSource(source, async ({ values, headerMap }) => {
      processed += 1;
      const row = normalizeVehicleVinRecord(values, headerMap, source.metadata);
      if (!row || (hasPcvFilter && !pcvFilter.has(row.pcv))) {
        if (processed % 1000000 === 0) {
          console.log(`[open-data-import] vehicle-vins processed=${processed} vins=${vinCount}`);
        }
        return;
      }

      const line = ["vin", "pcv", "dataset_filename", "dataset_date"]
        .map((column) => encodeCopyValue(row[column]))
        .join("\t") + "\n";
      if (!copy.write(line)) {
        await once(copy, "drain");
      }
      vinCount += 1;

      if (processed % 1000000 === 0) {
        console.log(`[open-data-import] vehicle-vins processed=${processed} vins=${vinCount}`);
      }
    });
    copy.end();
    await done;
  } catch (error) {
    console.error(
      `[open-data-import] vehicle-vins copy failed processed=${processed} vins=${vinCount} code=${error?.code || ""}`
    );
    copy.destroy(error);
    throw error;
  }

  console.log(
    `[open-data-import] vehicle-vins done processed=${processed} vins=${vinCount} file=${source.metadata.filename}`
  );
  return { count: vinCount, companyPcvs: null, vinIndexOnly: true };
}

async function switchActiveSource(client, source) {
  await client.query("begin");
  try {
    await client.query(resolveSwitchSql(source));
    await client.query(
      "update dataset_versions set active = false, updated_at = now() where source = $1",
      [source.name]
    );
    await client.query(
      `
        update dataset_versions
        set status = 'ready',
          active = true,
          record_count = $3,
          import_finished_at = now(),
          last_checked_at = now(),
          error = null,
          updated_at = now()
        where source = $1 and filename = $2
      `,
      [source.name, source.metadata.filename, source.count]
    );
	    await client.query("commit");
	    invalidateActiveDatasetVersionCache(source.name);
	    console.log(`[open-data-import] ${source.name} activated count=${source.count} file=${source.metadata.filename}`);
			    if (source.name === "ownership" && !source.replaceMain) {
			      console.log("[open-data-import] refreshing company vehicle facts");
			      await refreshCompanyVehicleFacts(client);
			    } else if (source.name === "ownership") {
			      // ponytail: keep the 5GB rebuild lean; rebuild facts later only if ICO fleet lookup latency matters.
			      console.log("[open-data-import] skipped company vehicle facts refresh for destructive replace");
			    }
				    if (source.name === "vehicles" && !source.vinIndexOnly) {
				      await ensureOpenDataSchema();
				      console.log("[open-data-import] refreshing vehicle plate summaries");
			      await refreshVehiclePlateSummaries(client);
			    } else if (source.name === "vehicles") {
			      // ponytail: VIN ownership DB does not store vehicle summaries; API provides vehicle data.
			      console.log("[open-data-import] skipped vehicle summary refresh for VIN-only import");
			    }
		    if (source.name === "inspections") {
		      console.log("[open-data-import] refreshing vehicle inspection summaries");
		      await refreshVehicleInspectionSummaries(client);
		    }
	  } catch (error) {
    await client.query("rollback");
    throw error;
  }
}

function resolveSwitchSql(source) {
  if (source.name === "vehicles" && source.vinIndexOnly) {
    return VEHICLE_VINS_ONLY_SWITCH_SQL;
  }

  if (source.name === "ownership" && source.replaceMain) {
    return shouldUseLeanOwnershipIndexes()
      ? OWNERSHIP_DESTRUCTIVE_REPLACE_LEAN_SWITCH_SQL
      : OWNERSHIP_DESTRUCTIVE_REPLACE_SWITCH_SQL;
  }

  return source.config.switchSql;
}

async function scanCsvSource(source, onRow) {
  const input = source.localPath ? createLocalDatasetReadStream(source.localPath) : await openRemoteCsv(source);
  const reader = readline.createInterface({ input, crlfDelay: Infinity });
  let headerMap = null;

  for await (const rawLine of reader) {
    const line = headerMap ? rawLine : rawLine.replace(/^\uFEFF/, "");
    if (!headerMap) {
      const headers = parseCsvLine(line);
      headerMap = headers.reduce((map, header, index) => {
        map[canonicalizeCsvHeader(header)] = index;
        return map;
      }, Object.create(null));
      continue;
    }

    if (!line) {
      continue;
    }

    const values = parseCsvLine(line);
    await onRow({ values, headerMap });
  }
}

function createLocalDatasetReadStream(filePath) {
  const stream = fs.createReadStream(filePath);
  if (/\.gz$/i.test(filePath)) {
    return stream.pipe(zlib.createGunzip()).setEncoding("utf8");
  }

  stream.setEncoding("utf8");
  return stream;
}

async function openRemoteCsv(source) {
  return await new Promise((resolve, reject) => {
    const request = https.request(
      {
        protocol: "https:",
        hostname: "download.dataovozidlech.cz",
        path: source.config.route,
        method: "GET",
        headers: {
          Accept: "text/csv",
          "User-Agent": "Mozilla/5.0",
          _ren: source.token
        }
      },
      (response) => {
        if ((response.statusCode || 500) >= 400) {
          reject(new Error(`Open data download returned ${response.statusCode || 500} for ${source.name}.`));
          response.resume();
          return;
        }

        const contentLength = Number(response.headers["content-length"] || 0);
        let downloadedBytes = 0;
        let nextProgressBytes = 256 * 1024 * 1024;
        console.log(
          `[open-data-import] ${source.name} remote stream opened size=${
            contentLength ? formatBytes(contentLength) : "unknown"
          }`
        );
        const progressStream = new Transform({
          transform(chunk, encoding, callback) {
            downloadedBytes += Buffer.byteLength(chunk);
            if (downloadedBytes >= nextProgressBytes) {
              console.log(
                `[open-data-import] ${source.name} downloaded=${formatBytes(downloadedBytes)}${
                  contentLength ? `/${formatBytes(contentLength)}` : ""
                }`
              );
              nextProgressBytes += 256 * 1024 * 1024;
            }
            callback(null, chunk);
          }
        });
        progressStream.setEncoding("utf8");
        response.on("error", (error) => progressStream.destroy(error));
        response.pipe(progressStream);
        resolve(progressStream);
      }
    );

    request.on("error", reject);
    request.setTimeout(getNetworkTimeoutMs(), () => {
      request.destroy(new Error(`Open data download timed out for ${source.name}.`));
    });
    request.end();
  });
}

function formatBytes(bytes) {
  const value = Number(bytes);
  if (!Number.isFinite(value) || value <= 0) {
    return "0 B";
  }

  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = value;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(size >= 10 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
}
async function findLocalDataset(config, filename) {
  if (String(process.env.OPEN_DATA_IMPORT_USE_LOCAL_CACHE || "true").toLowerCase() === "false") {
    return null;
  }

	const direct = path.join(OPEN_DATA_DIR, filename);
	if (fs.existsSync(direct)) {
	  return direct;
	}
	const compressedDirect = `${direct}.gz`;
	if (fs.existsSync(compressedDirect)) {
	  return compressedDirect;
	}

  if (String(process.env.OPEN_DATA_IMPORT_ALLOW_STALE_LOCAL_CACHE || "false").toLowerCase() !== "true") {
    return null;
  }

  const entries = await fs.promises.readdir(OPEN_DATA_DIR).catch(() => []);
  const candidates = [];
  for (const name of entries) {
    if (!config.localPattern.test(name)) {
      continue;
    }

    const localPath = path.join(OPEN_DATA_DIR, name);
    const stats = await fs.promises.stat(localPath).catch(() => null);
    if (stats?.isFile()) {
      candidates.push({ name, localPath, size: stats.size });
    }
  }

  candidates.sort((left, right) => {
	    const leftComplete = isCompleteLocalDatasetFile(left.name) ? 1 : 0;
	    const rightComplete = isCompleteLocalDatasetFile(right.name) ? 1 : 0;
    if (leftComplete !== rightComplete) {
      return rightComplete - leftComplete;
    }
    return right.size - left.size;
  });

  return candidates[0]?.localPath || null;
}

async function findLatestLocalDataset(config) {
  const entries = await fs.promises.readdir(OPEN_DATA_DIR).catch(() => []);
  const candidates = [];
  for (const name of entries) {
    if (!config.localPattern.test(name)) {
      continue;
    }

    const localPath = path.join(OPEN_DATA_DIR, name);
    const stats = await fs.promises.stat(localPath).catch(() => null);
    if (stats?.isFile()) {
      candidates.push({ name, localPath, size: stats.size });
    }
  }

  candidates.sort((left, right) => {
	    const leftComplete = isCompleteLocalDatasetFile(left.name) ? 1 : 0;
	    const rightComplete = isCompleteLocalDatasetFile(right.name) ? 1 : 0;
    if (leftComplete !== rightComplete) {
      return rightComplete - leftComplete;
    }
    return right.size - left.size;
  });

  if (!candidates[0]) {
    throw new Error("V lokální cache není dostupný soubor pro vybraný open-data zdroj.");
  }

	  return candidates[0];
	}

function isCompleteLocalDatasetFile(name) {
  return /\.csv(?:\.gz)?$/i.test(name);
}

async function getRenToken() {
  return await new Promise((resolve, reject) => {
    const request = https.request(
      {
        protocol: "https:",
        hostname: "download.dataovozidlech.cz",
        path: "/",
        method: "GET",
        headers: {
          Accept: "text/html",
          "User-Agent": "Mozilla/5.0"
        }
      },
      (response) => {
        const token = normalizeWhitespace(response.headers["_ren"]);
        response.resume();
        token ? resolve(token) : reject(new Error("Could not obtain _ren token."));
      }
    );

	    request.on("error", reject);
	    request.setTimeout(getNetworkTimeoutMs(), () => {
	      request.destroy(new Error("Open data token request timed out."));
	    });
	    request.end();
	  });
}

async function fetchDatasetMetadata(route, token) {
  return await new Promise((resolve, reject) => {
    const request = https.request(
      {
        protocol: "https:",
        hostname: "download.dataovozidlech.cz",
        path: route,
        method: "HEAD",
        headers: {
          Accept: "text/csv",
          "User-Agent": "Mozilla/5.0",
          _ren: token
        }
      },
      (response) => {
        if ((response.statusCode || 500) >= 400) {
          reject(new Error(`Open data metadata returned ${response.statusCode || 500} for ${route}.`));
          response.resume();
          return;
        }

        const header = response.headers["content-disposition"];
        const filename = parseContentDispositionFilename(header);
        if (!filename) {
          reject(new Error(`Could not determine dataset filename for ${route}.`));
          response.resume();
          return;
        }
        resolve({
          filename,
          datasetDate: parseDatasetDateFromFilename(header)
        });
        response.resume();
      }
    );

	    request.on("error", reject);
	    request.setTimeout(getNetworkTimeoutMs(), () => {
	      request.destroy(new Error(`Open data metadata timed out for ${route}.`));
	    });
	    request.end();
	  });
}

function getNetworkTimeoutMs() {
  const timeout = Number(process.env.OPEN_DATA_IMPORT_NETWORK_TIMEOUT_MS || 300000);
  return Number.isFinite(timeout) && timeout > 0 ? timeout : 300000;
}

function selfCheck() {
  const previousVinOnly = process.env.OPEN_DATA_IMPORT_VEHICLES_VIN_INDEX_ONLY;
  const previousLeanOwnership = process.env.OPEN_DATA_IMPORT_LEAN_VIN_OWNERSHIP;
  try {
    process.env.OPEN_DATA_IMPORT_VEHICLES_VIN_INDEX_ONLY = "true";
    process.env.OPEN_DATA_IMPORT_LEAN_VIN_OWNERSHIP = "true";
    if (!shouldImportVehicleVinIndexOnly()) {
      throw new Error("VIN-only vehicle import should be enabled by default.");
    }
    const vehicleSql = resolveSwitchSql({
      name: "vehicles",
      vinIndexOnly: true,
      config: SOURCE_CONFIGS.vehicles
    });
    if (!vehicleSql.includes("vehicle_vins_next") || vehicleSql.includes("vehicles_next")) {
      throw new Error("VIN-only vehicle switch must update vehicle_vins without building vehicles.");
    }
    const ownershipSql = resolveSwitchSql({
      name: "ownership",
      replaceMain: true,
      config: SOURCE_CONFIGS.ownership
    });
    if (!ownershipSql.includes("ownership_relations_pcv_history_idx") || ownershipSql.includes("ownership_relations_ico_history_idx")) {
      throw new Error("Lean ownership switch must keep only the PČV history index.");
    }
    console.log("[open-data-import] self-check ok");
  } finally {
    restoreEnvValue("OPEN_DATA_IMPORT_VEHICLES_VIN_INDEX_ONLY", previousVinOnly);
    restoreEnvValue("OPEN_DATA_IMPORT_LEAN_VIN_OWNERSHIP", previousLeanOwnership);
  }
}

function restoreEnvValue(name, value) {
  if (value === undefined) {
    delete process.env[name];
  } else {
    process.env[name] = value;
  }
}

function normalizeVehicleRecord(values, headerMap, metadata) {
  const pcv = normalizeWhitespace(values[headerMap.PCV]);
  if (!pcv) {
    return null;
  }
  const plate = normalizePlate(firstNonEmptyByHeader(values, headerMap, [
    "SPZ",
    "RZ",
    "ECV",
    "REGISTRACNIZNACKA",
    "REGZNACKA",
    "EVIDENCNICISLO"
  ]));
  const dimensions = parseVehicleDimensionTriplet(values[headerMap.CELKOVADELKASIRKAVYSKAMM]);

  return {
    pcv,
    plate,
    vin: normalizeWhitespace(values[headerMap.VIN]) || null,
    make: normalizeWhitespace(values[headerMap.TOVARNIZNACKA]) || null,
	    model: normalizeWhitespace(values[headerMap.OBCHODNIOZNACENI]) || null,
	    type: normalizeWhitespace(values[headerMap.TYP]) || null,
	    variant: normalizeWhitespace(values[headerMap.VARIANTA]) || null,
	    category: normalizeWhitespace(values[headerMap.KATEGORIEVOZIDLA]) || null,
	    fuel: normalizeWhitespace(values[headerMap.PALIVO]) || null,
	    first_registration: normalizeOpenDataDate(values[headerMap.DATUM1REGISTRACE]),
	    first_registration_cz: normalizeOpenDataDate(values[headerMap.DATUM1REGISTRACEVCR]),
	    power: normalizeWhitespace(values[headerMap.MAXVYKONKWMIN]) || null,
	    color: normalizeWhitespace(values[headerMap.BARVA]) || null,
	    length_mm: dimensions[0] || null,
	    width_mm: dimensions[1] || null,
	    height_mm: dimensions[2] || null,
	    wheelbase_mm: normalizeVehicleMeasure(values[headerMap.ROZVORMM]),
	    weight_kg: normalizeVehicleMeasure(values[headerMap.PROVOZNIHMOTNOST]),
	    status: normalizeWhitespace(values[headerMap.STATUS]) || null,
    dataset_filename: metadata.filename,
    dataset_date: metadata.datasetDate
  };
}

function normalizeVehicleVinRecord(values, headerMap, metadata) {
  const pcv = normalizeWhitespace(values[headerMap.PCV]);
  const vin = normalizeVin(values[headerMap.VIN]);
  if (!pcv || !vin) {
    return null;
  }

  return {
    vin,
    pcv,
    dataset_filename: metadata.filename,
    dataset_date: metadata.datasetDate
  };
}

function normalizeOwnershipRecord(values, headerMap, metadata, options = getOwnershipImportOptions()) {
  const pcv = normalizeWhitespace(values[headerMap.PCV]);
  const ico = sanitizeIco(values[headerMap.ICO]);
  const current = normalizeBoolean(values[headerMap.AKTUALNI]);
  const subjectType = normalizeWhitespace(values[headerMap.TYPSUBJEKTU]) || null;
  const name = normalizeWhitespace(values[headerMap.NAZEV]) || null;
  const address = normalizeWhitespace(values[headerMap.ADRESA]) || null;
  const { requireIco, requireCurrent, requireIdentified } = options;
  if (!pcv) {
    return null;
  }
  if (requireIco && !ico) {
    return null;
  }
  if (!requireIco && requireIdentified && !isDisplayableOwnershipSubject({ ico, subjectType, name, address })) {
    return null;
  }
  if (requireCurrent && current !== true) {
    return null;
  }

  return {
    pcv,
    ico,
    name,
    address,
    relation: normalizeVehicleRelation(values[headerMap.VZTAHKVOZIDLU]) || null,
    subject_type: subjectType,
    current,
    date_from: normalizeOpenDataDate(values[headerMap.DATUMOD]),
    date_to: normalizeOpenDataDate(values[headerMap.DATUMDO]),
    dataset_filename: metadata.filename,
    dataset_date: metadata.datasetDate
  };
}

function getOwnershipImportOptions() {
  const fullLocal = isFullLocalImportProfile();
  return {
    requireIco: fullLocal
      ? false
      : String(process.env.OPEN_DATA_IMPORT_OWNERSHIP_REQUIRE_ICO || "false").toLowerCase() === "true",
    requireCurrent: fullLocal
      ? false
      : String(process.env.OPEN_DATA_IMPORT_OWNERSHIP_REQUIRE_CURRENT || "false").toLowerCase() === "true",
    requireIdentified:
      fullLocal
        ? false
        : String(process.env.OPEN_DATA_IMPORT_OWNERSHIP_REQUIRE_IDENTIFIED ?? "true").toLowerCase() !== "false"
  };
}

function getOwnershipHistoryScope() {
  return normalizeForMatch(
    process.env.OPEN_DATA_IMPORT_OWNERSHIP_HISTORY_SCOPE ||
      (isFullLocalImportProfile() ? "all" : OWNERSHIP_HISTORY_SCOPE_LEGAL_HISTORY)
  );
}

function shouldImportAllVehicleRows() {
  const scope = normalizeForMatch(process.env.OPEN_DATA_IMPORT_VEHICLES_SCOPE || "");
  return scope === "all" ||
    String(
      process.env.OPEN_DATA_IMPORT_VEHICLES_ALL_VINS ??
        (isFullLocalImportProfile() ? "true" : "false")
    ).toLowerCase() === "true";
}

function shouldImportVehicleVinIndexOnly() {
  return String(process.env.OPEN_DATA_IMPORT_VEHICLES_VIN_INDEX_ONLY || "true").toLowerCase() !== "false";
}

function shouldDestructivelyReplaceOwnershipMain(source) {
  return source.name === "ownership" &&
    String(process.env.OPEN_DATA_IMPORT_OWNERSHIP_DESTRUCTIVE_REPLACE || "false").toLowerCase() === "true";
}

function shouldUseLeanOwnershipIndexes() {
  return String(process.env.OPEN_DATA_IMPORT_LEAN_VIN_OWNERSHIP || "true").toLowerCase() !== "false";
}

function isFleetOwnershipRelation(value) {
  const normalized = normalizeForMatch(value);
  return normalized.includes("vlast") || normalized.includes("provoz");
}

function isDisplayableOwnershipSubject({ ico, subjectType, name, address }) {
  if (ico) {
    return true;
  }

  if (!isLegalEntitySubjectType(subjectType) && !looksLikeCompanyName(name)) {
    return false;
  }

  return hasDisplayableOwnershipText(name) || hasDisplayableOwnershipText(address);
}

function isLegalEntitySubjectType(value) {
  const normalized = normalizeForMatch(value);
  return normalized === "2" || normalized.includes("pravnick") || normalized.includes("company") || normalized.includes("firma");
}

function looksLikeCompanyName(value) {
  const normalized = normalizeForMatch(value);
  return /\b(s\s*r\s*o|spol|a\s*s|druzstvo|zapsany ustav|statni podnik|obec|mesto|kraj)\b/.test(normalized);
}

function hasDisplayableOwnershipText(value) {
  const normalized = normalizeForMatch(value);
  if (!normalized || normalized === "-") {
    return false;
  }

  return !(
    normalized.includes("fyzicka osoba") ||
    normalized.includes("anonym") ||
    normalized.includes("nezverej") ||
    normalized.includes("neuved")
  );
}

function normalizeInspectionRecord(values, headerMap, metadata) {
  const pcv = normalizeWhitespace(values[headerMap.PCV]);
  if (!pcv) {
    return null;
  }

  const odometer = normalizeOdometer(firstNonEmpty([
    values[headerMap.STAVTACHOMETRU],
    values[headerMap.STAVKM],
    values[headerMap.STAVKILOMETRU],
    values[headerMap.TACHOMETR],
    values[headerMap.KM]
  ]));

  return {
    pcv,
    type: normalizeWhitespace(values[headerMap.TYP]) || null,
    state: normalizeWhitespace(values[headerMap.STAV]) || null,
    station_code: normalizeWhitespace(values[headerMap.KODSTK]) || null,
    station_name: normalizeWhitespace(values[headerMap.NAZEVSTK]) || null,
    valid_from: normalizeOpenDataDate(values[headerMap.PLATNOSTOD]),
    valid_until: normalizeOpenDataDate(values[headerMap.PLATNOSTDO]),
    protocol_number: normalizeWhitespace(values[headerMap.CISLOPROTOKOLU]) || null,
    odometer,
    odometer_unit: odometer === null ? null : "km",
    current: normalizeBoolean(values[headerMap.AKTUALNI]),
    dataset_filename: metadata.filename,
    dataset_date: metadata.datasetDate
  };
}

function normalizeImportedVehicleRecord(values, headerMap, metadata) {
  const pcv = normalizeWhitespace(values[headerMap.PCV]);
  if (!pcv) {
    return null;
  }

  return {
    pcv,
    country: normalizeWhitespace(values[headerMap.STAT]) || null,
    imported_on: normalizeOpenDataDate(values[headerMap.DATUMDOVOZU]),
    dataset_filename: metadata.filename,
    dataset_date: metadata.datasetDate
  };
}

function normalizeDeregistrationRecord(values, headerMap, metadata) {
  const pcv = normalizeWhitespace(values[headerMap.PCV]);
  if (!pcv) {
    return null;
  }

  return {
    pcv,
    date_from: normalizeOpenDataDate(values[headerMap.DATUMOD]),
    date_to: normalizeOpenDataDate(values[headerMap.DATUMDO]),
    reason: normalizeWhitespace(values[headerMap.DUVOD]) || null,
    rm_code: normalizeWhitespace(values[headerMap.RMKOD]) || null,
    rm_name: normalizeWhitespace(values[headerMap.RMNAZEV]) || null,
    dataset_filename: metadata.filename,
    dataset_date: metadata.datasetDate
  };
}

function normalizeAuxiliaryRecord(sourceName, values, headerMap, metadata) {
  const payload = buildPayload(values, headerMap);
  const pcv = firstNonEmptyByHeader(values, headerMap, ["PCV", "PCVVOZIDLA", "PORADOVECISLOVOZIDLA"]);
  const vin = normalizeVin(firstNonEmptyByHeader(values, headerMap, ["VIN", "CISLOVIN", "IDENTIFIKACNICISLOVOZIDLA"]));
  if (!pcv && !vin && Object.keys(payload).length === 0) {
    return null;
  }

  const stableKey = crypto
    .createHash("sha1")
    .update(JSON.stringify(payload))
    .digest("hex");

  return {
    source: sourceName,
    pcv: pcv || null,
    vin: vin || null,
    record_key: stableKey,
    payload: JSON.stringify(payload),
    dataset_filename: metadata.filename,
    dataset_date: metadata.datasetDate
  };
}

function buildPayload(values, headerMap) {
  const payload = {};
  for (const [key, index] of Object.entries(headerMap)) {
    const value = normalizeWhitespace(values[index]);
    if (value) {
      payload[key] = value;
    }
  }
  return payload;
}

function firstNonEmptyByHeader(values, headerMap, keys) {
  for (const key of keys) {
    if (headerMap[key] === undefined) {
      continue;
    }
    const value = normalizeWhitespace(values[headerMap[key]]);
    if (value) {
      return value;
    }
  }
  return null;
}

function encodeCopyValue(value) {
  if (value === null || value === undefined || value === "") {
    return "\\N";
  }

  return String(value)
    .replace(/\\/g, "\\\\")
    .replace(/\t/g, "\\t")
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r");
}

function parseCsvLine(line) {
  const values = [];
  let current = "";
  let insideQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];

    if (char === "\"") {
      if (insideQuotes && next === "\"") {
        current += "\"";
        index += 1;
      } else {
        insideQuotes = !insideQuotes;
      }
    } else if (char === "," && !insideQuotes) {
      values.push(current);
      current = "";
    } else {
      current += char;
    }
  }

  values.push(current);
  return values;
}

function canonicalizeCsvHeader(value) {
  return String(value || "")
    .replace(/^\uFEFF/, "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "");
}

function parseContentDispositionFilename(header) {
  const value = String(header || "");
  const encodedMatch = value.match(/filename\*=UTF-8''([^;]+)/i);
  if (encodedMatch) {
    return decodeURIComponent(encodedMatch[1].replace(/"/g, ""));
  }

  const match = value.match(/filename="?([^";]+)"?/i);
  return match ? match[1].replace(/"/g, "") : null;
}

function parseDatasetDateFromFilename(header) {
  const filename = parseContentDispositionFilename(header) || String(header || "");
  const match = filename.match(/(\d{4})(\d{2})(\d{2})/);
  if (!match) {
    return null;
  }
  return `${match[1]}-${match[2]}-${match[3]}`;
}

function normalizeOpenDataDate(value) {
  const normalized = normalizeWhitespace(value);
  if (!normalized) {
    return null;
  }

  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) {
    return normalized.slice(0, 10);
  }

  return parsed.toISOString().slice(0, 10);
}

function normalizeBoolean(value) {
  const normalized = normalizeWhitespace(value).toLowerCase();
  if (!normalized) {
    return null;
  }
  if (["true", "1", "ano"].includes(normalized)) {
    return true;
  }
  if (["false", "0", "ne"].includes(normalized)) {
    return false;
  }
  return null;
}

function normalizeOdometer(value) {
  const normalized = normalizeWhitespace(value).replace(/\s+/g, "").replace(",", ".");
  if (!normalized) {
    return null;
  }

  const parsed = Number.parseInt(normalized, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function firstNonEmpty(values) {
  for (const value of values) {
    if (value === 0) {
      return value;
    }
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
    if (value !== null && value !== undefined && value !== "") {
      return value;
    }
  }
  return null;
}

function normalizeVehicleRelation(value) {
  const normalized = normalizeWhitespace(value).toLowerCase();
  if (!normalized) {
    return null;
  }
  if (normalized === "1" || normalized.includes("vlast")) {
    return "Vlastnik";
  }
  if (normalized === "2" || normalized.includes("provoz")) {
    return "Provozovatel";
  }
  return normalizeWhitespace(value);
}

function sanitizeIco(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return /^\d{8}$/.test(digits) ? digits : null;
}

function normalizePlate(value) {
  const normalized = normalizeWhitespace(value).toUpperCase().replace(/[^A-Z0-9]/g, "");
  return /^[A-Z0-9]{5,10}$/.test(normalized) ? normalized : null;
}

function normalizeVin(value) {
  const normalized = normalizeWhitespace(value)
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
  return normalized.length >= 6 ? normalized : null;
}

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function parseVehicleDimensionTriplet(value) {
  return normalizeWhitespace(value)
    .replace(/[x×]/gi, "/")
    .split("/")
    .map(normalizeVehicleMeasure)
    .filter(Boolean);
}

function normalizeVehicleMeasure(value) {
  const text = normalizeWhitespace(value).replace(/\b(?:mm|kg)\b/gi, "").trim();
  return text ? text.replace(/\s+/g, " ") : null;
}

function normalizeForMatch(value) {
  return normalizeWhitespace(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) {
    return;
  }

  const contents = fs.readFileSync(filePath, "utf8");
  contents.split(/\r?\n/).forEach((line) => {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      return;
    }

    const separatorIndex = trimmed.indexOf("=");
    if (separatorIndex === -1) {
      return;
    }

    const key = trimmed.slice(0, separatorIndex).trim();
    const value = trimmed.slice(separatorIndex + 1).trim();
    if (!(key in process.env)) {
      process.env[key] = value;
    }
  });
}
