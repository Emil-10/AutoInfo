#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const ROOT_DIR = path.join(__dirname, "..");
loadEnvFile(path.join(ROOT_DIR, ".env"));

const {
  closeDatabasePool,
  ensureOpenDataSchema,
  getPool,
  invalidateActiveDatasetVersionCache,
  refreshCompanyVehicleFacts,
  refreshVehicleInspectionSummaries,
  refreshVehiclePlateSummaries
} = require("../open-data-db");

const IMPORT_LOCK_KEY = [720191, 20260519];
const ACTIVATORS = {
  ownership: {
    stagingTable: "ownership_relations_staging",
    cleanupTables: ["ownership_relations_staging"],
    prepare: prepareOwnership,
    swap: swapOwnership
  },
  vehicles: {
    stagingTable: "vehicles_staging",
    cleanupTables: ["vehicle_vins_staging", "vehicles_staging"],
    prepare: prepareVehicles,
    swap: swapVehicles
  },
  inspections: {
    stagingTable: "inspections_staging",
    cleanupTables: ["inspections_staging"],
    prepare: prepareInspections,
    swap: swapInspections
  }
};

main().catch(async (error) => {
  console.error("[open-data-activate] failed");
  console.error(error && error.stack ? error.stack : String(error));
  await closeDatabasePool().catch(() => {});
  process.exitCode = 1;
});

async function main() {
  const source = getArg("--source") || "ownership";
  const activator = ACTIVATORS[source];
  if (!activator) {
    throw new Error("Supported sources are: ownership, vehicles, inspections.");
  }

  const filename = getArg("--filename");
  const recordCount = Number(getArg("--count") || 0);
  const datasetDate = getArg("--dataset-date") || parseDatasetDateFromFilename(filename);

  if (!filename) {
    throw new Error("--filename is required.");
  }
  if (!Number.isFinite(recordCount) || recordCount <= 0) {
    throw new Error("--count must be a positive number.");
  }

  const pool = getPool();
  if (!pool) {
    throw new Error("DATABASE_URL neni nastaveny.");
  }

  await ensureOpenDataSchema();
  const client = await pool.connect();
  let lockAcquired = false;
  try {
    await client.query("set statement_timeout = 0");
    await client.query("set lock_timeout = 0");
    await client.query("set idle_in_transaction_session_timeout = 0");
    await client.query(`set maintenance_work_mem = '${escapeSqlLiteral(process.env.OPEN_DATA_IMPORT_MAINTENANCE_WORK_MEM || "512MB")}'`);
    await client.query("select pg_advisory_lock($1, $2)", IMPORT_LOCK_KEY);
    lockAcquired = true;

    const estimate = await client.query(`
      select reltuples::bigint as estimate
      from pg_class
      where relname = $1
    `, [activator.stagingTable]);
    console.log(`[open-data-activate] staging estimate=${estimate.rows[0]?.estimate || 0}`);

    await activator.prepare(client);

    console.log("[open-data-activate] swapping tables");
    await client.query("begin");
    await activator.swap(client);
    for (const table of activator.cleanupTables) {
      await client.query(`truncate table ${table}`);
    }
	    await updateDatasetVersion(client, source, filename, datasetDate, recordCount);
		    await client.query("commit");
		    invalidateActiveDatasetVersionCache(source);
		    console.log(`[open-data-activate] activated source=${source} count=${recordCount} file=${filename}`);
		    if (source === "ownership") {
		      console.log("[open-data-activate] refreshing company vehicle facts");
		      await refreshCompanyVehicleFacts(client);
		    }
				    if (source === "vehicles") {
				      await ensureOpenDataSchema();
			      console.log("[open-data-activate] refreshing vehicle plate summaries");
			      await refreshVehiclePlateSummaries(client);
			    }
		    if (source === "inspections") {
		      console.log("[open-data-activate] refreshing vehicle inspection summaries");
		      await refreshVehicleInspectionSummaries(client);
		    }
	  } catch (error) {
    await client.query("rollback").catch(() => {});
    throw error;
  } finally {
    if (lockAcquired) {
      await client.query("select pg_advisory_unlock($1, $2)", IMPORT_LOCK_KEY).catch(() => {});
    }
    client.release();
    await closeDatabasePool();
  }
}

async function prepareOwnership(client) {
  await client.query("drop table if exists ownership_relations_next");
  await client.query(`
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
    )
  `);

  console.log("[open-data-activate] inserting ownership_relations_next");
  await client.query(`
    insert into ownership_relations_next (
      pcv, ico, name, address, relation, subject_type, current,
      date_from, date_to, dataset_filename, dataset_date, imported_at
    )
    select
      pcv, ico, name, address, relation, subject_type, current,
      date_from, date_to, dataset_filename, dataset_date, now()
	    from ownership_relations_staging
	    where pcv is not null
	  `);

	  await client.query(`
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
	      updated_at = now()
	  `);
	  await client.query(`
	    update ownership_relations_next r
	    set party_id = p.id
	    from ownership_parties p
	    where p.party_key = md5(
	      coalesce(r.ico, '') || '|' ||
	      coalesce(lower(r.name), '') || '|' ||
	      coalesce(r.address, '') || '|' ||
	      coalesce(r.subject_type, '')
	    )
	  `);

	  console.log("[open-data-activate] creating ownership indexes");
	  await client.query("create index ownership_relations_next_ico_idx on ownership_relations_next (ico)");
	  await client.query("create index ownership_relations_next_party_id_idx on ownership_relations_next (party_id) where party_id is not null");
	  await client.query("create index ownership_relations_next_pcv_idx on ownership_relations_next (pcv)");
  await client.query("create index ownership_relations_next_current_idx on ownership_relations_next (ico, pcv) where current is true and date_to is null");
  await client.query("create index ownership_relations_next_ico_current_relation_idx on ownership_relations_next (ico, pcv) where current is true and date_to is null and relation in ('Vlastnik', 'Provozovatel')");
  await client.query("create index ownership_relations_next_ico_history_idx on ownership_relations_next (ico, date_from desc, pcv) where relation in ('Vlastnik', 'Provozovatel')");
  await client.query("create index ownership_relations_next_pcv_history_idx on ownership_relations_next (pcv, date_from desc) where relation in ('Vlastnik', 'Provozovatel')");
  await client.query("create index ownership_relations_next_missing_ico_name_current_relation_idx on ownership_relations_next (lower(name), pcv) where ico is null and name is not null and current is true and date_to is null and relation in ('Vlastnik', 'Provozovatel')");
  await client.query("create index ownership_relations_next_missing_ico_name_history_idx on ownership_relations_next (lower(name), date_from desc, pcv) where ico is null and name is not null and relation in ('Vlastnik', 'Provozovatel')");

  console.log("[open-data-activate] analyzing ownership_relations_next");
  await client.query("analyze ownership_relations_next");
}

async function swapOwnership(client) {
  await client.query("drop table if exists ownership_relations_old");
  await client.query("alter table ownership_relations rename to ownership_relations_old");
  await client.query("alter table ownership_relations_next rename to ownership_relations");
  await client.query("drop table ownership_relations_old");
	  await client.query("alter index ownership_relations_next_pkey rename to ownership_relations_pkey");
	  await client.query("alter index ownership_relations_next_ico_idx rename to ownership_relations_ico_idx");
	  await client.query("alter index ownership_relations_next_party_id_idx rename to ownership_relations_party_id_idx");
	  await client.query("alter index ownership_relations_next_pcv_idx rename to ownership_relations_pcv_idx");
  await client.query("alter index ownership_relations_next_current_idx rename to ownership_relations_current_idx");
  await client.query("alter index ownership_relations_next_ico_current_relation_idx rename to ownership_relations_ico_current_relation_idx");
  await client.query("alter index ownership_relations_next_ico_history_idx rename to ownership_relations_ico_history_idx");
  await client.query("alter index ownership_relations_next_pcv_history_idx rename to ownership_relations_pcv_history_idx");
  await client.query("alter index ownership_relations_next_missing_ico_name_current_relation_idx rename to ownership_relations_missing_ico_name_current_relation_idx");
  await client.query("alter index ownership_relations_next_missing_ico_name_history_idx rename to ownership_relations_missing_ico_name_history_idx");
}

async function prepareVehicles(client) {
  await client.query("drop table if exists vehicle_vins_next");
  await client.query("drop table if exists vehicles_next");
  await client.query(`
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
      status text,
      dataset_filename text,
      dataset_date date,
      imported_at timestamptz not null default now()
    )
  `);

  console.log("[open-data-activate] inserting vehicles_next");
  await client.query(`
    insert into vehicles_next (
      pcv, plate, vin, make, model, type, variant, category, fuel, first_registration,
      first_registration_cz, power, color, status, dataset_filename, dataset_date, imported_at
    )
    select distinct on (pcv)
      pcv, plate, vin, make, model, type, variant, category, fuel, first_registration,
      first_registration_cz, power, color, status, dataset_filename, dataset_date, now()
    from vehicles_staging
    where pcv is not null
    order by pcv, vin nulls last
  `);

  console.log("[open-data-activate] creating vehicle indexes");
  await client.query("create index vehicles_next_vin_idx on vehicles_next (vin) where vin is not null");
  await client.query(`
    create table vehicle_vins_next (
      vin text primary key,
      pcv text not null,
      dataset_filename text,
      dataset_date date,
      imported_at timestamptz not null default now()
    )
  `);
  await client.query(`
    insert into vehicle_vins_next (
      vin, pcv, dataset_filename, dataset_date, imported_at
    )
    select distinct on (vin)
      vin, pcv, dataset_filename, dataset_date, now()
    from vehicles_next
    where vin is not null and pcv is not null
    order by vin, pcv
  `);
  await client.query("create index vehicle_vins_next_pcv_idx on vehicle_vins_next (pcv)");

  console.log("[open-data-activate] analyzing vehicle tables");
  await client.query("analyze vehicles_next");
  await client.query("analyze vehicle_vins_next");
}

async function swapVehicles(client) {
  await client.query(`
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
    end $$
  `);
  await client.query("alter table if exists vehicle_plate_links drop constraint if exists vehicle_plate_links_pcv_fkey");
  await client.query("alter table if exists vehicle_plate_summaries drop constraint if exists vehicle_plate_summaries_pcv_fkey");
  await client.query("alter table if exists supplemental_ownership_relations drop constraint if exists supplemental_ownership_relations_pcv_fkey");
  await client.query("drop table if exists vehicles_old");
  await client.query("drop table if exists vehicle_vins_old");
  await client.query("alter table vehicles rename to vehicles_old");
  await client.query("alter table vehicle_vins rename to vehicle_vins_old");
  await client.query("alter table vehicles_next rename to vehicles");
  await client.query("alter table vehicle_vins_next rename to vehicle_vins");
  await client.query("drop table vehicles_old");
  await client.query("drop table vehicle_vins_old");
  await client.query("alter index vehicles_next_pkey rename to vehicles_pkey");
  await client.query("alter index vehicles_next_vin_idx rename to vehicles_vin_idx");
  await client.query("alter index vehicle_vins_next_pkey rename to vehicle_vins_pkey");
  await client.query("alter index vehicle_vins_next_pcv_idx rename to vehicle_vins_pcv_idx");
  await client.query(`
    delete from vehicle_plate_links vpl
    where vpl.pcv is not null
      and not exists (select 1 from vehicles v where v.pcv = vpl.pcv)
  `);
  await client.query(`
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
    end $$
  `);
}

async function prepareInspections(client) {
  await client.query("drop table if exists inspections_next");
  await client.query(`
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
    )
  `);

  console.log("[open-data-activate] inserting inspections_next");
  await client.query(`
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
    where pcv is not null
  `);

  console.log("[open-data-activate] creating inspection indexes");
  await client.query("create index inspections_next_pcv_idx on inspections_next (pcv)");
  await client.query("create index inspections_next_current_idx on inspections_next (pcv) where current is true");
  await client.query("create index inspections_next_pcv_valid_until_idx on inspections_next (pcv, valid_until desc)");

  console.log("[open-data-activate] analyzing inspections_next");
  await client.query("analyze inspections_next");
}

async function swapInspections(client) {
  await client.query("drop table if exists inspections_old");
  await client.query("alter table inspections rename to inspections_old");
  await client.query("alter table inspections_next rename to inspections");
  await client.query("drop table inspections_old");
  await client.query("alter index inspections_next_pkey rename to inspections_pkey");
  await client.query("alter index inspections_next_pcv_idx rename to inspections_pcv_idx");
  await client.query("alter index inspections_next_current_idx rename to inspections_current_idx");
  await client.query("alter index inspections_next_pcv_valid_until_idx rename to inspections_pcv_valid_until_idx");
}

async function updateDatasetVersion(client, source, filename, datasetDate, recordCount) {
  await client.query(
    `
      update dataset_versions
      set active = false, updated_at = now()
      where source = $1
    `,
    [source]
  );
  await client.query(
    `
      insert into dataset_versions (
        source, filename, dataset_date, status, active, record_count,
        last_checked_at, import_started_at, import_finished_at, error, updated_at
      )
      values ($1, $2, $3, 'ready', true, $4, now(), now(), now(), null, now())
      on conflict (source, filename) do update set
        status = 'ready',
        active = true,
        record_count = excluded.record_count,
        last_checked_at = now(),
        import_finished_at = now(),
        error = null,
        updated_at = now()
    `,
    [source, filename, datasetDate, recordCount]
  );
}

function getArg(name) {
  const prefix = `${name}=`;
  const withEquals = process.argv.find((arg) => arg.startsWith(prefix));
  if (withEquals) {
    return withEquals.slice(prefix.length);
  }
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : null;
}

function parseDatasetDateFromFilename(filename) {
  const match = String(filename || "").match(/(\d{4})(\d{2})(\d{2})/);
  return match ? `${match[1]}-${match[2]}-${match[3]}` : null;
}

function escapeSqlLiteral(value) {
  return String(value).replace(/'/g, "''");
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
