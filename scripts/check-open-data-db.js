#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const { Client } = require("pg");

loadEnvFile(path.join(__dirname, "..", ".env"));

const { closeDatabasePool, getOpenDataStatus } = require("../open-data-db");

main().catch((error) => {
  console.error("[db:status] failed");
  console.error(error && error.stack ? error.stack : String(error));
  process.exitCode = 1;
});

async function main() {
  const status = await getOpenDataStatus([
    "ownership",
    "vehicles",
    "inspections",
    "deregistered",
    "imports",
    "equipment",
    "manufacturer_reports"
  ]);
  status.plateCoverage = await getPlateCoverage();
  await closeDatabasePool();
  console.log(JSON.stringify(status, null, 2));
}

async function getPlateCoverage() {
  if (!process.env.DATABASE_URL) {
    return {
      configured: false
    };
  }

  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: String(process.env.DATABASE_SSL || "").toLowerCase() === "true" ? { rejectUnauthorized: false } : undefined
  });
  await client.connect();
  try {
    const includeHistory = parseBoolean(process.env.STATUS_INCLUDE_HISTORY || process.env.OPEN_DATA_STATUS_INCLUDE_HISTORY, false);
    const [globalCoverage, icoCoverage] = await Promise.all([
      getGlobalPlateCoverage(client),
      getIcoPlateCoverage(client, process.env.STATUS_ICO || process.env.OPEN_DATA_STATUS_ICO, { includeHistory })
    ]);

    return {
      configured: true,
      ...globalCoverage,
      ico: icoCoverage
    };
  } finally {
    await client.end();
  }
}

async function getGlobalPlateCoverage(client) {
  const result = await client.query(`
    select
      coalesce(
        (select record_count::bigint from dataset_versions where source = 'vehicles' and active is true and status = 'ready' order by import_finished_at desc nulls last, id desc limit 1),
        (select greatest(0, reltuples)::bigint from pg_class where oid = 'vehicles'::regclass)
      ) as vehicle_count,
      (select count(*)::bigint from vehicles where plate is not null and length(btrim(plate)) > 0) as vehicle_plate_count,
      (select greatest(0, reltuples)::bigint from pg_class where oid = 'supplemental_ownership_relations'::regclass) as supplemental_relation_count,
      (select count(*)::bigint from supplemental_ownership_relations where plate is not null and length(btrim(plate)) > 0) as supplemental_plate_count,
      (select count(*)::bigint from vehicle_plate_links) as vehicle_plate_link_count,
	      (select count(*)::bigint from vehicle_plate_summaries) as vehicle_plate_summary_count,
	      (select count(*)::bigint from vehicle_plate_summaries) as fleet_fact_plate_count,
	      (select count(*)::bigint from plate_resolutions) as plate_resolution_count,
	      (select greatest(0, reltuples)::bigint from pg_class where oid = 'vehicle_inspection_summaries'::regclass) as inspection_summary_count,
	      coalesce(
	        (select greatest(0, reltuples)::bigint from pg_class where oid = to_regclass('vehicle_fleet_facts')),
	        0::bigint
	      ) as vehicle_fleet_fact_count,
	      coalesce(
	        (select greatest(0, reltuples)::bigint from pg_class where oid = to_regclass('company_vehicle_facts')),
	        0::bigint
	      ) as company_vehicle_fact_count
	  `);
  return result.rows[0] || {};
}

async function getIcoPlateCoverage(client, ico, options = {}) {
  const normalizedIco = String(ico || "").replace(/\D/g, "");
  if (!/^\d{8}$/.test(normalizedIco)) {
    return null;
  }
  const includeHistory = Boolean(options.includeHistory);
  const currentFilter = includeHistory ? "" : "and current is true and date_to is null";
  const currentFilterAlias = includeHistory ? "" : "and o.current is true and o.date_to is null";

  const result = await client.query(
    `
      with company_names as (
        select coalesce(
          array_agg(distinct lower(name)) filter (where name is not null and length(btrim(name)) > 0),
          array[]::text[]
        ) as names
        from ares_companies
        where ico = $1
      ),
      pcvs as materialized (
        select distinct pcv
        from ownership_relations
        where ico = $1
          and pcv is not null
          and relation in ('Vlastnik', 'Provozovatel')
          ${currentFilter}
        union
        select distinct o.pcv
        from ownership_relations o, company_names
        where o.ico is null
          and lower(o.name) = any(company_names.names)
          and o.pcv is not null
          and o.relation in ('Vlastnik', 'Provozovatel')
          ${currentFilterAlias}
        union
        select distinct pcv
        from supplemental_ownership_relations
        where ico = $1
          and pcv is not null
          and relation in ('Vlastnik', 'Provozovatel')
          ${currentFilter}
      ),
      coverage as (
        select
          p.pcv,
          (coalesce(nullif(btrim(v.plate), ''), nullif(btrim(vps.plate), '')) is not null) as has_plate,
          (vis.performed_on is not null and vis.valid_until is not null) as has_stk
        from pcvs p
        left join vehicles v on v.pcv = p.pcv
        left join vehicle_plate_summaries vps on vps.pcv = p.pcv and (vps.expires_at is null or vps.expires_at > now())
        left join vehicle_inspection_summaries vis on vis.pcv = p.pcv
      )
      select
        $1 as ico,
        count(*)::bigint as vehicle_count,
        count(*) filter (where has_plate)::bigint as plate_count,
        count(*) filter (where not has_plate)::bigint as missing_plate_count,
        count(*) filter (where has_stk)::bigint as stk_count,
        count(*) filter (where not has_stk)::bigint as missing_stk_count
      from coverage
    `,
    [normalizedIco]
  );
  const row = result.rows[0] || null;
  if (!row) {
    return null;
  }

  row.include_history = includeHistory;
  row.missing_plate_samples = await getIcoMissingPlateSamples(client, normalizedIco, { includeHistory });
  return row;
}

async function getIcoMissingPlateSamples(client, ico, options = {}) {
  const includeHistory = Boolean(options.includeHistory);
  const currentFilter = includeHistory ? "" : "and current is true and date_to is null";
  const currentFilterAlias = includeHistory ? "" : "and o.current is true and o.date_to is null";
  const result = await client.query(
    `
      with company_names as (
        select coalesce(
          array_agg(distinct lower(name)) filter (where name is not null and length(btrim(name)) > 0),
          array[]::text[]
        ) as names
        from ares_companies
        where ico = $1
      ),
      pcvs as materialized (
        select distinct pcv
        from ownership_relations
        where ico = $1
          and pcv is not null
          and relation in ('Vlastnik', 'Provozovatel')
          ${currentFilter}
        union
        select distinct o.pcv
        from ownership_relations o, company_names
        where o.ico is null
          and lower(o.name) = any(company_names.names)
          and o.pcv is not null
          and o.relation in ('Vlastnik', 'Provozovatel')
          ${currentFilterAlias}
        union
        select distinct pcv
        from supplemental_ownership_relations
        where ico = $1
          and pcv is not null
          and relation in ('Vlastnik', 'Provozovatel')
          ${currentFilter}
      )
      select
        p.pcv,
        v.vin,
        trim(concat_ws(' ', v.make, v.model, v.type)) as title,
        v.first_registration,
        v.status as vehicle_status
      from pcvs p
      left join vehicles v on v.pcv = p.pcv
      left join vehicle_plate_summaries vps on vps.pcv = p.pcv and (vps.expires_at is null or vps.expires_at > now())
      where coalesce(nullif(btrim(v.plate), ''), nullif(btrim(vps.plate), '')) is null
      order by v.first_registration desc nulls last, p.pcv asc
      limit 20
    `,
    [ico]
  );
  return result.rows;
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

function parseBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "n", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
}
