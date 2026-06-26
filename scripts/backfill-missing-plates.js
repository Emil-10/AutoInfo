#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const ROOT_DIR = path.join(__dirname, "..");
loadEnvFile(path.join(ROOT_DIR, ".env"));

const {
  closeDatabasePool,
  ensureOpenDataSchema,
  getPool,
  refreshVehiclePlateSummaries
} = require("../open-data-db");
const { resolveVehiclePlate } = require("../vehicle-service");

main().catch(async (error) => {
  console.error("[plate-backfill] failed");
  console.error(error && error.stack ? error.stack : String(error));
  await closeDatabasePool().catch(() => {});
  process.exitCode = 1;
});

async function main() {
  const ico = sanitizeIco(getArg("--ico") || process.env.PLATE_BACKFILL_ICO || process.env.STATUS_ICO);
  const dryRun = hasFlag("--dry-run") || parseBoolean(process.env.PLATE_BACKFILL_DRY_RUN, false);
  const confirmed = hasFlag("--confirm-external-lookup") ||
    parseBoolean(process.env.PLATE_BACKFILL_CONFIRM_EXTERNAL_LOOKUP, false);
  const includeHistory = hasFlag("--include-history") || parseBoolean(process.env.PLATE_BACKFILL_INCLUDE_HISTORY, false);
  const limit = normalizePositiveInteger(getArg("--limit") || process.env.PLATE_BACKFILL_LIMIT, 25);
  const delayMs = normalizeNonNegativeInteger(getArg("--delay-ms") || process.env.PLATE_BACKFILL_DELAY_MS, 750);

  if (!ico) {
    throw new Error("Pouziti: npm run db:backfill-plates -- --ico 06649114 --dry-run");
  }
  if (!process.env.DATABASE_URL) {
    throw new Error("DATABASE_URL neni nastaveny.");
  }

  if (!dryRun && confirmed) {
    await ensureOpenDataSchema();
  }
  const pool = getPool();
  if (!pool) {
    throw new Error("DATABASE_URL neni nastaveny.");
  }

  const client = await pool.connect();
  let rows = [];
  try {
    rows = await getMissingPlateRows(client, ico, limit, { includeHistory });
  } finally {
    client.release();
  }

  if (dryRun || !confirmed) {
    console.log(JSON.stringify({
      ico,
      dryRun: true,
      externalLookupConfirmed: confirmed,
      includeHistory,
      message: confirmed
        ? "Dry-run: zadne externi dohledani nebylo spusteno."
        : "Externi dohledani neni potvrzene. Pro realny backfill pouzijte --confirm-external-lookup.",
      rows
    }, null, 2));
    await closeDatabasePool();
    return;
  }

  const results = [];
  let resolvedCount = 0;
  for (const row of rows) {
    const result = await resolveVehiclePlate({ vin: row.vin, pcv: row.pcv }).catch((error) => ({
      status: "error",
      plate: null,
      message: error && error.message ? error.message : String(error)
    }));
    const normalized = {
      pcv: row.pcv,
      vin: row.vin,
      title: row.title,
      status: result?.status || "unknown",
      plate: result?.plate || null,
      source: result?.source || null,
      message: result?.message || null
    };
    if (normalized.plate) {
      resolvedCount += 1;
    }
    results.push(normalized);
    if (delayMs > 0) {
      await sleep(delayMs);
    }
  }

  if (resolvedCount > 0) {
    await refreshVehiclePlateSummaries();
  }
  await closeDatabasePool();
  console.log(JSON.stringify({
    ico,
    includeHistory,
    checked: rows.length,
    resolved: resolvedCount,
    unresolved: rows.length - resolvedCount,
    results
  }, null, 2));
}

async function getMissingPlateRows(client, ico, limit, options = {}) {
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
        v.first_registration
      from pcvs p
      left join vehicles v on v.pcv = p.pcv
      left join vehicle_plate_summaries vps on vps.pcv = p.pcv and (vps.expires_at is null or vps.expires_at > now())
      where coalesce(nullif(btrim(v.plate), ''), nullif(btrim(vps.plate), '')) is null
        and (v.vin is not null or p.pcv is not null)
      order by v.first_registration desc nulls last, p.pcv asc
      limit $2
    `,
    [ico, limit]
  );
  return result.rows;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getArg(name) {
  const index = process.argv.indexOf(name);
  if (index === -1 || index + 1 >= process.argv.length) {
    return "";
  }
  return process.argv[index + 1];
}

function hasFlag(name) {
  return process.argv.includes(name);
}

function sanitizeIco(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return /^\d{8}$/.test(digits) ? digits : "";
}

function normalizePositiveInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function normalizeNonNegativeInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
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
