#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const { Client } = require("pg");

loadEnvFile(path.join(__dirname, "..", ".env"));

main().catch((error) => {
  console.error("[plate-template] failed");
  console.error(error && error.stack ? error.stack : String(error));
  process.exitCode = 1;
});

async function main() {
  const ico = sanitizeIco(getArg("--ico") || process.env.STATUS_ICO || process.env.PLATE_TEMPLATE_ICO);
  const includeHistory = hasFlag("--include-history") || parseBoolean(process.env.PLATE_TEMPLATE_INCLUDE_HISTORY, false);
  if (!ico) {
    throw new Error("Pouziti: npm run db:missing-plates -- --ico 06649114 [--output missing-plates.csv]");
  }
  if (!process.env.DATABASE_URL) {
    throw new Error("DATABASE_URL neni nastaveny.");
  }

  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: String(process.env.DATABASE_SSL || "").toLowerCase() === "true" ? { rejectUnauthorized: false } : undefined
  });
  await client.connect();
  try {
    const rows = await getMissingPlateRows(client, ico, { includeHistory });
    const csv = toCsv([
      ["pcv", "vin", "title", "spz"],
      ...rows.map((row) => [row.pcv || "", row.vin || "", row.title || "", ""])
    ]);
    const outputPath = getArg("--output") || process.env.PLATE_TEMPLATE_OUTPUT;
    if (outputPath) {
      const resolved = path.resolve(process.cwd(), outputPath);
      await fs.promises.mkdir(path.dirname(resolved), { recursive: true });
      await fs.promises.writeFile(resolved, csv, "utf8");
      console.log(JSON.stringify({ ico, includeHistory, rows: rows.length, output: resolved }, null, 2));
      return;
    }
    process.stdout.write(csv);
  } finally {
    await client.end();
  }
}

async function getMissingPlateRows(client, ico, options = {}) {
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
      order by v.first_registration desc nulls last, p.pcv asc
    `,
    [ico]
  );
  return result.rows;
}

function toCsv(rows) {
  return `${rows.map((row) => row.map(escapeCsvValue).join(",")).join("\n")}\n`;
}

function escapeCsvValue(value) {
  const text = String(value ?? "");
  if (!/[",\r\n]/.test(text)) {
    return text;
  }
  return `"${text.replace(/"/g, '""')}"`;
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

function sanitizeIco(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return /^\d{8}$/.test(digits) ? digits : "";
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
