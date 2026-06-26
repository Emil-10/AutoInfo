#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const readline = require("readline");

const ROOT_DIR = path.join(__dirname, "..");
loadEnvFile(path.join(ROOT_DIR, ".env"));

const {
  closeDatabasePool,
  ensureOpenDataSchema,
  getPool,
  refreshVehiclePlateSummaries
} = require("../open-data-db");

const DEFAULT_CONFIDENCE = 0.95;
const INSERT_CHUNK_SIZE = 500;

main().catch(async (error) => {
  console.error("[plate-import] failed");
  console.error(error && error.stack ? error.stack : String(error));
  await closeDatabasePool().catch(() => {});
  process.exitCode = 1;
});

async function main() {
  const filePath = resolveInputFile();
  const source = normalizeText(getArg("--source") || process.env.PLATE_IMPORT_SOURCE) || "plate-import";
  const confidence = normalizeConfidence(getArg("--confidence") || process.env.PLATE_IMPORT_CONFIDENCE, DEFAULT_CONFIDENCE);
  const ttlDays = normalizeNonNegativeInteger(getArg("--ttl-days") || process.env.PLATE_IMPORT_TTL_DAYS, 0);
  const requiredIcos = parseIcoList(getArg("--require-ico") || process.env.PLATE_IMPORT_REQUIRE_ICO);
  const allowMissingStk = hasFlag("--allow-missing-stk") || parseBoolean(process.env.PLATE_IMPORT_ALLOW_MISSING_STK);
  const dryRun = hasFlag("--dry-run");

  const rows = await readPlateRows(filePath, { source, confidence, ttlDays });
  if (rows.valid.length === 0) {
	    console.log(JSON.stringify({
	      file: filePath,
	      dryRun,
	      requiredIcos,
	      coverageCheck: dryRun && requiredIcos.length > 0 ? "skipped" : "not-requested",
	      validRows: 0,
	      invalidRows: rows.invalid.length,
	      invalidSamples: rows.invalid.slice(0, 10)
    }, null, 2));
    await closeDatabasePool();
    return;
  }

  if (dryRun) {
	    console.log(JSON.stringify({
	      file: filePath,
	      dryRun: true,
	      requiredIcos,
	      coverageCheck: requiredIcos.length > 0 ? "skipped" : "not-requested",
	      validRows: rows.valid.length,
	      invalidRows: rows.invalid.length,
      validSamples: rows.valid.slice(0, 5),
      invalidSamples: rows.invalid.slice(0, 10)
    }, null, 2));
    await closeDatabasePool();
    return;
  }

  const pool = getPool();
  if (!pool) {
    throw new Error("DATABASE_URL neni nastaveny.");
  }

  await ensureOpenDataSchema();
  const client = await pool.connect();
  try {
    await client.query("begin");
    await client.query(`
      create temp table tmp_vehicle_plate_import (
        plate text not null,
        vin text,
        pcv text,
        source text not null,
        confidence numeric(3, 2) not null,
        expires_at timestamptz
      ) on commit drop
    `);

    for (let index = 0; index < rows.valid.length; index += INSERT_CHUNK_SIZE) {
      await insertImportChunk(client, rows.valid.slice(index, index + INSERT_CHUNK_SIZE));
    }

	    const stats = await applyImportedPlateRows(client);
	    await refreshVehiclePlateSummaries(client);
	    const coverage = await assertRequiredIcoCoverage(client, requiredIcos, { allowMissingStk });
    await client.query("commit");
    console.log(JSON.stringify({
      file: filePath,
      dryRun: false,
      validRows: rows.valid.length,
      invalidRows: rows.invalid.length,
      invalidSamples: rows.invalid.slice(0, 10),
      requiredIcos,
      coverage,
      ...stats
    }, null, 2));
  } catch (error) {
    await client.query("rollback").catch(() => {});
    throw error;
  } finally {
    client.release();
    await closeDatabasePool();
  }
}

async function insertImportChunk(client, rows) {
  const values = [];
  const placeholders = rows.map((row, index) => {
    const offset = index * 6;
    values.push(row.plate, row.vin, row.pcv, row.source, row.confidence, row.expiresAt);
    return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6})`;
  });

  await client.query(
    `
      insert into tmp_vehicle_plate_import (
        plate, vin, pcv, source, confidence, expires_at
      )
      values ${placeholders.join(",\n")}
    `,
    values
  );
}

async function applyImportedPlateRows(client) {
  const result = await client.query(`
    with normalized as (
      select distinct on (plate, coalesce(pcv, ''), coalesce(vin, ''))
        plate,
        vin,
        pcv,
        source,
        confidence,
        expires_at
      from tmp_vehicle_plate_import
      where plate is not null
        and (pcv is not null or vin is not null)
      order by plate, coalesce(pcv, ''), coalesce(vin, ''), confidence desc
    ),
    resolved as (
      select
        n.plate,
        coalesce(n.vin, v.vin, vv.vin) as vin,
        coalesce(v.pcv, vv.pcv, n.pcv) as pcv,
        case
          when v.pcv is not null or vv.pcv is not null then 'pcv'
          else 'vin'
        end as vehicle_key_type,
        coalesce(v.pcv, vv.pcv, n.vin) as vehicle_key,
        n.source,
        n.confidence,
        n.expires_at,
        (v.pcv is not null or vv.pcv is not null or (n.vin is not null and n.pcv is null)) as linkable
      from normalized n
      left join vehicles v on n.pcv is not null and v.pcv = n.pcv
	      left join vehicle_vins vv
	        on n.vin is not null
	        and upper(regexp_replace(coalesce(vv.vin, ''), '[^A-Za-z0-9]', '', 'g')) = n.vin
    ),
    valid_links as (
      select *
      from resolved
      where linkable is true
        and vehicle_key is not null
    ),
    inserted_links as (
      insert into vehicle_plate_links (
        plate, vin, pcv, vehicle_key_type, vehicle_key, source, confidence,
        first_seen_at, last_seen_at, expires_at, updated_at
      )
      select
        plate, vin, pcv, vehicle_key_type, vehicle_key, source, confidence,
        now(), now(), expires_at, now()
      from valid_links
      on conflict (vehicle_key_type, vehicle_key, plate) do update set
        vin = coalesce(excluded.vin, vehicle_plate_links.vin),
        pcv = coalesce(excluded.pcv, vehicle_plate_links.pcv),
        source = excluded.source,
        confidence = greatest(excluded.confidence, vehicle_plate_links.confidence),
        last_seen_at = excluded.last_seen_at,
        expires_at = excluded.expires_at,
        updated_at = now()
      returning 1
    ),
    inserted_legacy as (
      insert into plate_resolutions (
        plate, vin, pcv, source, confidence, resolved_at, expires_at, updated_at
      )
      select distinct on (plate)
        plate, vin, pcv, source, confidence, now(), expires_at, now()
      from valid_links
      order by plate, confidence desc, pcv nulls last, vin nulls last
      on conflict (plate) do update set
        vin = coalesce(excluded.vin, plate_resolutions.vin),
        pcv = coalesce(excluded.pcv, plate_resolutions.pcv),
        source = excluded.source,
        confidence = excluded.confidence,
        resolved_at = excluded.resolved_at,
        expires_at = excluded.expires_at,
        updated_at = excluded.updated_at
      returning 1
    )
    select
      (select count(*)::int from normalized) as normalized_rows,
      (select count(*)::int from valid_links) as linkable_rows,
      (select count(*)::int from resolved where linkable is false) as skipped_rows,
      (select count(*)::int from inserted_links) as plate_links_upserted,
      (select count(*)::int from inserted_legacy) as legacy_resolutions_upserted
  `);

  return result.rows[0] || {};
}

async function assertRequiredIcoCoverage(client, requiredIcos, options = {}) {
  if (!Array.isArray(requiredIcos) || requiredIcos.length === 0) {
    return [];
  }

  const coverage = [];
  const failures = [];
  for (const ico of requiredIcos) {
    const row = await getIcoFleetCoverage(client, ico);
    coverage.push(row);
    if (Number(row.missing_plate_count || 0) > 0) {
      failures.push(`${ico}: ${row.missing_plate_count}/${row.vehicle_count} vehicles missing SPZ`);
    }
    if (!options.allowMissingStk && Number(row.missing_stk_count || 0) > 0) {
      failures.push(`${ico}: ${row.missing_stk_count}/${row.vehicle_count} vehicles missing STK`);
    }
  }

  if (failures.length > 0) {
    throw new Error(`Coverage check failed after plate import: ${failures.join("; ")}`);
  }

  return coverage;
}

async function getIcoFleetCoverage(client, ico) {
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
      active_pcvs as (
        select distinct pcv
        from ownership_relations, company_names
        where (
            ico = $1
            or (
              ico is null
              and lower(name) = any(company_names.names)
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
      coverage as (
        select
          p.pcv,
          (
            exists (
              select 1
              from supplemental_ownership_relations s
              where s.ico = $1
                and s.pcv = p.pcv
                and s.current is true
                and s.date_to is null
                and s.plate is not null
                and length(btrim(s.plate)) > 0
            )
            or (vf.plate is not null and length(btrim(vf.plate)) > 0)
          ) as has_plate,
          (vf.inspection_performed_on is not null and vf.inspection_valid_until is not null) as has_stk
        from active_pcvs p
        left join vehicle_fleet_facts vf on vf.pcv = p.pcv
      )
      select
        $1 as ico,
        count(*)::int as vehicle_count,
        count(*) filter (where has_plate)::int as plate_count,
        count(*) filter (where not has_plate)::int as missing_plate_count,
        count(*) filter (where has_stk)::int as stk_count,
        count(*) filter (where not has_stk)::int as missing_stk_count
      from coverage
    `,
    [ico]
  );

  return result.rows[0] || {
    ico,
    vehicle_count: 0,
    plate_count: 0,
    missing_plate_count: 0,
    stk_count: 0,
    missing_stk_count: 0
  };
}

async function readPlateRows(filePath, options) {
  const valid = [];
  const invalid = [];
  const extension = path.extname(filePath).toLowerCase();
  if (extension === ".json") {
    const parsed = JSON.parse(await fs.promises.readFile(filePath, "utf8"));
    const entries = Array.isArray(parsed) ? parsed : Array.isArray(parsed.rows) ? parsed.rows : [];
    entries.forEach((entry, index) => collectRow(entry, index + 1, options, valid, invalid));
    return { valid, invalid };
  }

  if (extension === ".jsonl" || extension === ".ndjson") {
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath, { encoding: "utf8" }),
      crlfDelay: Infinity
    });
    let lineNumber = 0;
    for await (const line of rl) {
      lineNumber += 1;
      const trimmed = line.trim();
      if (!trimmed) {
        continue;
      }
      try {
        collectRow(JSON.parse(trimmed), lineNumber, options, valid, invalid);
      } catch (error) {
        invalid.push({ line: lineNumber, reason: "invalid_json" });
      }
    }
    return { valid, invalid };
  }

  return await readCsvPlateRows(filePath, options);
}

async function readCsvPlateRows(filePath, options) {
  const valid = [];
  const invalid = [];
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity
  });
  let headers = null;
  let lineNumber = 0;
  for await (const line of rl) {
    lineNumber += 1;
    if (!line.trim()) {
      continue;
    }

    const delimiter = detectDelimiter(line);
    const cells = parseDelimitedLine(line, delimiter);
    if (!headers) {
      headers = cells.map(normalizeHeader);
      continue;
    }

    const row = {};
    headers.forEach((header, index) => {
      if (header) {
        row[header] = cells[index] || "";
      }
    });
    collectRow(row, lineNumber, options, valid, invalid);
  }

  return { valid, invalid };
}

function collectRow(raw, line, options, valid, invalid) {
  const plate = normalizePlate(firstValue(raw, [
    "plate",
    "spz",
    "rz",
    "registrationplate",
    "registrationplatenumber",
    "regznacka",
    "registračníznačka",
    "registracniznacka"
  ]));
  const pcv = normalizeText(firstValue(raw, ["pcv", "pčv", "pcvvozidla", "poradovecislovozidla"]));
  const vin = normalizeVin(firstValue(raw, ["vin", "vinvozidla"]));

  if (!plate || (!pcv && !vin)) {
    invalid.push({
      line,
      reason: !plate ? "missing_or_invalid_plate" : "missing_vehicle_identifier",
      plate: plate || null,
      pcv: pcv || null,
      vin: vin || null
    });
    return;
  }

  valid.push({
    plate,
    pcv: pcv || null,
    vin: vin || null,
    source: options.source,
    confidence: options.confidence,
    expiresAt: options.ttlDays > 0
      ? new Date(Date.now() + options.ttlDays * 24 * 60 * 60 * 1000).toISOString()
      : null
  });
}

function firstValue(row, keys) {
  if (!row || typeof row !== "object") {
    return "";
  }

  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(row, key) && row[key] !== undefined && row[key] !== null && row[key] !== "") {
      return row[key];
    }
  }

  const normalizedEntries = new Map(Object.entries(row).map(([key, value]) => [normalizeHeader(key), value]));
  for (const key of keys) {
    const normalized = normalizeHeader(key);
    if (normalizedEntries.has(normalized)) {
      const value = normalizedEntries.get(normalized);
      if (value !== undefined && value !== null && value !== "") {
        return value;
      }
    }
  }

  return "";
}

function parseDelimitedLine(line, delimiter) {
  const cells = [];
  let current = "";
  let quoted = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (char === "\"") {
      if (quoted && line[index + 1] === "\"") {
        current += "\"";
        index += 1;
      } else {
        quoted = !quoted;
      }
      continue;
    }

    if (char === delimiter && !quoted) {
      cells.push(current.trim());
      current = "";
      continue;
    }

    current += char;
  }
  cells.push(current.trim());
  return cells;
}

function detectDelimiter(line) {
  const candidates = [",", ";", "\t"];
  return candidates
    .map((delimiter) => ({ delimiter, count: line.split(delimiter).length }))
    .sort((left, right) => right.count - left.count)[0].delimiter;
}

function resolveInputFile() {
  const raw = getArg("--file") || process.env.PLATE_IMPORT_FILE;
  if (!raw) {
    throw new Error("Pouziti: npm run db:import-plates -- --file cesta.csv [--source zdroj]");
  }
  const filePath = path.resolve(raw);
  if (!fs.existsSync(filePath)) {
    throw new Error(`Soubor neexistuje: ${filePath}`);
  }
  return filePath;
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

function hasFlag(name) {
  return process.argv.includes(name);
}

function parseIcoList(value) {
  return Array.from(
    new Set(
      String(value || "")
        .split(/[,\s;]+/)
        .map((item) => item.replace(/\D/g, ""))
        .filter((item) => /^\d{8}$/.test(item))
    )
  );
}

function parseBoolean(value) {
  return ["1", "true", "yes", "on"].includes(String(value || "").trim().toLowerCase());
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

function normalizeHeader(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "");
}

function normalizeText(value) {
  return String(value || "").trim().replace(/\s+/g, " ");
}

function normalizeVin(value) {
  const normalized = normalizeText(value).toUpperCase().replace(/[^A-Z0-9]/g, "");
  return /^[A-HJ-NPR-Z0-9]{5,17}$/.test(normalized) ? normalized : "";
}

function normalizePlate(value) {
  const normalized = normalizeText(value).toUpperCase().replace(/[^A-Z0-9]/g, "");
  return /^[A-Z0-9]{5,10}$/.test(normalized) ? normalized : "";
}

function normalizeConfidence(value, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.min(1, Math.max(0.01, parsed));
}

function normalizeNonNegativeInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback;
  }
  return parsed;
}
