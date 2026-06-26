#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const { Client } = require("pg");

const ROOT_DIR = path.join(__dirname, "..");
loadEnvFile(path.join(ROOT_DIR, ".env"));

const DATABASE_URL = process.env.DATABASE_URL;

main().catch((error) => {
  console.error("[open-data-name-indexes] failed");
  console.error(error && error.stack ? error.stack : String(error));
  process.exitCode = 1;
});

async function main() {
  if (!DATABASE_URL) {
    throw new Error("DATABASE_URL neni nastaveny.");
  }

  const client = new Client({ connectionString: DATABASE_URL });
  await client.connect();
  try {
    await client.query("set statement_timeout = 0");
    await client.query("set lock_timeout = 0");
    await client.query(`set maintenance_work_mem = '${escapeSqlLiteral(process.env.OPEN_DATA_IMPORT_MAINTENANCE_WORK_MEM || "512MB")}'`);

    await createIndex(
      client,
      "ownership_relations_missing_ico_name_current_relation_idx",
      `
        create index concurrently if not exists ownership_relations_missing_ico_name_current_relation_idx
          on ownership_relations (lower(name), pcv)
          where ico is null
            and name is not null
            and current is true
            and date_to is null
            and relation in ('Vlastnik', 'Provozovatel')
      `
    );

    await createIndex(
      client,
      "ownership_relations_missing_ico_name_history_idx",
      `
        create index concurrently if not exists ownership_relations_missing_ico_name_history_idx
          on ownership_relations (lower(name), date_from desc, pcv)
          where ico is null
            and name is not null
            and relation in ('Vlastnik', 'Provozovatel')
      `
    );

    await dropIndex(client, "ownership_relations_name_current_relation_idx");
    await dropIndex(client, "ownership_relations_name_history_idx");

    console.log("[open-data-name-indexes] analyzing ownership_relations");
    await client.query("analyze ownership_relations");
    console.log("[open-data-name-indexes] done");
  } finally {
    await client.end();
  }
}

async function createIndex(client, name, sql) {
  console.log(`[open-data-name-indexes] creating ${name}`);
  const started = Date.now();
  await client.query(sql);
  const seconds = ((Date.now() - started) / 1000).toFixed(1);
  console.log(`[open-data-name-indexes] created ${name} in ${seconds}s`);
}

async function dropIndex(client, name) {
  console.log(`[open-data-name-indexes] dropping obsolete ${name}`);
  const started = Date.now();
  await client.query(`drop index concurrently if exists ${name}`);
  const seconds = ((Date.now() - started) / 1000).toFixed(1);
  console.log(`[open-data-name-indexes] dropped obsolete ${name} in ${seconds}s`);
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
