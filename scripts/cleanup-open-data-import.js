#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const ROOT_DIR = path.join(__dirname, "..");
loadEnvFile(path.join(ROOT_DIR, ".env"));

const {
  closeDatabasePool,
  ensureOpenDataSchema,
  getPool
} = require("../open-data-db");

main().catch(async (error) => {
  console.error("[open-data-cleanup] failed");
  console.error(error && error.stack ? error.stack : String(error));
  await closeDatabasePool().catch(() => {});
  process.exitCode = 1;
});

async function main() {
  const pool = getPool();
  if (!pool) {
    throw new Error("DATABASE_URL neni nastaveny.");
  }

  await ensureOpenDataSchema();
  await pool.query(`
    truncate table
	      ownership_relations_staging,
	      vehicles_staging,
	      vehicle_vins_staging,
	      inspections_staging,
	      vehicle_imports_staging,
	      vehicle_deregistrations_staging,
	      open_data_aux_records_staging
	  `);
  const result = await pool.query(`
    update dataset_versions
    set
      status = 'failed',
      error = 'Import stopped before completion; staging tables were cleaned up.',
      import_finished_at = now(),
      updated_at = now()
    where status = 'importing'
    returning source, filename
  `);

  console.log(`[open-data-cleanup] staging tables truncated; importsReset=${result.rowCount}`);
  await closeDatabasePool();
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
