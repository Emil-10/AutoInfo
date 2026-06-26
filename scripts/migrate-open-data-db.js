#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

loadEnvFile(path.join(__dirname, "..", ".env"));

const {
  closeDatabasePool,
  ensureOpenDataSchema,
  refreshCompanyVehicleFacts,
  refreshVehicleFleetFacts,
  refreshVehicleInspectionSummaries,
  refreshVehiclePlateSummaries
} = require("../open-data-db");

main().catch((error) => {
  console.error("[db:migrate] failed");
  console.error(error && error.stack ? error.stack : String(error));
  process.exitCode = 1;
});

async function main() {
  const refreshPlates = parseBoolean(process.env.MIGRATE_REFRESH_PLATES, true);
  const refreshInspections = parseBoolean(process.env.MIGRATE_REFRESH_INSPECTIONS, false);
  const refreshFleetFacts = parseBoolean(process.env.MIGRATE_REFRESH_FLEET_FACTS, true);
  const refreshCompanyFacts = parseBoolean(process.env.MIGRATE_REFRESH_COMPANY_FACTS, true);

  await ensureOpenDataSchema();
  if (refreshPlates) {
    await refreshVehiclePlateSummaries();
  }
  if (refreshInspections) {
    await refreshVehicleInspectionSummaries();
  }
  if (!refreshPlates && !refreshInspections && refreshFleetFacts) {
    await refreshVehicleFleetFacts();
  }
  if (!refreshPlates && !refreshInspections && refreshCompanyFacts) {
    await refreshCompanyVehicleFacts();
  }
  await closeDatabasePool();
  console.log(`[db:migrate] schema ready (plates=${refreshPlates ? "refreshed" : "skipped"}, inspections=${refreshInspections ? "refreshed" : "skipped"}, fleetFacts=${refreshFleetFacts ? "refreshed" : "skipped"}, companyFacts=${refreshCompanyFacts ? "refreshed" : "skipped"})`);
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
