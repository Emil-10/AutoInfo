#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const ROOT_DIR = path.join(__dirname, "..");
loadEnvFile(path.join(ROOT_DIR, ".env"));

const OPEN_DATA_DIR = path.resolve(process.env.OPEN_DATA_PERSIST_DIR || path.join(ROOT_DIR, ".cache", "open-data"));

main().catch((error) => {
  console.error("[audit:storage] failed");
  console.error(error && error.stack ? error.stack : String(error));
  process.exitCode = 1;
});

async function main() {
  const storage = await auditStorage();
  const database = await auditDatabase();
  console.log(JSON.stringify({ storage, database }, null, 2));
}

async function auditStorage() {
  const root = await measurePath(OPEN_DATA_DIR);
  const fleetDb = await measurePath(path.join(OPEN_DATA_DIR, "fleet-db"));
  const topFiles = await listTopFiles(OPEN_DATA_DIR, 30);
  const fleetDbGroups = await measureFleetDbGroups();

  return {
    openDataDir: OPEN_DATA_DIR,
    totalBytes: root.bytes,
    totalGb: toGb(root.bytes),
    fleetDbBytes: fleetDb.bytes,
    fleetDbGb: toGb(fleetDb.bytes),
    fleetDbGroups,
    topFiles
  };
}

async function auditDatabase() {
  const openDataDb = require("../open-data-db");
  if (!openDataDb.isDatabaseConfigured()) {
    return {
      configured: false
    };
  }

  const pool = openDataDb.getPool();
  const client = await pool.connect();
  try {
    const [tables, indexes, totals] = await Promise.all([
      client.query(`
        select
          schemaname,
          relname as table,
          pg_total_relation_size(format('%I.%I', schemaname, relname))::bigint as total_bytes,
          pg_relation_size(format('%I.%I', schemaname, relname))::bigint as table_bytes,
          pg_indexes_size(format('%I.%I', schemaname, relname))::bigint as index_bytes,
          coalesce(n_live_tup, 0)::bigint as estimated_rows
        from pg_stat_user_tables
        order by pg_total_relation_size(format('%I.%I', schemaname, relname)) desc
        limit 30
      `),
      client.query(`
        select
          schemaname,
          indexrelname as index,
          relname as table,
          pg_relation_size(indexrelid)::bigint as bytes,
          idx_scan::bigint as scans
        from pg_stat_user_indexes
        order by pg_relation_size(indexrelid) desc
        limit 30
      `),
      client.query(`
        select
          pg_database_size(current_database())::bigint as database_bytes,
          current_database() as database_name
      `)
    ]);

    return {
      configured: true,
      databaseName: totals.rows[0]?.database_name || null,
      databaseBytes: Number(totals.rows[0]?.database_bytes || 0),
      databaseGb: toGb(Number(totals.rows[0]?.database_bytes || 0)),
      largestTables: tables.rows.map((row) => ({
        ...row,
        totalGb: toGb(row.total_bytes),
        tableGb: toGb(row.table_bytes),
        indexGb: toGb(row.index_bytes)
      })),
      largestIndexes: indexes.rows.map((row) => ({
        ...row,
        gb: toGb(row.bytes)
      }))
    };
  } finally {
    client.release();
    await openDataDb.closeDatabasePool();
  }
}

async function measureFleetDbGroups() {
  const fleetDir = path.join(OPEN_DATA_DIR, "fleet-db");
  const entries = await fs.promises.readdir(fleetDir, { withFileTypes: true }).catch(() => []);
  const groups = [];
  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }
    const measured = await measurePath(path.join(fleetDir, entry.name));
    groups.push({
      name: entry.name,
      bytes: measured.bytes,
      gb: toGb(measured.bytes),
      files: measured.files
    });
  }
  return groups.sort((left, right) => right.bytes - left.bytes);
}

async function measurePath(targetPath) {
  const stats = await fs.promises.stat(targetPath).catch(() => null);
  if (!stats) {
    return { bytes: 0, files: 0 };
  }
  if (stats.isFile()) {
    return { bytes: stats.size, files: 1 };
  }

  let bytes = 0;
  let files = 0;
  const entries = await fs.promises.readdir(targetPath, { withFileTypes: true }).catch(() => []);
  for (const entry of entries) {
    const child = await measurePath(path.join(targetPath, entry.name));
    bytes += child.bytes;
    files += child.files;
  }
  return { bytes, files };
}

async function listTopFiles(targetPath, limit) {
  const files = [];
  await walkFiles(targetPath, async (filePath, stats) => {
    files.push({
      path: path.relative(ROOT_DIR, filePath),
      bytes: stats.size,
      gb: toGb(stats.size),
      modifiedAt: stats.mtime.toISOString()
    });
  });
  return files.sort((left, right) => right.bytes - left.bytes).slice(0, limit);
}

async function walkFiles(targetPath, onFile) {
  const entries = await fs.promises.readdir(targetPath, { withFileTypes: true }).catch(() => []);
  for (const entry of entries) {
    const filePath = path.join(targetPath, entry.name);
    const stats = await fs.promises.stat(filePath).catch(() => null);
    if (!stats) {
      continue;
    }
    if (entry.isDirectory()) {
      await walkFiles(filePath, onFile);
    } else if (entry.isFile()) {
      await onFile(filePath, stats);
    }
  }
}

function toGb(bytes) {
  return Number((Number(bytes || 0) / 1024 / 1024 / 1024).toFixed(2));
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
