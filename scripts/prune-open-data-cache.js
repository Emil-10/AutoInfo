#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const { pipeline } = require("stream/promises");

const ROOT_DIR = path.join(__dirname, "..");
loadEnvFile(path.join(ROOT_DIR, ".env"));

const OPEN_DATA_DIR = path.resolve(process.env.OPEN_DATA_PERSIST_DIR || path.join(ROOT_DIR, ".cache", "open-data"));
const APPLY = process.argv.includes("--apply");
const COMPRESS = !process.argv.includes("--no-compress");
const KEEP = Math.max(1, Number(getArg("--keep") || process.env.OPEN_DATA_PRUNE_KEEP || 1) || 1);

const DATASET_PATTERNS = [
  { key: "vehicles", pattern: /^RSV_vypis_vozidel_(\d{8})\.csv(?:\.gz)?$/i },
  { key: "ownership", pattern: /^RSV_vlastnik_provozovatel_vozidla_(\d{8})\.csv(?:\.gz)?$/i },
  { key: "inspections", pattern: /^RSV_technicke_prohlidky_(\d{8})\.csv(?:\.gz)?$/i },
  { key: "imports", pattern: /^RSV_vozidla_dovoz_(\d{8})\.csv(?:\.gz)?$/i },
  { key: "deregistered", pattern: /^RSV_vozidla_vyrazena_z_provozu_(\d{8})\.csv(?:\.gz)?$/i },
  { key: "equipment", pattern: /^RSV_.*dopln.*vybav.*(\d{8})\.csv(?:\.gz)?$/i },
  { key: "manufacturer_reports", pattern: /^RSV_.*zprav.*vyrobc.*(\d{8})\.csv(?:\.gz)?$/i }
];

main().catch((error) => {
  console.error("[open-data-prune] failed");
  console.error(error && error.stack ? error.stack : String(error));
  process.exitCode = 1;
});

async function main() {
  const before = await measurePath(OPEN_DATA_DIR);
  const actions = [];
  const entries = await fs.promises.readdir(OPEN_DATA_DIR, { withFileTypes: true }).catch(() => []);
  const files = [];

  for (const entry of entries) {
    if (!entry.isFile()) {
      continue;
    }
    const filePath = path.join(OPEN_DATA_DIR, entry.name);
    const stats = await fs.promises.stat(filePath).catch(() => null);
    if (stats) {
      files.push({ name: entry.name, path: filePath, stats });
    }
  }

  for (const file of files) {
    if (/\.tmp$/i.test(file.name)) {
      actions.push(await deleteFile(file, "delete-tmp"));
    }
  }

  for (const pattern of DATASET_PATTERNS) {
    const matches = files
      .map((file) => ({ ...file, match: file.name.match(pattern.pattern) }))
      .filter((file) => file.match && !/\.tmp$/i.test(file.name))
      .sort((left, right) => {
        const dateCompare = String(right.match[1]).localeCompare(String(left.match[1]));
        return dateCompare || right.stats.mtimeMs - left.stats.mtimeMs;
      });

    const keepers = new Set(matches.slice(0, KEEP).map((file) => file.path));
    for (const file of matches) {
      if (!keepers.has(file.path)) {
        actions.push(await deleteFile(file, `delete-old-${pattern.key}`));
        continue;
      }

      if (COMPRESS && /\.csv$/i.test(file.name)) {
        actions.push(await gzipCsv(file));
      }
    }
  }

  const after = APPLY ? await measurePath(OPEN_DATA_DIR) : before;
  console.log(JSON.stringify({
    openDataDir: OPEN_DATA_DIR,
    dryRun: !APPLY,
    keepPerDataset: KEEP,
    compress: COMPRESS,
    beforeBytes: before.bytes,
    afterBytes: after.bytes,
    reclaimedBytes: Math.max(0, before.bytes - after.bytes),
    actions
  }, null, 2));
}

async function deleteFile(file, action) {
  if (APPLY) {
    await fs.promises.rm(file.path, { force: true });
  }
  return {
    action,
    path: path.relative(ROOT_DIR, file.path),
    bytes: file.stats.size,
    applied: APPLY
  };
}

async function gzipCsv(file) {
  const gzipPath = `${file.path}.gz`;
  const existing = await fs.promises.stat(gzipPath).catch(() => null);
  if (existing?.isFile()) {
    return {
      action: "skip-compress-existing",
      path: path.relative(ROOT_DIR, file.path),
      gzipPath: path.relative(ROOT_DIR, gzipPath),
      bytes: file.stats.size,
      applied: false
    };
  }

  if (APPLY) {
    await pipeline(
      fs.createReadStream(file.path),
      zlib.createGzip({ level: 9 }),
      fs.createWriteStream(gzipPath)
    );
    await fs.promises.rm(file.path, { force: true });
  }

  return {
    action: "compress-csv",
    path: path.relative(ROOT_DIR, file.path),
    gzipPath: path.relative(ROOT_DIR, gzipPath),
    bytes: file.stats.size,
    applied: APPLY
  };
}

async function measurePath(targetPath) {
  const stats = await fs.promises.stat(targetPath).catch(() => null);
  if (!stats) {
    return { bytes: 0 };
  }
  if (stats.isFile()) {
    return { bytes: stats.size };
  }

  let bytes = 0;
  const entries = await fs.promises.readdir(targetPath, { withFileTypes: true }).catch(() => []);
  for (const entry of entries) {
    const child = await measurePath(path.join(targetPath, entry.name));
    bytes += child.bytes;
  }
  return { bytes };
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
