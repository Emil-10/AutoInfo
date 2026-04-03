#!/usr/bin/env node

const fs = require("fs");
const https = require("https");
const path = require("path");
const readline = require("readline");

const ROOT_DIR = path.join(__dirname, "..");
const OPEN_DATA_DIR = path.join(ROOT_DIR, ".cache", "open-data");
const FLEET_DB_DIR = path.join(OPEN_DATA_DIR, "fleet-db");
const FLEET_DB_TMP_DIR = path.join(OPEN_DATA_DIR, "fleet-db-tmp");
const OWNER_DIR = path.join(FLEET_DB_TMP_DIR, "owners");
const META_FILE = path.join(FLEET_DB_TMP_DIR, "meta.json");
const OWNER_ROUTE = "/vypiszregistru/vlastnikprovozovatelvozidla";

main().catch((error) => {
  console.error("[fleet-db] build failed");
  console.error(error && error.stack ? error.stack : String(error));
  process.exitCode = 1;
});

async function main() {
  const token = await getRenToken();
  const ownerMeta = await fetchDatasetMetadata(OWNER_ROUTE, token);

  const currentMeta = await readJson(path.join(FLEET_DB_DIR, "meta.json"));
  if (
    currentMeta?.ready &&
    currentMeta.ownerFilename === ownerMeta.filename
  ) {
    console.log("[fleet-db] up to date, skipping rebuild");
    return;
  }

  await fs.promises.rm(FLEET_DB_TMP_DIR, { recursive: true, force: true });
  await fs.promises.mkdir(OWNER_DIR, { recursive: true });

  console.log("[fleet-db] building owner shards");
  await buildShardSet({
    route: OWNER_ROUTE,
    token,
    outputDir: OWNER_DIR,
    metadata: ownerMeta,
    normalizeRecord: normalizeOwnerRecord,
    progressLabel: "owners"
  });

  await writeJson(META_FILE, {
    ready: true,
    builtAt: new Date().toISOString(),
    ownerFilename: ownerMeta.filename,
    ownerDatasetDate: ownerMeta.datasetDate || null,
    vehicleFilename: null,
    vehicleDatasetDate: null
  });

  await fs.promises.rm(FLEET_DB_DIR, { recursive: true, force: true });
  await fs.promises.rename(FLEET_DB_TMP_DIR, FLEET_DB_DIR);
  console.log("[fleet-db] build complete");
}

async function buildShardSet({ route, token, outputDir, metadata, normalizeRecord, progressLabel }) {
  const buffers = new Map();
  let processed = 0;
  let written = 0;

  try {
    await scanRemoteCsv(route, token, async ({ values, headerMap }) => {
      processed += 1;
      const record = normalizeRecord(values, headerMap);
      if (record) {
        const shard = getShardKey(record.shardKey);
        await appendShardLine(buffers, path.join(outputDir, `${shard}.jsonl`), JSON.stringify(record.payload));
        written += 1;
      }

      if (processed % 100000 === 0) {
        console.log(`[fleet-db] ${progressLabel} processed=${processed} written=${written}`);
      }
    });
  } finally {
    await flushAllShardBuffers(buffers);
  }

  console.log(`[fleet-db] ${progressLabel} done processed=${processed} written=${written} file=${metadata.filename}`);
}

function normalizeOwnerRecord(values, headerMap) {
  const ico = sanitizeIco(values[headerMap.ICO]);
  const pcv = normalizeWhitespace(values[headerMap.PCV]);
  const current = normalizeBoolean(values[headerMap.AKTUALNI]);

  if (!ico || !pcv || current !== true) {
    return null;
  }

  return {
    shardKey: ico,
    payload: {
      ico,
      pcv,
      relation: normalizeWhitespace(values[headerMap.VZTAHKVOZIDLU]) || "Subjekt",
      subjectType: normalizeWhitespace(values[headerMap.TYPSUBJEKTU]) || null,
      current: true,
      name: normalizeWhitespace(values[headerMap.NAZEV]),
      address: normalizeWhitespace(values[headerMap.ADRESA]),
      dateFrom: normalizeOpenDataDate(values[headerMap.DATUMOD]),
      dateTo: normalizeOpenDataDate(values[headerMap.DATUMDO])
    }
  };
}

async function scanRemoteCsv(route, token, onRow) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        protocol: "https:",
        hostname: "download.dataovozidlech.cz",
        path: route,
        method: "GET",
        headers: {
          Accept: "text/csv",
          "User-Agent": "Mozilla/5.0",
          _ren: token
        }
      },
      (res) => {
        if ((res.statusCode || 500) >= 400) {
          reject(new Error(`Fleet DB source returned ${res.statusCode || 500} for ${route}.`));
          res.resume();
          return;
        }

        (async () => {
          const reader = readline.createInterface({ input: res, crlfDelay: Infinity });
          let headerMap = null;

          for await (const rawLine of reader) {
            const line = headerMap ? rawLine : rawLine.replace(/^\uFEFF/, "");
            if (!headerMap) {
              const headers = parseCsvLine(line);
              headerMap = headers.reduce((accumulator, header, index) => {
                accumulator[canonicalizeCsvHeader(header)] = index;
                return accumulator;
              }, Object.create(null));
              continue;
            }

            if (!line) {
              continue;
            }

            const values = parseCsvLine(line);
            await onRow({ values, headerMap });
          }

          resolve();
        })().catch(reject);
      }
    );

    req.on("error", reject);
    req.end();
  });
}

async function appendShardLine(buffers, filePath, line) {
  const current = buffers.get(filePath) || "";
  const next = `${current}${line}\n`;
  if (next.length < 1024 * 1024) {
    buffers.set(filePath, next);
    return;
  }

  await fs.promises.appendFile(filePath, next, "utf8");
  buffers.set(filePath, "");
}

async function flushAllShardBuffers(buffers) {
  for (const [filePath, buffer] of buffers.entries()) {
    if (!buffer) {
      continue;
    }
    await fs.promises.appendFile(filePath, buffer, "utf8");
  }
}

async function getRenToken() {
  return await new Promise((resolve, reject) => {
    const req = https.request(
      {
        protocol: "https:",
        hostname: "download.dataovozidlech.cz",
        path: "/",
        method: "GET",
        headers: {
          Accept: "text/html",
          "User-Agent": "Mozilla/5.0"
        }
      },
      (res) => {
        const token = normalizeWhitespace(res.headers["_ren"]);
        res.resume();
        token ? resolve(token) : reject(new Error("Could not obtain _ren token."));
      }
    );

    req.on("error", reject);
    req.end();
  });
}

async function fetchDatasetMetadata(route, token) {
  return await new Promise((resolve, reject) => {
    const req = https.request(
      {
        protocol: "https:",
        hostname: "download.dataovozidlech.cz",
        path: route,
        method: "HEAD",
        headers: {
          Accept: "text/csv",
          "User-Agent": "Mozilla/5.0",
          _ren: token
        }
      },
      (res) => {
        if ((res.statusCode || 500) >= 400) {
          reject(new Error(`Fleet DB metadata returned ${res.statusCode || 500} for ${route}.`));
          res.resume();
          return;
        }

        const header = res.headers["content-disposition"];
        resolve({
          filename: parseContentDispositionFilename(header),
          datasetDate: parseDatasetDateFromFilename(header)
        });
        res.resume();
      }
    );

    req.on("error", reject);
    req.end();
  });
}

function parseCsvLine(line) {
  const values = [];
  let current = "";
  let insideQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];

    if (char === "\"") {
      if (insideQuotes && next === "\"") {
        current += "\"";
        index += 1;
      } else {
        insideQuotes = !insideQuotes;
      }
    } else if (char === "," && !insideQuotes) {
      values.push(current);
      current = "";
    } else {
      current += char;
    }
  }

  values.push(current);
  return values;
}

function canonicalizeCsvHeader(value) {
  return String(value || "")
    .replace(/^\uFEFF/, "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^A-Za-z0-9]/g, "")
    .toUpperCase();
}

function parseContentDispositionFilename(header) {
  const value = normalizeWhitespace(header);
  const match = value.match(/filename=([^;]+)/i);
  return match ? match[1].replace(/"/g, "") : null;
}

function parseDatasetDateFromFilename(header) {
  const filename = parseContentDispositionFilename(header);
  if (!filename) {
    return null;
  }

  const match = filename.match(/(\d{4})(\d{2})(\d{2})/);
  return match ? `${match[1]}-${match[2]}-${match[3]}` : null;
}

function normalizeOpenDataDate(value) {
  const normalized = normalizeWhitespace(value);
  if (!normalized) {
    return null;
  }

  const parsed = new Date(normalized);
  return Number.isNaN(parsed.getTime()) ? normalized : parsed.toISOString();
}

function normalizeBoolean(value) {
  const normalized = normalizeWhitespace(value).toLowerCase();
  if (!normalized) {
    return null;
  }

  if (["true", "1", "ano"].includes(normalized)) {
    return true;
  }

  if (["false", "0", "ne"].includes(normalized)) {
    return false;
  }

  return null;
}

function sanitizeIco(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits.length === 8 ? digits : null;
}

function firstNonEmpty(values) {
  for (const value of values) {
    if (value === 0) {
      return value;
    }
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
    if (value !== null && value !== undefined && value !== "") {
      return value;
    }
  }
  return null;
}

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function getShardKey(value) {
  const normalized = String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

  return normalized ? normalized.slice(0, 2).padEnd(2, "_") : "__";
}

async function readJson(filePath) {
  try {
    return JSON.parse(await fs.promises.readFile(filePath, "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

async function writeJson(filePath, value) {
  await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
  await fs.promises.writeFile(filePath, JSON.stringify(value, null, 2), "utf8");
}
