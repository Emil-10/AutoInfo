#!/usr/bin/env node

const fs = require("fs");
const https = require("https");
const path = require("path");
const readline = require("readline");
const zlib = require("zlib");

const ROOT_DIR = path.join(__dirname, "..");
const OPEN_DATA_DIR = path.join(ROOT_DIR, ".cache", "open-data");
const FLEET_DB_DIR = path.join(OPEN_DATA_DIR, "fleet-db");
const FLEET_DB_TMP_DIR = path.join(OPEN_DATA_DIR, "fleet-db-tmp");
const OWNER_DIR = path.join(FLEET_DB_TMP_DIR, "owners");
const OWNER_NAME_DIR = path.join(FLEET_DB_TMP_DIR, "owner-names");
const OWNERSHIP_PCV_DIR = path.join(FLEET_DB_TMP_DIR, "ownership-pcv");
const VEHICLE_DIR = path.join(FLEET_DB_TMP_DIR, "vehicles");
const VIN_PCV_DIR = path.join(FLEET_DB_TMP_DIR, "vin-pcv");
const META_FILE = path.join(FLEET_DB_TMP_DIR, "meta.json");
const OWNER_ROUTE = "/vypiszregistru/vlastnikprovozovatelvozidla";
const VEHICLE_ROUTE = "/vypiszregistru/vypisvozidel";
const FLEET_DB_INDEX_VERSION = 4;
const OWNERSHIP_ONLY = process.argv.includes("--ownership-only");
const CURRENT_OWNER_DIR = path.join(FLEET_DB_DIR, "owners");
const CURRENT_OWNER_NAME_DIR = path.join(FLEET_DB_DIR, "owner-names");
const CURRENT_OWNERSHIP_PCV_DIR = path.join(FLEET_DB_DIR, "ownership-pcv");

main().catch((error) => {
  console.error("[fleet-db] build failed");
  console.error(error && error.stack ? error.stack : String(error));
  process.exitCode = 1;
});

async function main() {
  const token = await getRenToken();
  const ownerSource = await resolveDatasetSource(OWNER_ROUTE, token);
  const ownerMeta = ownerSource.metadata;

  const currentMeta = await readJson(path.join(FLEET_DB_DIR, "meta.json"));
  if (OWNERSHIP_ONLY) {
    if (
      currentMeta?.ready &&
      currentMeta.indexVersion === FLEET_DB_INDEX_VERSION &&
      currentMeta.ownerFilename === ownerMeta.filename &&
      fs.existsSync(CURRENT_OWNER_DIR) &&
      fs.existsSync(CURRENT_OWNER_NAME_DIR) &&
      fs.existsSync(CURRENT_OWNERSHIP_PCV_DIR)
    ) {
      console.log("[fleet-db] ownership index up to date, skipping rebuild");
      return;
    }

    await buildOwnershipOnly({ ownerSource, ownerMeta, currentMeta });
    return;
  }

  const vehicleSource = await resolveDatasetSource(VEHICLE_ROUTE, token);
  const vehicleMeta = vehicleSource.metadata;

  if (
    currentMeta?.ready &&
    currentMeta.indexVersion === FLEET_DB_INDEX_VERSION &&
    currentMeta.ownerFilename === ownerMeta.filename &&
    currentMeta.vehicleFilename === vehicleMeta.filename
  ) {
    console.log("[fleet-db] up to date, skipping rebuild");
    return;
  }

  await fs.promises.rm(FLEET_DB_TMP_DIR, { recursive: true, force: true });
  await fs.promises.mkdir(OWNER_DIR, { recursive: true });
  await fs.promises.mkdir(OWNER_NAME_DIR, { recursive: true });
  await fs.promises.mkdir(OWNERSHIP_PCV_DIR, { recursive: true });
  await fs.promises.mkdir(VEHICLE_DIR, { recursive: true });
  await fs.promises.mkdir(VIN_PCV_DIR, { recursive: true });

  console.log("[fleet-db] building owner shards");
  const companyPcvs = await buildOwnerShardSets({
    source: ownerSource
  });

  console.log("[fleet-db] building vehicle shards");
  await buildVehicleShardSets({
    source: vehicleSource,
    companyPcvs
  });

  await writeJson(META_FILE, {
    ready: true,
    indexVersion: FLEET_DB_INDEX_VERSION,
    builtAt: new Date().toISOString(),
    ownerFilename: ownerMeta.filename,
    ownerDatasetDate: ownerMeta.datasetDate || null,
    vehicleFilename: vehicleMeta.filename,
    vehicleDatasetDate: vehicleMeta.datasetDate || null
  });

  await fs.promises.rm(FLEET_DB_DIR, { recursive: true, force: true });
  await fs.promises.rename(FLEET_DB_TMP_DIR, FLEET_DB_DIR);
  console.log("[fleet-db] build complete");
}

async function buildOwnershipOnly({ ownerSource, ownerMeta, currentMeta }) {
  await fs.promises.rm(FLEET_DB_TMP_DIR, { recursive: true, force: true });
  await fs.promises.mkdir(OWNER_DIR, { recursive: true });
  await fs.promises.mkdir(OWNER_NAME_DIR, { recursive: true });
  await fs.promises.mkdir(OWNERSHIP_PCV_DIR, { recursive: true });

  console.log("[fleet-db] rebuilding ownership shards only");
  await buildOwnerShardSets({
    source: ownerSource
  });

  await fs.promises.mkdir(FLEET_DB_DIR, { recursive: true });
  await replaceDirectory(OWNER_DIR, CURRENT_OWNER_DIR);
  await replaceDirectory(OWNER_NAME_DIR, CURRENT_OWNER_NAME_DIR);
  await replaceDirectory(OWNERSHIP_PCV_DIR, CURRENT_OWNERSHIP_PCV_DIR);

  await writeJson(path.join(FLEET_DB_DIR, "meta.json"), {
    ...(currentMeta || {}),
    ready: true,
    indexVersion: FLEET_DB_INDEX_VERSION,
    builtAt: new Date().toISOString(),
    ownerFilename: ownerMeta.filename,
    ownerDatasetDate: ownerMeta.datasetDate || null
  });

  await fs.promises.rm(FLEET_DB_TMP_DIR, { recursive: true, force: true });
  console.log("[fleet-db] ownership rebuild complete");
}

async function replaceDirectory(sourceDir, targetDir) {
  const backupDir = `${targetDir}-old-${Date.now()}`;
  const targetExists = fs.existsSync(targetDir);
  if (targetExists) {
    await fs.promises.rename(targetDir, backupDir);
  }

  try {
    await fs.promises.rename(sourceDir, targetDir);
    if (targetExists) {
      await fs.promises.rm(backupDir, { recursive: true, force: true });
    }
  } catch (error) {
    if (targetExists && !fs.existsSync(targetDir) && fs.existsSync(backupDir)) {
      await fs.promises.rename(backupDir, targetDir);
    }
    throw error;
  }
}

async function buildOwnerShardSets({ source }) {
  const icoBuffers = new Map();
  const nameBuffers = new Map();
  const pcvBuffers = new Map();
  const collectCompanyPcvs = !OWNERSHIP_ONLY;
  const companyPcvs = collectCompanyPcvs ? new Set() : null;
  let processed = 0;
  let ownerWritten = 0;
  let nameWritten = 0;
  let pcvWritten = 0;

  try {
    await scanCsvSource(source, async ({ values, headerMap }) => {
      processed += 1;

      const ownerRecord = normalizeOwnerRecord(values, headerMap);
      if (ownerRecord) {
        const shard = getShardKey(ownerRecord.shardKey);
        await appendShardLine(icoBuffers, path.join(OWNER_DIR, `${shard}.jsonl`), JSON.stringify(ownerRecord.payload));
        if (collectCompanyPcvs) {
          companyPcvs.add(ownerRecord.payload.pcv);
        }
        ownerWritten += 1;
      }

      const ownerNameRecord = normalizeOwnerNameRecord(values, headerMap);
      if (ownerNameRecord) {
        const shard = getShardKey(ownerNameRecord.shardKey);
        await appendShardLine(
          nameBuffers,
          path.join(OWNER_NAME_DIR, `${shard}.jsonl`),
          JSON.stringify(ownerNameRecord.payload)
        );
        if (collectCompanyPcvs) {
          companyPcvs.add(ownerNameRecord.payload.pcv);
        }
        nameWritten += 1;
      }

      const ownershipRecord = normalizeOwnershipPcvRecord(values, headerMap);
      if (ownershipRecord) {
        const shard = getShardKey(ownershipRecord.shardKey);
        await appendShardLine(
          pcvBuffers,
          path.join(OWNERSHIP_PCV_DIR, `${shard}.jsonl`),
          JSON.stringify(ownershipRecord.payload)
        );
        pcvWritten += 1;
      }

      if (processed % 100000 === 0) {
        console.log(
          `[fleet-db] owners processed=${processed} icoWritten=${ownerWritten} nameWritten=${nameWritten} pcvWritten=${pcvWritten}`
        );
      }
    });
  } finally {
    await flushAllShardBuffers(icoBuffers);
    await flushAllShardBuffers(nameBuffers);
    await flushAllShardBuffers(pcvBuffers);
  }

  console.log(
    `[fleet-db] owners done processed=${processed} icoWritten=${ownerWritten} nameWritten=${nameWritten} pcvWritten=${pcvWritten} companyPcvs=${collectCompanyPcvs ? companyPcvs.size : "skipped"} file=${source.metadata.filename}`
  );

  return companyPcvs || new Set();
}

async function buildShardSet({ source, outputDir, normalizeRecord, progressLabel }) {
  const buffers = new Map();
  let processed = 0;
  let written = 0;

  try {
    await scanCsvSource(source, async ({ values, headerMap }) => {
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

  console.log(`[fleet-db] ${progressLabel} done processed=${processed} written=${written} file=${source.metadata.filename}`);
}

function normalizeOwnerRecord(values, headerMap) {
  const ico = sanitizeIco(values[headerMap.ICO]);
  const pcv = normalizeWhitespace(values[headerMap.PCV]);

  if (!ico || !pcv) {
    return null;
  }

  return {
    shardKey: ico,
    payload: {
      ico,
      pcv,
      relation: normalizeWhitespace(values[headerMap.VZTAHKVOZIDLU]) || "Subjekt",
      subjectType: normalizeWhitespace(values[headerMap.TYPSUBJEKTU]) || null,
      current: normalizeBoolean(values[headerMap.AKTUALNI]),
      name: normalizeWhitespace(values[headerMap.NAZEV]),
      address: normalizeWhitespace(values[headerMap.ADRESA]),
      dateFrom: normalizeOpenDataDate(values[headerMap.DATUMOD]),
      dateTo: normalizeOpenDataDate(values[headerMap.DATUMDO])
    }
  };
}

function normalizeOwnerNameRecord(values, headerMap) {
  const pcv = normalizeWhitespace(values[headerMap.PCV]);
  const name = normalizeWhitespace(values[headerMap.NAZEV]);
  const nameKey = normalizeCompanyNameForMatch(name);
  if (!pcv || !nameKey || !looksLikeCompanyName(name)) {
    return null;
  }

  return {
    shardKey: nameKey,
    payload: {
      nameKey,
      ico: sanitizeIco(values[headerMap.ICO]),
      pcv,
      relation: normalizeWhitespace(values[headerMap.VZTAHKVOZIDLU]) || "Subjekt",
      subjectType: normalizeWhitespace(values[headerMap.TYPSUBJEKTU]) || null,
      current: normalizeBoolean(values[headerMap.AKTUALNI]),
      name,
      address: normalizeWhitespace(values[headerMap.ADRESA]),
      dateFrom: normalizeOpenDataDate(values[headerMap.DATUMOD]),
      dateTo: normalizeOpenDataDate(values[headerMap.DATUMDO])
    }
  };
}

function normalizeVehicleRecord(values, headerMap) {
  const pcv = normalizeWhitespace(values[headerMap.PCV]);
  if (!pcv) {
    return null;
  }
  const plate = normalizePlate(firstNonEmptyByHeader(values, headerMap, [
    "SPZ",
    "RZ",
    "ECV",
    "REGISTRACNIZNACKA",
    "REGZNACKA",
    "EVIDENCNICISLO"
  ]));
  const dimensions = parseVehicleDimensionTriplet(values[headerMap.CELKOVADELKASIRKAVYSKAMM]);

  return {
    shardKey: pcv,
    payload: {
      pcv,
      plate,
      vin: normalizeWhitespace(values[headerMap.VIN]) || null,
      make: normalizeWhitespace(values[headerMap.TOVARNIZNACKA]),
      model: normalizeWhitespace(values[headerMap.OBCHODNIOZNACENI]),
      type: normalizeWhitespace(values[headerMap.TYP]),
      variant: normalizeWhitespace(values[headerMap.VARIANTA]),
      status: normalizeWhitespace(values[headerMap.STATUS]) || null,
      category: normalizeWhitespace(values[headerMap.KATEGORIEVOZIDLA]),
      fuel: normalizeWhitespace(values[headerMap.PALIVO]),
      firstRegistration: normalizeOpenDataDate(values[headerMap.DATUM1REGISTRACE]),
      firstRegistrationCz: normalizeOpenDataDate(values[headerMap.DATUM1REGISTRACEVCR]),
      power: normalizeWhitespace(values[headerMap.MAXVYKONKWMIN1]),
      color: normalizeWhitespace(values[headerMap.BARVA]) || null,
      lengthMm: dimensions[0] || null,
      widthMm: dimensions[1] || null,
      heightMm: dimensions[2] || null,
      wheelbaseMm: normalizeVehicleMeasure(values[headerMap.ROZVORMM]),
      weightKg: normalizeVehicleMeasure(values[headerMap.PROVOZNIHMOTNOST])
    }
  };
}

function normalizeOwnershipPcvRecord(values, headerMap) {
  const ico = sanitizeIco(values[headerMap.ICO]);
  const pcv = normalizeWhitespace(values[headerMap.PCV]);
  if (!pcv) {
    return null;
  }

  return {
    shardKey: pcv,
    payload: {
      ico,
      pcv,
      relation: normalizeWhitespace(values[headerMap.VZTAHKVOZIDLU]) || "Subjekt",
      subjectType: normalizeWhitespace(values[headerMap.TYPSUBJEKTU]) || null,
      current: normalizeBoolean(values[headerMap.AKTUALNI]),
      name: normalizeWhitespace(values[headerMap.NAZEV]),
      address: normalizeWhitespace(values[headerMap.ADRESA]),
      dateFrom: normalizeOpenDataDate(values[headerMap.DATUMOD]),
      dateTo: normalizeOpenDataDate(values[headerMap.DATUMDO])
    }
  };
}

async function buildVehicleShardSets({ source, companyPcvs }) {
  const vehicleBuffers = new Map();
  const vinBuffers = new Map();
  let processed = 0;
  let vehicleWritten = 0;
  let vinWritten = 0;

  try {
    await scanCsvSource(source, async ({ values, headerMap }) => {
      processed += 1;
      const vehicleRecord = normalizeVehicleRecord(values, headerMap);
      if (!vehicleRecord) {
        return;
      }

      const pcv = vehicleRecord.payload.pcv;
      if (companyPcvs.has(pcv)) {
        const vehicleShard = getShardKey(vehicleRecord.shardKey);
        await appendShardLine(
          vehicleBuffers,
          path.join(VEHICLE_DIR, `${vehicleShard}.jsonl`),
          JSON.stringify(vehicleRecord.payload)
        );
        vehicleWritten += 1;
      }

      if (vehicleRecord.payload.vin) {
        const vinShard = getShardKey(vehicleRecord.payload.vin);
        await appendShardLine(
          vinBuffers,
          path.join(VIN_PCV_DIR, `${vinShard}.jsonl`),
          JSON.stringify({
            vin: vehicleRecord.payload.vin,
            pcv
          })
        );
        vinWritten += 1;
      }

      if (processed % 100000 === 0) {
        console.log(
          `[fleet-db] vehicles processed=${processed} summaries=${vehicleWritten} vinPcvs=${vinWritten}`
        );
      }
    });
  } finally {
    await flushAllShardBuffers(vehicleBuffers);
    await flushAllShardBuffers(vinBuffers);
  }

  console.log(
    `[fleet-db] vehicles done processed=${processed} summaries=${vehicleWritten} vinPcvs=${vinWritten} file=${source.metadata.filename}`
  );
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

async function scanLocalCsv(filePath, onRow) {
  const stream = createLocalDatasetReadStream(filePath);
  const reader = readline.createInterface({ input: stream, crlfDelay: Infinity });
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
}

function createLocalDatasetReadStream(filePath) {
  const stream = fs.createReadStream(filePath);
  if (/\.gz$/i.test(filePath)) {
    return stream.pipe(zlib.createGunzip()).setEncoding("utf8");
  }

  stream.setEncoding("utf8");
  return stream;
}

async function scanCsvSource(source, onRow) {
  if (source.localPath) {
    console.log(`[fleet-db] using local source ${source.localPath}`);
    await scanLocalCsv(source.localPath, onRow);
    return;
  }

  await scanRemoteCsv(source.route, source.token, onRow);
}

async function resolveDatasetSource(route, token) {
  const local = await findLocalDataset(route);
  if (local) {
    return {
      route,
      token,
      localPath: local.localPath,
      metadata: {
        filename: local.filename,
        datasetDate: local.datasetDate
      }
    };
  }

  return {
    route,
    token,
    localPath: null,
    metadata: await fetchDatasetMetadata(route, token)
  };
}

async function findLocalDataset(route) {
  const cached = await readJson(path.join(OPEN_DATA_DIR, "datasets.json"));
  const cachedKey = route === VEHICLE_ROUTE ? "vehicles" : null;
  if (cachedKey && cached?.[cachedKey]?.localPath && fs.existsSync(cached[cachedKey].localPath)) {
    return {
      localPath: cached[cachedKey].localPath,
      filename: cached[cachedKey].filename || path.basename(cached[cachedKey].localPath),
      datasetDate: cached[cachedKey].datasetDate || parseDatasetDateFromFilename(cached[cachedKey].filename)
    };
  }

  const entries = await fs.promises.readdir(OPEN_DATA_DIR).catch(() => []);
  const pattern =
	    route === OWNER_ROUTE
	      ? /^RSV_vlastnik_provozovatel_vozidla_\d{8}\.csv(?:\.gz)?$/i
      : route === VEHICLE_ROUTE
        ? /^RSV_vypis_vozidel_\d{8}\.csv(?:\.gz)?$/i
        : null;

  if (!pattern) {
    return null;
  }

  const candidates = [];
  for (const name of entries) {
    if (!pattern.test(name)) {
      continue;
    }
    const localPath = path.join(OPEN_DATA_DIR, name);
    const stats = await fs.promises.stat(localPath).catch(() => null);
    if (stats?.isFile()) {
      candidates.push({ localPath, filename: name, size: stats.size });
    }
  }

  candidates.sort((left, right) => {
	    const leftComplete = isCompleteLocalDatasetFile(left.filename) ? 1 : 0;
	    const rightComplete = isCompleteLocalDatasetFile(right.filename) ? 1 : 0;
    if (leftComplete !== rightComplete) {
      return rightComplete - leftComplete;
    }
    return right.size - left.size;
  });

  const selected = candidates[0];
  if (!selected) {
    return null;
  }

  return {
    ...selected,
    datasetDate: parseDatasetDateFromFilename(selected.filename)
	  };
	}

function isCompleteLocalDatasetFile(name) {
  return /\.csv(?:\.gz)?$/i.test(name);
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
  const filename = parseContentDispositionFilename(header) || normalizeWhitespace(header);
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

function firstNonEmptyByHeader(values, headerMap, keys) {
  for (const key of keys) {
    const index = headerMap[key];
    if (index === undefined) {
      continue;
    }
    const value = normalizeWhitespace(values[index]);
    if (value) {
      return value;
    }
  }
  return null;
}

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function parseVehicleDimensionTriplet(value) {
  return normalizeWhitespace(value)
    .replace(/[x×]/gi, "/")
    .split("/")
    .map(normalizeVehicleMeasure)
    .filter(Boolean);
}

function normalizeVehicleMeasure(value) {
  const text = normalizeWhitespace(value).replace(/\b(?:mm|kg)\b/gi, "").trim();
  return text ? text.replace(/\s+/g, " ") : null;
}

function normalizeForMatch(value) {
  return normalizeWhitespace(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function normalizeCompanyNameForMatch(value) {
  return normalizeForMatch(value)
    .replace(/\b(s r o|spol s r o|a s|akc spol|akciova spolecnost|spolecnost s rucenim omezenym)\b/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function looksLikeCompanyName(value) {
  const normalized = normalizeForMatch(value);
  return /\b(s\s*r\s*o|spol|a\s*s|druzstvo|zapsany ustav|statni podnik|obec|mesto|kraj)\b/.test(normalized);
}

function normalizePlate(value) {
  const normalized = normalizeWhitespace(value).toUpperCase().replace(/[^A-Z0-9]/g, "");
  return /^[A-Z0-9]{5,10}$/.test(normalized) ? normalized : null;
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
