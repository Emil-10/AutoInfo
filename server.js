const http = require("http");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { AsyncLocalStorage } = require("async_hooks");
const { URL } = require("url");
const lookupAudit = require("./lookup-audit");

loadEnvFile(path.join(__dirname, ".env"));

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || "0.0.0.0";
const DIST_DIR = path.join(__dirname, "dist");
const PUBLIC_DIR = path.join(__dirname, "public");
const STATIC_DIR = fs.existsSync(path.join(DIST_DIR, "index.html")) ? DIST_DIR : PUBLIC_DIR;
let vehicleService = null;
let vehicleServiceLoadError = null;
const requestContext = new AsyncLocalStorage();

const LOOKUP_AUDIT_EVENTS = new Map([
  ["/api/lookup", "vehicle_lookup"],
  ["/api/lookup/inspections", "inspection_lookup"],
  ["/api/lookup/ownership", "ownership_lookup"],
  ["/api/lookup/vignette", "vignette_lookup"],
  ["/api/company-fleet", "company_fleet_lookup"],
  ["/api/company-fleet/history", "company_fleet_history_lookup"],
  ["/api/resolve-plate", "plate_resolution_lookup"],
  ["/api/scan-plate", "plate_scan"],
  ["/api/vehicle-history", "vehicle_history_lookup"]
]);

const MIME_TYPES = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".ico": "image/x-icon",
  ".txt": "text/plain; charset=utf-8"
};

const server = http.createServer(async (req, res) => {
  try {
    const requestUrl = new URL(req.url, `http://${req.headers.host || "localhost"}`);
    requestContext.enterWith({
      req,
      requestUrl,
      startedAt: Date.now(),
      auditRecorded: false
    });

    if (requestUrl.pathname === "/api/lookup") {
      await handleLookup(req, requestUrl, res);
      return;
    }

    if (requestUrl.pathname === "/api/lookup/inspections") {
      await handleInspectionLookup(req, requestUrl, res);
      return;
    }

    if (requestUrl.pathname === "/api/lookup/ownership") {
      await handleOwnershipLookup(req, requestUrl, res);
      return;
    }

    if (requestUrl.pathname === "/api/lookup/vignette") {
      await handleVignetteLookup(req, requestUrl, res);
      return;
    }

    if (requestUrl.pathname === "/api/company-fleet") {
      sendJson(res, 404, { message: "Lightweight verze podporuje pouze vyhledávání podle SPZ nebo VIN." });
      return;
    }

    if (requestUrl.pathname === "/api/company-fleet/history") {
      sendJson(res, 404, { message: "Lightweight verze nepodporuje IČO historii vozidel." });
      return;
    }

    if (requestUrl.pathname === "/api/resolve-plate") {
      await handlePlateResolutionLookup(req, requestUrl, res);
      return;
    }

    if (requestUrl.pathname === "/api/scan-plate") {
      sendJson(res, 404, { message: "Foto rozpoznávání SPZ není v lightweight verzi zapnuté." });
      return;
    }

    if (requestUrl.pathname === "/api/vehicle-history") {
      await handleVehicleHistoryLookup(req, requestUrl, res);
      return;
    }

    if (requestUrl.pathname === "/api/admin/lookup-stats") {
      await handleLookupStats(req, requestUrl, res);
      return;
    }

    if (requestUrl.pathname === "/api/health") {
      const runtimeStatus = safeGetLookupRuntimeStatus();
      const openDataStatus = await safeGetOpenDataRuntimeStatus();
      const openDataRequired = Boolean(runtimeStatus?.openDataDatabase?.configured);
      const ok = !vehicleServiceLoadError && (!openDataRequired || !openDataStatus?.error);
      sendJson(res, ok ? 200 : 503, {
        ok,
        uptime: process.uptime(),
        lookup: runtimeStatus,
        openData: openDataStatus
      });
      return;
    }

    await serveStatic(requestUrl.pathname, res);
  } catch (error) {
    sendJson(res, 500, {
      message: "Server narazil na neocekavanou chybu.",
      detail: error.message
    });
  }
});

server.listen(PORT, HOST, () => {
  const address = server.address();
  const host =
    address && typeof address === "object" && address.address
      ? address.address
      : HOST;

  console.log(`Info.exleasing.cz bezi na http://${host}:${PORT}`);
  console.log(`[startup] bound host=${HOST} port=${PORT}`);
  console.log(`[startup] lookup runtime ${JSON.stringify(safeGetLookupRuntimeStatus())}`);
});

function normalizeLookupType(value) {
  const normalized = String(value || "").trim().toLowerCase();

  if (normalized === "spz" || normalized === "plate") {
    return "plate";
  }

  if (normalized === "vin") {
    return "vin";
  }

  return "";
}

async function handleLookup(req, requestUrl, res) {
  const query = (requestUrl.searchParams.get("query") || "").trim();
  const requestedType = normalizeLookupType(requestUrl.searchParams.get("type"));

  if (!query) {
    sendJson(res, 400, {
      message: "Zadejte SPZ nebo VIN.",
      hints: ["Například 1AB2345 nebo TMBJJ7NE8L0123456."]
    });
    return;
  }

  try {
    const { lookupVehicle, parseLookupQuery, describeLookupFailure } = getVehicleService();
    const parsedQuery = typeof parseLookupQuery === "function" ? parseLookupQuery(query, requestedType) : null;
    if (parsedQuery?.type === "ico") {
      sendJson(res, 400, {
        message: "Lightweight verze podporuje pouze vyhledávání podle SPZ nebo VIN.",
        hints: ["Použijte SPZ nebo 17místný VIN."]
      });
      return;
    }

    const { record, diagnostics } = await lookupVehicle(query, { includeOwnership: true, type: requestedType });

    if (!record) {
      const payload = describeLookupFailure(query, diagnostics, requestedType);
      const statusCode = hasBlockingLookupError(diagnostics) ? 502 : 404;
      logLookupOutcome(query, diagnostics, statusCode);
      sendJson(res, statusCode, payload);
      return;
    }

    if (diagnostics?.attempts?.some((attempt) => attempt.status === "error")) {
      logLookupOutcome(query, diagnostics, 200);
    }

    sendJson(res, 200, record);
  } catch (error) {
    sendJson(res, 502, {
      message: "Nepodařilo se načíst data ze zdroje.",
      detail: error.message
    });
  }
}

async function handleInspectionLookup(req, requestUrl, res) {
  const query = (requestUrl.searchParams.get("query") || "").trim();
  const vin = (requestUrl.searchParams.get("vin") || "").trim();
  const pcv = (requestUrl.searchParams.get("pcv") || "").trim();

  if (!query && !vin && !pcv) {
    sendJson(res, 400, {
      message: "Zadejte VIN, PČV nebo query."
    });
    return;
  }

  try {
    const { lookupVehicleInspections } = getVehicleService();
    const result = await lookupVehicleInspections({ query, vin, pcv });
    sendJson(res, result.status === "ready" ? 200 : result.status === "pending" ? 202 : 404, result);
  } catch (error) {
    sendJson(res, 502, {
      message: "Nepodařilo se načíst technické prohlídky.",
      detail: error.message
    });
  }
}

async function serveStatic(requestPath, res) {
  const safePath = requestPath === "/" ? "/index.html" : requestPath;
  const filePath = path.normalize(path.join(STATIC_DIR, safePath));

  if (!filePath.startsWith(STATIC_DIR)) {
    sendText(res, 403, "Pristup odepren.");
    return;
  }

  try {
    const content = await fs.promises.readFile(filePath);
    const extension = path.extname(filePath).toLowerCase();
    res.writeHead(200, {
      "Content-Type": MIME_TYPES[extension] || "application/octet-stream",
      "Cache-Control": "no-cache"
    });
    res.end(content);
  } catch (error) {
    if (error.code === "ENOENT") {
      if (fs.existsSync(path.join(STATIC_DIR, "index.html"))) {
        const fallback = await fs.promises.readFile(path.join(STATIC_DIR, "index.html"));
        res.writeHead(200, {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": "no-cache"
        });
        res.end(fallback);
        return;
      }

      sendText(res, 404, "Soubor nenalezen.");
      return;
    }

    sendText(res, 500, "Nepodařilo se načíst soubor.");
  }
}

async function handlePlateScan(req, requestUrl, res) {
  if (req.method !== "POST") {
    sendJson(res, 405, {
      message: "Použijte POST s fotkou SPZ."
    });
    return;
  }

  try {
    const body = await readJsonRequestBody(req, 7500000);
    const image = body?.image || body?.dataUrl || "";
    if (!image) {
      sendJson(res, 400, {
        message: "Nahrajte fotku SPZ."
      });
      return;
    }

    const { scanPlateImage } = getVehicleService();
    const result = await scanPlateImage(image);
    sendJson(res, 200, result);
  } catch (error) {
    sendJson(res, error.code === "BODY_TOO_LARGE" ? 413 : 400, {
      message: error.message || "SPZ se z fotky nepodařilo přečíst.",
      detail: error.code || null
    });
  }
}

function readJsonRequestBody(req, maxBytes) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;

    req.on("data", (chunk) => {
      total += chunk.length;
      if (total > maxBytes) {
        const error = new Error("Požadavek je příliš velký.");
        error.code = "BODY_TOO_LARGE";
        reject(error);
        req.destroy(error);
        return;
      }

      chunks.push(chunk);
    });

    req.on("end", () => {
      try {
        const raw = Buffer.concat(chunks).toString("utf8");
        resolve(raw ? JSON.parse(raw) : {});
      } catch (error) {
        reject(new Error("Požadavek není validní JSON."));
      }
    });

    req.on("error", reject);
  });
}

async function handleOwnershipLookup(req, requestUrl, res) {
  const query = (requestUrl.searchParams.get("query") || "").trim();
  const vin = (requestUrl.searchParams.get("vin") || "").trim();
  const pcv = (requestUrl.searchParams.get("pcv") || "").trim();
  const plate = (requestUrl.searchParams.get("plate") || "").trim();

  if (!query && !vin && !pcv && !plate) {
    sendJson(res, 400, {
      message: "Zadejte VIN, PČV nebo query."
    });
    return;
  }

  try {
    const { lookupVehicleOwnership } = getVehicleService();
    const result = await lookupVehicleOwnership({ query, vin, pcv, plate });
    sendJson(res, result.status === "ready" ? 200 : result.status === "pending" ? 202 : 404, result);
  } catch (error) {
    sendJson(res, 502, {
      message: "Nepodařilo se načíst vlastníky a provozovatele.",
      detail: error.message
    });
  }
}

async function handleVignetteLookup(req, requestUrl, res) {
  const plate = (requestUrl.searchParams.get("plate") || requestUrl.searchParams.get("spz") || requestUrl.searchParams.get("query") || "").trim();
  const country = (requestUrl.searchParams.get("country") || "CZ").trim();

  if (!plate) {
    sendJson(res, 400, {
      message: "Zadejte SPZ pro overeni dalnicni znamky."
    });
    return;
  }

  try {
    const { lookupVignette } = getVehicleService();
    const result = await lookupVignette({ plate, country });
    sendJson(res, 200, result);
  } catch (error) {
    sendJson(res, 502, {
      message: "Nepodarilo se overit dalnicni znamku.",
      detail: error.message
    });
  }
}

function sendJson(res, statusCode, payload) {
  scheduleLookupAudit(statusCode, payload);
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  const body = process.env.NODE_ENV === "production"
    ? JSON.stringify(payload)
    : JSON.stringify(payload, null, 2);
  res.end(body);
}

function scheduleLookupAudit(statusCode, payload) {
  const context = requestContext.getStore();
  const eventType = context?.requestUrl ? LOOKUP_AUDIT_EVENTS.get(context.requestUrl.pathname) : null;

  if (!context || !eventType || context.auditRecorded) {
    return;
  }

  context.auditRecorded = true;

  lookupAudit.recordLookupEvent({
    req: context.req,
    requestUrl: context.requestUrl,
    eventType,
    statusCode,
    durationMs: Date.now() - context.startedAt,
    payload
  }).catch((error) => {
    console.warn(`[lookup-audit] ${error.message}`);
  });
}

function sendText(res, statusCode, text) {
  res.writeHead(statusCode, {
    "Content-Type": "text/plain; charset=utf-8",
    "Cache-Control": "no-store"
  });
  res.end(text);
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

function getVehicleService() {
  if (vehicleService) {
    return vehicleService;
  }

  try {
    vehicleService = require("./vehicle-service");
    vehicleServiceLoadError = null;
    return vehicleService;
  } catch (error) {
    vehicleServiceLoadError = error;
    throw error;
  }
}

async function handleLookupStats(req, requestUrl, res) {
  const adminToken = getLookupAdminToken();

  if (!adminToken) {
    sendJson(res, 403, {
      message: "Statistiky nejsou zapnute. Nastavte LOOKUP_ADMIN_TOKEN."
    });
    return;
  }

  if (!isAuthorizedAdminRequest(req, requestUrl, adminToken)) {
    sendJson(res, 401, {
      message: "Chybi platny admin token."
    });
    return;
  }

  try {
    const days = requestUrl.searchParams.get("days");
    const limit = requestUrl.searchParams.get("limit");
    const stats = await lookupAudit.getLookupStats({ days, limit });
    sendJson(res, stats.ok === false ? 503 : 200, stats);
  } catch (error) {
    sendJson(res, 502, {
      message: "Nepodarilo se nacist lookup statistiky.",
      detail: error.message
    });
  }
}

function getLookupAdminToken() {
  return String(process.env.LOOKUP_ADMIN_TOKEN || process.env.ADMIN_STATS_TOKEN || "").trim();
}

function isAuthorizedAdminRequest(req, requestUrl, expectedToken) {
  const providedToken = getProvidedAdminToken(req, requestUrl);
  if (!providedToken) {
    return false;
  }

  const expectedHash = crypto.createHash("sha256").update(expectedToken).digest();
  const providedHash = crypto.createHash("sha256").update(providedToken).digest();
  return crypto.timingSafeEqual(expectedHash, providedHash);
}

function getProvidedAdminToken(req, requestUrl) {
  const authHeader = String(req.headers.authorization || "").trim();
  if (/^bearer\s+/i.test(authHeader)) {
    return authHeader.replace(/^bearer\s+/i, "").trim();
  }

  return String(
    req.headers["x-admin-token"] ||
      req.headers["x-lookup-admin-token"] ||
      requestUrl.searchParams.get("token") ||
      ""
  ).trim();
}

async function handleCompanyFleetLookup(req, requestUrl, res) {
  const ico = (requestUrl.searchParams.get("ico") || "").trim();

  if (!ico) {
    sendJson(res, 400, {
      message: "Zadejte IČO právnické osoby.",
      hints: ["Například 27074358."]
    });
    return;
  }

  try {
    const { lookupVehiclesByIco } = getVehicleService();
    const result = await lookupVehiclesByIco(ico);
    sendJson(res, 200, result);
  } catch (error) {
    sendJson(res, 502, {
      message: "Nepodařilo se načíst seznam vozidel pro zadané IČO.",
      detail: error.message
    });
  }
}

async function handleCompanyFleetHistoryLookup(req, requestUrl, res) {
  const ico = (requestUrl.searchParams.get("ico") || "").trim();
  const pcv = (requestUrl.searchParams.get("pcv") || "").trim();

  if (!ico || !pcv) {
    sendJson(res, 400, {
      message: "Zadejte IČO a PČV pro načtení historie vztahu."
    });
    return;
  }

  try {
    const { lookupCompanyVehicleHistory } = getVehicleService();
    const result = await lookupCompanyVehicleHistory(ico, pcv);
    sendJson(res, 200, result);
  } catch (error) {
    sendJson(res, 502, {
      message: "Nepodařilo se načíst historii vztahu firmy k vozidlu.",
      detail: error.message
    });
  }
}

async function handlePlateResolutionLookup(req, requestUrl, res) {
  const vin = (requestUrl.searchParams.get("vin") || "").trim();
  const pcv = (requestUrl.searchParams.get("pcv") || "").trim();

  if (!vin && !pcv) {
    sendJson(res, 400, {
      message: "Zadejte VIN nebo PČV pro dohledání SPZ."
    });
    return;
  }

  try {
    const { resolveVehiclePlate } = getVehicleService();
    const result = await resolveVehiclePlate({ vin, pcv, allowPvzpFallback: true, allowUniqaFallback: false });
    sendJson(res, 200, result);
  } catch (error) {
    sendJson(res, 502, {
      message: "Nepodařilo se dohledat SPZ vozidla.",
      detail: error.message
    });
  }
}

async function handleVehicleHistoryLookup(req, requestUrl, res) {
  const pcv = (requestUrl.searchParams.get("pcv") || "").trim();
  const vin = (requestUrl.searchParams.get("vin") || "").trim();

  if (!pcv && !vin) {
    sendJson(res, 400, {
      message: "Zadejte PČV nebo VIN pro načtení historie vozidla."
    });
    return;
  }

  try {
    const { lookupVehicleHistory } = getVehicleService();
    const result = await lookupVehicleHistory({ pcv, vin });
    sendJson(res, 200, result);
  } catch (error) {
    sendJson(res, 502, {
      message: "Nepodařilo se načíst historii vozidla.",
      detail: error.message
    });
  }
}

function safeGetLookupRuntimeStatus() {
  try {
    const { getLookupRuntimeStatus } = getVehicleService();
    const runtimeStatus = getLookupRuntimeStatus();
    if (vehicleServiceLoadError) {
      runtimeStatus.loaderError = vehicleServiceLoadError.message;
    }
    return runtimeStatus;
  } catch (error) {
    return {
      platform: process.platform,
      nodeVersion: process.version,
      loaderError: error.message,
      warnings: ["Lookup modul se nepodařilo inicializovat při startu serveru."]
    };
  }
}

async function safeGetOpenDataRuntimeStatus() {
  try {
    const { getOpenDataRuntimeStatus } = getVehicleService();
    return typeof getOpenDataRuntimeStatus === "function" ? await getOpenDataRuntimeStatus() : null;
  } catch (error) {
    return {
      error: error.message
    };
  }
}

function logLookupOutcome(query, diagnostics, statusCode) {
  const payload = {
    statusCode,
    query: maskLookupQuery(query),
    queryType: diagnostics?.queryType || "unknown",
    attempts: Array.isArray(diagnostics?.attempts)
      ? diagnostics.attempts.map((attempt) => ({
          source: sanitizeLookupLogSource(attempt.source),
          status: attempt.status,
          detail: sanitizeLookupLogDetail(attempt.detail),
          host: attempt.host || null,
          method: attempt.method || null
        }))
      : [],
    warnings: Array.isArray(diagnostics?.runtime?.warnings)
      ? diagnostics.runtime.warnings.map(sanitizeLookupLogDetail).filter(Boolean)
      : []
  };

  const message = `[lookup] ${JSON.stringify(payload)}`;

  if (statusCode >= 500) {
    console.error(message);
    return;
  }

  console.warn(message);
}

function hasBlockingLookupError(diagnostics) {
  const attempts = Array.isArray(diagnostics?.attempts) ? diagnostics.attempts : [];
  return attempts.some((attempt) => {
    if (attempt?.status !== "error") {
      return false;
    }

    return [
      "open-data-db",
      "open-data-db-resolved",
      "plate-resolution-cache",
      "official-vin-api"
    ].includes(attempt.source);
  });
}

function sanitizeLookupLogSource(source) {
  if (source === "pvzp-browser" || source === "uniqa-browser") {
    return "external-browser-source";
  }
  return source;
}

function sanitizeLookupLogDetail(value) {
  return String(value || "")
    .replace(/\bPVZP\b/gi, "externí zdroj")
    .replace(/\bUNIQA\b/gi, "externí zdroj")
    .replace(/\bPVZP_BROWSER_PATH\b/g, "BROWSER_PATH")
    .replace(/\bUNIQA_PHONE\b/g, "CONTACT_PHONE")
    .replace(/\bTRANSPORT_CUBE_LOOKUP_URL\b/g, "primární zdroj")
    .replace(/\bbrowserType\.launch:[^.;]*(?:[.;]|$)/gi, "Spuštění browseru selhalo. ")
    .replace(/\bEPERM:[^.;]*(?:[.;]|$)/gi, "Operace není povolená. ")
    .replace(/\bmkdtemp\s+'[^']*'/gi, "dočasný adresář")
    .replace(/\s+/g, " ")
    .trim();
}

function maskLookupQuery(query) {
  const value = String(query || "").trim();

  if (!value) {
    return "";
  }

  if (value.length <= 6) {
    return value.slice(0, 2) + "*".repeat(Math.max(0, value.length - 2));
  }

  return `${value.slice(0, 3)}***${value.slice(-3)}`;
}
