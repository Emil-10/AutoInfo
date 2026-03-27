const http = require("http");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");

loadEnvFile(path.join(__dirname, ".env"));

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || "0.0.0.0";
const DIST_DIR = path.join(__dirname, "dist");
const PUBLIC_DIR = path.join(__dirname, "public");
const STATIC_DIR = fs.existsSync(path.join(DIST_DIR, "index.html")) ? DIST_DIR : PUBLIC_DIR;
let vehicleService = null;
let vehicleServiceLoadError = null;

const MIME_TYPES = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".ico": "image/x-icon"
};

const server = http.createServer(async (req, res) => {
  try {
    const requestUrl = new URL(req.url, `http://${req.headers.host || "localhost"}`);

    if (requestUrl.pathname === "/api/lookup") {
      await handleLookup(requestUrl, res);
      return;
    }

    if (requestUrl.pathname === "/api/lookup/inspections") {
      await handleInspectionLookup(requestUrl, res);
      return;
    }

    if (requestUrl.pathname === "/api/health") {
      const runtimeStatus = safeGetLookupRuntimeStatus();
      sendJson(res, 200, {
        ok: true,
        uptime: process.uptime(),
        lookup: runtimeStatus
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

async function handleLookup(requestUrl, res) {
  const query = (requestUrl.searchParams.get("query") || "").trim();

  if (!query) {
    sendJson(res, 400, {
      message: "Zadejte SPZ nebo VIN.",
      hints: ["Napriklad 1AB2345 nebo TMBJJ7NE8L0123456."]
    });
    return;
  }

  try {
    const { lookupVehicle, describeLookupFailure } = getVehicleService();
    const { record, diagnostics } = await lookupVehicle(query);

    if (!record) {
      const payload = describeLookupFailure(query, diagnostics);
      const statusCode = diagnostics?.attempts?.some((attempt) => attempt.status === "error") ? 502 : 404;
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
      message: "Nepodarilo se nacist data ze zdroje.",
      detail: error.message
    });
  }
}

async function handleInspectionLookup(requestUrl, res) {
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
      message: "Nepodarilo se nacist technicke prohlidky.",
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
      "Cache-Control": extension === ".html" ? "no-cache" : "public, max-age=3600"
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

    sendText(res, 500, "Nepodarilo se nacist soubor.");
  }
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  res.end(JSON.stringify(payload, null, 2));
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
      warnings: ["Lookup modul se nepodarilo inicializovat pri startu serveru."]
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
          source: attempt.source,
          status: attempt.status,
          detail: attempt.detail,
          host: attempt.host || null,
          method: attempt.method || null
        }))
      : [],
    warnings: Array.isArray(diagnostics?.runtime?.warnings) ? diagnostics.runtime.warnings : []
  };

  const message = `[lookup] ${JSON.stringify(payload)}`;

  if (statusCode >= 500) {
    console.error(message);
    return;
  }

  console.warn(message);
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
