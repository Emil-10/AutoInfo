#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright-core");

const ROOT_DIR = path.join(__dirname, "..");
const BASE_URL = normalizeBaseUrl(process.env.E2E_BASE_URL || "http://127.0.0.1:3002");
const ICO = process.env.E2E_ICO || "29145872";
const COMPANY = process.env.E2E_COMPANY || "MediaRey, SE";
const VEHICLE_TITLE = process.env.E2E_VEHICLE_TITLE || "VOLVO XC90 / L";
const EXPECTED_PLATE = process.env.E2E_PLATE === undefined ? "EL828CC" : process.env.E2E_PLATE;
const EXPECTED_PLATE_REQUIRED = Boolean(EXPECTED_PLATE && EXPECTED_PLATE !== "__SKIP__");
const EXPECTED_VIN = process.env.E2E_VIN || "YV1LFH5V5R1222680";
const EXPECTED_PCV = process.env.E2E_PCV || "18277250";
const EXPECTED_STK_PERFORMED = process.env.E2E_STK_PERFORMED || "07. 02. 2024";
const EXPECTED_STK_VALID_UNTIL = process.env.E2E_STK_VALID_UNTIL || "07. 02. 2028";
const REQUIRE_ALL_VISIBLE_PLATES = parseBooleanEnv(process.env.E2E_REQUIRE_ALL_VISIBLE_PLATES);
const REQUIRE_ALL_VISIBLE_STK = parseBooleanEnv(process.env.E2E_REQUIRE_ALL_VISIBLE_STK);
const SCREENSHOT_PATH = path.join(ROOT_DIR, ".cache", "e2e", "ico-fleet-spz-stk.png");

main().catch((error) => {
  console.error("[e2e:ico] failed");
  console.error(error && error.stack ? error.stack : String(error));
  process.exitCode = 1;
});

async function main() {
  await assertHealth();
  await assertPlateApi();
  await assertApi();

  await fs.promises.mkdir(path.dirname(SCREENSHOT_PATH), { recursive: true });
  const browser = await chromium.launch({
    headless: true,
    executablePath: resolveBrowserPath(),
    args: ["--no-sandbox"]
  });

  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
    const url = `${BASE_URL}/ico/${encodeURIComponent(ICO)}`;
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForFunction(
      ({ company, vehicleTitle }) => {
        const text = document.body.innerText || "";
        return text.includes(company) && text.includes(vehicleTitle);
      },
      { company: COMPANY, vehicleTitle: VEHICLE_TITLE },
      { timeout: 60000 }
    );
    await page.waitForTimeout(2500);

    const result = await page.evaluate(
      ({ company, vehicleTitle, plate, plateRequired, performed, validUntil, requireAllVisiblePlates, requireAllVisibleStk }) => {
        const body = document.body.innerText || "";
        const articles = Array.from(document.querySelectorAll("article")).map((element) => element.innerText || "");
        const vehicleArticle = articles.find((text) => text.includes(vehicleTitle)) || "";
        const visibleVehicleArticles = articles.filter((text) => {
          const normalized = normalizeText(text);
          return normalized.includes("VIN") && normalized.includes("STK PROVEDENA");
        });
        const normalizedVehicleArticle = normalizeText(vehicleArticle);
        const checks = {
          hasFleetPage: body.includes(company),
          hasVehicle: vehicleArticle.includes(vehicleTitle),
          hidesPlateInFleet: !normalizedVehicleArticle.includes("SPZ") && (!plateRequired || !vehicleArticle.includes(plate)),
          hasStkPerformed: vehicleArticle.includes("STK PROVEDENA") && vehicleArticle.includes(performed),
          hasStkValidUntil: vehicleArticle.includes("STK PLATNA DO") || vehicleArticle.includes("STK PLATNÁ DO")
            ? vehicleArticle.includes(validUntil)
            : false
        };
        const visibleCoverage = visibleVehicleArticles.map((text) => {
          const title = extractVehicleTitle(text);
          const stkPerformedValue = extractLabelValue(text, ["STK PROVEDENA"]);
          const stkValidUntilValue = extractLabelValue(text, ["STK PLATNA DO", "STK PLATNÁ DO"]);
          return {
            title,
            stkPerformed: stkPerformedValue,
            stkValidUntil: stkValidUntilValue,
            hasStkPerformed: isPresentValue(stkPerformedValue),
            hasStkValidUntil: isPresentValue(stkValidUntilValue)
          };
        });
        const coverageFailures = [];
        if (requireAllVisibleStk) {
          visibleCoverage
            .filter((item) => !item.hasStkPerformed || !item.hasStkValidUntil)
            .forEach((item) => coverageFailures.push(`${item.title}: missing STK`));
        }

        return {
          checks,
          visibleCoverage,
          coverageFailures,
          pass: Object.values(checks).every(Boolean) && coverageFailures.length === 0,
          vehicleArticle
        };

        function normalizeText(value) {
          return String(value || "")
            .normalize("NFD")
            .replace(/[\u0300-\u036f]/g, "")
            .toUpperCase();
        }

        function extractVehicleTitle(text) {
          const lines = text.split(/\n+/).map((line) => line.trim()).filter(Boolean);
          return lines.find((line) => !normalizeText(line).includes("AKTUALNI") && !normalizeText(line).includes("HISTORIE")) || "Vozidlo";
        }

        function extractLabelValue(text, labels) {
          const lines = text.split(/\n+/).map((line) => line.trim()).filter(Boolean);
          const normalizedLabels = labels.map(normalizeText);
          for (let index = 0; index < lines.length; index += 1) {
            if (normalizedLabels.includes(normalizeText(lines[index]))) {
              return lines[index + 1] || "";
            }
          }
          return "";
        }

        function isPresentValue(value) {
          const normalized = normalizeText(value);
          return Boolean(
            normalized &&
            normalized !== "-" &&
            normalized !== "NEDOSTUPNA" &&
            normalized !== "ZJISTUJI..."
          );
        }
      },
      {
        company: COMPANY,
        vehicleTitle: VEHICLE_TITLE,
        plate: EXPECTED_PLATE,
        plateRequired: EXPECTED_PLATE_REQUIRED,
        performed: EXPECTED_STK_PERFORMED,
        validUntil: EXPECTED_STK_VALID_UNTIL,
        requireAllVisiblePlates: REQUIRE_ALL_VISIBLE_PLATES,
        requireAllVisibleStk: REQUIRE_ALL_VISIBLE_STK
      }
    );

    await page.screenshot({ path: SCREENSHOT_PATH, fullPage: false });

    if (!result.pass) {
      throw new Error(`Rendered fleet card did not include all required fields: ${JSON.stringify(result, null, 2)}`);
    }

    console.log(JSON.stringify({
      ok: true,
      url,
      company: COMPANY,
      vehicleTitle: VEHICLE_TITLE,
      plate: EXPECTED_PLATE_REQUIRED ? EXPECTED_PLATE : null,
      stkPerformed: EXPECTED_STK_PERFORMED,
      stkValidUntil: EXPECTED_STK_VALID_UNTIL,
      requireAllVisiblePlates: REQUIRE_ALL_VISIBLE_PLATES,
      requireAllVisibleStk: REQUIRE_ALL_VISIBLE_STK,
      visibleCoverage: result.visibleCoverage,
      screenshot: SCREENSHOT_PATH
    }, null, 2));
  } finally {
    await browser.close();
  }
}

async function assertApi() {
  const response = await fetch(`${BASE_URL}/api/lookup?type=ico&query=${encodeURIComponent(ICO)}`, {
    headers: { Accept: "application/json" }
  });
  if (!response.ok) {
    throw new Error(`ICO API returned HTTP ${response.status}`);
  }

  const payload = await response.json();
  const records = Array.isArray(payload.records) ? payload.records : [];
  const record = records.find((item) => item.title === VEHICLE_TITLE);
  if (!record) {
    throw new Error(`ICO API did not return expected vehicle ${VEHICLE_TITLE}`);
  }

  const failures = [];
  if (EXPECTED_PLATE_REQUIRED && record.plate !== EXPECTED_PLATE) {
    failures.push(`plate expected ${EXPECTED_PLATE}, got ${record.plate || "null"}`);
  }
  if (!record.inspection?.performedOn) {
    failures.push("inspection.performedOn missing");
  }
  if (!record.inspection?.validUntil) {
    failures.push("inspection.validUntil missing");
  }
  if (failures.length > 0) {
    throw new Error(`ICO API record is incomplete: ${failures.join("; ")}`);
  }

  const coverageFailures = [];
  if (REQUIRE_ALL_VISIBLE_PLATES) {
    records
      .filter((record) => !record.plate)
      .forEach((record) => coverageFailures.push(`${record.title || record.pcv || record.vin}: API missing plate`));
  }
  if (REQUIRE_ALL_VISIBLE_STK) {
    records
      .filter((record) => !record.inspection?.performedOn || !record.inspection?.validUntil)
      .forEach((record) => coverageFailures.push(`${record.title || record.pcv || record.vin}: API missing STK`));
  }
  if (coverageFailures.length > 0) {
    throw new Error(`ICO API coverage check failed: ${coverageFailures.join("; ")}`);
  }
}

async function assertHealth() {
  const response = await fetch(`${BASE_URL}/api/health`, {
    headers: { Accept: "application/json" }
  });
  const payload = await response.json().catch(() => null);
  if (!response.ok || !payload?.ok) {
    throw new Error(`Health check failed before lookup tests: ${JSON.stringify(payload)}`);
  }
  if (payload?.openData?.error) {
    throw new Error(`Open data DB is not available: ${payload.openData.error}`);
  }
}

async function assertPlateApi() {
  if (!EXPECTED_PLATE_REQUIRED) {
    return;
  }

  const response = await fetch(`${BASE_URL}/api/lookup?type=plate&query=${encodeURIComponent(EXPECTED_PLATE)}`, {
    headers: { Accept: "application/json" }
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => null);
    throw new Error(`SPZ API returned HTTP ${response.status}: ${JSON.stringify(payload)}`);
  }

  const payload = await response.json();
  const identifiers = Array.isArray(payload.highlights) ? payload.highlights : [];
  const plate = getIdentifierValue(identifiers, "SPZ");
  const vin = getIdentifierValue(identifiers, "VIN");
  const pcv = getIdentifierValue(identifiers, "PČV") || getIdentifierValue(identifiers, "PCV");
  const failures = [];

  if (plate !== EXPECTED_PLATE) {
    failures.push(`SPZ expected ${EXPECTED_PLATE}, got ${plate || "null"}`);
  }
  if (EXPECTED_VIN && vin !== EXPECTED_VIN) {
    failures.push(`VIN expected ${EXPECTED_VIN}, got ${vin || "null"}`);
  }
  if (EXPECTED_PCV && pcv !== EXPECTED_PCV) {
    failures.push(`PCV expected ${EXPECTED_PCV}, got ${pcv || "null"}`);
  }

  if (failures.length > 0) {
    throw new Error(`SPZ API record is incomplete: ${failures.join("; ")}`);
  }
}

function getIdentifierValue(items, label) {
  const normalizedLabel = normalizeForLookup(label);
  const item = items.find((entry) => normalizeForLookup(entry?.label) === normalizedLabel);
  return item?.value || null;
}

function normalizeForLookup(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "");
}

function normalizeBaseUrl(value) {
  return String(value || "").replace(/\/+$/, "");
}

function parseBooleanEnv(value) {
  return ["1", "true", "yes", "on"].includes(String(value || "").trim().toLowerCase());
}

function resolveBrowserPath() {
  const explicit = process.env.E2E_CHROMIUM_PATH || process.env.PVZP_BROWSER_PATH || process.env.UNIQA_BROWSER_PATH;
  if (explicit && fs.existsSync(explicit)) {
    return explicit;
  }

  const candidates = [
    "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
    "C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe",
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
  ];
  const systemBrowser = candidates.find((candidate) => fs.existsSync(candidate));
  if (systemBrowser) {
    return systemBrowser;
  }

  const playwrightPath = typeof chromium.executablePath === "function" ? chromium.executablePath() : "";
  return playwrightPath && fs.existsSync(playwrightPath) ? playwrightPath : undefined;
}
