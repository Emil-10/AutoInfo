#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright-core");

const ROOT_DIR = path.join(__dirname, "..");
const BASE_URL = normalizeBaseUrl(process.env.E2E_BASE_URL || "https://spz.up.railway.app");
const PLATE = process.env.E2E_PLATE || "718JSEXY";
const EXPECTED_VIN = process.env.E2E_VIN || "WP0ZZZ98ZJK272232";
const EXPECTED_TITLE = process.env.E2E_TITLE || "PORSCHE";
const MAX_LOOKUP_MS = Math.max(1, Number(process.env.E2E_MAX_LOOKUP_MS || 3000) || 3000);
const OUT_DIR = path.join(ROOT_DIR, ".cache", "e2e");

main().catch((error) => {
  console.error("[e2e:spz-photo] failed");
  console.error(error && error.stack ? error.stack : String(error));
  process.exitCode = 1;
});

async function main() {
  await fs.promises.mkdir(OUT_DIR, { recursive: true });

  const api = await timedFetch(`${BASE_URL}/api/lookup?type=plate&query=${encodeURIComponent(PLATE)}`);
  if (api.response.status !== 200) {
    throw new Error(`SPZ API returned HTTP ${api.response.status}: ${JSON.stringify(api.payload)}`);
  }
  assertLookupPayload(api.payload, "SPZ API");
  if (api.ms > MAX_LOOKUP_MS) {
    throw new Error(`SPZ API took ${api.ms} ms, limit is ${MAX_LOOKUP_MS} ms`);
  }

  const browser = await chromium.launch({
    headless: true,
    executablePath: resolveBrowserPath(),
    args: ["--no-sandbox"]
  });

  try {
    await assertSharePage(browser);
    await assertPhotoScan(browser);
  } finally {
    await browser.close();
  }

  console.log(JSON.stringify({
    ok: true,
    baseUrl: BASE_URL,
    plate: PLATE,
    vin: EXPECTED_VIN,
    apiMs: api.ms,
    screenshots: [
      path.join(OUT_DIR, "spz-share-page.png"),
      path.join(OUT_DIR, "spz-photo-upload.png")
    ]
  }, null, 2));
}

async function assertSharePage(browser) {
  const page = await browser.newPage({ viewport: { width: 1360, height: 950 } });
  await page.goto(`${BASE_URL}/spz/${encodeURIComponent(PLATE)}`, {
    waitUntil: "domcontentloaded",
    timeout: 30000
  });
  await page.waitForFunction(
    ({ plate, vin, title }) => {
      const text = document.body.innerText || "";
      return text.includes(plate) && text.includes(vin) && text.toUpperCase().includes(title);
    },
    { plate: PLATE, vin: EXPECTED_VIN, title: EXPECTED_TITLE.toUpperCase() },
    { timeout: 30000 }
  );
  await page.screenshot({ path: path.join(OUT_DIR, "spz-share-page.png"), fullPage: false });
  await page.close();
}

async function assertPhotoScan(browser) {
  const imagePath = await createPlateImage(browser);
  const page = await browser.newPage({ viewport: { width: 1360, height: 950 } });
  await page.goto(BASE_URL, { waitUntil: "domcontentloaded", timeout: 30000 });
  await page.locator('input[type="file"]').setInputFiles(imagePath);
  await page.waitForFunction(
    ({ plate, vin }) => {
      const text = document.body.innerText || "";
      const inputValue = document.querySelector("input")?.value || "";
      return inputValue.toUpperCase().includes(plate) && text.includes(vin);
    },
    { plate: PLATE, vin: EXPECTED_VIN },
    { timeout: 45000 }
  );
  await page.screenshot({ path: path.join(OUT_DIR, "spz-photo-upload.png"), fullPage: false });
  await page.close();
}

async function createPlateImage(browser) {
  const page = await browser.newPage({ viewport: { width: 900, height: 360 }, deviceScaleFactor: 2 });
  await page.setContent(`
    <!doctype html>
    <html>
      <body style="margin:0;display:grid;place-items:center;width:900px;height:360px;background:#f2f2f2;">
        <div id="plate" style="width:760px;height:190px;border:12px solid #111;border-radius:18px;background:#fff;display:grid;place-items:center;color:#050505;font:900 92px Arial, Helvetica, sans-serif;letter-spacing:8px;">
          ${escapeHtml(PLATE)}
        </div>
      </body>
    </html>
  `);
  const imagePath = path.join(OUT_DIR, "spz-photo-source.png");
  await page.locator("#plate").screenshot({ path: imagePath });
  await page.close();
  return imagePath;
}

async function timedFetch(url) {
  const startedAt = Date.now();
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  const payload = await response.json().catch(() => null);
  return { response, payload, ms: Date.now() - startedAt };
}

function assertLookupPayload(payload, label) {
  const text = JSON.stringify(payload || {});
  if (!text.includes(PLATE)) {
    throw new Error(`${label} did not include plate ${PLATE}`);
  }
  if (!text.includes(EXPECTED_VIN)) {
    throw new Error(`${label} did not include VIN ${EXPECTED_VIN}`);
  }
  if (!text.toUpperCase().includes(EXPECTED_TITLE.toUpperCase())) {
    throw new Error(`${label} did not include title marker ${EXPECTED_TITLE}`);
  }
}

function normalizeBaseUrl(value) {
  return String(value || "").replace(/\/+$/, "");
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
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
  return candidates.find((candidate) => fs.existsSync(candidate));
}
