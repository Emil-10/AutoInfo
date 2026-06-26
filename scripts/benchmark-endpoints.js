#!/usr/bin/env node

const http = require("http");
const https = require("https");
const { URL } = require("url");

const BASE_URL = normalizeBaseUrl(process.env.BENCH_BASE_URL || "http://127.0.0.1:3000");
const ITERATIONS = Math.max(1, Number(process.env.BENCH_ITERATIONS || getArgNumber("--iterations", 5)) || 5);
const REQUEST_TIMEOUT_MS = Math.max(1000, Number(process.env.BENCH_TIMEOUT_MS || 60000) || 60000);

const ENDPOINTS = [
  {
    name: "company-fleet",
    path: `/api/company-fleet?ico=${encodeURIComponent(process.env.BENCH_ICO || "29145872")}`
  },
  {
    name: "lookup-vin",
    path: `/api/lookup?type=vin&query=${encodeURIComponent(process.env.BENCH_VIN || "YV1LFH5V5R1222680")}`
  },
  {
    name: "vehicle-history",
    path: `/api/vehicle-history?pcv=${encodeURIComponent(process.env.BENCH_PCV || "18277250")}`
  }
];

main().catch((error) => {
  console.error("[benchmark] failed");
  console.error(error && error.stack ? error.stack : String(error));
  process.exitCode = 1;
});

async function main() {
  const results = [];
  for (const endpoint of ENDPOINTS) {
    const cold = await timeRequest(endpoint);
    const warm = [];
    for (let index = 0; index < ITERATIONS; index += 1) {
      warm.push(await timeRequest(endpoint));
    }
    results.push({
      name: endpoint.name,
      url: `${BASE_URL}${endpoint.path}`,
      cold,
      warm: summarize(warm)
    });
  }

  console.log(JSON.stringify({
    baseUrl: BASE_URL,
    iterations: ITERATIONS,
    generatedAt: new Date().toISOString(),
    results
  }, null, 2));
}

async function timeRequest(endpoint) {
  const start = process.hrtime.bigint();
  try {
    const response = await requestJson(`${BASE_URL}${endpoint.path}`);
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
    return {
      statusCode: response.statusCode,
      bytes: response.bytes,
      ms: Number(elapsedMs.toFixed(2))
    };
  } catch (error) {
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
    return {
      statusCode: 0,
      bytes: 0,
      ms: Number(elapsedMs.toFixed(2)),
      error: error.message
    };
  }
}

function summarize(samples) {
  const sorted = samples.slice().sort((left, right) => left.ms - right.ms);
  const statuses = samples.reduce((map, sample) => {
    map[sample.statusCode] = (map[sample.statusCode] || 0) + 1;
    return map;
  }, Object.create(null));
  return {
    statuses,
    errors: samples.filter((sample) => sample.error).map((sample) => sample.error),
    minMs: round(sorted[0]?.ms),
    medianMs: round(percentile(sorted, 0.5)),
    p95Ms: round(percentile(sorted, 0.95)),
    maxMs: round(sorted[sorted.length - 1]?.ms),
    avgMs: round(samples.reduce((sum, sample) => sum + sample.ms, 0) / samples.length),
    bytesAvg: Math.round(samples.reduce((sum, sample) => sum + sample.bytes, 0) / samples.length)
  };
}

function percentile(sortedSamples, ratio) {
  if (sortedSamples.length === 0) {
    return 0;
  }
  const index = Math.min(sortedSamples.length - 1, Math.ceil(sortedSamples.length * ratio) - 1);
  return sortedSamples[index].ms;
}

function round(value) {
  return Number(Number(value || 0).toFixed(2));
}

function requestJson(targetUrl) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(targetUrl);
    const transport = parsed.protocol === "https:" ? https : http;
    const req = transport.request(
      parsed,
      {
        method: "GET",
        timeout: REQUEST_TIMEOUT_MS,
        headers: {
          Accept: "application/json",
          "User-Agent": "autoinfo-benchmark/1.0"
        }
      },
      (res) => {
        let bytes = 0;
        res.on("data", (chunk) => {
          bytes += chunk.length;
        });
        res.on("end", () => {
          resolve({
            statusCode: res.statusCode || 0,
            bytes
          });
        });
      }
    );

    req.on("timeout", () => {
      req.destroy(new Error(`Request timed out after ${REQUEST_TIMEOUT_MS}ms: ${targetUrl}`));
    });
    req.on("error", reject);
    req.end();
  });
}

function normalizeBaseUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

function getArgNumber(name, fallback) {
  const prefix = `${name}=`;
  const match = process.argv.find((arg) => arg.startsWith(prefix));
  if (!match) {
    return fallback;
  }
  const parsed = Number(match.slice(prefix.length));
  return Number.isFinite(parsed) ? parsed : fallback;
}
