#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

loadEnvFile(path.join(__dirname, "..", ".env"));

const serviceMode = String(process.env.SERVICE_MODE || "web").toLowerCase();

if (["open-data-cron", "open-data-import", "cron"].includes(serviceMode)) {
  const intervalMs = getImportIntervalMs();
  if (intervalMs > 0) {
    runOpenDataImportLoop(intervalMs).catch((error) => {
      console.error("[open-data-cron] failed");
      console.error(error && error.stack ? error.stack : String(error));
      process.exitCode = 1;
    });
  } else {
    require("./import-open-data-postgres");
  }
} else if (["open-data-cleanup", "cleanup"].includes(serviceMode)) {
  require("./cleanup-open-data-import");
} else {
  require("../server");
}

function getImportIntervalMs() {
  const minutes = Number(process.env.OPEN_DATA_IMPORT_INTERVAL_MINUTES || 0);
  if (Number.isFinite(minutes) && minutes > 0) {
    return minutes * 60 * 1000;
  }

  const hours = Number(process.env.OPEN_DATA_IMPORT_INTERVAL_HOURS || 0);
  if (Number.isFinite(hours) && hours > 0) {
    return hours * 60 * 60 * 1000;
  }

  return 0;
}

async function runOpenDataImportLoop(intervalMs) {
  let stopping = false;
  let currentChild = null;
  const runOnStart = String(process.env.OPEN_DATA_IMPORT_RUN_ON_START || "true").toLowerCase() !== "false";
  const exitOnError = String(process.env.OPEN_DATA_IMPORT_EXIT_ON_ERROR || "false").toLowerCase() === "true";

  const requestStop = () => {
    stopping = true;
    if (currentChild && !currentChild.killed) {
      currentChild.kill("SIGTERM");
    }
  };

  process.once("SIGTERM", requestStop);
  process.once("SIGINT", requestStop);

  console.log(
    `[open-data-cron] loop started intervalMinutes=${Math.round(intervalMs / 60000)} runOnStart=${runOnStart}`
  );

  if (!runOnStart) {
    await sleep(intervalMs, () => stopping);
  }

  while (!stopping) {
    try {
      currentChild = spawn(process.execPath, [path.join(__dirname, "import-open-data-postgres.js")], {
        env: process.env,
        stdio: "inherit"
      });
      await waitForChild(currentChild);
    } catch (error) {
      if (stopping) {
        return;
      }
      console.error("[open-data-cron] import run failed");
      console.error(error && error.stack ? error.stack : String(error));
      if (exitOnError) {
        process.exitCode = 1;
        return;
      }
    } finally {
      currentChild = null;
    }

    if (!stopping) {
      await sleep(intervalMs, () => stopping);
    }
  }
}

function waitForChild(child) {
  return new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) {
        resolve();
        return;
      }
      if (signal) {
        reject(new Error(`Import process terminated by ${signal}.`));
        return;
      }
      reject(new Error(`Import process exited with code ${code}.`));
    });
  });
}

function sleep(ms, shouldStop) {
  return new Promise((resolve) => {
    const deadline = Date.now() + ms;
    const tick = () => {
      if (shouldStop() || Date.now() >= deadline) {
        resolve();
        return;
      }
      setTimeout(tick, 1000);
    };
    tick();
  });
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
