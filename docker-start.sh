#!/usr/bin/env bash
set -euo pipefail

echo "[boot] node $(node -v)"
echo "[boot] npm $(npm -v)"
echo "[boot] PORT=${PORT:-unset}"
echo "[boot] PLAYWRIGHT_BROWSERS_PATH=${PLAYWRIGHT_BROWSERS_PATH:-unset}"

if ! command -v xvfb-run >/dev/null 2>&1; then
  echo "[boot] xvfb-run not found"
  exit 1
fi

echo "[boot] starting app via xvfb-run"
exec xvfb-run -a npm start
