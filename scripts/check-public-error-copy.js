#!/usr/bin/env node

const { describeLookupFailure, getLookupRuntimeStatus } = require("../vehicle-service");

const payload = describeLookupFailure(
  "EL828CC",
  {
    queryType: "plate",
    runtime: getLookupRuntimeStatus(),
    attempts: [
      {
        source: "plate-resolution-cache",
        status: "miss",
        detail: "SPZ neni v lokalni cache resolveru."
      },
      {
        source: "pvzp-browser",
        status: "error",
        detail: "PVZP fallback: browserType.launch: EPERM: operation not permitted, mkdtemp 'C:\\tmp\\playwright-artifacts-XXXXXX'"
      },
      {
        source: "uniqa-browser",
        status: "error",
        detail: "UNIQA fallback: browserType.launch: EPERM: operation not permitted, mkdtemp 'C:\\tmp\\playwright-artifacts-XXXXXX'"
      },
      {
        source: "transport-cube",
        status: "missing_config",
        detail: "Chybi TRANSPORT_CUBE_LOOKUP_URL."
      }
    ]
  },
  "plate"
);

const serialized = JSON.stringify(payload);
const bannedPatterns = [
  /\bPVZP\b/i,
  /\bUNIQA\b/i,
  /browserType\.launch/i,
  /mkdtemp/i,
  /TRANSPORT_CUBE_LOOKUP_URL/i,
  /\bEPERM\b/i
];
const failures = bannedPatterns
  .filter((pattern) => pattern.test(serialized))
  .map((pattern) => pattern.toString());

if (failures.length > 0) {
  console.error("[error-copy] public error payload leaked internal details");
  console.error(JSON.stringify({ failures, payload }, null, 2));
  process.exit(1);
}

console.log(JSON.stringify({
  ok: true,
  message: payload.message,
  hints: payload.hints
}, null, 2));
