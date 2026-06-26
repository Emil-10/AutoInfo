const fs = require("fs");
const path = require("path");
const vm = require("vm");
const { createRequire } = require("module");

const root = path.resolve(__dirname, "..");
const servicePath = path.join(root, "vehicle-service.js");
const projectRequire = createRequire(servicePath);

const check = `
const base = {
  records: [
    {
      type: "P - Regular",
      state: "A",
      validFrom: "2026-06-09T00:00:00.000Z",
      current: false
    }
  ]
};
const vinRecords = [
  {
    type: "P - Regular",
    state: "A",
    validFrom: "2026-06-09T00:00:00.000Z",
    odometer: 120000,
    odometerUnit: "km",
    source: "OpenDataLab STK portal",
    sourceId: "regular"
  },
  {
    type: "E - Evidence",
    state: "A",
    validFrom: "2026-06-09T00:00:00.000Z",
    odometer: 120961,
    odometerUnit: "km",
    source: "OpenDataLab STK portal",
    sourceId: "evidence"
  }
];
const result = mergeOpenDataLabInspectionRecords(base, vinRecords, "TESTVIN1234567890");
const ids = result.records.map((record) => record.sourceId).filter(Boolean).sort();
if (!ids.includes("regular") || !ids.includes("evidence")) {
  throw new Error("VIN STK merge dropped a same-day record: " + ids.join(","));
}
const evidence = result.records.find((record) => record.sourceId === "evidence");
if (!evidence || evidence.odometer !== 120961) {
  throw new Error("VIN STK merge lost the evidence-check odometer");
}
`;

vm.runInNewContext(fs.readFileSync(servicePath, "utf8") + check, {
  require: projectRequire,
  console,
  process,
  Buffer,
  setTimeout,
  clearTimeout,
  setInterval,
  clearInterval,
  __dirname: root,
  __filename: servicePath,
  module: { exports: {} },
  exports: {}
});
