#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");

const version = String(process.argv[2] || "").trim();
if (!/^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/.test(version)) {
  console.error("Usage: node scripts/set-release-version.cjs <semver>");
  process.exit(1);
}

const root = path.resolve(__dirname, "..");
const manifestPath = path.join(root, "custom_components", "akuvox_ac", "manifest.json");
const constPath = path.join(root, "custom_components", "akuvox_ac", "const.py");

const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
manifest.version = version;
fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);

let constants = fs.readFileSync(constPath, "utf8");
constants = constants.replace(
  /^INTEGRATION_VERSION = ".*"$/m,
  `INTEGRATION_VERSION = "${version}"`,
);
constants = constants.replace(
  /^INTEGRATION_VERSION_LABEL = ".*"$/m,
  `INTEGRATION_VERSION_LABEL = "${version}"`,
);
fs.writeFileSync(constPath, constants);
