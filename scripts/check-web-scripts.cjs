#!/usr/bin/env node
// Parse shipped JavaScript, including inline dashboard scripts, without running it.
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.resolve(__dirname, "../custom_components/akuvox_ac/www");
let checked = 0;
for (const name of fs.readdirSync(root).sort()) {
  if (!/\.(html|js)$/.test(name)) continue;
  const source = fs.readFileSync(path.join(root, name), "utf8");
  const scripts = name.endsWith(".js")
    ? [source]
    : [...source.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script\s*>/gi)]
      .filter((match) => !/\bsrc\s*=|application\/json|application\/ld\+json/i.test(match[1]))
      .map((match) => match[2]);
  for (const [index, script] of scripts.entries()) {
    new vm.Script(script, { filename: `${name}:script-${index + 1}` });
    checked += 1;
  }
}
console.log(`Parsed ${checked} dashboard scripts successfully.`);
