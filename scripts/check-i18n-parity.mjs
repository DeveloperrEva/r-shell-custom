#!/usr/bin/env node

/**
 * Checks that every locale file in src/locales has the same set of keys as the
 * reference (en.json). Exits with code 1 if any locale has missing or extra keys.
 */

import { readFileSync, readdirSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const localeDir = resolve(__dirname, '..', 'src', 'locales');
const REFERENCE = 'en.json';

function flattenKeys(obj, prefix = '') {
  const keys = [];
  for (const [key, value] of Object.entries(obj)) {
    const fullKey = prefix ? `${prefix}.${key}` : key;
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      keys.push(...flattenKeys(value, fullKey));
    } else {
      keys.push(fullKey);
    }
  }
  return keys;
}

function loadJson(filePath) {
  return JSON.parse(readFileSync(filePath, 'utf-8'));
}

const en = loadJson(resolve(localeDir, REFERENCE));
const enKeys = new Set(flattenKeys(en));

const others = readdirSync(localeDir)
  .filter(f => f.endsWith('.json') && f !== REFERENCE)
  .sort();

let failed = false;
for (const file of others) {
  const keys = new Set(flattenKeys(loadJson(resolve(localeDir, file))));
  const missing = [...enKeys].filter(k => !keys.has(k));
  const extra = [...keys].filter(k => !enKeys.has(k));

  if (missing.length === 0 && extra.length === 0) {
    console.log(`✓ ${file}: parity OK (${enKeys.size} keys).`);
    continue;
  }

  failed = true;
  if (missing.length > 0) {
    console.error(`✗ ${file}: missing ${missing.length} key(s):`);
    missing.forEach(k => console.error(`  - ${k}`));
  }
  if (extra.length > 0) {
    console.error(`✗ ${file}: ${extra.length} extra key(s) not in ${REFERENCE}:`);
    extra.forEach(k => console.error(`  + ${k}`));
  }
}

if (failed) {
  process.exit(1);
} else {
  console.log(`\n✓ All ${others.length} locale(s) match ${REFERENCE} (${enKeys.size} keys).`);
  process.exit(0);
}
