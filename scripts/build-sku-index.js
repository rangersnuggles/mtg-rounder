#!/usr/bin/env node
// Generates skus-index.json in the repo root from MTGJSON TcgplayerSkus.json.gz.
// Run: node scripts/build-sku-index.js
// Commit the output; the frontend fetches it as a static file.

const https      = require('https');
const zlib       = require('zlib');
const fs         = require('fs');
const path       = require('path');
const JSONStream = require('JSONStream');

async function main() {
  console.log('Downloading TcgplayerSkus.json.gz from MTGJSON…');

  const index = {};
  let total   = 0;

  await new Promise((resolve, reject) => {
    https.get('https://mtgjson.com/api/v5/TcgplayerSkus.json.gz', res => {
      if (res.statusCode !== 200) {
        reject(new Error(`HTTP ${res.statusCode}`));
        return;
      }

      // JSONStream.parse('data.*') emits each value under the top-level 'data' key —
      // i.e. one array of SKU objects per MTGJSON UUID.
      const stream = JSONStream.parse('data.*');

      stream.on('data', skus => {
        for (const sku of skus) {
          if (sku.language !== 'ENGLISH') continue;
          // Key: "${productId}:${printing}:${condition}"
          // e.g. "641720:FOIL:NEAR MINT" → 8800915
          const key = `${sku.productId}:${sku.printing}:${sku.condition}`;
          index[key] = sku.skuId;
          total++;
        }
      });

      stream.on('end',   resolve);
      stream.on('error', reject);

      res
        .pipe(zlib.createGunzip())
        .pipe(stream);

      res.on('error', reject);
    }).on('error', reject);
  });

  console.log(`Indexed ${total.toLocaleString()} SKUs. Writing file…`);

  const out  = JSON.stringify(index);
  const dest = path.join(__dirname, '..', 'skus-index.json');
  fs.writeFileSync(dest, out, 'utf8');

  const mb = (out.length / 1024 / 1024).toFixed(2);
  console.log(`Done — ${Object.keys(index).length.toLocaleString()} unique keys, ${mb} MB → skus-index.json`);
}

main().catch(err => { console.error(err); process.exit(1); });
