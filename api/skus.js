const https = require('https');
const zlib  = require('zlib');

// Module-level cache (survives warm invocations)
let index     = null;
let cacheTime = 0;
const TTL     = 12 * 60 * 60 * 1000; // 12 hours

function fetchGzippedJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'Accept-Encoding': 'gzip' } }, res => {
      const chunks  = [];
      const gunzip  = zlib.createGunzip();
      res.pipe(gunzip);
      gunzip.on('data',  c => chunks.push(c));
      gunzip.on('end',   () => resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))));
      gunzip.on('error', reject);
      res.on('error',    reject);
    }).on('error', reject);
  });
}

async function getIndex() {
  if (index && Date.now() - cacheTime < TTL) return index;

  const json = await fetchGzippedJson('https://mtgjson.com/api/v5/TcgplayerSkus.json.gz');

  // Build reverse index keyed by `${productId}:FOIL` or `${productId}:NON FOIL`
  // Only include Near Mint English SKUs (the only condition used for fresh TCGPlayer imports)
  const idx = {};
  for (const skus of Object.values(json.data)) {
    for (const sku of skus) {
      if (sku.condition === 'NEAR_MINT' && sku.language === 'English') {
        idx[`${sku.productId}:${sku.printing}`] = sku.skuId;
      }
    }
  }

  index     = idx;
  cacheTime = Date.now();
  return idx;
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST')    return res.status(405).json({ error: 'Method not allowed' });

  try {
    const { cards } = req.body || {};
    if (!Array.isArray(cards)) return res.status(400).json({ error: 'Expected { cards: [...] }' });

    const idx    = await getIndex();
    const result = {};

    for (const { scryfallId, productId, foil } of cards) {
      if (!scryfallId || !productId) continue;
      const printing = foil ? 'FOIL' : 'NON FOIL';
      const skuId    = idx[`${productId}:${printing}`];
      if (skuId) result[`${scryfallId}:${foil ? 'foil' : 'normal'}`] = skuId;
    }

    res.status(200).json({ skus: result });
  } catch (err) {
    console.error('skus error:', err);
    res.status(500).json({ error: err.message });
  }
};
