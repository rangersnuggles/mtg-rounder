// Cloudflare Pages Function — POST /api/skus
// Input:  { cards: [{ scryfallId, foil, condition }, ...] }
// Output: { skus: { "scryfallId:foil|normal": skuId, ... } }
// D1 key format: "{scryfallId}:{FOIL|NON FOIL}:{NEAR_MINT|LIGHTLY_PLAYED|...}"

const COND_MAP = {
  near_mint:         'NEAR MINT',
  lightly_played:    'LIGHTLY PLAYED',
  moderately_played: 'MODERATELY PLAYED',
  heavily_played:    'HEAVILY PLAYED',
  damaged:           'DAMAGED',
};

const CORS_HEADERS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

export async function onRequestOptions() {
  return new Response(null, { status: 204, headers: CORS_HEADERS });
}

export async function onRequestPost({ request, env }) {
  try {
    const { cards } = await request.json();
    if (!Array.isArray(cards)) {
      return Response.json({ error: 'Expected { cards: [...] }' }, { status: 400, headers: CORS_HEADERS });
    }

    // Build D1 key → response key mapping
    const keyToCard = {};
    for (const { scryfallId, foil, condition } of cards) {
      if (!scryfallId) continue;
      const printing = foil ? 'FOIL' : 'NON FOIL';
      const cond     = COND_MAP[condition] || 'NEAR_MINT';
      const d1key    = `${scryfallId}:${printing}:${cond}`;
      keyToCard[d1key] = `${scryfallId}:${foil ? 'foil' : 'normal'}`;
    }

    const keys = Object.keys(keyToCard);
    if (keys.length === 0) {
      return Response.json({ skus: {} }, { headers: CORS_HEADERS });
    }

    // Query D1 in batches of 100 (D1 bind limit)
    const BATCH = 100;
    const result = {};
    for (let i = 0; i < keys.length; i += BATCH) {
      const batch        = keys.slice(i, i + BATCH);
      const placeholders = batch.map(() => '?').join(',');
      const { results }  = await env.DB
        .prepare(`SELECT key, sku_id FROM skus WHERE key IN (${placeholders})`)
        .bind(...batch)
        .all();

      for (const row of results) {
        const cardKey = keyToCard[row.key];
        if (cardKey) result[cardKey] = row.sku_id;
      }
    }

    return Response.json({ skus: result }, { headers: CORS_HEADERS });
  } catch (err) {
    return Response.json({ error: err.message }, { status: 500, headers: CORS_HEADERS });
  }
}
