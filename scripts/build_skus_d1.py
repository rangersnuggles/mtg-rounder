#!/usr/bin/env python3
"""
Build Cloudflare D1 skus table from MTGJSON TcgplayerSkus + AllIdentifiers.

Uses MTGJSON's own UUID→scryfallId cross-reference (AllIdentifiers) rather
than Scryfall's tcgplayer_id field, which is missing for many variant cards
(e.g. high-collector-number showcase/borderless/extended-art printings).

Usage:
  python scripts/build_skus_d1.py [--dry-run]

Requires:
  pip install requests
  wrangler in PATH (or installed via npm install -g wrangler)

Environment variables (for CI):
  CLOUDFLARE_API_TOKEN
  CLOUDFLARE_ACCOUNT_ID
"""

import argparse
import gzip
import json
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

import requests

MTGJSON_SKUS_URL        = "https://mtgjson.com/api/v5/TcgplayerSkus.json.gz"
MTGJSON_IDENTIFIERS_URL = "https://mtgjson.com/api/v5/AllIdentifiers.json.gz"
D1_DATABASE_NAME        = "mtg-skus"
STMT_SIZE               = 100   # rows per INSERT statement (avoid SQLITE_TOOBIG)
STMTS_PER_FILE          = 500   # INSERT statements per file → 50k rows/file


def _download_gz_json(url, label):
    print(f"Downloading {label}...")
    r = requests.get(url, stream=True, timeout=600)
    r.raise_for_status()
    chunks = []
    total      = int(r.headers.get("content-length", 0))
    downloaded = 0
    for chunk in r.iter_content(chunk_size=1024 * 512):
        chunks.append(chunk)
        downloaded += len(chunk)
        if total:
            pct = downloaded * 100 // total
            print(f"\r  {pct:3d}%  {downloaded // 1024 // 1024} MB", end="", flush=True)
    print()
    raw = b"".join(chunks)
    return json.loads(gzip.decompress(raw) if url.endswith(".gz") else raw)


def download_mtgjson_uuid_to_card():
    """Return dict: mtgjsonUUID -> {scryfallId, oracleId, setCode, number} from AllIdentifiers."""
    data = _download_gz_json(MTGJSON_IDENTIFIERS_URL, "MTGJSON AllIdentifiers")["data"]

    uuid_to_card = {}
    for mtgjson_uuid, card in data.items():
        ids         = card.get("identifiers", {})
        scryfall_id = ids.get("scryfallId")
        if scryfall_id:
            uuid_to_card[mtgjson_uuid] = {
                "scryfallId": scryfall_id,
                "oracleId":   ids.get("scryfallOracleId", ""),
                "setCode":    card.get("setCode", "").lower(),
                "number":     card.get("number", ""),
            }

    print(f"  {len(uuid_to_card):,} MTGJSON UUIDs mapped")
    return uuid_to_card


def download_mtgjson_skus():
    """Return MTGJSON TcgplayerSkus data dict (mtgjsonUUID -> [sku, ...])."""
    data = _download_gz_json(MTGJSON_SKUS_URL, "MTGJSON TcgplayerSkus")["data"]
    print(f"  {sum(len(v) for v in data.values()):,} total SKU entries")
    return data


def build_sku_map(uuid_to_card, mtgjson_skus):
    """Return dict of D1 keys -> skuId.

    Three key formats per SKU:
      Primary:       "{scryfallId}:{printing}:{condition}"
      Fallback:      "{setCode}:{number}:{printing}:{condition}"
      Oracle xref:   same two formats for same-oracle-same-set variants not in
                     TcgplayerSkus (e.g. showcase/borderless alternate-number cards
                     like Shark Shredder #320 foil whose SKUs live under the #73 UUID)
    """
    sku_map  = {}
    skipped  = 0
    # oracle_set_skus: (oracleId, setCode) -> {(printing, condition) -> skuId}
    oracle_set_skus = {}

    for mtgjson_uuid, sku_list in mtgjson_skus.items():
        card_info = uuid_to_card.get(mtgjson_uuid)
        if not card_info:
            skipped += len(sku_list)
            continue

        scryfall_id = card_info["scryfallId"]
        oracle_id   = card_info["oracleId"]
        set_code    = card_info["setCode"]
        number      = card_info["number"]

        for sku in sku_list:
            lang = sku.get("language", "")
            if lang not in ("ENGLISH", "English"):
                continue

            printing  = sku.get("printing", "NON FOIL")
            condition = sku.get("condition", "NEAR MINT")
            sku_id    = sku.get("skuId")

            # Primary key: scryfallId-based
            sku_map[f"{scryfall_id}:{printing}:{condition}"] = sku_id

            # Fallback key: setCode:collectorNumber-based
            if set_code and number:
                sku_map[f"{set_code}:{number}:{printing}:{condition}"] = sku_id

            # Track by (oracleId, setCode) for cross-reference pass below
            if oracle_id and set_code:
                oracle_set_skus.setdefault((oracle_id, set_code), {})[
                    (printing, condition)
                ] = sku_id

    # Cross-reference pass: for any UUID in AllIdentifiers that shares an oracleId+setCode
    # with a UUID that HAS TcgplayerSkus data but itself has no SKU entry, add D1 keys
    # so that both the variant's scryfallId and its collector number resolve correctly.
    # This covers showcase/borderless/extended-art cards like Shark Shredder #320 foil.
    xref_added = 0
    for mtgjson_uuid, card_info in uuid_to_card.items():
        if mtgjson_uuid in mtgjson_skus:
            continue  # already handled above

        oracle_id = card_info["oracleId"]
        set_code  = card_info["setCode"]
        if not oracle_id or not set_code:
            continue

        skus_for_oracle = oracle_set_skus.get((oracle_id, set_code))
        if not skus_for_oracle:
            continue

        scryfall_id = card_info["scryfallId"]
        number      = card_info["number"]

        for (printing, condition), sku_id in skus_for_oracle.items():
            sku_map[f"{scryfall_id}:{printing}:{condition}"] = sku_id
            if set_code and number:
                sku_map[f"{set_code}:{number}:{printing}:{condition}"] = sku_id
            xref_added += 1

    print(f"  {len(sku_map):,} SKU rows built  "
          f"({skipped:,} SKUs had no mapping, {xref_added:,} added via oracle cross-ref)")
    return sku_map


def write_sql_batches(sku_map, tmp_dir):
    """Write SQL files, each containing many small INSERT statements."""
    items         = list(sku_map.items())
    today         = date.today().isoformat()
    rows_per_file = STMT_SIZE * STMTS_PER_FILE
    files         = []

    for file_num, file_start in enumerate(range(0, len(items), rows_per_file)):
        file_rows = items[file_start : file_start + rows_per_file]
        fpath     = Path(tmp_dir) / f"batch_{file_num:04d}.sql"
        is_last   = (file_start + rows_per_file) >= len(items)

        with open(fpath, "w") as f:
            if file_num == 0:
                f.write(
                    "CREATE TABLE IF NOT EXISTS skus "
                    "(key TEXT PRIMARY KEY, sku_id INTEGER NOT NULL);\n"
                    "CREATE TABLE IF NOT EXISTS meta "
                    "(key TEXT PRIMARY KEY, value TEXT);\n"
                    "DELETE FROM skus;\n"
                )

            for stmt_start in range(0, len(file_rows), STMT_SIZE):
                chunk = file_rows[stmt_start : stmt_start + STMT_SIZE]
                rows  = []
                for key, sku_id in chunk:
                    escaped = key.replace("'", "''")
                    rows.append(f"  ('{escaped}', {sku_id})")
                f.write("INSERT OR REPLACE INTO skus (key, sku_id) VALUES\n")
                f.write(",\n".join(rows) + ";\n")

            if is_last:
                f.write(
                    f"INSERT OR REPLACE INTO meta (key, value) "
                    f"VALUES ('built', '{today}');\n"
                )

        files.append(str(fpath))

    print(f"  {len(files)} SQL batch file(s) ({len(items):,} rows total)")
    return files


def apply_to_d1(sql_files, dry_run=False):
    if dry_run:
        print(f"[DRY RUN] Would apply {len(sql_files)} SQL file(s) to D1 '{D1_DATABASE_NAME}'")
        return

    try:
        subprocess.run(["wrangler", "--version"], capture_output=True, check=True)
        wrangler_cmd = ["wrangler"]
    except (FileNotFoundError, subprocess.CalledProcessError):
        wrangler_cmd = ["npx", "--yes", "wrangler"]

    print(f"Applying {len(sql_files)} batch file(s) to D1...")
    for i, sql_file in enumerate(sql_files, 1):
        print(f"  Batch {i}/{len(sql_files)}: {Path(sql_file).name}")
        cmd    = wrangler_cmd + ["d1", "execute", D1_DATABASE_NAME, f"--file={sql_file}", "--remote"]
        result = subprocess.run(cmd)
        if result.returncode != 0:
            print(f"ERROR on batch {i}", file=sys.stderr)
            sys.exit(1)

    print("D1 sync complete!")


def main():
    parser = argparse.ArgumentParser(description="Sync SKUs to Cloudflare D1")
    parser.add_argument("--dry-run", action="store_true", help="Skip D1 upload step")
    args = parser.parse_args()

    uuid_to_card = download_mtgjson_uuid_to_card()
    mtgjson_skus = download_mtgjson_skus()
    sku_map      = build_sku_map(uuid_to_card, mtgjson_skus)

    with tempfile.TemporaryDirectory() as tmp_dir:
        sql_files = write_sql_batches(sku_map, tmp_dir)
        apply_to_d1(sql_files, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
