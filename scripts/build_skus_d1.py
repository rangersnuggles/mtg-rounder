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
D1_DATABASE_NAME        = "mtg-rounder-skus"
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


def download_mtgjson_uuid_to_scryfall():
    """Return dict: mtgjsonUUID -> scryfallId using MTGJSON AllIdentifiers."""
    data = _download_gz_json(MTGJSON_IDENTIFIERS_URL, "MTGJSON AllIdentifiers")["data"]

    uuid_to_scryfall = {}
    for mtgjson_uuid, card in data.items():
        scryfall_id = card.get("identifiers", {}).get("scryfallId")
        if scryfall_id:
            uuid_to_scryfall[mtgjson_uuid] = scryfall_id

    print(f"  {len(uuid_to_scryfall):,} MTGJSON UUIDs mapped to Scryfall IDs")
    return uuid_to_scryfall


def download_mtgjson_skus():
    """Return MTGJSON TcgplayerSkus data dict (mtgjsonUUID -> [sku, ...])."""
    data = _download_gz_json(MTGJSON_SKUS_URL, "MTGJSON TcgplayerSkus")["data"]
    print(f"  {sum(len(v) for v in data.values()):,} total SKU entries")
    return data


def build_sku_map(uuid_to_scryfall, mtgjson_skus):
    """Return dict: "{scryfallId}:{FOIL|NON FOIL}:{CONDITION}" -> skuId"""
    sku_map = {}
    skipped = 0

    for mtgjson_uuid, sku_list in mtgjson_skus.items():
        scryfall_id = uuid_to_scryfall.get(mtgjson_uuid)
        if not scryfall_id:
            skipped += len(sku_list)
            continue

        for sku in sku_list:
            lang = sku.get("language", "")
            if lang not in ("ENGLISH", "English"):
                continue

            printing  = sku.get("printing", "NON FOIL")   # "FOIL" or "NON FOIL"
            condition = sku.get("condition", "NEAR MINT")  # e.g. "NEAR MINT"
            sku_id    = sku.get("skuId")

            key = f"{scryfall_id}:{printing}:{condition}"
            sku_map[key] = sku_id

    print(f"  {len(sku_map):,} SKU rows built  ({skipped:,} SKUs had no Scryfall ID match)")
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

    uuid_to_scryfall = download_mtgjson_uuid_to_scryfall()
    mtgjson_skus     = download_mtgjson_skus()
    sku_map          = build_sku_map(uuid_to_scryfall, mtgjson_skus)

    with tempfile.TemporaryDirectory() as tmp_dir:
        sql_files = write_sql_batches(sku_map, tmp_dir)
        apply_to_d1(sql_files, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
