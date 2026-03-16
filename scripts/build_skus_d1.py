#!/usr/bin/env python3
"""
Build Cloudflare D1 skus table from Scryfall bulk data + MTGJSON TcgplayerSkus.

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
import os
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

import requests

SCRYFALL_BULK_META_URL = "https://api.scryfall.com/bulk-data/default-cards"
MTGJSON_SKUS_URL       = "https://mtgjson.com/api/v5/TcgplayerSkus.json.gz"
D1_DATABASE_NAME       = "mtg-rounder-skus"
BATCH_SIZE             = 10_000   # rows per SQL file (D1 import limit ~10k)


def _download_stream(url, label):
    print(f"Downloading {label}...")
    r = requests.get(url, stream=True, timeout=300)
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
    return b"".join(chunks)


def download_scryfall_ids():
    """Return dict: tcgplayer_id (int) -> [scryfall_id, ...]"""
    meta    = requests.get(SCRYFALL_BULK_META_URL, timeout=30).json()
    url     = meta["download_uri"]
    data    = json.loads(_download_stream(url, f"Scryfall default-cards ({url.split('/')[-1]})"))

    tcg_to_scryfall = {}
    for card in data:
        for field in ("tcgplayer_id", "tcgplayer_etched_id"):
            tcg_id = card.get(field)
            if tcg_id:
                tcg_to_scryfall.setdefault(int(tcg_id), []).append(card["id"])

    print(f"  {len(tcg_to_scryfall):,} unique TCGPlayer product IDs from Scryfall")
    return tcg_to_scryfall


def download_mtgjson_skus():
    """Return MTGJSON TcgplayerSkus data dict (uuid -> [sku, ...])."""
    raw  = _download_stream(MTGJSON_SKUS_URL, "MTGJSON TcgplayerSkus")
    data = json.loads(gzip.decompress(raw))["data"]
    print(f"  {sum(len(v) for v in data.values()):,} total SKU entries from MTGJSON")
    return data


def build_sku_map(tcg_to_scryfall, mtgjson_data):
    """Return dict: "{scryfallId}:{FOIL|NON FOIL}:{CONDITION}" -> skuId"""
    sku_map  = {}
    skipped  = 0

    for sku_list in mtgjson_data.values():
        for sku in sku_list:
            lang = sku.get("language", "")
            if lang not in ("ENGLISH", "English"):
                continue

            product_id   = sku.get("productId")
            scryfall_ids = tcg_to_scryfall.get(product_id)
            if not scryfall_ids:
                skipped += 1
                continue

            printing  = sku.get("printing", "NON FOIL")   # "FOIL" or "NON FOIL"
            condition = sku.get("condition", "NEAR_MINT")  # e.g. "NEAR_MINT"
            sku_id    = sku.get("skuId")

            for scryfall_id in scryfall_ids:
                key = f"{scryfall_id}:{printing}:{condition}"
                sku_map[key] = sku_id

    print(f"  {len(sku_map):,} SKU rows built  ({skipped:,} MTGJSON entries had no Scryfall match)")
    return sku_map


def write_sql_batches(sku_map, tmp_dir):
    """Write SQL files in BATCH_SIZE-row chunks. Returns list of file paths."""
    items  = list(sku_map.items())
    files  = []
    today  = date.today().isoformat()

    for batch_num, start in enumerate(range(0, len(items), BATCH_SIZE)):
        batch = items[start : start + BATCH_SIZE]
        fpath = Path(tmp_dir) / f"batch_{batch_num:04d}.sql"

        with open(fpath, "w") as f:
            if batch_num == 0:
                # Schema setup only in first file
                f.write(
                    "CREATE TABLE IF NOT EXISTS skus "
                    "(key TEXT PRIMARY KEY, sku_id INTEGER NOT NULL);\n"
                    "CREATE TABLE IF NOT EXISTS meta "
                    "(key TEXT PRIMARY KEY, value TEXT);\n"
                    "DELETE FROM skus;\n"
                )

            rows = []
            for key, sku_id in batch:
                escaped = key.replace("'", "''")
                rows.append(f"  ('{escaped}', {sku_id})")
            f.write("INSERT OR REPLACE INTO skus (key, sku_id) VALUES\n")
            f.write(",\n".join(rows) + ";\n")

            # Write build date in last batch
            if start + BATCH_SIZE >= len(items):
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

    # Detect wrangler
    try:
        subprocess.run(["wrangler", "--version"], capture_output=True, check=True)
        wrangler_cmd = ["wrangler"]
    except (FileNotFoundError, subprocess.CalledProcessError):
        wrangler_cmd = ["npx", "--yes", "wrangler"]

    print(f"Applying {len(sql_files)} batch file(s) to D1...")
    for i, sql_file in enumerate(sql_files, 1):
        print(f"  Batch {i}/{len(sql_files)}: {Path(sql_file).name}")
        cmd = wrangler_cmd + [
            "d1", "execute", D1_DATABASE_NAME,
            f"--file={sql_file}",
            "--remote",
        ]
        result = subprocess.run(cmd)
        if result.returncode != 0:
            print(f"ERROR on batch {i}", file=sys.stderr)
            sys.exit(1)

    print("D1 sync complete!")


def main():
    parser = argparse.ArgumentParser(description="Sync SKUs to Cloudflare D1")
    parser.add_argument("--dry-run", action="store_true",
                        help="Build SQL files but skip the D1 upload step")
    args = parser.parse_args()

    tcg_to_scryfall = download_scryfall_ids()
    mtgjson_data    = download_mtgjson_skus()
    sku_map         = build_sku_map(tcg_to_scryfall, mtgjson_data)

    with tempfile.TemporaryDirectory() as tmp_dir:
        sql_files = write_sql_batches(sku_map, tmp_dir)
        apply_to_d1(sql_files, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
