"""
pierre_monthly_run.py — automates the monthly retail_cdao run.

Does end-to-end:
  1. SFTP your monthly CSV from avalonwinscp.fnb.co.za
  2. Hash the PII columns (cust_id_reg_no, EMAIL_ADDR, CUST_CELL_NO) with SHA-256
  3. Convert CSV -> Parquet in chunks (memory-flat on any laptop)
  4. Upload the parquet to gs://customer_spend_data/

Override defaults with CLI flags or environment variables.

Run via the wrapper:  bash pierre_monthly_run.sh --stamp 20260512
Or directly:          python3 pierre_monthly_run.py --stamp 20260512

Required env / .env:
  AD_USERNAME            Your FNB AD username (e.g. f3799182)
  AD_PASSWORD            Your FNB AD password   (DOMAIN_PW is also accepted)

Optional env:
  SFTP_HOST              default: avalonwinscp.fnb.co.za
  REMOTE_DIR             default: /data/fnb/retail_sales_and_cdao/Pierre/
  STEM                   default: burger
  GCP_PROJECT            default: fmn-sandbox
  GCP_BUCKET             default: customer_spend_data
  TEST_BUCKET            set this to your test bucket name, then pass --test
"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import logging
import os
import sys
import time
from pathlib import Path
from typing import List

LOG = logging.getLogger("pierre_monthly")

# ── Defaults (override with env vars or CLI) ─────────────────────────────────
DEFAULTS = {
    "sftp_host":   "avalonwinscp.fnb.co.za",
    "sftp_port":   "22",
    "remote_dir":  "/data/fnb/retail_sales_and_cdao/Pierre/",
    "stem":        "burger",        # file stem; full name = <stem>_<YYYYMMDD>.csv
    "out_dir":     "./out",         # local cache for CSV + parquet
    "gcp_project": "fmn-sandbox",
    "gcp_bucket":  "customer_spend_data",
    # Test bucket — set TEST_BUCKET in env/.env to your test bucket name.
    # Used when --test is passed (so the prod bucket isn't touched during smoke tests).
    "test_bucket": "",              # e.g. "customer_spend_data_test"
    "chunksize":   "500000",        # rows per CSV chunk
    "compression": "snappy",        # parquet compression
}

# Columns we hash (PII). These are the three string columns in
# customer_spend.base_data + cust_id_reg_no from the SAS export.
HASH_COLUMNS = ["cust_id_reg_no", "EMAIL_ADDR", "CUST_CELL_NO"]

# Types we enforce per chunk so the parquet schema matches what BigQuery expects.
# Found via inspect_source.sh on customer_spend.base_data.
INT_COLS   = ["demo_1", "demo_4", "demo_8"]
FLOAT_COLS = ["demo_2", "demo_3", "demo_7"]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _cfg(key: str) -> str:
    val = os.getenv(key.upper())
    if val:
        return val
    return DEFAULTS.get(key, "")


def _hash_value(val) -> str:
    """SHA-256 of a value (lowercased, spaces stripped). Empty/null passes through."""
    import pandas as pd
    if pd.isna(val):
        return val
    s = str(val).lower().replace(" ", "")
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _die_on_missing_deps() -> None:
    required = {
        "paramiko":            "paramiko",
        "pandas":              "pandas",
        "pyarrow":             "pyarrow",
        "google.cloud.storage":"google-cloud-storage",
    }
    missing = []
    for mod, pip_name in required.items():
        try:
            __import__(mod)
        except ImportError:
            missing.append(pip_name)
    if missing:
        print("Missing Python packages:", ", ".join(missing), file=sys.stderr)
        print(f"\n    pip install {' '.join(missing)}\n", file=sys.stderr)
        sys.exit(2)


# ── SFTP ──────────────────────────────────────────────────────────────────────

def sftp_get(remote_path: str, local_path: Path, user: str, pw: str) -> None:
    import paramiko

    host = _cfg("sftp_host")
    port = int(_cfg("sftp_port"))

    LOG.info("SFTP %s:%s as %s", host, port, user)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(
            hostname=host, port=port, username=user, password=pw,
            timeout=30, allow_agent=False, look_for_keys=False,
        )
        sftp = ssh.open_sftp()
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            LOG.info("  Downloading %s -> %s", remote_path, local_path)
            t0 = time.time()
            sftp.get(remote_path, str(local_path))
            size_mb = local_path.stat().st_size / 1e6
            LOG.info("  Done — %.1f MB in %.1fs", size_mb, time.time() - t0)
        finally:
            sftp.close()
    finally:
        ssh.close()


# ── CSV -> Parquet with hashing ──────────────────────────────────────────────

def csv_to_parquet(csv_path: Path, parquet_path: Path) -> int:
    """
    Stream the CSV in chunks. For each chunk:
      - strip BOM and whitespace from column names
      - cast int_cols / float_cols
      - parse EFF_DATE if present
      - SHA-256 hash any HASH_COLUMNS that exist
      - append to the parquet file
    Returns total rows written.
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    chunksize = int(_cfg("chunksize"))
    compression = _cfg("compression")

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    if parquet_path.exists():
        parquet_path.unlink()

    LOG.info("Converting %s -> %s  (chunksize=%d, compression=%s)",
             csv_path.name, parquet_path.name, chunksize, compression)

    writer = None
    total = 0
    t0 = time.time()

    for chunk in pd.read_csv(
        csv_path,
        chunksize=chunksize,
        encoding="ascii",
        engine="python",
        encoding_errors="replace",
        sep=",",
    ):
        # Clean column names — strip BOM + whitespace
        chunk.columns = (
            chunk.columns.astype(str)
            .str.replace("\ufeff", "", regex=False)
            .str.strip()
        )

        # Cast numeric columns where present
        for col in FLOAT_COLS:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce").astype("float64")
        for col in INT_COLS:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce").astype("Int64")

        # Parse EFF_DATE if present
        if "EFF_DATE" in chunk.columns:
            chunk["EFF_DATE"] = pd.to_datetime(
                chunk["EFF_DATE"], format="%d%b%Y", errors="coerce"
            ).dt.date

        # Hash PII
        for col in HASH_COLUMNS:
            if col in chunk.columns:
                chunk[col] = chunk[col].apply(_hash_value)

        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(parquet_path, table.schema, compression=compression)
        writer.write_table(table)

        total += len(chunk)
        LOG.info("  ... %s rows", f"{total:,}")

    if writer is not None:
        writer.close()

    size_mb = parquet_path.stat().st_size / 1e6
    LOG.info("Wrote %s rows -> %s  (%.1f MB in %.1fs)",
             f"{total:,}", parquet_path.name, size_mb, time.time() - t0)
    return total


# ── GCS upload ────────────────────────────────────────────────────────────────

def upload_to_gcs(local_path: Path, bucket_name: str, remote_name: str = None) -> str:
    from google.cloud import storage

    project = _cfg("gcp_project")
    remote_name = remote_name or local_path.name
    uri = f"gs://{bucket_name}/{remote_name}"

    LOG.info("Uploading %s -> %s", local_path.name, uri)
    t0 = time.time()
    client = storage.Client(project=project) if project else storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(remote_name)
    blob.upload_from_filename(str(local_path))
    size_mb = local_path.stat().st_size / 1e6
    LOG.info("  Done — %.1f MB in %.1fs", size_mb, time.time() - t0)
    return uri


# ── Credential resolution ────────────────────────────────────────────────────

def get_ad_credentials() -> tuple[str, str]:
    # Allow .env
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    user = os.getenv("AD_USERNAME", "").strip()
    pw = os.getenv("AD_PASSWORD") or os.getenv("DOMAIN_PW") or ""

    if not user:
        user = input("  AD username (e.g. f3799182): ").strip()
    if not pw:
        pw = getpass.getpass("  AD password (hidden as you type): ")

    if not user or not pw:
        LOG.error("Missing AD credentials. Set AD_USERNAME + AD_PASSWORD.")
        sys.exit(1)
    return user, pw


# ── Main ──────────────────────────────────────────────────────────────────────

def main(argv: list = None) -> int:
    parser = argparse.ArgumentParser(
        description="Monthly retail_cdao run — SFTP, hash, parquet, upload."
    )
    parser.add_argument("--stamp", default=None,
                        help="YYYYMMDD date stamp (default: today's date)")
    parser.add_argument("--stem", default=None,
                        help=f"File stem (default: {DEFAULTS['stem']})")
    parser.add_argument("--remote-dir", default=None,
                        help=f"Remote SFTP folder (default: {DEFAULTS['remote_dir']})")
    parser.add_argument("--bucket", default=None,
                        help=f"GCS bucket (default: {DEFAULTS['gcp_bucket']})")
    parser.add_argument("--test", action="store_true",
                        help="Upload to TEST bucket (default if TEST_BUCKET is set and --prod not used)")
    parser.add_argument("--prod", action="store_true",
                        help="Force upload to PROD bucket (override safe default)")
    parser.add_argument("-y", "--yes", action="store_true",
                        help="Skip the confirmation prompt before uploading")
    parser.add_argument("--out-dir", default=None,
                        help=f"Local cache folder (default: {DEFAULTS['out_dir']})")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do everything except the GCS upload — dry run")
    parser.add_argument("--keep-csv", action="store_true",
                        help="Don't delete the local CSV after parquet conversion")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    _die_on_missing_deps()

    # Default stamp = today's date (YYYYMMDD)
    if not args.stamp:
        args.stamp = time.strftime("%Y%m%d")
        LOG.info("No --stamp given; using today's date: %s", args.stamp)

    stem       = args.stem       or _cfg("stem")
    remote_dir = args.remote_dir or _cfg("remote_dir")
    out_dir    = Path(args.out_dir or _cfg("out_dir")).expanduser()

    # Bucket resolution priority (safety-first):
    #   --bucket flag           → that exact bucket, always wins
    #   --prod                  → prod bucket explicitly
    #   --test                  → test bucket
    #   TEST_BUCKET in .env     → test bucket (SAFE DEFAULT — won't touch prod)
    #   nothing set             → prod bucket
    test_bucket = _cfg("test_bucket")

    if args.bucket:
        bucket = args.bucket
        bucket_label = f"CUSTOM: {bucket}"
    elif args.prod:
        bucket = _cfg("gcp_bucket")
        bucket_label = f"PROD: {bucket}"
    elif args.test:
        if not test_bucket:
            LOG.error("--test passed but TEST_BUCKET is not set in .env.")
            sys.exit(1)
        bucket = test_bucket
        bucket_label = f"TEST: {bucket}"
    elif test_bucket:
        bucket = test_bucket
        bucket_label = f"TEST (safe default): {bucket}"
        LOG.info("TEST_BUCKET is set — defaulting to test bucket. Use --prod to push to prod.")
    else:
        bucket = _cfg("gcp_bucket")
        bucket_label = f"PROD: {bucket}"

    file_basename = f"{stem}_{args.stamp}"
    remote_path   = remote_dir.rstrip("/") + "/" + file_basename + ".csv"
    local_csv     = out_dir / f"{file_basename}.csv"
    local_parq    = out_dir / f"{file_basename}.parquet"

    # ── Preview & confirm ────────────────────────────────────────────────────
    print()
    print("═" * 60)
    print("  Monthly retail_cdao run")
    print("═" * 60)
    print(f"  Date stamp:    {args.stamp}")
    print(f"  Source CSV:    {remote_path}")
    print(f"  Local cache:   {local_csv}")
    print(f"  Will hash:     {', '.join(HASH_COLUMNS)}")
    print(f"  Output parq:   {local_parq.name}")
    if args.skip_upload:
        print(f"  Upload:        SKIPPED (--skip-upload)")
    else:
        print(f"  Upload to:     {bucket_label}")
    print("═" * 60)
    print()

    if not args.skip_upload and not args.yes:
        try:
            ans = input("Proceed? [y/N] ").strip().lower()
        except EOFError:
            ans = "n"
        if ans not in ("y", "yes"):
            print("Cancelled. (Pass --yes to skip this prompt.)")
            return 0
        print()

    # 1. SFTP
    user, pw = get_ad_credentials()
    sftp_get(remote_path, local_csv, user, pw)

    # 2 + 3. Hash + parquet
    rows = csv_to_parquet(local_csv, local_parq)
    LOG.info("Parquet ready: %s rows", f"{rows:,}")

    # 4. Upload (or skip)
    if args.skip_upload:
        LOG.info("--skip-upload set, leaving parquet at %s", local_parq)
    else:
        upload_to_gcs(local_parq, bucket)

    # 5. Tidy CSV
    if not args.keep_csv:
        try:
            local_csv.unlink()
            LOG.info("Cleaned up local CSV (use --keep-csv to retain).")
        except FileNotFoundError:
            pass

    LOG.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
