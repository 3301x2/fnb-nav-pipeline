"""
Retail Sales CDAO - Monthly GCS Upload
======================================

Pulls the pre-landed monthly table(s) from BI_SANDBOX on RSD-RBSQLDEV, writes
them to CSV, and uploads to the fmn-sandbox GCS bucket.

First-time setup: see README.md.

Quick runs
----------
    python retail_cdao_upload.py --doctor              # run all pre-flight checks, no data work
    python retail_cdao_upload.py --stamp 202601        # pulls BASE202601
    python retail_cdao_upload.py --stamp 202601 --tables BASE202601 TRNS202601
    python retail_cdao_upload.py --stamp 202601 --skip-upload   # dry run, CSVs only
"""

from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────────────
# EDIT HERE IF YOUR SETUP DIFFERS
# Everything below can also be overridden via environment variables (same name,
# upper-cased) or CLI flags. You shouldn't need to touch the rest of the file.
# ──────────────────────────────────────────────────────────────────────────────
DEFAULTS = {
    "sql_server":   "RSD-RBSQLDEV",
    "sql_port":     "1433",
    "sql_database": "BI_SANDBOX",
    "sql_schema":   "dbo",
    "sql_driver":   "ODBC Driver 17 for SQL Server",

    "gcp_project":  "fmn-sandbox",
    "gcp_bucket":   "customer_spend_data",

    # Where to cache CSVs on disk before upload
    "out_dir":      "./out",

    # Optional: path to a GCP service-account JSON.
    # Leave as None to use gcloud ADC (recommended for interactive use).
    "gcp_sa_key":   None,
}
# ──────────────────────────────────────────────────────────────────────────────

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

LOG = logging.getLogger("retail_cdao_upload")


# ──────────────────────────────────────────────────────────────────────────────
# Dependency guard — runs before any heavy imports so errors are friendly
# ──────────────────────────────────────────────────────────────────────────────

REQUIRED_PACKAGES = {
    "pandas":               "pandas",
    "pyodbc":               "pyodbc",
    "google.cloud.storage": "google-cloud-storage",
    "google.auth":          "google-auth",
}


def check_dependencies() -> List[str]:
    """Return a list of missing pip package names."""
    missing = []
    for module, pip_name in REQUIRED_PACKAGES.items():
        try:
            __import__(module)
        except ImportError:
            missing.append(pip_name)
    return missing


def _die_on_missing_deps() -> None:
    missing = check_dependencies()
    if not missing:
        return
    print("Missing Python packages:", ", ".join(missing), file=sys.stderr)
    print("\nInstall them with:\n", file=sys.stderr)
    print(f"    pip install {' '.join(missing)}\n", file=sys.stderr)
    if "pyodbc" in missing:
        print("If pyodbc fails to install on Linux, you also need:", file=sys.stderr)
        print("    sudo apt-get install unixodbc-dev msodbcsql17\n", file=sys.stderr)
    sys.exit(2)


# ──────────────────────────────────────────────────────────────────────────────
# Config resolution (DEFAULTS  →  env vars  →  CLI flags)
# ──────────────────────────────────────────────────────────────────────────────

def _cfg(key: str) -> Optional[str]:
    """Read DEFAULTS[key], with env-var override (upper-cased key)."""
    env_val = os.getenv(key.upper())
    if env_val not in (None, ""):
        return env_val
    return DEFAULTS.get(key)


# ──────────────────────────────────────────────────────────────────────────────
# SQL Server
# ──────────────────────────────────────────────────────────────────────────────

def _sql_connection_string() -> str:
    server = _cfg('sql_server')
    port = _cfg('sql_port') or "1433"
    parts = [
        f"DRIVER={{{_cfg('sql_driver')}}}",
        f"SERVER={server},{port}",
        f"DATABASE={_cfg('sql_database')}",
        "Encrypt=yes",
        "TrustServerCertificate=yes",
    ]
    sql_user = os.getenv("SQL_USER")
    sql_pass = os.getenv("SQL_PASSWORD")
    if sql_user and sql_pass:
        parts += [f"UID={sql_user}", f"PWD={sql_pass}"]
    else:
        parts.append("Trusted_Connection=yes")  # Windows auth
    return ";".join(parts) + ";"


def sql_connect():
    import pyodbc

    # Surface a clear error if the ODBC driver isn't installed
    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    if _cfg("sql_driver") not in drivers:
        raise RuntimeError(
            f"ODBC driver '{_cfg('sql_driver')}' not found.\n"
            f"  Installed SQL Server drivers: {drivers or '(none)'}\n"
            f"  Install 'ODBC Driver 17 for SQL Server' from Microsoft, or set\n"
            f"  SQL_DRIVER to one of the drivers above."
        )

    auth_mode = "Windows auth" if not os.getenv("SQL_USER") else f"SQL auth as {os.getenv('SQL_USER')}"
    LOG.info("Connecting to %s:%s/%s  (%s)",
             _cfg("sql_server"), _cfg("sql_port") or "1433", _cfg("sql_database"), auth_mode)
    try:
        return pyodbc.connect(_sql_connection_string(), timeout=15)
    except pyodbc.InterfaceError as e:
        raise RuntimeError(
            f"Could not reach SQL Server {_cfg('sql_server')}.\n"
            f"  - Are you on VPN / the FNB corporate network?\n"
            f"  - Can you ping {_cfg('sql_server')}?\n"
            f"  - Original error: {e}"
        ) from e
    except pyodbc.Error as e:
        raise RuntimeError(
            f"SQL Server login failed on {_cfg('sql_server')}.\n"
            f"  - Confirm your AD account has db_datareader on {_cfg('sql_database')}.\n"
            f"  - For SQL auth, set SQL_USER and SQL_PASSWORD env vars.\n"
            f"  - Original error: {e}"
        ) from e


def table_exists(conn, table: str, schema: Optional[str] = None) -> bool:
    schema = schema or _cfg("sql_schema")
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        schema, table,
    )
    return cur.fetchone() is not None


def fetch_to_csv(conn, table: str, csv_path: Path, chunksize: int = 50_000) -> int:
    """Stream table -> CSV in chunks. Returns total rows written."""
    import pandas as pd

    schema = _cfg("sql_schema")
    sql = f"SELECT * FROM [{schema}].[{table}]"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    LOG.info("Reading [%s].[%s] in chunks of %d ...", schema, table, chunksize)
    t0 = time.time()
    total = 0
    first = True
    for chunk in pd.read_sql(sql, conn, chunksize=chunksize):
        chunk.to_csv(
            csv_path,
            index=False,
            mode="w" if first else "a",
            header=first,
        )
        total += len(chunk)
        first = False
        LOG.info("  ... %d rows written", total)

    LOG.info("Wrote %d rows -> %s  (%.1fs)", total, csv_path, time.time() - t0)
    return total


# ──────────────────────────────────────────────────────────────────────────────
# Google Cloud
# ──────────────────────────────────────────────────────────────────────────────

def _adc_default_path() -> Path:
    # Same path gcloud uses on every platform
    if os.name == "nt":
        return Path(os.environ.get("APPDATA", "")) / "gcloud" / "application_default_credentials.json"
    return Path.home() / ".config" / "gcloud" / "application_default_credentials.json"


def resolve_gcp_credentials() -> str:
    """
    Try, in order:
        1. DEFAULTS['gcp_sa_key'] / GCP_SA_KEY env var  (explicit service-account JSON)
        2. GOOGLE_APPLICATION_CREDENTIALS env var
        3. Application Default Credentials from `gcloud auth application-default login`

    Returns a human description of which method was chosen.
    Raises RuntimeError with a remediation hint if none work.
    """
    sa_key = _cfg("gcp_sa_key")
    if sa_key:
        p = Path(sa_key).expanduser()
        if not p.is_file():
            raise RuntimeError(f"Service account key not found at: {p}")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(p)
        return f"service-account key: {p}"

    gac = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if gac:
        if not Path(gac).is_file():
            raise RuntimeError(f"GOOGLE_APPLICATION_CREDENTIALS points to missing file: {gac}")
        return f"GOOGLE_APPLICATION_CREDENTIALS: {gac}"

    adc = _adc_default_path()
    if adc.is_file():
        return f"gcloud ADC: {adc}"

    raise RuntimeError(
        "No GCP credentials found. Pick ONE of these:\n\n"
        "  A) Run this once, then re-run the script:\n"
        "       gcloud auth application-default login\n\n"
        "  B) Point to a service-account JSON:\n"
        "       export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json\n\n"
        "  C) Edit DEFAULTS['gcp_sa_key'] at the top of this script."
    )


def gcs_client():
    from google.cloud import storage
    project = _cfg("gcp_project")
    return storage.Client(project=project) if project else storage.Client()


def bucket_writable(client, bucket_name: str) -> None:
    """Raise a clear error if the bucket is missing or we can't write to it."""
    from google.api_core import exceptions as gexc
    try:
        bucket = client.get_bucket(bucket_name)
    except gexc.NotFound:
        raise RuntimeError(
            f"GCS bucket '{bucket_name}' does not exist in project '{_cfg('gcp_project')}'.\n"
            f"  Check the name, or pick a different bucket with --bucket."
        )
    except gexc.Forbidden:
        raise RuntimeError(
            f"No access to bucket '{bucket_name}'. "
            f"Your account / service-account needs the 'Storage Object Admin' role."
        )
    # Tiny no-op write probe
    probe = bucket.blob(".writecheck_retail_cdao")
    try:
        probe.upload_from_string("ok", timeout=10)
        probe.delete(timeout=10)
    except gexc.Forbidden:
        raise RuntimeError(
            f"Read-only access to bucket '{bucket_name}'. "
            f"Needs 'Storage Object Admin' (or at least objectCreator+objectDeleter)."
        )


def upload_to_gcs(client, local_path: Path, bucket_name: str, remote_name: Optional[str] = None) -> str:
    remote_name = remote_name or local_path.name
    uri = f"gs://{bucket_name}/{remote_name}"
    LOG.info("Uploading %s -> %s", local_path, uri)
    t0 = time.time()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(remote_name)
    blob.upload_from_filename(str(local_path))
    LOG.info("  done (%.1fs, %.1f MB)",
             time.time() - t0, local_path.stat().st_size / 1e6)
    return uri


# ──────────────────────────────────────────────────────────────────────────────
# Pre-flight (--doctor)
# ──────────────────────────────────────────────────────────────────────────────

def doctor(tables: Iterable[str], bucket_name: str) -> int:
    """Run every check the real pipeline does, without extracting data."""
    tables = list(tables)
    ok = True

    def step(label, fn):
        nonlocal ok
        print(f"  [ .. ] {label}", end="\r")
        try:
            detail = fn()
            print(f"  [ OK ] {label}" + (f"  — {detail}" if detail else ""))
        except Exception as e:
            ok = False
            print(f"  [FAIL] {label}")
            for line in str(e).splitlines():
                print(f"         {line}")

    print("\nPre-flight checks\n" + "─" * 40)

    def _deps():
        miss = check_dependencies()
        if miss:
            raise RuntimeError(
                "Missing: " + ", ".join(miss) +
                "\nRun: pip install " + " ".join(miss))
        return f"{len(REQUIRED_PACKAGES)} required packages present"
    step("Python packages installed", _deps)

    state = {"conn": None, "creds": None}

    def _sql():
        state["conn"] = sql_connect()
        return f"{_cfg('sql_server')}/{_cfg('sql_database')}"
    step("SQL Server reachable + auth", _sql)

    def _tables():
        if state["conn"] is None:
            raise RuntimeError("skipped — SQL connection failed above")
        missing = [t for t in tables if not table_exists(state["conn"], t)]
        if missing:
            raise RuntimeError(
                "Table(s) not found: " + ", ".join(missing) +
                f"\nAsk Sipho whether {missing[0]} has been landed in "
                f"{_cfg('sql_database')}.{_cfg('sql_schema')}.")
        return ", ".join(tables)
    step("Expected tables exist", _tables)

    def _creds():
        state["creds"] = resolve_gcp_credentials()
        return state["creds"]
    step("GCP credentials resolvable", _creds)

    def _bucket():
        if state["creds"] is None:
            raise RuntimeError("skipped — no GCP credentials")
        bucket_writable(gcs_client(), bucket_name)
        return f"gs://{bucket_name} (write-check passed)"
    step("GCS bucket exists + writable", _bucket)

    # Always close the SQL connection if we opened one
    if state["conn"] is not None:
        try:
            state["conn"].close()
        except Exception:
            pass

    print("─" * 40)
    print("All green ✔\n" if ok else "Some checks failed ✘ — fix them above, then re-run --doctor.\n")
    return 0 if ok else 1


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def run(tables: List[str], bucket_name: str, skip_upload: bool) -> None:
    out_dir = Path(_cfg("out_dir")).expanduser()

    LOG.info("== Retail CDAO upload | tables=%s | bucket=%s%s ==",
             tables, bucket_name, " | DRY-RUN" if skip_upload else "")

    # Resolve GCP creds up front so we fail fast if auth is broken
    if not skip_upload:
        creds_desc = resolve_gcp_credentials()
        LOG.info("GCP auth: %s", creds_desc)
        client = gcs_client()
        bucket_writable(client, bucket_name)
    else:
        client = None

    with sql_connect() as conn:
        # Verify every table up-front so we don't get halfway through
        missing = [t for t in tables if not table_exists(conn, t)]
        if missing:
            raise RuntimeError(
                f"Table(s) not in {_cfg('sql_database')}.{_cfg('sql_schema')}: "
                + ", ".join(missing)
                + "\nLikely cause: Sipho hasn't landed this month yet."
            )

        for table in tables:
            csv_path = out_dir / f"{table}.csv"
            fetch_to_csv(conn, table, csv_path)

            if skip_upload:
                LOG.info("--skip-upload set, leaving CSV at %s", csv_path)
                continue

            upload_to_gcs(client, csv_path, bucket_name)

    LOG.info("Pipeline complete.")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Pull pre-landed Retail CDAO tables from BI_SANDBOX and upload to GCS.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--stamp", default=datetime.today().strftime("%Y%m"),
                   help="YYYYMM, used to default table name to BASE<stamp> (default: current month)")
    p.add_argument("--tables", nargs="+",
                   help="Explicit table list, e.g. BASE202601 TRNS202601. Overrides --stamp default.")
    p.add_argument("--bucket", default=_cfg("gcp_bucket"),
                   help=f"GCS bucket to upload to (default: {_cfg('gcp_bucket')})")
    p.add_argument("--skip-upload", action="store_true",
                   help="Write CSVs locally but do not upload to GCS")
    p.add_argument("--doctor", action="store_true",
                   help="Run all pre-flight checks and exit without processing data")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


def main(argv: Optional[list] = None) -> int:
    # Parse args first so --help works even without deps installed
    args = build_parser().parse_args(argv)

    # Now enforce deps before we try to import pandas / pyodbc / google.cloud
    _die_on_missing_deps()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    tables = args.tables or [f"BASE{args.stamp}"]

    try:
        if args.doctor:
            return doctor(tables, args.bucket)
        run(tables, args.bucket, args.skip_upload)
    except RuntimeError as e:
        # These are our own friendly errors — print them plainly, no stack trace
        print(f"\nERROR: {e}\n", file=sys.stderr)
        return 1
    except Exception:
        LOG.exception("Pipeline failed unexpectedly")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
