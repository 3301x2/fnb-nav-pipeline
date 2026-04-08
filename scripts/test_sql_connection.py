"""
SQL Server Connection Test
──────────────────────────
Tests connectivity to the FRG staging SQL Server.
Fill in your credentials below or use a .env file.

Usage:
    pip install pyodbc python-dotenv
    python test_sql_connection.py
"""

import sys
import os

try:
    import pyodbc
except ImportError:
    print("❌ pyodbc not installed. Run: pip install pyodbc")
    print("   Also make sure the ODBC driver is installed:")
    print("   macOS:  brew install microsoft/mssql-release/msodbcsql18")
    print("   Linux:  sudo apt install msodbcsql18")
    print("   Windows: download from https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
    sys.exit(1)

# ── Connection Details ───────────────────────────────────────
# Option 1: Fill these in directly
SERVER   = os.getenv("SQL_SERVER",   "")  # e.g. "10.x.x.x" or "hostname.fnb.co.za"
DATABASE = os.getenv("SQL_DATABASE", "")  # e.g. "DataOnboarding"
USERNAME = os.getenv("SQL_USERNAME", "")  # e.g. "prosper.sikhwari"
PASSWORD = os.getenv("SQL_PASSWORD", "")  # e.g. "your_password"

# Option 2: Load from .env file (pip install python-dotenv)
try:
    from dotenv import load_dotenv
    load_dotenv()
    SERVER   = SERVER   or os.getenv("SQL_SERVER",   "")
    DATABASE = DATABASE or os.getenv("SQL_DATABASE", "")
    USERNAME = USERNAME or os.getenv("SQL_USERNAME", "")
    PASSWORD = PASSWORD or os.getenv("SQL_PASSWORD", "")
except ImportError:
    pass

# ── Validate ─────────────────────────────────────────────────
missing = []
if not SERVER:   missing.append("SQL_SERVER")
if not DATABASE: missing.append("SQL_DATABASE")
if not USERNAME: missing.append("SQL_USERNAME")
if not PASSWORD: missing.append("SQL_PASSWORD")

if missing:
    print("❌ Missing connection details:", ", ".join(missing))
    print()
    print("Either edit this file directly or create a .env file:")
    print()
    print("   SQL_SERVER=10.x.x.x")
    print("   SQL_DATABASE=YourDatabase")
    print("   SQL_USERNAME=your_user")
    print("   SQL_PASSWORD=your_pass")
    print()
    sys.exit(1)

# ── Detect available ODBC drivers ────────────────────────────
print("🔍 Available ODBC drivers:")
drivers = pyodbc.drivers()
for d in drivers:
    print(f"   - {d}")

sql_drivers = [d for d in drivers if "SQL Server" in d]
if not sql_drivers:
    print()
    print("❌ No SQL Server ODBC driver found!")
    print("   Install one:")
    print("   macOS:  brew install microsoft/mssql-release/msodbcsql18")
    print("   Linux:  sudo apt install msodbcsql18")
    print("   Windows: download ODBC Driver 18 from Microsoft")
    sys.exit(1)

# Use the newest driver available
driver = sorted(sql_drivers)[-1]
print(f"\n✅ Using driver: {driver}")

# ── Connect ──────────────────────────────────────────────────
conn_str = (
    f"DRIVER={{{driver}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    f"TrustServerCertificate=yes;"
    f"Connection Timeout=10;"
)

print(f"\n🔌 Connecting to {SERVER} / {DATABASE} as {USERNAME}...")

try:
    conn = pyodbc.connect(conn_str, timeout=10)
    print("✅ Connected successfully!\n")
except pyodbc.Error as e:
    print(f"❌ Connection failed: {e}")
    print()
    print("Common fixes:")
    print("  - Check you're on the VDI / VPN / correct network")
    print("  - Verify the server IP and port (default 1433)")
    print("  - Confirm credentials with Marshall or Pierre")
    print("  - Try adding PORT: SERVER=10.x.x.x,1433")
    sys.exit(1)

# ── Test: list tables ────────────────────────────────────────
print("📋 Tables you have access to:")
cursor = conn.cursor()
try:
    tables = cursor.tables(tableType="TABLE")
    count = 0
    for row in tables:
        schema = row.table_schem
        name   = row.table_name
        print(f"   [{schema}].{name}")
        count += 1
    if count == 0:
        print("   (none found — you may only have access to specific tables)")
        print("   Ask the team for the exact table name and try the query below manually")
except pyodbc.Error as e:
    print(f"   ⚠️  Could not list tables: {e}")

# ── Test: sample query ───────────────────────────────────────
print()
TABLE_NAME = ""  # fill in once you know it, e.g. "dbo.customer_data"

if TABLE_NAME:
    print(f"🔎 Sampling 5 rows from {TABLE_NAME}...")
    try:
        import pandas as pd
        df = pd.read_sql(f"SELECT TOP 5 * FROM {TABLE_NAME}", conn)
        print(df.to_string(index=False))
        print(f"\n   Columns: {list(df.columns)}")
        print(f"   Dtypes:\n{df.dtypes}")
    except Exception as e:
        print(f"   ❌ Query failed: {e}")
else:
    print("💡 Once you know the table name, set TABLE_NAME in this script")
    print("   and re-run to preview the data.")

# ── Cleanup ──────────────────────────────────────────────────
conn.close()
print("\n🏁 Done. Connection closed.")
