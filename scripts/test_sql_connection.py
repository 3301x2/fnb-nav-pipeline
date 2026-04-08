"""
SQL Server Connection Test — macOS + VPN
────────────────────────────────────────
Connects to RSD-RBSQLDEV / BI_SANDBOX from Mac over VPN.
Since Trusted_Connection only works on Windows, this uses
your AD domain credentials explicitly.

Setup:
    brew install microsoft/mssql-release/msodbcsql18
    pip install pyodbc pandas python-dotenv

Usage:
    1. Fill in your .env file with your AD credentials
    2. python test_sql_connection.py
"""

import sys
import os
import getpass

try:
    import pyodbc
except ImportError:
    print("pyodbc not installed.")
    print()
    print("Run these commands first:")
    print("  brew install microsoft/mssql-release/msodbcsql18")
    print("  pip install pyodbc pandas python-dotenv")
    sys.exit(1)

# ── Load .env if available ───────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Connection Details ───────────────────────────────────────
SERVER   = "RSD-RBSQLDEV"
SERVER_IP = "10.32.176.173"
PORT     = "1433"
DATABASE = "BI_SANDBOX"
TABLE    = "BASE202601"
DOMAIN   = "FNBJNB01"

# Your AD credentials (from .env or entered manually)
AD_USERNAME = os.getenv("AD_USERNAME", "")   # just your username, no domain
AD_PASSWORD = os.getenv("AD_PASSWORD", "")

if not AD_USERNAME:
    print("Enter your FNB AD credentials (same as your Windows/VPN login):")
    AD_USERNAME = input("  Username (without domain): ").strip()

if not AD_PASSWORD:
    AD_PASSWORD = getpass.getpass("  Password: ")

# ── Detect ODBC drivers ──────────────────────────────────────
print("\nAvailable ODBC drivers:")
drivers = pyodbc.drivers()
for d in drivers:
    print(f"   - {d}")

sql_drivers = [d for d in drivers if "SQL Server" in d or "ODBC Driver" in d]
if not sql_drivers:
    print("\nERROR: No SQL Server ODBC driver found!")
    print("Install it: brew install microsoft/mssql-release/msodbcsql18")
    sys.exit(1)

driver = sorted(sql_drivers)[-1]
print(f"\nUsing driver: {driver}")

# ── Connection attempts ──────────────────────────────────────
# Try multiple auth methods since Mac + AD can be tricky

attempts = [
    {
        "label": "Server name + domain credentials",
        "server": SERVER,
        "extra": f"UID={DOMAIN}\\{AD_USERNAME};PWD={AD_PASSWORD};",
    },
    {
        "label": "IP address + domain credentials",
        "server": SERVER_IP,
        "extra": f"UID={DOMAIN}\\{AD_USERNAME};PWD={AD_PASSWORD};",
    },
    {
        "label": "Server name + plain credentials",
        "server": SERVER,
        "extra": f"UID={AD_USERNAME};PWD={AD_PASSWORD};",
    },
    {
        "label": "IP + plain credentials",
        "server": SERVER_IP,
        "extra": f"UID={AD_USERNAME};PWD={AD_PASSWORD};",
    },
]

conn = None

for i, attempt in enumerate(attempts, 1):
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={attempt['server']},{PORT};"
        f"DATABASE={DATABASE};"
        f"{attempt['extra']}"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=15;"
    )
    print(f"\nAttempt {i}: {attempt['label']}...")
    try:
        conn = pyodbc.connect(conn_str, timeout=15)
        print("CONNECTED!\n")
        break
    except pyodbc.Error as e:
        error_msg = str(e)
        # Don't print full password in error
        print(f"   Failed: {error_msg[:200]}")

if not conn:
    print("\nAll connection attempts failed.")
    print()
    print("Troubleshooting:")
    print("  1. Confirm VPN is connected")
    print("  2. Try: ping 10.32.176.173")
    print("  3. Check your credentials are correct (same as Windows login)")
    print("  4. Ask Sipho/Marshall if your AD account has SQL Server access")
    print("  5. You may need to request access via the Retail Sales Data Manual")
    print("  6. Check if ODBC driver is installed: brew list msodbcsql18")
    sys.exit(1)

cursor = conn.cursor()

# ── Check if BASE202601 exists ───────────────────────────────
print(f"Looking for table: {TABLE}...")
cursor.execute("""
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = ?
""", TABLE)
result = cursor.fetchone()

if result:
    full_table = f"[{result.TABLE_SCHEMA}].[{result.TABLE_NAME}]"
    print(f"Found: {full_table}\n")
else:
    print(f"Table '{TABLE}' not found! Listing all tables in {DATABASE}:\n")
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    for row in cursor.fetchall():
        print(f"   [{row.TABLE_SCHEMA}].{row.TABLE_NAME}")
    conn.close()
    sys.exit(0)

# ── Row count ────────────────────────────────────────────────
cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
count = cursor.fetchone()[0]
print(f"Row count: {count:,}")

# ── Column info ──────────────────────────────────────────────
print(f"\nColumns in {TABLE}:")
cursor.execute(f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{TABLE}'
    ORDER BY ORDINAL_POSITION
""")
columns = cursor.fetchall()
for col in columns:
    length = f"({col.CHARACTER_MAXIMUM_LENGTH})" if col.CHARACTER_MAXIMUM_LENGTH else ""
    nullable = "NULL" if col.IS_NULLABLE == "YES" else "NOT NULL"
    print(f"   {col.COLUMN_NAME:<40} {col.DATA_TYPE}{length:<15} {nullable}")
print(f"\n   Total columns: {len(columns)}")

# ── Preview 5 rows ───────────────────────────────────────────
print(f"\nFirst 5 rows of {TABLE}:")
try:
    import pandas as pd
    df = pd.read_sql(f"SELECT TOP 5 * FROM {full_table}", conn)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 200)
    print(df.to_string(index=False))
except ImportError:
    cursor.execute(f"SELECT TOP 5 * FROM {full_table}")
    col_names = [desc[0] for desc in cursor.description]
    print("   " + " | ".join(col_names))
    print("   " + "-" * 80)
    for row in cursor.fetchall():
        print("   " + " | ".join(str(v) for v in row))

# ── Done ─────────────────────────────────────────────────────
conn.close()
print("\nDone. Connection closed.")
