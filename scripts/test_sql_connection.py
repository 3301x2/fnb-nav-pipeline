"""
SQL Server Connection Test — Windows Auth
──────────────────────────────────────────
Connects to RSD-RBSQLDEV / BI_SANDBOX and previews BASE202601.
Uses your Windows AD login via VPN — no password needed.

Usage:
    pip install pyodbc pandas
    python test_sql_connection.py
"""

import sys

try:
    import pyodbc
except ImportError:
    print("pyodbc not installed. Run: pip install pyodbc")
    sys.exit(1)

# ── Connection Details (from Sipho Nkosi) ────────────────────
SERVER   = "RSD-RBSQLDEV"
SERVER_IP = "10.32.176.173"
PORT     = "1433"
DATABASE = "BI_SANDBOX"
TABLE    = "BASE202601"

# ── Detect ODBC drivers ──────────────────────────────────────
print("Available ODBC drivers:")
drivers = pyodbc.drivers()
for d in drivers:
    print(f"   - {d}")

sql_drivers = [d for d in drivers if "SQL Server" in d]
if not sql_drivers:
    print("\nERROR: No SQL Server ODBC driver found!")
    sys.exit(1)

driver = sorted(sql_drivers)[-1]
print(f"\nUsing driver: {driver}")

# ── Try connecting (server name first, then IP) ──────────────
def try_connect(server_addr):
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server_addr},{PORT};"
        f"DATABASE={DATABASE};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=15;"
    )
    return pyodbc.connect(conn_str, timeout=15)

conn = None

print(f"\nAttempt 1: Connecting to {SERVER},{PORT} / {DATABASE}...")
try:
    conn = try_connect(SERVER)
    print("Connected successfully!\n")
except pyodbc.Error as e1:
    print(f"   Failed with server name: {e1}")
    print(f"\nAttempt 2: Trying IP {SERVER_IP},{PORT}...")
    try:
        conn = try_connect(SERVER_IP)
        print("Connected successfully!\n")
    except pyodbc.Error as e2:
        print(f"   Failed with IP: {e2}")
        print()
        print("Troubleshooting:")
        print("  1. Make sure you are connected to VPN")
        print("  2. Check you can ping: ping 10.32.176.173")
        print("  3. This must run from a domain-joined Windows machine")
        print("  4. Contact Sipho Nkosi or Marshall Petersen for access")
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
print("\nNext step: send Prosper a screenshot of this output!")
