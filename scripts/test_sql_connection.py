"""
SQL Server Connection Test — macOS + VPN
────────────────────────────────────────
Connects to RSD-RBSQLDEV / BI_SANDBOX from Mac over VPN.
Runs full network diagnostics before attempting SQL connection.

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
import subprocess
import socket

# ── Step 1: Network Diagnostics ──────────────────────────────
SERVER_NAME = "RSD-RBSQLDEV"
SERVER_IP   = "10.32.176.173"
PORT        = 1433

print("=" * 60)
print("  STEP 1: Network Diagnostics")
print("=" * 60)

# Check VPN — look for active VPN interfaces
print("\n[1.1] Checking VPN status...")
try:
    ifconfig = subprocess.run(["ifconfig"], capture_output=True, text=True)
    vpn_indicators = ["utun", "ipsec", "ppp", "tun"]
    vpn_found = [line for line in ifconfig.stdout.split("\n")
                 if any(v in line.lower() for v in vpn_indicators) and "flags" in line]
    if vpn_found:
        for v in vpn_found:
            iface = v.split(":")[0].strip()
            print(f"   VPN interface detected: {iface}")
    else:
        print("   WARNING: No VPN interface detected (utun/ipsec/ppp)")
        print("   If you're sure VPN is connected, this might be fine — some VPN clients don't show here")
except Exception as e:
    print(f"   Could not check VPN: {e}")

# DNS resolution
print(f"\n[1.2] Resolving {SERVER_NAME}...")
resolved_ip = None
try:
    resolved_ip = socket.gethostbyname(SERVER_NAME)
    print(f"   Resolved to: {resolved_ip}")
except socket.gaierror:
    print(f"   Could not resolve {SERVER_NAME} via DNS")
    print(f"   Will fall back to IP: {SERVER_IP}")

# Ping server
targets = []
if resolved_ip:
    targets.append((SERVER_NAME, resolved_ip))
if resolved_ip != SERVER_IP:
    targets.append(("IP", SERVER_IP))

for label, addr in targets:
    print(f"\n[1.3] Pinging {label} ({addr})...")
    try:
        result = subprocess.run(
            ["ping", "-c", "3", "-W", "3", addr],
            capture_output=True, text=True, timeout=15
        )
        # Extract the summary line
        for line in result.stdout.split("\n"):
            if "packets" in line.lower() or "avg" in line.lower():
                print(f"   {line.strip()}")
        if result.returncode == 0:
            print(f"   Ping OK")
        else:
            print(f"   Ping FAILED — server may block ICMP (not necessarily a problem)")
    except subprocess.TimeoutExpired:
        print(f"   Ping timed out — network may not be reachable")
    except Exception as e:
        print(f"   Ping error: {e}")

# TCP port check
for label, addr in targets:
    print(f"\n[1.4] Checking TCP port {PORT} on {label} ({addr})...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        result = sock.connect_ex((addr, PORT))
        if result == 0:
            print(f"   Port {PORT} is OPEN — SQL Server is reachable!")
        else:
            print(f"   Port {PORT} is CLOSED or filtered (error code: {result})")
            print(f"   This means the server is not reachable on this port")
            print(f"   Check: VPN connected? Correct server? Firewall rules?")
    except socket.timeout:
        print(f"   Connection timed out — port not reachable")
    except Exception as e:
        print(f"   Error: {e}")
    finally:
        sock.close()

# ── Step 2: ODBC Driver Check ────────────────────────────────
print("\n" + "=" * 60)
print("  STEP 2: ODBC Driver Check")
print("=" * 60)

try:
    import pyodbc
except ImportError:
    print("\npyodbc not installed. Run:")
    print("  brew install microsoft/mssql-release/msodbcsql18")
    print("  pip install pyodbc")
    sys.exit(1)

print("\nAvailable ODBC drivers:")
drivers = pyodbc.drivers()
for d in drivers:
    print(f"   - {d}")

sql_drivers = [d for d in drivers if "SQL Server" in d or "ODBC Driver" in d]
if not sql_drivers:
    print("\nERROR: No SQL Server ODBC driver found!")
    print("Install: brew install microsoft/mssql-release/msodbcsql18")
    sys.exit(1)

driver = sorted(sql_drivers)[-1]
print(f"\nUsing driver: {driver}")

# ── Step 3: Credentials ─────────────────────────────────────
print("\n" + "=" * 60)
print("  STEP 3: Credentials")
print("=" * 60)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DATABASE = "BI_SANDBOX"
TABLE    = "BASE202601"
DOMAIN   = "FNBJNB01"

AD_USERNAME = os.getenv("AD_USERNAME", "")
AD_PASSWORD = os.getenv("AD_PASSWORD", "")

if not AD_USERNAME:
    print("\nEnter your FNB AD credentials (same as VPN/Windows login):")
    AD_USERNAME = input("  Username (without domain): ").strip()

if not AD_PASSWORD:
    AD_PASSWORD = getpass.getpass("  Password: ")

print(f"\n   Using: {DOMAIN}\\{AD_USERNAME}")

# ── Step 4: SQL Connection ───────────────────────────────────
print("\n" + "=" * 60)
print("  STEP 4: SQL Server Connection")
print("=" * 60)

attempts = [
    ("Server name + DOMAIN\\user", SERVER_NAME, f"UID={DOMAIN}\\{AD_USERNAME};PWD={AD_PASSWORD};"),
    ("IP + DOMAIN\\user",          SERVER_IP,   f"UID={DOMAIN}\\{AD_USERNAME};PWD={AD_PASSWORD};"),
    ("Server name + plain user",   SERVER_NAME, f"UID={AD_USERNAME};PWD={AD_PASSWORD};"),
    ("IP + plain user",            SERVER_IP,   f"UID={AD_USERNAME};PWD={AD_PASSWORD};"),
]

conn = None

for i, (label, server, creds) in enumerate(attempts, 1):
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server},{PORT};"
        f"DATABASE={DATABASE};"
        f"{creds}"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=15;"
    )
    print(f"\n[4.{i}] {label}...")
    try:
        conn = pyodbc.connect(conn_str, timeout=15)
        print("   CONNECTED!\n")
        break
    except pyodbc.Error as e:
        err = str(e).replace(AD_PASSWORD, "****")
        print(f"   Failed: {err[:300]}")

if not conn:
    print("\nAll connection attempts failed.")
    print()
    print("Summary of what we know:")
    print("  - VPN: check the interface results above")
    print("  - Port 1433: check if it showed OPEN above")
    print("  - If port is open but auth fails: credentials may be wrong")
    print("  - If port is closed: server not reachable over VPN")
    print()
    print("Actions:")
    print("  1. Confirm credentials with Sipho Nkosi")
    print("  2. Ask if your AD account has been granted SQL Server access")
    print("  3. Check the Retail Sales Data User Manual for access request steps")
    sys.exit(1)

cursor = conn.cursor()

# ── Step 5: Explore Data ─────────────────────────────────────
print("=" * 60)
print("  STEP 5: Data Exploration")
print("=" * 60)

print(f"\nLooking for table: {TABLE}...")
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

# Row count
cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
count = cursor.fetchone()[0]
print(f"Row count: {count:,}")

# Column info
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

# Preview
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
    for row in cursor.fetchall():
        print("   " + " | ".join(str(v) for v in row))

# ── Done ─────────────────────────────────────────────────────
conn.close()
print("\n" + "=" * 60)
print("  ALL DONE — screenshot this output for Claude!")
print("=" * 60)
