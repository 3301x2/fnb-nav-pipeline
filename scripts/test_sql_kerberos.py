"""
SQL Server Connection Test — macOS + Kerberos (Windows-integrated auth)
───────────────────────────────────────────────────────────────────────
Use this when plain SQL auth (test_sql_connection.py) fails with
"Login failed" even though port 1433 is open.

Flow:
  1. Sanity-check Kerberos config (krb5.conf, klist).
  2. Offer to run `kinit` to get a TGT if one isn't already cached.
  3. Connect to SQL Server using Trusted_Connection=yes (GSSAPI / Kerberos).

Setup (run once — see docs/sql_server_mac_kerberos_setup.md):
    brew install krb5
    brew install microsoft/mssql-release/msodbcsql18
    pip install pyodbc pandas python-dotenv
    # Add the FNB realm block to /etc/krb5.conf (runbook has the exact content)

Usage:
    python3 scripts/test_sql_kerberos.py
"""

import sys
import os
import getpass
import subprocess
import socket
import shutil

# ── Config ──────────────────────────────────────────────────────
SERVER_NAME = "RSD-RBSQLDEV"
SERVER_IP   = "10.32.176.173"
PORT        = 1433
DATABASE    = "BI_SANDBOX"
TABLE       = "BASE202601"

# The AD domain. Realm for Kerberos is the UPPERCASE form.
DOMAIN      = "FNBJNB01"
REALM       = "FNBJNB01.FIRSTRAND.CO.ZA"  # adjust if IT tells you otherwise
# The SPN (service principal name) — usually MSSQLSvc/<FQDN>:1433
# On a default SQL instance it's the hostname. Adjust if IT gave you a different one.
SPN_HOST    = f"{SERVER_NAME}.fnbjnb01.firstrand.co.za"

# ── Helpers ─────────────────────────────────────────────────────
def hdr(title):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def run(cmd, check=False, capture=True):
    """Run a command, return (returncode, stdout, stderr)."""
    try:
        r = subprocess.run(cmd, capture_output=capture, text=True, timeout=30)
        return r.returncode, r.stdout or "", r.stderr or ""
    except FileNotFoundError:
        return 127, "", f"command not found: {cmd[0]}"
    except subprocess.TimeoutExpired:
        return 124, "", "timed out"


# ── Step 0: Load .env if present ────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ── Step 1: Toolchain check ─────────────────────────────────────
hdr("STEP 1: Toolchain check")

missing = []
for tool in ("klist", "kinit", "kdestroy"):
    if shutil.which(tool) is None:
        missing.append(tool)

if missing:
    print("Missing Kerberos tools:", ", ".join(missing))
    print("Install with: brew install krb5")
    print("Then add Homebrew's krb5 to PATH (see runbook).")
    sys.exit(1)

# macOS has /usr/bin/klist (Heimdal) AND brew's krb5 (MIT). We want MIT.
rc, out, err = run(["klist", "-V"])
kind = "MIT" if "MIT" in out or "MIT" in err else ("Heimdal" if "Heimdal" in out + err else "unknown")
print(f"Using klist:  {shutil.which('klist')}")
print(f"Kerberos:     {kind}")
if kind == "Heimdal":
    print("WARNING: /usr/bin/klist (Heimdal) is first on PATH.")
    print("Put Homebrew krb5 ahead: export PATH=\"/opt/homebrew/opt/krb5/bin:$PATH\"")

# Check /etc/krb5.conf
krb_conf = "/etc/krb5.conf"
if not os.path.exists(krb_conf):
    print(f"\n{krb_conf} not found.")
    print("See runbook: docs/sql_server_mac_kerberos_setup.md")
    print("You need a realm block for", REALM)
    sys.exit(1)

with open(krb_conf) as f:
    conf_text = f.read()

if REALM not in conf_text:
    print(f"\n{krb_conf} exists but doesn't mention {REALM}.")
    print("Add the realm block from the runbook and re-run.")
    sys.exit(1)

print(f"krb5.conf:    {krb_conf} (contains {REALM})")


# ── Step 2: Ticket-granting ticket (TGT) ────────────────────────
hdr("STEP 2: Kerberos TGT")

rc, out, err = run(["klist"])
have_tgt = (rc == 0 and REALM in out)
if have_tgt:
    print("Existing TGT found:")
    for line in out.splitlines():
        if line.strip():
            print(f"  {line}")
else:
    print("No TGT cached. Running kinit...")
    user = os.environ.get("AD_USERNAME") or input("  AD username (just the user, no domain): ").strip()
    principal = f"{user}@{REALM}"
    # kinit will prompt for password interactively — don't pass via env.
    print(f"  Running: kinit {principal}")
    rc = subprocess.call(["kinit", principal])
    if rc != 0:
        print("\nkinit failed.")
        print("Common causes:")
        print("  - Wrong realm in krb5.conf (ask IT for the exact realm name)")
        print("  - KDC not reachable on VPN (check admin_server / kdc hostnames)")
        print("  - Password wrong")
        sys.exit(1)
    # Re-check
    rc, out, err = run(["klist"])
    if rc != 0 or REALM not in out:
        print("TGT not acquired. klist output:")
        print(out or err)
        sys.exit(1)
    print("\nTGT acquired:")
    for line in out.splitlines():
        if line.strip():
            print(f"  {line}")


# ── Step 3: Network reachability ────────────────────────────────
hdr("STEP 3: Network reachability")

# DNS
try:
    resolved = socket.gethostbyname(SERVER_NAME)
    print(f"DNS: {SERVER_NAME} -> {resolved}")
except socket.gaierror:
    resolved = SERVER_IP
    print(f"DNS: could not resolve {SERVER_NAME}, using IP {SERVER_IP}")

# TCP 1433
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(5)
rc = sock.connect_ex((resolved, PORT))
sock.close()
if rc != 0:
    print(f"TCP {PORT} on {resolved}: CLOSED (error {rc})")
    print("VPN not routing to SQL server, or firewall blocked. Fix VPN first.")
    sys.exit(1)
print(f"TCP {PORT} on {resolved}: OPEN")


# ── Step 4: ODBC driver ─────────────────────────────────────────
hdr("STEP 4: ODBC driver")

try:
    import pyodbc
except ImportError:
    print("pyodbc not installed. Run: pip3 install pyodbc")
    sys.exit(1)

drivers = pyodbc.drivers()
print("Available drivers:")
for d in drivers:
    print(f"  - {d}")
sql_drivers = [d for d in drivers if "SQL Server" in d or "ODBC Driver" in d]
if not sql_drivers:
    print("\nNo SQL Server ODBC driver found.")
    print("Install: brew install microsoft/mssql-release/msodbcsql18")
    sys.exit(1)
driver = sorted(sql_drivers)[-1]
print(f"Using: {driver}")


# ── Step 5: Connect with Trusted_Connection (Kerberos / GSSAPI) ─
hdr("STEP 5: SQL Server — Kerberos (Trusted_Connection)")

# When Trusted_Connection=yes, the driver asks the OS for a Kerberos ticket for
# the SPN that corresponds to SERVER,PORT and sends it via GSSAPI. No UID/PWD.
attempts = [
    ("FQDN + Trusted_Connection",     SPN_HOST,    PORT),
    ("Short name + Trusted_Connection", SERVER_NAME, PORT),
    ("IP + Trusted_Connection",       SERVER_IP,   PORT),
]

conn = None
last_err = None
for label, host, port in attempts:
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={host},{port};"
        f"DATABASE={DATABASE};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=15;"
    )
    print(f"\n[{label}] server={host}")
    try:
        conn = pyodbc.connect(conn_str, timeout=15)
        print("  CONNECTED")
        break
    except pyodbc.Error as e:
        last_err = str(e)[:300]
        print(f"  Failed: {last_err}")

if not conn:
    hdr("All Kerberos attempts failed")
    print("Likely causes:")
    print("  1. SPN mismatch: SQL isn't registered as MSSQLSvc/<host>:1433 under")
    print("     the service account the driver expects. Ask IT: 'what SPN should")
    print("     I use to Kerberos-auth to RSD-RBSQLDEV?'")
    print("  2. Server doesn't have Kerberos enabled — falls back to NTLM, which")
    print("     the Mac Microsoft ODBC driver does NOT implement. In that case")
    print("     only option is IT creating a SQL login (username/password).")
    print("  3. TGT expired mid-run — try 'kdestroy && kinit' and re-run.")
    print("\nLast driver error:")
    print(f"  {last_err}")
    sys.exit(1)


# ── Step 6: Sanity query ────────────────────────────────────────
hdr("STEP 6: Sanity query")

cur = conn.cursor()
cur.execute("SELECT SYSTEM_USER, SUSER_SNAME(), DB_NAME()")
u, sn, db = cur.fetchone()
print(f"  SYSTEM_USER    : {u}")
print(f"  SUSER_SNAME    : {sn}")
print(f"  DB_NAME        : {db}")

cur.execute("""
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = ?
""", TABLE)
row = cur.fetchone()
if row:
    print(f"\n  Table {TABLE}: found as [{row.TABLE_SCHEMA}].[{row.TABLE_NAME}]")
else:
    print(f"\n  Table {TABLE}: not visible to your login (may be a grants issue — separate from connection)")

conn.close()
print("\nDone — Kerberos auth worked.")
