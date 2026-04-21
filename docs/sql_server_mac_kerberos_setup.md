# SQL Server on Mac via Kerberos — Setup Runbook

**When to use this:** you've tried `scripts/test_sql_connection.py` with plain SQL auth and all 4 attempts fail with `Login failed for user 'FNBJNB01\<you>'` even though port 1433 is open. That's the server rejecting auth, often because it only accepts Windows-integrated (Kerberos) connections — which a Mac won't do out of the box.

**What this sets up:** MIT Kerberos on your Mac so you can get a ticket-granting ticket (`kinit`), then let the Microsoft ODBC driver use that ticket to authenticate to SQL Server (`Trusted_Connection=yes`).

**Honest up-front:** this works only if the SQL Server has a valid SPN registered (IT-side). If it doesn't, Kerberos will fail too — you'll need IT to either register the SPN or create a SQL login for you. The test script will tell you which.

---

## 1. Install Kerberos + Microsoft ODBC

```bash
# Kerberos (MIT)
brew install krb5

# Microsoft ODBC driver for SQL Server
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18

# Python deps
pip3 install pyodbc pandas python-dotenv
```

## 2. Put Homebrew's krb5 ahead of macOS's built-in Heimdal

macOS ships its own (different) Kerberos tools. You want MIT. Add this to `~/.zshrc`:

```bash
# Apple Silicon
echo 'export PATH="/opt/homebrew/opt/krb5/bin:/opt/homebrew/opt/krb5/sbin:$PATH"' >> ~/.zshrc

# Intel Mac
echo 'export PATH="/usr/local/opt/krb5/bin:/usr/local/opt/krb5/sbin:$PATH"' >> ~/.zshrc

source ~/.zshrc
```

Verify:

```bash
which klist
# should say /opt/homebrew/opt/krb5/bin/klist (NOT /usr/bin/klist)

klist -V
# should mention MIT
```

## 3. Configure `/etc/krb5.conf`

You need the realm name, KDC hostname, and admin-server hostname from IT. **Ask them:**

> For Kerberos auth to `RSD-RBSQLDEV` from my Mac, can you give me:
> - The Kerberos realm for `FNBJNB01` (likely `FNBJNB01.FIRSTRAND.CO.ZA` — but confirm)
> - The KDC hostnames (domain controllers) reachable from the VPN
> - The SPN I should use for SQL Server (likely `MSSQLSvc/RSD-RBSQLDEV.fnbjnb01.firstrand.co.za:1433`, but confirm)

Once you have those values, create `/etc/krb5.conf` (needs `sudo`):

```bash
sudo tee /etc/krb5.conf > /dev/null <<'EOF'
[libdefaults]
    default_realm = FNBJNB01.FIRSTRAND.CO.ZA
    dns_lookup_realm = true
    dns_lookup_kdc = true
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    rdns = false
    default_ccache_name = FILE:/tmp/krb5cc_%{uid}

[realms]
    FNBJNB01.FIRSTRAND.CO.ZA = {
        kdc = <ASK-IT-FOR-KDC-HOSTNAME-1>
        kdc = <ASK-IT-FOR-KDC-HOSTNAME-2>
        admin_server = <ASK-IT-FOR-ADMIN-SERVER>
        default_domain = fnbjnb01.firstrand.co.za
    }

[domain_realm]
    .fnbjnb01.firstrand.co.za = FNBJNB01.FIRSTRAND.CO.ZA
    fnbjnb01.firstrand.co.za = FNBJNB01.FIRSTRAND.CO.ZA
EOF
```

Replace the `<ASK-IT-...>` placeholders with real hostnames. If IT gives you IPs instead of hostnames, those also work.

## 4. Get a Kerberos ticket

```bash
# Make sure VPN is up first
kinit <yourusername>@FNBJNB01.FIRSTRAND.CO.ZA
# enter your AD password when prompted

# Verify
klist
# should show a ticket for krbtgt/FNBJNB01.FIRSTRAND.CO.ZA@FNBJNB01.FIRSTRAND.CO.ZA
```

If `kinit` fails:

| Error | Cause | Fix |
|---|---|---|
| `Cannot resolve servers for KDC` | KDC hostname wrong or not on VPN | Check `/etc/krb5.conf` KDC values. `nslookup <kdc>` |
| `Client not found in Kerberos database` | Username or realm wrong | Confirm exact realm name and username with IT |
| `Preauthentication failed` | Wrong password | Re-check password |
| `Clock skew too great` | Your Mac's clock is more than 5 min off the KDC | Sync: `sudo sntp -sS time.apple.com` |

## 5. Run the Kerberos test

```bash
cd ~/Documents/nav_latest/fnb-nav-pipeline
python3 scripts/test_sql_kerberos.py
```

The script prints a section per step. When it works, Step 5 says `CONNECTED` and Step 6 prints your SQL identity (`SYSTEM_USER`, `SUSER_SNAME`, `DB_NAME`).

## 6. When to give up on Kerberos

If after all of this the script gets past `kinit` (Step 2 ok) but Step 5 still fails with an auth error — and IT confirms the SPN is registered correctly — then **this SQL Server probably only speaks NTLM, not Kerberos**. The Microsoft ODBC driver on Mac does **not** support NTLM with a password for remote auth. There's no software fix on our side.

At that point the only path is: **ask IT to create a SQL Server login (username + password) for you on the `RSD-RBSQLDEV` instance** and grant it access to `BI_SANDBOX`. You then use `test_sql_connection.py` with those credentials.

## Daily use once set up

```bash
kinit <you>@FNBJNB01.FIRSTRAND.CO.ZA   # once per 24 hours (ticket lifetime)
python3 scripts/test_sql_kerberos.py    # or your own pyodbc code with Trusted_Connection=yes
```

To clear all tickets:

```bash
kdestroy
```

---

## What to tell IT if this runbook itself isn't enough

Copy this message:

> I'm on macOS trying to connect to `RSD-RBSQLDEV` / `BI_SANDBOX` over VPN. Port 1433 reaches, but auth fails.
>
> I've installed MIT Kerberos and the Microsoft ODBC driver 18. To finish setup I need:
>
> 1. The exact Kerberos **realm name** for the `FNBJNB01` AD (is it `FNBJNB01.FIRSTRAND.CO.ZA`?)
> 2. The **KDC hostnames** (AD domain controllers) reachable from the corporate VPN
> 3. The **SPN** registered for the SQL Server service on `RSD-RBSQLDEV` (expected format: `MSSQLSvc/RSD-RBSQLDEV.<domain>:1433`)
> 4. Confirmation that my AD account has been granted a SQL Server login with access to `BI_SANDBOX`
>
> If the SQL Server doesn't accept Kerberos, alternatively please create a SQL Server (non-Windows) login with a password — Mac clients can use that without any Kerberos setup.

*Last updated: April 2026*
