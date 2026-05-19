#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# SFTP smoke test — avalonwinscp.fnb.co.za
#
# Self-contained. Pulls this file on the work machine, runs it, gets a clear
# pass/fail at every step.
#
# Usage:
#   git pull
#   bash retail_cdao/test_sftp.sh                 # interactive
#   bash retail_cdao/test_sftp.sh /data/fnb/...   # also list a remote path
#
# Credentials are read from (priority order):
#   1. AD_USERNAME / AD_PASSWORD env vars
#   2. AD_USERNAME / DOMAIN_PW   (Pierre's legacy naming)
#   3. .env file in the same folder as this script
#   4. Interactive prompt — password is hidden
# ─────────────────────────────────────────────────────────────────────────────

set -uo pipefail

# ── Config ───────────────────────────────────────────────────────────────────
SFTP_HOST="avalonwinscp.fnb.co.za"
SFTP_PORT="22"

# ── Cosmetics ────────────────────────────────────────────────────────────────
R='\033[0;31m'; G='\033[0;32m'; Y='\033[1;33m'; B='\033[0;34m'; N='\033[0m'
ok()   { printf "${G}✅${N} %s\n" "$*"; }
fail() { printf "${R}❌${N} %s\n" "$*"; }
warn() { printf "${Y}⚠${N}  %s\n" "$*"; }
info() { printf "${B}▸${N}  %s\n" "$*"; }
hr()   { printf -- "────────────────────────────────────────────\n"; }
die()  { fail "$*"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo
echo "════════════════════════════════════════════════════════════"
echo "  SFTP smoke test — ${SFTP_HOST}:${SFTP_PORT}"
echo "════════════════════════════════════════════════════════════"

# ── Step 1 — required tools ──────────────────────────────────────────────────
hr; info "Step 1 — required tools"

for cmd in nc sftp ssh; do
    if command -v "$cmd" >/dev/null 2>&1; then
        ok "$cmd present"
    else
        if [ "$cmd" = "nc" ]; then
            warn "nc (netcat) not found — port check will use /dev/tcp"
        elif [ "$cmd" = "sftp" ] || [ "$cmd" = "ssh" ]; then
            die "$cmd not found — install OpenSSH client and retry"
        fi
    fi
done

# sshpass lets us pipe a password in. macOS doesn't ship with it; we work without.
HAVE_SSHPASS=0
if command -v sshpass >/dev/null 2>&1; then
    HAVE_SSHPASS=1
    ok "sshpass present (will pipe password automatically)"
else
    warn "sshpass not installed — you'll be prompted for the password interactively"
    warn "   To install: brew install sshpass  (or sudo apt-get install sshpass on Linux)"
fi

# ── Step 2 — DNS + TCP ───────────────────────────────────────────────────────
hr; info "Step 2 — network reachability"

# DNS — use getent on Linux, host/dscacheutil on macOS, fall back to /etc/hosts
RESOLVED_IP=""
if command -v getent >/dev/null 2>&1; then
    RESOLVED_IP="$(getent hosts "$SFTP_HOST" 2>/dev/null | awk '{print $1}' | head -1)"
elif command -v dscacheutil >/dev/null 2>&1; then
    RESOLVED_IP="$(dscacheutil -q host -a name "$SFTP_HOST" 2>/dev/null | awk '/ip_address/ {print $2; exit}')"
elif command -v host >/dev/null 2>&1; then
    RESOLVED_IP="$(host "$SFTP_HOST" 2>/dev/null | awk '/has address/ {print $4; exit}')"
fi

if [ -n "$RESOLVED_IP" ]; then
    ok "DNS: ${SFTP_HOST} → ${RESOLVED_IP}"
else
    fail "DNS: cannot resolve ${SFTP_HOST}"
    echo "    Likely cause: VPN not connected, or DNS not pointing at FNB internal resolvers."
    echo "    Reconnect VPN and re-run this script."
    exit 1
fi

# TCP probe — prefer nc, fall back to /dev/tcp
TCP_OK=0
if command -v nc >/dev/null 2>&1; then
    if nc -z -w 5 "$RESOLVED_IP" "$SFTP_PORT" >/dev/null 2>&1; then
        TCP_OK=1
    fi
else
    # Bash builtin
    if (echo > "/dev/tcp/${RESOLVED_IP}/${SFTP_PORT}") >/dev/null 2>&1; then
        TCP_OK=1
    fi
fi

if [ $TCP_OK -eq 1 ]; then
    ok "TCP port ${SFTP_PORT} reachable on ${RESOLVED_IP}"
else
    fail "TCP port ${SFTP_PORT} NOT reachable"
    echo "    Likely cause: VPN dropped, firewall rule, or server down."
    exit 1
fi

# ── Step 3 — credentials ─────────────────────────────────────────────────────
hr; info "Step 3 — credentials"

# Load .env if present
if [ -f "$SCRIPT_DIR/.env" ]; then
    # shellcheck disable=SC1091
    set -a; source "$SCRIPT_DIR/.env"; set +a
    ok "Loaded $SCRIPT_DIR/.env"
fi

# Allow either AD_PASSWORD (our standard) or DOMAIN_PW (Pierre's legacy name)
AD_USERNAME="${AD_USERNAME:-}"
AD_PASSWORD="${AD_PASSWORD:-${DOMAIN_PW:-}}"

if [ -z "$AD_USERNAME" ]; then
    read -r -p "  AD username (e.g. f3799182): " AD_USERNAME
fi

if [ -z "$AD_PASSWORD" ]; then
    if [ $HAVE_SSHPASS -eq 0 ]; then
        info "No sshpass installed — you'll be prompted by ssh itself in the next step."
        info "If you'd rather not retype, install sshpass or set AD_PASSWORD in env / .env"
    else
        # Read password silently into AD_PASSWORD
        printf "  AD password (hidden as you type): "
        stty -echo
        IFS= read -r AD_PASSWORD
        stty echo
        echo
    fi
fi

if [ -z "$AD_USERNAME" ]; then
    die "Username missing — re-run script"
fi

ok "Will authenticate as: $AD_USERNAME"

# ── Step 4 — SFTP auth + home folder listing ────────────────────────────────
hr; info "Step 4 — SFTP auth + home folder"

# Build a one-shot batch for sftp
TARGET_PATH="${1:-}"
SFTP_BATCH="$(mktemp)"
trap 'rm -f "$SFTP_BATCH"' EXIT

{
    echo "pwd"
    echo "ls"
    if [ -n "$TARGET_PATH" ]; then
        echo "ls -l ${TARGET_PATH}"
    fi
    echo "bye"
} > "$SFTP_BATCH"

# Standard ssh options for a non-interactive test:
#   StrictHostKeyChecking=accept-new — accepts the host key the first time, then pins it
#   UserKnownHostsFile=/dev/null     — but don't pollute ~/.ssh/known_hosts on the work machine
#   PreferredAuthentications=password — force password auth (we want to test AD password,
#                                       not whatever ssh-agent might offer)
SSH_OPTS=(
    -o StrictHostKeyChecking=accept-new
    -o UserKnownHostsFile=/dev/null
    -o LogLevel=ERROR
    -o PreferredAuthentications=password
    -o PubkeyAuthentication=no
    -o ConnectTimeout=15
    -P "$SFTP_PORT"
    -b "$SFTP_BATCH"
)

set +e
if [ $HAVE_SSHPASS -eq 1 ] && [ -n "$AD_PASSWORD" ]; then
    sshpass -p "$AD_PASSWORD" sftp "${SSH_OPTS[@]}" "${AD_USERNAME}@${SFTP_HOST}"
    SFTP_RC=$?
else
    info "Running sftp interactively — enter password when prompted"
    sftp "${SSH_OPTS[@]}" "${AD_USERNAME}@${SFTP_HOST}"
    SFTP_RC=$?
fi
set -e

if [ $SFTP_RC -eq 0 ]; then
    hr
    ok "SFTP CONNECTION WORKS"
    echo
    if [ -n "$TARGET_PATH" ]; then
        echo "   Listed both home folder AND target path: $TARGET_PATH"
    else
        echo "   Listed home folder. To also test a specific path, run:"
        echo "     bash $0 /data/fnb/retail_sales_and_cdao/"
    fi
    echo
    echo "Next steps:"
    echo "  1. If home folder listed correctly, your AD account works for SFTP."
    echo "  2. Find your monthly CSV target path with Sipho."
    echo "  3. Re-run with that path as the argument to confirm read access."
    echo "  4. We then pivot retail_cdao_upload to fetch via SFTP instead of SQL."
else
    hr
    fail "SFTP FAILED (exit code $SFTP_RC)"
    echo
    echo "Common reasons (most likely first):"
    echo "  - Wrong username or password — log in to FNB VPN portal with the same"
    echo "    password to verify."
    echo "  - Your AD account is not entitled to access avalonwinscp."
    echo "    Pierre's account works; check with Sipho whether yours has been granted."
    echo "  - Account locked from too many bad attempts — wait 15 min and retry."
    echo "  - VPN dropped between Steps 2 and 4."
    exit 1
fi
