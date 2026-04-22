#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# retail_cdao — one-shot macOS setup
#
# Installs everything the notebook needs:
#   1. Homebrew               (if missing)
#   2. Microsoft ODBC 18      (for SQL Server)
#   3. Google Cloud SDK       (for `gcloud` auth)
#   4. Python packages        (from requirements.txt)
#   5. gcloud ADC login       (browser popup)
#
# Safe to re-run. Each step checks if it's already done and skips.
#
# Usage:
#   cd retail_cdao
#   bash setup_mac.sh
# ─────────────────────────────────────────────────────────────────────────────

set -uo pipefail  # -e intentionally omitted — we handle errors per-step

# ── Cosmetics ────────────────────────────────────────────────────────────────
R='\033[0;31m'; G='\033[0;32m'; Y='\033[1;33m'; B='\033[0;34m'; N='\033[0m'
say()  { printf "${B}▸${N} %s\n" "$*"; }
ok()   { printf "${G}✓${N} %s\n" "$*"; }
warn() { printf "${Y}⚠${N} %s\n" "$*"; }
die()  { printf "${R}✗${N} %s\n" "$*" >&2; exit 1; }
hr()   { printf "\n────────────────────────────────────────────\n"; }

banner() {
    hr
    printf "  %s\n" "$1"
    hr
}

# ── Preconditions ────────────────────────────────────────────────────────────
[ "$(uname)" = "Darwin" ] || die "This script is macOS-only. You're on $(uname)."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR" || die "Could not cd into $SCRIPT_DIR"

# Detect architecture — determines where Homebrew lives
ARCH="$(uname -m)"
if [ "$ARCH" = "arm64" ]; then
    BREW_PREFIX="/opt/homebrew"
    MAC_TYPE="Apple Silicon ($ARCH)"
else
    BREW_PREFIX="/usr/local"
    MAC_TYPE="Intel ($ARCH)"
fi

banner "retail_cdao — macOS setup"
echo "  Mac type:  $MAC_TYPE"
echo "  Brew path: $BREW_PREFIX"
echo "  Script:    $SCRIPT_DIR"

# ── Step 1: Homebrew ─────────────────────────────────────────────────────────
banner "Step 1/5 — Homebrew"

if command -v brew >/dev/null 2>&1; then
    ok "Homebrew already installed: $(brew --version | head -1)"
elif [ -x "$BREW_PREFIX/bin/brew" ]; then
    warn "Homebrew installed but not on PATH — fixing that now"
    eval "$("$BREW_PREFIX/bin/brew" shellenv)"
    # Persist to ~/.zshrc if not already there
    if ! grep -q "brew shellenv" "$HOME/.zshrc" 2>/dev/null; then
        echo "eval \"\$($BREW_PREFIX/bin/brew shellenv)\"" >> "$HOME/.zshrc"
        ok "Added Homebrew to ~/.zshrc (takes effect in new terminals)"
    fi
    ok "Homebrew now reachable: $(brew --version | head -1)"
else
    say "Installing Homebrew (you'll be asked for your Mac password — that's your login password)…"
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)" \
        || die "Homebrew install failed. Check the error above and try again."

    # Put it on PATH for THIS shell
    if [ -x "$BREW_PREFIX/bin/brew" ]; then
        eval "$("$BREW_PREFIX/bin/brew" shellenv)"
    fi

    # Persist to ~/.zshrc so future terminals work
    if ! grep -q "brew shellenv" "$HOME/.zshrc" 2>/dev/null; then
        echo "eval \"\$($BREW_PREFIX/bin/brew shellenv)\"" >> "$HOME/.zshrc"
        ok "Added Homebrew to ~/.zshrc"
    fi

    command -v brew >/dev/null 2>&1 \
        && ok "Homebrew installed: $(brew --version | head -1)" \
        || die "Homebrew installed but still not on PATH. Close this terminal, open a new one, and re-run the script."
fi

# ── Step 2: Microsoft ODBC 18 ────────────────────────────────────────────────
banner "Step 2/5 — Microsoft ODBC Driver 18 (for SQL Server)"

if brew list msodbcsql18 >/dev/null 2>&1; then
    ok "msodbcsql18 already installed"
else
    say "Tapping microsoft/mssql-release…"
    brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release \
        || warn "Tap already present or network issue — continuing"

    say "Installing msodbcsql18 + mssql-tools18 (accepting Microsoft EULA)…"
    HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18 \
        || die "ODBC install failed. If Homebrew complains about old tap URL, run:
          brew untap microsoft/mssql-release
          brew tap microsoft/mssql-release
        Then re-run this script."

    ok "ODBC Driver 18 installed"
fi

# ── Step 3: Google Cloud SDK ─────────────────────────────────────────────────
banner "Step 3/5 — Google Cloud SDK"

if command -v gcloud >/dev/null 2>&1; then
    ok "gcloud already installed: $(gcloud --version | head -1)"
else
    say "Installing google-cloud-sdk (this one takes a few minutes — it's a big download)…"
    brew install --cask google-cloud-sdk \
        || die "google-cloud-sdk install failed. You can still proceed if you have another way to set GOOGLE_APPLICATION_CREDENTIALS."

    # The cask drops gcloud into the Homebrew bin — make sure it's on PATH in this shell
    if ! command -v gcloud >/dev/null 2>&1; then
        if [ -x "$BREW_PREFIX/bin/gcloud" ]; then
            eval "$("$BREW_PREFIX/bin/brew" shellenv)"
        fi
    fi

    command -v gcloud >/dev/null 2>&1 \
        && ok "gcloud installed: $(gcloud --version | head -1)" \
        || warn "gcloud installed but not yet on PATH in THIS shell. Close this terminal, open a new one, and re-run this script to finish."
fi

# ── Step 4: Python packages ──────────────────────────────────────────────────
banner "Step 4/5 — Python packages (pandas, pyodbc, google-cloud-storage, google-auth)"

# Which python is the notebook most likely using?
# Prefer python3 that ships with the user's active environment; fall back to system python3.
if command -v python3 >/dev/null 2>&1; then
    PY=python3
else
    die "No python3 on PATH. Install Python 3.10+ or ensure your notebook kernel's Python is on PATH."
fi

PY_VERSION="$($PY -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
say "Using $PY ($(which $PY)) — Python $PY_VERSION"

if [ -n "${VIRTUAL_ENV:-}" ]; then
    warn "You're in a virtual env: $VIRTUAL_ENV"
    warn "Packages will install into that env — fine if it's the same env as your Jupyter kernel."
elif [ -n "${CONDA_DEFAULT_ENV:-}" ] && [ "$CONDA_DEFAULT_ENV" != "base" ]; then
    warn "You're in conda env: $CONDA_DEFAULT_ENV"
    warn "Packages will install into that conda env."
fi

if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
    say "Installing from requirements.txt…"
    $PY -m pip install --upgrade pip >/dev/null 2>&1 || true
    $PY -m pip install -r "$SCRIPT_DIR/requirements.txt" \
        || die "pip install failed. Check the error above. Common causes: no internet, wrong Python, or conda env clash."
    ok "Python packages installed"
else
    warn "requirements.txt not found — installing the four essentials individually"
    $PY -m pip install pandas pyodbc google-cloud-storage google-auth \
        || die "pip install failed. Check the error above."
    ok "Python packages installed"
fi

# Quick sanity check
say "Verifying imports…"
$PY - <<'PYEOF' || die "Imports failed after install. Try restarting the terminal and re-running."
import sys
mods = ["pandas", "pyodbc", "google.cloud.storage", "google.auth"]
missing = []
for m in mods:
    try:
        __import__(m)
    except ImportError as e:
        missing.append(f"{m}  ({e})")
if missing:
    print("FAILED imports:")
    for m in missing:
        print("  -", m)
    sys.exit(1)
print(f"  All imports OK — Python {sys.version.split()[0]}")
PYEOF
ok "Python imports verified"

# ── Step 5: GCP ADC login ────────────────────────────────────────────────────
banner "Step 5/5 — Google Cloud authentication"

ADC_PATH="$HOME/.config/gcloud/application_default_credentials.json"

if [ -f "$ADC_PATH" ]; then
    ok "GCP ADC credentials already in place: $ADC_PATH"
    say "(If uploads later fail with auth errors, run: gcloud auth application-default login)"
else
    if command -v gcloud >/dev/null 2>&1; then
        say "Opening a browser for you to sign in with your FNB Google account…"
        say "This is a ONE-TIME step. Click through the consent screens."
        gcloud auth application-default login \
            || warn "gcloud login skipped or failed. You can re-run it later: gcloud auth application-default login"
    else
        warn "gcloud not on PATH in THIS shell — close terminal, open a new one, and run:"
        echo "      gcloud auth application-default login"
    fi
fi

# ── Done ─────────────────────────────────────────────────────────────────────
banner "Setup complete"

cat <<EOF

Next steps for Pierre:

  1. Close this terminal and open a NEW one (so PATH changes take effect).
  2. Open the notebook:
       $SCRIPT_DIR/retail_cdao_upload.ipynb
  3. In Jupyter: Kernel → Restart (critical — picks up the new packages).
  4. In the config cell, set DRY_RUN = True for the first run.
  5. Runtime → Run All. The "Pre-flight checks" cell tells you if
     anything's still misconfigured.

If any step above said ⚠ or ✗, fix that first and re-run this script.
The script is safe to re-run — finished steps are skipped.

EOF
