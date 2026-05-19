# ─────────────────────────────────────────────────────────────────────────────
# retail_cdao — one-shot Windows / VDI setup
#
# Use this on a corporate Windows VDI (Citrix, Horizon, AVD, etc.) where you
# already have access to RSD-RBSQLDEV / BI_SANDBOX through SQL Server Management
# Studio. The notebook will reuse that same Windows identity automatically
# (Trusted_Connection=yes) — no extra auth setup needed.
#
# What this script installs / checks:
#   1. Python 3.10+              (instructs how to install if missing)
#   2. Microsoft ODBC Driver 18  (instructs if missing; usually pre-installed)
#   3. Python packages           (from requirements.txt — pip install)
#   4. Google Cloud SDK          (offers to install via the GCloud installer)
#   5. gcloud ADC login          (browser popup)
#
# Safe to re-run.
#
# Usage:
#   Right-click setup_windows.ps1 → Run with PowerShell
#   OR open PowerShell, cd to the folder, and run:
#       powershell -ExecutionPolicy Bypass -File .\setup_windows.ps1
# ─────────────────────────────────────────────────────────────────────────────

$ErrorActionPreference = "Continue"   # we handle per-step errors ourselves

function Say($msg)  { Write-Host "▸ $msg" -ForegroundColor Blue }
function OK($msg)   { Write-Host "✓ $msg" -ForegroundColor Green }
function Warn($msg) { Write-Host "⚠ $msg" -ForegroundColor Yellow }
function Die($msg)  { Write-Host "✗ $msg" -ForegroundColor Red; exit 1 }
function Banner($title) {
    Write-Host ""
    Write-Host "────────────────────────────────────────────"
    Write-Host "  $title"
    Write-Host "────────────────────────────────────────────"
}

# ── Working directory ────────────────────────────────────────────────────────
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $ScriptDir

Banner "retail_cdao — Windows / VDI setup"
Write-Host "  Folder: $ScriptDir"
Write-Host "  Host:   $env:COMPUTERNAME ($env:USERNAME)"

# ── Step 1: Python ───────────────────────────────────────────────────────────
Banner "Step 1/5 — Python 3.10+"

$pythonOK = $false
$pyCmd = $null

# Try each launcher: 'py -3' (Windows Python launcher), 'python', 'python3'
$candidates = @(
    @{ Exe = "py";      Args = @("-3", "--version") },
    @{ Exe = "python";  Args = @("--version") },
    @{ Exe = "python3"; Args = @("--version") }
)

foreach ($c in $candidates) {
    try {
        $ver = & $c.Exe @($c.Args) 2>&1
        if ($LASTEXITCODE -eq 0 -and $ver -match "Python (\d+)\.(\d+)") {
            $major = [int]$Matches[1]; $minor = [int]$Matches[2]
            if ($major -ge 3 -and $minor -ge 10) {
                $pyCmd = if ($c.Exe -eq "py") { "py -3" } else { $c.Exe }
                $pythonOK = $true
                OK "Python found: $ver  (via '$pyCmd')"
                break
            } else {
                Warn "Found $ver but we need 3.10+"
            }
        }
    } catch {
        # Command not found — try next
    }
}

if (-not $pythonOK) {
    Warn "Python 3.10+ not found."
    Write-Host ""
    Write-Host "Install one of these (any works), then re-run this script:" -ForegroundColor Yellow
    Write-Host "  - Anaconda Distribution (easiest, includes Jupyter):  https://www.anaconda.com/download"
    Write-Host "  - Python.org installer:                                https://www.python.org/downloads/"
    Write-Host "  - Microsoft Store: search 'Python 3.12' and install"
    Write-Host ""
    Write-Host "During install, TICK 'Add Python to PATH' or you'll have to do it manually."
    Die "Re-run this script after Python is installed."
}

# ── Step 2: ODBC Driver 18 ───────────────────────────────────────────────────
Banner "Step 2/5 — Microsoft ODBC Driver 18 (for SQL Server)"

# Check registry for installed ODBC drivers
$odbcKey = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers"
$driverFound = $null
if (Test-Path $odbcKey) {
    $drivers = (Get-ItemProperty $odbcKey).PSObject.Properties |
        Where-Object { $_.Name -match "SQL Server" } |
        Select-Object -ExpandProperty Name
    if ($drivers) {
        OK ("Installed SQL Server ODBC drivers: " + ($drivers -join ", "))
        $driverFound = $drivers[-1]
    }
}

if (-not $driverFound) {
    Warn "No Microsoft SQL Server ODBC driver detected."
    Write-Host ""
    Write-Host "On a corporate VDI it's almost always already installed. If it isn't:" -ForegroundColor Yellow
    Write-Host "  1. Ask IT to install 'ODBC Driver 18 for SQL Server', OR"
    Write-Host "  2. Download from Microsoft:"
    Write-Host "     https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server"
    Write-Host ""
    Write-Host "The script will continue — pip install will still work, but the"
    Write-Host "notebook's SQL preflight will fail until a driver is installed."
}

# ── Step 3: Python packages ──────────────────────────────────────────────────
Banner "Step 3/5 — Python packages"

# Resolve how to invoke pip from the chosen python
if ($pyCmd -eq "py -3") {
    $pyExe = "py"
    $pyArgs = @("-3", "-m", "pip")
} else {
    $pyExe = $pyCmd
    $pyArgs = @("-m", "pip")
}

# Detect virtual env / conda
if ($env:VIRTUAL_ENV) {
    Warn "You're in a virtual env: $env:VIRTUAL_ENV"
    Warn "Packages will install into that env."
} elseif ($env:CONDA_DEFAULT_ENV -and $env:CONDA_DEFAULT_ENV -ne "base") {
    Warn "You're in conda env: $env:CONDA_DEFAULT_ENV"
}

Say "Upgrading pip…"
& $pyExe @pyArgs install --upgrade pip 2>&1 | Out-Null

if (Test-Path "$ScriptDir\requirements.txt") {
    Say "Installing from requirements.txt…"
    & $pyExe @pyArgs install -r "$ScriptDir\requirements.txt"
    if ($LASTEXITCODE -ne 0) { Die "pip install failed. Check the error above." }
} else {
    Warn "requirements.txt not found — installing the four essentials individually"
    & $pyExe @pyArgs install pandas pyodbc google-cloud-storage google-auth
    if ($LASTEXITCODE -ne 0) { Die "pip install failed." }
}
OK "Python packages installed"

# Sanity check — write a temp file and run it (more reliable than stdin piping)
Say "Verifying imports…"
$tmpPy = [System.IO.Path]::GetTempFileName() + ".py"
@'
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
print(f"  All imports OK -- Python {sys.version.split()[0]}")
'@ | Out-File -FilePath $tmpPy -Encoding ASCII

if ($pyCmd -eq "py -3") {
    & py -3 $tmpPy
} else {
    & $pyCmd $tmpPy
}
$importsOK = ($LASTEXITCODE -eq 0)
Remove-Item $tmpPy -ErrorAction SilentlyContinue
if (-not $importsOK) { Die "Imports failed after install. Try restarting PowerShell and re-running." }
OK "Python imports verified"

# ── Step 4: Google Cloud SDK ─────────────────────────────────────────────────
Banner "Step 4/5 — Google Cloud SDK"

$gcloudCmd = Get-Command gcloud -ErrorAction SilentlyContinue
if ($gcloudCmd) {
    OK "gcloud already installed: $($gcloudCmd.Source)"
} else {
    Warn "gcloud not found on PATH."
    Write-Host ""
    Write-Host "Install Google Cloud SDK from one of these:" -ForegroundColor Yellow
    Write-Host "  - https://cloud.google.com/sdk/docs/install#windows"
    Write-Host ""
    Write-Host "On a VDI you may need IT to install it for you. After install," -ForegroundColor Yellow
    Write-Host "close this PowerShell window and open a fresh one before re-running."
    Write-Host ""
    Warn "Skipping gcloud auth (Step 5) for now."
    Write-Host ""
    Write-Host "Once gcloud is installed:"
    Write-Host "  gcloud auth application-default login"
    Write-Host ""
    # Continue — they can still test SQL even without GCS
}

# ── Step 5: GCP ADC login ────────────────────────────────────────────────────
Banner "Step 5/5 — Google Cloud authentication (ADC)"

$adcPath = Join-Path $env:APPDATA "gcloud\application_default_credentials.json"

if (Test-Path $adcPath) {
    OK "GCP ADC credentials already in place: $adcPath"
    Say "(If uploads later fail with auth errors, run: gcloud auth application-default login)"
} elseif ($gcloudCmd) {
    Say "Opening a browser for you to sign in with your FNB Google account…"
    Say "This is a ONE-TIME step. Click through the consent screens."
    & gcloud auth application-default login
    if ($LASTEXITCODE -ne 0) {
        Warn "gcloud login skipped or failed. Re-run later: gcloud auth application-default login"
    }
} else {
    Warn "gcloud not available — skipping. Install Google Cloud SDK then run:"
    Write-Host "      gcloud auth application-default login"
}

# ── Done ─────────────────────────────────────────────────────────────────────
Banner "Setup complete"

@"

Next steps:

  1. Open the notebook:
       $ScriptDir\retail_cdao_upload.ipynb
     (Easiest: open Jupyter — 'jupyter notebook' from this PowerShell, or
      launch from the Start Menu if Anaconda is installed.)

  2. In Jupyter: Kernel → Restart (picks up the new packages).

  3. In the config cell:
       - Set STAMP to the month you want (e.g. '202604').
       - Set DRY_RUN = True for the first run.

  4. Runtime → Run All. The "Pre-flight checks" cell will tell you in
     plain English if anything's misconfigured.

Because you're on a domain-joined Windows VDI, SQL Server auth uses your
Windows identity automatically (Trusted_Connection=yes). You should NOT
need to set SQL_USER / SQL_PASSWORD.

If any step above said ⚠ or ✗, fix that first and re-run this script.
"@
