# Retail Sales CDAO — Monthly GCS Upload

Replaces the manual "run SAS → export CSV → drag to GCS" step.
Pulls the pre-landed monthly table(s) from `BI_SANDBOX` on `RSD-RBSQLDEV`
and uploads them to the `fmn-sandbox` bucket.

**Main file:** `retail_cdao_upload.ipynb` — open in Jupyter / VS Code / your usual notebook env.

---

## One-time setup (Mac) — the easy way

Run this once, from Terminal, in this folder. The script installs Homebrew,
the ODBC driver, the Google Cloud SDK, the Python packages, and walks you
through the gcloud login. Safe to re-run — it skips steps already done.

```bash
cd retail_cdao
bash setup_mac.sh
```

If anything fails, the script tells you exactly what to do next. Once it
finishes cleanly, skip to **Every month after that** below.

---

## One-time setup (Mac) — the manual way

Only follow this section if the script above failed on your machine.

### 1. ODBC driver for SQL Server

```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18
# (or msodbcsql17 if the rest of your stack uses 17 — match whichever your other scripts use)
```

If you install driver 18 instead of 17, update the `SQL_DRIVER` line in the config
cell to `"ODBC Driver 18 for SQL Server"`.

### 2. Python packages

From your notebook environment's terminal:

```bash
pip install -r requirements.txt
```

If you use a venv / conda env for your notebook, activate it first.

### 3. Google Cloud auth

```bash
gcloud auth application-default login
```

This opens a browser, you sign in once with your FNB Google account, and from
then on the notebook can talk to GCS. No key files to manage.

*(If you don't have the `gcloud` CLI: `brew install --cask google-cloud-sdk`)*

### 4. First run

Open `retail_cdao_upload.ipynb`, and before doing anything else set
`DRY_RUN = True` in the config cell. Run all cells. The pre-flight check cell
will tell you in plain English if anything is misconfigured. When it's all
green and the CSV preview looks right, flip `DRY_RUN = False` and re-run the
upload cell.

---

## Every month after that

1. Open the notebook
2. Change `STAMP = "202601"` to the new month in the config cell
3. `Runtime → Run All`
4. Done

---

## Configuration cheat sheet

Everything lives in the **Config** cell at the top of the notebook:

| Variable      | Meaning                                            |
| ------------- | -------------------------------------------------- |
| `SQL_SERVER`  | Default: `RSD-RBSQLDEV`                            |
| `SQL_DATABASE`| Default: `BI_SANDBOX`                              |
| `SQL_DRIVER`  | `ODBC Driver 17 for SQL Server` (or 18)            |
| `STAMP`       | `YYYYMM` — the month you're pulling                |
| `TABLES`      | List of table names; default `[f"BASE{STAMP}"]`    |
| `GCP_PROJECT` | Default: `fmn-sandbox`                             |
| `GCP_BUCKET`  | `customer_spend_data` or `customer_spend_data_processed` |
| `OUT_DIR`     | Where CSVs cache before upload (default `./out`)   |
| `DRY_RUN`     | `True` = preview only, no upload                   |

For SQL auth instead of Windows/AD auth: set `SQL_USER` and `SQL_PASSWORD` as
environment variables before launching the notebook.

---

## Troubleshooting

The pre-flight cell surfaces most issues, but here's the quick table:

| Symptom                                      | Fix                                                                              |
| -------------------------------------------- | -------------------------------------------------------------------------------- |
| `No GCP credentials`                         | `gcloud auth application-default login`                                          |
| `Could not reach SQL Server`                 | Check VPN / corporate network; try `ping RSD-RBSQLDEV` from terminal             |
| `SQL Server login failed`                    | AD account needs `db_datareader` on `BI_SANDBOX` — ask DBA                       |
| `Not found: ['BASE202601']`                  | Sipho hasn't landed this month yet — ping him                                     |
| `Bucket ... not found`                       | Typo in bucket name, or wrong `GCP_PROJECT`                                       |
| `No access to bucket ...`                    | Your principal needs `Storage Object Admin` on the bucket                         |
| `ODBC driver '...' not installed`            | See step 1 above                                                                  |
| Notebook can't find `pyodbc` after install   | Your notebook kernel is a different Python than `pip` targeted. Install from the notebook: `!pip install pyodbc` |

---

## For later: scheduled / unattended runs

If you ever want this to run on a cron without opening the notebook, there's
also `retail_cdao_upload.py` in this folder — same logic as the notebook, just
a command-line version. `python retail_cdao_upload.py --doctor` runs the same
pre-flight checks from a terminal.
