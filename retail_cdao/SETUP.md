# Setup instructions for the people you're handing this to

Two paths — pick whichever matches their machine. Copy the blockquote and paste.

---

## Path A — macOS (paste to Pierre)

> Hi Pierre — try this one script. It handles Homebrew, the ODBC driver, Google Cloud SDK, and all the Python packages in one go. Safe to re-run if it fails partway.
>
> 1. Unzip the folder I sent (double-click the zip in Finder if you haven't already).
> 2. Open **Terminal** (the Mac app — press ⌘+Space, type "terminal", hit enter).
> 3. Type `cd ` (with a space after) — **don't press enter yet**. Then drag the `retail_cdao` folder from Finder into the Terminal window. The path appears automatically. Press enter.
> 4. Then run:
>
>    ```
>    bash setup_mac.sh
>    ```
>
> It'll ask for your Mac password once (that's normal — Homebrew needs it). A browser will open at the end for Google sign-in.
>
> When it says "Setup complete", **close Terminal, open a NEW one**, then open `retail_cdao_upload.ipynb` and do **Kernel → Restart** before running cells.
>
> If anything fails, screenshot the red ✗ line and send it.

---

## If he's already tried `brew` and got "command not found"

> Don't worry about the earlier brew error — the script installs Homebrew for you if it's missing. Just run `bash setup_mac.sh` and it'll sort itself out.

## If he asks what packages to `pip install`

> The script does this for you. But if you ever need to do it manually inside the notebook, the line is:
>
> ```
> !pip install pandas pyodbc google-cloud-storage google-auth
> ```
>
> The Google package is `google-cloud-storage` (with dashes). Don't install a package called just `google` — that's unrelated and breaks things.

## If he's in a conda env and things get weird

> If you started Jupyter from a conda env (Anaconda/miniconda), make sure the same env is activated in Terminal before running `bash setup_mac.sh`, so the Python packages land in the right place. Check with `which python3` — it should point inside your conda env, not to `/usr/bin/python3`.

---

## Path B — Windows / Corporate VDI (paste to whoever's on the VDI)

> You're on a Windows VDI that already has SQL Server access through Management Studio — that's the easy path. We'll reuse your Windows identity so you don't need any extra auth setup.
>
> 1. Unzip the `retail_cdao` folder I sent (right-click → Extract All).
> 2. Open **PowerShell** (press Start, type "powershell", hit enter).
> 3. Type `cd ` (with a space after) — **don't press enter yet**. Drag the unzipped `retail_cdao` folder from File Explorer into the PowerShell window. The path appears. Press enter.
> 4. Run:
>
>    ```
>    powershell -ExecutionPolicy Bypass -File .\setup_windows.ps1
>    ```
>
>    (The `-ExecutionPolicy Bypass` part is just because Windows blocks unsigned scripts by default. It only applies to this one run.)
>
> The script checks Python, ODBC driver, installs the Python packages, and walks you through Google sign-in at the end. If Python or the Google Cloud SDK isn't installed, it tells you exactly where to download them.
>
> When it says "Setup complete":
>
> 5. Launch Jupyter — easiest is from the Start Menu (if Anaconda is installed), or from the same PowerShell window run `jupyter notebook`.
> 6. Open `retail_cdao_upload.ipynb`. **Kernel → Restart** before running cells.
> 7. In the config cell, set `STAMP` to the month you want and `DRY_RUN = True` for the first run. Then Cell → Run All.
>
> Because you're on a domain-joined Windows VDI, the SQL connection uses your Windows identity automatically — you do **not** need to set `SQL_USER` or `SQL_PASSWORD`. The pre-flight cell will tell you in plain English if anything's misconfigured.
>
> If anything fails, screenshot the red ✗ line and send it.

### Windows-specific gotchas

- If `python` isn't recognised in PowerShell after install, the installer didn't add it to PATH. Re-run the Python installer and tick the "Add Python to PATH" box at the bottom of the first screen.
- On Anaconda installs, the recommended way to open PowerShell is **Anaconda Powershell Prompt** (from Start Menu) — that activates conda automatically.
- Some FNB VDIs block `pip install` to the internet. If pip times out, ask IT for the internal PyPI mirror URL, then run:
  ```
  pip install -i https://<internal-mirror-url>/simple -r requirements.txt
  ```
- The Google Cloud SDK may need IT approval to install on a locked-down VDI. If so, paste them this: *"need google-cloud-sdk installed for a Python script to authenticate against GCS in the fmn-sandbox project"*.
