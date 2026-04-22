# Setup instructions (paste this to Pierre)

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
