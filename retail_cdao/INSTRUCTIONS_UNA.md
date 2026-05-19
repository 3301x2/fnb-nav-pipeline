# Hi Una — quick SQL connection test (5 min)

We just need to confirm your VDI can talk to the SQL Server box from Python. Nothing else for now. Once this works, we'll do the real pipeline.

You'll do this **on your Windows VDI**, not on your Mac side.

---

## 1. Get the files

I sent you a zip. Unzip it anywhere on the VDI (Desktop is fine). You'll get a folder called `retail_cdao`.

The only file you need from it is **`sql_smoke_test.ipynb`**. Ignore everything else.

---

## 2. Open the notebook

Open Jupyter however you normally do on the VDI:
- Anaconda Navigator → Launch Jupyter Notebook, **or**
- Open Anaconda Prompt → type `jupyter notebook` → enter

Navigate to the `retail_cdao` folder you unzipped and double-click **`sql_smoke_test.ipynb`**.

> ⚠ Open `sql_smoke_test.ipynb`, **not** `retail_cdao_upload.ipynb`. Wrong file = different test.

---

## 3. Run the cells one at a time

Click into the first cell, then press **Shift + Enter**. That runs the cell and moves to the next one.

Do this all the way down. **Don't skip cells. Don't change any code.**

The notebook checks everything itself and tells you what to do at each step. If a check fails, it prints exactly what to do next — follow that, then carry on.

There's **one** thing the notebook can't do for you: after the very first install cell, you need to go to the menu and click **Kernel → Restart**. The notebook tells you when. Don't skip this — if you do, the next cell will fail with "No module named pyodbc".

---

## 4. Send me the result

If everything passed, the last cell prints **`✅ ALL CHECKS PASSED`** and shows you a small data preview.

Take a screenshot of:
- the success message, and
- the table preview underneath

…and send it to me on Teams.

If any cell printed **`❌`**, take a screenshot of *that* cell (including all the red text) and send it to me. Don't try to fix it yourself — we'll work it out together.

---

## Common questions

**Do I need to install anything before opening the notebook?**
No — the notebook installs the two packages it needs (pyodbc, pandas). Just open it and run.

**Do I need a password?**
No. The notebook uses your Windows login automatically. If it ever prompts you for one, that's a bug — screenshot and send.

**Does this need VPN?**
You're on the VDI, so no — the VDI is already inside the network.

**How long should this take?**
2–5 minutes including the kernel restart. If you've been at it for 15 minutes, stop and message me.

Thanks Una.
