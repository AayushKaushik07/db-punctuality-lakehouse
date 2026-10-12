# Setup & Run Guide

This guide takes you from a fresh laptop to a finished run on real data and a working dashboard. Pick **one** way of running it:

| Your situation | Use |
|---|---|
| Any OS, simplest path, nothing to install except Docker | **[A — Docker](#a--run-with-docker-recommended)** (recommended) |
| macOS or Linux, you want to run and edit code locally | **[B — Local Python](#b--run-locally-with-python-macos--linux--wsl2)** |
| Windows, but you don't want Docker | **[C — Windows via WSL2](#c--windows-without-docker-wsl2)** |

> **Do I need to know Java?** No. You never write Java. Spark (the engine behind PySpark)
> needs a Java *runtime* installed to run — like a driver. With Docker it is already inside the
> container; with option B you install it once with one command.

---

## 0. Requirements

| | Minimum | Recommended |
|---|---|---|
| RAM | 8 GB | 16 GB |
| Free disk | 5 GB | 10 GB |
| CPU | 4 cores | 8 cores |
| Internet | needed for the first download and the first build | |

**Data size per month:**
- Months **2024-07 → 2025-10**: ~100 MB each (the ~100 biggest stations). The project uses
  **2024-11 → 2025-10**, one full year from this period.
- Months **2025-11 onwards**: ~600 MB each (all stations). They need more RAM, see [section 5](#5-choosing-months--memory).

---

## 1. Get the code

**If you have the ZIP:** unzip it to a folder without spaces in the path, e.g.
`C:\projects\db-punctuality-lakehouse` or `~/projects/db-punctuality-lakehouse`.

**Once it is on GitHub:**

```bash
git clone https://github.com/AayushKaushik07/db-punctuality-lakehouse.git
cd db-punctuality-lakehouse
```

All commands below are run **inside this folder**.

---

## A — Run with Docker (recommended)

### A1. Install Docker Desktop

- **Windows / macOS:** install [Docker Desktop](https://www.docker.com/products/docker-desktop/).
  On Windows, accept the **WSL 2** backend when the installer asks.
- **Linux:** install Docker Engine + the Compose plugin (`docker compose version` should work).

**Give Docker enough memory** (Windows/macOS): Docker Desktop → *Settings* → *Resources* →
Memory **at least 8 GB**; 10 GB is recommended for the full year on a 16 GB laptop. On Windows with
WSL 2 the limit is set in `%UserProfile%\.wslconfig`:

```ini
[wsl2]
memory=10GB
```

then run `wsl --shutdown` in PowerShell and restart Docker Desktop.

Check it works:

```bash
docker --version
docker compose version
```

### A2. Build the image (first time only)

```bash
docker compose build
```

It takes **5–10 minutes the first time**. It installs Python packages and Java, and downloads the
Delta Lake jars. Later builds are much faster because Docker caches the layers.

### A3. Run the pipeline

Linux / macOS only: create the output folders first so they belong to you, not to root:

```bash
mkdir -p data artifacts docs
```

Then, on all systems:

```bash
docker compose run --rm pipeline
```

This downloads the 12 months set in `config/pipeline.yaml` (November 2024 – October 2025, ~1.25 GB)
and runs every stage. You will see log lines like:

```
16:05:28 | INFO | dbpipe.silver  | 2025-09: bronze=… quarantined=… duplicates=… silver=…
16:05:44 | INFO | dbpipe.quality | Quality: 60 checks, 0 errors, 0 warnings
16:05:50 | INFO | dbpipe.train   | gbm  ROC-AUC … | PR-AUC … | precision@top20% …
16:05:54 | INFO | dbpipe.report  | Wrote /app/docs/RESULTS.md and 9 charts
```

The full year is about 24 million rows. On a 16 GB Windows laptop with Docker set to 10 GB, the stages
after the download take about 15–20 minutes; silver (de-duplication and journey reconstruction)
is the longest stage. Warnings in the quality stage are informational; only **errors** stop the run.

### A4. Open the dashboard

```bash
docker compose up dashboard
```

Open **http://127.0.0.1:8501**. Stop it with `Ctrl+C` (and `docker compose down` to remove the container).

### A5. Useful Docker commands

| Task | Command |
|---|---|
| Different months | `docker compose run --rm -e DBPIPE_MONTHS=2025-07,2025-08,2025-09 pipeline` |
| Fewer parallel tasks (less memory) | `docker compose run --rm -e DBPIPE_SPARK_MASTER=local[4] pipeline` |
| Run one stage | `docker compose run --rm pipeline python -m dbpipe gold` |
| Run the tests | `docker compose run --rm pipeline make test` |
| Quick smoke test (synthetic, ~1 min) | `docker compose run --rm pipeline make sample-run` |
| SQL query on the results | `docker compose run --rm pipeline python -m dbpipe query sql/analysis/01_bad_day_streaks.sql` |
| Open a shell in the container | `docker compose run --rm pipeline bash` |

---

## B — Run locally with Python (macOS / Linux / WSL2)

### B1. Install Python (3.11 or 3.12) and Java 17

| OS | Python | Java 17 |
|---|---|---|
| **macOS** | `brew install python@3.11` | `brew install openjdk@17` then add to `~/.zshrc`: `export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"` |
| **Ubuntu 24.04 / WSL2** | built in (3.12): `sudo apt install python3-venv` | `sudo apt install openjdk-17-jre-headless` |

Check (open a new terminal first):

```bash
python3 --version      # must be 3.11 or 3.12  (on macOS with Homebrew: python3.11 --version)
java -version          # openjdk version "17..."
```

On Ubuntu 22.04, the default Python is 3.10, which is too old. Install 3.11 from the deadsnakes PPA:
`sudo add-apt-repository ppa:deadsnakes/ppa && sudo apt install python3.11 python3.11-venv`.

Java 21 also works. Java 8 or 11 does **not** (Spark 4 needs 17+).

### B2. Create a virtual environment and install

```bash
python3 -m venv .venv            # macOS/Homebrew or Ubuntu 22.04: python3.11 -m venv .venv
source .venv/bin/activate          # the prompt now starts with (.venv)
make install                       # = pip install -r requirements-dev.txt && pip install -e .
```

Activate the environment (`source .venv/bin/activate`) every time you open a new terminal.

### B3. Smoke test (1 minute, no download)

```bash
make sample-run
```

This runs the whole pipeline on a small **synthetic** sample in `data/sample/`. It never
touches your real data or `docs/`. If it ends with `report finished`, your setup works.
The **first** Spark run with Delta Lake downloads the Delta jars from Maven Central (a few
MB, cached in `~/.ivy2*`). The sample run uses plain Parquet, so the Delta download happens on
your first real run.

### B4. Real run

```bash
make all          # download + full pipeline
make dashboard    # http://127.0.0.1:8501
```

### B5. Everyday commands

| Command | What it does |
|---|---|
| `make help` | list all commands |
| `make download` | download the configured months only |
| `make run` | all stages after download |
| `make bronze` / `silver` / `gold` / `quality` / `features` / `train` / `report` | one stage |
| `make test` | pytest suite (about 1 minute) |
| `make lint` / `make format` | code style check / auto-format |
| `make clean` | delete lakehouse tables + artifacts (keeps downloaded files) |

Without `make`, use `python -m dbpipe <stage>` directly, e.g. `python -m dbpipe run`.

---

## C — Windows without Docker (WSL2)

PySpark on plain Windows needs extra Hadoop helper files (`winutils.exe`, `HADOOP_HOME`) and
is fragile, so this project uses **WSL2**, a real Ubuntu inside Windows:

1. Open **PowerShell as Administrator**: `wsl --install -d Ubuntu`, then restart the PC.
2. Open **Ubuntu** from the Start menu and create a user.
3. Put the project **inside the Linux file system** (much faster than `/mnt/c/...`):
   ```bash
   mkdir -p ~/projects && cd ~/projects
   git clone https://github.com/AayushKaushik07/db-punctuality-lakehouse.git
   cd db-punctuality-lakehouse
   ```
   (or copy the unzipped folder via Explorer → `\\wsl$\Ubuntu\home\<you>\projects`)
4. `sudo apt update && sudo apt install -y python3-venv openjdk-17-jre-headless make git`
5. Continue with **[B2](#b2-create-a-virtual-environment-and-install)**.

The dashboard at http://127.0.0.1:8501 opens in your normal Windows browser.
**VS Code:** install the *WSL* extension, then run `code .` inside the project folder in Ubuntu.

---

## 4. What gets created

| Path | Content |
|---|---|
| `data/landing/` | downloaded monthly Parquet files + `_manifest.json` (checksums) |
| `data/lakehouse/bronze/` | raw data with enforced schema + `_ingestion_log` |
| `data/lakehouse/silver/` | clean stop events, `quarantine`, `_run_metrics` |
| `data/lakehouse/gold/` | `dim_*`, `fct_stop_events`, `agg_*`, `dq_results`, `ml_features` |
| `artifacts/` | trained model (`model/delay_model_gbm.joblib`), `metrics.json`, model charts |
| `docs/RESULTS.md`, `docs/images/` | the generated results page and charts (**these you commit**) |

`data/` and `artifacts/` are in `.gitignore`: data never goes to GitHub.

---

## 5. Choosing months & memory

Edit `dataset.months` in `config/pipeline.yaml`, or override it for one run:

```bash
DBPIPE_MONTHS=2025-07,2025-08,2025-09,2025-10 make all       # macOS/Linux/WSL
docker compose run --rm -e DBPIPE_MONTHS=2025-07,2025-08,2025-09,2025-10 pipeline   # Docker
```

- The default is one full year (2024-11 → 2025-10). For a quicker first try, process fewer months,
  e.g. `DBPIPE_MONTHS=2025-09,2025-10` (~200 MB, a few minutes).
- Keep all months on **one side of 2025-11**: from November 2025 the dataset covers all stations
  instead of ~100, and mixing both would look like a sudden jump in punctuality.
- **Memory:** Spark's Java heap (`DBPIPE_DRIVER_MEMORY`, default `4g`) is only part of what it uses;
  every parallel task also needs memory outside the heap. On a 16 GB laptop keep the default 4 GB heap
  and at most 6 parallel tasks (`DBPIPE_SPARK_MASTER=local[6]`, the default). A bigger heap leaves
  *less* room for the rest and can make the run fail with `Cannot allocate memory`.
- For **2025-11 or later** (600 MB/month, all stations) use a machine with 32 GB+ RAM, or process one
  month at a time.
- Already-loaded months with unchanged files are **skipped** in bronze (checksum match). Use
  `python -m dbpipe bronze --force` to reload anyway.

**All settings that can be overridden with environment variables:**

| Variable | Default | Meaning |
|---|---|---|
| `DBPIPE_MONTHS` | `2024-11` … `2025-10` | months to process (comma-separated) |
| `DBPIPE_DRIVER_MEMORY` | `4g` | Spark's Java heap (keep well below Docker's memory) |
| `DBPIPE_STORAGE_FORMAT` | `delta` | `delta` or `parquet` |
| `DBPIPE_SPARK_MASTER` | `local[6]` | Spark master: at most 6 parallel tasks |
| `DBPIPE_TEST_DAYS` | `31` | days held out to test the model (the last month) |
| `DBPIPE_LANDING_PATH`, `DBPIPE_LAKEHOUSE_PATH`, `DBPIPE_ARTIFACTS_PATH`, `DBPIPE_DOCS_PATH` | see config | output folders |
| `DBPIPE_CONFIG` | `config/pipeline.yaml` | use a different config file |

---

## 6. Exploring the data with SQL

Every gold table is a Spark SQL view named `gold_<table>`:

```bash
python -m dbpipe query                                   # list available views
python -m dbpipe query "SELECT * FROM gold_dim_station ORDER BY stop_events DESC LIMIT 10"
python -m dbpipe query sql/analysis/02_worst_hour_per_station.sql --limit 20
```

The files in `sql/analysis/` are ready-made analyses you can talk through in an interview:
bad-day streaks (gaps and islands), worst hour per station (top-N per group), delay recovery
along a ride (`FIRST_VALUE`/`LAST_VALUE`) and cancellation quartiles (`NTILE`).

---

## 7. Troubleshooting

| Symptom | Fix |
|---|---|
| `JAVA_HOME is not set` / `Java gateway process exited` | Java isn't installed or not found. `java -version` must show 17+. On macOS set `JAVA_HOME` (see B1) and open a new terminal. |
| `UnsupportedClassVersionError` | Java too old. Install Java 17. |
| `java.io.IOException: Cannot allocate memory` / `FAILED_READ_FILE` | Docker's VM ran out of memory (not Spark's heap). Give Docker more memory (`.wslconfig`), keep `DBPIPE_DRIVER_MEMORY` at `4g`, and/or use fewer parallel tasks: `-e DBPIPE_SPARK_MASTER=local[4]`. Close other heavy apps. |
| `java.lang.OutOfMemoryError: Java heap space` | Spark's heap is too small for a task: use fewer parallel tasks (`local[4]`) before raising `DBPIPE_DRIVER_MEMORY`. |
| Stuck at `Downloading 2025-09…` for a long time | ~100 MB per file; slow networks take a while. Failed downloads retry 3×. Re-running continues where it stopped. |
| `Could not download 2026-xx` | That month isn't published yet. Check the file list on the [dataset page](https://huggingface.co/datasets/piebro/deutsche-bahn-data/tree/main/monthly_processed_data). |
| `required column(s) missing` | The upstream dataset changed its schema. The error names the column; update `src/dbpipe/ingest/contract.py`. |
| Delta jars can't be downloaded (company network/proxy) | Use Docker (jars are baked into the image), point `DBPIPE_DELTA_JARS` to a folder containing the Delta jars, or set `DBPIPE_STORAGE_FORMAT=parquet`. |
| `Stage failed: DataQualityError` | An ERROR-level check failed. The log lines just above it say which check and why. |
| Dashboard: "No gold tables found" | Run the pipeline first. With Docker, run the dashboard from the same folder. |
| Dashboard keeps loading forever at `localhost:8501` (Windows + Docker Desktop) | Open **http://127.0.0.1:8501** instead. `localhost` may resolve to IPv6 (`::1`) first, which Docker Desktop's port forwarding can leave hanging. The compose file publishes the port on IPv4 only to avoid this. |
| Port 8501 already in use | Stop the other app, or `streamlit run dashboard/app.py --server.port 8502`. |
| Linux + Docker: `Permission denied` writing `data/` | Docker created the folders as root. `sudo chown -R $USER data artifacts docs`, then run again. |
| `make: *** missing separator` | Windows line endings. Re-clone after `git config --global core.autocrlf input`, or use Docker. |
| Need the full Python traceback | Add `-v`: `python -m dbpipe silver -v` |
