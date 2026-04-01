# CLAUDE.md

Read this file at the start of every session. It contains everything needed to understand and continue work on this project.

---

## What This Project Is

A **Dagster-orchestrated financial data pipeline** that:
1. Ingests equity OHLCV from **Databento** and fundamentals/corporate actions from **FMP** into **PostgreSQL on TARS**
2. Writes data in **LEAN CLI format** (QuantConnect) for local backtesting
3. Exposes a **drop-in library** (`lean_pipeline/`) that algorithms import to get automatic data coverage checking and on-demand ingestion

The core value: algorithms never need to know where data comes from. They call `request_ohlcv("SPY", start, end)` and either proceed (data exists) or exit cleanly while Dagster fetches it.

---

## Infrastructure

| Component | Host | Details |
|---|---|---|
| Dagster webserver + daemon | HAL-107 (Docker) | Port 3000, `docker-compose up -d` |
| PostgreSQL (TimescaleDB) | TARS @ 192.168.17.4:5432 | DB: FinancialData, user: eqty, pass: 1234 |
| LEAN data files | HAL-107 host | `~/Projects/Algo/data/` (bind-mounted to `/app/data` in Docker) |
| Algorithms | HAL-107 host | `~/Projects/Algo/{StrategyName}/` |
| Git | GitHub: Smiggus/lean-data-platform | GitLab at git.mpholdings.co pending (TARS was down) |

**Important:** Docker must be run from **Windows Anaconda Prompt**, not WSL — OneDrive paths are unreliable via `/mnt/c`.

---

## Repository Structure

```
lean-data-platform/
├── pipeline/                   # Dagster code
│   ├── definitions.py          # Entry point loaded by workspace.yaml
│   ├── resources/
│   │   ├── databento_resource.py
│   │   ├── fmp_resource.py
│   │   └── timescale_resource.py
│   ├── assets/
│   │   ├── databento_assets.py  # fetch_ohlcv_op → write_lean_equity_op
│   │   └── fmp_assets.py        # fetch_fundamentals_op → store → write_lean
│   ├── jobs/data_jobs.py
│   ├── sensors/lean_sensor.py   # polls missing_data_manifest.json every 30s
│   └── schedules/               # daily_equity_refresh_schedule at 20:00 UTC
├── lean_pipeline/               # Drop-in library for QC algorithms (NO Dagster imports)
│   ├── base_strategy.py         # BaseStrategy(QCAlgorithm) - extend this
│   ├── coverage_checker.py      # Checks PostgreSQL gaps + LEAN ZIP on disk
│   ├── pipeline_client.py       # Fires Dagster jobs via GraphQL launchRun
│   └── custom_data_reader.py    # PipelineEquityData, PipelineFundamentals (QC PythonData)
├── lean_bridge/                 # LEAN format writer (used inside Dagster containers)
│   └── data_writer.py           # Writes LEAN ZIPs and fundamental JSONs
├── scripts/
│   └── seed_russell2000.py      # Bulk FMP seed: Russell 2000, 3 years, all data types
├── db/
│   └── init.sql                 # PostgreSQL schema (safe to re-run)
├── manifests/
│   └── watchlist.json           # Tickers for nightly Databento refresh
├── algorithms/
│   └── ExamplePipelineStrategy/ # Reference implementation
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── workspace.yaml
└── pyproject.toml
```

---

## Environment Variables

| Variable | Value | Notes |
|---|---|---|
| `DATABENTO_API_KEY` | in `.env` | Historical API — cost-controlled, only missing segments fetched |
| `FMP_API_KEY` | in `.env` | $29/mo plan; use `/stable/` endpoints |
| `PGHOST` | `192.168.17.4` | TARS — never use localhost |
| `PGPORT` | `5432` | |
| `PGDB` | `FinancialData` | |
| `PGUSER` | `eqty` | |
| `PGPASS` | `1234` | |
| `QC_RUN_ENV` | `local` | Tells algorithms to use pipeline data; unset = QC cloud |
| `DAGSTER_HOME` | `/app/dagster_home` | Inside container |
| `DAGSTER_HOST` | `localhost` | Used by algorithms to fire jobs; use HAL-107 LAN IP for remote access |
| `DAGSTER_PORT` | `3000` | |
| `LEAN_DATA_ROOT` | `/home/mp/Projects/Algo/data` | **Full absolute path, no `~`** — Docker won't expand it |

---

## Database Schema (PostgreSQL @ TARS)

| Schema.Table | Key | Notes |
|---|---|---|
| `ohlcv.prices` | `(ts_event, ticker, resolution)` PK | Databento + FMP OHLCV |
| `fundamentals.income_statement` | `(ticker, date, period)` unique | Annual + quarterly |
| `fundamentals.balance_sheet` | `(ticker, date, period)` unique | |
| `fundamentals.cash_flow` | `(ticker, date, period)` unique | |
| `fundamentals.key_metrics` | `(ticker, date, period)` unique | |
| `corporate_actions.dividends` | `(ticker, ex_date)` unique | |
| `corporate_actions.splits` | `(ticker, split_date)` unique | |
| `lean.written_files` | `(ticker, resolution, start_date, end_date)` unique | Tracks LEAN ZIP writes |

---

## LEAN Data Format

Verified against the LEAN spec at https://github.com/QuantConnect/Lean/blob/master/Data/equity/readme.md

| Resolution | Path on disk | Time column | Price unit |
|---|---|---|---|
| Daily | `equity/usa/daily/{ticker}.zip` → `{ticker}.csv` | `YYYYMMDD 00:00` | deci-cents (× 10,000) |
| Hourly | `equity/usa/hour/{ticker}.zip` → `{ticker}.csv` | `YYYYMMDD HH:MM` | deci-cents |
| Minute | `equity/usa/minute/{ticker}/{YYYYMMDD}_trade.zip` | ms since midnight (int) | deci-cents |
| Second | `equity/usa/second/{ticker}/{YYYYMMDD}_trade.zip` | ms since midnight (int) | deci-cents |

Fundamentals: `data/fundamental/fine/{ticker}/{YYYYMMDD}.json`

**Key:** `lean_bridge/data_writer.py` handles all resolution formats correctly. The `_LEAN_DIR` map converts `"hourly"` → `"hour"` (the actual LEAN directory name). Do not change this mapping.

---

## FMP API Quirks (important — these have bitten us before)

All endpoints use base URL `https://financialmodelingprep.com/stable` (NOT `/api/v3/` — deprecated).

| Endpoint | Response format | Notes |
|---|---|---|
| `historical-price-eod/full` | Direct JSON array | No `"historical"` wrapper |
| `dividends` | Direct JSON array | NOT `historical-dividends` |
| `splits` | Direct JSON array | NOT `historical-stock-splits`; field is `splitType` not `label` |
| `income-statement` etc. | Direct JSON array | Includes `symbol` field — `.drop(columns=["symbol"])` before DB insert |
| `key-metrics` quarterly | 402 if not on plan | Handled silently — returns None |
| 402 / 404 responses | Return None immediately | No retry — not a transient error |

Ticker is always passed as `?symbol=TICKER` query param, not in the URL path.

---

## Dagster Jobs

| Job | Trigger | What it does |
|---|---|---|
| `databento_equity_job` | Algorithm → GraphQL or sensor | Checks DB for missing segments → fetches only gaps from Databento → upserts → writes LEAN ZIP |
| `fmp_fundamentals_job` | Algorithm → GraphQL or sensor | Fetches income/balance/cashflow/metrics → upserts → writes LEAN fine JSON |
| `fmp_corporate_actions_job` | Manual / sensor | Fetches dividends + splits → upserts |
| `daily_equity_refresh_job` | Schedule: 20:00 UTC Mon-Fri | Last 7 days for all tickers in `manifests/watchlist.json` |

Jobs are defined in `pipeline/jobs/data_jobs.py`. Resources are wired in `pipeline/definitions.py` — do NOT put `resource_defs` in `to_job()` calls, this overrides the Definitions-level resources and causes empty API keys.

---

## lean_pipeline Library — How It Works

`lean_pipeline/` has zero Dagster imports. Safe to import inside QC algorithm environment.

### BaseStrategy flow

```
Initialize() called by LEAN
  |
  +-- detect environment (QC_RUN_ENV env var → "local" or "cloud")
  |
  +-- is_local=True → _init_local_data()
  |     |
  |     +-- request_ohlcv("SPY", start, end)
  |           |
  |           +-- CoverageChecker.is_ohlcv_covered()
  |                 1. generate_series query → finds missing weekdays in ohlcv.prices
  |                 2. _lean_zip_exists() → checks ~/Projects/Algo/data/.../spy.zip
  |                 |
  |                 +-- covered → return silently ✓
  |                 |
  |                 +-- missing → PipelineClient.request_ohlcv()
  |                               → GraphQL launchRun mutation → Dagster
  |                               → raise DataRequestError
  |
  +-- DataRequestError caught → self.Quit() → algorithm exits cleanly
  |
  +-- is_local=False → _init_cloud_data() → normal QC AddEquity()
```

### Integrating an existing algorithm

See README.md "Integrating into an Existing Algorithm" section. Summary:
1. Add `sys.path.insert` at top of file
2. Change `class MyStrategy(QCAlgorithm)` → `class MyStrategy(BaseStrategy)`
3. Move `Initialize` body into `_init_local_data()` replacing `AddEquity` with `request_ohlcv` + `AddData(PipelineEquityData, ...)`
4. Add `_init_cloud_data()` with original `AddEquity` calls
5. Add `super().Initialize()` call in `Initialize()`
6. Update `OnData` to branch on `self.is_local`

---

## Bulk Seeding

`scripts/seed_russell2000.py` — seeds Russell 2000 OHLCV + fundamentals + corporate actions from FMP.

```bash
# Test run first
python scripts/seed_russell2000.py --tickers QQQ,SPY,IWM,GLD,TLT --delay 0.1

# Full overnight run (safe to interrupt)
python scripts/seed_russell2000.py --delay 0.1 --resume
```

Constituent list fallback chain:
1. FMP API (requires premium index tier — likely 402)
2. quanthero GitHub CSV (~1,980 tickers)
3. ikoniaris GitHub CSV (~1,909 tickers)

Progress saved every 50 tickers to `scripts/seed_progress.json` (gitignored).

**Survivorship bias:** All free sources are current constituents only. Point-in-time historical data requires Norgate Data (~$630/yr) or CRSP via WRDS (institutional). See `PLAN.md`.

---

## HAL-107 Deployment Checklist

When setting up on a fresh HAL-107 (or after re-cloning):

```bash
# 1. Create LEAN data directory
mkdir -p ~/Projects/Algo/data

# 2. Symlink lean_pipeline for all algorithms
ln -s ~/Projects/Coding/DataFeeds/lean-data-platform/lean_pipeline \
      ~/Projects/Algo/lean_pipeline

# 3. Configure environment
cp .env.example .env
# Edit: LEAN_DATA_ROOT, PGPASS, API keys

# 4. Add to ~/.bashrc
export PGHOST=192.168.17.4 PGPASS=1234
export LEAN_DATA_ROOT=/home/mp/Projects/Algo/data
export DAGSTER_HOST=localhost DAGSTER_PORT=3000
export QC_RUN_ENV=local

# 5. Start Dagster
docker-compose up -d --build

# 6. Verify: http://localhost:3000
```

---

## Common Issues & Fixes

| Error | Cause | Fix |
|---|---|---|
| `No module named 'pipeline'` | Running from wrong directory or Python env | Run from repo root; check `PYTHONPATH=/app` in container |
| `column "symbol" does not exist` | FMP `/stable/` API returns `symbol` field | Already fixed: `.drop(columns=["symbol"])` in `fmp_resource.py` |
| `syntax error at or near ":"` | SQLAlchemy parses `::` cast as param | Already fixed: use `CAST(:x AS date)` not `:x::date` |
| `auth_invalid_username` (empty API key) | `resource_defs` in `to_job()` overrides Definitions resources | Already fixed: remove `resource_defs` from all `to_job()` calls |
| `DagsterExecutionStepNotFoundError` | Stale queued runs referencing removed ops | Cancel stale runs in Dagster UI, launch fresh |
| `ohlcv=0` in seed script | Wrong response format check | Already fixed: FMP returns direct array, not `{"historical": [...]}` |
| Docker `lean_data` volume not visible to host | Named volume is Docker-internal | Already fixed: bind mount `${LEAN_DATA_ROOT}:/app/data` |
| `LEAN_DATA_ROOT` with `~` fails | Docker Compose doesn't expand `~` | Use full absolute path in `.env` |

---

## Key Design Decisions (don't change without reason)

- **`pipeline/` not `dagster/`** — renamed to avoid shadowing the installed `dagster` package
- **`lean_pipeline/` has no Dagster imports** — must remain safe to import inside QC algorithm environment which doesn't have Dagster installed
- **Segment-aware fetching** — `get_missing_segments()` returns only gap windows; Databento is only called for missing date ranges, never the full history
- **FMP for seeding, Databento for ongoing** — FMP `/stable/` API for bulk historical backfill; Databento for nightly refresh (higher quality tick-normalized data)
- **`resource_defs` removed from `to_job()`** — resources must only be defined in `Definitions()`, not in individual jobs
- **`dagster.yaml` uses PostgreSQL storage** — all Dagster run/event state lives in TARS PostgreSQL under `dagster` schema, not in container volumes
- **Bind mount not named volume** — `${LEAN_DATA_ROOT}:/app/data` so LEAN CLI on the host can read files written by Dagster inside the container

---

## When Helping With This Project

- Always read the relevant source file before suggesting changes
- Check `pipeline/resources/fmp_resource.py` uses `/stable/` base URL
- Check `pipeline/jobs/data_jobs.py` has no `resource_defs` in `to_job()` calls
- When adding a new Dagster job: define it in `data_jobs.py`, register in `pipeline/definitions.py`
- When adding a new algorithm: follow the `BaseStrategy` pattern, use `_init_local_data()` / `_init_cloud_data()` split
- When modifying `data_writer.py`: verify format against the LEAN spec table above — daily/hourly are single ZIPs, minute/second are per-day ZIPs with ms-since-midnight time column
- `coverage_checker.py` lives in `lean_pipeline/` (no Dagster) and `timescale_resource.py` lives in `pipeline/resources/` (has Dagster) — they have similar gap-detection logic but are separate
