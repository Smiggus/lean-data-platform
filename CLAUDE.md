# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

---

## Current State (as of 2026-04-01)

The platform is fully operational. Key work completed this session:

- **HAL-107 deployment**: Docker bind mount replaces named volume so LEAN CLI can read Dagster-written ZIPs on the host
- **FMP stable API migration**: All endpoints moved from `/api/v3/` to `/stable/`, ticker from URL path to `?symbol=` param, `symbol` column dropped from responses
- **LEAN format correctness**: `data_writer.py` now writes correct minute/second format (per-day ZIPs, milliseconds-since-midnight time column, `hour/` directory for hourly)
- **Coverage checker**: Now verifies LEAN ZIP exists on disk in addition to PostgreSQL coverage; triggers cost-free DB re-write if file missing
- **Bulk seed script**: `scripts/seed_russell2000.py` seeds Russell 2000 OHLCV + fundamentals + corporate actions from FMP with resume support

---

## Running the Platform

```bash
# Start all services (Dagster webserver on :3000, Dagster daemon)
# Run from Windows Anaconda Prompt — NOT WSL (OneDrive paths unreliable via /mnt/c)
docker-compose up -d --build

# Environment setup (required before first run)
cp .env.example .env
# Edit .env:
#   PGHOST = LAN IP of TARS (192.168.17.4) — not localhost
#   LEAN_DATA_ROOT = full absolute path to ~/Projects/Algo/data (no ~)
#   DAGSTER_HOST = localhost (or HAL-107 LAN IP for remote access)
```

**No build step required.** Python dependencies are installed in the Docker image via `requirements.txt`.

## Running Jobs Manually

```bash
# Execute a Dagster job directly via CLI
dagster job execute -j databento_equity_job

# Bulk seed Russell 2000 from FMP (run on server, safe overnight)
python scripts/seed_russell2000.py --delay 0.1 --resume

# Seed watchlist ETFs only
python scripts/seed_russell2000.py --tickers QQQ,SPY,IWM,GLD,TLT --delay 0.1

# Access Dagster UI
http://localhost:3000
```

---

## Architecture Overview

A **Dagster-orchestrated financial data pipeline** running on HAL-107, ingesting from Databento (OHLCV) and FMP (fundamentals + corporate actions) into PostgreSQL on TARS, then writing LEAN CLI format files for QuantConnect backtesting.

### Infrastructure

| Component | Host | Details |
|---|---|---|
| Dagster webserver + daemon | HAL-107 (Docker) | Port 3000, `docker-compose up` |
| PostgreSQL (TimescaleDB) | TARS @ 192.168.17.4:5432 | DB: FinancialData, user: eqty |
| LEAN data files | HAL-107 host | `~/Projects/Algo/data/` (bind-mounted to `/app/data`) |
| Algorithms | HAL-107 host | `~/Projects/Algo/{StrategyName}/` |

### Data Flow (algorithm-driven)

```
Algorithm calls request_ohlcv("SPY", start, end)
        |
        v
CoverageChecker (lean_pipeline/coverage_checker.py)
  1. Queries ohlcv.prices via generate_series — finds missing weekdays
  2. Checks ~/Projects/Algo/data/equity/usa/daily/spy.zip exists on disk
        |
   Covered? ──YES──> Algorithm continues, AddData(PipelineEquityData)
        |
       NO
        |
        v
PipelineClient fires GraphQL launchRun → Dagster
  databento_equity_job:
    fetch_ohlcv_op  — checks DB for missing segments, fetches only gaps from Databento
    write_lean_equity_op — writes LEAN ZIP to /app/data (= host ~/Projects/Algo/data)
        |
        v
Algorithm exits cleanly (DataRequestError caught in Initialize())
User re-runs after Dagster completes
```

### Key Directories

| Directory | Purpose |
|---|---|
| `pipeline/` | Dagster code: resources, ops, jobs, sensors, definitions |
| `lean_bridge/` | LEAN format writer, custom QC data readers, base strategy |
| `lean_pipeline/` | Drop-in library for QC algorithms (no Dagster import) |
| `db/` | PostgreSQL schema (`init.sql`) |
| `manifests/` | `watchlist.json` (nightly tickers), `missing_data_manifest.json` (generated) |
| `algorithms/` | QC algorithm implementations |
| `scripts/` | Bulk seeding scripts |
| `dagster_home/` | Dagster config (`dagster.yaml` points storage to PostgreSQL) |

### Dagster Components (`pipeline/`)

- **`definitions.py`** — entry point loaded by `workspace.yaml`; wires all resources, jobs, sensors, schedules
- **Resources** (`resources/`) — `DatabentoResource`, `FMPResource`, `TimescaleResource`
- **Ops** (`assets/`) — `fetch_ohlcv_op → write_lean_equity_op` (Databento); `fetch_fundamentals_op → store_fundamentals_op → write_lean_fundamentals_op` (FMP)
- **Jobs** (`jobs/data_jobs.py`) — `databento_equity_job`, `fmp_fundamentals_job`, `fmp_corporate_actions_job`, `daily_equity_refresh_job`
- **Sensor** (`sensors/lean_sensor.py`) — polls manifest every 30s, routes to jobs, deletes manifest after queuing
- **Schedule** — `daily_equity_refresh_schedule` runs at 20:00 UTC Mon-Fri for watchlist tickers

### lean_pipeline Library (`lean_pipeline/`)

No Dagster imports — safe inside QC algorithm environment.

- **`base_strategy.py`** — `BaseStrategy(QCAlgorithm)` with `request_ohlcv()` and `request_fundamentals()`; detects local vs cloud via `QC_RUN_ENV`; raises `DataRequestError` on miss (caught in `Initialize()`, calls `self.Quit()`)
- **`coverage_checker.py`** — checks PostgreSQL gaps AND LEAN ZIP existence on disk; falls back gracefully if DB unreachable
- **`pipeline_client.py`** — fires Dagster `launchRun` GraphQL mutation via plain `urllib`
- **`custom_data_reader.py`** — `PipelineEquityData`, `PipelineMinuteData`, `PipelineFundamentals` QC `PythonData` subclasses

On HAL-107, symlinked so all algorithms share one copy:
```bash
ln -s ~/Projects/Coding/DataFeeds/lean-data-platform/lean_pipeline ~/Projects/Algo/lean_pipeline
```

### LEAN Data Format

All formats verified against https://github.com/QuantConnect/Lean/blob/master/Data/equity/readme.md

| Resolution | Path | Time column | Price unit |
|---|---|---|---|
| Daily | `equity/usa/daily/{ticker}.zip` → `{ticker}.csv` | `YYYYMMDD 00:00` | deci-cents (× 10,000) |
| Hourly | `equity/usa/hour/{ticker}.zip` → `{ticker}.csv` | `YYYYMMDD HH:MM` | deci-cents |
| Minute | `equity/usa/minute/{ticker}/{YYYYMMDD}_trade.zip` | ms since midnight (int) | deci-cents |
| Second | `equity/usa/second/{ticker}/{YYYYMMDD}_trade.zip` | ms since midnight (int) | deci-cents |

Fundamentals: `data/fundamental/fine/{ticker}/{YYYYMMDD}.json`

### Database Schema

PostgreSQL at `192.168.17.4:5432`, database `FinancialData`.

| Schema.Table | Key columns | Notes |
|---|---|---|
| `ohlcv.prices` | `(ts_event, ticker, resolution)` PK | Databento + FMP data; source column distinguishes |
| `fundamentals.income_statement` | `(ticker, date, period)` unique | Annual + quarterly |
| `fundamentals.balance_sheet` | `(ticker, date, period)` unique | |
| `fundamentals.cash_flow` | `(ticker, date, period)` unique | |
| `fundamentals.key_metrics` | `(ticker, date, period)` unique | |
| `corporate_actions.dividends` | `(ticker, ex_date)` unique | |
| `corporate_actions.splits` | `(ticker, split_date)` unique | |
| `lean.written_files` | `(ticker, resolution, start_date, end_date)` unique | Tracks LEAN ZIP writes |

### Environment Variables

| Variable | Purpose |
|---|---|
| `DATABENTO_API_KEY` | Databento Historical API (cost-controlled: only missing segments fetched) |
| `FMP_API_KEY` | Financial Modeling Prep — $29/mo plan; use `/stable/` endpoints |
| `PGHOST` | TimescaleDB LAN IP (`192.168.17.4`) — not localhost |
| `PGPORT` / `PGDB` / `PGUSER` / `PGPASS` | DB connection |
| `QC_RUN_ENV` | `local` = use pipeline custom data; unset = QC cloud data |
| `DAGSTER_HOME` | `/app/dagster_home` (inside container) |
| `DAGSTER_HOST` | Host/IP where Dagster webserver runs (default `localhost`) |
| `DAGSTER_PORT` | Default `3000` |
| `LEAN_DATA_ROOT` | **Full absolute path** to `~/Projects/Algo/data` on host (no `~`; Docker won't expand it) |

### Writing New Algorithms

1. Create `~/Projects/Algo/MyStrategy/main.py` extending `BaseStrategy`
2. Add `sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))` to import `lean_pipeline`
3. Implement `_init_local_data()` calling `request_ohlcv()` / `request_fundamentals()`
4. Run: `lean backtest "MyStrategy" --data-provider-historical Local`
5. First run fires Dagster jobs and exits; second run backtests normally

```python
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from AlgorithmImports import *
from lean_pipeline.base_strategy import BaseStrategy
from lean_pipeline.custom_data_reader import PipelineEquityData

class MyStrategy(BaseStrategy):
    def Initialize(self):
        self.SetStartDate(2023, 1, 1)
        self.SetEndDate(2025, 12, 31)
        self.SetCash(100_000)
        super().Initialize()

    def _init_local_data(self):
        self.request_ohlcv("SPY", self.StartDate, self.EndDate)
        self._spy = self.AddData(PipelineEquityData, "SPY", Resolution.Daily).Symbol

    def _init_cloud_data(self):
        self._spy = self.AddEquity("SPY", Resolution.Daily).Symbol

    def OnData(self, data):
        if not self.Portfolio.Invested:
            self.SetHoldings(self._spy, 1.0)
```

### request_ohlcv() options

```python
self.request_ohlcv(
    ticker     = "AAPL",
    start_date = self.StartDate,
    end_date   = self.EndDate,
    resolution = "daily",       # "daily" | "minute" | "hourly"
    dataset    = "XNAS.ITCH",   # XNAS.ITCH (NASDAQ) | XNYS.PILLAR (NYSE) | GLBX.MDP3 (futures) | OPRA.PILLAR (options)
)
```

### Bulk Seeding (`scripts/seed_russell2000.py`)

Seeds Russell 2000 historical data from FMP. Constituent list falls back through:
1. FMP API (requires premium index tier)
2. quanthero GitHub CSV (~1,980 tickers)
3. ikoniaris GitHub CSV (~1,909 tickers)

```bash
# Full seed overnight (safe to interrupt, resumes via progress file)
python scripts/seed_russell2000.py --delay 0.1 --resume

# OHLCV only
python scripts/seed_russell2000.py --skip-fundamentals --skip-corporate --delay 0.1

# Inside Docker container
docker exec -it lean_dagster_webserver python /app/scripts/seed_russell2000.py --resume
```

Progress saved to `scripts/seed_progress.json` (gitignored) every 50 tickers.

**Survivorship bias note:** All free constituent sources are current-only. For point-in-time historical membership (backtest accuracy), see `PLAN.md`.

### Known API Quirks (FMP stable endpoints)

| Endpoint | Response format |
|---|---|
| `historical-price-eod/full` | Direct JSON array (no `historical` wrapper) |
| `dividends` | Direct JSON array |
| `splits` | Direct JSON array, field `splitType` not `label` |
| `income-statement` / `balance-sheet-statement` / `cash-flow-statement` / `key-metrics` | Direct JSON array, includes `symbol` field — drop before DB insert |
| `key-metrics` quarterly | Requires higher FMP plan tier (402 if not available — handled silently) |

### Cost Controls

- **Databento**: Only missing date segments fetched (not full range). `generate_series` in PostgreSQL identifies exact gaps; Databento API called once per contiguous segment.
- **FMP**: 402/404 responses return `None` immediately with no retry. Rate-limited endpoints skipped silently.
- **Nightly refresh**: `daily_equity_refresh_schedule` at 20:00 UTC Mon-Fri, last 7 days only, watchlist tickers in `manifests/watchlist.json`.
