# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the Platform

```bash
# Start all services (TimescaleDB, Dagster webserver on :3000, Dagster daemon)
docker-compose up -d

# Environment setup (required before first run)
cp .env.example .env
# Edit .env — PGHOST must be the LAN IP of the TARS host, not localhost
```

**No build step required.** Python dependencies are installed in the Docker image via `requirements.txt`.

## Running Jobs Manually

```bash
# Trigger a backtest (checks coverage, writes manifest if missing, runs lean)
python lean_bridge/lean_runner.py --strategy ExamplePipelineStrategy --lean-root /app

# Execute a Dagster job directly via CLI
dagster job execute -j databento_equity_job

# Access Dagster UI
# http://localhost:3000
```

## Architecture Overview

This is a **Dagster-orchestrated financial data pipeline** that automates data ingestion and converts it to LEAN CLI (QuantConnect) format for local backtesting.

### Data Flow

```
lean_runner.py checks data coverage in TimescaleDB
    └─ Missing data → writes manifests/missing_data_manifest.json
         └─ lean_missing_data_sensor (polls every 30s) picks it up
              ├─ equity/futures → databento_equity_job
              └─ fundamentals / source=fmp → fmp_fundamentals_job
                   └─ Fetch → Store (TimescaleDB) → Write LEAN ZIP/JSON
                        └─ lean_runner.py re-runs → lean backtest
```

### Key Directories

- `dagster/` — Dagster code: resources, ops, jobs, sensors, definitions entry point
- `lean_bridge/` — LEAN CLI integration: runner, data writer, custom QC data readers, base strategy
- `db/` — TimescaleDB schema (`init.sql` auto-runs on first Docker start)
- `manifests/` — `watchlist.json` (nightly tickers) and `missing_data_manifest.json` (generated, not committed)
- `algorithms/` — QC algorithm implementations using the pipeline data
- `dagster_home/` — Dagster storage config (PostgreSQL-backed)

### Dagster Components (`dagster/`)

- **`definitions.py`** — main entry point loaded by `workspace.yaml`; wires together all resources, jobs, sensors, schedules
- **Resources** (`resources/`) — `DatabentoResource` (equity OHLCV), `FMPResource` (fundamentals), `TimescaleResource` (DB CRUD + gap detection)
- **Ops** (`assets/`) — op chains: `fetch → store → write_lean` for both Databento and FMP
- **Jobs** (`jobs/data_jobs.py`) — `databento_equity_job`, `fmp_fundamentals_job`, `daily_equity_refresh_job` (scheduled 8 PM ET weekdays)
- **Sensor** (`sensors/lean_sensor.py`) — reads manifest, routes to jobs, deletes manifest after queuing

### LEAN Bridge (`lean_bridge/`)

- **`lean_runner.py`** — CLI entry point; checks coverage, writes manifest on miss, re-runs backtest on hit
- **`data_writer.py`** — converts DataFrames to LEAN ZIP (equity OHLCV, prices × 10,000 = deci-cents) and JSON (fundamentals fine universe)
- **`custom_data_reader.py`** — QC `PythonData` subclasses (`PipelineEquityData`, `PipelineFundamentals`) used inside algorithms
- **`base_strategy.py`** — abstract QC algorithm base class; detects environment via `QC_RUN_ENV` env var and routes to local vs. cloud data init

### Database Schema

TimescaleDB at `PGHOST:5432`, database `FinancialData`. Key schemas:
- `ohlcv.prices` — hypertable; PK is `(ts_event, ticker, resolution)`; source column tracks Databento vs FMP
- `fundamentals.*` — income_statement, balance_sheet, cash_flow, key_metrics
- `corporate_actions.*` — dividends, splits
- `lean.written_files` — tracks what has been written to the LEAN data library

### Environment Variables

| Variable | Purpose |
|---|---|
| `DATABENTO_API_KEY` | Databento Historical API |
| `FMP_API_KEY` | Financial Modeling Prep REST API |
| `PGHOST` | TimescaleDB LAN IP (TARS host — not localhost) |
| `PGPORT` / `PGDB` / `PGUSER` / `PGPASS` | DB connection |
| `QC_RUN_ENV` | `local` = use pipeline custom data; anything else = QC cloud data |
| `DAGSTER_HOME` | Path to `dagster_home/` directory |

### Writing New Strategies

1. Create `algorithms/MyStrategy/main.py` extending `BaseStrategy` from `lean_bridge/base_strategy.py`
2. Create `algorithms/MyStrategy/data_requirements.json` listing required tickers, dates, resolutions, and sources
3. Run via `lean_runner.py --strategy MyStrategy`

The `data_requirements.json` schema:
```json
[{ "ticker": "SPY", "resolution": "daily", "asset_class": "equity", "start_date": "2022-01-01", "end_date": "2023-12-31", "source": "databento" }]
```

### LEAN Data Format Details

- **Equity OHLCV ZIPs:** `data/equity/usa/{resolution}/{ticker}.zip` — CSV inside, no header, prices in deci-cents (× 10,000), date column format `YYYYMMDD HH:MM`
- **Fundamentals JSON:** `data/fundamental/fine/{ticker}/{YYYYMMDD}.json` — merged income/balance/cashflow/metrics keyed by date
