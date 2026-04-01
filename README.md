# LEAN Data Platform

Dagster-orchestrated data pipeline: **Databento + FMP → PostgreSQL → LEAN CLI**.

Algorithms call `request_ohlcv()` / `request_fundamentals()` directly - the library checks PostgreSQL for gaps, fires Dagster jobs for only the missing data, and the algorithm exits cleanly. Re-run after ingestion completes.

```
Algorithm calls request_ohlcv("SPY", start, end)
        │
        ▼
CoverageChecker queries PostgreSQL for gaps
        │
   All covered?
    ┌───┴───┐
   Yes      No → PipelineClient fires databento_equity_job via GraphQL
    │              │
    │       Dagster fetches missing segments only (cost-controlled)
    │       Stores in PostgreSQL → writes LEAN ZIP
    │              │
    │       Re-run algorithm
    ▼
LEAN backtests with complete data
```

## Stack

| Layer | Tech |
|---|---|
| Orchestration | Dagster 1.7+ |
| Storage | PostgreSQL 16 on TARS (192.168.17.4) |
| Price source | Databento |
| Fundamental source | FMP |
| Backtesting | LEAN CLI (QuantConnect) |
| Runtime | Docker Compose → K8s (future) |

## Structure

```
lean-data-platform/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── workspace.yaml
├── .env.example
├── pipeline/                        ← Dagster code
│   ├── definitions.py               ← entry point
│   ├── resources/
│   │   ├── databento_resource.py    ← segment-aware Databento fetcher
│   │   ├── fmp_resource.py
│   │   └── timescale_resource.py    ← gap detection + upserts
│   ├── assets/
│   │   ├── databento_assets.py      ← fetch_ohlcv_op + write_lean_equity_op
│   │   └── fmp_assets.py
│   ├── sensors/
│   │   └── lean_sensor.py
│   └── jobs/
│       └── data_jobs.py
├── lean_pipeline/                   ← drop-in library for QC algorithms
│   ├── base_strategy.py             ← BaseStrategy with request_ohlcv/fundamentals
│   ├── coverage_checker.py          ← PostgreSQL gap detection (no Dagster import)
│   ├── pipeline_client.py           ← fires Dagster jobs via GraphQL
│   └── custom_data_reader.py        ← QC PythonData readers for pipeline ZIPs
├── lean_bridge/                     ← legacy helpers (kept for compatibility)
│   ├── data_writer.py               ← DataFrame → LEAN ZIP / JSON
│   └── custom_data_reader.py
├── db/
│   └── init.sql                     ← run once on TARS
├── dagster_home/
│   └── dagster.yaml                 ← Dagster storage on Postgres
├── manifests/
│   └── watchlist.json
└── algorithms/
    └── ExamplePipelineStrategy/
        └── main.py
```

## Quick Start

### 1. Configure environment

```bash
cp .env.example .env
```

Edit `.env` - fill in all values:

```
DATABENTO_API_KEY=db-your-key-here
FMP_API_KEY=your-fmp-key-here
PGHOST=192.168.17.4          # LAN IP of TARS - not localhost
PGPORT=5432
PGDB=FinancialData
PGUSER=eqty
PGPASS=your-password
QC_RUN_ENV=local             # tells algorithms to use local pipeline data
DAGSTER_HOME=/app/dagster_home
DAGSTER_HOST=localhost        # used by algorithms to fire jobs
DAGSTER_PORT=3000
LEAN_DATA_ROOT=/app/data      # must match docker-compose.yml volume mount
```

### 2. Initialise the database (once)

Run `db/init.sql` on TARS (the PostgreSQL host):

```bash
scp db/init.sql user@192.168.17.4:~/init.sql
# On TARS:
docker exec -i <pg_container> psql -U eqty -d FinancialData < ~/init.sql
```

### 3. Start Dagster

Run from Windows (Anaconda Prompt) - not WSL, as OneDrive paths are unreliable via `/mnt/c`:

```cmd
cd C:\Users\MP\OneDrive\CASE\Projects\Coding\DataFeeds\lean-data-platform
docker-compose up -d --build
```

Dagster UI: `http://localhost:3000`

### 4. Run a backtest

```bash
lean backtest "ExamplePipelineStrategy" --data-provider-historical Local
```

- **First run (data missing):** algorithm fires Dagster jobs and exits. Dagster fetches only the missing date segments from Databento and writes LEAN ZIPs.
- **Second run (data present):** backtest proceeds normally.

---

## Deploying to HAL-107 (or any Linux server)

Run Dagster on a dedicated machine so all algorithms share one data pipeline and you can monitor everything from a browser.

### Prerequisites

- Docker and Docker Compose installed on HAL-107
- LEAN CLI initialized at `~/Projects/Algo` (creates `~/Projects/Algo/data/`)
- This repo cloned on HAL-107 (e.g. `~/Projects/Coding/DataFeeds/lean-data-platform`)

### 1. One-time host setup

```bash
# Create the LEAN data directory (bind-mount target for Docker)
mkdir -p ~/Projects/Algo/data

# Symlink lean_pipeline so all algorithms can import it without copying
ln -s ~/Projects/Coding/DataFeeds/lean-data-platform/lean_pipeline \
      ~/Projects/Algo/lean_pipeline
```

### 2. Configure `.env`

```bash
cd ~/Projects/Coding/DataFeeds/lean-data-platform
cp .env.example .env
```

Edit `.env` - key HAL-107 values:

```
LEAN_DATA_ROOT=/home/mp/Projects/Algo/data   # ← full absolute path, no ~
DAGSTER_HOST=localhost
DAGSTER_PORT=3000
PGHOST=192.168.17.4
```

> **Important:** Docker Compose does not expand `~`. `LEAN_DATA_ROOT` must be a full absolute path.

### 3. Start Dagster

```bash
docker-compose up -d --build
```

Dagster UI: `http://localhost:3000` (or `http://hal-107-ip:3000` from another machine)

### 4. Verify the bind mount

After the first ingestion job completes:

```bash
ls ~/Projects/Algo/data/equity/usa/daily/
# spy.zip  qqq.zip  ...
```

Files written by Dagster inside the container are immediately visible on the host at `~/Projects/Algo/data`.

### 5. Algorithm setup

Each algorithm only needs `lean_pipeline` on its `sys.path`. Because the symlink lives at `~/Projects/Algo/lean_pipeline`, insert the `Algo/` parent:

```python
import sys, os
# Insert ~/Projects/Algo so `from lean_pipeline...` resolves via the symlink
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from AlgorithmImports import *
from lean_pipeline.base_strategy import BaseStrategy
```

### 6. Host environment variables

Algorithms running on HAL-107 need these set in the shell (add to `~/.bashrc` for persistence):

```bash
export PGHOST=192.168.17.4
export PGPORT=5432
export PGDB=FinancialData
export PGUSER=eqty
export PGPASS=your-password
export LEAN_DATA_ROOT=/home/mp/Projects/Algo/data
export DAGSTER_HOST=localhost
export DAGSTER_PORT=3000
export QC_RUN_ENV=local
```

### 7. Run a backtest

```bash
cd ~/Projects/Algo
lean backtest "MyStrategy" --data-provider-historical Local
```

- **First run:** algorithm detects missing data, fires Dagster job, exits cleanly. Watch the run in the Dagster UI.
- **Second run:** backtest runs normally using data from `~/Projects/Algo/data`.

### Multiple algorithms concurrently

Each algorithm fires its own `launchRun` mutation. Dagster queues and runs them independently. If two algorithms both need SPY, the second job finds the DB already populated, skips Databento, and re-writes the ZIP from PostgreSQL at no cost.

### Remote access (from laptop)

Set `DAGSTER_HOST=192.168.17.X` (HAL-107's LAN IP) in your shell before running `lean backtest` on the laptop, or SSH into HAL-107 where `localhost` works directly.

---

## Writing an Algorithm

### Step 1 - Make `lean_pipeline` importable

On HAL-107, symlink once (see Deploying to HAL-107 above) so all algorithms share one copy:

```
~/Projects/Algo/
├── lean_pipeline/       ← symlink to lean-data-platform/lean_pipeline
├── MyStrategy/
│   └── main.py
└── AnotherStrategy/
    └── main.py
```

No per-algorithm copying needed. Each algorithm inserts `~/Projects/Algo` onto `sys.path` and imports directly.

### Step 2 - Write your algorithm

```python
import sys, os
# Makes lean_pipeline importable via the symlink in ~/Projects/Algo
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from AlgorithmImports import *
from lean_pipeline.base_strategy import BaseStrategy
from lean_pipeline.custom_data_reader import PipelineEquityData, PipelineFundamentals


class MyStrategy(BaseStrategy):

    def Initialize(self):
        self.SetStartDate(2023, 1, 1)
        self.SetEndDate(2023, 12, 31)
        self.SetCash(100_000)
        super().Initialize()   # detects local vs cloud, calls _init_local/cloud_data

    def _init_local_data(self):
        # Call request_ohlcv for every ticker you need.
        # Tickers do not need to be known upfront - add them dynamically.
        # If data is missing: Dagster job fires, algorithm exits cleanly.
        # Re-run after Dagster completes ingestion.
        self.request_ohlcv("SPY", self.StartDate, self.EndDate)
        self.request_ohlcv("QQQ", self.StartDate, self.EndDate)

        # Only if your strategy needs fundamental data (P/E, revenue, etc.):
        # self.request_fundamentals("SPY")

        # Only reached if all data is present in PostgreSQL
        self._spy = self.AddData(PipelineEquityData, "SPY", Resolution.Daily).Symbol

    def _init_cloud_data(self):
        # Falls back to native QC data when running on QC cloud
        self._spy = self.AddEquity("SPY", Resolution.Daily).Symbol

    def OnData(self, data: Slice):
        if self._spy not in data:
            return
        if not self.Portfolio.Invested:
            self.SetHoldings(self._spy, 1.0)

    def OnEndOfAlgorithm(self):
        self.LogEnv(f"Final: ${self.Portfolio.TotalPortfolioValue:,.2f}")
```

### Step 3 - Set environment variables

On your local machine (outside Docker), set:

```bash
export QC_RUN_ENV=local
export PGHOST=192.168.17.4
export PGPORT=5432
export PGDB=FinancialData
export PGUSER=eqty
export PGPASS=your-password
export DAGSTER_HOST=localhost
export DAGSTER_PORT=3000
export LEAN_DATA_ROOT=/app/data
```

Or add them to your shell profile / `.env` file loaded by LEAN.

### Step 4 - Run

```bash
lean backtest "MyStrategy" --data-provider-historical Local
```

### request_ohlcv() options

```python
self.request_ohlcv(
    ticker     = "AAPL",
    start_date = self.StartDate,
    end_date   = self.EndDate,
    resolution = "daily",        # "daily" | "minute" | "hourly"
    dataset    = "XNAS.ITCH",    # Databento dataset:
                                 #   XNAS.ITCH  - NASDAQ equities (default)
                                 #   XNYS.PILLAR - NYSE equities
                                 #   GLBX.MDP3  - futures
                                 #   OPRA.PILLAR - options
)
```

### request_fundamentals() options

```python
self.request_fundamentals(
    ticker = "AAPL",
    period = "annual",    # "annual" | "quarter"
    limit  = 20,          # number of periods to fetch
)
```

Exposes in `OnData` via `PipelineFundamentals`:
`pe_ratio`, `pb_ratio`, `ev_ebitda`, `revenue`, `net_income`, `eps`, `roe`, `roa`, `free_cash_flow`, and more.

---

## Data Sources & Routing

| Asset | Resolution | Source | Dataset |
|---|---|---|---|
| US Equity | daily / minute | Databento | XNAS.ITCH |
| Options | daily | Databento | OPRA.PILLAR |
| Futures | daily | Databento | GLBX.MDP3 |
| Fundamentals | annual / quarter | FMP | - |
| Corporate actions | - | FMP | - |

## Cost Control

Databento charges per byte downloaded. The pipeline only fetches missing data:

1. `CoverageChecker` queries PostgreSQL with `generate_series` to find exact missing weekdays
2. Contiguous gaps are grouped into segments
3. `DatabentoResource.fetch_ohlcv()` fetches one API call per segment (not the full range)
4. Results are upserted - existing rows are never overwritten with duplicate downloads

## Nightly Watchlist Refresh

`daily_equity_refresh_schedule` runs at 20:00 UTC Mon–Fri and pulls the last 7 days for all tickers in `manifests/watchlist.json`:

```json
{ "tickers": ["QQQ", "SPY", "IWM", "GLD", "TLT"], "resolution": "daily" }
```

## K8s Migration Notes

- Each Docker service → `Deployment` (stateless)
- `lean_data` volume → `PersistentVolumeClaim`
- `.env` → Kubernetes `Secret`
- PostgreSQL on TARS → repoint `PGHOST` to cluster service or managed DB
- No application code changes needed
