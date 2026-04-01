# LEAN Data Platform

Dagster-orchestrated data pipeline: **Databento + FMP → PostgreSQL → LEAN CLI**.

Algorithms call `request_ohlcv()` / `request_fundamentals()` — the library checks PostgreSQL for gaps, fires Dagster jobs for only the missing data, and the algorithm exits cleanly. Re-run after ingestion completes.

```
Algorithm calls request_ohlcv("SPY", start, end)
        |
        v
CoverageChecker queries PostgreSQL for gaps + checks LEAN ZIP on disk
        |
   All covered? --YES--> Algorithm continues
        |
       NO
        |
        v
PipelineClient fires Dagster job (databento_equity_job)
  - Only fetches missing date segments (cost-controlled)
  - Writes LEAN ZIP to ~/Projects/Algo/data
        |
        v
Algorithm exits cleanly → re-run after Dagster completes
```

---

## Infrastructure

| Component | Host | Details |
|---|---|---|
| Dagster webserver + daemon | HAL-107 (Docker) | Port 3000 |
| PostgreSQL (TimescaleDB) | TARS @ 192.168.17.4 | DB: FinancialData, user: eqty |
| LEAN data files | HAL-107 host | `~/Projects/Algo/data/` |
| Algorithms | HAL-107 host | `~/Projects/Algo/{StrategyName}/` |

---

## Quick Start

### 1. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

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
DAGSTER_HOST=localhost
DAGSTER_PORT=3000
LEAN_DATA_ROOT=/home/your-username/Projects/Algo/data  # full absolute path, no ~
```

### 2. Initialise the database (first time only)

```bash
scp db/init.sql user@192.168.17.4:~/init.sql
# On TARS:
psql -U eqty -d FinancialData < ~/init.sql
```

### 3. Start Dagster

Run from Windows (Anaconda Prompt) - not WSL:

```cmd
cd C:\Users\MP\OneDrive\CASE\Projects\Coding\DataFeeds\lean-data-platform
docker-compose up -d --build
```

Dagster UI: `http://localhost:3000`

### 4. Run a backtest

```bash
lean backtest "ExamplePipelineStrategy" --data-provider-historical Local
```

- **First run (data missing):** algorithm fires Dagster jobs and exits cleanly. Watch progress in the Dagster UI.
- **Second run (data present):** backtest runs normally.

---

## Deploying to HAL-107 (or any Linux server)

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

Key values:

```
LEAN_DATA_ROOT=/home/mp/Projects/Algo/data   # full absolute path, no ~
DAGSTER_HOST=localhost
PGHOST=192.168.17.4
```

> Docker Compose does NOT expand `~` — use the full absolute path.

### 3. Start Dagster

```bash
docker-compose up -d --build
# Dagster UI: http://localhost:3000 or http://hal-107-ip:3000 from another machine
```

### 4. Verify the bind mount

After the first ingestion job completes:

```bash
ls ~/Projects/Algo/data/equity/usa/daily/
# spy.zip  qqq.zip  ...
```

### 5. Add host environment variables

Add to `~/.bashrc` on HAL-107:

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

---

## Integrating into an Existing Algorithm

If you have an existing QC algorithm and want it to use local pipeline data instead of QC cloud data, here is the migration path.

### Step 1 - Make lean_pipeline importable

On HAL-107 the symlink is already set up. In your algorithm file, add this at the top before any imports:

```python
import sys, os
# Adds ~/Projects/Algo to sys.path so lean_pipeline symlink is found
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
```

### Step 2 - Change your base class

**Before:**
```python
class MyStrategy(QCAlgorithm):
    def Initialize(self):
        self.SetStartDate(2023, 1, 1)
        self.SetEndDate(2023, 12, 31)
        self.SetCash(100_000)
        self.AddEquity("SPY", Resolution.Daily)
        self.AddEquity("QQQ", Resolution.Daily)
```

**After:**
```python
from lean_pipeline.base_strategy import BaseStrategy
from lean_pipeline.custom_data_reader import PipelineEquityData

class MyStrategy(BaseStrategy):
    def Initialize(self):
        self.SetStartDate(2023, 1, 1)
        self.SetEndDate(2023, 12, 31)
        self.SetCash(100_000)
        super().Initialize()  # <-- required: detects local vs cloud, calls init methods

    def _init_local_data(self):
        # Called automatically when QC_RUN_ENV=local
        # request_ohlcv() checks DB + disk; fires Dagster if data missing
        self.request_ohlcv("SPY", self.StartDate, self.EndDate)
        self.request_ohlcv("QQQ", self.StartDate, self.EndDate)

        # Only reached if ALL data is present - register subscriptions here
        self._spy = self.AddData(PipelineEquityData, "SPY", Resolution.Daily).Symbol
        self._qqq = self.AddData(PipelineEquityData, "QQQ", Resolution.Daily).Symbol

    def _init_cloud_data(self):
        # Called automatically when QC_RUN_ENV=cloud (QC platform)
        self._spy = self.AddEquity("SPY", Resolution.Daily).Symbol
        self._qqq = self.AddEquity("QQQ", Resolution.Daily).Symbol
```

### Step 3 - Update OnData to handle both environments

```python
    def OnData(self, data: Slice):
        if self.is_local:
            # Local: use PipelineEquityData custom data type
            if self._spy not in data:
                return
            bar = data[self._spy]
        else:
            # Cloud: use standard equity data
            if not self.Securities[self._spy].HasData:
                return
            bar = self.Securities[self._spy]

        close = bar.Close
        # ... rest of your strategy logic unchanged
```

### Step 4 - Run

```bash
# First run - triggers data ingestion if needed
lean backtest "MyStrategy" --data-provider-historical Local

# Watch Dagster UI for job progress: http://localhost:3000

# Second run - data is present, backtest runs normally
lean backtest "MyStrategy" --data-provider-historical Local
```

### What if I only want local data, no cloud fallback?

If you never intend to run on QC cloud, you can skip the dual-environment pattern and just call `request_ohlcv` directly:

```python
from lean_pipeline.base_strategy import BaseStrategy
from lean_pipeline.custom_data_reader import PipelineEquityData

class MyStrategy(BaseStrategy):
    def Initialize(self):
        self.SetStartDate(2023, 1, 1)
        self.SetEndDate(2023, 12, 31)
        self.SetCash(100_000)
        super().Initialize()

    def _init_local_data(self):
        self.request_ohlcv("AAPL", self.StartDate, self.EndDate)
        self._aapl = self.AddData(PipelineEquityData, "AAPL", Resolution.Daily).Symbol

    def _init_cloud_data(self):
        # Not intended for cloud - raise to make it obvious
        raise NotImplementedError("This strategy is local-only.")
```

### What does request_ohlcv() actually do?

1. Connects to PostgreSQL on TARS, runs `generate_series` to find every missing weekday
2. Checks `~/Projects/Algo/data/equity/usa/daily/{ticker}.zip` exists on disk
3. **If covered:** returns silently — backtest proceeds
4. **If missing:** fires a `databento_equity_job` via Dagster GraphQL, then raises `DataRequestError`
5. `DataRequestError` is caught in `Initialize()`, which calls `self.Quit()` — clean exit
6. Dagster fetches only the missing date segments (not the full range) and writes the LEAN ZIP
7. Re-run the backtest — coverage check passes, backtest runs

---

## Writing a New Algorithm from Scratch

### Step 1 - Make lean_pipeline importable

On HAL-107, the symlink at `~/Projects/Algo/lean_pipeline` is already in place. No copying needed.

### Step 2 - Create your algorithm

```
~/Projects/Algo/
├── lean_pipeline/           <- symlink to lean-data-platform/lean_pipeline
└── MyStrategy/
    └── main.py
```

```python
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from AlgorithmImports import *
from lean_pipeline.base_strategy import BaseStrategy
from lean_pipeline.custom_data_reader import PipelineEquityData, PipelineFundamentals


class MyStrategy(BaseStrategy):

    def Initialize(self):
        self.SetStartDate(2023, 1, 1)
        self.SetEndDate(2025, 12, 31)
        self.SetCash(100_000)
        super().Initialize()

    def _init_local_data(self):
        self.request_ohlcv("SPY", self.StartDate, self.EndDate)
        self.request_ohlcv("QQQ", self.StartDate, self.EndDate)

        # Optional: fundamental data (P/E, revenue, etc.)
        # self.request_fundamentals("AAPL")

        self._spy = self.AddData(PipelineEquityData, "SPY", Resolution.Daily).Symbol
        self._qqq = self.AddData(PipelineEquityData, "QQQ", Resolution.Daily).Symbol

    def _init_cloud_data(self):
        self._spy = self.AddEquity("SPY", Resolution.Daily).Symbol
        self._qqq = self.AddEquity("QQQ", Resolution.Daily).Symbol

    def OnData(self, data: Slice):
        if self.is_local:
            if self._spy not in data:
                return
            close = data[self._spy].Close
        else:
            if not self.Securities[self._spy].HasData:
                return
            close = self.Securities[self._spy].Close

        if not self.Portfolio.Invested:
            self.SetHoldings(self._spy, 1.0)
```

### Step 3 - Run

```bash
cd ~/Projects/Algo
lean backtest "MyStrategy" --data-provider-historical Local
```

---

## request_ohlcv() options

```python
self.request_ohlcv(
    ticker     = "AAPL",
    start_date = self.StartDate,
    end_date   = self.EndDate,
    resolution = "daily",        # "daily" | "minute" | "hourly"
    dataset    = "XNAS.ITCH",    # Databento dataset:
                                 #   XNAS.ITCH   - NASDAQ equities (default)
                                 #   XNYS.PILLAR - NYSE equities
                                 #   GLBX.MDP3   - futures
                                 #   OPRA.PILLAR - options
)
```

## request_fundamentals() options

```python
self.request_fundamentals(
    ticker = "AAPL",
    period = "annual",    # "annual" | "quarter"
    limit  = 20,          # number of periods to fetch
)

# In OnData, access via PipelineFundamentals:
if self._fund_symbol in data:
    pe = data[self._fund_symbol].GetProperty("ValuationRatios.PERatio")
```

---

## Bulk Seeding Historical Data

Seed the full Russell 2000 from FMP (OHLCV + fundamentals + corporate actions):

```bash
# Test with watchlist ETFs first
python scripts/seed_russell2000.py --tickers QQQ,SPY,IWM,GLD,TLT --delay 0.1

# Full Russell 2000 seed (safe to interrupt, resumes)
python scripts/seed_russell2000.py --delay 0.1 --resume

# OHLCV only
python scripts/seed_russell2000.py --skip-fundamentals --skip-corporate --delay 0.1

# Inside Docker container
docker exec -it lean_dagster_webserver python /app/scripts/seed_russell2000.py --resume
```

Progress is saved every 50 tickers to `scripts/seed_progress.json`.

---

## Data Sources & Routing

| Asset | Resolution | Source | Notes |
|---|---|---|---|
| US Equity OHLCV | daily / minute | Databento | On-demand via Dagster; cost-controlled to missing segments only |
| Fundamentals | annual / quarter | FMP | Income, balance, cash flow, key metrics |
| Corporate actions | - | FMP | Dividends and splits |
| Historical seed | daily | FMP | `seed_russell2000.py` for bulk backfill |

---

## Nightly Watchlist Refresh

`daily_equity_refresh_schedule` runs at 20:00 UTC Mon-Fri and refreshes the last 7 days for all tickers in `manifests/watchlist.json`:

```json
{ "tickers": ["QQQ", "SPY", "IWM", "GLD", "TLT"], "resolution": "daily" }
```

Add tickers to this file to include them in the nightly pull.
