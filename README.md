# LEAN Data Platform

Dagster-orchestrated data pipeline: **Databento + FMP → PostgreSQL → LEAN CLI**.

```
lean backtest "MyStrategy" --local
        │
        ▼
LeanRunner checks data_requirements.json
        │
   All data covered?
    ┌───┴───┐
   Yes      No → missing_data_manifest.json written
    │              │
    │       lean_missing_data_sensor fires
    │              │
    │       Routes by source:
    │       ├── equity OHLCV   → databento_equity_job
    │       └── fundamentals   → fmp_fundamentals_job
    │              │
    │       Re-run lean_runner.py
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
├── pipeline/                        ← Dagster code (NOT named 'dagster/')
│   ├── definitions.py               ← entry point
│   ├── resources/
│   │   ├── databento_resource.py
│   │   ├── fmp_resource.py
│   │   └── timescale_resource.py
│   ├── assets/
│   │   ├── databento_assets.py
│   │   └── fmp_assets.py
│   ├── sensors/
│   │   └── lean_sensor.py
│   └── jobs/
│       └── data_jobs.py
├── lean_bridge/
│   ├── data_writer.py               ← DataFrame → LEAN ZIP / JSON
│   ├── lean_runner.py               ← subprocess wrapper + manifest
│   ├── base_strategy.py             ← BaseStrategy (local/cloud routing)
│   └── custom_data_reader.py        ← QC PythonData subclasses
├── db/
│   ├── init.sql                     ← run once on TARS
│   └── migrations/
│       └── 001_partition_ohlcv_for_minute.sql
├── dagster_home/
│   └── dagster.yaml                 ← Dagster storage on Postgres
├── manifests/
│   └── watchlist.json
└── algorithms/
    └── ExamplePipelineStrategy/
        ├── main.py
        └── data_requirements.json
```

## Quick Start

```bash
# 1. Configure
cp .env.example .env
# Fill in DATABENTO_API_KEY and FMP_API_KEY

# 2. Run DB init on TARS (skip if already done)
scp db/init.sql user@192.168.17.4:~/init.sql
# On TARS:
docker exec -i <pg_container> psql -U eqty -d FinancialData < ~/init.sql

# 3. Start Dagster
docker-compose up -d --build
# UI: http://localhost:3000

# 4. Run a backtest with auto data ingestion
python lean_bridge/lean_runner.py --strategy ExamplePipelineStrategy --lean-root .
# If data missing → manifest written → Dagster ingests → re-run
```

## Data Sources & Routing

| Asset | Resolution | Source | Dataset |
|---|---|---|---|
| US Equity | daily / minute | Databento | XNAS.ITCH |
| Options | daily | Databento | OPRA.PILLAR |
| Futures | daily | Databento | GLBX.MDP3 |
| Fundamentals | annual / quarter | FMP | — |
| Corporate actions | — | FMP | — |

## Minute Data (when ready)

Only config changes needed — no code changes:
1. Change `resolution: "minute"` in `data_requirements.json`
2. Run `db/migrations/001_partition_ohlcv_for_minute.sql` on TARS for performance
3. Dagster routes to Databento `ohlcv-1m` schema automatically

## Algorithm Pattern

```python
from base_strategy import BaseStrategy
from custom_data_reader import PipelineEquityData, PipelineFundamentals

class MyStrategy(BaseStrategy):
    def Initialize(self):
        self.SetStartDate(2022, 1, 1)
        self.SetCash(100_000)
        super().Initialize()  # detects local vs cloud

    def _init_local_data(self):
        self.AddData(PipelineEquityData, "SPY", Resolution.Daily)
        self.AddData(PipelineFundamentals, "SPY")

    def _init_cloud_data(self):
        self.AddEquity("SPY", Resolution.Daily)
```

## K8s Migration Notes

- Each Docker service → `Deployment` (stateless)
- `lean_data` volume → `PersistentVolumeClaim`
- `.env` → Kubernetes `Secret`
- PostgreSQL on TARS → repoint `PGHOST` to cluster service or managed DB
- No application code changes needed
