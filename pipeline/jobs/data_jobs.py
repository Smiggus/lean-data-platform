"""
Jobs
────
Wires graphs → jobs with resource configs.
Also defines the nightly scheduled watchlist refresh.
"""

from __future__ import annotations

import os
import pandas as pd
from dagster import (
    ScheduleDefinition,
    graph,
    op,
    Out,
    Nothing,
    get_dagster_logger,
)

from pipeline.assets.databento_assets import databento_equity_graph
from pipeline.assets.fmp_assets import fmp_fundamentals_graph, fmp_corporate_actions_graph


# ── Sensor-triggered jobs ─────────────────────────────────────────────────────

databento_equity_job = databento_equity_graph.to_job(
    name="databento_equity_job",
    description="Databento OHLCV → PostgreSQL → LEAN ZIP",
    tags={"pipeline": "equity_ingestion"},
)

fmp_fundamentals_job = fmp_fundamentals_graph.to_job(
    name="fmp_fundamentals_job",
    description="FMP fundamentals → PostgreSQL → LEAN fine universe files",
    tags={"pipeline": "fundamentals_ingestion"},
)

fmp_corporate_actions_job = fmp_corporate_actions_graph.to_job(
    name="fmp_corporate_actions_job",
    description="FMP dividends + splits → PostgreSQL",
    tags={"pipeline": "corporate_actions"},
)


# ── Nightly watchlist refresh ─────────────────────────────────────────────────
# Reads /app/manifests/watchlist.json and pulls the last N days for each ticker.
# Runs at 20:00 UTC Mon–Fri (after US market close).

@op(
    required_resource_keys={"databento", "timescale"},
    config_schema={
        "watchlist_path": str,
        "lookback_days":  int,
        "lean_data_root": str,
    },
    out=Out(Nothing),
)
def daily_equity_refresh_op(context) -> None:
    import json
    from pathlib import Path
    from lean_bridge.data_writer import LeanDataWriter

    cfg            = context.op_config
    watchlist_path = Path(cfg.get("watchlist_path", "/app/manifests/watchlist.json"))
    lookback       = cfg.get("lookback_days", 7)
    lean_root      = cfg.get("lean_data_root", "/app/data")

    if not watchlist_path.exists():
        context.log.warning(f"[daily_refresh] No watchlist at {watchlist_path}")
        return

    watchlist  = json.loads(watchlist_path.read_text())
    tickers: list[str] = watchlist.get("tickers", [])
    resolution: str    = watchlist.get("resolution", "daily")

    end   = pd.Timestamp.utcnow().normalize()
    start = end - pd.Timedelta(days=lookback)

    dbn    = context.resources.databento
    db     = context.resources.timescale
    writer = LeanDataWriter(lean_data_root=lean_root)

    for ticker in tickers:
        try:
            df = dbn.fetch_ohlcv(ticker, start, end, resolution)
            db.upsert_ohlcv(df)
            # Write LEAN ZIP covering full history in DB
            full_df = db.get_ohlcv(ticker, start - pd.Timedelta(days=365 * 5), end, resolution)
            writer.write_equity_ohlcv(full_df, ticker=ticker, resolution=resolution)
            context.log.info(f"[daily_refresh] Updated {ticker}")
        except Exception as exc:
            context.log.warning(f"[daily_refresh] Failed for {ticker}: {exc}")


@graph
def daily_equity_refresh_graph():
    daily_equity_refresh_op()


daily_equity_refresh_job = daily_equity_refresh_graph.to_job(
    name="daily_equity_refresh_job",
    description="Nightly watchlist refresh — Databento → PostgreSQL → LEAN ZIPs",
    tags={"pipeline": "scheduled"},
)

daily_equity_refresh_schedule = ScheduleDefinition(
    job=daily_equity_refresh_job,
    cron_schedule="0 20 * * 1-5",  # 20:00 UTC Mon–Fri
    name="daily_equity_refresh_schedule",
    run_config={
        "ops": {
            "daily_equity_refresh_op": {
                "config": {
                    "watchlist_path": "/app/manifests/watchlist.json",
                    "lookback_days":  7,
                    "lean_data_root": "/app/data",
                }
            }
        }
    },
)
