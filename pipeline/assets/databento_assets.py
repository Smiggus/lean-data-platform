"""
Databento Ops
─────────────
Op chain: fetch_ohlcv_op → write_lean_equity_op
Upsert is handled inside fetch_ohlcv_op (segment-aware, cost-controlled).
"""

from __future__ import annotations

import pandas as pd
from dagster import In, Nothing, Out, Output, graph, op, MetadataValue, get_dagster_logger

from pipeline.resources.databento_resource import DatabentoResource
from pipeline.resources.timescale_resource import TimescaleResource
from lean_bridge.data_writer import LeanDataWriter


@op(
    required_resource_keys={"databento", "timescale"},
    config_schema={
        "ticker":     str,
        "start_date": str,
        "end_date":   str,
        "resolution": str,
        "dataset":    str,
    },
    out={"df": Out(pd.DataFrame)},
    description=(
        "Fetch OHLCV from Databento for missing segments only. "
        "Skips any date ranges already present in ohlcv.prices."
    ),
)
def fetch_ohlcv_op(context) -> pd.DataFrame:
    cfg        = context.op_config
    ticker     = cfg["ticker"]
    resolution = cfg["resolution"]
    start      = pd.to_datetime(cfg["start_date"]).tz_localize("UTC")
    end        = pd.to_datetime(cfg["end_date"]).tz_localize("UTC")
    dataset    = cfg.get("dataset") or None

    db_res: TimescaleResource = context.resources.timescale
    dbn:    DatabentoResource = context.resources.databento

    missing_segments = db_res.get_missing_segments(
        ticker, start.date(), end.date(), resolution
    )

    if not missing_segments:
        context.log.info(f"[fetch_ohlcv_op] {ticker} fully covered — reading from DB")
        df = db_res.get_ohlcv(ticker, start, end, resolution)
        yield Output(df, output_name="df", metadata={
            "ticker":           ticker,
            "rows":             MetadataValue.int(len(df)),
            "segments_fetched": MetadataValue.int(0),
            "source":           "db_cache",
        })
        return

    context.log.info(
        f"[fetch_ohlcv_op] {ticker} has {len(missing_segments)} missing segment(s)"
    )

    new_df = dbn.fetch_ohlcv(
        ticker, start, end, resolution,
        dataset=dataset,
        segments=missing_segments,
    )

    if not new_df.empty:
        db_res.upsert_ohlcv(new_df)

    df = db_res.get_ohlcv(ticker, start, end, resolution)

    yield Output(df, output_name="df", metadata={
        "ticker":           ticker,
        "rows":             MetadataValue.int(len(df)),
        "segments_fetched": MetadataValue.int(len(missing_segments)),
        "source":           "databento",
    })


@op(
    required_resource_keys={"timescale"},
    config_schema={
        "ticker":         str,
        "start_date":     str,
        "end_date":       str,
        "resolution":     str,
        "lean_data_root": str,
    },
    ins={"df": In(pd.DataFrame)},
    out=Out(Nothing),
    description="Write LEAN-format ZIP to the local data library.",
)
def write_lean_equity_op(context, df: pd.DataFrame) -> None:
    cfg        = context.op_config
    ticker     = cfg["ticker"]
    resolution = cfg["resolution"]
    start      = pd.to_datetime(cfg["start_date"]).date()
    end        = pd.to_datetime(cfg["end_date"]).date()
    lean_root  = cfg.get("lean_data_root", "/app/data")

    writer   = LeanDataWriter(lean_data_root=lean_root)
    zip_path = writer.write_equity_ohlcv(df, ticker=ticker, resolution=resolution)

    context.resources.timescale.record_lean_write(
        ticker=ticker,
        resolution=resolution,
        start_date=start,
        end_date=end,
        file_path=str(zip_path),
    )
    context.log.info(f"[write_lean_equity_op] Wrote {zip_path}")


@graph(description="Databento → PostgreSQL → LEAN ZIP (segment-aware)")
def databento_equity_graph():
    df = fetch_ohlcv_op()
    write_lean_equity_op(df)
