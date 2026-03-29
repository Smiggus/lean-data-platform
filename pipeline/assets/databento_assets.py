"""
Databento Ops
─────────────
Op chain: fetch_ohlcv_op → store_ohlcv_op → write_lean_equity_op
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
    description="Fetch OHLCV from Databento; skip API call if already in DB.",
)
def fetch_ohlcv_op(context) -> pd.DataFrame:
    cfg        = context.op_config
    ticker     = cfg["ticker"]
    resolution = cfg["resolution"]
    start      = pd.to_datetime(cfg["start_date"]).tz_localize("UTC")
    end        = pd.to_datetime(cfg["end_date"]).tz_localize("UTC")
    dataset    = cfg.get("dataset") or None

    db_res: TimescaleResource  = context.resources.timescale
    dbn:    DatabentoResource  = context.resources.databento

    if db_res.is_covered(ticker, start, end, resolution):
        context.log.info(f"[fetch_ohlcv_op] {ticker} already covered — reading from DB")
        df = db_res.get_ohlcv(ticker, start, end, resolution)
    else:
        df = dbn.fetch_ohlcv(ticker, start, end, resolution, dataset=dataset)

    yield Output(df, output_name="df", metadata={
        "ticker":     ticker,
        "rows":       MetadataValue.int(len(df)),
        "resolution": resolution,
    })


@op(
    required_resource_keys={"timescale"},
    ins={"df": In(pd.DataFrame)},
    out={"stored_df": Out(pd.DataFrame)},
    description="Upsert OHLCV rows into ohlcv.prices.",
)
def store_ohlcv_op(context, df: pd.DataFrame) -> pd.DataFrame:
    rows = context.resources.timescale.upsert_ohlcv(df)
    yield Output(df, output_name="stored_df", metadata={
        "rows_upserted": MetadataValue.int(rows),
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
    ins={"stored_df": In(pd.DataFrame)},
    out=Out(Nothing),
    description="Write LEAN-format ZIP to the local data library.",
)
def write_lean_equity_op(context, stored_df: pd.DataFrame) -> None:
    cfg        = context.op_config
    ticker     = cfg["ticker"]
    resolution = cfg["resolution"]
    start      = pd.to_datetime(cfg["start_date"]).date()
    end        = pd.to_datetime(cfg["end_date"]).date()
    lean_root  = cfg.get("lean_data_root", "/app/data")

    writer   = LeanDataWriter(lean_data_root=lean_root)
    zip_path = writer.write_equity_ohlcv(stored_df, ticker=ticker, resolution=resolution)

    context.resources.timescale.record_lean_write(
        ticker=ticker,
        resolution=resolution,
        start_date=start,
        end_date=end,
        file_path=str(zip_path),
    )
    context.log.info(f"[write_lean_equity_op] Wrote {zip_path}")


@graph(description="Databento → PostgreSQL → LEAN ZIP")
def databento_equity_graph():
    df     = fetch_ohlcv_op()
    stored = store_ohlcv_op(df)
    write_lean_equity_op(stored)
