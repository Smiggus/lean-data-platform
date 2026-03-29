"""
FMP Ops
────────
Op chain: fetch_fundamentals_op → store_fundamentals_op → write_lean_fundamentals_op
          fetch_corporate_actions_op → store_corporate_actions_op
"""

from __future__ import annotations

import pandas as pd
from dagster import In, Nothing, Out, Output, graph, op, MetadataValue, get_dagster_logger

from pipeline.resources.fmp_resource import FMPResource
from pipeline.resources.timescale_resource import TimescaleResource
from lean_bridge.data_writer import LeanDataWriter


@op(
    required_resource_keys={"fmp"},
    config_schema={"ticker": str, "period": str, "limit": int},
    out={"fundamentals": Out(dict)},
    description="Fetch income statement, balance sheet, cash flow, key metrics from FMP.",
)
def fetch_fundamentals_op(context) -> dict[str, pd.DataFrame]:
    cfg    = context.op_config
    ticker = cfg["ticker"]
    period = cfg.get("period", "annual")
    limit  = cfg.get("limit", 10)

    result     = context.resources.fmp.fetch_all_fundamentals(ticker, period=period, limit=limit)
    total_rows = sum(len(v) for v in result.values())
    context.log.info(f"[fetch_fundamentals_op] {total_rows} rows across {len(result)} tables for {ticker}")

    yield Output(result, output_name="fundamentals", metadata={
        "ticker":        ticker,
        "tables_fetched": MetadataValue.int(len(result)),
        "total_rows":    MetadataValue.int(total_rows),
    })


@op(
    required_resource_keys={"timescale"},
    ins={"fundamentals": In(dict)},
    out={"stored_fundamentals": Out(dict)},
    description="Upsert fundamental tables into PostgreSQL fundamentals schema.",
)
def store_fundamentals_op(context, fundamentals: dict) -> dict:
    db: TimescaleResource = context.resources.timescale
    rows_written: dict[str, int] = {}

    for table_name, df in fundamentals.items():
        if df.empty:
            continue
        rows = db.upsert_fundamentals(df, table=table_name)
        rows_written[table_name] = rows

    yield Output(fundamentals, output_name="stored_fundamentals", metadata={
        "tables": MetadataValue.json(rows_written),
    })


@op(
    required_resource_keys={"timescale"},
    config_schema={"ticker": str, "lean_data_root": str},
    ins={"stored_fundamentals": In(dict)},
    out=Out(Nothing),
    description="Write LEAN fine fundamental JSON files.",
)
def write_lean_fundamentals_op(context, stored_fundamentals: dict) -> None:
    cfg       = context.op_config
    ticker    = cfg["ticker"]
    lean_root = cfg.get("lean_data_root", "/app/data")

    writer     = LeanDataWriter(lean_data_root=lean_root)
    income_df  = stored_fundamentals.get("income_statement", pd.DataFrame())
    balance_df = stored_fundamentals.get("balance_sheet", pd.DataFrame())
    cashflow_df = stored_fundamentals.get("cash_flow", pd.DataFrame())
    metrics_df = stored_fundamentals.get("key_metrics", pd.DataFrame())

    if not income_df.empty:
        writer.write_fine_fundamental(
            ticker=ticker,
            income_df=income_df,
            balance_df=balance_df,
            cashflow_df=cashflow_df,
            metrics_df=metrics_df,
        )
    context.log.info(f"[write_lean_fundamentals_op] Wrote LEAN fundamental files for {ticker}")


@op(
    required_resource_keys={"fmp"},
    config_schema={"ticker": str},
    out={"actions": Out(dict)},
    description="Fetch dividends and splits from FMP.",
)
def fetch_corporate_actions_op(context) -> dict:
    ticker    = context.op_config["ticker"]
    dividends = context.resources.fmp.fetch_dividends(ticker)
    splits    = context.resources.fmp.fetch_splits(ticker)
    yield Output(
        {"dividends": dividends, "splits": splits},
        output_name="actions",
        metadata={
            "dividend_rows": MetadataValue.int(len(dividends)),
            "split_rows":    MetadataValue.int(len(splits)),
        },
    )


@op(
    required_resource_keys={"timescale"},
    ins={"actions": In(dict)},
    out=Out(Nothing),
    description="Store dividends and splits in corporate_actions schema.",
)
def store_corporate_actions_op(context, actions: dict) -> None:
    db: TimescaleResource = context.resources.timescale
    db.upsert_dividends(actions.get("dividends", pd.DataFrame()))
    db.upsert_splits(actions.get("splits", pd.DataFrame()))


@graph(description="FMP fundamentals → PostgreSQL → LEAN fine universe files")
def fmp_fundamentals_graph():
    fetched = fetch_fundamentals_op()
    stored  = store_fundamentals_op(fetched)
    write_lean_fundamentals_op(stored)


@graph(description="FMP corporate actions → PostgreSQL")
def fmp_corporate_actions_graph():
    actions = fetch_corporate_actions_op()
    store_corporate_actions_op(actions)
