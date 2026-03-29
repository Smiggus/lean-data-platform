"""
DatabentoResource
─────────────────
ConfigurableResource wrapping the Databento Historical client.
Handles schema routing (daily/minute/second), uint64 → int64 coercion,
and ts_event normalisation so all returned DataFrames are ready for
PostgreSQL upsert.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Literal

import databento as db
import numpy as np
import pandas as pd
from dagster import ConfigurableResource, get_dagster_logger

RESOLUTION_TO_SCHEMA: dict[str, str] = {
    "daily":  "ohlcv-1d",
    "hourly": "ohlcv-1h",
    "minute": "ohlcv-1m",
    "second": "ohlcv-1s",
}

ASSET_CLASS_TO_DATASET: dict[str, str] = {
    "equity": "XNAS.ITCH",
    "option": "OPRA.PILLAR",
    "future": "GLBX.MDP3",
}


class DatabentoResource(ConfigurableResource):
    """
    Wraps the Databento Historical API.

    Config:
        api_key:         Databento API key
        default_dataset: Dataset code (default: XNAS.ITCH for NASDAQ equities)
    """

    api_key: str
    default_dataset: str = "XNAS.ITCH"

    def _client(self) -> db.Historical:
        return db.Historical(self.api_key)

    @staticmethod
    def _coerce_uint64(df: pd.DataFrame) -> pd.DataFrame:
        """Postgres cannot store uint64. Downcast to int64 where safe, float64 otherwise."""
        for col in df.select_dtypes(include=["uint64"]).columns:
            if df[col].max() > np.iinfo("int64").max:
                df[col] = df[col].astype("float64")
            else:
                df[col] = df[col].astype("int64")
        return df

    @staticmethod
    def _normalise(df: pd.DataFrame, ticker: str, resolution: str) -> pd.DataFrame:
        """Ensure ts_event is a UTC-aware column and add metadata columns."""
        if df.index.name == "ts_event":
            df = df.reset_index()
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
        base_cols = ["ts_event", "open", "high", "low", "close", "volume"]
        df = df[[c for c in base_cols if c in df.columns]].copy()
        df["ticker"]     = ticker
        df["resolution"] = resolution
        df["source"]     = "databento"
        return df

    def fetch_ohlcv(
        self,
        ticker: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        resolution: Literal["daily", "hourly", "minute", "second"] = "daily",
        dataset: str | None = None,
        buffer_days: int = 3,
    ) -> pd.DataFrame:
        """
        Fetch OHLCV bars from Databento.
        Adds buffer_days on each side to avoid boundary gaps.
        Returns a normalised DataFrame ready for TimescaleResource.upsert_ohlcv.
        """
        logger = get_dagster_logger()
        schema = RESOLUTION_TO_SCHEMA.get(resolution, "ohlcv-1d")
        ds     = dataset or self.default_dataset
        delta  = timedelta(days=buffer_days)

        logger.info(
            f"[Databento] Fetching {ticker} | {schema} | "
            f"{(start_date - delta).date()} → {(end_date + delta).date()} | dataset={ds}"
        )

        try:
            raw = self._client().timeseries.get_range(
                dataset=ds,
                symbols=ticker,
                start=(start_date - delta).strftime("%Y-%m-%d"),
                end=(end_date + delta).strftime("%Y-%m-%d"),
                schema=schema,
            )
            df = raw.to_df()
        except Exception as exc:
            logger.error(f"[Databento] Fetch failed for {ticker}: {exc}")
            raise

        df = self._coerce_uint64(df)
        df = self._normalise(df, ticker=ticker, resolution=resolution)
        logger.info(f"[Databento] Got {len(df)} rows for {ticker}")
        return df

    def fetch_multiple(
        self,
        tickers: list[str],
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        resolution: str = "daily",
        dataset: str | None = None,
    ) -> pd.DataFrame:
        """Fetch a batch of tickers and concatenate results."""
        frames: list[pd.DataFrame] = []
        for ticker in tickers:
            try:
                frames.append(self.fetch_ohlcv(ticker, start_date, end_date, resolution, dataset))
            except Exception as exc:
                get_dagster_logger().warning(f"[Databento] Skipping {ticker}: {exc}")
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
