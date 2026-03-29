"""
FMPResource
───────────
ConfigurableResource wrapping Financial Modeling Prep REST API.
Covers: income statement, balance sheet, cash flow, key metrics,
daily prices (fallback), dividends, splits.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

import pandas as pd
import requests
from dagster import ConfigurableResource, get_dagster_logger

Period = Literal["annual", "quarter"]


class FMPResource(ConfigurableResource):
    """
    Config:
        api_key:  FMP API key
        base_url: FMP v3 base URL
    """

    api_key: str
    base_url: str = "https://financialmodelingprep.com/api/v3"

    def _get(self, endpoint: str, params: dict | None = None) -> list | dict:
        p = {**(params or {}), "apikey": self.api_key}
        resp = requests.get(f"{self.base_url}/{endpoint}", params=p, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _tag(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        df["ticker"]     = ticker
        df["fetched_at"] = datetime.now(tz=timezone.utc)
        return df

    # ── Price data (fallback / cross-check) ──────────────────────────────────

    def fetch_daily_prices(self, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        data = self._get(f"historical-price-full/{ticker}", {"from": start_date, "to": end_date})
        if "historical" not in data:
            get_dagster_logger().warning(f"[FMP] No price data for {ticker}")
            return pd.DataFrame()
        df = pd.DataFrame(data["historical"])
        df["ts_event"]   = pd.to_datetime(df["date"], utc=True)
        df["ticker"]     = ticker
        df["resolution"] = "daily"
        df["source"]     = "fmp"
        cols = ["ts_event", "open", "high", "low", "close", "volume", "ticker", "resolution", "source"]
        return df[[c for c in cols if c in df.columns]].copy()

    # ── Fundamentals ──────────────────────────────────────────────────────────

    def fetch_income_statement(self, ticker: str, period: Period = "annual", limit: int = 10) -> pd.DataFrame:
        data = self._get(f"income-statement/{ticker}", {"period": period, "limit": limit})
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["date"]   = pd.to_datetime(df["date"])
        df["period"] = period
        return self._tag(df, ticker)

    def fetch_balance_sheet(self, ticker: str, period: Period = "annual", limit: int = 10) -> pd.DataFrame:
        data = self._get(f"balance-sheet-statement/{ticker}", {"period": period, "limit": limit})
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["date"]   = pd.to_datetime(df["date"])
        df["period"] = period
        return self._tag(df, ticker)

    def fetch_cash_flow(self, ticker: str, period: Period = "annual", limit: int = 10) -> pd.DataFrame:
        data = self._get(f"cash-flow-statement/{ticker}", {"period": period, "limit": limit})
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["date"]   = pd.to_datetime(df["date"])
        df["period"] = period
        return self._tag(df, ticker)

    def fetch_key_metrics(self, ticker: str, period: Period = "annual", limit: int = 10) -> pd.DataFrame:
        data = self._get(f"key-metrics/{ticker}", {"period": period, "limit": limit})
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["date"]   = pd.to_datetime(df["date"])
        df["period"] = period
        return self._tag(df, ticker)

    # ── Corporate actions ─────────────────────────────────────────────────────

    def fetch_dividends(self, ticker: str) -> pd.DataFrame:
        data = self._get(f"historical-price-full/stock_dividend/{ticker}")
        if "historical" not in data:
            return pd.DataFrame()
        df = pd.DataFrame(data["historical"])
        df["ex_date"] = pd.to_datetime(df["date"])
        return self._tag(df, ticker)

    def fetch_splits(self, ticker: str) -> pd.DataFrame:
        data = self._get(f"historical-price-full/stock_split/{ticker}")
        if "historical" not in data:
            return pd.DataFrame()
        df = pd.DataFrame(data["historical"])
        df["split_date"] = pd.to_datetime(df["date"])
        return self._tag(df, ticker)

    # ── Convenience: all fundamentals for one ticker ──────────────────────────

    def fetch_all_fundamentals(self, ticker: str, period: Period = "annual", limit: int = 10) -> dict[str, pd.DataFrame]:
        return {
            "income_statement": self.fetch_income_statement(ticker, period, limit),
            "balance_sheet":    self.fetch_balance_sheet(ticker, period, limit),
            "cash_flow":        self.fetch_cash_flow(ticker, period, limit),
            "key_metrics":      self.fetch_key_metrics(ticker, period, limit),
        }
