"""
LeanDataWriter
──────────────
Converts normalised DataFrames into the exact file format LEAN CLI expects.

LEAN equity daily format:
    data/equity/usa/daily/{ticker}.zip  →  {ticker}.csv  (no header)
    Columns: date (YYYYMMDD HH:MM), open, high, low, close, volume
    Price unit: deci-cents (price × 10000, integer)

LEAN fine fundamental format:
    data/fundamental/fine/{ticker}/{YYYYMMDD}.json
"""

from __future__ import annotations

import json
import zipfile
from datetime import date
from pathlib import Path
from typing import Literal

import pandas as pd

Resolution = Literal["daily", "hourly", "minute"]


class LeanDataWriter:

    def __init__(self, lean_data_root: str = "/app/data"):
        self.root = Path(lean_data_root)

    # ── Equity OHLCV ─────────────────────────────────────────────────────────

    def write_equity_ohlcv(
        self,
        df: pd.DataFrame,
        ticker: str,
        resolution: Resolution = "daily",
    ) -> Path:
        """
        Write a normalised OHLCV DataFrame as a LEAN-format ZIP.

        Args:
            df:         DataFrame with ts_event, open, high, low, close, volume
            ticker:     Symbol (stored lowercase)
            resolution: LEAN resolution directory name

        Returns:
            Path to the written ZIP file.
        """
        if df.empty:
            raise ValueError(f"Empty DataFrame for {ticker} — nothing to write")

        lean_df = self._to_lean_columns(df, resolution)
        out_dir = self.root / "equity" / "usa" / resolution
        out_dir.mkdir(parents=True, exist_ok=True)

        csv_path = out_dir / f"{ticker.lower()}.csv"
        zip_path = out_dir / f"{ticker.lower()}.zip"

        lean_df.to_csv(csv_path, index=False, header=False)

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(csv_path, arcname=f"{ticker.lower()}.csv")

        csv_path.unlink()  # ZIP only — keep the data library clean
        return zip_path

    # ── Fundamental (fine universe) ───────────────────────────────────────────

    def write_fine_fundamental(
        self,
        ticker: str,
        income_df: pd.DataFrame,
        balance_df: pd.DataFrame,
        cashflow_df: pd.DataFrame,
        metrics_df: pd.DataFrame,
    ) -> list[Path]:
        """
        Write one LEAN fine fundamental JSON per fiscal period.
        Files go to: data/fundamental/fine/{ticker}/{YYYYMMDD}.json
        """
        out_dir = self.root / "fundamental" / "fine" / ticker.lower()
        out_dir.mkdir(parents=True, exist_ok=True)

        merged  = self._merge_fundamentals(income_df, balance_df, cashflow_df, metrics_df)
        written: list[Path] = []

        for _, row in merged.iterrows():
            row_date = pd.to_datetime(row["date"]).strftime("%Y%m%d")
            payload  = self._build_fine_json(row)
            path     = out_dir / f"{row_date}.json"
            path.write_text(json.dumps(payload, indent=2, default=str))
            written.append(path)

        return written

    # ── Coarse universe ───────────────────────────────────────────────────────

    def write_coarse_entry(
        self,
        ticker: str,
        as_of_date: date,
        close_price: float,
        volume: int,
        dollar_volume: float,
        has_fundamental_data: bool = False,
    ) -> Path:
        """Append/update a row in the LEAN coarse universe CSV."""
        coarse_dir = self.root / "equity" / "usa" / "fundamental" / "coarse"
        coarse_dir.mkdir(parents=True, exist_ok=True)
        coarse_path = coarse_dir / f"{as_of_date.strftime('%Y%m%d')}.csv"

        row = pd.DataFrame([{
            "Ticker":               ticker.upper(),
            "Close":                close_price,
            "Volume":               volume,
            "DollarVolume":         dollar_volume,
            "HasFundamentalData":   str(has_fundamental_data).lower(),
            "PriceScaleFactor":     1,
        }])

        if coarse_path.exists():
            existing = pd.read_csv(coarse_path)
            existing = existing[existing["Ticker"] != ticker.upper()]
            combined = pd.concat([existing, row], ignore_index=True)
        else:
            combined = row

        combined.to_csv(coarse_path, index=False)
        return coarse_path

    # ── Private helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _to_lean_columns(df: pd.DataFrame, resolution: str) -> pd.DataFrame:
        """Transform normalised DataFrame to the exact LEAN CSV column order."""
        out = pd.DataFrame()
        ts  = pd.to_datetime(df["ts_event"], utc=True).dt.tz_convert("America/New_York")

        if resolution == "daily":
            out["date"] = ts.dt.strftime("%Y%m%d 00:00")
        else:
            out["date"] = ts.dt.strftime("%Y%m%d %H:%M")

        for col in ["open", "high", "low", "close"]:
            out[col] = (df[col] * 10_000).round().astype(int)
        out["volume"] = df["volume"].astype(int)

        return out

    @staticmethod
    def _merge_fundamentals(
        income_df: pd.DataFrame,
        balance_df: pd.DataFrame,
        cashflow_df: pd.DataFrame,
        metrics_df: pd.DataFrame,
    ) -> pd.DataFrame:
        dfs = [d for d in [income_df, balance_df, cashflow_df, metrics_df] if not d.empty]
        if not dfs:
            return pd.DataFrame()

        merged = dfs[0]
        for other in dfs[1:]:
            shared = [c for c in ["ticker", "date", "period"] if c in merged.columns and c in other.columns]
            merged = pd.merge(merged, other, on=shared, how="left", suffixes=("", "_dup"))
            merged = merged[[c for c in merged.columns if not c.endswith("_dup")]]

        merged["date"] = pd.to_datetime(merged["date"])
        merged.sort_values("date", inplace=True)
        return merged

    @staticmethod
    def _build_fine_json(row: pd.Series) -> dict:
        """Map column names → LEAN FineFundamental JSON schema."""
        def safe(key: str, default=None):
            val = row.get(key, default)
            return None if pd.isna(val) else val

        return {
            "EarningReports": {
                "BasicEPS":             safe("eps"),
                "DilutedEPS":           safe("epsDiluted"),
                "BasicAverageShares":   safe("weightedAverageShsOut"),
                "DilutedAverageShares": safe("weightedAverageShsOutDil"),
            },
            "FinancialStatements": {
                "IncomeStatement": {
                    "TotalRevenue":          safe("revenue"),
                    "GrossProfit":           safe("grossProfit"),
                    "Ebitda":                safe("ebitda"),
                    "NetIncome":             safe("netIncome"),
                    "OperatingIncome":       safe("operatingIncome"),
                    "ResearchAndDevelopment": safe("researchAndDevelopmentExpenses"),
                },
                "BalanceSheet": {
                    "TotalAssets":                        safe("totalAssets"),
                    "TotalLiabilitiesNetMinorityInterest": safe("totalLiabilities"),
                    "CommonStockEquity":                  safe("totalEquity"),
                    "CashAndCashEquivalents":             safe("cashAndCashEquivalents"),
                    "LongTermDebt":                       safe("longTermDebt"),
                    "TotalDebt":                          safe("totalDebt"),
                },
                "CashFlowStatement": {
                    "OperatingCashFlow":  safe("operatingCashFlow"),
                    "CapitalExpenditure": safe("capitalExpenditure"),
                    "FreeCashFlow":       safe("freeCashFlow"),
                    "DividendsPaid":      safe("dividendsPaid"),
                },
            },
            "ValuationRatios": {
                "PERatio":          safe("peRatio"),
                "PBRatio":          safe("pbRatio"),
                "EVToEBITDA":       safe("evToEbitda"),
                "DebtToEquityRatio": safe("debtToEquity"),
                "ReturnOnEquity":   safe("roe"),
                "ReturnOnAssets":   safe("roa"),
            },
            "Period":   safe("period", "annual"),
            "FileDate": str(pd.to_datetime(row.get("date", "")).strftime("%Y%m%d")
                           if pd.notna(row.get("date")) else ""),
        }
