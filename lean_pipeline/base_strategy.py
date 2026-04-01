"""
BaseStrategy
─────────────
Base class for all pipeline-aware QC algorithms.

Handles:
  - Environment detection (local backtest vs QC cloud)
  - request_ohlcv()        - checks DB, fires Databento job if missing
  - request_fundamentals() - checks DB, fires FMP job if missing
  - Fail-fast pattern      - exits cleanly if data missing so Dagster can fill it

You do NOT need to know your tickers upfront. Call request_ohlcv() for each
ticker you need inside _init_local_data(). If any are missing, all pending
jobs are fired and the algorithm exits - re-run after Dagster completes.

Usage:
    from lean_pipeline.base_strategy import BaseStrategy
    from lean_pipeline.custom_data_reader import PipelineEquityData

    class MyStrategy(BaseStrategy):

        def Initialize(self):
            self.SetStartDate(2023, 1, 1)
            self.SetEndDate(2023, 12, 31)
            self.SetCash(100_000)
            super().Initialize()

        def _init_local_data(self):
            # Call for every ticker you need - dynamic, no upfront list required
            self.request_ohlcv("SPY", self.StartDate, self.EndDate)
            self.request_ohlcv("QQQ", self.StartDate, self.EndDate)

            # Optional - only if your strategy uses fundamental data
            # self.request_fundamentals("SPY")

            # Only reached if ALL data is present in DB
            self.AddData(PipelineEquityData, "SPY", Resolution.Daily)
            self.AddData(PipelineEquityData, "QQQ", Resolution.Daily)

        def _init_cloud_data(self):
            self.AddEquity("SPY", Resolution.Daily)
            self.AddEquity("QQQ", Resolution.Daily)

Environment detection order:
    1. QC_RUN_ENV env var ("local" or "cloud") - most explicit, use this
    2. Hostname match against _LOCAL_HOSTNAMES - fallback
    3. Default: cloud (safe for QC platform deployment)
"""

from AlgorithmImports import *   # noqa
import os
import platform
from enum import Enum


class RunEnvironment(Enum):
    LOCAL = "local"
    CLOUD = "cloud"


_LOCAL_HOSTNAMES: frozenset[str] = frozenset({
    # Add your machine hostname here as a fallback (run `hostname` in terminal)
    # e.g. "TARS", "my-dev-box"
    # Only used when QC_RUN_ENV env var is not set
})


def _detect_environment() -> RunEnvironment:
    env_var = os.environ.get("QC_RUN_ENV", "").strip().lower()
    if env_var == "local":
        return RunEnvironment.LOCAL
    if env_var == "cloud":
        return RunEnvironment.CLOUD
    if platform.node().strip() in _LOCAL_HOSTNAMES:
        return RunEnvironment.LOCAL
    return RunEnvironment.CLOUD


class DataRequestError(Exception):
    """Raised when data is missing and Dagster jobs have been fired to fetch it."""
    pass


class BaseStrategy(QCAlgorithm):

    run_env:  RunEnvironment
    is_local: bool
    _requested_tickers: list
    _missing_data: list

    def Initialize(self) -> None:
        self.run_env  = _detect_environment()
        self.is_local = (self.run_env == RunEnvironment.LOCAL)
        self._requested_tickers = []
        self._missing_data      = []

        self.Log(
            f"[BaseStrategy] env={self.run_env.value} | "
            f"host={platform.node()} | "
            f"QC_RUN_ENV={os.environ.get('QC_RUN_ENV', 'not set')}"
        )

        if self.is_local:
            try:
                self._init_local_data()
            except DataRequestError as e:
                # Jobs fired - exit cleanly, re-run after Dagster completes
                self.Log(str(e))
                self.Quit(str(e))
                return
        else:
            self._init_cloud_data()

    def _init_local_data(self) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} must implement _init_local_data()")

    def _init_cloud_data(self) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} must implement _init_cloud_data()")

    # ── Data request methods ──────────────────────────────────────────────────

    def request_ohlcv(
        self,
        ticker: str,
        start_date,
        end_date,
        resolution: str = "daily",
        dataset: str = "XNAS.ITCH",
    ) -> None:
        """
        Ensure OHLCV data for ticker+range exists in DB and LEAN data library.

        Checks PostgreSQL for gaps - only fires a Dagster job for missing segments.
        If data is complete: returns silently.
        If data is missing: fires databento_equity_job and raises DataRequestError.

        Args:
            ticker:     Symbol e.g. "SPY", "QQQ", "AAPL" - any ticker, determined at runtime
            start_date: datetime or date
            end_date:   datetime or date
            resolution: "daily" | "minute" | "hourly"
            dataset:    Databento dataset (default "XNAS.ITCH" for NASDAQ equities;
                        use "XNYS.PILLAR" for NYSE, "GLBX.MDP3" for futures)
        """
        from lean_pipeline.coverage_checker import CoverageChecker
        from lean_pipeline.pipeline_client  import PipelineClient

        if hasattr(start_date, "date"):
            start_date = start_date.date()
        if hasattr(end_date, "date"):
            end_date = end_date.date()

        checker = CoverageChecker()

        if checker.is_ohlcv_covered(ticker, start_date, end_date, resolution):
            self.Log(f"[BaseStrategy] {ticker} OHLCV covered ✓")
            self._requested_tickers.append(ticker)
            return

        missing = checker.get_missing_segments(ticker, start_date, end_date, resolution)
        self.Log(
            f"[BaseStrategy] {ticker} missing {len(missing)} segment(s) - "
            f"firing databento_equity_job"
        )

        lean_root = os.environ.get("LEAN_DATA_ROOT", "/app/data")
        client    = PipelineClient()
        run_id    = client.request_ohlcv(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            resolution=resolution,
            dataset=dataset,
            lean_data_root=lean_root,
        )

        self._missing_data.append({
            "ticker":     ticker,
            "resolution": resolution,
            "start":      str(start_date),
            "end":        str(end_date),
            "run_id":     run_id,
        })

        raise DataRequestError(
            f"[BaseStrategy] Data missing for {ticker} ({resolution} "
            f"{start_date} → {end_date}). "
            f"Dagster job fired (run_id={run_id}). "
            f"Re-run this backtest once ingestion completes."
        )

    def request_fundamentals(
        self,
        ticker: str,
        period: str = "annual",
        limit: int = 20,
    ) -> None:
        """
        Ensure FMP fundamental data exists for ticker.

        Optional - only call this if your strategy uses P/E, P/B, revenue, etc.
        If present: returns silently.
        If missing: fires fmp_fundamentals_job and raises DataRequestError.

        Args:
            ticker: Symbol e.g. "SPY"
            period: "annual" | "quarter"
            limit:  Number of periods to fetch (default 20 years annual)
        """
        from lean_pipeline.coverage_checker import CoverageChecker
        from lean_pipeline.pipeline_client  import PipelineClient

        checker = CoverageChecker()

        if checker.is_fundamentals_covered(ticker):
            self.Log(f"[BaseStrategy] {ticker} fundamentals covered ✓")
            return

        self.Log(f"[BaseStrategy] {ticker} fundamentals missing - firing fmp_fundamentals_job")

        lean_root = os.environ.get("LEAN_DATA_ROOT", "/app/data")
        client    = PipelineClient()
        run_id    = client.request_fundamentals(
            ticker=ticker,
            period=period,
            limit=limit,
            lean_data_root=lean_root,
        )

        self._missing_data.append({
            "ticker": ticker,
            "type":   "fundamentals",
            "run_id": run_id,
        })

        raise DataRequestError(
            f"[BaseStrategy] Fundamentals missing for {ticker}. "
            f"Dagster job fired (run_id={run_id}). "
            f"Re-run this backtest once ingestion completes."
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def LogEnv(self, msg: str) -> None:
        self.Log(f"[{self.run_env.value.upper()}] {msg}")
