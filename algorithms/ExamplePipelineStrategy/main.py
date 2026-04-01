"""
ExamplePipelineStrategy
────────────────────────
Demonstrates the lean_pipeline library pattern.

local:  request_ohlcv() checks DB coverage and fires Dagster jobs if missing.
        Re-run after Dagster completes ingestion.
cloud:  native QC data via AddEquity.

Run locally:
    lean backtest "ExamplePipelineStrategy" --data-provider-historical Local

Tickers do NOT need to be declared upfront - call request_ohlcv() for each
ticker your strategy needs inside _init_local_data().
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from AlgorithmImports import *
from lean_pipeline.base_strategy import BaseStrategy
from lean_pipeline.custom_data_reader import PipelineEquityData, PipelineFundamentals


class ExamplePipelineStrategy(BaseStrategy):

    def Initialize(self):
        self.SetStartDate(2023, 1, 1)
        self.SetEndDate(2023, 12, 31)
        self.SetCash(100_000)
        self.SetBenchmark("SPY")
        super().Initialize()

    def _init_local_data(self):
        # request_ohlcv() checks PostgreSQL for gaps.
        # Only missing date ranges are downloaded from Databento.
        # If any data is missing, a Dagster job is fired and the
        # algorithm exits cleanly - re-run after ingestion completes.
        self.request_ohlcv("SPY", self.StartDate, self.EndDate)

        # Add more tickers as needed - determined at runtime, no upfront list:
        # self.request_ohlcv("QQQ", self.StartDate, self.EndDate)
        # self.request_ohlcv("AAPL", self.StartDate, self.EndDate)

        # Optional - only if your strategy needs fundamental data:
        # self.request_fundamentals("SPY")  # fires fmp_fundamentals_job if missing

        # Only reached if all data is present in DB
        self._spy  = self.AddEquity("SPY", Resolution.Daily).Symbol
        self._pipe = self.AddData(PipelineEquityData, "SPY", Resolution.Daily).Symbol
        self.LogEnv("All data present - subscriptions registered")

    def _init_cloud_data(self):
        self._spy  = self.AddEquity("SPY", Resolution.Daily).Symbol
        self._pipe = None
        self.LogEnv("Cloud QC data registered")

    def OnData(self, data: Slice):
        if self.is_local:
            if not self._pipe or self._pipe not in data:
                return
            close = data[self._pipe].Close
        else:
            if not self.Securities[self._spy].HasData:
                return
            close = self.Securities[self._spy].Close

        if not self.Portfolio.Invested:
            self.SetHoldings(self._spy, 1.0)
            self.LogEnv(f"BUY SPY @ {close:.2f}")

    def OnEndOfAlgorithm(self):
        self.LogEnv(f"Final: ${self.Portfolio.TotalPortfolioValue:,.2f}")
