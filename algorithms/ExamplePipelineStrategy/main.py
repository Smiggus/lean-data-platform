"""
ExamplePipelineStrategy
────────────────────────
Demonstrates BaseStrategy local/cloud routing.

Local:  PipelineEquityData (Databento OHLCV) + PipelineFundamentals (FMP)
Cloud:  Native QC AddEquity

Run locally:
    lean backtest "ExamplePipelineStrategy" --data-provider-historical Local

Ensure data exists first:
    python lean_bridge/lean_runner.py --strategy ExamplePipelineStrategy
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "lean_bridge"))

from AlgorithmImports import *           # noqa
from base_strategy import BaseStrategy
from custom_data_reader import PipelineEquityData, PipelineFundamentals


class ExamplePipelineStrategy(BaseStrategy):

    def Initialize(self):
        self.SetStartDate(2022, 1, 1)
        self.SetEndDate(2023, 12, 31)
        self.SetCash(100_000)
        self.SetBenchmark("SPY")

        # BaseStrategy detects env and calls _init_local_data or _init_cloud_data
        super().Initialize()

        self._pe_ratio: float | None = None
        self._invested: bool = False

    def _init_local_data(self):
        self._spy      = self.AddEquity("SPY", Resolution.Daily).Symbol
        self._pipe_sym = self.AddData(PipelineEquityData, "SPY", Resolution.Daily).Symbol
        self._fund_sym = self.AddData(PipelineFundamentals, "SPY").Symbol
        self.LogEnv("Local pipeline data registered — SPY")

    def _init_cloud_data(self):
        self._spy      = self.AddEquity("SPY", Resolution.Daily).Symbol
        self._pipe_sym = None
        self._fund_sym = None
        self.LogEnv("Cloud QC data registered — SPY")

    def OnData(self, data: Slice):
        # Update fundamentals (local only)
        if self.is_local and self._fund_sym and self._fund_sym in data:
            self._pe_ratio = data[self._fund_sym].get("pe_ratio")

        # Get close price from correct source
        if self.is_local:
            if not self._pipe_sym or self._pipe_sym not in data:
                return
            close = data[self._pipe_sym].Close
        else:
            if not self.Securities[self._spy].HasData:
                return
            close = self.Securities[self._spy].Close

        # Simple P/E entry/exit
        if not self._invested:
            if self._pe_ratio is None or self._pe_ratio < 25:
                self.SetHoldings(self._spy, 1.0)
                self._invested = True
                self.LogEnv(f"BUY | P/E={self._pe_ratio} | Close={close:.2f}")
        elif self._pe_ratio and self._pe_ratio > 30:
            self.Liquidate(self._spy)
            self._invested = False
            self.LogEnv(f"SELL | P/E={self._pe_ratio:.2f} | Close={close:.2f}")

    def OnEndOfAlgorithm(self):
        self.LogEnv(f"Final value: ${self.Portfolio.TotalPortfolioValue:,.2f}")
