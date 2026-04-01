"""
BaseStrategy
─────────────
Base class for all pipeline-aware QC algorithms.
Handles environment detection (local vs cloud) in one place.

Environment resolution priority:
  1. QC_RUN_ENV env var  →  "local" | "cloud"
  2. Hostname match against _LOCAL_HOSTNAMES
  3. Default → "cloud"

Usage:
    from base_strategy import BaseStrategy
    from custom_data_reader import PipelineEquityData, PipelineFundamentals

    class MyStrategy(BaseStrategy):

        def _init_local_data(self):
            self.AddData(PipelineEquityData, "SPY", Resolution.Daily)
            self.AddData(PipelineFundamentals, "SPY")

        def _init_cloud_data(self):
            self.AddEquity("SPY", Resolution.Daily)

        def OnData(self, data):
            bar = self.GetPipelineBar(data, "SPY")
            if bar:
                self.Log(f"Pipeline close: {bar.Close}")
"""

from AlgorithmImports import *   # noqa - QC runtime injection
import os
import platform
from enum import Enum


class RunEnvironment(Enum):
    LOCAL = "local"
    CLOUD = "cloud"


# ── Add your local machine hostnames here ─────────────────────────────────────
# Run `hostname` in your terminal and paste the result.
# Only used as fallback when QC_RUN_ENV env var is not set.
_LOCAL_HOSTNAMES: frozenset[str] = frozenset({
    # e.g. "TARS", "Miguels-MacBook-Pro", "DESKTOP-ABC123"
})


def _detect_environment() -> RunEnvironment:
    env_var = os.environ.get("QC_RUN_ENV", "").strip().lower()
    if env_var in ("local", "cloud"):
        return RunEnvironment(env_var)
    if platform.node().strip() in _LOCAL_HOSTNAMES:
        return RunEnvironment.LOCAL
    return RunEnvironment.CLOUD


class BaseStrategy(QCAlgorithm):
    """
    Subclasses must implement _init_local_data() and _init_cloud_data().
    Call super().Initialize() first in your own Initialize().
    """

    run_env:  RunEnvironment
    is_local: bool

    def Initialize(self) -> None:
        self.run_env  = _detect_environment()
        self.is_local = (self.run_env == RunEnvironment.LOCAL)

        self.Log(
            f"[BaseStrategy] env={self.run_env.value} | "
            f"host={platform.node()} | "
            f"QC_RUN_ENV={os.environ.get('QC_RUN_ENV', 'not set')}"
        )

        if self.is_local:
            self._init_local_data()
        else:
            self._init_cloud_data()

    def _init_local_data(self) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} must implement _init_local_data()")

    def _init_cloud_data(self) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} must implement _init_cloud_data()")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def GetPipelineBar(self, data: Slice, ticker: str):
        """Safely get a PipelineEquityData bar. Returns None on cloud or if absent."""
        if not self.is_local:
            return None
        from custom_data_reader import PipelineEquityData
        if ticker not in self.Securities:
            return None
        sym = self.Securities[ticker].Symbol
        return data.Get(PipelineEquityData, sym) if data.ContainsKey(sym) else None

    def GetFundamentals(self, data: Slice, ticker: str):
        """Safely get a PipelineFundamentals object. Returns None on cloud or if absent."""
        if not self.is_local:
            return None
        from custom_data_reader import PipelineFundamentals
        if ticker not in self.Securities:
            return None
        sym = self.Securities[ticker].Symbol
        return data.Get(PipelineFundamentals, sym) if data.ContainsKey(sym) else None

    def LogEnv(self, msg: str) -> None:
        """Log with environment prefix."""
        self.Log(f"[{self.run_env.value.upper()}] {msg}")
