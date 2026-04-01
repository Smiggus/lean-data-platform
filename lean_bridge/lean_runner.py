"""
LeanRunner
──────────
Wraps `lean backtest` as a subprocess.
Reads data_requirements.json per strategy, checks DB/filesystem coverage,
writes missing_data_manifest.json if gaps exist (Dagster sensor picks it up),
otherwise shells out to LEAN.

CLI usage:
    python lean_bridge/lean_runner.py --strategy MyStrategy --lean-root /app

Module usage (from a Dagster op):
    runner = LeanRunner(lean_root="/app", strategy_name="MyStrategy")
    result = runner.run()
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


class LeanRunner:

    def __init__(
        self,
        lean_root: str = "/app",
        strategy_name: str = "",
        manifest_dir: str = "/app/manifests",
        timescale_resource=None,
    ):
        self.lean_root    = Path(lean_root)
        self.strategy_name = strategy_name
        self.manifest_dir  = Path(manifest_dir)
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
        self.timescale    = timescale_resource

    @property
    def requirements_path(self) -> Path:
        return self.lean_root / "algorithms" / self.strategy_name / "data_requirements.json"

    @property
    def manifest_path(self) -> Path:
        return self.manifest_dir / "missing_data_manifest.json"

    # ── Requirements ──────────────────────────────────────────────────────────

    def load_requirements(self) -> list[dict]:
        """
        Load data_requirements.json for the strategy.

        Example:
        [
          {
            "ticker":      "SPY",
            "resolution":  "daily",
            "asset_class": "equity",
            "start_date":  "2022-01-01",
            "end_date":    "2023-12-31",
            "source":      "databento"
          },
          {
            "ticker":      "SPY",
            "resolution":  "fundamental",
            "asset_class": "equity",
            "start_date":  "2020-01-01",
            "end_date":    "2023-12-31",
            "source":      "fmp"
          }
        ]
        """
        if not self.requirements_path.exists():
            print(f"[LeanRunner] No data_requirements.json at {self.requirements_path}")
            return []
        with open(self.requirements_path) as f:
            return json.load(f)

    # ── Coverage check ────────────────────────────────────────────────────────

    def check_coverage(self, requirements: list[dict]) -> list[dict]:
        """Return requirements that are NOT yet satisfied."""
        missing: list[dict] = []

        for req in requirements:
            if req.get("resolution") == "fundamental":
                # Check filesystem for fine JSON files
                fine_dir = self.lean_root / "data" / "fundamental" / "fine" / req["ticker"].lower()
                if not fine_dir.exists() or not any(fine_dir.glob("*.json")):
                    missing.append(req)
                continue

            # OHLCV - prefer DB check, fall back to ZIP check
            if self.timescale is not None:
                start = pd.to_datetime(req["start_date"]).tz_localize("UTC")
                end   = pd.to_datetime(req["end_date"]).tz_localize("UTC")
                if not self.timescale.is_covered(req["ticker"], start, end, req["resolution"]):
                    missing.append(req)
            else:
                zip_path = (
                    self.lean_root / "data" / "equity" / "usa"
                    / req["resolution"] / f"{req['ticker'].lower()}.zip"
                )
                if not zip_path.exists():
                    missing.append(req)

        return missing

    # ── Manifest ──────────────────────────────────────────────────────────────

    def write_manifest(self, missing: list[dict]) -> None:
        payload = {
            "strategy_name": self.strategy_name,
            "generated_at":  datetime.now(tz=timezone.utc).isoformat(),
            "missing_data":  missing,
        }
        with open(self.manifest_path, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"[LeanRunner] Manifest written → {self.manifest_path}")

    def clear_manifest(self) -> None:
        if self.manifest_path.exists():
            self.manifest_path.unlink()

    # ── Run ───────────────────────────────────────────────────────────────────

    def run(self) -> dict:
        requirements = self.load_requirements()
        missing      = self.check_coverage(requirements)

        if missing:
            print(f"[LeanRunner] {len(missing)} requirements unmet - writing manifest")
            self.write_manifest(missing)
            return {"status": "missing_data", "missing_data": missing, "output": "", "returncode": -1}

        self.clear_manifest()
        return self._run_lean()

    def _run_lean(self) -> dict:
        cmd = ["lean", "backtest", self.strategy_name, "--data-provider-historical", "Local"]
        print(f"[LeanRunner] Running: {' '.join(cmd)}")

        proc = subprocess.run(cmd, cwd=str(self.lean_root), capture_output=True, text=True)
        output = proc.stdout + proc.stderr
        status = "success" if proc.returncode == 0 else "error"
        print(output)

        return {"status": status, "output": output, "returncode": proc.returncode, "missing_data": []}


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run a LEAN backtest with data pre-check")
    parser.add_argument("--strategy",     required=True)
    parser.add_argument("--lean-root",    default="/app")
    parser.add_argument("--manifest-dir", default="/app/manifests")
    args = parser.parse_args()

    runner = LeanRunner(
        lean_root=args.lean_root,
        strategy_name=args.strategy,
        manifest_dir=args.manifest_dir,
    )
    result = runner.run()

    if result["status"] == "missing_data":
        print("[LeanRunner] Data missing - Dagster sensor will trigger ingestion.")
        sys.exit(1)
    elif result["status"] == "error":
        print(f"[LeanRunner] Backtest failed (rc={result['returncode']})")
        sys.exit(result["returncode"])
    else:
        print("[LeanRunner] Backtest completed successfully.")
        sys.exit(0)
