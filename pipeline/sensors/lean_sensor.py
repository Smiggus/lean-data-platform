"""
LeanMissingDataSensor
──────────────────────
Polls /app/manifests/missing_data_manifest.json written by LeanRunner.
Routes each missing item to the correct Dagster job.
"""

from __future__ import annotations

import json
from pathlib import Path

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    sensor,
)

DATABENTO_JOB_NAME         = "databento_equity_job"
FMP_FUNDAMENTALS_JOB_NAME  = "fmp_fundamentals_job"
FMP_CORP_ACTIONS_JOB_NAME  = "fmp_corporate_actions_job"

MANIFEST_PATH  = Path("/app/manifests/missing_data_manifest.json")
LEAN_DATA_ROOT = "/app/data"


def _route(item: dict) -> str:
    source     = item.get("source", "").lower()
    resolution = item.get("resolution", "daily").lower()

    if source == "fmp" or resolution == "fundamental":
        return FMP_FUNDAMENTALS_JOB_NAME
    return DATABENTO_JOB_NAME


def _build_run_config(item: dict) -> dict:
    job_name   = _route(item)
    ticker     = item["ticker"]
    start_date = item["start_date"]
    end_date   = item["end_date"]
    resolution = item.get("resolution", "daily")

    if job_name == DATABENTO_JOB_NAME:
        dataset_map = {
            "equity": "XNAS.ITCH",
            "option": "OPRA.PILLAR",
            "future": "GLBX.MDP3",
        }
        dataset = dataset_map.get(item.get("asset_class", "equity"), "XNAS.ITCH")
        return {
            "ops": {
                "fetch_ohlcv_op": {"config": {
                    "ticker":     ticker,
                    "start_date": start_date,
                    "end_date":   end_date,
                    "resolution": resolution,
                    "dataset":    dataset,
                }},
                "write_lean_equity_op": {"config": {
                    "ticker":         ticker,
                    "start_date":     start_date,
                    "end_date":       end_date,
                    "resolution":     resolution,
                    "lean_data_root": LEAN_DATA_ROOT,
                }},
            }
        }

    if job_name in (FMP_FUNDAMENTALS_JOB_NAME, FMP_CORP_ACTIONS_JOB_NAME):
        return {
            "ops": {
                "fetch_fundamentals_op": {"config": {
                    "ticker": ticker,
                    "period": item.get("period", "annual"),
                    "limit":  item.get("limit", 20),
                }},
                "write_lean_fundamentals_op": {"config": {
                    "ticker":         ticker,
                    "lean_data_root": LEAN_DATA_ROOT,
                }},
            }
        }

    return {}


@sensor(
    name="lean_missing_data_sensor",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    description="Polls missing_data_manifest.json and routes items to ingest jobs.",
)
def lean_missing_data_sensor(context: SensorEvaluationContext):
    if not MANIFEST_PATH.exists():
        yield SkipReason("No missing data manifest found.")
        return

    try:
        manifest = json.loads(MANIFEST_PATH.read_text())
    except json.JSONDecodeError as exc:
        yield SkipReason(f"Manifest parse error: {exc}")
        return

    missing_items: list[dict] = manifest.get("missing_data", [])
    if not missing_items:
        MANIFEST_PATH.unlink(missing_ok=True)
        yield SkipReason("Manifest empty — nothing to fetch.")
        return

    strategy_name = manifest.get("strategy_name", "unknown")
    context.log.info(f"[lean_sensor] {len(missing_items)} missing items for '{strategy_name}'")

    run_requests: list[RunRequest] = []

    for item in missing_items:
        job_name = _route(item)
        run_key  = (
            f"{strategy_name}"
            f"__{item['ticker']}"
            f"__{item.get('resolution', 'daily')}"
            f"__{item['start_date']}"
            f"__{item['end_date']}"
        )
        run_requests.append(RunRequest(
            run_key=run_key,
            job_name=job_name,
            run_config=_build_run_config(item),
            tags={
                "triggered_by": "lean_missing_data_sensor",
                "strategy":     strategy_name,
                "ticker":       item["ticker"],
            },
        ))
        context.log.info(f"[lean_sensor] Queuing {job_name} for {item['ticker']}")

    MANIFEST_PATH.unlink(missing_ok=True)
    context.log.info("[lean_sensor] Manifest consumed.")
    yield from run_requests
