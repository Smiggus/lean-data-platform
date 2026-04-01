"""
PipelineClient
──────────────
Fires Dagster jobs via the GraphQL API.
No Dagster imports - uses plain urllib, works in any Python environment.

Endpoint: http://<DAGSTER_HOST>:<DAGSTER_PORT>/graphql
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import date


class PipelineClient:
    """
    Config from environment variables:
        DAGSTER_HOST - hostname of dagster-webserver (default: localhost)
        DAGSTER_PORT - port (default: 3000)
    """

    def __init__(self):
        host = os.environ.get("DAGSTER_HOST", "localhost")
        port = os.environ.get("DAGSTER_PORT", "3000")
        self.graphql_url = f"http://{host}:{port}/graphql"

    def _mutation(self, payload: dict) -> dict:
        body = json.dumps(payload).encode("utf-8")
        req  = urllib.request.Request(
            self.graphql_url,
            data=body,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"[PipelineClient] GraphQL request failed: {e}")
            return {}

    def _launch_run(self, job_name: str, run_config: dict, tags: dict | None = None) -> str | None:
        """Launch a Dagster job and return the run ID, or None on failure."""
        mutation = """
        mutation LaunchRun($executionParams: ExecutionParams!) {
          launchRun(executionParams: $executionParams) {
            __typename
            ... on LaunchRunSuccess { run { runId } }
            ... on PythonError { message }
            ... on InvalidSubsetError { message }
          }
        }
        """
        variables = {
            "executionParams": {
                "selector": {
                    "repositoryLocationName": "lean-data-platform",
                    "repositoryName":         "__repository__",
                    "jobName":                job_name,
                },
                "runConfigData": run_config,
                "tags": [{"key": k, "value": v} for k, v in (tags or {}).items()],
            }
        }
        result = self._mutation({"query": mutation, "variables": variables})
        try:
            launch_result = result["data"]["launchRun"]
            if launch_result["__typename"] == "LaunchRunSuccess":
                run_id = launch_result["run"]["runId"]
                print(f"[PipelineClient] Launched {job_name} → run {run_id}")
                return run_id
            else:
                print(f"[PipelineClient] Launch failed: {launch_result.get('message')}")
                return None
        except (KeyError, TypeError) as e:
            print(f"[PipelineClient] Unexpected response: {e} | {result}")
            return None

    def request_ohlcv(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        resolution: str = "daily",
        dataset: str = "XNAS.ITCH",
        lean_data_root: str = "/app/data",
    ) -> str | None:
        """
        Trigger databento_equity_job for a ticker+date range.
        The job checks DB coverage internally and only fetches missing segments.
        Returns Dagster run ID or None.
        """
        run_config = {
            "ops": {
                "fetch_ohlcv_op": {"config": {
                    "ticker":     ticker,
                    "start_date": str(start_date),
                    "end_date":   str(end_date),
                    "resolution": resolution,
                    "dataset":    dataset,
                }},
                "write_lean_equity_op": {"config": {
                    "ticker":         ticker,
                    "start_date":     str(start_date),
                    "end_date":       str(end_date),
                    "resolution":     resolution,
                    "lean_data_root": lean_data_root,
                }},
            }
        }
        return self._launch_run(
            job_name="databento_equity_job",
            run_config=run_config,
            tags={"triggered_by": "algorithm", "ticker": ticker},
        )

    def request_fundamentals(
        self,
        ticker: str,
        period: str = "annual",
        limit: int = 20,
        lean_data_root: str = "/app/data",
    ) -> str | None:
        """
        Trigger fmp_fundamentals_job for a ticker.
        Returns Dagster run ID or None.
        """
        run_config = {
            "ops": {
                "fetch_fundamentals_op": {"config": {
                    "ticker": ticker,
                    "period": period,
                    "limit":  limit,
                }},
                "write_lean_fundamentals_op": {"config": {
                    "ticker":         ticker,
                    "lean_data_root": lean_data_root,
                }},
            }
        }
        return self._launch_run(
            job_name="fmp_fundamentals_job",
            run_config=run_config,
            tags={"triggered_by": "algorithm", "ticker": ticker},
        )
