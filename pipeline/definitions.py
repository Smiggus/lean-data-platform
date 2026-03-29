"""
Dagster Definitions
────────────────────
Single entry point loaded by workspace.yaml → pipeline.definitions
"""

from __future__ import annotations

import os

from dagster import Definitions, EnvVar

from pipeline.jobs.data_jobs import (
    databento_equity_job,
    fmp_fundamentals_job,
    fmp_corporate_actions_job,
    daily_equity_refresh_job,
    daily_equity_refresh_schedule,
)
from pipeline.resources.databento_resource import DatabentoResource
from pipeline.resources.fmp_resource import FMPResource
from pipeline.resources.timescale_resource import TimescaleResource
from pipeline.sensors.lean_sensor import lean_missing_data_sensor


resources = {
    "databento": DatabentoResource(
        api_key=EnvVar("DATABENTO_API_KEY"),
    ),
    "fmp": FMPResource(
        api_key=EnvVar("FMP_API_KEY"),
    ),
    "timescale": TimescaleResource(
        pguser=EnvVar("PGUSER"),
        pgpass=EnvVar("PGPASS"),
        pghost=EnvVar("PGHOST"),
        pgport=int(os.environ.get("PGPORT", 5432)),
        pgdb=EnvVar("PGDB"),
    ),
}

defs = Definitions(
    jobs=[
        databento_equity_job,
        fmp_fundamentals_job,
        fmp_corporate_actions_job,
        daily_equity_refresh_job,
    ],
    sensors=[
        lean_missing_data_sensor,
    ],
    schedules=[
        daily_equity_refresh_schedule,
    ],
    resources=resources,
)
