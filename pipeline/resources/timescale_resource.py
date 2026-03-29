"""
TimescaleResource
─────────────────
ConfigurableResource for all PostgreSQL operations.
Handles OHLCV upserts, gap detection, fundamental upserts,
and LEAN manifest tracking.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Optional

import pandas as pd
from dagster import ConfigurableResource, get_dagster_logger
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool


class TimescaleResource(ConfigurableResource):
    """
    Config (from env):
        pguser, pgpass, pghost, pgport, pgdb
    """

    pguser: str
    pgpass: str
    pghost: str
    pgport: int = 5432
    pgdb: str = "FinancialData"

    def _engine(self):
        url = (
            f"postgresql://{self.pguser}:{self.pgpass}"
            f"@{self.pghost}:{self.pgport}/{self.pgdb}"
        )
        return create_engine(url, poolclass=NullPool, pool_pre_ping=True)

    @contextmanager
    def _conn(self):
        engine = self._engine()
        try:
            with engine.begin() as conn:
                yield conn
        finally:
            engine.dispose()

    # ── OHLCV ─────────────────────────────────────────────────────────────────

    def upsert_ohlcv(self, df: pd.DataFrame) -> int:
        """
        Upsert rows into ohlcv.prices.
        DataFrame must have: ts_event, ticker, open, high, low, close,
                             volume, resolution, source.
        """
        logger = get_dagster_logger()
        if df.empty:
            logger.warning("[DB] upsert_ohlcv called with empty DataFrame")
            return 0

        required = ["ts_event", "ticker", "open", "high", "low", "close", "volume", "resolution", "source"]
        df = df[[c for c in required if c in df.columns]].copy()
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
        rows = df.to_dict(orient="records")

        stmt = text("""
            INSERT INTO ohlcv.prices
                (ts_event, ticker, open, high, low, close, volume, resolution, source)
            VALUES
                (:ts_event, :ticker, :open, :high, :low, :close, :volume, :resolution, :source)
            ON CONFLICT (ts_event, ticker, resolution)
            DO UPDATE SET
                open   = EXCLUDED.open,
                high   = EXCLUDED.high,
                low    = EXCLUDED.low,
                close  = EXCLUDED.close,
                volume = EXCLUDED.volume,
                source = EXCLUDED.source
        """)

        try:
            with self._conn() as conn:
                conn.execute(stmt, rows)
            logger.info(f"[DB] Upserted {len(rows)} rows into ohlcv.prices")
            return len(rows)
        except SQLAlchemyError as exc:
            logger.error(f"[DB] upsert_ohlcv error: {exc}")
            raise

    def get_ohlcv(
        self,
        ticker: str,
        start_date,
        end_date,
        resolution: str = "daily",
    ) -> pd.DataFrame:
        engine = self._engine()
        try:
            df = pd.read_sql(
                text("""
                    SELECT ts_event, ticker, open, high, low, close, volume, resolution, source
                    FROM ohlcv.prices
                    WHERE ticker     = :ticker
                      AND resolution = :resolution
                      AND ts_event  >= :start_date
                      AND ts_event  <= :end_date
                    ORDER BY ts_event ASC
                """),
                engine,
                params={
                    "ticker":     ticker,
                    "resolution": resolution,
                    "start_date": str(start_date),
                    "end_date":   str(end_date),
                },
            )
            df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
            return df
        finally:
            engine.dispose()

    def get_coverage(
        self,
        ticker: str,
        resolution: str = "daily",
    ) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        """Returns (min_ts, max_ts) for ticker+resolution. (None, None) if no data."""
        engine = self._engine()
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT MIN(ts_event), MAX(ts_event)
                        FROM ohlcv.prices
                        WHERE ticker = :ticker AND resolution = :resolution
                    """),
                    {"ticker": ticker, "resolution": resolution},
                ).fetchone()
            if row[0] is None:
                return None, None
            return pd.Timestamp(row[0]), pd.Timestamp(row[1])
        finally:
            engine.dispose()

    def is_covered(
        self,
        ticker: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        resolution: str = "daily",
    ) -> bool:
        """True if DB already has data covering the full [start_date, end_date] range."""
        min_ts, max_ts = self.get_coverage(ticker, resolution)
        if min_ts is None:
            return False
        return (
            start_date.date() >= min_ts.date()
            and end_date.date() <= max_ts.date()
        )

    def get_missing_dates(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        resolution: str = "daily",
    ) -> list[date]:
        """Returns list of weekday dates in [start, end] missing from ohlcv.prices."""
        engine = self._engine()
        try:
            rows = pd.read_sql(
                text("""
                    SELECT gs::date AS missing_date
                    FROM generate_series(:start::date, :end::date, '1 day'::interval) AS gs
                    WHERE EXTRACT(DOW FROM gs) NOT IN (0, 6)
                      AND gs::date NOT IN (
                          SELECT ts_event::date
                          FROM ohlcv.prices
                          WHERE ticker = :ticker AND resolution = :resolution
                      )
                    ORDER BY missing_date
                """),
                engine,
                params={
                    "start":      str(start_date),
                    "end":        str(end_date),
                    "ticker":     ticker,
                    "resolution": resolution,
                },
            )
            return list(rows["missing_date"])
        finally:
            engine.dispose()

    # ── Fundamentals ──────────────────────────────────────────────────────────

    def upsert_fundamentals(self, df: pd.DataFrame, table: str, schema: str = "fundamentals") -> int:
        """
        Upsert a fundamentals DataFrame.
        Deletes existing rows for the ticker then re-inserts (safe for small tables).
        """
        logger = get_dagster_logger()
        if df.empty:
            return 0
        engine = self._engine()
        try:
            ticker = df["ticker"].iloc[0] if "ticker" in df.columns else None
            if ticker:
                with engine.begin() as conn:
                    conn.execute(
                        text(f'DELETE FROM {schema}."{table}" WHERE ticker = :ticker'),
                        {"ticker": ticker},
                    )
            df.to_sql(table, engine, schema=schema, if_exists="append", index=False)
            logger.info(f"[DB] Upserted {len(df)} rows into {schema}.{table}")
            return len(df)
        except SQLAlchemyError as exc:
            logger.error(f"[DB] fundamentals upsert error {schema}.{table}: {exc}")
            raise
        finally:
            engine.dispose()

    def get_fundamentals(self, ticker: str, table: str, schema: str = "fundamentals") -> pd.DataFrame:
        engine = self._engine()
        try:
            return pd.read_sql(
                text(f'SELECT * FROM {schema}."{table}" WHERE ticker = :ticker ORDER BY date DESC'),
                engine,
                params={"ticker": ticker},
            )
        except Exception as exc:
            get_dagster_logger().error(f"[DB] get_fundamentals error: {exc}")
            return pd.DataFrame()
        finally:
            engine.dispose()

    # ── Corporate actions ─────────────────────────────────────────────────────

    def upsert_dividends(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        engine = self._engine()
        try:
            ticker = df["ticker"].iloc[0]
            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM corporate_actions.dividends WHERE ticker = :ticker"),
                    {"ticker": ticker},
                )
            df.to_sql("dividends", engine, schema="corporate_actions", if_exists="append", index=False)
            return len(df)
        finally:
            engine.dispose()

    def upsert_splits(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        engine = self._engine()
        try:
            ticker = df["ticker"].iloc[0]
            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM corporate_actions.splits WHERE ticker = :ticker"),
                    {"ticker": ticker},
                )
            df.to_sql("splits", engine, schema="corporate_actions", if_exists="append", index=False)
            return len(df)
        finally:
            engine.dispose()

    # ── LEAN manifest ─────────────────────────────────────────────────────────

    def record_lean_write(
        self,
        ticker: str,
        resolution: str,
        start_date: date,
        end_date: date,
        file_path: str,
        asset_class: str = "equity",
    ) -> None:
        with self._conn() as conn:
            conn.execute(
                text("""
                    INSERT INTO lean.written_files
                        (ticker, resolution, asset_class, start_date, end_date, file_path)
                    VALUES
                        (:ticker, :resolution, :asset_class, :start_date, :end_date, :file_path)
                    ON CONFLICT (ticker, resolution, start_date, end_date)
                    DO UPDATE SET
                        file_path  = EXCLUDED.file_path,
                        written_at = NOW()
                """),
                {
                    "ticker":      ticker,
                    "resolution":  resolution,
                    "asset_class": asset_class,
                    "start_date":  str(start_date),
                    "end_date":    str(end_date),
                    "file_path":   file_path,
                },
            )

    def get_pending_manifest_items(self) -> pd.DataFrame:
        engine = self._engine()
        try:
            return pd.read_sql(
                text("SELECT * FROM lean.data_manifest WHERE status = 'pending' ORDER BY id"),
                engine,
            )
        finally:
            engine.dispose()

    def update_manifest_status(self, manifest_id: int, status: str) -> None:
        with self._conn() as conn:
            conn.execute(
                text("UPDATE lean.data_manifest SET status = :status WHERE id = :id"),
                {"status": status, "id": manifest_id},
            )
