"""
CoverageChecker
───────────────
Lightweight PostgreSQL client used by BaseStrategy to check data coverage
before firing Dagster jobs. No Dagster imports - safe in any QC environment.
"""

from __future__ import annotations

import os
from datetime import date


class CoverageChecker:
    """
    Connects directly to PostgreSQL to check what data already exists.

    Credentials from environment variables: PGUSER, PGPASS, PGHOST, PGPORT, PGDB

    Falls back gracefully if DB is unreachable - returns not covered so the
    pipeline fetches fresh data.
    """

    def __init__(self, lean_data_root: str | None = None):
        self._conn_params = {
            "host":     os.environ.get("PGHOST", "192.168.17.4"),
            "port":     int(os.environ.get("PGPORT", 5432)),
            "dbname":   os.environ.get("PGDB", "FinancialData"),
            "user":     os.environ.get("PGUSER", "eqty"),
            "password": os.environ.get("PGPASS", ""),
        }
        self._lean_data_root = lean_data_root or os.environ.get(
            "LEAN_DATA_ROOT",
            os.path.expanduser("~/Projects/Algo/data"),
        )

    def _connect(self):
        import psycopg2
        return psycopg2.connect(**self._conn_params)

    def _lean_zip_exists(self, ticker: str, resolution: str = "daily") -> bool:
        import pathlib
        # LEAN directory names differ from internal resolution strings
        lean_dir = {"hourly": "hour", "hour": "hour"}.get(resolution, resolution)
        base = pathlib.Path(self._lean_data_root) / "equity" / "usa" / lean_dir
        if resolution in ("minute", "second"):
            # Intraday: one ZIP per day inside a ticker subdirectory
            ticker_dir = base / ticker.lower()
            return ticker_dir.is_dir() and any(ticker_dir.glob("*_trade.zip"))
        else:
            # Daily / hourly: single ZIP file
            return (base / f"{ticker.lower()}.zip").is_file()

    def is_ohlcv_covered(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        resolution: str = "daily",
    ) -> bool:
        """
        True if ohlcv.prices has no weekday gaps in [start_date, end_date].
        Uses generate_series to check every weekday - not just min/max bounds.
        """
        try:
            conn = self._connect()
            cur  = conn.cursor()
            cur.execute("""
                SELECT COUNT(*)
                FROM generate_series(%s::date, %s::date, '1 day'::interval) AS gs
                WHERE EXTRACT(DOW FROM gs) NOT IN (0, 6)
                  AND gs::date NOT IN (
                      SELECT ts_event::date
                      FROM ohlcv.prices
                      WHERE ticker = %s AND resolution = %s
                  )
            """, (str(start_date), str(end_date), ticker, resolution))
            missing_count = cur.fetchone()[0]
            cur.close()
            conn.close()
            if missing_count != 0:
                return False
        except Exception as e:
            print(f"[CoverageChecker] DB unreachable, assuming not covered: {e}")
            return False

        # PostgreSQL covered - verify the LEAN ZIP exists on disk
        if not self._lean_zip_exists(ticker, resolution):
            print(
                f"[CoverageChecker] {ticker} covered in DB but LEAN ZIP missing at "
                f"{self._lean_data_root}/equity/usa/{resolution}/{ticker.lower()}.zip "
                f"- will trigger re-write from DB"
            )
            return False

        return True

    def get_missing_segments(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        resolution: str = "daily",
    ) -> list[dict]:
        """
        Returns list of {"start": date, "end": date} contiguous missing segments.
        Empty list means fully covered.
        """
        try:
            conn = self._connect()
            cur  = conn.cursor()
            cur.execute("""
                SELECT gs::date
                FROM generate_series(%s::date, %s::date, '1 day'::interval) AS gs
                WHERE EXTRACT(DOW FROM gs) NOT IN (0, 6)
                  AND gs::date NOT IN (
                      SELECT ts_event::date
                      FROM ohlcv.prices
                      WHERE ticker = %s AND resolution = %s
                  )
                ORDER BY gs
            """, (str(start_date), str(end_date), ticker, resolution))
            rows = [r[0] for r in cur.fetchall()]
            cur.close()
            conn.close()
        except Exception as e:
            print(f"[CoverageChecker] DB error in get_missing_segments: {e}")
            return [{"start": start_date, "end": end_date}]

        if not rows:
            return []

        segments = []
        seg_start = rows[0]
        seg_end   = rows[0]

        for d in rows[1:]:
            if (d - seg_end).days > 5:
                segments.append({"start": seg_start, "end": seg_end})
                seg_start = d
            seg_end = d

        segments.append({"start": seg_start, "end": seg_end})
        return segments

    def is_fundamentals_covered(self, ticker: str) -> bool:
        """
        True if fundamentals.income_statement has at least one row for ticker.
        Proxy check - if income statement exists, all tables are assumed populated.
        """
        try:
            conn = self._connect()
            cur  = conn.cursor()
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM fundamentals.income_statement "
                "WHERE ticker = %s LIMIT 1)",
                (ticker,)
            )
            exists = cur.fetchone()[0]
            cur.close()
            conn.close()
            return exists
        except Exception as e:
            print(f"[CoverageChecker] DB error in is_fundamentals_covered: {e}")
            return False
