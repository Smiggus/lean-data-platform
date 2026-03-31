#!/usr/bin/env python3
"""
seed_russell2000.py
───────────────────
Bulk seed: Russell 2000 OHLCV + fundamentals + corporate actions → PostgreSQL.

Usage:
    # Full seed (Russell 2000, all data types, from 2023-01-01)
    python scripts/seed_russell2000.py

    # Resume a previous run
    python scripts/seed_russell2000.py --resume

    # Specific tickers only
    python scripts/seed_russell2000.py --tickers AAPL,MSFT,GOOG

    # OHLCV only, faster delay for premium FMP plan
    python scripts/seed_russell2000.py --skip-fundamentals --skip-corporate --delay 0.05

    # Run from Docker container (has all dependencies)
    docker exec -it lean_dagster_webserver python /app/scripts/seed_russell2000.py --resume

Requirements:
    pip install requests psycopg2-binary
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
import requests

# ── Defaults ───────────────────────────────────────────────────────────────────

FMP_KEY  = os.environ.get("FMP_API_KEY", "3e3302c736aaf63fe4e6195f749bf754")
FMP_BASE = "https://financialmodelingprep.com/stable"

PG = {
    "host":     os.environ.get("PGHOST",   "192.168.17.4"),
    "port":     int(os.environ.get("PGPORT", 5432)),
    "dbname":   os.environ.get("PGDB",     "FinancialData"),
    "user":     os.environ.get("PGUSER",   "eqty"),
    "password": os.environ.get("PGPASS",   "1234"),
}

PROGRESS_FILE = Path(__file__).parent / "seed_progress.json"
START_DATE    = "2023-01-01"
END_DATE      = date.today().isoformat()

# ── FMP → DB column maps ───────────────────────────────────────────────────────

_INCOME_MAP = {
    "revenue":                        "revenue",
    "costOfRevenue":                  "cost_of_revenue",
    "grossProfit":                    "gross_profit",
    "operatingExpenses":              "operating_expenses",
    "operatingIncome":                "operating_income",
    "ebitda":                         "ebitda",
    "netIncome":                      "net_income",
    "eps":                            "eps",
    "epsDiluted":                     "eps_diluted",
    "weightedAverageShsOut":          "shares_outstanding",
    "weightedAverageShsOutDil":       "shares_outstanding_diluted",
    "researchAndDevelopmentExpenses": "research_and_development",
}

_BALANCE_MAP = {
    "totalAssets":             "total_assets",
    "totalCurrentAssets":      "current_assets",
    "cashAndCashEquivalents":  "cash_and_equivalents",
    "shortTermInvestments":    "short_term_investments",
    "netReceivables":          "net_receivables",
    "inventory":               "inventory",
    "totalLiabilities":        "total_liabilities",
    "totalCurrentLiabilities": "current_liabilities",
    "longTermDebt":            "long_term_debt",
    "totalDebt":               "total_debt",
    "totalStockholdersEquity": "total_equity",
    "retainedEarnings":        "retained_earnings",
}

_CASHFLOW_MAP = {
    "operatingCashFlow":      "operating_cash_flow",
    "capitalExpenditure":     "capital_expenditure",
    "freeCashFlow":           "free_cash_flow",
    "dividendsPaid":          "dividends_paid",
    "netChangeInCash":        "net_change_in_cash",
    "debtRepayment":          "debt_repayment",
    "stockBasedCompensation": "stock_based_compensation",
}

_METRICS_MAP = {
    "peRatio":                 "pe_ratio",
    "pbRatio":                 "pb_ratio",
    "priceToSalesRatio":       "ps_ratio",
    "evToEbitda":              "ev_ebitda",
    "evToRevenue":             "ev_revenue",
    "returnOnEquity":          "roe",
    "returnOnAssets":          "roa",
    "returnOnCapitalEmployed": "roic",
    "debtToEquity":            "debt_to_equity",
    "currentRatio":            "current_ratio",
    "bookValuePerShare":       "book_value_ps",
    "freeCashFlowPerShare":    "fcf_ps",
}

# ── FMP API ────────────────────────────────────────────────────────────────────

def fmp_get(endpoint: str, params: dict | None = None, delay: float = 0.3) -> Any:
    """GET from FMP stable API. Returns parsed JSON or None on error."""
    p = {**(params or {}), "apikey": FMP_KEY}
    url = f"{FMP_BASE}/{endpoint}"
    for attempt in range(3):
        try:
            r = requests.get(url, params=p, timeout=30)
            if r.status_code == 429:
                wait = 61 * (attempt + 1)
                print(f"\n  [RATE LIMIT] waiting {wait}s ...", flush=True)
                time.sleep(wait)
                continue
            if r.status_code in (402, 404):
                # 402 = endpoint not on this plan; 404 = endpoint doesn't exist
                return None
            r.raise_for_status()
            time.sleep(delay)
            data = r.json()
            if isinstance(data, dict) and "error" in data:
                return None
            return data
        except requests.exceptions.RequestException as e:
            if attempt == 2:
                print(f"\n  [API ERROR] {endpoint}: {e}", flush=True)
            time.sleep(2 ** attempt)
    return None

# ── PostgreSQL ─────────────────────────────────────────────────────────────────

def pg_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(**PG)


def insert_ohlcv(conn, rows: list[dict]) -> int:
    if not rows:
        return 0
    stmt = """
        INSERT INTO ohlcv.prices
            (ts_event, ticker, open, high, low, close, volume, resolution, source)
        VALUES
            (%(ts_event)s, %(ticker)s, %(open)s, %(high)s, %(low)s, %(close)s,
             %(volume)s, %(resolution)s, %(source)s)
        ON CONFLICT (ts_event, ticker, resolution) DO UPDATE SET
            open   = EXCLUDED.open,
            high   = EXCLUDED.high,
            low    = EXCLUDED.low,
            close  = EXCLUDED.close,
            volume = EXCLUDED.volume,
            source = EXCLUDED.source
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, stmt, rows, page_size=500)
    conn.commit()
    return len(rows)


def insert_fundamentals(conn, ticker: str, table: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.execute(f'DELETE FROM fundamentals."{table}" WHERE ticker = %s', (ticker,))
        cols    = list(rows[0].keys())
        col_sql = ", ".join(f'"{c}"' for c in cols)
        val_sql = ", ".join(f"%({c})s" for c in cols)
        stmt    = f'INSERT INTO fundamentals."{table}" ({col_sql}) VALUES ({val_sql})'
        psycopg2.extras.execute_batch(cur, stmt, rows)
    conn.commit()
    return len(rows)


def insert_dividends(conn, ticker: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.execute("DELETE FROM corporate_actions.dividends WHERE ticker = %s", (ticker,))
        stmt = """
            INSERT INTO corporate_actions.dividends
                (ticker, ex_date, record_date, payment_date, declaration_date, amount, adj_dividend)
            VALUES
                (%(ticker)s, %(ex_date)s, %(record_date)s, %(payment_date)s,
                 %(declaration_date)s, %(amount)s, %(adj_dividend)s)
        """
        psycopg2.extras.execute_batch(cur, stmt, rows)
    conn.commit()
    return len(rows)


def insert_splits(conn, ticker: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.execute("DELETE FROM corporate_actions.splits WHERE ticker = %s", (ticker,))
        stmt = """
            INSERT INTO corporate_actions.splits
                (ticker, split_date, numerator, denominator, ratio_label)
            VALUES
                (%(ticker)s, %(split_date)s, %(numerator)s, %(denominator)s, %(ratio_label)s)
        """
        psycopg2.extras.execute_batch(cur, stmt, rows)
    conn.commit()
    return len(rows)

# ── Data fetchers ──────────────────────────────────────────────────────────────

def fetch_ohlcv(ticker: str, start: str, end: str, delay: float) -> list[dict]:
    # Stable API returns a direct JSON array (no "historical" wrapper)
    data = fmp_get("historical-price-eod/full", {"symbol": ticker, "from": start, "to": end}, delay=delay)
    if not data or not isinstance(data, list):
        return []
    rows = []
    for bar in data:
        d = bar.get("date")
        if not d:
            continue
        rows.append({
            "ts_event":   f"{d} 00:00:00+00",
            "ticker":     ticker,
            "open":       bar.get("open"),
            "high":       bar.get("high"),
            "low":        bar.get("low"),
            "close":      bar.get("close"),
            "volume":     int(bar.get("volume") or 0),
            "resolution": "daily",
            "source":     "fmp",
        })
    return rows


def _map_fundamental_row(raw: dict, col_map: dict, ticker: str, period: str) -> dict:
    row: dict = {
        "ticker":   ticker,
        "date":     raw.get("date"),
        "period":   period,
        "raw_json": psycopg2.extras.Json(raw),
    }
    for fmp_col, db_col in col_map.items():
        val = raw.get(fmp_col)
        row[db_col] = None if val == "" else val
    return row


def fetch_fundamentals(ticker: str, delay: float) -> dict[str, list[dict]]:
    result: dict[str, list[dict]] = {
        "income_statement": [],
        "balance_sheet":    [],
        "cash_flow":        [],
        "key_metrics":      [],
    }
    endpoints = [
        ("income-statement",       "income_statement", _INCOME_MAP),
        ("balance-sheet-statement","balance_sheet",    _BALANCE_MAP),
        ("cash-flow-statement",    "cash_flow",        _CASHFLOW_MAP),
        ("key-metrics",            "key_metrics",      _METRICS_MAP),
    ]
    for period in ("annual", "quarterly"):
        for endpoint, table, col_map in endpoints:
            data = fmp_get(endpoint, {"symbol": ticker, "period": period, "limit": 20}, delay=delay)
            if not data or not isinstance(data, list):
                continue
            for raw in data:
                raw.pop("symbol", None)
                row = _map_fundamental_row(raw, col_map, ticker, period)
                if row["date"]:
                    result[table].append(row)
    return result


def fetch_corporate_actions(ticker: str, delay: float) -> dict:
    divs: list[dict] = []
    spls: list[dict] = []

    # Stable API: "dividends" endpoint, direct array (no "historical" wrapper)
    div_data = fmp_get("dividends", {"symbol": ticker}, delay=delay)
    if div_data and isinstance(div_data, list):
        for r in div_data:
            divs.append({
                "ticker":           ticker,
                "ex_date":          r.get("date"),
                "record_date":      r.get("recordDate")      or None,
                "payment_date":     r.get("paymentDate")     or None,
                "declaration_date": r.get("declarationDate") or None,
                "amount":           r.get("dividend"),
                "adj_dividend":     r.get("adjDividend"),
            })

    # Stable API: "splits" endpoint, direct array, field "splitType" (not "label")
    spl_data = fmp_get("splits", {"symbol": ticker}, delay=delay)
    if spl_data and isinstance(spl_data, list):
        for r in spl_data:
            spls.append({
                "ticker":      ticker,
                "split_date":  r.get("date"),
                "numerator":   r.get("numerator"),
                "denominator": r.get("denominator"),
                "ratio_label": r.get("splitType"),
            })

    return {"dividends": divs, "splits": spls}

# ── Progress file ──────────────────────────────────────────────────────────────

def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"done": [], "errors": {}}


def save_progress(done: set[str], errors: dict[str, str]) -> None:
    PROGRESS_FILE.write_text(json.dumps({
        "done":   sorted(done),
        "errors": errors,
    }, indent=2))

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Bulk seed Russell 2000 data from FMP → PostgreSQL")
    parser.add_argument("--start",             default=START_DATE,  help="OHLCV start date YYYY-MM-DD")
    parser.add_argument("--end",               default=END_DATE,    help="OHLCV end date YYYY-MM-DD")
    parser.add_argument("--delay",             default=0.3, type=float, help="Seconds between FMP calls (default 0.3)")
    parser.add_argument("--tickers",           default=None, help="Comma-separated ticker override (skips R2K lookup)")
    parser.add_argument("--skip-ohlcv",        action="store_true")
    parser.add_argument("--skip-fundamentals", action="store_true")
    parser.add_argument("--skip-corporate",    action="store_true")
    parser.add_argument("--resume",            action="store_true", help="Skip tickers already in seed_progress.json")
    parser.add_argument("--dry-run",           action="store_true", help="Print tickers only, no fetching")
    args = parser.parse_args()

    # ── Ticker list ────────────────────────────────────────────────────────────
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        print(f"Custom list: {len(tickers)} tickers")
    else:
        print("Fetching Russell 2000 constituents from FMP...", flush=True)
        data = fmp_get("russell-2000-constituent", delay=0)
        if not data or not isinstance(data, list):
            print("ERROR: Could not fetch Russell 2000 constituent list. Check FMP_API_KEY.")
            sys.exit(1)
        tickers = sorted({r["symbol"] for r in data if r.get("symbol")})
        print(f"  {len(tickers)} constituents")

    if args.dry_run:
        print("\n".join(tickers))
        return

    # ── Resume ─────────────────────────────────────────────────────────────────
    progress = load_progress() if args.resume else {"done": [], "errors": {}}
    done     = set(progress.get("done", []))
    errors   = dict(progress.get("errors", {}))
    remaining = [t for t in tickers if t not in done]

    if args.resume and done:
        print(f"Resuming: {len(done)} done, {len(remaining)} remaining")

    # ── DB connection ──────────────────────────────────────────────────────────
    try:
        conn = pg_connect()
        print(f"Connected to {PG['host']}:{PG['port']}/{PG['dbname']} as {PG['user']}")
    except Exception as e:
        print(f"ERROR: Cannot connect to PostgreSQL: {e}")
        sys.exit(1)

    # ── Estimate ───────────────────────────────────────────────────────────────
    calls_per_ticker = (
        (0 if args.skip_ohlcv        else 1) +
        (0 if args.skip_fundamentals else 8) +  # 4 endpoints × 2 periods
        (0 if args.skip_corporate    else 2)
    )
    est_min = len(remaining) * calls_per_ticker * args.delay / 60
    print(f"\n{len(remaining)} tickers × {calls_per_ticker} calls × {args.delay}s = ~{est_min:.0f} min estimated")
    print(f"Range: {args.start} → {args.end}\n")

    # ── Seed loop ──────────────────────────────────────────────────────────────
    total   = len(remaining)
    t_start = time.time()

    for i, ticker in enumerate(remaining, 1):
        elapsed = time.time() - t_start
        eta     = (elapsed / i) * (total - i) if i > 1 else 0
        print(
            f"[{i:4d}/{total}]  {ticker:<8s}"
            f"  {elapsed/60:5.1f}m elapsed  eta {eta/60:.0f}m",
            end="  ",
            flush=True,
        )

        try:
            parts = []

            if not args.skip_ohlcv:
                bars = fetch_ohlcv(ticker, args.start, args.end, args.delay)
                n    = insert_ohlcv(conn, bars)
                parts.append(f"ohlcv={n}")

            if not args.skip_fundamentals:
                fund = fetch_fundamentals(ticker, args.delay)
                fn   = sum(insert_fundamentals(conn, ticker, tbl, rows) for tbl, rows in fund.items())
                parts.append(f"fund={fn}")

            if not args.skip_corporate:
                corp = fetch_corporate_actions(ticker, args.delay)
                nd   = insert_dividends(conn, ticker, corp["dividends"])
                ns   = insert_splits(conn, ticker, corp["splits"])
                parts.append(f"div={nd} spl={ns}")

            print("  ".join(parts))
            done.add(ticker)
            errors.pop(ticker, None)

        except Exception as e:
            msg = str(e)
            print(f"ERROR: {msg}", flush=True)
            errors[ticker] = msg
            conn.rollback()
            # Reconnect on broken pipe / connection errors
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn = pg_connect()
            except Exception as ce:
                print(f"Cannot reconnect to DB: {ce} — aborting")
                break

        # Save progress every 50 tickers
        if i % 50 == 0:
            save_progress(done, errors)

    save_progress(done, errors)
    conn.close()

    elapsed_total = time.time() - t_start
    print(f"\n{'─'*60}")
    print(f"Seeded {len(done)} tickers in {elapsed_total/60:.1f} min")
    if errors:
        print(f"{len(errors)} errors (saved to seed_progress.json):")
        for t, e in list(errors.items())[:20]:
            print(f"  {t}: {e}")
        if len(errors) > 20:
            print(f"  ... and {len(errors) - 20} more")


if __name__ == "__main__":
    main()
