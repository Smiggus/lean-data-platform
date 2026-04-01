-- ─────────────────────────────────────────────────────────────────────────────
-- LEAN Data Platform - Database Init
-- Target:   FinancialData @ 192.168.17.4:5432 (TARS)
-- Postgres: 16.4
--
-- SAFE TO RUN ON EXISTING DB: only creates new schemas/tables.
-- Existing schemas are NOT touched.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS ohlcv;
CREATE SCHEMA IF NOT EXISTS fundamentals;
CREATE SCHEMA IF NOT EXISTS corporate_actions;
CREATE SCHEMA IF NOT EXISTS lean;
CREATE SCHEMA IF NOT EXISTS dagster;

GRANT ALL ON SCHEMA ohlcv             TO eqty;
GRANT ALL ON SCHEMA fundamentals      TO eqty;
GRANT ALL ON SCHEMA corporate_actions TO eqty;
GRANT ALL ON SCHEMA lean              TO eqty;
GRANT ALL ON SCHEMA dagster           TO eqty;

-- ── OHLCV ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ohlcv.prices (
    id          BIGSERIAL,
    ts_event    TIMESTAMPTZ      NOT NULL,
    ticker      TEXT             NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      BIGINT           NOT NULL,
    resolution  TEXT             NOT NULL DEFAULT 'daily',
    source      TEXT             NOT NULL DEFAULT 'databento',
    inserted_at TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_ohlcv_prices PRIMARY KEY (ts_event, ticker, resolution)
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_ticker_res_ts
    ON ohlcv.prices (ticker, resolution, ts_event DESC);

CREATE INDEX IF NOT EXISTS idx_ohlcv_ts_res
    ON ohlcv.prices (ts_event DESC, resolution);

COMMENT ON TABLE ohlcv.prices IS
    'Consolidated OHLCV. One row per (ticker, ts_event, resolution). '
    'Old schemas (databento_ohlcv, equities) preserved for backward compatibility.';

-- ── Fundamentals ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fundamentals.income_statement (
    id                          BIGSERIAL PRIMARY KEY,
    ticker                      TEXT    NOT NULL,
    date                        DATE    NOT NULL,
    period                      TEXT    NOT NULL DEFAULT 'annual',
    revenue                     DOUBLE PRECISION,
    cost_of_revenue             DOUBLE PRECISION,
    gross_profit                DOUBLE PRECISION,
    operating_expenses          DOUBLE PRECISION,
    operating_income            DOUBLE PRECISION,
    ebitda                      DOUBLE PRECISION,
    net_income                  DOUBLE PRECISION,
    eps                         DOUBLE PRECISION,
    eps_diluted                 DOUBLE PRECISION,
    shares_outstanding          DOUBLE PRECISION,
    shares_outstanding_diluted  DOUBLE PRECISION,
    research_and_development    DOUBLE PRECISION,
    raw_json                    JSONB,
    fetched_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_income UNIQUE (ticker, date, period)
);

CREATE INDEX IF NOT EXISTS idx_income_ticker_date
    ON fundamentals.income_statement (ticker, date DESC);

CREATE TABLE IF NOT EXISTS fundamentals.balance_sheet (
    id                      BIGSERIAL PRIMARY KEY,
    ticker                  TEXT    NOT NULL,
    date                    DATE    NOT NULL,
    period                  TEXT    NOT NULL DEFAULT 'annual',
    total_assets             DOUBLE PRECISION,
    current_assets           DOUBLE PRECISION,
    cash_and_equivalents     DOUBLE PRECISION,
    short_term_investments   DOUBLE PRECISION,
    net_receivables          DOUBLE PRECISION,
    inventory                DOUBLE PRECISION,
    total_liabilities        DOUBLE PRECISION,
    current_liabilities      DOUBLE PRECISION,
    long_term_debt           DOUBLE PRECISION,
    total_debt               DOUBLE PRECISION,
    total_equity             DOUBLE PRECISION,
    retained_earnings        DOUBLE PRECISION,
    raw_json                 JSONB,
    fetched_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_balance UNIQUE (ticker, date, period)
);

CREATE INDEX IF NOT EXISTS idx_balance_ticker_date
    ON fundamentals.balance_sheet (ticker, date DESC);

CREATE TABLE IF NOT EXISTS fundamentals.cash_flow (
    id                       BIGSERIAL PRIMARY KEY,
    ticker                   TEXT    NOT NULL,
    date                     DATE    NOT NULL,
    period                   TEXT    NOT NULL DEFAULT 'annual',
    operating_cash_flow      DOUBLE PRECISION,
    capital_expenditure      DOUBLE PRECISION,
    free_cash_flow           DOUBLE PRECISION,
    dividends_paid           DOUBLE PRECISION,
    net_change_in_cash       DOUBLE PRECISION,
    debt_repayment           DOUBLE PRECISION,
    stock_based_compensation DOUBLE PRECISION,
    raw_json                 JSONB,
    fetched_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_cashflow UNIQUE (ticker, date, period)
);

CREATE INDEX IF NOT EXISTS idx_cashflow_ticker_date
    ON fundamentals.cash_flow (ticker, date DESC);

CREATE TABLE IF NOT EXISTS fundamentals.key_metrics (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT    NOT NULL,
    date            DATE    NOT NULL,
    period          TEXT    NOT NULL DEFAULT 'annual',
    pe_ratio        DOUBLE PRECISION,
    pb_ratio        DOUBLE PRECISION,
    ps_ratio        DOUBLE PRECISION,
    ev_ebitda       DOUBLE PRECISION,
    ev_revenue      DOUBLE PRECISION,
    roe             DOUBLE PRECISION,
    roa             DOUBLE PRECISION,
    roic            DOUBLE PRECISION,
    debt_to_equity  DOUBLE PRECISION,
    current_ratio   DOUBLE PRECISION,
    book_value_ps   DOUBLE PRECISION,
    fcf_ps          DOUBLE PRECISION,
    raw_json        JSONB,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_metrics UNIQUE (ticker, date, period)
);

CREATE INDEX IF NOT EXISTS idx_metrics_ticker_date
    ON fundamentals.key_metrics (ticker, date DESC);

-- ── Corporate actions ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS corporate_actions.dividends (
    id               BIGSERIAL PRIMARY KEY,
    ticker           TEXT    NOT NULL,
    ex_date          DATE    NOT NULL,
    record_date      DATE,
    payment_date     DATE,
    declaration_date DATE,
    amount           DOUBLE PRECISION,
    adj_dividend     DOUBLE PRECISION,
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dividends UNIQUE (ticker, ex_date)
);

CREATE INDEX IF NOT EXISTS idx_dividends_ticker
    ON corporate_actions.dividends (ticker, ex_date DESC);

CREATE TABLE IF NOT EXISTS corporate_actions.splits (
    id           BIGSERIAL PRIMARY KEY,
    ticker       TEXT    NOT NULL,
    split_date   DATE    NOT NULL,
    numerator    DOUBLE PRECISION,
    denominator  DOUBLE PRECISION,
    ratio_label  TEXT,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_splits UNIQUE (ticker, split_date)
);

CREATE INDEX IF NOT EXISTS idx_splits_ticker
    ON corporate_actions.splits (ticker, split_date DESC);

-- ── LEAN tracking ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS lean.written_files (
    id          BIGSERIAL PRIMARY KEY,
    ticker      TEXT    NOT NULL,
    resolution  TEXT    NOT NULL,
    asset_class TEXT    NOT NULL DEFAULT 'equity',
    start_date  DATE    NOT NULL,
    end_date    DATE    NOT NULL,
    file_path   TEXT    NOT NULL,
    written_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_lean_written UNIQUE (ticker, resolution, start_date, end_date)
);

CREATE TABLE IF NOT EXISTS lean.data_manifest (
    id              BIGSERIAL PRIMARY KEY,
    strategy_name   TEXT    NOT NULL,
    ticker          TEXT    NOT NULL,
    resolution      TEXT    NOT NULL,
    asset_class     TEXT    NOT NULL DEFAULT 'equity',
    required_start  DATE    NOT NULL,
    required_end    DATE    NOT NULL,
    source          TEXT    NOT NULL DEFAULT 'databento',
    status          TEXT    NOT NULL DEFAULT 'pending',
    error_msg       TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_manifest_status
    ON lean.data_manifest (status, strategy_name);

CREATE OR REPLACE FUNCTION lean.update_manifest_timestamp()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_manifest_updated ON lean.data_manifest;
CREATE TRIGGER trg_manifest_updated
    BEFORE UPDATE ON lean.data_manifest
    FOR EACH ROW EXECUTE FUNCTION lean.update_manifest_timestamp();

-- ── Grants ────────────────────────────────────────────────────────────────────
GRANT ALL ON ALL TABLES    IN SCHEMA ohlcv             TO eqty;
GRANT ALL ON ALL SEQUENCES IN SCHEMA ohlcv             TO eqty;
GRANT ALL ON ALL TABLES    IN SCHEMA fundamentals      TO eqty;
GRANT ALL ON ALL SEQUENCES IN SCHEMA fundamentals      TO eqty;
GRANT ALL ON ALL TABLES    IN SCHEMA corporate_actions TO eqty;
GRANT ALL ON ALL SEQUENCES IN SCHEMA corporate_actions TO eqty;
GRANT ALL ON ALL TABLES    IN SCHEMA lean              TO eqty;
GRANT ALL ON ALL SEQUENCES IN SCHEMA lean              TO eqty;
GRANT ALL ON ALL TABLES    IN SCHEMA dagster           TO eqty;
GRANT ALL ON ALL SEQUENCES IN SCHEMA dagster           TO eqty;

ALTER DEFAULT PRIVILEGES IN SCHEMA ohlcv             GRANT ALL ON TABLES TO eqty;
ALTER DEFAULT PRIVILEGES IN SCHEMA fundamentals      GRANT ALL ON TABLES TO eqty;
ALTER DEFAULT PRIVILEGES IN SCHEMA corporate_actions GRANT ALL ON TABLES TO eqty;
ALTER DEFAULT PRIVILEGES IN SCHEMA lean              GRANT ALL ON TABLES TO eqty;
ALTER DEFAULT PRIVILEGES IN SCHEMA dagster           GRANT ALL ON TABLES TO eqty;
