-- ─────────────────────────────────────────────────────────────────────────────
-- Migration 001: Partition ohlcv.prices for minute-bar scale
-- Run ONLY when stepping up to minute data. Not needed for daily.
--
-- How to run:
--   docker exec -i <pg_container> psql -U eqty -d FinancialData \
--     < db/migrations/001_partition_ohlcv_for_minute.sql
--
-- Back up first:
--   docker exec -it <pg_container> psql -U eqty -d FinancialData \
--     -c "CREATE TABLE ohlcv.prices_backup AS SELECT * FROM ohlcv.prices;"
-- ─────────────────────────────────────────────────────────────────────────────

BEGIN;

ALTER TABLE ohlcv.prices RENAME TO prices_old;

CREATE TABLE ohlcv.prices (
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
    inserted_at TIMESTAMPTZ      NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (ts_event);

-- Add yearly partitions - extend this list as needed
CREATE TABLE ohlcv.prices_2020 PARTITION OF ohlcv.prices FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE ohlcv.prices_2021 PARTITION OF ohlcv.prices FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE ohlcv.prices_2022 PARTITION OF ohlcv.prices FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE ohlcv.prices_2023 PARTITION OF ohlcv.prices FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE ohlcv.prices_2024 PARTITION OF ohlcv.prices FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE ohlcv.prices_2025 PARTITION OF ohlcv.prices FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE ohlcv.prices_2026 PARTITION OF ohlcv.prices FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE INDEX idx_ohlcv_ticker_res_ts ON ohlcv.prices (ticker, resolution, ts_event DESC);
CREATE INDEX idx_ohlcv_ts_res        ON ohlcv.prices (ts_event DESC, resolution);

INSERT INTO ohlcv.prices
    SELECT id, ts_event, ticker, open, high, low, close, volume, resolution, source, inserted_at
    FROM ohlcv.prices_old;

-- Verify row counts before committing
DO $$
DECLARE
    old_count BIGINT;
    new_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO old_count FROM ohlcv.prices_old;
    SELECT COUNT(*) INTO new_count FROM ohlcv.prices;
    IF old_count <> new_count THEN
        RAISE EXCEPTION 'Row count mismatch: old=% new=%', old_count, new_count;
    END IF;
    RAISE NOTICE 'Migration verified: % rows moved successfully', new_count;
END $$;

-- Uncomment to drop backup after verifying:
-- DROP TABLE ohlcv.prices_old;

COMMIT;
