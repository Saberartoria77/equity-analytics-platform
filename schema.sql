CREATE TABLE IF NOT EXISTS stocks (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(16) UNIQUE NOT NULL,
    name VARCHAR(100),
    sector VARCHAR(100),
    industry VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_prices (
    id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL CHECK (volume >= 0),
    UNIQUE(stock_id, date)
);

CREATE TABLE IF NOT EXISTS indicators (
    id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL,
    date DATE NOT NULL,
    sma_20 DOUBLE PRECISION,
    sma_50 DOUBLE PRECISION,
    sma_200 DOUBLE PRECISION,
    rsi_14 DOUBLE PRECISION,
    macd_line DOUBLE PRECISION,
    macd_signal DOUBLE PRECISION,
    macd_histogram DOUBLE PRECISION,
    bb_upper DOUBLE PRECISION,
    bb_middle DOUBLE PRECISION,
    bb_lower DOUBLE PRECISION,
    CONSTRAINT fk_indicators_stock
        FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE,
    CONSTRAINT uq_indicators_stock_date UNIQUE(stock_id, date)
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    tickers_attempted INTEGER NOT NULL DEFAULT 0,
    tickers_succeeded INTEGER NOT NULL DEFAULT 0,
    rows_affected INTEGER NOT NULL DEFAULT 0,
    errors TEXT
);

-- Upgrade databases created by earlier project versions.
-- Views must be dropped because PostgreSQL cannot reorder/rename their columns
-- through CREATE OR REPLACE VIEW.
DROP VIEW IF EXISTS rolling_volatility_view;
DROP VIEW IF EXISTS daily_returns_view;

ALTER TABLE stocks ADD COLUMN IF NOT EXISTS sector VARCHAR(100);
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS industry VARCHAR(100);
ALTER TABLE stocks ALTER COLUMN ticker TYPE VARCHAR(16);
ALTER TABLE stocks ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at::timestamptz;
UPDATE stocks SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL;
ALTER TABLE stocks ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE stocks ALTER COLUMN created_at SET NOT NULL;
SELECT setval(
    pg_get_serial_sequence('stocks', 'id'),
    COALESCE((SELECT MAX(id) FROM stocks), 1),
    EXISTS (SELECT 1 FROM stocks)
);

ALTER TABLE daily_prices ALTER COLUMN id TYPE BIGINT;
ALTER TABLE daily_prices ALTER COLUMN stock_id TYPE INTEGER USING stock_id::integer;
ALTER TABLE daily_prices ALTER COLUMN open TYPE DOUBLE PRECISION USING open::double precision;
ALTER TABLE daily_prices ALTER COLUMN high TYPE DOUBLE PRECISION USING high::double precision;
ALTER TABLE daily_prices ALTER COLUMN low TYPE DOUBLE PRECISION USING low::double precision;
ALTER TABLE daily_prices ALTER COLUMN close TYPE DOUBLE PRECISION USING close::double precision;
ALTER TABLE daily_prices ALTER COLUMN volume TYPE BIGINT;
ALTER TABLE daily_prices ALTER COLUMN stock_id SET NOT NULL;
ALTER TABLE daily_prices ALTER COLUMN date SET NOT NULL;
ALTER TABLE daily_prices ALTER COLUMN open SET NOT NULL;
ALTER TABLE daily_prices ALTER COLUMN high SET NOT NULL;
ALTER TABLE daily_prices ALTER COLUMN low SET NOT NULL;
ALTER TABLE daily_prices ALTER COLUMN close SET NOT NULL;
ALTER TABLE daily_prices ALTER COLUMN volume SET NOT NULL;
ALTER TABLE daily_prices DROP CONSTRAINT IF EXISTS daily_prices_stock_id_fkey;
ALTER TABLE daily_prices ADD CONSTRAINT daily_prices_stock_id_fkey
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE;
ALTER SEQUENCE daily_prices_id_seq AS BIGINT;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'daily_prices'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%volume >= 0%'
    ) THEN
        ALTER TABLE daily_prices ADD CONSTRAINT daily_prices_volume_nonnegative
            CHECK (volume >= 0);
    END IF;
END
$$;
SELECT setval(
    pg_get_serial_sequence('daily_prices', 'id'),
    COALESCE((SELECT MAX(id) FROM daily_prices), 1),
    EXISTS (SELECT 1 FROM daily_prices)
);

ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'running';
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS rows_affected INTEGER DEFAULT 0;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'ingestion_runs'
          AND column_name = 'run_at'
    ) THEN
        EXECUTE 'UPDATE ingestion_runs
                 SET started_at = run_at,
                     completed_at = COALESCE(completed_at, run_at),
                     status = CASE
                         WHEN errors IS NULL OR errors = '''' THEN ''completed''
                         ELSE ''partial''
                     END
                 WHERE run_at IS NOT NULL';
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'ingestion_runs'
          AND column_name = 'rows_inserted'
    ) THEN
        EXECUTE 'UPDATE ingestion_runs SET rows_affected = COALESCE(rows_inserted, 0)';
    END IF;
END
$$;

UPDATE ingestion_runs
SET started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
    status = COALESCE(
        status,
        CASE WHEN errors IS NULL OR errors = '' THEN 'completed' ELSE 'partial' END
    ),
    tickers_attempted = COALESCE(tickers_attempted, 0),
    tickers_succeeded = COALESCE(tickers_succeeded, 0),
    rows_affected = COALESCE(rows_affected, 0);
ALTER TABLE ingestion_runs ALTER COLUMN id TYPE BIGINT;
ALTER TABLE ingestion_runs ALTER COLUMN started_at SET DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE ingestion_runs ALTER COLUMN started_at SET NOT NULL;
ALTER TABLE ingestion_runs ALTER COLUMN status SET DEFAULT 'running';
ALTER TABLE ingestion_runs ALTER COLUMN status SET NOT NULL;
ALTER TABLE ingestion_runs ALTER COLUMN tickers_attempted SET DEFAULT 0;
ALTER TABLE ingestion_runs ALTER COLUMN tickers_attempted SET NOT NULL;
ALTER TABLE ingestion_runs ALTER COLUMN tickers_succeeded SET DEFAULT 0;
ALTER TABLE ingestion_runs ALTER COLUMN tickers_succeeded SET NOT NULL;
ALTER TABLE ingestion_runs ALTER COLUMN rows_affected SET DEFAULT 0;
ALTER TABLE ingestion_runs ALTER COLUMN rows_affected SET NOT NULL;
ALTER TABLE ingestion_runs DROP COLUMN IF EXISTS run_at;
ALTER TABLE ingestion_runs DROP COLUMN IF EXISTS rows_inserted;
ALTER SEQUENCE ingestion_runs_id_seq AS BIGINT;
SELECT setval(
    pg_get_serial_sequence('ingestion_runs', 'id'),
    COALESCE((SELECT MAX(id) FROM ingestion_runs), 1),
    EXISTS (SELECT 1 FROM ingestion_runs)
);

-- Retrofit the table that older versions allowed Pandas to create without
-- defaults, a primary key, a foreign key, or stock/date uniqueness.
CREATE SEQUENCE IF NOT EXISTS indicators_id_seq OWNED BY indicators.id;
ALTER TABLE indicators ALTER COLUMN id SET DEFAULT nextval('indicators_id_seq');
ALTER TABLE indicators ALTER COLUMN id TYPE BIGINT;
ALTER TABLE indicators ALTER COLUMN stock_id TYPE INTEGER USING stock_id::integer;
ALTER TABLE indicators ALTER COLUMN sma_20 TYPE DOUBLE PRECISION USING sma_20::double precision;
ALTER TABLE indicators ALTER COLUMN sma_50 TYPE DOUBLE PRECISION USING sma_50::double precision;
ALTER TABLE indicators ALTER COLUMN sma_200 TYPE DOUBLE PRECISION USING sma_200::double precision;
ALTER TABLE indicators ALTER COLUMN rsi_14 TYPE DOUBLE PRECISION USING rsi_14::double precision;
ALTER TABLE indicators ALTER COLUMN macd_line TYPE DOUBLE PRECISION USING macd_line::double precision;
ALTER TABLE indicators ALTER COLUMN macd_signal TYPE DOUBLE PRECISION USING macd_signal::double precision;
ALTER TABLE indicators ALTER COLUMN macd_histogram TYPE DOUBLE PRECISION USING macd_histogram::double precision;
ALTER TABLE indicators ALTER COLUMN bb_upper TYPE DOUBLE PRECISION USING bb_upper::double precision;
ALTER TABLE indicators ALTER COLUMN bb_middle TYPE DOUBLE PRECISION USING bb_middle::double precision;
ALTER TABLE indicators ALTER COLUMN bb_lower TYPE DOUBLE PRECISION USING bb_lower::double precision;
ALTER TABLE indicators ALTER COLUMN id SET NOT NULL;
ALTER TABLE indicators ALTER COLUMN stock_id SET NOT NULL;
ALTER TABLE indicators ALTER COLUMN date SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'indicators'::regclass AND contype = 'p'
    ) THEN
        ALTER TABLE indicators ADD CONSTRAINT indicators_pkey PRIMARY KEY (id);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'indicators'::regclass AND conname = 'fk_indicators_stock'
    ) THEN
        ALTER TABLE indicators ADD CONSTRAINT fk_indicators_stock
            FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'indicators'::regclass AND conname = 'uq_indicators_stock_date'
    ) THEN
        ALTER TABLE indicators ADD CONSTRAINT uq_indicators_stock_date
            UNIQUE (stock_id, date);
    END IF;
END
$$;

SELECT setval(
    pg_get_serial_sequence('indicators', 'id'),
    COALESCE((SELECT MAX(id) FROM indicators), 1),
    EXISTS (SELECT 1 FROM indicators)
);

CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON daily_prices(date);
CREATE INDEX IF NOT EXISTS idx_indicators_date ON indicators(date);

CREATE OR REPLACE VIEW daily_returns_view AS
SELECT
    stock_id,
    date,
    close,
    LAG(close) OVER (PARTITION BY stock_id ORDER BY date) AS prev_close,
    close / NULLIF(LAG(close) OVER (PARTITION BY stock_id ORDER BY date), 0) - 1
        AS daily_return_pct
FROM daily_prices;

CREATE OR REPLACE VIEW rolling_volatility_view AS
SELECT
    stock_id,
    date,
    close,
    daily_return_pct,
    STDDEV(daily_return_pct) OVER (
        PARTITION BY stock_id
        ORDER BY date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS rolling_20d_volatility
FROM daily_returns_view;
