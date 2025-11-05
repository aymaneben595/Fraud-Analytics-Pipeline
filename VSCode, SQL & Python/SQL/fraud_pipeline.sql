-- fraud_pipeline.sql
-- SMART BUDGET / Risk & Fraud — PostgreSQL ingest, cleaning, feature engineering
-- Author: ChatGPT (adapted for Aïmane)
-- Date: 2025-11-03
-- Purpose: Create schema, load raw CSV into transactions_raw, clean, engineer features,
-- and produce transactions_clean ready for analytics and Power BI.

-- === 0. Preparations ===
-- NOTE: This script assumes the database 'cdb' ALREADY EXISTS.
-- We are running this script *inside* 'cdb'.

-- Create a dedicated schema
CREATE SCHEMA IF NOT EXISTS fraud;

-- Set the search path so we don't have to type 'fraud.' for every table
SET search_path = fraud, public;

-- Drop tables if re-running (safe for development)
DROP TABLE IF EXISTS fraud.transactions_clean CASCADE;
DROP TABLE IF EXISTS fraud.transactions_raw CASCADE;
DROP TABLE IF EXISTS fraud._staging_step1;
DROP TABLE IF EXISTS fraud._staging_step2;
DROP TABLE IF EXISTS fraud._staging_step3;
DROP TABLE IF EXISTS fraud._tmp_dedup;
DROP TABLE IF EXISTS fraud._medians;


-- === 1. Create transactions_raw table ===
-- The Kaggle dataset columns: "Time","V1".."V28","Amount","Class"
CREATE TABLE fraud.transactions_raw (
    id SERIAL PRIMARY KEY,
    time_seconds DOUBLE PRECISION, -- "Time" column in seconds (from dataset)
    v1 DOUBLE PRECISION, v2 DOUBLE PRECISION, v3 DOUBLE PRECISION, v4 DOUBLE PRECISION,
    v5 DOUBLE PRECISION, v6 DOUBLE PRECISION, v7 DOUBLE PRECISION, v8 DOUBLE PRECISION,
    v9 DOUBLE PRECISION, v10 DOUBLE PRECISION, v11 DOUBLE PRECISION, v12 DOUBLE PRECISION,
    v13 DOUBLE PRECISION, v14 DOUBLE PRECISION, v15 DOUBLE PRECISION, v16 DOUBLE PRECISION,
    v17 DOUBLE PRECISION, v18 DOUBLE PRECISION, v19 DOUBLE PRECISION, v20 DOUBLE PRECISION,
    v21 DOUBLE PRECISION, v22 DOUBLE PRECISION, v23 DOUBLE PRECISION, v24 DOUBLE PRECISION,
    v25 DOUBLE PRECISION, v26 DOUBLE PRECISION, v27 DOUBLE PRECISION, v28 DOUBLE PRECISION,
    amount DOUBLE PRECISION,
    class INTEGER, -- 0 = non-fraud, 1 = fraud
    raw_loaded_at TIMESTAMP WITH TIME ZONE DEFAULT now()  -- load timestamp
);

-- === 2. Load CSV into transactions_raw ===
-- Replace the file path with the path available to the Postgres server.
COPY fraud.transactions_raw(time_seconds, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21, v22, v23, v24, v25, v26, v27, v28, amount, class)
FROM 'C:\Users\ayman\OneDrive\Desktop\New folder (3)\VSCode, SQL & Python\CSV\creditcard.csv'
WITH (FORMAT csv, HEADER true);

-- === 3. Basic cleaning: remove exact duplicates & identify nulls ===

-- Remove exact duplicate rows (keeping the first occurrence)
CREATE TABLE fraud._tmp_dedup AS
SELECT DISTINCT ON (time_seconds, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21, v22, v23, v24, v25, v26, v27, v28, amount, class)
        *
FROM fraud.transactions_raw
ORDER BY time_seconds, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21, v22, v23, v24, v25, v26, v27, v28, amount, class, id;

-- Replace transactions_raw with deduped version
DROP TABLE fraud.transactions_raw;
ALTER TABLE fraud._tmp_dedup RENAME TO transactions_raw;

-- === 4. Impute missing values ===
-- NOTE: This dataset is clean, but this is robust code for a real-world scenario.

-- Compute medians into a temp table for readability
CREATE TEMP TABLE _medians AS
SELECT
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v1) AS v1,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v2) AS v2,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v3) AS v3,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v4) AS v4,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v5) AS v5,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v6) AS v6,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v7) AS v7,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v8) AS v8,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v9) AS v9,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v10) AS v10,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v11) AS v11,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v12) AS v12,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v13) AS v13,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v14) AS v14,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v15) AS v15,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v16) AS v16,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v17) AS v17,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v18) AS v18,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v19) AS v19,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v20) AS v20,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v21) AS v21,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v22) AS v22,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v23) AS v23,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v24) AS v24,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v25) AS v25,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v26) AS v26,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v27) AS v27,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY v28) AS v28,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY amount) AS amount_median
FROM fraud.transactions_raw;

-- Apply imputation using COALESCE and medians
DROP TABLE IF EXISTS fraud._staging_step1;
CREATE TABLE fraud._staging_step1 AS
SELECT
  id,
  time_seconds,
  COALESCE(v1, (SELECT v1 FROM _medians)) AS v1,
  COALESCE(v2, (SELECT v2 FROM _medians)) AS v2,
  COALESCE(v3, (SELECT v3 FROM _medians)) AS v3,
  COALESCE(v4, (SELECT v4 FROM _medians)) AS v4,
  COALESCE(v5, (SELECT v5 FROM _medians)) AS v5,
  COALESCE(v6, (SELECT v6 FROM _medians)) AS v6,
  COALESCE(v7, (SELECT v7 FROM _medians)) AS v7,
  COALESCE(v8, (SELECT v8 FROM _medians)) AS v8,
  COALESCE(v9, (SELECT v9 FROM _medians)) AS v9,
  COALESCE(v10, (SELECT v10 FROM _medians)) AS v10,
  COALESCE(v11, (SELECT v11 FROM _medians)) AS v11,
  COALESCE(v12, (SELECT v12 FROM _medians)) AS v12,
  COALESCE(v13, (SELECT v13 FROM _medians)) AS v13,
  COALESCE(v14, (SELECT v14 FROM _medians)) AS v14,
  COALESCE(v15, (SELECT v15 FROM _medians)) AS v15,
  COALESCE(v16, (SELECT v16 FROM _medians)) AS v16,
  COALESCE(v17, (SELECT v17 FROM _medians)) AS v17,
  COALESCE(v18, (SELECT v18 FROM _medians)) AS v18,
  COALESCE(v19, (SELECT v19 FROM _medians)) AS v19,
  COALESCE(v20, (SELECT v20 FROM _medians)) AS v20,
  COALESCE(v21, (SELECT v21 FROM _medians)) AS v21,
  COALESCE(v22, (SELECT v22 FROM _medians)) AS v22,
  COALESCE(v23, (SELECT v23 FROM _medians)) AS v23,
  COALESCE(v24, (SELECT v24 FROM _medians)) AS v24,
  COALESCE(v25, (SELECT v25 FROM _medians)) AS v25,
  COALESCE(v26, (SELECT v26 FROM _medians)) AS v26,
  COALESCE(v27, (SELECT v27 FROM _medians)) AS v27,
  COALESCE(v28, (SELECT v28 FROM _medians)) AS v28,
  COALESCE(amount, (SELECT amount_median FROM _medians)) AS amount,
  COALESCE(class, 0)::INTEGER AS class, -- missing class -> 0 (non-fraud) conservative
  raw_loaded_at
FROM fraud.transactions_raw;

-- === 5. Transformations: timestamp and synthetic card_id ===
DROP TABLE IF EXISTS fraud._staging_step2;
CREATE TABLE fraud._staging_step2 AS
SELECT
  id,
  time_seconds,
  -- Create a synthetic timestamp
  timestamp with time zone '2013-01-01 00:00:00+00' + (time_seconds || ' seconds')::interval AS transaction_ts,
  v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15,v16,v17,v18,v19,v20,v21,v22,v23,v24,v25,v26,v27,v28,
  amount,
  class,
  raw_loaded_at
FROM fraud._staging_step1;

-- === 6. Synthetic card_id (user identifier) ===
DROP TABLE IF EXISTS fraud._staging_step3;
CREATE TABLE fraud._staging_step3 AS
SELECT
  *,
  -- create card_bucket as synthetic card_id
  ( (row_number() OVER (ORDER BY md5(CAST(time_seconds as text) || '|' || CAST(amount as text)) ) - 1) % 2000 )::integer AS card_bucket
FROM fraud._staging_step2
ORDER BY id;

-- Add a human-friendly card_id text column
ALTER TABLE fraud._staging_step3 ADD COLUMN card_id TEXT;
UPDATE fraud._staging_step3
SET card_id = 'card_' || LPAD(card_bucket::text, 4, '0');

-- === 7. Feature engineering (OPTIMIZED) ===
-- We use Window Functions instead of slow correlated subqueries.
-- This is much faster as it calculates in one pass over the data.
CREATE TABLE fraud.transactions_clean AS
WITH base AS (
  SELECT
    id,
    transaction_ts,
    card_id,
    card_bucket,
    v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15,v16,v17,v18,v19,v20,v21,v22,v23,v24,v25,v26,v27,v28,
    amount,
    class,
    EXTRACT(hour FROM transaction_ts)::int AS transaction_hour,
    CASE WHEN EXTRACT(hour FROM transaction_ts)::int BETWEEN 0 AND 5 THEN TRUE ELSE FALSE END AS is_night_transaction,
    ln(amount + 1) AS transaction_amount_log
  FROM fraud._staging_step3
)
-- Now apply window functions
SELECT
    b.*,
    -- Count transactions for this card_id in the last 24 hours (including current)
    COUNT(*) OVER (
      PARTITION BY card_id
      ORDER BY transaction_ts
      RANGE BETWEEN '24 hours' PRECEDING AND CURRENT ROW
    ) AS transactions_per_user_last_24h,

    -- Get average amount for this card_id in the last 7 days
    -- We use '1 second PRECEDING' to *exclude* the current transaction from the avg.
    -- This prevents data leakage in the model.
    COALESCE(
      AVG(amount) OVER (
        PARTITION BY card_id
        ORDER BY transaction_ts
        RANGE BETWEEN '7 days' PRECEDING AND '1 second' PRECEDING
      ),
    0.0) AS avg_amount_last_7days
FROM base b;

-- Add indexes useful for analytics
CREATE INDEX ON fraud.transactions_clean (transaction_ts);
CREATE INDEX ON fraud.transactions_clean (card_id);
CREATE INDEX ON fraud.transactions_clean (class);
CREATE INDEX ON fraud.transactions_clean (transaction_hour);

-- === 8. Cleanup staging tables ===
DROP TABLE IF EXISTS fraud._staging_step1, fraud._staging_step2, fraud._staging_step3, fraud._medians;