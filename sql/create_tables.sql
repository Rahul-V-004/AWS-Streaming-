-- ============================================
-- AWS Streaming Pipeline — RDS PostgreSQL Schema
-- ============================================
-- Run this against your RDS PostgreSQL instance to create
-- the target tables for the pipeline.
--
-- Usage:
--   psql -h <rds-endpoint> -U admin -d ecommerce_analytics -f create_tables.sql
-- ============================================

-- Enriched orders table (main fact table)
CREATE TABLE IF NOT EXISTS enriched_orders (
    order_id            VARCHAR(64) PRIMARY KEY,
    customer_id         VARCHAR(32) NOT NULL,
    customer_name       VARCHAR(128),
    customer_city       VARCHAR(128),
    order_date          DATE NOT NULL,
    order_timestamp     TIMESTAMP,
    product_id          INTEGER,
    product_name        VARCHAR(256),
    brand               VARCHAR(128),
    category            VARCHAR(64),
    unit_price          NUMERIC(12, 2),
    quantity            INTEGER,
    subtotal            NUMERIC(12, 2),
    discount_pct        INTEGER DEFAULT 0,
    discount_amount     NUMERIC(12, 2) DEFAULT 0,
    total_amount        NUMERIC(12, 2),
    payment_method      VARCHAR(32),
    region              VARCHAR(32),
    order_status        VARCHAR(16),
    product_rating      NUMERIC(3, 1),
    order_year          INTEGER,
    order_month         INTEGER,
    order_day           INTEGER,
    day_name            VARCHAR(16),
    price_tier          VARCHAR(16),
    order_size          VARCHAR(8),
    has_discount        BOOLEAN DEFAULT FALSE,
    profit_estimate     NUMERIC(12, 2),
    rating_tier         VARCHAR(16),
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily sales summary (aggregated by date, category, region)
CREATE TABLE IF NOT EXISTS daily_summary (
    id                  SERIAL PRIMARY KEY,
    order_date          DATE NOT NULL,
    category            VARCHAR(64),
    region              VARCHAR(32),
    total_orders        INTEGER,
    total_revenue       NUMERIC(14, 2),
    avg_order_value     NUMERIC(12, 2),
    total_units_sold    INTEGER,
    total_discounts     NUMERIC(12, 2),
    estimated_profit    NUMERIC(14, 2),
    unique_customers    INTEGER,
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Category performance summary
CREATE TABLE IF NOT EXISTS category_summary (
    id                  SERIAL PRIMARY KEY,
    category            VARCHAR(64) NOT NULL,
    total_orders        INTEGER,
    total_revenue       NUMERIC(14, 2),
    avg_order_value     NUMERIC(12, 2),
    avg_discount_pct    NUMERIC(5, 2),
    total_units_sold    INTEGER,
    unique_customers    INTEGER,
    unique_products     INTEGER,
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product performance summary
CREATE TABLE IF NOT EXISTS product_summary (
    id                  SERIAL PRIMARY KEY,
    product_id          INTEGER,
    product_name        VARCHAR(256),
    category            VARCHAR(64),
    times_ordered       INTEGER,
    total_revenue       NUMERIC(14, 2),
    avg_selling_price   NUMERIC(12, 2),
    total_units_sold    INTEGER,
    brand               VARCHAR(128),
    api_rating          NUMERIC(3, 1),
    rating_tier         VARCHAR(16),
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_orders_date ON enriched_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_category ON enriched_orders(category);
CREATE INDEX IF NOT EXISTS idx_orders_status ON enriched_orders(order_status);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON enriched_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_summary(order_date);
CREATE INDEX IF NOT EXISTS idx_daily_category ON daily_summary(category);
CREATE INDEX IF NOT EXISTS idx_product_category ON product_summary(category);
