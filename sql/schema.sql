-- ===========================================================================
-- schema.sql  (PostgreSQL 12+)
-- Tables for the Spark pipeline:
--   1. Clean data tables     customers, products, orders   (from Spark Parquet)
--   2. Analytics tables      analytics_*                   (from spark_analytics.py)
--   3. Pipeline metadata     etl_audit, cleansing_stats
-- Every statement uses IF NOT EXISTS, so the file can run on every pipeline run.
-- The load step empties and reloads the data tables on each run (full refresh).
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1. Clean data
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id        INTEGER PRIMARY KEY,
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    email              VARCHAR(255) UNIQUE,          -- NULL allowed (missing e-mails are flagged, not invented)
    phone              VARCHAR(30),
    city               VARCHAR(100),
    registration_date  DATE,
    customer_type      VARCHAR(20) CHECK (customer_type IN ('Regular', 'Premium', 'VIP')),
    loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id      INTEGER PRIMARY KEY,
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    brand           VARCHAR(100),
    unit_price      NUMERIC(10,2) CHECK (unit_price >= 0),
    stock_quantity  INTEGER,
    is_active       BOOLEAN,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id         INTEGER PRIMARY KEY,
    customer_id      INTEGER NOT NULL REFERENCES customers(customer_id),
    product_id       INTEGER NOT NULL,
    quantity         INTEGER CHECK (quantity > 0),
    order_date       TIMESTAMP NOT NULL,
    order_status     VARCHAR(20) CHECK (order_status IN ('Pending', 'Shipped', 'Delivered')),
    payment_method   VARCHAR(30),
    total_amount     NUMERIC(10,2) NOT NULL CHECK (total_amount >= 0),
    discount_amount  NUMERIC(10,2) CHECK (discount_amount >= 0),
    shipping_cost    NUMERIC(10,2),
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CHECK (discount_amount <= total_amount)
);

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_product  ON orders(product_id);
CREATE INDEX IF NOT EXISTS idx_orders_date     ON orders(order_date);

-- ---------------------------------------------------------------------------
-- 2. Analytics (revenue = total_amount - discount_amount)
-- ---------------------------------------------------------------------------
-- Customer analytics
CREATE TABLE IF NOT EXISTS analytics_customer_metrics (
    customer_id       INTEGER PRIMARY KEY,
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    city              VARCHAR(100),
    customer_type     VARCHAR(20),
    lifetime_value    NUMERIC(14,2),
    order_count       INTEGER,
    avg_order_value   NUMERIC(14,2),
    first_order_date  TIMESTAMP,
    last_order_date   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics_customer_type_distribution (
    customer_type     VARCHAR(20) PRIMARY KEY,
    customer_count    INTEGER,
    pct_of_customers  NUMERIC(7,2)
);

CREATE TABLE IF NOT EXISTS analytics_top_customers (
    rank             INTEGER PRIMARY KEY,
    customer_id      INTEGER,
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    city             VARCHAR(100),
    customer_type    VARCHAR(20),
    lifetime_value   NUMERIC(14,2),
    order_count      INTEGER,
    avg_order_value  NUMERIC(14,2)
);

-- Product analytics
CREATE TABLE IF NOT EXISTS analytics_product_metrics (
    product_id         INTEGER PRIMARY KEY,
    product_name       VARCHAR(255),
    category           VARCHAR(100),
    brand              VARCHAR(100),
    unit_price         NUMERIC(10,2),
    revenue            NUMERIC(14,2),
    times_ordered      INTEGER,
    units_sold         INTEGER,
    avg_selling_price  NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS analytics_top_products (
    product_id         INTEGER PRIMARY KEY,
    product_name       VARCHAR(255),
    category           VARCHAR(100),
    brand              VARCHAR(100),
    unit_price         NUMERIC(10,2),
    revenue            NUMERIC(14,2),
    times_ordered      INTEGER,
    units_sold         INTEGER,
    avg_selling_price  NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS analytics_category_performance (
    category         VARCHAR(100) PRIMARY KEY,
    revenue          NUMERIC(16,2),
    order_count      INTEGER,
    units_sold       INTEGER,
    avg_order_value  NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS analytics_inventory_status (
    status         VARCHAR(20) PRIMARY KEY,
    product_count  INTEGER
);

-- Order analytics
CREATE TABLE IF NOT EXISTS analytics_sales_monthly (
    month        CHAR(7) PRIMARY KEY,          -- e.g. 2025-03
    revenue      NUMERIC(16,2),
    order_count  INTEGER
);

CREATE TABLE IF NOT EXISTS analytics_sales_daily (
    order_day    DATE PRIMARY KEY,
    revenue      NUMERIC(16,2),
    order_count  INTEGER
);

CREATE TABLE IF NOT EXISTS analytics_order_status_breakdown (
    order_status   VARCHAR(20) PRIMARY KEY,
    order_count    INTEGER,
    pct_of_orders  NUMERIC(7,2)
);

CREATE TABLE IF NOT EXISTS analytics_payment_method_distribution (
    payment_method  VARCHAR(30) PRIMARY KEY,
    revenue         NUMERIC(16,2),
    order_count     INTEGER
);

CREATE TABLE IF NOT EXISTS analytics_order_kpis (
    total_orders              INTEGER,
    gross_sales               NUMERIC(16,2),
    total_discounts           NUMERIC(16,2),
    net_revenue               NUMERIC(16,2),
    total_shipping            NUMERIC(16,2),
    avg_order_value           NUMERIC(14,2),
    avg_discount_rate_pct     NUMERIC(7,2),
    active_customers          INTEGER,
    repeat_customer_rate_pct  NUMERIC(7,2)
);

-- Cross-entity analytics
CREATE TABLE IF NOT EXISTS analytics_revenue_by_customer_type (
    customer_type          VARCHAR(20) PRIMARY KEY,
    revenue                NUMERIC(16,2),
    order_count            INTEGER,
    customers_with_orders  INTEGER,
    revenue_per_customer   NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS analytics_top_categories_by_customer_type (
    customer_type  VARCHAR(20),
    category       VARCHAR(100),
    order_count    INTEGER,
    revenue        NUMERIC(16,2),
    rank_in_type   INTEGER,
    PRIMARY KEY (customer_type, category)
);

CREATE TABLE IF NOT EXISTS analytics_city_revenue (
    city         VARCHAR(100) PRIMARY KEY,
    revenue      NUMERIC(16,2),
    order_count  INTEGER,
    customers    INTEGER
);

-- ---------------------------------------------------------------------------
-- 3. Pipeline metadata
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_audit (
    audit_id           SERIAL PRIMARY KEY,
    run_id             VARCHAR(50),
    dag_name           VARCHAR(100),
    start_time         TIMESTAMP,
    end_time           TIMESTAMP,
    status             VARCHAR(20),          -- SUCCESS / FAILED
    records_processed  INTEGER,
    error_message      TEXT
);

-- Before/after counts from the Spark cleansing jobs (one row per reason)
CREATE TABLE IF NOT EXISTS cleansing_stats (
    run_id   VARCHAR(50),
    entity   VARCHAR(20),
    type     VARCHAR(20),                    -- dropped / fixed / total
    detail   VARCHAR(100),
    count    INTEGER,
    PRIMARY KEY (run_id, entity, type, detail)
);


-- ===========================================================================
-- Useful queries (for testing and the demo; not run by the pipeline)
-- ===========================================================================
-- Row counts:
--   SELECT 'customers', COUNT(*) FROM customers UNION ALL
--   SELECT 'products',  COUNT(*) FROM products  UNION ALL
--   SELECT 'orders',    COUNT(*) FROM orders;
-- Before/after cleansing:
--   SELECT entity, detail, count FROM cleansing_stats WHERE type = 'total' ORDER BY entity, detail;
-- Monthly sales trend:
--   SELECT * FROM analytics_sales_monthly ORDER BY month;
-- Top 10 customers:
--   SELECT rank, first_name, last_name, lifetime_value FROM analytics_top_customers ORDER BY rank;
-- Pipeline history:
--   SELECT * FROM etl_audit ORDER BY audit_id DESC LIMIT 5;
