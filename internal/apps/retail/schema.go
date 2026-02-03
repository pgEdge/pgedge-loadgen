//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

// Package retail implements the Retail Analytics application (TPC-DS based).
package retail

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema SQL for creating the retail database schema.
// Based on TPC-DS with modifications for PostgreSQL.
const createSchemaSQL = `
-- Date Dimension
CREATE TABLE IF NOT EXISTS date_dim (
    d_date_sk          INTEGER PRIMARY KEY,
    d_date_id          CHAR(16) NOT NULL,
    d_date             DATE NOT NULL,
    d_month_seq        INTEGER NOT NULL,
    d_week_seq         INTEGER NOT NULL,
    d_quarter_seq      INTEGER NOT NULL,
    d_year             INTEGER NOT NULL,
    d_dow              INTEGER NOT NULL,
    d_moy              INTEGER NOT NULL,
    d_dom              INTEGER NOT NULL,
    d_qoy              INTEGER NOT NULL,
    d_fy_year          INTEGER NOT NULL,
    d_fy_quarter_seq   INTEGER NOT NULL,
    d_fy_week_seq      INTEGER NOT NULL,
    d_day_name         VARCHAR(9) NOT NULL,
    d_quarter_name     CHAR(6) NOT NULL,
    d_holiday          CHAR(1) NOT NULL,
    d_weekend          CHAR(1) NOT NULL,
    d_following_holiday CHAR(1) NOT NULL,
    d_first_dom        INTEGER NOT NULL,
    d_last_dom         INTEGER NOT NULL,
    d_same_day_ly      INTEGER NOT NULL,
    d_same_day_lq      INTEGER NOT NULL,
    d_current_day      CHAR(1) NOT NULL,
    d_current_week     CHAR(1) NOT NULL,
    d_current_month    CHAR(1) NOT NULL,
    d_current_quarter  CHAR(1) NOT NULL,
    d_current_year     CHAR(1) NOT NULL
);

-- Time Dimension
CREATE TABLE IF NOT EXISTS time_dim (
    t_time_sk   INTEGER PRIMARY KEY,
    t_time_id   CHAR(16) NOT NULL,
    t_time      INTEGER NOT NULL,
    t_hour      INTEGER NOT NULL,
    t_minute    INTEGER NOT NULL,
    t_second    INTEGER NOT NULL,
    t_am_pm     CHAR(2) NOT NULL,
    t_shift     CHAR(20) NOT NULL,
    t_sub_shift CHAR(20) NOT NULL,
    t_meal_time CHAR(20)
);

-- Item Dimension
CREATE TABLE IF NOT EXISTS item (
    i_item_sk        INTEGER PRIMARY KEY,
    i_item_id        CHAR(16) NOT NULL,
    i_rec_start_date DATE,
    i_rec_end_date   DATE,
    i_item_desc      VARCHAR(200),
    i_current_price  NUMERIC(7,2),
    i_wholesale_cost NUMERIC(7,2),
    i_brand_id       INTEGER,
    i_brand          CHAR(50),
    i_class_id       INTEGER,
    i_class          CHAR(50),
    i_category_id    INTEGER,
    i_category       CHAR(50),
    i_manufact_id    INTEGER,
    i_manufact       CHAR(50),
    i_size           CHAR(20),
    i_formulation    CHAR(20),
    i_color          CHAR(20),
    i_units          CHAR(10),
    i_container      CHAR(10),
    i_manager_id     INTEGER,
    i_product_name   CHAR(50)
);

-- Customer Dimension
CREATE TABLE IF NOT EXISTS customer (
    c_customer_sk          INTEGER PRIMARY KEY,
    c_customer_id          CHAR(16) NOT NULL,
    c_current_cdemo_sk     INTEGER,
    c_current_hdemo_sk     INTEGER,
    c_current_addr_sk      INTEGER,
    c_first_shipto_date_sk INTEGER,
    c_first_sales_date_sk  INTEGER,
    c_salutation           CHAR(10),
    c_first_name           CHAR(20),
    c_last_name            CHAR(30),
    c_preferred_cust_flag  CHAR(1),
    c_birth_day            INTEGER,
    c_birth_month          INTEGER,
    c_birth_year           INTEGER,
    c_birth_country        VARCHAR(20),
    c_login                CHAR(13),
    c_email_address        CHAR(50),
    c_last_review_date_sk  INTEGER
);

-- Customer Demographics
CREATE TABLE IF NOT EXISTS customer_demographics (
    cd_demo_sk            INTEGER PRIMARY KEY,
    cd_gender             CHAR(1),
    cd_marital_status     CHAR(1),
    cd_education_status   CHAR(20),
    cd_purchase_estimate  INTEGER,
    cd_credit_rating      CHAR(10),
    cd_dep_count          INTEGER,
    cd_dep_employed_count INTEGER,
    cd_dep_college_count  INTEGER
);

-- Household Demographics
CREATE TABLE IF NOT EXISTS household_demographics (
    hd_demo_sk        INTEGER PRIMARY KEY,
    hd_income_band_sk INTEGER,
    hd_buy_potential  CHAR(15),
    hd_dep_count      INTEGER,
    hd_vehicle_count  INTEGER
);

-- Customer Address
CREATE TABLE IF NOT EXISTS customer_address (
    ca_address_sk    INTEGER PRIMARY KEY,
    ca_address_id    CHAR(16) NOT NULL,
    ca_street_number CHAR(10),
    ca_street_name   VARCHAR(60),
    ca_street_type   CHAR(15),
    ca_suite_number  CHAR(10),
    ca_city          VARCHAR(60),
    ca_county        VARCHAR(30),
    ca_state         CHAR(2),
    ca_zip           CHAR(10),
    ca_country       VARCHAR(20),
    ca_gmt_offset    NUMERIC(5,2),
    ca_location_type CHAR(20)
);

-- Store Dimension
CREATE TABLE IF NOT EXISTS store (
    s_store_sk         INTEGER PRIMARY KEY,
    s_store_id         CHAR(16) NOT NULL,
    s_rec_start_date   DATE,
    s_rec_end_date     DATE,
    s_closed_date_sk   INTEGER,
    s_store_name       VARCHAR(50),
    s_number_employees INTEGER,
    s_floor_space      INTEGER,
    s_hours            CHAR(20),
    s_manager          VARCHAR(40),
    s_market_id        INTEGER,
    s_geography_class  VARCHAR(100),
    s_market_desc      VARCHAR(100),
    s_market_manager   VARCHAR(40),
    s_division_id      INTEGER,
    s_division_name    VARCHAR(50),
    s_company_id       INTEGER,
    s_company_name     VARCHAR(50),
    s_street_number    VARCHAR(10),
    s_street_name      VARCHAR(60),
    s_street_type      CHAR(15),
    s_suite_number     CHAR(10),
    s_city             VARCHAR(60),
    s_county           VARCHAR(30),
    s_state            CHAR(2),
    s_zip              CHAR(10),
    s_country          VARCHAR(20),
    s_gmt_offset       NUMERIC(5,2),
    s_tax_percentage   NUMERIC(5,2)
);

-- Warehouse Dimension
CREATE TABLE IF NOT EXISTS warehouse (
    w_warehouse_sk    INTEGER PRIMARY KEY,
    w_warehouse_id    CHAR(16) NOT NULL,
    w_warehouse_name  VARCHAR(20),
    w_warehouse_sq_ft INTEGER,
    w_street_number   CHAR(10),
    w_street_name     VARCHAR(60),
    w_street_type     CHAR(15),
    w_suite_number    CHAR(10),
    w_city            VARCHAR(60),
    w_county          VARCHAR(30),
    w_state           CHAR(2),
    w_zip             CHAR(10),
    w_country         VARCHAR(20),
    w_gmt_offset      NUMERIC(5,2)
);

-- Promotion Dimension
CREATE TABLE IF NOT EXISTS promotion (
    p_promo_sk        INTEGER PRIMARY KEY,
    p_promo_id        CHAR(16) NOT NULL,
    p_start_date_sk   INTEGER,
    p_end_date_sk     INTEGER,
    p_item_sk         INTEGER,
    p_cost            NUMERIC(15,2),
    p_response_target INTEGER,
    p_promo_name      CHAR(50),
    p_channel_dmail   CHAR(1),
    p_channel_email   CHAR(1),
    p_channel_catalog CHAR(1),
    p_channel_tv      CHAR(1),
    p_channel_radio   CHAR(1),
    p_channel_press   CHAR(1),
    p_channel_event   CHAR(1),
    p_channel_demo    CHAR(1),
    p_channel_details VARCHAR(100),
    p_purpose         CHAR(15),
    p_discount_active CHAR(1)
);

-- Store Sales Fact
CREATE TABLE IF NOT EXISTS store_sales (
    ss_sold_date_sk       INTEGER,
    ss_sold_time_sk       INTEGER,
    ss_item_sk            INTEGER NOT NULL,
    ss_customer_sk        INTEGER,
    ss_cdemo_sk           INTEGER,
    ss_hdemo_sk           INTEGER,
    ss_addr_sk            INTEGER,
    ss_store_sk           INTEGER,
    ss_promo_sk           INTEGER,
    ss_ticket_number      BIGINT NOT NULL,
    ss_quantity           INTEGER,
    ss_wholesale_cost     NUMERIC(7,2),
    ss_list_price         NUMERIC(7,2),
    ss_sales_price        NUMERIC(7,2),
    ss_ext_discount_amt   NUMERIC(7,2),
    ss_ext_sales_price    NUMERIC(7,2),
    ss_ext_wholesale_cost NUMERIC(7,2),
    ss_ext_list_price     NUMERIC(7,2),
    ss_ext_tax            NUMERIC(7,2),
    ss_coupon_amt         NUMERIC(7,2),
    ss_net_paid           NUMERIC(7,2),
    ss_net_paid_inc_tax   NUMERIC(7,2),
    ss_net_profit         NUMERIC(7,2),
    PRIMARY KEY (ss_item_sk, ss_ticket_number)
);

-- Web Sales Fact
CREATE TABLE IF NOT EXISTS web_sales (
    ws_sold_date_sk          INTEGER,
    ws_sold_time_sk          INTEGER,
    ws_ship_date_sk          INTEGER,
    ws_item_sk               INTEGER NOT NULL,
    ws_bill_customer_sk      INTEGER,
    ws_bill_cdemo_sk         INTEGER,
    ws_bill_hdemo_sk         INTEGER,
    ws_bill_addr_sk          INTEGER,
    ws_ship_customer_sk      INTEGER,
    ws_ship_cdemo_sk         INTEGER,
    ws_ship_hdemo_sk         INTEGER,
    ws_ship_addr_sk          INTEGER,
    ws_web_page_sk           INTEGER,
    ws_web_site_sk           INTEGER,
    ws_ship_mode_sk          INTEGER,
    ws_warehouse_sk          INTEGER,
    ws_promo_sk              INTEGER,
    ws_order_number          BIGINT NOT NULL,
    ws_quantity              INTEGER,
    ws_wholesale_cost        NUMERIC(7,2),
    ws_list_price            NUMERIC(7,2),
    ws_sales_price           NUMERIC(7,2),
    ws_ext_discount_amt      NUMERIC(7,2),
    ws_ext_sales_price       NUMERIC(7,2),
    ws_ext_wholesale_cost    NUMERIC(7,2),
    ws_ext_list_price        NUMERIC(7,2),
    ws_ext_tax               NUMERIC(7,2),
    ws_coupon_amt            NUMERIC(7,2),
    ws_ext_ship_cost         NUMERIC(7,2),
    ws_net_paid              NUMERIC(7,2),
    ws_net_paid_inc_tax      NUMERIC(7,2),
    ws_net_paid_inc_ship     NUMERIC(7,2),
    ws_net_paid_inc_ship_tax NUMERIC(7,2),
    ws_net_profit            NUMERIC(7,2),
    PRIMARY KEY (ws_item_sk, ws_order_number)
);

-- Catalog Sales Fact
CREATE TABLE IF NOT EXISTS catalog_sales (
    cs_sold_date_sk          INTEGER,
    cs_sold_time_sk          INTEGER,
    cs_ship_date_sk          INTEGER,
    cs_bill_customer_sk      INTEGER,
    cs_bill_cdemo_sk         INTEGER,
    cs_bill_hdemo_sk         INTEGER,
    cs_bill_addr_sk          INTEGER,
    cs_ship_customer_sk      INTEGER,
    cs_ship_cdemo_sk         INTEGER,
    cs_ship_hdemo_sk         INTEGER,
    cs_ship_addr_sk          INTEGER,
    cs_call_center_sk        INTEGER,
    cs_catalog_page_sk       INTEGER,
    cs_ship_mode_sk          INTEGER,
    cs_warehouse_sk          INTEGER,
    cs_item_sk               INTEGER NOT NULL,
    cs_promo_sk              INTEGER,
    cs_order_number          BIGINT NOT NULL,
    cs_quantity              INTEGER,
    cs_wholesale_cost        NUMERIC(7,2),
    cs_list_price            NUMERIC(7,2),
    cs_sales_price           NUMERIC(7,2),
    cs_ext_discount_amt      NUMERIC(7,2),
    cs_ext_sales_price       NUMERIC(7,2),
    cs_ext_wholesale_cost    NUMERIC(7,2),
    cs_ext_list_price        NUMERIC(7,2),
    cs_ext_tax               NUMERIC(7,2),
    cs_coupon_amt            NUMERIC(7,2),
    cs_ext_ship_cost         NUMERIC(7,2),
    cs_net_paid              NUMERIC(7,2),
    cs_net_paid_inc_tax      NUMERIC(7,2),
    cs_net_paid_inc_ship     NUMERIC(7,2),
    cs_net_paid_inc_ship_tax NUMERIC(7,2),
    cs_net_profit            NUMERIC(7,2),
    PRIMARY KEY (cs_item_sk, cs_order_number)
);

-- Inventory Fact
CREATE TABLE IF NOT EXISTS inventory (
    inv_date_sk      INTEGER NOT NULL,
    inv_item_sk      INTEGER NOT NULL,
    inv_warehouse_sk INTEGER NOT NULL,
    inv_quantity_on_hand INTEGER,
    PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk)
);

-- Create indexes for analytical queries
CREATE INDEX IF NOT EXISTS idx_store_sales_date ON store_sales(ss_sold_date_sk);
CREATE INDEX IF NOT EXISTS idx_store_sales_customer ON store_sales(ss_customer_sk);
CREATE INDEX IF NOT EXISTS idx_store_sales_store ON store_sales(ss_store_sk);
CREATE INDEX IF NOT EXISTS idx_store_sales_item ON store_sales(ss_item_sk);

CREATE INDEX IF NOT EXISTS idx_web_sales_date ON web_sales(ws_sold_date_sk);
CREATE INDEX IF NOT EXISTS idx_web_sales_customer ON web_sales(ws_bill_customer_sk);
CREATE INDEX IF NOT EXISTS idx_web_sales_item ON web_sales(ws_item_sk);

CREATE INDEX IF NOT EXISTS idx_catalog_sales_date ON catalog_sales(cs_sold_date_sk);
CREATE INDEX IF NOT EXISTS idx_catalog_sales_customer ON catalog_sales(cs_bill_customer_sk);
CREATE INDEX IF NOT EXISTS idx_catalog_sales_item ON catalog_sales(cs_item_sk);

CREATE INDEX IF NOT EXISTS idx_inventory_date ON inventory(inv_date_sk);
CREATE INDEX IF NOT EXISTS idx_inventory_item ON inventory(inv_item_sk);

CREATE INDEX IF NOT EXISTS idx_customer_address_state ON customer_address(ca_state);
CREATE INDEX IF NOT EXISTS idx_item_category ON item(i_category_id);
CREATE INDEX IF NOT EXISTS idx_item_brand ON item(i_brand_id);
CREATE INDEX IF NOT EXISTS idx_date_dim_year ON date_dim(d_year);
`

// Drop schema SQL
const dropSchemaSQL = `
DROP TABLE IF EXISTS inventory CASCADE;
DROP TABLE IF EXISTS catalog_sales CASCADE;
DROP TABLE IF EXISTS web_sales CASCADE;
DROP TABLE IF EXISTS store_sales CASCADE;
DROP TABLE IF EXISTS promotion CASCADE;
DROP TABLE IF EXISTS warehouse CASCADE;
DROP TABLE IF EXISTS store CASCADE;
DROP TABLE IF EXISTS customer_address CASCADE;
DROP TABLE IF EXISTS household_demographics CASCADE;
DROP TABLE IF EXISTS customer_demographics CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS item CASCADE;
DROP TABLE IF EXISTS time_dim CASCADE;
DROP TABLE IF EXISTS date_dim CASCADE;
`

// CreateSchema creates the retail database schema.
func CreateSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, createSchemaSQL)
	return err
}

// DropSchema drops the retail database schema.
func DropSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, dropSchemaSQL)
	return err
}
