//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Portions copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

// Package brokerage implements the Brokerage Firm application (TPC-E based).
package brokerage

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema SQL for creating the brokerage database schema.
// Based on TPC-E with modifications for PostgreSQL.
const createSchemaSQL = `
-- Exchange: Stock exchanges
CREATE TABLE IF NOT EXISTS exchange (
    ex_id       CHAR(6) PRIMARY KEY,
    ex_name     VARCHAR(100) NOT NULL,
    ex_num_symb INTEGER NOT NULL,
    ex_open     INTEGER NOT NULL,
    ex_close    INTEGER NOT NULL,
    ex_desc     VARCHAR(150),
    ex_ad_id    INTEGER
);

-- Status Type: Trade and order status codes
CREATE TABLE IF NOT EXISTS status_type (
    st_id   CHAR(4) PRIMARY KEY,
    st_name VARCHAR(10) NOT NULL
);

-- Trade Type: Types of trades
CREATE TABLE IF NOT EXISTS trade_type (
    tt_id      CHAR(3) PRIMARY KEY,
    tt_name    VARCHAR(12) NOT NULL,
    tt_is_sell BOOLEAN NOT NULL,
    tt_is_mrkt BOOLEAN NOT NULL
);

-- Sector: Industry sectors
CREATE TABLE IF NOT EXISTS sector (
    sc_id   CHAR(2) PRIMARY KEY,
    sc_name VARCHAR(30) NOT NULL
);

-- Industry: Industries within sectors
CREATE TABLE IF NOT EXISTS industry (
    in_id    CHAR(2) PRIMARY KEY,
    in_name  VARCHAR(50) NOT NULL,
    in_sc_id CHAR(2) NOT NULL REFERENCES sector(sc_id)
);

-- Company: Listed companies
CREATE TABLE IF NOT EXISTS company (
    co_id       INTEGER PRIMARY KEY,
    co_st_id    CHAR(4) NOT NULL REFERENCES status_type(st_id),
    co_name     VARCHAR(60) NOT NULL,
    co_in_id    CHAR(2) NOT NULL REFERENCES industry(in_id),
    co_sp_rate  CHAR(4) NOT NULL,
    co_ceo      VARCHAR(100) NOT NULL,
    co_desc     VARCHAR(150),
    co_open_date DATE NOT NULL,
    co_ad_id    INTEGER
);

-- Security: Tradeable securities
CREATE TABLE IF NOT EXISTS security (
    s_symb      CHAR(15) PRIMARY KEY,
    s_issue     CHAR(6) NOT NULL,
    s_st_id     CHAR(4) NOT NULL REFERENCES status_type(st_id),
    s_name      VARCHAR(70) NOT NULL,
    s_ex_id     CHAR(6) NOT NULL REFERENCES exchange(ex_id),
    s_co_id     INTEGER NOT NULL REFERENCES company(co_id),
    s_num_out   BIGINT NOT NULL,
    s_start_date DATE NOT NULL,
    s_exch_date DATE NOT NULL,
    s_pe        NUMERIC(10,2) NOT NULL,
    s_52wk_high NUMERIC(8,2) NOT NULL,
    s_52wk_low  NUMERIC(8,2) NOT NULL,
    s_dividend  NUMERIC(10,2) NOT NULL,
    s_yield     NUMERIC(5,2) NOT NULL
);

-- Customer: Customer accounts
CREATE TABLE IF NOT EXISTS customer (
    c_id      INTEGER PRIMARY KEY,
    c_tax_id  VARCHAR(20) NOT NULL,
    c_st_id   CHAR(4) NOT NULL REFERENCES status_type(st_id),
    c_l_name  VARCHAR(30) NOT NULL,
    c_f_name  VARCHAR(30) NOT NULL,
    c_m_name  CHAR(1),
    c_gndr    CHAR(1),
    c_tier    SMALLINT NOT NULL,
    c_dob     DATE NOT NULL,
    c_ad_id   INTEGER,
    c_ctry_1  CHAR(3),
    c_area_1  CHAR(3),
    c_local_1 CHAR(10),
    c_ext_1   CHAR(5),
    c_ctry_2  CHAR(3),
    c_area_2  CHAR(3),
    c_local_2 CHAR(10),
    c_ext_2   CHAR(5),
    c_ctry_3  CHAR(3),
    c_area_3  CHAR(3),
    c_local_3 CHAR(10),
    c_ext_3   CHAR(5),
    c_email_1 VARCHAR(50),
    c_email_2 VARCHAR(50)
);

-- Broker: Registered brokers
CREATE TABLE IF NOT EXISTS broker (
    b_id      INTEGER PRIMARY KEY,
    b_st_id   CHAR(4) NOT NULL REFERENCES status_type(st_id),
    b_name    VARCHAR(100) NOT NULL,
    b_num_trades INTEGER NOT NULL DEFAULT 0,
    b_comm_total NUMERIC(12,2) NOT NULL DEFAULT 0
);

-- Customer Account: Trading accounts
CREATE TABLE IF NOT EXISTS customer_account (
    ca_id      INTEGER PRIMARY KEY,
    ca_b_id    INTEGER NOT NULL REFERENCES broker(b_id),
    ca_c_id    INTEGER NOT NULL REFERENCES customer(c_id),
    ca_name    VARCHAR(50),
    ca_tax_st  SMALLINT NOT NULL,
    ca_bal     NUMERIC(12,2) NOT NULL DEFAULT 0
);

-- Holding: Current stock holdings
CREATE TABLE IF NOT EXISTS holding (
    h_t_id    BIGINT NOT NULL,
    h_ca_id   INTEGER NOT NULL REFERENCES customer_account(ca_id),
    h_s_symb  CHAR(15) NOT NULL REFERENCES security(s_symb),
    h_dts     TIMESTAMP NOT NULL,
    h_price   NUMERIC(8,2) NOT NULL,
    h_qty     INTEGER NOT NULL,
    PRIMARY KEY (h_t_id, h_ca_id, h_s_symb)
);

-- Holding Summary: Aggregated holdings per account/symbol
CREATE TABLE IF NOT EXISTS holding_summary (
    hs_ca_id  INTEGER NOT NULL REFERENCES customer_account(ca_id),
    hs_s_symb CHAR(15) NOT NULL REFERENCES security(s_symb),
    hs_qty    INTEGER NOT NULL,
    PRIMARY KEY (hs_ca_id, hs_s_symb)
);

-- Watch List: Customer watch lists
CREATE TABLE IF NOT EXISTS watch_list (
    wl_id   INTEGER PRIMARY KEY,
    wl_c_id INTEGER NOT NULL REFERENCES customer(c_id)
);

-- Watch Item: Items on watch lists
CREATE TABLE IF NOT EXISTS watch_item (
    wi_wl_id  INTEGER NOT NULL REFERENCES watch_list(wl_id),
    wi_s_symb CHAR(15) NOT NULL REFERENCES security(s_symb),
    PRIMARY KEY (wi_wl_id, wi_s_symb)
);

-- Trade: Trade transactions
CREATE TABLE IF NOT EXISTS trade (
    t_id        BIGINT PRIMARY KEY,
    t_dts       TIMESTAMP NOT NULL,
    t_st_id     CHAR(4) NOT NULL REFERENCES status_type(st_id),
    t_tt_id     CHAR(3) NOT NULL REFERENCES trade_type(tt_id),
    t_is_cash   BOOLEAN NOT NULL,
    t_s_symb    CHAR(15) NOT NULL REFERENCES security(s_symb),
    t_qty       INTEGER NOT NULL,
    t_bid_price NUMERIC(8,2) NOT NULL,
    t_ca_id     INTEGER NOT NULL REFERENCES customer_account(ca_id),
    t_exec_name VARCHAR(64) NOT NULL,
    t_trade_price NUMERIC(8,2),
    t_chrg      NUMERIC(10,2) NOT NULL DEFAULT 0,
    t_comm      NUMERIC(10,2) NOT NULL DEFAULT 0,
    t_tax       NUMERIC(10,2) NOT NULL DEFAULT 0,
    t_lifo      BOOLEAN NOT NULL
);

-- Trade History: Trade status history
CREATE TABLE IF NOT EXISTS trade_history (
    th_t_id  BIGINT NOT NULL REFERENCES trade(t_id),
    th_dts   TIMESTAMP NOT NULL,
    th_st_id CHAR(4) NOT NULL REFERENCES status_type(st_id),
    PRIMARY KEY (th_t_id, th_st_id)
);

-- Settlement: Trade settlements
CREATE TABLE IF NOT EXISTS settlement (
    se_t_id        BIGINT PRIMARY KEY REFERENCES trade(t_id),
    se_cash_type   VARCHAR(40) NOT NULL,
    se_cash_due_date DATE NOT NULL,
    se_amt         NUMERIC(10,2) NOT NULL
);

-- Cash Transaction: Cash movements
CREATE TABLE IF NOT EXISTS cash_transaction (
    ct_t_id  BIGINT PRIMARY KEY REFERENCES trade(t_id),
    ct_dts   TIMESTAMP NOT NULL,
    ct_amt   NUMERIC(10,2) NOT NULL,
    ct_name  VARCHAR(100)
);

-- Last Trade: Most recent trade info per security
CREATE TABLE IF NOT EXISTS last_trade (
    lt_s_symb    CHAR(15) PRIMARY KEY REFERENCES security(s_symb),
    lt_dts       TIMESTAMP NOT NULL,
    lt_price     NUMERIC(8,2) NOT NULL,
    lt_open_price NUMERIC(8,2) NOT NULL,
    lt_vol       BIGINT NOT NULL
);

-- Commission Rate: Broker commission rates
CREATE TABLE IF NOT EXISTS commission_rate (
    cr_c_tier   SMALLINT NOT NULL,
    cr_tt_id    CHAR(3) NOT NULL REFERENCES trade_type(tt_id),
    cr_ex_id    CHAR(6) NOT NULL REFERENCES exchange(ex_id),
    cr_from_qty INTEGER NOT NULL,
    cr_to_qty   INTEGER NOT NULL,
    cr_rate     NUMERIC(5,2) NOT NULL,
    PRIMARY KEY (cr_c_tier, cr_tt_id, cr_ex_id, cr_from_qty)
);

-- Daily Market: Daily market data
CREATE TABLE IF NOT EXISTS daily_market (
    dm_date    DATE NOT NULL,
    dm_s_symb  CHAR(15) NOT NULL REFERENCES security(s_symb),
    dm_close   NUMERIC(8,2) NOT NULL,
    dm_high    NUMERIC(8,2) NOT NULL,
    dm_low     NUMERIC(8,2) NOT NULL,
    dm_vol     BIGINT NOT NULL,
    PRIMARY KEY (dm_date, dm_s_symb)
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_trade_ca_id ON trade(t_ca_id);
CREATE INDEX IF NOT EXISTS idx_trade_s_symb ON trade(t_s_symb);
CREATE INDEX IF NOT EXISTS idx_trade_dts ON trade(t_dts);
CREATE INDEX IF NOT EXISTS idx_trade_st_id ON trade(t_st_id);
CREATE INDEX IF NOT EXISTS idx_holding_ca_id ON holding(h_ca_id);
CREATE INDEX IF NOT EXISTS idx_holding_s_symb ON holding(h_s_symb);
CREATE INDEX IF NOT EXISTS idx_customer_account_c_id ON customer_account(ca_c_id);
CREATE INDEX IF NOT EXISTS idx_customer_account_b_id ON customer_account(ca_b_id);
CREATE INDEX IF NOT EXISTS idx_security_co_id ON security(s_co_id);
CREATE INDEX IF NOT EXISTS idx_company_in_id ON company(co_in_id);
CREATE INDEX IF NOT EXISTS idx_watch_list_c_id ON watch_list(wl_c_id);
`

// Drop schema SQL
const dropSchemaSQL = `
DROP TABLE IF EXISTS daily_market CASCADE;
DROP TABLE IF EXISTS commission_rate CASCADE;
DROP TABLE IF EXISTS last_trade CASCADE;
DROP TABLE IF EXISTS cash_transaction CASCADE;
DROP TABLE IF EXISTS settlement CASCADE;
DROP TABLE IF EXISTS trade_history CASCADE;
DROP TABLE IF EXISTS trade CASCADE;
DROP TABLE IF EXISTS watch_item CASCADE;
DROP TABLE IF EXISTS watch_list CASCADE;
DROP TABLE IF EXISTS holding_summary CASCADE;
DROP TABLE IF EXISTS holding CASCADE;
DROP TABLE IF EXISTS customer_account CASCADE;
DROP TABLE IF EXISTS broker CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS security CASCADE;
DROP TABLE IF EXISTS company CASCADE;
DROP TABLE IF EXISTS industry CASCADE;
DROP TABLE IF EXISTS sector CASCADE;
DROP TABLE IF EXISTS trade_type CASCADE;
DROP TABLE IF EXISTS status_type CASCADE;
DROP TABLE IF EXISTS exchange CASCADE;
`

// CreateSchema creates the brokerage database schema.
func CreateSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, createSchemaSQL)
	return err
}

// DropSchema drops the brokerage database schema.
func DropSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, dropSchemaSQL)
	return err
}
