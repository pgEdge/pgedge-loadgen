//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

// Package wholesale implements the Wholesale Supplier application (TPC-C based).
package wholesale

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema SQL for creating the wholesale database schema.
// Based on TPC-C with modifications for PostgreSQL.
const createSchemaSQL = `
-- Warehouse: Distribution centers
CREATE TABLE IF NOT EXISTS warehouse (
    w_id        INTEGER PRIMARY KEY,
    w_name      VARCHAR(10) NOT NULL,
    w_street_1  VARCHAR(20) NOT NULL,
    w_street_2  VARCHAR(20),
    w_city      VARCHAR(20) NOT NULL,
    w_state     CHAR(2) NOT NULL,
    w_zip       CHAR(9) NOT NULL,
    w_tax       NUMERIC(4,4) NOT NULL,
    w_ytd       NUMERIC(12,2) NOT NULL
);

-- District: Districts within warehouses
CREATE TABLE IF NOT EXISTS district (
    d_id        INTEGER NOT NULL,
    d_w_id      INTEGER NOT NULL REFERENCES warehouse(w_id),
    d_name      VARCHAR(10) NOT NULL,
    d_street_1  VARCHAR(20) NOT NULL,
    d_street_2  VARCHAR(20),
    d_city      VARCHAR(20) NOT NULL,
    d_state     CHAR(2) NOT NULL,
    d_zip       CHAR(9) NOT NULL,
    d_tax       NUMERIC(4,4) NOT NULL,
    d_ytd       NUMERIC(12,2) NOT NULL,
    d_next_o_id INTEGER NOT NULL,
    PRIMARY KEY (d_w_id, d_id)
);

-- Customer: Customer accounts
CREATE TABLE IF NOT EXISTS customer (
    c_id            INTEGER NOT NULL,
    c_d_id          INTEGER NOT NULL,
    c_w_id          INTEGER NOT NULL,
    c_first         VARCHAR(16) NOT NULL,
    c_middle        CHAR(2),
    c_last          VARCHAR(16) NOT NULL,
    c_street_1      VARCHAR(20) NOT NULL,
    c_street_2      VARCHAR(20),
    c_city          VARCHAR(20) NOT NULL,
    c_state         CHAR(2) NOT NULL,
    c_zip           CHAR(9) NOT NULL,
    c_phone         CHAR(16) NOT NULL,
    c_since         TIMESTAMP NOT NULL,
    c_credit        CHAR(2) NOT NULL,
    c_credit_lim    NUMERIC(12,2) NOT NULL,
    c_discount      NUMERIC(4,4) NOT NULL,
    c_balance       NUMERIC(12,2) NOT NULL,
    c_ytd_payment   NUMERIC(12,2) NOT NULL,
    c_payment_cnt   INTEGER NOT NULL,
    c_delivery_cnt  INTEGER NOT NULL,
    c_data          VARCHAR(500),
    PRIMARY KEY (c_w_id, c_d_id, c_id),
    FOREIGN KEY (c_w_id, c_d_id) REFERENCES district(d_w_id, d_id)
);

-- History: Payment history
CREATE TABLE IF NOT EXISTS history (
    h_id        SERIAL PRIMARY KEY,
    h_c_id      INTEGER NOT NULL,
    h_c_d_id    INTEGER NOT NULL,
    h_c_w_id    INTEGER NOT NULL,
    h_d_id      INTEGER NOT NULL,
    h_w_id      INTEGER NOT NULL,
    h_date      TIMESTAMP NOT NULL,
    h_amount    NUMERIC(6,2) NOT NULL,
    h_data      VARCHAR(24)
);

-- Item: Product catalog
CREATE TABLE IF NOT EXISTS item (
    i_id        INTEGER PRIMARY KEY,
    i_im_id     INTEGER NOT NULL,
    i_name      VARCHAR(24) NOT NULL,
    i_price     NUMERIC(5,2) NOT NULL,
    i_data      VARCHAR(50) NOT NULL
);

-- Stock: Inventory per warehouse
CREATE TABLE IF NOT EXISTS stock (
    s_i_id      INTEGER NOT NULL REFERENCES item(i_id),
    s_w_id      INTEGER NOT NULL REFERENCES warehouse(w_id),
    s_quantity  INTEGER NOT NULL,
    s_dist_01   CHAR(24) NOT NULL,
    s_dist_02   CHAR(24) NOT NULL,
    s_dist_03   CHAR(24) NOT NULL,
    s_dist_04   CHAR(24) NOT NULL,
    s_dist_05   CHAR(24) NOT NULL,
    s_dist_06   CHAR(24) NOT NULL,
    s_dist_07   CHAR(24) NOT NULL,
    s_dist_08   CHAR(24) NOT NULL,
    s_dist_09   CHAR(24) NOT NULL,
    s_dist_10   CHAR(24) NOT NULL,
    s_ytd       INTEGER NOT NULL,
    s_order_cnt INTEGER NOT NULL,
    s_remote_cnt INTEGER NOT NULL,
    s_data      VARCHAR(50) NOT NULL,
    PRIMARY KEY (s_w_id, s_i_id)
);

-- Orders: Order headers
CREATE TABLE IF NOT EXISTS orders (
    o_id        INTEGER NOT NULL,
    o_d_id      INTEGER NOT NULL,
    o_w_id      INTEGER NOT NULL,
    o_c_id      INTEGER NOT NULL,
    o_entry_d   TIMESTAMP NOT NULL,
    o_carrier_id INTEGER,
    o_ol_cnt    INTEGER NOT NULL,
    o_all_local INTEGER NOT NULL,
    PRIMARY KEY (o_w_id, o_d_id, o_id),
    FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES customer(c_w_id, c_d_id, c_id)
);

-- New Orders: Pending orders queue
CREATE TABLE IF NOT EXISTS new_orders (
    no_o_id     INTEGER NOT NULL,
    no_d_id     INTEGER NOT NULL,
    no_w_id     INTEGER NOT NULL,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id),
    FOREIGN KEY (no_w_id, no_d_id, no_o_id) REFERENCES orders(o_w_id, o_d_id, o_id)
);

-- Order Line: Order line items
CREATE TABLE IF NOT EXISTS order_line (
    ol_o_id         INTEGER NOT NULL,
    ol_d_id         INTEGER NOT NULL,
    ol_w_id         INTEGER NOT NULL,
    ol_number       INTEGER NOT NULL,
    ol_i_id         INTEGER NOT NULL REFERENCES item(i_id),
    ol_supply_w_id  INTEGER NOT NULL,
    ol_delivery_d   TIMESTAMP,
    ol_quantity     INTEGER NOT NULL,
    ol_amount       NUMERIC(6,2) NOT NULL,
    ol_dist_info    CHAR(24) NOT NULL,
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number),
    FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) REFERENCES orders(o_w_id, o_d_id, o_id)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_customer_name ON customer(c_w_id, c_d_id, c_last, c_first);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(o_w_id, o_d_id, o_c_id);
`

// Drop schema SQL
const dropSchemaSQL = `
DROP TABLE IF EXISTS order_line CASCADE;
DROP TABLE IF EXISTS new_orders CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS history CASCADE;
DROP TABLE IF EXISTS stock CASCADE;
DROP TABLE IF EXISTS item CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS district CASCADE;
DROP TABLE IF EXISTS warehouse CASCADE;
`

// CreateSchema creates the wholesale database schema.
func CreateSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, createSchemaSQL)
	return err
}

// DropSchema drops the wholesale database schema.
func DropSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, dropSchemaSQL)
	return err
}
