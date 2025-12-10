// Package ecommerce implements the E-commerce application with semantic search.
package ecommerce

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema SQL template for creating the ecommerce database schema.
// Uses %d for embedding dimensions which will be formatted.
const createSchemaSQLTemplate = `
-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Category: Product categories
CREATE TABLE IF NOT EXISTS category (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    description TEXT,
    parent_id   INTEGER REFERENCES category(id),
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Brand: Product manufacturers
CREATE TABLE IF NOT EXISTS brand (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    description TEXT,
    website     VARCHAR(255),
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Product: Product catalog with embeddings
CREATE TABLE IF NOT EXISTS product (
    id              SERIAL PRIMARY KEY,
    sku             VARCHAR(50) NOT NULL UNIQUE,
    name            VARCHAR(200) NOT NULL,
    description     TEXT,
    category_id     INTEGER REFERENCES category(id),
    brand_id        INTEGER REFERENCES brand(id),
    price           NUMERIC(10,2) NOT NULL,
    cost            NUMERIC(10,2),
    weight          NUMERIC(8,2),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    embedding       vector(%d)
);

-- Inventory: Stock levels
CREATE TABLE IF NOT EXISTS inventory (
    id          SERIAL PRIMARY KEY,
    product_id  INTEGER NOT NULL REFERENCES product(id),
    warehouse   VARCHAR(50) NOT NULL,
    quantity    INTEGER NOT NULL DEFAULT 0,
    reserved    INTEGER NOT NULL DEFAULT 0,
    updated_at  TIMESTAMP DEFAULT NOW(),
    UNIQUE(product_id, warehouse)
);

-- Customer: User accounts
CREATE TABLE IF NOT EXISTS customer (
    id              SERIAL PRIMARY KEY,
    email           VARCHAR(255) NOT NULL UNIQUE,
    first_name      VARCHAR(50) NOT NULL,
    last_name       VARCHAR(50) NOT NULL,
    phone           VARCHAR(20),
    address_line1   VARCHAR(255),
    address_line2   VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(50),
    postal_code     VARCHAR(20),
    country         VARCHAR(50) DEFAULT 'USA',
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Cart: Shopping cart
CREATE TABLE IF NOT EXISTS cart (
    id          SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customer(id),
    session_id  VARCHAR(100),
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW()
);

-- Cart Item: Items in cart
CREATE TABLE IF NOT EXISTS cart_item (
    id          SERIAL PRIMARY KEY,
    cart_id     INTEGER NOT NULL REFERENCES cart(id) ON DELETE CASCADE,
    product_id  INTEGER NOT NULL REFERENCES product(id),
    quantity    INTEGER NOT NULL DEFAULT 1,
    added_at    TIMESTAMP DEFAULT NOW(),
    UNIQUE(cart_id, product_id)
);

-- Orders: Order headers
CREATE TABLE IF NOT EXISTS orders (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL REFERENCES customer(id),
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
    subtotal        NUMERIC(10,2) NOT NULL,
    tax             NUMERIC(10,2) NOT NULL DEFAULT 0,
    shipping        NUMERIC(10,2) NOT NULL DEFAULT 0,
    total           NUMERIC(10,2) NOT NULL,
    shipping_address TEXT,
    billing_address TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Order Item: Line items
CREATE TABLE IF NOT EXISTS order_item (
    id          SERIAL PRIMARY KEY,
    order_id    INTEGER NOT NULL REFERENCES orders(id),
    product_id  INTEGER NOT NULL REFERENCES product(id),
    quantity    INTEGER NOT NULL,
    unit_price  NUMERIC(10,2) NOT NULL,
    total_price NUMERIC(10,2) NOT NULL
);

-- Product Review: Customer reviews with sentiment
CREATE TABLE IF NOT EXISTS product_review (
    id              SERIAL PRIMARY KEY,
    product_id      INTEGER NOT NULL REFERENCES product(id),
    customer_id     INTEGER NOT NULL REFERENCES customer(id),
    rating          INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
    title           VARCHAR(200),
    review_text     TEXT,
    helpful_votes   INTEGER DEFAULT 0,
    verified        BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW(),
    embedding       vector(%d)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_product_category ON product(category_id);
CREATE INDEX IF NOT EXISTS idx_product_brand ON product(brand_id);
CREATE INDEX IF NOT EXISTS idx_product_price ON product(price);
CREATE INDEX IF NOT EXISTS idx_product_active ON product(is_active);
CREATE INDEX IF NOT EXISTS idx_inventory_product ON inventory(product_id);
CREATE INDEX IF NOT EXISTS idx_cart_customer ON cart(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_review_product ON product_review(product_id);
CREATE INDEX IF NOT EXISTS idx_review_customer ON product_review(customer_id);

-- Vector indexes for semantic search
CREATE INDEX IF NOT EXISTS idx_product_embedding ON product
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
CREATE INDEX IF NOT EXISTS idx_review_embedding ON product_review
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
`

// Drop schema SQL
const dropSchemaSQL = `
DROP TABLE IF EXISTS order_item CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS cart_item CASCADE;
DROP TABLE IF EXISTS cart CASCADE;
DROP TABLE IF EXISTS product_review CASCADE;
DROP TABLE IF EXISTS inventory CASCADE;
DROP TABLE IF EXISTS product CASCADE;
DROP TABLE IF EXISTS brand CASCADE;
DROP TABLE IF EXISTS category CASCADE;
`

// CreateSchema creates the ecommerce database schema.
func CreateSchema(ctx context.Context, pool *pgxpool.Pool, dimensions int) error {
	sql := fmt.Sprintf(createSchemaSQLTemplate, dimensions, dimensions)
	_, err := pool.Exec(ctx, sql)
	return err
}

// DropSchema drops the ecommerce database schema.
func DropSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, dropSchemaSQL)
	return err
}
