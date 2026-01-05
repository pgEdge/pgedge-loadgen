//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Portions copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

// Package knowledgebase implements the Knowledge Base application with semantic search.
package knowledgebase

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema SQL template for creating the knowledgebase database schema.
const createSchemaSQLTemplate = `
-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Category: Article categories
CREATE TABLE IF NOT EXISTS category (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    slug        VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    parent_id   INTEGER REFERENCES category(id),
    article_count INTEGER DEFAULT 0,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Tag: Article tags
CREATE TABLE IF NOT EXISTS tag (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(50) NOT NULL UNIQUE,
    slug        VARCHAR(50) NOT NULL UNIQUE,
    usage_count INTEGER DEFAULT 0
);

-- Users: Support agents and customers
CREATE TABLE IF NOT EXISTS kb_user (
    id          SERIAL PRIMARY KEY,
    email       VARCHAR(255) NOT NULL UNIQUE,
    username    VARCHAR(50) NOT NULL UNIQUE,
    role        VARCHAR(20) NOT NULL DEFAULT 'customer',
    first_name  VARCHAR(50),
    last_name   VARCHAR(50),
    is_active   BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Article: KB articles with embeddings
CREATE TABLE IF NOT EXISTS article (
    id              SERIAL PRIMARY KEY,
    title           VARCHAR(255) NOT NULL,
    slug            VARCHAR(255) NOT NULL UNIQUE,
    summary         TEXT,
    content         TEXT NOT NULL,
    category_id     INTEGER REFERENCES category(id),
    author_id       INTEGER REFERENCES kb_user(id),
    status          VARCHAR(20) NOT NULL DEFAULT 'draft',
    view_count      INTEGER DEFAULT 0,
    helpful_count   INTEGER DEFAULT 0,
    unhelpful_count INTEGER DEFAULT 0,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    published_at    TIMESTAMP,
    embedding       vector(%d)
);

-- Article Section: Sections within articles with embeddings
CREATE TABLE IF NOT EXISTS article_section (
    id          SERIAL PRIMARY KEY,
    article_id  INTEGER NOT NULL REFERENCES article(id) ON DELETE CASCADE,
    title       VARCHAR(255),
    content     TEXT NOT NULL,
    section_order INTEGER NOT NULL,
    embedding   vector(%d)
);

-- Article Tag: Many-to-many relationship
CREATE TABLE IF NOT EXISTS article_tag (
    article_id  INTEGER NOT NULL REFERENCES article(id) ON DELETE CASCADE,
    tag_id      INTEGER NOT NULL REFERENCES tag(id) ON DELETE CASCADE,
    PRIMARY KEY (article_id, tag_id)
);

-- Search Log: Search history with query embeddings
CREATE TABLE IF NOT EXISTS search_log (
    id              SERIAL PRIMARY KEY,
    user_id         INTEGER REFERENCES kb_user(id),
    query_text      VARCHAR(500) NOT NULL,
    results_count   INTEGER NOT NULL DEFAULT 0,
    clicked_article INTEGER REFERENCES article(id),
    session_id      VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW(),
    embedding       vector(%d)
);

-- Feedback: Article helpfulness ratings
CREATE TABLE IF NOT EXISTS feedback (
    id          SERIAL PRIMARY KEY,
    article_id  INTEGER NOT NULL REFERENCES article(id),
    user_id     INTEGER REFERENCES kb_user(id),
    is_helpful  BOOLEAN NOT NULL,
    comment     TEXT,
    session_id  VARCHAR(100),
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Related Articles: Pre-computed similar articles
CREATE TABLE IF NOT EXISTS related_article (
    article_id  INTEGER NOT NULL REFERENCES article(id) ON DELETE CASCADE,
    related_id  INTEGER NOT NULL REFERENCES article(id) ON DELETE CASCADE,
    similarity  NUMERIC(5,4) NOT NULL,
    PRIMARY KEY (article_id, related_id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_article_category ON article(category_id);
CREATE INDEX IF NOT EXISTS idx_article_author ON article(author_id);
CREATE INDEX IF NOT EXISTS idx_article_status ON article(status);
CREATE INDEX IF NOT EXISTS idx_article_section_article ON article_section(article_id);
CREATE INDEX IF NOT EXISTS idx_search_log_user ON search_log(user_id);
CREATE INDEX IF NOT EXISTS idx_search_log_created ON search_log(created_at);
CREATE INDEX IF NOT EXISTS idx_feedback_article ON feedback(article_id);
CREATE INDEX IF NOT EXISTS idx_category_parent ON category(parent_id);

-- Vector indexes for semantic search
CREATE INDEX IF NOT EXISTS idx_article_embedding ON article
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
CREATE INDEX IF NOT EXISTS idx_section_embedding ON article_section
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
CREATE INDEX IF NOT EXISTS idx_search_embedding ON search_log
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
`

// Drop schema SQL
const dropSchemaSQL = `
DROP TABLE IF EXISTS related_article CASCADE;
DROP TABLE IF EXISTS feedback CASCADE;
DROP TABLE IF EXISTS search_log CASCADE;
DROP TABLE IF EXISTS article_tag CASCADE;
DROP TABLE IF EXISTS article_section CASCADE;
DROP TABLE IF EXISTS article CASCADE;
DROP TABLE IF EXISTS kb_user CASCADE;
DROP TABLE IF EXISTS tag CASCADE;
DROP TABLE IF EXISTS category CASCADE;
`

// CreateSchema creates the knowledgebase database schema.
func CreateSchema(ctx context.Context, pool *pgxpool.Pool, dimensions int) error {
	sql := fmt.Sprintf(createSchemaSQLTemplate, dimensions, dimensions, dimensions)
	_, err := pool.Exec(ctx, sql)
	return err
}

// DropSchema drops the knowledgebase database schema.
func DropSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, dropSchemaSQL)
	return err
}
