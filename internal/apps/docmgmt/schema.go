//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Portions copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

// Package docmgmt implements the Document Management application with semantic search.
package docmgmt

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema SQL template for creating the docmgmt database schema.
const createSchemaSQLTemplate = `
-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Users: System users
CREATE TABLE IF NOT EXISTS doc_user (
    id          SERIAL PRIMARY KEY,
    email       VARCHAR(255) NOT NULL UNIQUE,
    username    VARCHAR(50) NOT NULL UNIQUE,
    full_name   VARCHAR(100),
    role        VARCHAR(20) NOT NULL DEFAULT 'user',
    department  VARCHAR(100),
    is_active   BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMP DEFAULT NOW(),
    last_login  TIMESTAMP
);

-- Folder: Folder hierarchy
CREATE TABLE IF NOT EXISTS folder (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    parent_id   INTEGER REFERENCES folder(id),
    owner_id    INTEGER REFERENCES doc_user(id),
    path        TEXT NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW()
);

-- Document: Document metadata with content embedding
CREATE TABLE IF NOT EXISTS document (
    id              SERIAL PRIMARY KEY,
    title           VARCHAR(255) NOT NULL,
    description     TEXT,
    file_type       VARCHAR(50) NOT NULL,
    file_size       BIGINT NOT NULL,
    mime_type       VARCHAR(100),
    folder_id       INTEGER REFERENCES folder(id),
    owner_id        INTEGER NOT NULL REFERENCES doc_user(id),
    status          VARCHAR(20) NOT NULL DEFAULT 'active',
    version         INTEGER DEFAULT 1,
    checksum        VARCHAR(64),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    embedding       vector(%d)
);

-- Document Version: Version history
CREATE TABLE IF NOT EXISTS document_version (
    id              SERIAL PRIMARY KEY,
    document_id     INTEGER NOT NULL REFERENCES document(id) ON DELETE CASCADE,
    version_number  INTEGER NOT NULL,
    file_size       BIGINT NOT NULL,
    checksum        VARCHAR(64),
    change_summary  TEXT,
    created_by      INTEGER REFERENCES doc_user(id),
    created_at      TIMESTAMP DEFAULT NOW(),
    embedding       vector(%d)
);

-- Document Chunk: Chunked content for large documents with embeddings
CREATE TABLE IF NOT EXISTS document_chunk (
    id          SERIAL PRIMARY KEY,
    document_id INTEGER NOT NULL REFERENCES document(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    content     TEXT NOT NULL,
    start_page  INTEGER,
    end_page    INTEGER,
    embedding   vector(%d)
);

-- Tag: Document tags
CREATE TABLE IF NOT EXISTS doc_tag (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(50) NOT NULL UNIQUE,
    color       VARCHAR(7) DEFAULT '#808080'
);

-- Document Tag: Many-to-many relationship
CREATE TABLE IF NOT EXISTS document_tag (
    document_id INTEGER NOT NULL REFERENCES document(id) ON DELETE CASCADE,
    tag_id      INTEGER NOT NULL REFERENCES doc_tag(id) ON DELETE CASCADE,
    PRIMARY KEY (document_id, tag_id)
);

-- Permission: Access control
CREATE TABLE IF NOT EXISTS permission (
    id              SERIAL PRIMARY KEY,
    document_id     INTEGER REFERENCES document(id) ON DELETE CASCADE,
    folder_id       INTEGER REFERENCES folder(id) ON DELETE CASCADE,
    user_id         INTEGER REFERENCES doc_user(id) ON DELETE CASCADE,
    permission_type VARCHAR(20) NOT NULL,
    granted_by      INTEGER REFERENCES doc_user(id),
    granted_at      TIMESTAMP DEFAULT NOW(),
    expires_at      TIMESTAMP,
    CONSTRAINT chk_target CHECK (
        (document_id IS NOT NULL AND folder_id IS NULL) OR
        (document_id IS NULL AND folder_id IS NOT NULL)
    )
);

-- Audit Log: Access audit trail
CREATE TABLE IF NOT EXISTS audit_log (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER REFERENCES doc_user(id),
    document_id INTEGER REFERENCES document(id) ON DELETE SET NULL,
    folder_id   INTEGER REFERENCES folder(id) ON DELETE SET NULL,
    action      VARCHAR(50) NOT NULL,
    details     JSONB,
    ip_address  VARCHAR(45),
    user_agent  TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Share Link: Public/private sharing links
CREATE TABLE IF NOT EXISTS share_link (
    id          SERIAL PRIMARY KEY,
    document_id INTEGER REFERENCES document(id) ON DELETE CASCADE,
    folder_id   INTEGER REFERENCES folder(id) ON DELETE CASCADE,
    token       VARCHAR(64) NOT NULL UNIQUE,
    created_by  INTEGER NOT NULL REFERENCES doc_user(id),
    access_type VARCHAR(20) NOT NULL DEFAULT 'view',
    password    VARCHAR(255),
    expires_at  TIMESTAMP,
    max_downloads INTEGER,
    download_count INTEGER DEFAULT 0,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_document_folder ON document(folder_id);
CREATE INDEX IF NOT EXISTS idx_document_owner ON document(owner_id);
CREATE INDEX IF NOT EXISTS idx_document_status ON document(status);
CREATE INDEX IF NOT EXISTS idx_document_type ON document(file_type);
CREATE INDEX IF NOT EXISTS idx_folder_parent ON folder(parent_id);
CREATE INDEX IF NOT EXISTS idx_folder_owner ON folder(owner_id);
CREATE INDEX IF NOT EXISTS idx_folder_path ON folder(path);
CREATE INDEX IF NOT EXISTS idx_version_document ON document_version(document_id);
CREATE INDEX IF NOT EXISTS idx_chunk_document ON document_chunk(document_id);
CREATE INDEX IF NOT EXISTS idx_permission_document ON permission(document_id);
CREATE INDEX IF NOT EXISTS idx_permission_folder ON permission(folder_id);
CREATE INDEX IF NOT EXISTS idx_permission_user ON permission(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_document ON audit_log(document_id);
CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at);

-- Vector indexes for semantic search
CREATE INDEX IF NOT EXISTS idx_document_embedding ON document
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
CREATE INDEX IF NOT EXISTS idx_version_embedding ON document_version
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
CREATE INDEX IF NOT EXISTS idx_chunk_embedding ON document_chunk
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
`

// Drop schema SQL
const dropSchemaSQL = `
DROP TABLE IF EXISTS share_link CASCADE;
DROP TABLE IF EXISTS audit_log CASCADE;
DROP TABLE IF EXISTS permission CASCADE;
DROP TABLE IF EXISTS document_tag CASCADE;
DROP TABLE IF EXISTS doc_tag CASCADE;
DROP TABLE IF EXISTS document_chunk CASCADE;
DROP TABLE IF EXISTS document_version CASCADE;
DROP TABLE IF EXISTS document CASCADE;
DROP TABLE IF EXISTS folder CASCADE;
DROP TABLE IF EXISTS doc_user CASCADE;
`

// CreateSchema creates the docmgmt database schema.
func CreateSchema(ctx context.Context, pool *pgxpool.Pool, dimensions int) error {
	sql := fmt.Sprintf(createSchemaSQLTemplate, dimensions, dimensions, dimensions)
	_, err := pool.Exec(ctx, sql)
	return err
}

// DropSchema drops the docmgmt database schema.
func DropSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, dropSchemaSQL)
	return err
}
