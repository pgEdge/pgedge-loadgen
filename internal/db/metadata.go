//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Portions copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/logging"
	"github.com/pgEdge/pgedge-loadgen/pkg/version"
)

const metadataTable = "loadgen_metadata"

// createMetadataTableSQL creates the metadata table if it doesn't exist.
const createMetadataTableSQL = `
CREATE TABLE IF NOT EXISTS loadgen_metadata (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
)`

// SaveMetadata saves initialization metadata to the database.
func SaveMetadata(ctx context.Context, pool *pgxpool.Pool, app string, targetSize string) error {
	// Create table if it doesn't exist
	_, err := pool.Exec(ctx, createMetadataTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create metadata table: %w", err)
	}

	// Insert or update metadata
	metadata := map[string]string{
		"app":            app,
		"version":        version.Short(),
		"initialized_at": time.Now().UTC().Format(time.RFC3339),
		"target_size":    targetSize,
	}

	for key, value := range metadata {
		_, err := pool.Exec(ctx, `
            INSERT INTO loadgen_metadata (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        `, key, value)
		if err != nil {
			return fmt.Errorf("failed to save metadata %s: %w", key, err)
		}
	}

	logging.Debug().
		Str("app", app).
		Str("target_size", targetSize).
		Msg("Saved metadata")

	return nil
}

// GetMetadataValue retrieves a single metadata value by key.
func GetMetadataValue(ctx context.Context, pool *pgxpool.Pool, key string) (string, error) {
	var value string
	err := pool.QueryRow(ctx, `
        SELECT value FROM loadgen_metadata WHERE key = $1
    `, key).Scan(&value)
	if err != nil {
		return "", err
	}
	return value, nil
}

// GetMetadataValueConn retrieves a single metadata value by key using a single connection.
func GetMetadataValueConn(ctx context.Context, conn *pgx.Conn, key string) (string, error) {
	var value string
	err := conn.QueryRow(ctx, `
        SELECT value FROM loadgen_metadata WHERE key = $1
    `, key).Scan(&value)
	if err != nil {
		return "", err
	}
	return value, nil
}

// GetAllMetadata retrieves all metadata as a map.
func GetAllMetadata(ctx context.Context, pool *pgxpool.Pool) (map[string]string, error) {
	rows, err := pool.Query(ctx, `SELECT key, value FROM loadgen_metadata`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	metadata := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		metadata[key] = value
	}

	return metadata, rows.Err()
}

// DropMetadata drops the metadata table.
func DropMetadata(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", metadataTable))
	return err
}

// MetadataExists checks if the metadata table exists.
func MetadataExists(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, `
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = $1
        )
    `, metadataTable).Scan(&exists)
	return exists, err
}
