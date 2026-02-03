//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

// Package testutil provides utilities for integration testing.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DefaultTestConnString is the default connection string for tests.
	// Override with PGEDGE_TEST_CONN environment variable.
	DefaultTestConnString = "postgres://postgres@localhost:5432/postgres"

	// TestDBPrefix is the prefix for test databases.
	TestDBPrefix = "loadgen_test_"
)

// PostgresAvailable checks if PostgreSQL is available for testing.
// Returns the connection string if available, empty string otherwise.
func PostgresAvailable() string {
	connStr := os.Getenv("PGEDGE_TEST_CONN")
	if connStr == "" {
		connStr = DefaultTestConnString
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return ""
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return ""
	}

	return connStr
}

// SkipIfNoPostgres skips the test if PostgreSQL is not available.
func SkipIfNoPostgres(t *testing.T) string {
	connStr := PostgresAvailable()
	if connStr == "" {
		t.Skip("PostgreSQL not available, skipping integration test")
	}
	return connStr
}

// CreateTestDB creates a test database and returns the connection string.
func CreateTestDB(t *testing.T, baseConnStr, appName string) string {
	t.Helper()

	// Generate random suffix for database name
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		t.Fatalf("Failed to generate random database name: %v", err)
	}
	dbName := TestDBPrefix + appName + "_" + hex.EncodeToString(randomBytes)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Connect to default database to create test database
	pool, err := pgxpool.New(ctx, baseConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to postgres: %v", err)
	}
	defer pool.Close()

	// Drop if exists and create fresh
	_, err = pool.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	if err != nil {
		t.Fatalf("Failed to drop existing test database: %v", err)
	}

	_, err = pool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Return connection string for new database
	// Parse the base connection string and build a new one with the test database
	config, err := pgxpool.ParseConfig(baseConnStr)
	if err != nil {
		t.Fatalf("Failed to parse connection string: %v", err)
	}

	// Build the connection string manually since ConnString() doesn't reflect
	// changes made to ConnConfig.Database
	host := config.ConnConfig.Host
	port := config.ConnConfig.Port
	user := config.ConnConfig.User
	password := config.ConnConfig.Password

	var testConnStr string
	if password != "" {
		testConnStr = fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
			user, password, host, port, dbName)
	} else {
		testConnStr = fmt.Sprintf("postgres://%s@%s:%d/%s",
			user, host, port, dbName)
	}

	return testConnStr
}

// DropTestDB drops the test database.
func DropTestDB(t *testing.T, baseConnStr, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, baseConnStr)
	if err != nil {
		t.Logf("Warning: Failed to connect to drop test database: %v", err)
		return
	}
	defer pool.Close()

	// Terminate connections to the database
	_, _ = pool.Exec(ctx, fmt.Sprintf(`
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = '%s' AND pid <> pg_backend_pid()
    `, dbName))

	_, err = pool.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	if err != nil {
		t.Logf("Warning: Failed to drop test database: %v", err)
	}
}

// GetDBNameFromConnStr extracts the database name from a connection string.
func GetDBNameFromConnStr(connStr string) string {
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return ""
	}
	return config.ConnConfig.Database
}

// ConnectTestDB connects to a test database.
func ConnectTestDB(t *testing.T, connStr string) *pgxpool.Pool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	return pool
}

// HasPgvector checks if the pgvector extension is available.
func HasPgvector(t *testing.T, pool *pgxpool.Pool) bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var available bool
	err := pool.QueryRow(ctx, `
        SELECT EXISTS (
            SELECT 1 FROM pg_available_extensions WHERE name = 'vector'
        )
    `).Scan(&available)
	if err != nil {
		return false
	}
	return available
}

// SkipIfNoPgvector skips the test if pgvector is not available.
func SkipIfNoPgvector(t *testing.T, pool *pgxpool.Pool) {
	if !HasPgvector(t, pool) {
		t.Skip("pgvector extension not available, skipping test")
	}
}

// TestCleanup is a helper that cleans up test resources.
type TestCleanup struct {
	t           *testing.T
	baseConnStr string
	dbName      string
	pool        *pgxpool.Pool
}

// NewTestCleanup creates a new test cleanup helper.
func NewTestCleanup(t *testing.T, baseConnStr, dbName string) *TestCleanup {
	return &TestCleanup{
		t:           t,
		baseConnStr: baseConnStr,
		dbName:      dbName,
	}
}

// SetPool sets the pool to close on cleanup.
func (tc *TestCleanup) SetPool(pool *pgxpool.Pool) {
	tc.pool = pool
}

// Cleanup performs the cleanup.
// The database is only dropped if the test passed; on failure it remains
// for diagnostic purposes.
func (tc *TestCleanup) Cleanup() {
	if tc.pool != nil {
		tc.pool.Close()
	}
	if tc.dbName != "" {
		if tc.t.Failed() {
			tc.t.Logf("Test failed - keeping database %s for diagnostics", tc.dbName)
		} else {
			DropTestDB(tc.t, tc.baseConnStr, tc.dbName)
		}
	}
}
