// Package db provides database connection management for pgedge-loadgen.
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/logging"
)

// DefaultPoolConfig returns default connection pool configuration.
func DefaultPoolConfig() *pgxpool.Config {
	config, _ := pgxpool.ParseConfig("")

	// Connection pool settings
	config.MaxConns = 100
	config.MinConns = 5
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	config.HealthCheckPeriod = 30 * time.Second

	return config
}

// Connect establishes a connection pool to the PostgreSQL database.
func Connect(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Apply default pool settings
	defaults := DefaultPoolConfig()
	config.MaxConns = defaults.MaxConns
	config.MinConns = defaults.MinConns
	config.MaxConnLifetime = defaults.MaxConnLifetime
	config.MaxConnIdleTime = defaults.MaxConnIdleTime
	config.HealthCheckPeriod = defaults.HealthCheckPeriod

	logging.Debug().
		Str("host", config.ConnConfig.Host).
		Uint16("port", config.ConnConfig.Port).
		Str("database", config.ConnConfig.Database).
		Msg("Connecting to database")

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logging.Info().
		Str("host", config.ConnConfig.Host).
		Str("database", config.ConnConfig.Database).
		Msg("Connected to database")

	return pool, nil
}

// ConnectWithMaxConns establishes a connection pool with a specified max connections.
func ConnectWithMaxConns(ctx context.Context, connString string, maxConns int32) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Apply default pool settings
	defaults := DefaultPoolConfig()
	config.MaxConns = maxConns
	config.MinConns = min(defaults.MinConns, maxConns)
	config.MaxConnLifetime = defaults.MaxConnLifetime
	config.MaxConnIdleTime = defaults.MaxConnIdleTime
	config.HealthCheckPeriod = defaults.HealthCheckPeriod

	logging.Debug().
		Str("host", config.ConnConfig.Host).
		Uint16("port", config.ConnConfig.Port).
		Str("database", config.ConnConfig.Database).
		Int32("max_conns", maxConns).
		Msg("Connecting to database")

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logging.Info().
		Str("host", config.ConnConfig.Host).
		Str("database", config.ConnConfig.Database).
		Int32("max_conns", maxConns).
		Msg("Connected to database")

	return pool, nil
}
