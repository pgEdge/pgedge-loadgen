//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

// Package db provides database connection management for pgedge-loadgen.
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/logging"
)

const (
	// AppNamePrefix is the prefix used for all application names.
	AppNamePrefix = "pgedge-loadgen"
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
func Connect(ctx context.Context, connString string, appNameSuffix string) (*pgxpool.Pool, error) {
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

	// Set application name
	appName := AppNamePrefix
	if appNameSuffix != "" {
		appName = fmt.Sprintf("%s - %s", AppNamePrefix, appNameSuffix)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["application_name"] = appName

	logging.Debug().
		Str("host", config.ConnConfig.Host).
		Uint16("port", config.ConnConfig.Port).
		Str("database", config.ConnConfig.Database).
		Str("application_name", appName).
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
func ConnectWithMaxConns(ctx context.Context, connString string, maxConns int32, appNameSuffix string) (*pgxpool.Pool, error) {
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

	// Set application name
	appName := AppNamePrefix
	if appNameSuffix != "" {
		appName = fmt.Sprintf("%s - %s", AppNamePrefix, appNameSuffix)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["application_name"] = appName

	logging.Debug().
		Str("host", config.ConnConfig.Host).
		Uint16("port", config.ConnConfig.Port).
		Str("database", config.ConnConfig.Database).
		Int32("max_conns", maxConns).
		Str("application_name", appName).
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

// ConnectSingle establishes a single database connection (not pooled).
// This is useful for workers that need dedicated connections with unique application names.
func ConnectSingle(ctx context.Context, connString string, appNameSuffix string) (*pgx.Conn, error) {
	config, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Set application name
	appName := AppNamePrefix
	if appNameSuffix != "" {
		appName = fmt.Sprintf("%s - %s", AppNamePrefix, appNameSuffix)
	}
	if config.RuntimeParams == nil {
		config.RuntimeParams = make(map[string]string)
	}
	config.RuntimeParams["application_name"] = appName

	logging.Debug().
		Str("host", config.Host).
		Uint16("port", config.Port).
		Str("database", config.Database).
		Str("application_name", appName).
		Msg("Connecting to database")

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Verify connection
	if err := conn.Ping(ctx); err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return conn, nil
}
