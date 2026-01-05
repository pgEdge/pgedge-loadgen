//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Portions copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

package brokerage

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
)

// App implements the brokerage firm application (TPC-E based).
type App struct {
	executor *QueryExecutor
}

// New creates a new brokerage application.
func New() *App {
	return &App{}
}

// Name returns the application name.
func (a *App) Name() string {
	return "brokerage"
}

// Description returns a human-readable description.
func (a *App) Description() string {
	return "Brokerage firm (TPC-E based) - Mixed OLTP workload simulating stock " +
		"trading with customers, brokers, accounts, securities, and trade transactions"
}

// WorkloadType returns the workload type.
func (a *App) WorkloadType() string {
	return "Mixed"
}

// CreateSchema creates the application's database schema.
func (a *App) CreateSchema(ctx context.Context, pool *pgxpool.Pool) error {
	return CreateSchema(ctx, pool)
}

// DropSchema drops the application's database schema.
func (a *App) DropSchema(ctx context.Context, pool *pgxpool.Pool) error {
	return DropSchema(ctx, pool)
}

// GenerateData generates test data for the application.
func (a *App) GenerateData(ctx context.Context, pool *pgxpool.Pool, cfg apps.GeneratorConfig) error {
	gen := NewGenerator()
	return gen.GenerateData(ctx, pool, cfg.TargetSize)
}

// GetQueries returns the available queries for this application.
func (a *App) GetQueries() []apps.QueryDefinition {
	return []apps.QueryDefinition{
		{
			Name:        "broker_volume",
			Description: "Calculate broker trading volume by sector",
			Weight:      5,
			Type:        "read",
		},
		{
			Name:        "customer_position",
			Description: "Get customer portfolio positions and values",
			Weight:      13,
			Type:        "read",
		},
		{
			Name:        "market_feed",
			Description: "Update security prices with market data",
			Weight:      1,
			Type:        "write",
		},
		{
			Name:        "market_watch",
			Description: "Check watched securities for price changes",
			Weight:      18,
			Type:        "read",
		},
		{
			Name:        "security_detail",
			Description: "Get detailed security information and history",
			Weight:      14,
			Type:        "read",
		},
		{
			Name:        "trade_lookup",
			Description: "Look up historical trades by account and date",
			Weight:      8,
			Type:        "read",
		},
		{
			Name:        "trade_order",
			Description: "Place a new trade order",
			Weight:      10,
			Type:        "write",
		},
		{
			Name:        "trade_result",
			Description: "Process and complete pending trades",
			Weight:      10,
			Type:        "write",
		},
		{
			Name:        "trade_status",
			Description: "Check status of recent trades",
			Weight:      19,
			Type:        "read",
		},
		{
			Name:        "trade_update",
			Description: "Modify existing pending trades",
			Weight:      2,
			Type:        "write",
		},
	}
}

// ExecuteQuery executes a randomly selected query based on the query mix.
func (a *App) ExecuteQuery(ctx context.Context, pool *pgxpool.Pool) apps.QueryResult {
	// Initialize executor if needed (lazy initialization to get counts)
	if a.executor == nil {
		numCustomers, numAccounts, numSecurities, numTrades, numBrokers := a.getTableCounts(ctx, pool)
		a.executor = NewQueryExecutor(numCustomers, numAccounts, numSecurities, numTrades, numBrokers)
	}
	return a.executor.ExecuteRandomQuery(ctx, pool)
}

// ExecuteQueryConn executes a randomly selected query using a single connection.
func (a *App) ExecuteQueryConn(ctx context.Context, conn *pgx.Conn) apps.QueryResult {
	// Initialize executor if needed (lazy initialization to get counts)
	if a.executor == nil {
		numCustomers, numAccounts, numSecurities, numTrades, numBrokers := a.getTableCountsConn(ctx, conn)
		a.executor = NewQueryExecutor(numCustomers, numAccounts, numSecurities, numTrades, numBrokers)
	}
	return a.executor.ExecuteRandomQuery(ctx, conn)
}

// RequiresPgvector returns true if the app needs pgvector extension.
func (a *App) RequiresPgvector() bool {
	return false
}

func (a *App) getTableCounts(ctx context.Context, pool *pgxpool.Pool) (int, int, int, int, int) {
	var numCustomers, numAccounts, numSecurities, numTrades, numBrokers int

	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM customer").Scan(&numCustomers)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM customer_account").Scan(&numAccounts)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM security").Scan(&numSecurities)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM trade").Scan(&numTrades)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM broker").Scan(&numBrokers)

	return max(1, numCustomers), max(1, numAccounts), max(1, numSecurities),
		max(1, numTrades), max(1, numBrokers)
}

func (a *App) getTableCountsConn(ctx context.Context, conn *pgx.Conn) (int, int, int, int, int) {
	var numCustomers, numAccounts, numSecurities, numTrades, numBrokers int

	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM customer").Scan(&numCustomers)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM customer_account").Scan(&numAccounts)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM security").Scan(&numSecurities)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM trade").Scan(&numTrades)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM broker").Scan(&numBrokers)

	return max(1, numCustomers), max(1, numAccounts), max(1, numSecurities),
		max(1, numTrades), max(1, numBrokers)
}

func init() {
	apps.Register(New())
}
