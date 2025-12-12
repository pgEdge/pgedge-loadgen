package analytics

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
)

// App implements the analytics warehouse application (TPC-H based).
type App struct {
	executor *QueryExecutor
}

// New creates a new analytics application.
func New() *App {
	return &App{}
}

// Name returns the application name.
func (a *App) Name() string {
	return "analytics"
}

// Description returns a human-readable description.
func (a *App) Description() string {
	return "Analytics warehouse (TPC-H based) - OLAP workload with complex " +
		"analytical queries on suppliers, parts, customers, and orders"
}

// WorkloadType returns the workload type.
func (a *App) WorkloadType() string {
	return "OLAP"
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
			Name:        "pricing_summary",
			Description: "Pricing summary report aggregating lineitem data",
			Weight:      12,
			Type:        "read",
		},
		{
			Name:        "min_cost_supplier",
			Description: "Find minimum cost supplier for parts",
			Weight:      5,
			Type:        "read",
		},
		{
			Name:        "shipping_priority",
			Description: "Analyze shipping priority by customer segment",
			Weight:      8,
			Type:        "read",
		},
		{
			Name:        "order_priority",
			Description: "Check order priority distribution",
			Weight:      6,
			Type:        "read",
		},
		{
			Name:        "local_supplier_volume",
			Description: "Analyze local supplier volume by region",
			Weight:      5,
			Type:        "read",
		},
		{
			Name:        "revenue_forecast",
			Description: "Forecast revenue changes",
			Weight:      10,
			Type:        "read",
		},
		{
			Name:        "volume_shipping",
			Description: "Analyze volume shipping between nations",
			Weight:      4,
			Type:        "read",
		},
		{
			Name:        "market_share",
			Description: "Calculate national market share",
			Weight:      4,
			Type:        "read",
		},
		{
			Name:        "product_profit",
			Description: "Measure product type profit",
			Weight:      5,
			Type:        "read",
		},
		{
			Name:        "returned_items",
			Description: "Report on returned items by customer",
			Weight:      6,
			Type:        "read",
		},
		{
			Name:        "important_stock",
			Description: "Identify important stock items",
			Weight:      4,
			Type:        "read",
		},
		{
			Name:        "shipping_modes",
			Description: "Analyze shipping modes and order priority",
			Weight:      6,
			Type:        "read",
		},
		{
			Name:        "customer_distribution",
			Description: "Analyze customer order distribution",
			Weight:      5,
			Type:        "read",
		},
		{
			Name:        "promotion_effect",
			Description: "Measure promotion effectiveness",
			Weight:      6,
			Type:        "read",
		},
		{
			Name:        "top_supplier",
			Description: "Find top suppliers by revenue",
			Weight:      4,
			Type:        "read",
		},
		{
			Name:        "parts_supplier",
			Description: "Analyze parts/supplier relationships",
			Weight:      3,
			Type:        "read",
		},
		{
			Name:        "small_quantity",
			Description: "Analyze small-quantity orders",
			Weight:      2,
			Type:        "read",
		},
		{
			Name:        "large_volume",
			Description: "Find large volume customers",
			Weight:      2,
			Type:        "read",
		},
		{
			Name:        "discounted_revenue",
			Description: "Calculate discounted revenue",
			Weight:      3,
			Type:        "read",
		},
	}
}

// ExecuteQuery executes a randomly selected query based on the query mix.
func (a *App) ExecuteQuery(ctx context.Context, pool *pgxpool.Pool) apps.QueryResult {
	// Initialize executor if needed (lazy initialization to get counts)
	if a.executor == nil {
		numSuppliers, numParts, numCustomers, numOrders := a.getTableCounts(ctx, pool)
		a.executor = NewQueryExecutor(numSuppliers, numParts, numCustomers, numOrders)
	}
	return a.executor.ExecuteRandomQuery(ctx, pool)
}

// ExecuteQueryConn executes a randomly selected query using a single connection.
func (a *App) ExecuteQueryConn(ctx context.Context, conn *pgx.Conn) apps.QueryResult {
	// Initialize executor if needed (lazy initialization to get counts)
	if a.executor == nil {
		numSuppliers, numParts, numCustomers, numOrders := a.getTableCountsConn(ctx, conn)
		a.executor = NewQueryExecutor(numSuppliers, numParts, numCustomers, numOrders)
	}
	return a.executor.ExecuteRandomQuery(ctx, conn)
}

// RequiresPgvector returns true if the app needs pgvector extension.
func (a *App) RequiresPgvector() bool {
	return false
}

func (a *App) getTableCounts(ctx context.Context, pool *pgxpool.Pool) (int, int, int, int) {
	var numSuppliers, numParts, numCustomers, numOrders int

	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM supplier").Scan(&numSuppliers)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM part").Scan(&numParts)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM customer").Scan(&numCustomers)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM orders").Scan(&numOrders)

	return max(1, numSuppliers), max(1, numParts), max(1, numCustomers), max(1, numOrders)
}

func (a *App) getTableCountsConn(ctx context.Context, conn *pgx.Conn) (int, int, int, int) {
	var numSuppliers, numParts, numCustomers, numOrders int

	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM supplier").Scan(&numSuppliers)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM part").Scan(&numParts)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM customer").Scan(&numCustomers)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM orders").Scan(&numOrders)

	return max(1, numSuppliers), max(1, numParts), max(1, numCustomers), max(1, numOrders)
}

func init() {
	apps.Register(New())
}
