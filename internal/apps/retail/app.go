package retail

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
)

// App implements the retail analytics application (TPC-DS based).
type App struct {
	executor *QueryExecutor
}

// New creates a new retail application.
func New() *App {
	return &App{}
}

// Name returns the application name.
func (a *App) Name() string {
	return "retail"
}

// Description returns a human-readable description.
func (a *App) Description() string {
	return "Retail analytics (TPC-DS based) - Decision support workload with " +
		"multi-channel sales analysis across store, web, and catalog channels"
}

// WorkloadType returns the workload type.
func (a *App) WorkloadType() string {
	return "Decision Support"
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
			Name:        "store_sales_by_date",
			Description: "Aggregate store sales by date dimension",
			Weight:      15,
			Type:        "read",
		},
		{
			Name:        "store_sales_by_item",
			Description: "Top selling items analysis",
			Weight:      12,
			Type:        "read",
		},
		{
			Name:        "store_sales_by_customer",
			Description: "Customer purchase patterns",
			Weight:      10,
			Type:        "read",
		},
		{
			Name:        "web_sales_analysis",
			Description: "Web channel performance analysis",
			Weight:      12,
			Type:        "read",
		},
		{
			Name:        "catalog_sales_analysis",
			Description: "Catalog channel performance analysis",
			Weight:      10,
			Type:        "read",
		},
		{
			Name:        "cross_channel_sales",
			Description: "Compare sales across all channels",
			Weight:      8,
			Type:        "read",
		},
		{
			Name:        "customer_demographics",
			Description: "Customer demographics analysis",
			Weight:      8,
			Type:        "read",
		},
		{
			Name:        "promotion_effect",
			Description: "Promotion effectiveness analysis",
			Weight:      7,
			Type:        "read",
		},
		{
			Name:        "inventory_analysis",
			Description: "Warehouse inventory analysis",
			Weight:      6,
			Type:        "read",
		},
		{
			Name:        "store_comparison",
			Description: "Compare performance across stores",
			Weight:      6,
			Type:        "read",
		},
		{
			Name:        "time_series_sales",
			Description: "Time series sales patterns",
			Weight:      6,
			Type:        "read",
		},
	}
}

// ExecuteQuery executes a randomly selected query based on the query mix.
func (a *App) ExecuteQuery(ctx context.Context, pool *pgxpool.Pool) apps.QueryResult {
	// Initialize executor if needed (lazy initialization to get counts)
	if a.executor == nil {
		numItems, numCustomers, numStores := a.getTableCounts(ctx, pool)
		a.executor = NewQueryExecutor(numItems, numCustomers, numStores)
	}
	return a.executor.ExecuteRandomQuery(ctx, pool)
}

// ExecuteQueryConn executes a randomly selected query using a single connection.
func (a *App) ExecuteQueryConn(ctx context.Context, conn *pgx.Conn) apps.QueryResult {
	// Initialize executor if needed (lazy initialization to get counts)
	if a.executor == nil {
		numItems, numCustomers, numStores := a.getTableCountsConn(ctx, conn)
		a.executor = NewQueryExecutor(numItems, numCustomers, numStores)
	}
	return a.executor.ExecuteRandomQuery(ctx, conn)
}

// RequiresPgvector returns true if the app needs pgvector extension.
func (a *App) RequiresPgvector() bool {
	return false
}

func (a *App) getTableCounts(ctx context.Context, pool *pgxpool.Pool) (int, int, int) {
	var numItems, numCustomers, numStores int

	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM item").Scan(&numItems)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM customer").Scan(&numCustomers)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM store").Scan(&numStores)

	return max(1, numItems), max(1, numCustomers), max(1, numStores)
}

func (a *App) getTableCountsConn(ctx context.Context, conn *pgx.Conn) (int, int, int) {
	var numItems, numCustomers, numStores int

	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM item").Scan(&numItems)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM customer").Scan(&numCustomers)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM store").Scan(&numStores)

	return max(1, numItems), max(1, numCustomers), max(1, numStores)
}

func init() {
	apps.Register(New())
}
