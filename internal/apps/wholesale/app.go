package wholesale

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
)

// App implements the wholesale supplier application (TPC-C based).
type App struct {
	executor *QueryExecutor
}

// New creates a new wholesale application.
func New() *App {
	return &App{}
}

// Name returns the application name.
func (a *App) Name() string {
	return "wholesale"
}

// Description returns a human-readable description.
func (a *App) Description() string {
	return "Wholesale supplier (TPC-C based) - OLTP workload with warehouses, " +
		"districts, customers, orders, and inventory management"
}

// WorkloadType returns the workload type.
func (a *App) WorkloadType() string {
	return "OLTP"
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
			Name:        "new_order",
			Description: "Create new customer orders with multiple line items",
			Weight:      45,
			Type:        "write",
		},
		{
			Name:        "payment",
			Description: "Process customer payments",
			Weight:      43,
			Type:        "write",
		},
		{
			Name:        "order_status",
			Description: "Check order status for a customer",
			Weight:      4,
			Type:        "read",
		},
		{
			Name:        "delivery",
			Description: "Process deliveries for orders",
			Weight:      4,
			Type:        "write",
		},
		{
			Name:        "stock_level",
			Description: "Check inventory levels below threshold",
			Weight:      4,
			Type:        "read",
		},
	}
}

// ExecuteQuery executes a randomly selected query based on the query mix.
func (a *App) ExecuteQuery(ctx context.Context, pool *pgxpool.Pool) apps.QueryResult {
	// Initialize executor if needed (lazy initialization to get warehouse count)
	if a.executor == nil {
		numWarehouses := a.getWarehouseCount(ctx, pool)
		a.executor = NewQueryExecutor(numWarehouses)
	}
	return a.executor.ExecuteRandomQuery(ctx, pool)
}

// ExecuteQueryConn executes a randomly selected query using a single connection.
func (a *App) ExecuteQueryConn(ctx context.Context, conn *pgx.Conn) apps.QueryResult {
	// Initialize executor if needed (lazy initialization to get warehouse count)
	if a.executor == nil {
		numWarehouses := a.getWarehouseCountConn(ctx, conn)
		a.executor = NewQueryExecutor(numWarehouses)
	}
	return a.executor.ExecuteRandomQuery(ctx, conn)
}

// RequiresPgvector returns true if the app needs pgvector extension.
func (a *App) RequiresPgvector() bool {
	return false
}

func (a *App) getWarehouseCount(ctx context.Context, pool *pgxpool.Pool) int {
	var count int
	err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM warehouse").Scan(&count)
	if err != nil {
		return 1
	}
	return max(1, count)
}

func (a *App) getWarehouseCountConn(ctx context.Context, conn *pgx.Conn) int {
	var count int
	err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM warehouse").Scan(&count)
	if err != nil {
		return 1
	}
	return max(1, count)
}

func init() {
	apps.Register(New())
}
