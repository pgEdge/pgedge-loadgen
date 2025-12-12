package ecommerce

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen/embeddings"
)

// App implements the e-commerce application with semantic search.
type App struct {
	executor   *QueryExecutor
	embedder   embeddings.Embedder
	dimensions int
}

// New creates a new ecommerce application.
func New() *App {
	return &App{
		dimensions: 384, // Default dimensions
	}
}

// Name returns the application name.
func (a *App) Name() string {
	return "ecommerce"
}

// Description returns a human-readable description.
func (a *App) Description() string {
	return "E-commerce with semantic product search (pgvector) - Online store with " +
		"vector similarity search for products and reviews"
}

// WorkloadType returns the workload type.
func (a *App) WorkloadType() string {
	return "Mixed (pgvector)"
}

// CreateSchema creates the application's database schema.
func (a *App) CreateSchema(ctx context.Context, pool *pgxpool.Pool) error {
	return CreateSchema(ctx, pool, a.dimensions)
}

// DropSchema drops the application's database schema.
func (a *App) DropSchema(ctx context.Context, pool *pgxpool.Pool) error {
	return DropSchema(ctx, pool)
}

// GenerateData generates test data for the application.
func (a *App) GenerateData(ctx context.Context, pool *pgxpool.Pool, cfg apps.GeneratorConfig) error {
	// Set up embedder based on config
	embCfg := embeddings.Config{
		Mode:          cfg.EmbeddingMode,
		Dimensions:    cfg.EmbeddingDimensions,
		OpenAIAPIKey:  cfg.OpenAIAPIKey,
		VectorizerURL: cfg.VectorizerURL,
	}
	if embCfg.Mode == "" {
		embCfg.Mode = "random"
	}
	if embCfg.Dimensions == 0 {
		embCfg.Dimensions = 384
	}

	a.dimensions = embCfg.Dimensions
	a.embedder = embeddings.NewEmbedder(embCfg)

	gen := NewGenerator(a.embedder, a.dimensions)
	return gen.GenerateData(ctx, pool, cfg.TargetSize)
}

// GetQueries returns the available queries for this application.
func (a *App) GetQueries() []apps.QueryDefinition {
	return []apps.QueryDefinition{
		{
			Name:        "semantic_search",
			Description: "Vector similarity search for products",
			Weight:      30,
			Type:        "read",
		},
		{
			Name:        "category_browse",
			Description: "Browse products by category",
			Weight:      20,
			Type:        "read",
		},
		{
			Name:        "similar_products",
			Description: "Find similar products using vector similarity",
			Weight:      15,
			Type:        "read",
		},
		{
			Name:        "add_to_cart",
			Description: "Add product to shopping cart",
			Weight:      10,
			Type:        "write",
		},
		{
			Name:        "checkout",
			Description: "Complete order checkout",
			Weight:      5,
			Type:        "write",
		},
		{
			Name:        "submit_review",
			Description: "Submit product review with embedding",
			Weight:      5,
			Type:        "write",
		},
		{
			Name:        "order_history",
			Description: "View customer order history",
			Weight:      10,
			Type:        "read",
		},
		{
			Name:        "inventory_check",
			Description: "Check product inventory levels",
			Weight:      5,
			Type:        "read",
		},
	}
}

// ExecuteQuery executes a randomly selected query based on the query mix.
func (a *App) ExecuteQuery(ctx context.Context, pool *pgxpool.Pool) apps.QueryResult {
	// Initialize executor if needed
	if a.executor == nil {
		// Ensure embedder is set
		if a.embedder == nil {
			a.embedder = embeddings.NewRandomEmbedder(a.dimensions)
		}
		numProducts, numCustomers, numCategories, numOrders := a.getTableCounts(ctx, pool)
		a.executor = NewQueryExecutor(a.embedder, numProducts, numCustomers, numCategories, numOrders)
	}
	return a.executor.ExecuteRandomQuery(ctx, pool)
}

// ExecuteQueryConn executes a randomly selected query using a single connection.
func (a *App) ExecuteQueryConn(ctx context.Context, conn *pgx.Conn) apps.QueryResult {
	// Initialize executor if needed
	if a.executor == nil {
		// Ensure embedder is set
		if a.embedder == nil {
			a.embedder = embeddings.NewRandomEmbedder(a.dimensions)
		}
		numProducts, numCustomers, numCategories, numOrders := a.getTableCountsConn(ctx, conn)
		a.executor = NewQueryExecutor(a.embedder, numProducts, numCustomers, numCategories, numOrders)
	}
	return a.executor.ExecuteRandomQuery(ctx, conn)
}

// RequiresPgvector returns true if the app needs pgvector extension.
func (a *App) RequiresPgvector() bool {
	return true
}

func (a *App) getTableCounts(ctx context.Context, pool *pgxpool.Pool) (int, int, int, int) {
	var numProducts, numCustomers, numCategories, numOrders int

	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM product").Scan(&numProducts)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM customer").Scan(&numCustomers)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM category").Scan(&numCategories)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM orders").Scan(&numOrders)

	return max(1, numProducts), max(1, numCustomers), max(1, numCategories), max(1, numOrders)
}

func (a *App) getTableCountsConn(ctx context.Context, conn *pgx.Conn) (int, int, int, int) {
	var numProducts, numCustomers, numCategories, numOrders int

	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM product").Scan(&numProducts)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM customer").Scan(&numCustomers)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM category").Scan(&numCategories)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM orders").Scan(&numOrders)

	return max(1, numProducts), max(1, numCustomers), max(1, numCategories), max(1, numOrders)
}

func init() {
	apps.Register(New())
}
