//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Portions copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

package knowledgebase

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen/embeddings"
)

// App implements the knowledge base application with semantic search.
type App struct {
	executor *QueryExecutor
	embedder embeddings.Embedder
}

// New creates a new knowledge base application.
func New() *App {
	return &App{}
}

// Name returns the application name.
func (a *App) Name() string {
	return "knowledgebase"
}

// Description returns a human-readable description.
func (a *App) Description() string {
	return "Knowledge Base with semantic search - FAQ/documentation system " +
		"using pgvector for intelligent article matching and search"
}

// WorkloadType returns the workload type.
func (a *App) WorkloadType() string {
	return "Hybrid (Vector + OLTP)"
}

// CreateSchema creates the application's database schema.
func (a *App) CreateSchema(ctx context.Context, pool *pgxpool.Pool) error {
	dimensions := 384 // Default dimensions
	if a.embedder != nil {
		dimensions = a.embedder.Dimensions()
	}
	return CreateSchema(ctx, pool, dimensions)
}

// DropSchema drops the application's database schema.
func (a *App) DropSchema(ctx context.Context, pool *pgxpool.Pool) error {
	return DropSchema(ctx, pool)
}

// GenerateData generates test data for the application.
func (a *App) GenerateData(ctx context.Context, pool *pgxpool.Pool, cfg apps.GeneratorConfig) error {
	embedder := embeddings.NewEmbedder(embeddings.Config{
		Mode:          cfg.EmbeddingMode,
		Dimensions:    cfg.EmbeddingDimensions,
		OpenAIAPIKey:  cfg.OpenAIAPIKey,
		VectorizerURL: cfg.VectorizerURL,
	})
	a.embedder = embedder

	gen := NewGenerator(embedder, cfg.EmbeddingDimensions)
	return gen.GenerateData(ctx, pool, cfg.TargetSize)
}

// GetQueries returns the available queries for this application.
func (a *App) GetQueries() []apps.QueryDefinition {
	return []apps.QueryDefinition{
		{
			Name:        "semantic_search",
			Description: "Vector similarity search for relevant articles",
			Weight:      40,
			Type:        "read",
		},
		{
			Name:        "similar_questions",
			Description: "Find articles matching previous search queries",
			Weight:      20,
			Type:        "read",
		},
		{
			Name:        "browse_category",
			Description: "Traditional category-based article browsing",
			Weight:      15,
			Type:        "read",
		},
		{
			Name:        "view_article",
			Description: "Read full article content with sections",
			Weight:      10,
			Type:        "read",
		},
		{
			Name:        "submit_feedback",
			Description: "Rate article helpfulness",
			Weight:      10,
			Type:        "write",
		},
		{
			Name:        "admin_update",
			Description: "Update article content and metadata",
			Weight:      5,
			Type:        "write",
		},
	}
}

// ExecuteQuery executes a randomly selected query based on the query mix.
func (a *App) ExecuteQuery(ctx context.Context, pool *pgxpool.Pool) apps.QueryResult {
	// Initialize executor if needed (lazy initialization to get counts)
	if a.executor == nil {
		numArticles, numUsers, numCategories, numSearches := a.getTableCounts(ctx, pool)

		// Initialize embedder with default if not set
		if a.embedder == nil {
			a.embedder = embeddings.NewEmbedder(embeddings.Config{
				Mode:       "random",
				Dimensions: 384,
			})
		}

		a.executor = NewQueryExecutor(a.embedder, numArticles, numUsers, numCategories, numSearches)
	}
	return a.executor.ExecuteRandomQuery(ctx, pool)
}

// ExecuteQueryConn executes a randomly selected query using a single connection.
func (a *App) ExecuteQueryConn(ctx context.Context, conn *pgx.Conn) apps.QueryResult {
	// Initialize executor if needed (lazy initialization to get counts)
	if a.executor == nil {
		numArticles, numUsers, numCategories, numSearches := a.getTableCountsConn(ctx, conn)

		// Initialize embedder with default if not set
		if a.embedder == nil {
			a.embedder = embeddings.NewEmbedder(embeddings.Config{
				Mode:       "random",
				Dimensions: 384,
			})
		}

		a.executor = NewQueryExecutor(a.embedder, numArticles, numUsers, numCategories, numSearches)
	}
	return a.executor.ExecuteRandomQuery(ctx, conn)
}

// RequiresPgvector returns true if the app needs pgvector extension.
func (a *App) RequiresPgvector() bool {
	return true
}

func (a *App) getTableCounts(ctx context.Context, pool *pgxpool.Pool) (int, int, int, int) {
	var numArticles, numUsers, numCategories, numSearches int

	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM article").Scan(&numArticles)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM kb_user").Scan(&numUsers)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM category").Scan(&numCategories)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM search_log").Scan(&numSearches)

	return max(1, numArticles), max(1, numUsers), max(1, numCategories), max(1, numSearches)
}

func (a *App) getTableCountsConn(ctx context.Context, conn *pgx.Conn) (int, int, int, int) {
	var numArticles, numUsers, numCategories, numSearches int

	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM article").Scan(&numArticles)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM kb_user").Scan(&numUsers)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM category").Scan(&numCategories)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM search_log").Scan(&numSearches)

	return max(1, numArticles), max(1, numUsers), max(1, numCategories), max(1, numSearches)
}

func init() {
	apps.Register(New())
}
