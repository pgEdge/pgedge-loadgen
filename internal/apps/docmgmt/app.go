package docmgmt

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen/embeddings"
)

// App implements the document management application with semantic search.
type App struct {
	executor *QueryExecutor
	embedder embeddings.Embedder
}

// New creates a new document management application.
func New() *App {
	return &App{}
}

// Name returns the application name.
func (a *App) Name() string {
	return "docmgmt"
}

// Description returns a human-readable description.
func (a *App) Description() string {
	return "Document Management System with semantic search - enterprise DMS " +
		"using pgvector for intelligent document search and similarity detection"
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
			Description: "Vector similarity search for documents",
			Weight:      35,
			Type:        "read",
		},
		{
			Name:        "find_similar",
			Description: "Find documents similar to a given document",
			Weight:      15,
			Type:        "read",
		},
		{
			Name:        "browse_folder",
			Description: "Traditional folder-based document browsing",
			Weight:      15,
			Type:        "read",
		},
		{
			Name:        "document_retrieve",
			Description: "Full document fetch with metadata",
			Weight:      15,
			Type:        "read",
		},
		{
			Name:        "version_history",
			Description: "Get document version history",
			Weight:      5,
			Type:        "read",
		},
		{
			Name:        "upload_update",
			Description: "Upload new document or update existing",
			Weight:      10,
			Type:        "write",
		},
		{
			Name:        "permission_check",
			Description: "Check user access permissions",
			Weight:      5,
			Type:        "read",
		},
	}
}

// ExecuteQuery executes a randomly selected query based on the query mix.
func (a *App) ExecuteQuery(ctx context.Context, pool *pgxpool.Pool) apps.QueryResult {
	// Initialize executor if needed (lazy initialization to get counts)
	if a.executor == nil {
		numDocuments, numUsers, numFolders, numChunks := a.getTableCounts(ctx, pool)

		// Initialize embedder with default if not set
		if a.embedder == nil {
			a.embedder = embeddings.NewEmbedder(embeddings.Config{
				Mode:       "random",
				Dimensions: 384,
			})
		}

		a.executor = NewQueryExecutor(a.embedder, numDocuments, numUsers, numFolders, numChunks)
	}
	return a.executor.ExecuteRandomQuery(ctx, pool)
}

// ExecuteQueryConn executes a randomly selected query using a single connection.
func (a *App) ExecuteQueryConn(ctx context.Context, conn *pgx.Conn) apps.QueryResult {
	// Initialize executor if needed (lazy initialization to get counts)
	if a.executor == nil {
		numDocuments, numUsers, numFolders, numChunks := a.getTableCountsConn(ctx, conn)

		// Initialize embedder with default if not set
		if a.embedder == nil {
			a.embedder = embeddings.NewEmbedder(embeddings.Config{
				Mode:       "random",
				Dimensions: 384,
			})
		}

		a.executor = NewQueryExecutor(a.embedder, numDocuments, numUsers, numFolders, numChunks)
	}
	return a.executor.ExecuteRandomQuery(ctx, conn)
}

// RequiresPgvector returns true if the app needs pgvector extension.
func (a *App) RequiresPgvector() bool {
	return true
}

func (a *App) getTableCounts(ctx context.Context, pool *pgxpool.Pool) (int, int, int, int) {
	var numDocuments, numUsers, numFolders, numChunks int

	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM document").Scan(&numDocuments)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM doc_user").Scan(&numUsers)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM folder").Scan(&numFolders)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM document_chunk").Scan(&numChunks)

	return max(1, numDocuments), max(1, numUsers), max(1, numFolders), max(1, numChunks)
}

func (a *App) getTableCountsConn(ctx context.Context, conn *pgx.Conn) (int, int, int, int) {
	var numDocuments, numUsers, numFolders, numChunks int

	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM document").Scan(&numDocuments)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM doc_user").Scan(&numUsers)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM folder").Scan(&numFolders)
	_ = conn.QueryRow(ctx, "SELECT COUNT(*) FROM document_chunk").Scan(&numChunks)

	return max(1, numDocuments), max(1, numUsers), max(1, numFolders), max(1, numChunks)
}

func init() {
	apps.Register(New())
}
