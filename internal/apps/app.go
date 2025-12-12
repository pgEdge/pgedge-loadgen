// Package apps defines the application interface and implementations.
package apps

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB is an interface that both *pgxpool.Pool and *pgx.Conn satisfy.
// This allows query executors to work with either a connection pool or
// a dedicated single connection.
type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// GeneratorConfig holds configuration for data generation.
type GeneratorConfig struct {
	// TargetSize is the target database size in bytes.
	TargetSize int64

	// EmbeddingMode controls how vector embeddings are generated.
	// Options: random, openai, sentence, vectorizer
	EmbeddingMode string

	// EmbeddingDimensions is the vector dimension size.
	EmbeddingDimensions int

	// VectorizerURL is the URL for pgedge-vectorizer service.
	VectorizerURL string

	// OpenAIAPIKey is the API key for OpenAI embeddings.
	OpenAIAPIKey string
}

// QueryResult holds the result of a query execution.
type QueryResult struct {
	// QueryName identifies the query type.
	QueryName string

	// Duration is how long the query took.
	Duration int64

	// RowsAffected is the number of rows affected (for DML).
	RowsAffected int64

	// Error is set if the query failed.
	Error error
}

// App defines the interface that all applications must implement.
type App interface {
	// Name returns the application name.
	Name() string

	// Description returns a human-readable description.
	Description() string

	// WorkloadType returns the workload type (OLTP, OLAP, Mixed).
	WorkloadType() string

	// CreateSchema creates the application's database schema.
	CreateSchema(ctx context.Context, pool *pgxpool.Pool) error

	// DropSchema drops the application's database schema.
	DropSchema(ctx context.Context, pool *pgxpool.Pool) error

	// GenerateData generates test data for the application.
	GenerateData(ctx context.Context, pool *pgxpool.Pool, cfg GeneratorConfig) error

	// GetQueries returns the available queries for this application.
	GetQueries() []QueryDefinition

	// ExecuteQuery executes a randomly selected query based on the query mix.
	ExecuteQuery(ctx context.Context, pool *pgxpool.Pool) QueryResult

	// ExecuteQueryConn executes a randomly selected query using a single connection.
	ExecuteQueryConn(ctx context.Context, conn *pgx.Conn) QueryResult

	// RequiresPgvector returns true if the app needs pgvector extension.
	RequiresPgvector() bool
}

// QueryDefinition describes a query type in the application's workload.
type QueryDefinition struct {
	// Name is the query identifier.
	Name string

	// Description describes what the query does.
	Description string

	// Weight is the relative frequency of this query (0-100).
	Weight int

	// Type is the query type (read, write, mixed).
	Type string
}

// TableDefinition describes a table in the application's schema.
type TableDefinition struct {
	// Name is the table name.
	Name string

	// BaseRowSize is the estimated average row size in bytes.
	BaseRowSize int64

	// ScaleRatio determines how row count scales relative to base.
	// For example, if the base table has 1000 rows at scale 1,
	// a table with ScaleRatio 10 would have 10000 rows.
	ScaleRatio float64
}
