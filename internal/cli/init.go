package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	"github.com/pgEdge/pgedge-loadgen/internal/db"
	"github.com/pgEdge/pgedge-loadgen/internal/logging"
)

var (
	initSize                string
	initEmbeddingMode       string
	initEmbeddingDimensions int
	initVectorizerURL       string
	initOpenAIAPIKey        string
	initDropExisting        bool
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a database with schema and test data",
	Long: `Initialize a PostgreSQL database with the schema and test data
for the specified application. The target size parameter controls how
much data is generated.

Example:
  pgedge-loadgen init --app wholesale --size 5GB --connection "postgres://..."`,
	RunE: runInit,
}

func init() {
	initCmd.Flags().StringVar(&initSize, "size", "",
		"target database size (e.g., 1GB, 500MB)")
	initCmd.Flags().StringVar(&initEmbeddingMode, "embedding-mode", "",
		"embedding generation mode: random, openai, sentence, vectorizer")
	initCmd.Flags().IntVar(&initEmbeddingDimensions, "embedding-dimensions", 0,
		"embedding vector dimensions (default: 384)")
	initCmd.Flags().StringVar(&initVectorizerURL, "vectorizer-url", "",
		"URL for pgedge-vectorizer service")
	initCmd.Flags().StringVar(&initOpenAIAPIKey, "openai-api-key", "",
		"OpenAI API key for embeddings")
	initCmd.Flags().BoolVar(&initDropExisting, "drop-existing", false,
		"drop existing schema before initialization")
}

func runInit(cmd *cobra.Command, args []string) error {
	// Override config with CLI flags
	if initSize != "" {
		cfg.Init.Size = initSize
	}
	if initEmbeddingMode != "" {
		cfg.Init.EmbeddingMode = initEmbeddingMode
	}
	if initEmbeddingDimensions > 0 {
		cfg.Init.EmbeddingDimensions = initEmbeddingDimensions
	}
	if initVectorizerURL != "" {
		cfg.Init.VectorizerURL = initVectorizerURL
	}
	if initOpenAIAPIKey != "" {
		cfg.Init.OpenAIAPIKey = initOpenAIAPIKey
	}
	if initDropExisting {
		cfg.Init.DropExisting = true
	}

	// Validate configuration
	if err := cfg.ValidateInit(); err != nil {
		return err
	}

	// Get the application
	application, err := apps.Get(cfg.App)
	if err != nil {
		return err
	}

	logging.Info().
		Str("app", cfg.App).
		Str("size", cfg.Init.Size).
		Msg("Initializing database")

	// Connect to database
	ctx := context.Background()
	pool, err := db.Connect(ctx, cfg.Connection)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	// Check if already initialized for a different app
	existingApp, err := db.GetMetadataValue(ctx, pool, "app")
	if err == nil && existingApp != "" {
		if existingApp != cfg.App {
			if !cfg.Init.DropExisting {
				return fmt.Errorf(
					"database was initialized for '%s' but '%s' was specified; "+
						"use --drop-existing to reinitialize",
					existingApp, cfg.App)
			}
			logging.Warn().
				Str("existing_app", existingApp).
				Str("new_app", cfg.App).
				Msg("Dropping existing schema")
		}
	}

	// Drop existing schema if requested
	if cfg.Init.DropExisting {
		logging.Info().Msg("Dropping existing schema")
		if err := application.DropSchema(ctx, pool); err != nil {
			return fmt.Errorf("failed to drop schema: %w", err)
		}
		if err := db.DropMetadata(ctx, pool); err != nil {
			logging.Debug().Err(err).Msg("No metadata table to drop")
		}
	}

	// Create schema
	logging.Info().Msg("Creating schema")
	if err := application.CreateSchema(ctx, pool); err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// Parse target size
	targetBytes, err := parseSize(cfg.Init.Size)
	if err != nil {
		return fmt.Errorf("invalid size: %w", err)
	}

	// Generate data
	logging.Info().
		Int64("target_bytes", targetBytes).
		Msg("Generating test data")

	genCfg := apps.GeneratorConfig{
		TargetSize:          targetBytes,
		EmbeddingMode:       cfg.Init.EmbeddingMode,
		EmbeddingDimensions: cfg.Init.EmbeddingDimensions,
		VectorizerURL:       cfg.Init.VectorizerURL,
		OpenAIAPIKey:        cfg.Init.OpenAIAPIKey,
	}

	if err := application.GenerateData(ctx, pool, genCfg); err != nil {
		return fmt.Errorf("failed to generate data: %w", err)
	}

	// Save metadata
	if err := db.SaveMetadata(ctx, pool, cfg.App, cfg.Init.Size); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	logging.Info().
		Str("app", cfg.App).
		Str("size", cfg.Init.Size).
		Msg("Database initialization complete")

	return nil
}

// parseSize converts a size string (e.g., "5GB", "500MB") to bytes.
func parseSize(s string) (int64, error) {
	var value float64
	var unit string

	_, err := fmt.Sscanf(s, "%f%s", &value, &unit)
	if err != nil {
		return 0, fmt.Errorf("invalid size format: %s", s)
	}

	var multiplier int64
	switch unit {
	case "B", "b":
		multiplier = 1
	case "KB", "kb", "K", "k":
		multiplier = 1024
	case "MB", "mb", "M", "m":
		multiplier = 1024 * 1024
	case "GB", "gb", "G", "g":
		multiplier = 1024 * 1024 * 1024
	case "TB", "tb", "T", "t":
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown size unit: %s", unit)
	}

	return int64(value * float64(multiplier)), nil
}
