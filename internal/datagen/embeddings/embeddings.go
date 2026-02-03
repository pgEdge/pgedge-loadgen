//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

// Package embeddings provides vector embedding generation for pgvector applications.
package embeddings

// Embedder is the interface for generating vector embeddings.
type Embedder interface {
	// Embed generates a vector embedding for the given text.
	Embed(text string) []float32

	// Dimensions returns the dimensionality of generated embeddings.
	Dimensions() int
}

// Config holds configuration for embedding generation.
type Config struct {
	// Mode is the embedding generation mode: random, openai, sentence, vectorizer
	Mode string

	// Dimensions is the embedding vector size.
	Dimensions int

	// OpenAIAPIKey is the API key for OpenAI embeddings.
	OpenAIAPIKey string

	// VectorizerURL is the URL for pgedge-vectorizer service.
	VectorizerURL string
}

// DefaultConfig returns default embedding configuration.
func DefaultConfig() Config {
	return Config{
		Mode:       "random",
		Dimensions: 384,
	}
}

// NewEmbedder creates an Embedder based on the configuration.
func NewEmbedder(cfg Config) Embedder {
	switch cfg.Mode {
	case "openai":
		return NewOpenAIEmbedder(cfg.OpenAIAPIKey, cfg.Dimensions)
	case "vectorizer":
		return NewVectorizerEmbedder(cfg.VectorizerURL, cfg.Dimensions)
	default:
		return NewRandomEmbedder(cfg.Dimensions)
	}
}
