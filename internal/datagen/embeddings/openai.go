//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

package embeddings

import (
	"github.com/pgEdge/pgedge-loadgen/internal/logging"
)

// OpenAIEmbedder generates embeddings using OpenAI's API.
// Falls back to random embeddings if API key is not set.
type OpenAIEmbedder struct {
	apiKey     string
	dimensions int
	fallback   *RandomEmbedder
}

// NewOpenAIEmbedder creates a new OpenAI embedder.
func NewOpenAIEmbedder(apiKey string, dimensions int) *OpenAIEmbedder {
	if apiKey == "" {
		logging.Warn().Msg("OpenAI API key not set, using random embeddings")
	}
	return &OpenAIEmbedder{
		apiKey:     apiKey,
		dimensions: dimensions,
		fallback:   NewRandomEmbedder(dimensions),
	}
}

// Embed generates an embedding using OpenAI's API.
// Falls back to random embeddings if API key is not available.
func (e *OpenAIEmbedder) Embed(text string) []float32 {
	if e.apiKey == "" {
		return e.fallback.Embed(text)
	}

	// TODO: Implement actual OpenAI API call
	// For now, fall back to random embeddings
	// This would use the text-embedding-ada-002 or text-embedding-3-small model
	return e.fallback.Embed(text)
}

// Dimensions returns the dimensionality of generated embeddings.
func (e *OpenAIEmbedder) Dimensions() int {
	return e.dimensions
}
