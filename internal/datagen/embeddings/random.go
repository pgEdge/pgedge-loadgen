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
	"hash/fnv"
	"math"
	"math/rand"
)

// RandomEmbedder generates random vector embeddings.
// Uses deterministic random generation based on text hash for consistency.
type RandomEmbedder struct {
	dimensions int
}

// NewRandomEmbedder creates a new random embedder.
func NewRandomEmbedder(dimensions int) *RandomEmbedder {
	return &RandomEmbedder{
		dimensions: dimensions,
	}
}

// Embed generates a random embedding for the given text.
// The embedding is deterministic based on the text hash.
func (e *RandomEmbedder) Embed(text string) []float32 {
	// Use text hash as seed for reproducibility
	h := fnv.New64a()
	h.Write([]byte(text))
	seed := int64(h.Sum64())

	rng := rand.New(rand.NewSource(seed))

	// Generate random vector
	embedding := make([]float32, e.dimensions)
	var norm float64
	for i := range embedding {
		// Use normal distribution for more realistic embeddings
		embedding[i] = float32(rng.NormFloat64())
		norm += float64(embedding[i] * embedding[i])
	}

	// Normalize to unit length
	norm = math.Sqrt(norm)
	if norm > 0 {
		for i := range embedding {
			embedding[i] /= float32(norm)
		}
	}

	return embedding
}

// Dimensions returns the dimensionality of generated embeddings.
func (e *RandomEmbedder) Dimensions() int {
	return e.dimensions
}
