package embeddings

import (
	"github.com/pgEdge/pgedge-loadgen/internal/logging"
)

// VectorizerEmbedder generates embeddings using pgedge-vectorizer service.
// Falls back to random embeddings if URL is not set.
type VectorizerEmbedder struct {
	url        string
	dimensions int
	fallback   *RandomEmbedder
}

// NewVectorizerEmbedder creates a new vectorizer embedder.
func NewVectorizerEmbedder(url string, dimensions int) *VectorizerEmbedder {
	if url == "" {
		logging.Warn().Msg("Vectorizer URL not set, using random embeddings")
	}
	return &VectorizerEmbedder{
		url:        url,
		dimensions: dimensions,
		fallback:   NewRandomEmbedder(dimensions),
	}
}

// Embed generates an embedding using pgedge-vectorizer service.
// Falls back to random embeddings if URL is not available.
func (e *VectorizerEmbedder) Embed(text string) []float32 {
	if e.url == "" {
		return e.fallback.Embed(text)
	}

	// TODO: Implement actual pgedge-vectorizer API call
	// For now, fall back to random embeddings
	// This would POST to the vectorizer's /embed endpoint
	return e.fallback.Embed(text)
}

// Dimensions returns the dimensionality of generated embeddings.
func (e *VectorizerEmbedder) Dimensions() int {
	return e.dimensions
}
