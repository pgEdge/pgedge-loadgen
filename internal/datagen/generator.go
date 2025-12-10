// Package datagen provides data generation utilities for pgedge-loadgen.
package datagen

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/logging"
)

// Generator is the interface for data generators.
type Generator interface {
	// Generate produces data for the target size.
	Generate(ctx context.Context, pool *pgxpool.Pool, targetSize int64) error
}

// BatchInsertConfig configures batch insert behavior.
type BatchInsertConfig struct {
	// BatchSize is the number of rows per batch insert.
	BatchSize int

	// ProgressInterval is how often to log progress (in rows).
	ProgressInterval int64
}

// DefaultBatchConfig returns default batch insert configuration.
func DefaultBatchConfig() BatchInsertConfig {
	return BatchInsertConfig{
		BatchSize:        1000,
		ProgressInterval: 100000,
	}
}

// ProgressReporter tracks and reports data generation progress.
type ProgressReporter struct {
	tableName        string
	totalRows        int64
	currentRow       int64
	progressInterval int64
}

// NewProgressReporter creates a new progress reporter.
func NewProgressReporter(tableName string, totalRows int64, interval int64) *ProgressReporter {
	return &ProgressReporter{
		tableName:        tableName,
		totalRows:        totalRows,
		progressInterval: interval,
	}
}

// Update updates the progress and logs if necessary.
func (p *ProgressReporter) Update(rowsInserted int64) {
	oldRow := p.currentRow
	p.currentRow += rowsInserted

	// Check if we crossed a progress interval
	if p.currentRow/p.progressInterval > oldRow/p.progressInterval {
		pct := float64(p.currentRow) / float64(p.totalRows) * 100
		logging.Info().
			Str("table", p.tableName).
			Int64("rows", p.currentRow).
			Int64("total", p.totalRows).
			Float64("percent", pct).
			Msg("Generating data")
	}
}

// Done logs completion.
func (p *ProgressReporter) Done() {
	logging.Info().
		Str("table", p.tableName).
		Int64("rows", p.currentRow).
		Msg("Table complete")
}

// SizeCalculator helps calculate row counts based on target size.
type SizeCalculator struct {
	tables []TableSizeInfo
}

// TableSizeInfo holds size information for a table.
type TableSizeInfo struct {
	Name        string
	BaseRowSize int64   // Average row size in bytes
	ScaleRatio  float64 // Ratio relative to base table
	IndexFactor float64 // Estimated index overhead (e.g., 1.3 = 30% overhead)
}

// NewSizeCalculator creates a new size calculator.
func NewSizeCalculator(tables []TableSizeInfo) *SizeCalculator {
	return &SizeCalculator{tables: tables}
}

// CalculateRowCounts calculates row counts for each table given a target size.
func (c *SizeCalculator) CalculateRowCounts(targetSize int64) map[string]int64 {
	// Calculate total size per scale unit
	var sizePerUnit float64
	for _, t := range c.tables {
		// Size per table = base_row_size * scale_ratio * index_factor
		indexFactor := t.IndexFactor
		if indexFactor == 0 {
			indexFactor = 1.3 // Default 30% index overhead
		}
		sizePerUnit += float64(t.BaseRowSize) * t.ScaleRatio * indexFactor
	}

	if sizePerUnit == 0 {
		return make(map[string]int64)
	}

	// Calculate scale factor
	scaleFactor := float64(targetSize) / sizePerUnit

	// Calculate row counts
	rowCounts := make(map[string]int64)
	for _, t := range c.tables {
		rows := int64(scaleFactor * t.ScaleRatio)
		if rows < 1 {
			rows = 1
		}
		rowCounts[t.Name] = rows
	}

	return rowCounts
}

// EstimatedSize returns the estimated size for given row counts.
func (c *SizeCalculator) EstimatedSize(rowCounts map[string]int64) int64 {
	var total int64
	for _, t := range c.tables {
		rows := rowCounts[t.Name]
		indexFactor := t.IndexFactor
		if indexFactor == 0 {
			indexFactor = 1.3
		}
		total += int64(float64(rows) * float64(t.BaseRowSize) * indexFactor)
	}
	return total
}

// FormatSize formats a byte count as a human-readable string.
func FormatSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/float64(TB))
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
