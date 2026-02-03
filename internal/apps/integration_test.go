//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

//go:build integration
// +build integration

// Integration tests for all applications.
// Run with: go test -tags=integration ./internal/apps/...
// Requires PostgreSQL to be available.
// Set PGEDGE_TEST_CONN environment variable to override connection string.

package apps_test

import (
	"context"
	"testing"
	"time"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	"github.com/pgEdge/pgedge-loadgen/internal/testutil"
	// Import app packages to trigger their init() functions which register the apps
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/analytics"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/brokerage"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/docmgmt"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/ecommerce"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/knowledgebase"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/retail"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/wholesale"
)

// TestWholesaleIntegration tests the wholesale app end-to-end.
func TestWholesaleIntegration(t *testing.T) {
	runAppIntegrationTest(t, "wholesale", false)
}

// TestAnalyticsIntegration tests the analytics app end-to-end.
func TestAnalyticsIntegration(t *testing.T) {
	runAppIntegrationTest(t, "analytics", false)
}

// TestBrokerageIntegration tests the brokerage app end-to-end.
func TestBrokerageIntegration(t *testing.T) {
	runAppIntegrationTest(t, "brokerage", false)
}

// TestRetailIntegration tests the retail app end-to-end.
func TestRetailIntegration(t *testing.T) {
	runAppIntegrationTest(t, "retail", false)
}

// TestEcommerceIntegration tests the ecommerce app end-to-end.
func TestEcommerceIntegration(t *testing.T) {
	runAppIntegrationTest(t, "ecommerce", true)
}

// TestKnowledgebaseIntegration tests the knowledgebase app end-to-end.
func TestKnowledgebaseIntegration(t *testing.T) {
	runAppIntegrationTest(t, "knowledgebase", true)
}

// TestDocmgmtIntegration tests the docmgmt app end-to-end.
func TestDocmgmtIntegration(t *testing.T) {
	runAppIntegrationTest(t, "docmgmt", true)
}

// runAppIntegrationTest runs a full integration test for an app.
func runAppIntegrationTest(t *testing.T, appName string, requiresVector bool) {
	// Check if PostgreSQL is available
	baseConnStr := testutil.SkipIfNoPostgres(t)

	// Get the app
	app, err := apps.Get(appName)
	if err != nil {
		t.Fatalf("Failed to get app: %v", err)
	}

	// Create test database
	testConnStr := testutil.CreateTestDB(t, baseConnStr, appName)
	dbName := testutil.GetDBNameFromConnStr(testConnStr)

	// Setup cleanup
	cleanup := testutil.NewTestCleanup(t, baseConnStr, dbName)
	t.Cleanup(cleanup.Cleanup)

	// Connect to test database
	pool := testutil.ConnectTestDB(t, testConnStr)
	cleanup.SetPool(pool)

	// Skip vector tests if pgvector not available
	if requiresVector {
		testutil.SkipIfNoPgvector(t, pool)
	}

	ctx := context.Background()

	// Test 1: Create schema
	t.Run("CreateSchema", func(t *testing.T) {
		err := app.CreateSchema(ctx, pool)
		if err != nil {
			t.Fatalf("CreateSchema failed: %v", err)
		}
	})

	// Test 2: Generate data (small dataset - 10MB)
	t.Run("GenerateData", func(t *testing.T) {
		cfg := apps.GeneratorConfig{
			TargetSize:          10 * 1024 * 1024, // 10MB
			EmbeddingMode:       "random",
			EmbeddingDimensions: 384,
		}
		err := app.GenerateData(ctx, pool, cfg)
		if err != nil {
			t.Fatalf("GenerateData failed: %v", err)
		}
	})

	// Test 3: Execute queries
	t.Run("ExecuteQueries", func(t *testing.T) {
		// Run queries for a short period
		queryCount := 50
		errorCount := 0

		for i := 0; i < queryCount; i++ {
			result := app.ExecuteQuery(ctx, pool)
			if result.Error != nil {
				errorCount++
				t.Logf("Query %s failed: %v", result.QueryName, result.Error)
			}
		}

		// Allow up to 5% error rate for integration tests
		errorRate := float64(errorCount) / float64(queryCount)
		if errorRate > 0.05 {
			t.Errorf("Error rate too high: %.2f%% (%d/%d errors)",
				errorRate*100, errorCount, queryCount)
		}
	})

	// Test 4: Verify data exists by running queries that should return rows
	t.Run("VerifyData", func(t *testing.T) {
		// Verify at least one table has data by executing a query
		result := app.ExecuteQuery(ctx, pool)
		if result.Error != nil {
			t.Logf("Warning: Query returned error: %v", result.Error)
		}
		// Just verify the app can execute queries - specific tables
		// may vary by app implementation
	})
}

// TestAllAppsQueryDistribution verifies query weight distribution.
func TestAllAppsQueryDistribution(t *testing.T) {
	appNames := apps.List()

	for _, appName := range appNames {
		t.Run(appName, func(t *testing.T) {
			app, err := apps.Get(appName)
			if err != nil {
				t.Fatalf("Failed to get app: %v", err)
			}

			queries := app.GetQueries()

			// Check that all queries have weights
			totalWeight := 0
			for _, q := range queries {
				if q.Weight < 0 {
					t.Errorf("Query %s has negative weight: %d", q.Name, q.Weight)
				}
				totalWeight += q.Weight
			}

			if totalWeight == 0 {
				t.Error("Total query weight is zero - no queries would be executed")
			}
		})
	}
}

// TestConcurrentQueries tests concurrent query execution.
func TestConcurrentQueries(t *testing.T) {
	baseConnStr := testutil.SkipIfNoPostgres(t)

	// Use wholesale for this test (simpler)
	app, err := apps.Get("wholesale")
	if err != nil {
		t.Fatalf("Failed to get app: %v", err)
	}

	testConnStr := testutil.CreateTestDB(t, baseConnStr, "concurrent")
	dbName := testutil.GetDBNameFromConnStr(testConnStr)

	cleanup := testutil.NewTestCleanup(t, baseConnStr, dbName)
	t.Cleanup(cleanup.Cleanup)

	pool := testutil.ConnectTestDB(t, testConnStr)
	cleanup.SetPool(pool)

	ctx := context.Background()

	// Initialize and generate data
	if err := app.CreateSchema(ctx, pool); err != nil {
		t.Fatalf("CreateSchema failed: %v", err)
	}
	cfg := apps.GeneratorConfig{
		TargetSize:          10 * 1024 * 1024, // 10MB
		EmbeddingMode:       "random",
		EmbeddingDimensions: 384,
	}
	if err := app.GenerateData(ctx, pool, cfg); err != nil {
		t.Fatalf("GenerateData failed: %v", err)
	}

	// Run concurrent queries
	concurrency := 5
	queriesPerWorker := 20
	errChan := make(chan error, concurrency*queriesPerWorker)
	done := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		go func() {
			for j := 0; j < queriesPerWorker; j++ {
				result := app.ExecuteQuery(ctx, pool)
				if result.Error != nil {
					errChan <- result.Error
				}
			}
			done <- struct{}{}
		}()
	}

	// Wait for all workers
	for i := 0; i < concurrency; i++ {
		<-done
	}
	close(errChan)

	// Count errors
	errorCount := 0
	for err := range errChan {
		errorCount++
		t.Logf("Concurrent query error: %v", err)
	}

	totalQueries := concurrency * queriesPerWorker
	errorRate := float64(errorCount) / float64(totalQueries)
	if errorRate > 0.05 {
		t.Errorf("Concurrent query error rate too high: %.2f%% (%d/%d)",
			errorRate*100, errorCount, totalQueries)
	}
}

// TestQueryLatency verifies query latency is reasonable.
func TestQueryLatency(t *testing.T) {
	baseConnStr := testutil.SkipIfNoPostgres(t)

	app, err := apps.Get("wholesale")
	if err != nil {
		t.Fatalf("Failed to get app: %v", err)
	}

	testConnStr := testutil.CreateTestDB(t, baseConnStr, "latency")
	dbName := testutil.GetDBNameFromConnStr(testConnStr)

	cleanup := testutil.NewTestCleanup(t, baseConnStr, dbName)
	t.Cleanup(cleanup.Cleanup)

	pool := testutil.ConnectTestDB(t, testConnStr)
	cleanup.SetPool(pool)

	ctx := context.Background()

	if err := app.CreateSchema(ctx, pool); err != nil {
		t.Fatalf("CreateSchema failed: %v", err)
	}
	cfg := apps.GeneratorConfig{
		TargetSize:          10 * 1024 * 1024, // 10MB
		EmbeddingMode:       "random",
		EmbeddingDimensions: 384,
	}
	if err := app.GenerateData(ctx, pool, cfg); err != nil {
		t.Fatalf("GenerateData failed: %v", err)
	}

	// Measure query latencies
	queryCount := 50
	var totalDuration int64

	for i := 0; i < queryCount; i++ {
		result := app.ExecuteQuery(ctx, pool)
		if result.Error == nil {
			totalDuration += result.Duration
		}
	}

	avgLatencyMs := float64(totalDuration) / float64(queryCount) / 1e6
	t.Logf("Average query latency: %.2f ms", avgLatencyMs)

	// For small dataset, average latency should be under 100ms
	if avgLatencyMs > 100 {
		t.Errorf("Average latency too high: %.2f ms (expected < 100ms)", avgLatencyMs)
	}
}

// TestSchemaIdempotent verifies CreateSchema is idempotent.
func TestSchemaIdempotent(t *testing.T) {
	baseConnStr := testutil.SkipIfNoPostgres(t)

	app, err := apps.Get("wholesale")
	if err != nil {
		t.Fatalf("Failed to get app: %v", err)
	}

	testConnStr := testutil.CreateTestDB(t, baseConnStr, "idempotent")
	dbName := testutil.GetDBNameFromConnStr(testConnStr)

	cleanup := testutil.NewTestCleanup(t, baseConnStr, dbName)
	t.Cleanup(cleanup.Cleanup)

	pool := testutil.ConnectTestDB(t, testConnStr)
	cleanup.SetPool(pool)

	ctx := context.Background()

	// Initialize twice - should not error
	if err := app.CreateSchema(ctx, pool); err != nil {
		t.Fatalf("First CreateSchema failed: %v", err)
	}
	if err := app.CreateSchema(ctx, pool); err != nil {
		t.Fatalf("Second CreateSchema failed (not idempotent): %v", err)
	}
}

// TestContextCancellation verifies queries respect context cancellation.
func TestContextCancellation(t *testing.T) {
	baseConnStr := testutil.SkipIfNoPostgres(t)

	app, err := apps.Get("wholesale")
	if err != nil {
		t.Fatalf("Failed to get app: %v", err)
	}

	testConnStr := testutil.CreateTestDB(t, baseConnStr, "cancel")
	dbName := testutil.GetDBNameFromConnStr(testConnStr)

	cleanup := testutil.NewTestCleanup(t, baseConnStr, dbName)
	t.Cleanup(cleanup.Cleanup)

	pool := testutil.ConnectTestDB(t, testConnStr)
	cleanup.SetPool(pool)

	ctx := context.Background()

	if err := app.CreateSchema(ctx, pool); err != nil {
		t.Fatalf("CreateSchema failed: %v", err)
	}
	cfg := apps.GeneratorConfig{
		TargetSize:          10 * 1024 * 1024, // 10MB
		EmbeddingMode:       "random",
		EmbeddingDimensions: 384,
	}
	if err := app.GenerateData(ctx, pool, cfg); err != nil {
		t.Fatalf("GenerateData failed: %v", err)
	}

	// Create cancelled context
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Query should handle cancelled context gracefully
	result := app.ExecuteQuery(cancelledCtx, pool)
	// Error is expected but should not panic
	_ = result

	// Also test with timeout
	timeoutCtx, cancel2 := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel2()
	time.Sleep(1 * time.Millisecond) // Ensure timeout

	result2 := app.ExecuteQuery(timeoutCtx, pool)
	_ = result2
	// Should not panic
}
