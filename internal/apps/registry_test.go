//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

package apps_test

import (
	"testing"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	// Import app packages to trigger their init() functions which register the apps
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/analytics"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/brokerage"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/docmgmt"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/ecommerce"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/knowledgebase"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/retail"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/wholesale"
)

func TestGet(t *testing.T) {
	knownApps := []string{
		"wholesale",
		"analytics",
		"brokerage",
		"retail",
		"ecommerce",
		"knowledgebase",
		"docmgmt",
	}

	for _, appName := range knownApps {
		t.Run(appName, func(t *testing.T) {
			app, err := apps.Get(appName)
			if err != nil {
				t.Fatalf("Failed to get app '%s': %v", appName, err)
			}
			if app == nil {
				t.Fatalf("Get('%s') returned nil", appName)
			}

			// Verify app implements the interface
			if app.Name() != appName {
				t.Errorf("App name mismatch: expected '%s', got '%s'", appName, app.Name())
			}
			if app.Description() == "" {
				t.Error("App description should not be empty")
			}
		})
	}
}

func TestGetInvalidApp(t *testing.T) {
	_, err := apps.Get("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent app, got nil")
	}
}

func TestGetEmptyName(t *testing.T) {
	_, err := apps.Get("")
	if err == nil {
		t.Error("Expected error for empty app name, got nil")
	}
}

func TestList(t *testing.T) {
	appList := apps.List()

	if len(appList) == 0 {
		t.Error("List returned empty slice")
	}

	expectedApps := []string{
		"wholesale",
		"analytics",
		"brokerage",
		"retail",
		"ecommerce",
		"knowledgebase",
		"docmgmt",
	}

	for _, expected := range expectedApps {
		found := false
		for _, app := range appList {
			if app == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected app '%s' not found in List()", expected)
		}
	}
}

func TestAppInfo(t *testing.T) {
	appList := apps.List()

	for _, appName := range appList {
		t.Run(appName, func(t *testing.T) {
			app, err := apps.Get(appName)
			if err != nil {
				t.Fatalf("Failed to get app: %v", err)
			}

			// Check required fields
			if app.Name() == "" {
				t.Error("Name() should not be empty")
			}
			if app.Description() == "" {
				t.Error("Description() should not be empty")
			}
			if app.WorkloadType() == "" {
				t.Error("WorkloadType() should not be empty")
			}

			queries := app.GetQueries()
			if len(queries) == 0 {
				t.Error("GetQueries() should not be empty")
			}

			// Check query info
			for _, query := range queries {
				if query.Name == "" {
					t.Error("Query name should not be empty")
				}
				if query.Description == "" {
					t.Errorf("Query '%s' description should not be empty", query.Name)
				}
				if query.Type != "read" && query.Type != "write" && query.Type != "mixed" {
					t.Errorf("Query '%s' type should be 'read', 'write', or 'mixed', got '%s'",
						query.Name, query.Type)
				}
			}
		})
	}
}

func TestAppRequiresVectors(t *testing.T) {
	tests := []struct {
		appName   string
		expectVec bool
	}{
		{"wholesale", false},
		{"analytics", false},
		{"brokerage", false},
		{"retail", false},
		{"ecommerce", true},
		{"knowledgebase", true},
		{"docmgmt", true},
	}

	for _, tt := range tests {
		t.Run(tt.appName, func(t *testing.T) {
			app, err := apps.Get(tt.appName)
			if err != nil {
				t.Fatalf("Failed to get app: %v", err)
			}

			if app.RequiresPgvector() != tt.expectVec {
				t.Errorf("RequiresPgvector for %s: expected %v, got %v",
					tt.appName, tt.expectVec, app.RequiresPgvector())
			}
		})
	}
}

func TestQueryResult(t *testing.T) {
	// Test QueryResult struct
	result := apps.QueryResult{
		QueryName:    "test_query",
		RowsAffected: 10,
		Duration:     1000000, // 1ms in nanoseconds
		Error:        nil,
	}

	if result.QueryName != "test_query" {
		t.Errorf("QueryName mismatch")
	}
	if result.RowsAffected != 10 {
		t.Errorf("RowsAffected mismatch")
	}
	if result.Duration != 1000000 {
		t.Errorf("Duration mismatch")
	}
	if result.Error != nil {
		t.Errorf("Error should be nil")
	}
}

// Benchmark app retrieval
func BenchmarkGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		apps.Get("wholesale")
	}
}

func BenchmarkList(b *testing.B) {
	for i := 0; i < b.N; i++ {
		apps.List()
	}
}
