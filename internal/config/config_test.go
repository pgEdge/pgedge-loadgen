package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg == nil {
		t.Fatal("DefaultConfig returned nil")
	}

	// Check default values
	if cfg.LogLevel != "info" {
		t.Errorf("Expected LogLevel 'info', got '%s'", cfg.LogLevel)
	}

	// Init defaults
	if cfg.Init.Size != "1GB" {
		t.Errorf("Expected Init.Size '1GB', got '%s'", cfg.Init.Size)
	}
	if cfg.Init.EmbeddingMode != "random" {
		t.Errorf("Expected Init.EmbeddingMode 'random', got '%s'", cfg.Init.EmbeddingMode)
	}
	if cfg.Init.EmbeddingDimensions != 384 {
		t.Errorf("Expected Init.EmbeddingDimensions 384, got %d", cfg.Init.EmbeddingDimensions)
	}
	if cfg.Init.DropExisting != false {
		t.Error("Expected Init.DropExisting false")
	}

	// Run defaults
	if cfg.Run.Connections != 10 {
		t.Errorf("Expected Run.Connections 10, got %d", cfg.Run.Connections)
	}
	if cfg.Run.Profile != "local-office" {
		t.Errorf("Expected Run.Profile 'local-office', got '%s'", cfg.Run.Profile)
	}
	if cfg.Run.Timezone != "Local" {
		t.Errorf("Expected Run.Timezone 'Local', got '%s'", cfg.Run.Timezone)
	}
	if cfg.Run.ReportInterval != 60 {
		t.Errorf("Expected Run.ReportInterval 60, got %d", cfg.Run.ReportInterval)
	}
	if cfg.Run.ConnectionMode != "pool" {
		t.Errorf("Expected Run.ConnectionMode 'pool', got '%s'", cfg.Run.ConnectionMode)
	}
	if cfg.Run.SessionMinDuration != 300 {
		t.Errorf("Expected Run.SessionMinDuration 300, got %d", cfg.Run.SessionMinDuration)
	}
	if cfg.Run.SessionMaxDuration != 1800 {
		t.Errorf("Expected Run.SessionMaxDuration 1800, got %d", cfg.Run.SessionMaxDuration)
	}
	if cfg.Run.ThinkTimeMin != 1000 {
		t.Errorf("Expected Run.ThinkTimeMin 1000, got %d", cfg.Run.ThinkTimeMin)
	}
	if cfg.Run.ThinkTimeMax != 5000 {
		t.Errorf("Expected Run.ThinkTimeMax 5000, got %d", cfg.Run.ThinkTimeMax)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *Config
		wantError bool
	}{
		{
			name: "valid config",
			cfg: &Config{
				Connection: "postgres://user:pass@localhost/db",
				App:        "wholesale",
			},
			wantError: false,
		},
		{
			name: "missing connection",
			cfg: &Config{
				App: "wholesale",
			},
			wantError: true,
		},
		{
			name: "missing app",
			cfg: &Config{
				Connection: "postgres://user:pass@localhost/db",
			},
			wantError: true,
		},
		{
			name:      "empty config",
			cfg:       &Config{},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantError && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}
		})
	}
}

func TestConfigValidateInit(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *Config
		wantError bool
	}{
		{
			name: "valid init config",
			cfg: &Config{
				Connection: "postgres://user:pass@localhost/db",
				App:        "wholesale",
				Init: InitConfig{
					Size:                "1GB",
					EmbeddingMode:       "random",
					EmbeddingDimensions: 384,
				},
			},
			wantError: false,
		},
		{
			name: "missing size",
			cfg: &Config{
				Connection: "postgres://user:pass@localhost/db",
				App:        "wholesale",
				Init: InitConfig{
					Size:                "",
					EmbeddingMode:       "random",
					EmbeddingDimensions: 384,
				},
			},
			wantError: true,
		},
		{
			name: "missing connection for init",
			cfg: &Config{
				App: "wholesale",
				Init: InitConfig{
					Size:                "1GB",
					EmbeddingMode:       "random",
					EmbeddingDimensions: 384,
				},
			},
			wantError: true,
		},
		{
			name: "missing app for init",
			cfg: &Config{
				Connection: "postgres://user:pass@localhost/db",
				Init: InitConfig{
					Size:                "1GB",
					EmbeddingMode:       "random",
					EmbeddingDimensions: 384,
				},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.ValidateInit()
			if tt.wantError && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}
		})
	}
}

func TestConfigValidateRun(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *Config
		wantError bool
	}{
		{
			name: "valid run config pool mode",
			cfg: &Config{
				Connection: "postgres://user:pass@localhost/db",
				App:        "wholesale",
				Run: RunConfig{
					Connections:    10,
					ConnectionMode: "pool",
				},
			},
			wantError: false,
		},
		{
			name: "valid run config session mode",
			cfg: &Config{
				Connection: "postgres://user:pass@localhost/db",
				App:        "wholesale",
				Run: RunConfig{
					Connections:        10,
					ConnectionMode:     "session",
					SessionMinDuration: 60,
					SessionMaxDuration: 300,
					ThinkTimeMin:       500,
					ThinkTimeMax:       2000,
				},
			},
			wantError: false,
		},
		{
			name: "zero connections",
			cfg: &Config{
				Connection: "postgres://user:pass@localhost/db",
				App:        "wholesale",
				Run: RunConfig{
					Connections:    0,
					ConnectionMode: "pool",
				},
			},
			wantError: true,
		},
		{
			name: "invalid connection mode",
			cfg: &Config{
				Connection: "postgres://user:pass@localhost/db",
				App:        "wholesale",
				Run: RunConfig{
					Connections:    10,
					ConnectionMode: "invalid",
				},
			},
			wantError: true,
		},
		{
			name: "session mode invalid min duration",
			cfg: &Config{
				Connection: "postgres://user:pass@localhost/db",
				App:        "wholesale",
				Run: RunConfig{
					Connections:        10,
					ConnectionMode:     "session",
					SessionMinDuration: 0,
					SessionMaxDuration: 300,
					ThinkTimeMin:       500,
					ThinkTimeMax:       2000,
				},
			},
			wantError: true,
		},
		{
			name: "session mode max < min duration",
			cfg: &Config{
				Connection: "postgres://user:pass@localhost/db",
				App:        "wholesale",
				Run: RunConfig{
					Connections:        10,
					ConnectionMode:     "session",
					SessionMinDuration: 300,
					SessionMaxDuration: 60,
					ThinkTimeMin:       500,
					ThinkTimeMax:       2000,
				},
			},
			wantError: true,
		},
		{
			name: "session mode negative think time min",
			cfg: &Config{
				Connection: "postgres://user:pass@localhost/db",
				App:        "wholesale",
				Run: RunConfig{
					Connections:        10,
					ConnectionMode:     "session",
					SessionMinDuration: 60,
					SessionMaxDuration: 300,
					ThinkTimeMin:       -1,
					ThinkTimeMax:       2000,
				},
			},
			wantError: true,
		},
		{
			name: "session mode think time max < min",
			cfg: &Config{
				Connection: "postgres://user:pass@localhost/db",
				App:        "wholesale",
				Run: RunConfig{
					Connections:        10,
					ConnectionMode:     "session",
					SessionMinDuration: 60,
					SessionMaxDuration: 300,
					ThinkTimeMin:       2000,
					ThinkTimeMax:       500,
				},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.ValidateRun()
			if tt.wantError && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}
		})
	}
}

func TestLoadConfigFile(t *testing.T) {
	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "pgedge-loadgen.yaml")

	configContent := `
connection: "postgres://testuser:testpass@localhost:5432/testdb"
app: "wholesale"
log_level: "debug"

init:
  size: "5GB"
  embedding_mode: "random"
  embedding_dimensions: 512
  drop_existing: true

run:
  connections: 50
  profile: "global"
  timezone: "UTC"
  report_interval: 30
  duration: 60
  connection_mode: "session"
  session_min_duration: 120
  session_max_duration: 600
  think_time_min: 500
  think_time_max: 3000
`
	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify loaded values
	if cfg.Connection != "postgres://testuser:testpass@localhost:5432/testdb" {
		t.Errorf("Connection mismatch: %s", cfg.Connection)
	}
	if cfg.App != "wholesale" {
		t.Errorf("App mismatch: %s", cfg.App)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel mismatch: %s", cfg.LogLevel)
	}
	if cfg.Init.Size != "5GB" {
		t.Errorf("Init.Size mismatch: %s", cfg.Init.Size)
	}
	if cfg.Init.EmbeddingDimensions != 512 {
		t.Errorf("Init.EmbeddingDimensions mismatch: %d", cfg.Init.EmbeddingDimensions)
	}
	if cfg.Init.DropExisting != true {
		t.Error("Init.DropExisting mismatch")
	}
	if cfg.Run.Connections != 50 {
		t.Errorf("Run.Connections mismatch: %d", cfg.Run.Connections)
	}
	if cfg.Run.Profile != "global" {
		t.Errorf("Run.Profile mismatch: %s", cfg.Run.Profile)
	}
	if cfg.Run.Timezone != "UTC" {
		t.Errorf("Run.Timezone mismatch: %s", cfg.Run.Timezone)
	}
	if cfg.Run.ReportInterval != 30 {
		t.Errorf("Run.ReportInterval mismatch: %d", cfg.Run.ReportInterval)
	}
	if cfg.Run.Duration != 60 {
		t.Errorf("Run.Duration mismatch: %d", cfg.Run.Duration)
	}
	if cfg.Run.ConnectionMode != "session" {
		t.Errorf("Run.ConnectionMode mismatch: %s", cfg.Run.ConnectionMode)
	}
}

func TestLoadConfigFileNotFound(t *testing.T) {
	// When a specific config file is provided but doesn't exist, Load returns an error
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("Load should error when specified config file doesn't exist")
	}
}

func TestLoadConfigDefaultPath(t *testing.T) {
	// When no config file is specified (empty string), Load returns defaults
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load should not error with empty path, got: %v", err)
	}
	if cfg == nil {
		t.Fatal("Load should return default config")
	}
	// Should have default values
	if cfg.LogLevel != "info" {
		t.Errorf("Expected default LogLevel 'info', got '%s'", cfg.LogLevel)
	}
}

func TestLoadConfigInvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid.yaml")

	invalidContent := `
connection: [invalid yaml
  that: won't parse
`
	err := os.WriteFile(configPath, []byte(invalidContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	_, err = Load(configPath)
	if err == nil {
		t.Error("Expected error for invalid YAML, got nil")
	}
}
