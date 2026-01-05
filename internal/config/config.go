//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Portions copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

// Package config handles configuration management for pgedge-loadgen.
// Configuration is loaded from config files and CLI flags (no environment variables).
// CLI flags take precedence over config file values.
package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

// Config holds all configuration for pgedge-loadgen.
type Config struct {
	// Connection is the PostgreSQL connection string.
	Connection string `mapstructure:"connection"`

	// App is the application type to use.
	App string `mapstructure:"app"`

	// LogLevel controls logging verbosity (debug, info, warn, error).
	LogLevel string `mapstructure:"log_level"`

	// Init holds configuration for the init subcommand.
	Init InitConfig `mapstructure:"init"`

	// Run holds configuration for the run subcommand.
	Run RunConfig `mapstructure:"run"`
}

// InitConfig holds configuration for database initialization.
type InitConfig struct {
	// Size is the target database size (e.g., "5GB", "500MB").
	Size string `mapstructure:"size"`

	// EmbeddingMode controls how vector embeddings are generated.
	// Options: random, openai, sentence, vectorizer
	EmbeddingMode string `mapstructure:"embedding_mode"`

	// EmbeddingDimensions is the vector dimension size.
	EmbeddingDimensions int `mapstructure:"embedding_dimensions"`

	// VectorizerURL is the URL for pgedge-vectorizer service.
	VectorizerURL string `mapstructure:"vectorizer_url"`

	// OpenAIAPIKey is the API key for OpenAI embeddings.
	OpenAIAPIKey string `mapstructure:"openai_api_key"`

	// DropExisting drops existing schema before initialization.
	DropExisting bool `mapstructure:"drop_existing"`
}

// RunConfig holds configuration for load generation.
type RunConfig struct {
	// Connections is the maximum number of database connections.
	Connections int `mapstructure:"connections"`

	// Profile is the usage profile to simulate.
	Profile string `mapstructure:"profile"`

	// Timezone is the timezone for profile calculations.
	Timezone string `mapstructure:"timezone"`

	// ReportInterval is how often to print statistics (in seconds).
	ReportInterval int `mapstructure:"report_interval"`

	// Duration is how long to run in minutes (0 = indefinite).
	Duration int `mapstructure:"duration"`

	// ConnectionMode controls how connections are used: "pool" or "session".
	// Pool mode (default): connections are shared and reused rapidly (web apps).
	// Session mode: workers simulate user sessions with think time (desktop apps).
	ConnectionMode string `mapstructure:"connection_mode"`

	// SessionMinDuration is the minimum session duration in seconds (session mode only).
	SessionMinDuration int `mapstructure:"session_min_duration"`

	// SessionMaxDuration is the maximum session duration in seconds (session mode only).
	SessionMaxDuration int `mapstructure:"session_max_duration"`

	// ThinkTimeMin is the minimum think time between queries in milliseconds (session mode only).
	ThinkTimeMin int `mapstructure:"think_time_min"`

	// ThinkTimeMax is the maximum think time between queries in milliseconds (session mode only).
	ThinkTimeMax int `mapstructure:"think_time_max"`
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() *Config {
	return &Config{
		LogLevel: "info",
		Init: InitConfig{
			Size:                "1GB",
			EmbeddingMode:       "random",
			EmbeddingDimensions: 384,
			DropExisting:        false,
		},
		Run: RunConfig{
			Connections:        10,
			Profile:            "local-office",
			Timezone:           "Local",
			ReportInterval:     60,
			ConnectionMode:     "pool",
			SessionMinDuration: 300,  // 5 minutes
			SessionMaxDuration: 1800, // 30 minutes
			ThinkTimeMin:       1000, // 1 second
			ThinkTimeMax:       5000, // 5 seconds
		},
	}
}

// Load reads configuration from config files.
// Config file locations (in order of precedence):
// 1. Path specified by configFile parameter
// 2. ./pgedge-loadgen.yaml
// 3. ~/.config/pgedge-loadgen/config.yaml
func Load(configFile string) (*Config, error) {
	v := viper.New()

	// Set config name and type
	v.SetConfigName("pgedge-loadgen")
	v.SetConfigType("yaml")

	// Add config paths
	v.AddConfigPath(".")
	if home, err := os.UserHomeDir(); err == nil {
		v.AddConfigPath(filepath.Join(home, ".config", "pgedge-loadgen"))
	}

	// Use specific config file if provided
	if configFile != "" {
		v.SetConfigFile(configFile)
	}

	// Read config file (ignore if not found)
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	// Start with defaults
	cfg := DefaultConfig()

	// Unmarshal config file values
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("error parsing config: %w", err)
	}

	return cfg, nil
}

// Validate checks that required configuration is present.
func (c *Config) Validate() error {
	if c.Connection == "" {
		return fmt.Errorf("connection string is required")
	}
	if c.App == "" {
		return fmt.Errorf("app type is required")
	}
	return nil
}

// ValidateInit checks configuration required for init command.
func (c *Config) ValidateInit() error {
	if err := c.Validate(); err != nil {
		return err
	}
	if c.Init.Size == "" {
		return fmt.Errorf("target size is required for init")
	}
	return nil
}

// ValidateRun checks configuration required for run command.
func (c *Config) ValidateRun() error {
	if err := c.Validate(); err != nil {
		return err
	}
	if c.Run.Connections < 1 {
		return fmt.Errorf("connections must be at least 1")
	}
	if c.Run.ConnectionMode != "pool" && c.Run.ConnectionMode != "session" {
		return fmt.Errorf("connection_mode must be 'pool' or 'session'")
	}
	if c.Run.ConnectionMode == "session" {
		if c.Run.SessionMinDuration < 1 {
			return fmt.Errorf("session_min_duration must be at least 1 second")
		}
		if c.Run.SessionMaxDuration < c.Run.SessionMinDuration {
			return fmt.Errorf("session_max_duration must be >= session_min_duration")
		}
		if c.Run.ThinkTimeMin < 0 {
			return fmt.Errorf("think_time_min must be non-negative")
		}
		if c.Run.ThinkTimeMax < c.Run.ThinkTimeMin {
			return fmt.Errorf("think_time_max must be >= think_time_min")
		}
	}
	return nil
}
