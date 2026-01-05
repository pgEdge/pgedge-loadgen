//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Portions copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

// Package cli implements the command-line interface for pgedge-loadgen.
package cli

import (
	"github.com/spf13/cobra"

	"github.com/pgEdge/pgedge-loadgen/internal/config"
	"github.com/pgEdge/pgedge-loadgen/internal/logging"
	"github.com/pgEdge/pgedge-loadgen/pkg/version"
)

var (
	// Global flags
	cfgFile    string
	connection string
	app        string
	logLevel   string

	// Global config
	cfg *config.Config

	rootCmd = &cobra.Command{
		Use:   "pgedge-loadgen",
		Short: "PostgreSQL load generator for realistic workload simulation",
		Long: `pgedge-loadgen is a CLI tool that connects to PostgreSQL databases,
creates schemas for fictional applications, populates them with test data,
and runs realistic load simulations with temporal usage patterns.

This tool is designed to generate realistic database workloads for testing,
NOT for benchmarking. It simulates actual user behavior patterns including
time-of-day variations, weekday/weekend differences, and seasonal patterns.`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return initConfig()
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}
)

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "",
		"config file (default: ./pgedge-loadgen.yaml)")
	rootCmd.PersistentFlags().StringVar(&connection, "connection", "",
		"PostgreSQL connection string")
	rootCmd.PersistentFlags().StringVar(&app, "app", "",
		"application type (wholesale, analytics, brokerage, retail, ecommerce, knowledgebase, docmgmt)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "",
		"log level (debug, info, warn, error)")

	// Add subcommands
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(appsCmd)
	rootCmd.AddCommand(profilesCmd)
}

func initConfig() error {
	var err error
	cfg, err = config.Load(cfgFile)
	if err != nil {
		return err
	}

	// Override with CLI flags
	if connection != "" {
		cfg.Connection = connection
	}
	if app != "" {
		cfg.App = app
	}
	if logLevel != "" {
		cfg.LogLevel = logLevel
	}

	// Reinitialize logger with config
	logging.Init(logging.Config{
		Level:  cfg.LogLevel,
		Pretty: true,
	})

	return nil
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Println(version.Info())
	},
}

var appsCmd = &cobra.Command{
	Use:   "apps",
	Short: "List available applications",
	Long: `List all available fictional applications that can be used for
load generation. Each application has a unique schema and query mix.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Println("Available applications:")
		cmd.Println()
		cmd.Println("TPC-Based Applications:")
		cmd.Println("  wholesale     - Wholesale supplier (TPC-C based) - OLTP workload")
		cmd.Println("  analytics     - Analytics warehouse (TPC-H based) - OLAP workload")
		cmd.Println("  brokerage     - Brokerage firm (TPC-E based) - Mixed workload")
		cmd.Println("  retail        - Retail analytics (TPC-DS based) - Decision support")
		cmd.Println()
		cmd.Println("pgvector Applications:")
		cmd.Println("  ecommerce     - E-commerce with semantic product search")
		cmd.Println("  knowledgebase - Knowledge base with semantic article search")
		cmd.Println("  docmgmt       - Document management with similarity search")
		cmd.Println()
		cmd.Println("Use 'pgedge-loadgen apps describe <app>' for details.")
	},
}

var profilesCmd = &cobra.Command{
	Use:   "profiles",
	Short: "List available usage profiles",
	Long: `List all available usage profiles that simulate different
patterns of database activity based on time of day and week.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Println("Available usage profiles:")
		cmd.Println()
		cmd.Println("  local-office   - Local office hours (8AM-6PM, weekday focus)")
		cmd.Println("  global         - Global enterprise (24/7 with rolling peaks)")
		cmd.Println("  store-regional - Online store, regional (evening peak)")
		cmd.Println("  store-global   - Online store, global (24/7 multi-region)")
		cmd.Println()
		cmd.Println("Profiles affect:")
		cmd.Println("  - Query rate variations throughout the day")
		cmd.Println("  - Weekend vs weekday activity levels")
		cmd.Println("  - Break and lunch time reductions")
	},
}
