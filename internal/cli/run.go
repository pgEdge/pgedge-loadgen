package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	"github.com/pgEdge/pgedge-loadgen/internal/db"
	"github.com/pgEdge/pgedge-loadgen/internal/logging"
	"github.com/pgEdge/pgedge-loadgen/internal/workload"
)

var (
	runConnections        int
	runProfile            string
	runTimezone           string
	runReportInterval     int
	runDuration           int
	runConnectionMode     string
	runSessionMinDuration int
	runSessionMaxDuration int
	runThinkTimeMin       int
	runThinkTimeMax       int
	runNoMaintainSize     bool
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run load simulation against an initialized database",
	Long: `Run realistic load simulation against a database that was previously
initialized with the 'init' command. The simulation will continue until
interrupted with Ctrl+C or until the specified duration expires.

Connection Modes:
  pool    - Connections are shared and reused rapidly (web apps, default)
  session - Workers simulate user sessions with think time (desktop apps)

Example:
  pgedge-loadgen run --app wholesale --connections 50 --profile local-office
  pgedge-loadgen run --app wholesale --connections 50 --duration 30
  pgedge-loadgen run --app brokerage --connections 20 --connection-mode session`,
	RunE: runRun,
}

func init() {
	runCmd.Flags().IntVar(&runConnections, "connections", 0,
		"number of database connections")
	runCmd.Flags().StringVar(&runProfile, "profile", "",
		"usage profile: local-office, global, store-regional, store-global")
	runCmd.Flags().StringVar(&runTimezone, "timezone", "",
		"timezone for profile calculations (default: Local)")
	runCmd.Flags().IntVar(&runReportInterval, "report-interval", 0,
		"statistics reporting interval in seconds")
	runCmd.Flags().IntVar(&runDuration, "duration", 0,
		"duration to run in minutes (0 = run indefinitely)")
	runCmd.Flags().StringVar(&runConnectionMode, "connection-mode", "",
		"connection mode: pool (web apps) or session (desktop apps)")
	runCmd.Flags().IntVar(&runSessionMinDuration, "session-min-duration", 0,
		"minimum session duration in seconds (session mode only)")
	runCmd.Flags().IntVar(&runSessionMaxDuration, "session-max-duration", 0,
		"maximum session duration in seconds (session mode only)")
	runCmd.Flags().IntVar(&runThinkTimeMin, "think-time-min", 0,
		"minimum think time between queries in milliseconds (session mode only)")
	runCmd.Flags().IntVar(&runThinkTimeMax, "think-time-max", 0,
		"maximum think time between queries in milliseconds (session mode only)")
	runCmd.Flags().BoolVar(&runNoMaintainSize, "no-maintain-size", false,
		"disable automatic cleanup of old data to maintain target database size")
}

func runRun(cmd *cobra.Command, args []string) error {
	// Override config with CLI flags
	if runConnections > 0 {
		cfg.Run.Connections = runConnections
	}
	if runProfile != "" {
		cfg.Run.Profile = runProfile
	}
	if runTimezone != "" {
		cfg.Run.Timezone = runTimezone
	}
	if runReportInterval > 0 {
		cfg.Run.ReportInterval = runReportInterval
	}
	if runDuration > 0 {
		cfg.Run.Duration = runDuration
	}
	if runConnectionMode != "" {
		cfg.Run.ConnectionMode = runConnectionMode
	}
	if runSessionMinDuration > 0 {
		cfg.Run.SessionMinDuration = runSessionMinDuration
	}
	if runSessionMaxDuration > 0 {
		cfg.Run.SessionMaxDuration = runSessionMaxDuration
	}
	if runThinkTimeMin > 0 {
		cfg.Run.ThinkTimeMin = runThinkTimeMin
	}
	if runThinkTimeMax > 0 {
		cfg.Run.ThinkTimeMax = runThinkTimeMax
	}

	// Validate configuration
	if err := cfg.ValidateRun(); err != nil {
		return err
	}

	// Get the application
	application, err := apps.Get(cfg.App)
	if err != nil {
		return err
	}

	// Connect to database for metadata check (single connection, not pool)
	ctx := context.Background()
	conn, err := db.ConnectSingle(ctx, cfg.Connection, "metadata")
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	// Check that database was initialized for this app
	existingApp, err := db.GetMetadataValueConn(ctx, conn, "app")
	if err != nil {
		return fmt.Errorf(
			"database has not been initialized; run 'pgedge-loadgen init' first")
	}
	if existingApp != cfg.App {
		conn.Close(ctx)
		return fmt.Errorf(
			"database was initialized for '%s' but '%s' was specified; "+
				"re-run with --app=%s or initialize a new database",
			existingApp, cfg.App, existingApp)
	}

	// Get target size for size maintenance
	var targetSize int64
	maintainSize := !runNoMaintainSize
	if maintainSize {
		targetSizeStr, err := db.GetMetadataValueConn(ctx, conn, "target_size")
		if err != nil {
			logging.Warn().
				Msg("Could not retrieve target_size; size maintenance disabled")
			maintainSize = false
		} else {
			targetSize, err = parseSize(targetSizeStr)
			if err != nil {
				logging.Warn().
					Str("target_size", targetSizeStr).
					Msg("Could not parse target_size; size maintenance disabled")
				maintainSize = false
			}
		}
	}

	// Close metadata connection - workers will create their own connections
	conn.Close(ctx)

	durationMsg := "indefinitely"
	if cfg.Run.Duration > 0 {
		durationMsg = fmt.Sprintf("%d minutes", cfg.Run.Duration)
	}

	logging.Info().
		Str("app", cfg.App).
		Int("connections", cfg.Run.Connections).
		Str("profile", cfg.Run.Profile).
		Str("connection_mode", cfg.Run.ConnectionMode).
		Str("duration", durationMsg).
		Msg("Starting load simulation")

	// Set up context with cancellation (and optional timeout)
	var cancel context.CancelFunc
	if cfg.Run.Duration > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(cfg.Run.Duration)*time.Minute)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logging.Info().
			Str("signal", sig.String()).
			Msg("Received shutdown signal")
		cancel()
	}()

	// Create and run the workload executor
	executor, err := workload.NewExecutor(workload.ExecutorConfig{
		ConnString:         cfg.Connection,
		App:                application,
		Connections:        cfg.Run.Connections,
		Profile:            cfg.Run.Profile,
		Timezone:           cfg.Run.Timezone,
		ReportInterval:     cfg.Run.ReportInterval,
		ConnectionMode:     cfg.Run.ConnectionMode,
		SessionMinDuration: cfg.Run.SessionMinDuration,
		SessionMaxDuration: cfg.Run.SessionMaxDuration,
		ThinkTimeMin:       cfg.Run.ThinkTimeMin,
		ThinkTimeMax:       cfg.Run.ThinkTimeMax,
		MaintainSize:       maintainSize,
		TargetSize:         targetSize,
	})
	if err != nil {
		return fmt.Errorf("failed to create executor: %w", err)
	}

	// Run until context is cancelled (signal or timeout)
	if err := executor.Run(ctx); err != nil {
		if ctx.Err() != nil {
			// Context was cancelled (shutdown signal or timeout)
			if ctx.Err() == context.DeadlineExceeded {
				logging.Info().Msg("Duration limit reached, stopping simulation")
			} else {
				logging.Info().Msg("Load simulation stopped")
			}
			executor.PrintSummary()
			return nil
		}
		return fmt.Errorf("executor error: %w", err)
	}

	// Normal completion (shouldn't happen unless duration was set)
	logging.Info().Msg("Load simulation completed")
	executor.PrintSummary()
	return nil
}
