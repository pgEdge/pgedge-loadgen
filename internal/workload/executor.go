// Package workload implements the query execution engine.
package workload

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	"github.com/pgEdge/pgedge-loadgen/internal/db"
	"github.com/pgEdge/pgedge-loadgen/internal/logging"
	"github.com/pgEdge/pgedge-loadgen/internal/workload/profiles"
)

// ExecutorConfig holds configuration for the workload executor.
type ExecutorConfig struct {
	ConnString         string // Connection string for creating per-worker connections
	App                apps.App
	Connections        int
	Profile            string
	Timezone           string
	ReportInterval     int
	ConnectionMode     string
	SessionMinDuration int   // seconds
	SessionMaxDuration int   // seconds
	ThinkTimeMin       int   // milliseconds
	ThinkTimeMax       int   // milliseconds
	MaintainSize       bool  // Enable automatic cleanup to maintain target size
	TargetSize         int64 // Target database size in bytes
	CleanupInterval    int   // Cleanup check interval in seconds (default 300)
}

// Executor manages the workload execution.
type Executor struct {
	connString     string
	app            apps.App
	connections    int
	profile        profiles.Profile
	reportInterval time.Duration

	// Connection mode settings
	connectionMode     string
	sessionMinDuration time.Duration
	sessionMaxDuration time.Duration
	thinkTimeMin       time.Duration
	thinkTimeMax       time.Duration

	// Size maintenance settings
	maintainSize    bool
	targetSize      int64
	cleanupInterval time.Duration

	// Metrics
	totalQueries    atomic.Int64
	successQueries  atomic.Int64
	failedQueries   atomic.Int64
	totalDurationNs atomic.Int64
	startTime       time.Time

	// Session metrics (session mode only)
	totalSessions  atomic.Int64
	activeSessions atomic.Int64

	// Cleanup metrics
	totalDeleted atomic.Int64

	// Query type metrics
	queryMetrics sync.Map // map[string]*queryMetric
}

type queryMetric struct {
	count      atomic.Int64
	durationNs atomic.Int64
	errors     atomic.Int64
}

// NewExecutor creates a new workload executor.
func NewExecutor(cfg ExecutorConfig) (*Executor, error) {
	profile, err := profiles.Get(cfg.Profile, cfg.Timezone)
	if err != nil {
		return nil, fmt.Errorf("failed to get profile: %w", err)
	}

	// Default to pool mode if not specified
	connectionMode := cfg.ConnectionMode
	if connectionMode == "" {
		connectionMode = "pool"
	}

	// Default cleanup interval to 5 minutes
	cleanupInterval := cfg.CleanupInterval
	if cleanupInterval == 0 {
		cleanupInterval = 300
	}

	return &Executor{
		connString:         cfg.ConnString,
		app:                cfg.App,
		connections:        cfg.Connections,
		profile:            profile,
		reportInterval:     time.Duration(cfg.ReportInterval) * time.Second,
		connectionMode:     connectionMode,
		sessionMinDuration: time.Duration(cfg.SessionMinDuration) * time.Second,
		sessionMaxDuration: time.Duration(cfg.SessionMaxDuration) * time.Second,
		thinkTimeMin:       time.Duration(cfg.ThinkTimeMin) * time.Millisecond,
		thinkTimeMax:       time.Duration(cfg.ThinkTimeMax) * time.Millisecond,
		maintainSize:       cfg.MaintainSize,
		targetSize:         cfg.TargetSize,
		cleanupInterval:    time.Duration(cleanupInterval) * time.Second,
	}, nil
}

// Run starts the workload execution and blocks until context is cancelled.
func (e *Executor) Run(ctx context.Context) error {
	e.startTime = time.Now()

	logging.Info().
		Str("mode", e.connectionMode).
		Int("connections", e.connections).
		Msg("Starting workload execution")

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < e.connections; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			if e.connectionMode == "session" {
				e.sessionWorker(ctx, workerID)
			} else {
				e.poolWorker(ctx, workerID)
			}
		}(i)
	}

	// Start reporter
	if e.reportInterval > 0 {
		go e.reporter(ctx)
	}

	// Start size maintenance if enabled and app supports it
	if e.maintainSize && e.targetSize > 0 {
		if _, ok := e.app.(apps.SizeMaintainer); ok {
			go e.cleaner(ctx)
		}
	}

	// Wait for all workers to complete
	wg.Wait()

	return nil
}

// poolWorker implements the pool connection mode where connections are
// shared and reused rapidly, typical for web applications.
func (e *Executor) poolWorker(ctx context.Context, id int) {
	logging.Debug().Int("worker_id", id).Msg("Pool worker started")

	// Create dedicated connection for this worker with unique application name
	appNameSuffix := fmt.Sprintf("client %d", id+1)
	conn, err := db.ConnectSingle(ctx, e.connString, appNameSuffix)
	if err != nil {
		logging.Error().Err(err).Int("worker_id", id).Msg("Failed to create worker connection")
		return
	}
	defer conn.Close(ctx)

	for {
		select {
		case <-ctx.Done():
			logging.Debug().Int("worker_id", id).Msg("Pool worker stopped")
			return
		default:
			// Get current activity level from profile
			activityLevel := e.profile.GetActivityLevel(time.Now())

			// Skip if activity level is very low
			if activityLevel < 0.01 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Execute query using dedicated connection
			result := e.app.ExecuteQueryConn(ctx, conn)

			// Update metrics
			e.totalQueries.Add(1)
			e.totalDurationNs.Add(result.Duration)

			metric := e.getOrCreateQueryMetric(result.QueryName)
			metric.count.Add(1)
			metric.durationNs.Add(result.Duration)

			if result.Error != nil {
				// Don't count context cancellation errors as failures
				// (these occur at shutdown when run duration ends)
				if !errors.Is(result.Error, context.Canceled) &&
					!errors.Is(result.Error, context.DeadlineExceeded) {
					e.failedQueries.Add(1)
					metric.errors.Add(1)
					logging.Debug().
						Err(result.Error).
						Str("query", result.QueryName).
						Msg("Query failed")
				}
			} else {
				e.successQueries.Add(1)
			}

			// Apply delay based on activity level
			// Lower activity = longer delay
			delay := e.calculateDelay(activityLevel)
			if delay > 0 {
				time.Sleep(delay)
			}
		}
	}
}

// sessionWorker implements the session connection mode where each worker
// simulates a user session with think time between queries, typical for
// desktop applications like trading terminals.
func (e *Executor) sessionWorker(ctx context.Context, id int) {
	logging.Debug().Int("worker_id", id).Msg("Session worker started")

	// Create dedicated connection for this worker with unique application name
	appNameSuffix := fmt.Sprintf("client %d", id+1)
	conn, err := db.ConnectSingle(ctx, e.connString, appNameSuffix)
	if err != nil {
		logging.Error().Err(err).Int("worker_id", id).Msg("Failed to create worker connection")
		return
	}
	defer conn.Close(ctx)

	for {
		select {
		case <-ctx.Done():
			logging.Debug().Int("worker_id", id).Msg("Session worker stopped")
			return
		default:
			// Get current activity level from profile
			activityLevel := e.profile.GetActivityLevel(time.Now())

			// Skip starting new sessions if activity level is very low
			if activityLevel < 0.01 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Run a user session
			e.runSession(ctx, id, conn, activityLevel)
		}
	}
}

// runSession simulates a single user session with multiple queries and think time.
func (e *Executor) runSession(ctx context.Context, workerID int, conn *pgx.Conn, activityLevel float64) {
	// Calculate session duration (randomized within range)
	sessionDuration := e.randomDuration(e.sessionMinDuration, e.sessionMaxDuration)

	// Scale session duration by activity level (lower activity = shorter sessions)
	sessionDuration = time.Duration(float64(sessionDuration) * activityLevel)
	if sessionDuration < time.Second {
		sessionDuration = time.Second
	}

	e.totalSessions.Add(1)
	e.activeSessions.Add(1)
	defer e.activeSessions.Add(-1)

	logging.Debug().
		Int("worker_id", workerID).
		Dur("session_duration", sessionDuration).
		Msg("Session started")

	sessionStart := time.Now()
	sessionEnd := sessionStart.Add(sessionDuration)

	for time.Now().Before(sessionEnd) {
		select {
		case <-ctx.Done():
			return
		default:
			// Execute query using dedicated connection
			result := e.app.ExecuteQueryConn(ctx, conn)

			// Update metrics
			e.totalQueries.Add(1)
			e.totalDurationNs.Add(result.Duration)

			metric := e.getOrCreateQueryMetric(result.QueryName)
			metric.count.Add(1)
			metric.durationNs.Add(result.Duration)

			if result.Error != nil {
				// Don't count context cancellation errors as failures
				// (these occur at shutdown when run duration ends)
				if !errors.Is(result.Error, context.Canceled) &&
					!errors.Is(result.Error, context.DeadlineExceeded) {
					e.failedQueries.Add(1)
					metric.errors.Add(1)
					logging.Debug().
						Err(result.Error).
						Str("query", result.QueryName).
						Msg("Query failed")
				}
			} else {
				e.successQueries.Add(1)
			}

			// Apply think time between queries
			thinkTime := e.randomDuration(e.thinkTimeMin, e.thinkTimeMax)
			select {
			case <-ctx.Done():
				return
			case <-time.After(thinkTime):
			}
		}
	}

	logging.Debug().
		Int("worker_id", workerID).
		Msg("Session ended")

	// Brief pause between sessions (simulates user logging out and back in)
	select {
	case <-ctx.Done():
		return
	case <-time.After(e.randomDuration(time.Second, 5*time.Second)):
	}
}

// randomDuration returns a random duration between min and max.
func (e *Executor) randomDuration(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	delta := max - min
	// Use simple random based on current time nanoseconds
	randomNs := time.Now().UnixNano() % int64(delta)
	return min + time.Duration(randomNs)
}

func (e *Executor) getOrCreateQueryMetric(name string) *queryMetric {
	if m, ok := e.queryMetrics.Load(name); ok {
		return m.(*queryMetric)
	}

	m := &queryMetric{}
	actual, _ := e.queryMetrics.LoadOrStore(name, m)
	return actual.(*queryMetric)
}

func (e *Executor) calculateDelay(activityLevel float64) time.Duration {
	if activityLevel >= 1.0 {
		return 0
	}

	// Base delay at full activity is 0, increases as activity decreases
	// At 50% activity, delay is ~100ms per query
	// At 10% activity, delay is ~900ms per query
	baseDelay := time.Duration((1.0-activityLevel)*1000) * time.Millisecond
	return baseDelay
}

func (e *Executor) reporter(ctx context.Context) {
	ticker := time.NewTicker(e.reportInterval)
	defer ticker.Stop()

	var lastTotal int64
	lastTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			total := e.totalQueries.Load()
			success := e.successQueries.Load()
			failed := e.failedQueries.Load()
			durationNs := e.totalDurationNs.Load()

			// Calculate rate since last report
			elapsed := now.Sub(lastTime).Seconds()
			rate := float64(total-lastTotal) / elapsed

			// Calculate average latency
			var avgLatencyMs float64
			if total > 0 {
				avgLatencyMs = float64(durationNs) / float64(total) / 1e6
			}

			activityLevel := e.profile.GetActivityLevel(now)

			logEvent := logging.Info().
				Int64("total", total).
				Int64("success", success).
				Int64("failed", failed).
				Float64("rate_qps", rate).
				Float64("avg_latency_ms", avgLatencyMs).
				Float64("activity_level", activityLevel)

			// Add session metrics if in session mode
			if e.connectionMode == "session" {
				logEvent = logEvent.
					Int64("total_sessions", e.totalSessions.Load()).
					Int64("active_sessions", e.activeSessions.Load())
			}

			logEvent.Msg("Statistics")

			lastTotal = total
			lastTime = now
		}
	}
}

// cleaner periodically checks if the database has grown beyond the target size
// and deletes old data to bring it back within bounds.
func (e *Executor) cleaner(ctx context.Context) {
	ticker := time.NewTicker(e.cleanupInterval)
	defer ticker.Stop()

	maintainer := e.app.(apps.SizeMaintainer)

	logging.Info().
		Int64("target_size_bytes", e.targetSize).
		Dur("cleanup_interval", e.cleanupInterval).
		Msg("Size maintenance enabled")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Create a dedicated connection for cleanup
			conn, err := db.ConnectSingle(ctx, e.connString, "cleaner")
			if err != nil {
				logging.Error().Err(err).Msg("Failed to create cleanup connection")
				continue
			}

			deleted, err := maintainer.MaintainSize(ctx, conn, e.targetSize)
			conn.Close(ctx)

			if err != nil {
				logging.Error().Err(err).Msg("Size maintenance failed")
				continue
			}

			if deleted > 0 {
				e.totalDeleted.Add(deleted)
				logging.Info().
					Int64("deleted", deleted).
					Int64("total_deleted", e.totalDeleted.Load()).
					Msg("Size maintenance completed")
			}
		}
	}
}

// PrintSummary prints a final summary of the workload execution.
func (e *Executor) PrintSummary() {
	elapsed := time.Since(e.startTime)
	total := e.totalQueries.Load()
	success := e.successQueries.Load()
	failed := e.failedQueries.Load()
	durationNs := e.totalDurationNs.Load()

	var avgLatencyMs float64
	if total > 0 {
		avgLatencyMs = float64(durationNs) / float64(total) / 1e6
	}

	logEvent := logging.Info().
		Str("connection_mode", e.connectionMode).
		Dur("duration", elapsed).
		Int64("total_queries", total).
		Int64("successful", success).
		Int64("failed", failed).
		Float64("avg_qps", float64(total)/elapsed.Seconds()).
		Float64("avg_latency_ms", avgLatencyMs)

	// Add session metrics if in session mode
	if e.connectionMode == "session" {
		totalSessions := e.totalSessions.Load()
		var avgQueriesPerSession float64
		if totalSessions > 0 {
			avgQueriesPerSession = float64(total) / float64(totalSessions)
		}
		logEvent = logEvent.
			Int64("total_sessions", totalSessions).
			Float64("avg_queries_per_session", avgQueriesPerSession)
	}

	logEvent.Msg("Final summary")

	// Print per-query statistics
	logging.Info().Msg("Per-query statistics:")
	e.queryMetrics.Range(func(key, value interface{}) bool {
		name := key.(string)
		m := value.(*queryMetric)
		count := m.count.Load()
		dNs := m.durationNs.Load()
		errors := m.errors.Load()

		var avgMs float64
		if count > 0 {
			avgMs = float64(dNs) / float64(count) / 1e6
		}

		logging.Info().
			Str("query", name).
			Int64("count", count).
			Int64("errors", errors).
			Float64("avg_latency_ms", avgMs).
			Msg("")

		return true
	})
}
