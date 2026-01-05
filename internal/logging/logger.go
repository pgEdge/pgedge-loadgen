//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Portions copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

// Package logging provides structured logging for pgedge-loadgen.
package logging

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// Logger is the global logger instance.
var Logger zerolog.Logger

// Config holds logging configuration.
type Config struct {
	Level      string
	Pretty     bool
	TimeFormat string
}

// DefaultConfig returns default logging configuration.
func DefaultConfig() Config {
	return Config{
		Level:      "info",
		Pretty:     true,
		TimeFormat: time.RFC3339,
	}
}

// Init initializes the global logger with the given configuration.
func Init(cfg Config) {
	var output io.Writer = os.Stderr

	// Use default time format if not specified
	timeFormat := cfg.TimeFormat
	if timeFormat == "" {
		timeFormat = time.RFC3339
	}

	if cfg.Pretty {
		output = zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: timeFormat,
		}
	}

	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}

	Logger = zerolog.New(output).
		Level(level).
		With().
		Timestamp().
		Logger()
}

// Debug returns a debug level event.
func Debug() *zerolog.Event {
	return Logger.Debug()
}

// Info returns an info level event.
func Info() *zerolog.Event {
	return Logger.Info()
}

// Warn returns a warning level event.
func Warn() *zerolog.Event {
	return Logger.Warn()
}

// Error returns an error level event.
func Error() *zerolog.Event {
	return Logger.Error()
}

// Fatal returns a fatal level event.
func Fatal() *zerolog.Event {
	return Logger.Fatal()
}

func init() {
	Init(DefaultConfig())
}
