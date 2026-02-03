//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

// Package profiles implements usage profiles for workload simulation.
package profiles

import (
	"fmt"
	"time"
)

// Profile defines the interface for usage profiles.
type Profile interface {
	// Name returns the profile name.
	Name() string

	// Description returns a human-readable description.
	Description() string

	// GetActivityLevel returns the current activity level (0.0 to 1.0+).
	// Values above 1.0 indicate higher-than-normal activity (e.g., weekends for stores).
	GetActivityLevel(t time.Time) float64
}

var registry = make(map[string]func(tz *time.Location) Profile)

// Register adds a profile constructor to the registry.
func Register(name string, constructor func(tz *time.Location) Profile) {
	registry[name] = constructor
}

// Get retrieves a profile by name with the specified timezone.
func Get(name, timezone string) (Profile, error) {
	constructor, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown profile: %s", name)
	}

	var loc *time.Location
	var err error

	if timezone == "" || timezone == "Local" {
		loc = time.Local
	} else {
		loc, err = time.LoadLocation(timezone)
		if err != nil {
			return nil, fmt.Errorf("invalid timezone: %w", err)
		}
	}

	return constructor(loc), nil
}

// List returns all registered profile names.
func List() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}

func init() {
	Register("local-office", NewLocalOffice)
	Register("global", NewGlobalEnterprise)
	Register("store-regional", NewStoreRegional)
	Register("store-global", NewStoreGlobal)
}
