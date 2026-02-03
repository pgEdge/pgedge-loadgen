//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

package profiles

import (
	"math"
	"time"
)

// GlobalEnterprise simulates a global enterprise usage pattern.
// 24/7 operation with rolling peaks following the sun
// Minimum activity: 30% (never drops below)
// Peak follows business hours across timezones
// Slight reduction during global "quiet hours" (2AM - 4AM UTC)
// Weekend: 60% of weekday activity
type GlobalEnterprise struct {
	tz *time.Location
}

// NewGlobalEnterprise creates a new GlobalEnterprise profile.
func NewGlobalEnterprise(tz *time.Location) Profile {
	return &GlobalEnterprise{tz: tz}
}

func (p *GlobalEnterprise) Name() string {
	return "global"
}

func (p *GlobalEnterprise) Description() string {
	return "Global enterprise (24/7 with rolling peaks)"
}

func (p *GlobalEnterprise) GetActivityLevel(t time.Time) float64 {
	utc := t.UTC()
	utcHour := utc.Hour()
	weekday := utc.Weekday()

	// Weekend factor: 60% of weekday
	weekendFactor := 1.0
	if weekday == time.Saturday || weekday == time.Sunday {
		weekendFactor = 0.60
	}

	// Base activity from multiple timezone peaks
	// Simulate peaks in Americas (EST), Europe (CET), and Asia (JST)

	// Americas peak: 9AM-5PM EST = 14:00-22:00 UTC
	americasPeak := peakContribution(utcHour, 14, 22)

	// Europe peak: 9AM-5PM CET = 8:00-16:00 UTC
	europePeak := peakContribution(utcHour, 8, 16)

	// Asia peak: 9AM-5PM JST = 0:00-8:00 UTC
	asiaPeak := peakContribution(utcHour, 0, 8)

	// Combine peaks (they overlap somewhat)
	combined := math.Max(americasPeak, math.Max(europePeak, asiaPeak))

	// Global quiet hours: 2AM - 4AM UTC (slight reduction)
	if utcHour >= 2 && utcHour < 4 {
		combined *= 0.80
	}

	// Scale to range [0.30, 1.0]
	activity := 0.30 + 0.70*combined

	return activity * weekendFactor
}

// peakContribution returns a value between 0 and 1 based on whether
// the hour falls within peak hours. Includes ramp-up and ramp-down.
func peakContribution(hour, peakStart, peakEnd int) float64 {
	// Handle wraparound for Asia timezone
	if peakStart > peakEnd {
		// e.g., 22:00 to 6:00
		if hour >= peakStart || hour < peakEnd {
			return 1.0
		}
		// Ramp up before peak
		if hour == peakStart-1 {
			return 0.5
		}
		// Ramp down after peak
		if hour == peakEnd {
			return 0.5
		}
		return 0.0
	}

	// Normal case
	if hour >= peakStart && hour < peakEnd {
		return 1.0
	}

	// Ramp up hour before peak
	if hour == peakStart-1 {
		return 0.5
	}

	// Ramp down hour after peak
	if hour == peakEnd {
		return 0.5
	}

	return 0.0
}
