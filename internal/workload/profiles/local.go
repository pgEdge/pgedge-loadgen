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
	"time"
)

// LocalOffice simulates a local office usage pattern.
// Peak hours: 8AM - 6PM local time
// Lunch dip: 12PM - 1PM (50% reduction)
// Break dips: 10:30AM, 3:30PM (30% reduction)
// Evening: 6PM - 10PM (20% of peak)
// Night: 10PM - 6AM (5% of peak, batch processing)
// Weekend: 10% of weekday peak
type LocalOffice struct {
	tz *time.Location
}

// NewLocalOffice creates a new LocalOffice profile.
func NewLocalOffice(tz *time.Location) Profile {
	return &LocalOffice{tz: tz}
}

func (p *LocalOffice) Name() string {
	return "local-office"
}

func (p *LocalOffice) Description() string {
	return "Local office hours (8AM-6PM, weekday focus)"
}

func (p *LocalOffice) GetActivityLevel(t time.Time) float64 {
	t = t.In(p.tz)
	hour := t.Hour()
	minute := t.Minute()
	weekday := t.Weekday()

	// Weekend: 10% activity
	if weekday == time.Saturday || weekday == time.Sunday {
		return 0.10
	}

	// Convert to decimal hour for easier comparison
	decimalHour := float64(hour) + float64(minute)/60.0

	// Night: 10PM - 6AM (5% - batch processing)
	if hour >= 22 || hour < 6 {
		return 0.05
	}

	// Early morning: 6AM - 8AM (ramp up from 5% to 100%)
	if hour >= 6 && hour < 8 {
		progress := (decimalHour - 6.0) / 2.0 // 0 to 1 over 2 hours
		return 0.05 + 0.95*progress
	}

	// Peak hours: 8AM - 6PM with breaks
	if hour >= 8 && hour < 18 {
		base := 1.0

		// Lunch dip: 12PM - 1PM (50% reduction)
		if hour == 12 {
			base = 0.50
		}

		// Morning break: 10:30AM - 11AM (30% reduction)
		if hour == 10 && minute >= 30 {
			base = 0.70
		}

		// Afternoon break: 3:30PM - 4PM (30% reduction)
		if hour == 15 && minute >= 30 {
			base = 0.70
		}

		return base
	}

	// Evening: 6PM - 10PM (ramp down from 100% to 20%)
	if hour >= 18 && hour < 22 {
		progress := (decimalHour - 18.0) / 4.0 // 0 to 1 over 4 hours
		return 1.0 - 0.80*progress
	}

	return 0.05
}
