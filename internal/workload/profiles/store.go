package profiles

import (
	"math"
	"time"
)

// StoreRegional simulates a regional online store usage pattern.
// Morning: 6AM - 12PM (40% of peak)
// Afternoon: 12PM - 5PM (60% of peak)
// Evening peak: 5PM - 10PM (100%)
// Late night: 10PM - 12AM (70%)
// Night: 12AM - 6AM (15%)
// Weekend: 120% of weekday (higher activity)
type StoreRegional struct {
	tz *time.Location
}

// NewStoreRegional creates a new StoreRegional profile.
func NewStoreRegional(tz *time.Location) Profile {
	return &StoreRegional{tz: tz}
}

func (p *StoreRegional) Name() string {
	return "store-regional"
}

func (p *StoreRegional) Description() string {
	return "Online store, regional (evening peak)"
}

func (p *StoreRegional) GetActivityLevel(t time.Time) float64 {
	t = t.In(p.tz)
	hour := t.Hour()
	weekday := t.Weekday()

	var base float64

	switch {
	case hour >= 0 && hour < 6:
		// Night: 12AM - 6AM (15%)
		base = 0.15
	case hour >= 6 && hour < 12:
		// Morning: 6AM - 12PM (40%)
		base = 0.40
	case hour >= 12 && hour < 17:
		// Afternoon: 12PM - 5PM (60%)
		base = 0.60
	case hour >= 17 && hour < 22:
		// Evening peak: 5PM - 10PM (100%)
		base = 1.0
	case hour >= 22 && hour < 24:
		// Late night: 10PM - 12AM (70%)
		base = 0.70
	default:
		base = 0.15
	}

	// Weekend bonus: 120% of weekday
	if weekday == time.Saturday || weekday == time.Sunday {
		base *= 1.20
	}

	return base
}

// StoreGlobal simulates a global online store usage pattern.
// 24/7 with multiple regional peaks
// Base activity: 40% minimum
// Peaks follow evening hours in major markets
// Weekend: 110% of weekday
type StoreGlobal struct {
	tz *time.Location
}

// NewStoreGlobal creates a new StoreGlobal profile.
func NewStoreGlobal(tz *time.Location) Profile {
	return &StoreGlobal{tz: tz}
}

func (p *StoreGlobal) Name() string {
	return "store-global"
}

func (p *StoreGlobal) Description() string {
	return "Online store, global (24/7 multi-region)"
}

func (p *StoreGlobal) GetActivityLevel(t time.Time) float64 {
	utc := t.UTC()
	utcHour := utc.Hour()
	weekday := utc.Weekday()

	// Evening peaks in different regions (5PM-10PM local = peak shopping)
	// Americas: 5PM-10PM EST = 22:00-03:00 UTC
	// Europe: 5PM-10PM CET = 16:00-21:00 UTC
	// Asia: 5PM-10PM JST = 08:00-13:00 UTC

	// Calculate contribution from each region's evening peak
	americasPeak := eveningPeakContribution(utcHour, 22, 3) // wraps around midnight
	europePeak := eveningPeakContribution(utcHour, 16, 21)
	asiaPeak := eveningPeakContribution(utcHour, 8, 13)

	// Combine peaks
	combined := math.Max(americasPeak, math.Max(europePeak, asiaPeak))

	// Scale to range [0.40, 1.0] - global stores never drop below 40%
	activity := 0.40 + 0.60*combined

	// Weekend bonus: 110% of weekday
	if weekday == time.Saturday || weekday == time.Sunday {
		activity *= 1.10
	}

	return activity
}

// eveningPeakContribution returns a value between 0 and 1 for evening shopping peaks.
func eveningPeakContribution(hour, peakStart, peakEnd int) float64 {
	// Handle wraparound (e.g., 22:00 to 03:00)
	if peakStart > peakEnd {
		if hour >= peakStart || hour < peakEnd {
			return 1.0
		}
		// Gradual ramp up/down
		if hour == peakStart-1 || hour == peakEnd {
			return 0.6
		}
		if hour == peakStart-2 || hour == peakEnd+1 {
			return 0.3
		}
		return 0.0
	}

	// Normal case
	if hour >= peakStart && hour < peakEnd {
		return 1.0
	}

	// Gradual ramp up before peak
	if hour == peakStart-1 {
		return 0.6
	}
	if hour == peakStart-2 {
		return 0.3
	}

	// Gradual ramp down after peak
	if hour == peakEnd {
		return 0.6
	}
	if hour == peakEnd+1 {
		return 0.3
	}

	return 0.0
}
