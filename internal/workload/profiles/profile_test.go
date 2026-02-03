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
	"testing"
	"time"
)

func TestGet(t *testing.T) {
	tests := []struct {
		name      string
		profile   string
		timezone  string
		wantError bool
	}{
		{"local-office", "local-office", "Local", false},
		{"global", "global", "UTC", false},
		{"store-regional", "store-regional", "America/New_York", false},
		{"store-global", "store-global", "Europe/London", false},
		{"invalid profile", "invalid", "Local", true},
		{"empty profile", "", "Local", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			profile, err := Get(tt.profile, tt.timezone)
			if tt.wantError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if profile == nil {
					t.Error("Expected profile, got nil")
				}
			}
		})
	}
}

func TestGetAvailable(t *testing.T) {
	profiles := List()

	if len(profiles) == 0 {
		t.Error("List returned empty slice")
	}

	// Should contain the known profiles
	expected := []string{"local-office", "global", "store-regional", "store-global"}
	for _, exp := range expected {
		found := false
		for _, p := range profiles {
			if p == exp {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected profile '%s' not found", exp)
		}
	}
}

func TestLocalOfficeProfile(t *testing.T) {
	profile, err := Get("local-office", "UTC")
	if err != nil {
		t.Fatalf("Failed to get profile: %v", err)
	}

	// Test activity levels at different times
	testCases := []struct {
		hour        int
		minute      int
		dayOfWeek   time.Weekday
		expectLow   bool
		expectHigh  bool
		description string
	}{
		// Business hours (high activity)
		{9, 0, time.Monday, false, true, "9 AM Monday - business hours"},
		{14, 0, time.Tuesday, false, true, "2 PM Tuesday - business hours"},
		{17, 0, time.Wednesday, false, true, "5 PM Wednesday - business hours"},

		// Lunch dip
		{12, 30, time.Thursday, false, false, "12:30 PM Thursday - lunch"},

		// Early morning (very low)
		{3, 0, time.Friday, true, false, "3 AM Friday - night"},

		// Weekend (lower activity)
		{10, 0, time.Saturday, false, false, "10 AM Saturday - weekend"},
		{14, 0, time.Sunday, false, false, "2 PM Sunday - weekend"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			testTime := time.Date(2024, 6, 10+int(tc.dayOfWeek), tc.hour, tc.minute, 0, 0, time.UTC)
			level := profile.GetActivityLevel(testTime)

			if tc.expectLow && level > 0.1 {
				t.Errorf("Expected low activity, got %f", level)
			}
			if tc.expectHigh && level < 0.5 {
				t.Errorf("Expected high activity, got %f", level)
			}
			if level < 0 || level > 1 {
				t.Errorf("Activity level should be in [0, 1], got %f", level)
			}
		})
	}
}

func TestGlobalProfile(t *testing.T) {
	profile, err := Get("global", "UTC")
	if err != nil {
		t.Fatalf("Failed to get profile: %v", err)
	}

	// Global profile should never drop below minimum threshold
	for hour := 0; hour < 24; hour++ {
		testTime := time.Date(2024, 6, 10, hour, 0, 0, 0, time.UTC)
		level := profile.GetActivityLevel(testTime)

		// Global should have minimum 30% activity
		if level < 0.3 {
			t.Errorf("Global profile at hour %d should have min 30%% activity, got %f", hour, level)
		}
		if level < 0 || level > 1 {
			t.Errorf("Activity level should be in [0, 1], got %f", level)
		}
	}
}

func TestStoreRegionalProfile(t *testing.T) {
	profile, err := Get("store-regional", "UTC")
	if err != nil {
		t.Fatalf("Failed to get profile: %v", err)
	}

	// Test evening peak
	eveningTime := time.Date(2024, 6, 10, 20, 0, 0, 0, time.UTC) // 8 PM
	eveningLevel := profile.GetActivityLevel(eveningTime)

	// Test night low
	nightTime := time.Date(2024, 6, 10, 4, 0, 0, 0, time.UTC) // 4 AM
	nightLevel := profile.GetActivityLevel(nightTime)

	if eveningLevel <= nightLevel {
		t.Errorf("Evening should have higher activity than night: evening=%f, night=%f",
			eveningLevel, nightLevel)
	}
}

func TestStoreGlobalProfile(t *testing.T) {
	profile, err := Get("store-global", "UTC")
	if err != nil {
		t.Fatalf("Failed to get profile: %v", err)
	}

	// Should have higher base activity
	for hour := 0; hour < 24; hour++ {
		testTime := time.Date(2024, 6, 10, hour, 0, 0, 0, time.UTC)
		level := profile.GetActivityLevel(testTime)

		// Store global should have minimum 40% activity
		if level < 0.4 {
			t.Errorf("Store global profile at hour %d should have min 40%% activity, got %f",
				hour, level)
		}
	}
}

func TestProfileActivityLevelRange(t *testing.T) {
	profiles := []string{"local-office", "global", "store-regional", "store-global"}

	for _, profileName := range profiles {
		t.Run(profileName, func(t *testing.T) {
			profile, err := Get(profileName, "UTC")
			if err != nil {
				t.Fatalf("Failed to get profile: %v", err)
			}

			// Test over entire week
			startTime := time.Date(2024, 6, 10, 0, 0, 0, 0, time.UTC)
			for i := 0; i < 7*24*4; i++ { // Every 15 minutes for a week
				testTime := startTime.Add(time.Duration(i) * 15 * time.Minute)
				level := profile.GetActivityLevel(testTime)

				if level < 0 {
					t.Errorf("%s: Activity level should not be negative at %v, got %f",
						profileName, testTime, level)
				}
				// Values above 1.0 are allowed for store profiles (weekend boosts)
				// according to the Profile interface documentation
				if level > 2 {
					t.Errorf("%s: Activity level should not exceed 2 at %v, got %f",
						profileName, testTime, level)
				}
			}
		})
	}
}

func TestProfileWithDifferentTimezones(t *testing.T) {
	timezones := []string{"UTC", "America/New_York", "Europe/London", "Asia/Tokyo", "Local"}

	for _, tz := range timezones {
		t.Run(tz, func(t *testing.T) {
			profile, err := Get("local-office", tz)
			if err != nil {
				t.Fatalf("Failed to get profile with timezone %s: %v", tz, err)
			}

			// Should work without error
			now := time.Now()
			level := profile.GetActivityLevel(now)
			if level < 0 || level > 1 {
				t.Errorf("Invalid activity level for timezone %s: %f", tz, level)
			}
		})
	}
}

func TestProfileConsistency(t *testing.T) {
	profile, err := Get("local-office", "UTC")
	if err != nil {
		t.Fatalf("Failed to get profile: %v", err)
	}

	// Same time should always produce same activity level
	testTime := time.Date(2024, 6, 10, 10, 30, 0, 0, time.UTC)

	level1 := profile.GetActivityLevel(testTime)
	level2 := profile.GetActivityLevel(testTime)

	if level1 != level2 {
		t.Errorf("Same time should produce same level: %f != %f", level1, level2)
	}
}

// Benchmarks

func BenchmarkGetActivityLevel(b *testing.B) {
	profile, _ := Get("local-office", "UTC")
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		profile.GetActivityLevel(now)
	}
}

func BenchmarkGetProfile(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Get("local-office", "UTC")
	}
}
