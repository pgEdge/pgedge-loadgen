// Package version provides build and version information for pgedge-loadgen.
package version

import (
	"fmt"
	"runtime"
)

// Build information set at compile time via ldflags.
var (
	Version   = "1.0.0-alpha2"
	Commit    = "unknown"
	BuildDate = "unknown"
)

// Info returns formatted version information.
func Info() string {
	return fmt.Sprintf(
		"pgedge-loadgen %s (commit: %s, built: %s, go: %s)",
		Version, Commit, BuildDate, runtime.Version(),
	)
}

// Short returns just the version string.
func Short() string {
	return Version
}
