// Package main is the entry point for pgedge-loadgen.
package main

import (
	"fmt"
	"os"

	"github.com/pgEdge/pgedge-loadgen/internal/cli"

	// Register applications
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/analytics"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/brokerage"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/docmgmt"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/ecommerce"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/knowledgebase"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/retail"
	_ "github.com/pgEdge/pgedge-loadgen/internal/apps/wholesale"
)

func main() {
	if err := cli.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
