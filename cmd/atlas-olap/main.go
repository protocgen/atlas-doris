// Command atlas-olap provides the unified Atlas-compatible OLAP schema migration
// driver, differ, linter, executor, parity verifier, and external_schema plugin
// for Apache Doris 4.x, StarRocks 4.x, and Apache Iceberg.
package main

import (
	"context"
	"io"
	"os"

	"github.com/protocgen/atlas-doris/pkg/cli"
	"github.com/protocgen/atlas-doris/pkg/external"
)

var exitFunc = os.Exit

func main() {
	exitFunc(Run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

// Run executes the atlas-olap CLI with the given arguments and output writers.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return cli.Run(ctx, cli.Config{
		BinaryName:    "atlas-olap",
		DefaultTarget: external.TargetAll,
	}, args, stdout, stderr)
}
