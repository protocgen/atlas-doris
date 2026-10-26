// Command atlas-starrocks provides the StarRocks 4.x specialized Atlas-compatible
// schema migration driver, differ, linter, executor, and external_schema plugin
// with DefaultTarget set to TargetStarRocks.
package main

import (
	"context"
	"io"
	"os"

	"github.com/protocgen/atlas-doris/pkg/cli"
	"github.com/protocgen/atlas-doris/pkg/external"
)

func main() {
	os.Exit(Run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

// Run executes the atlas-starrocks CLI with the given arguments and output writers.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return cli.Run(ctx, cli.Config{
		BinaryName:    "atlas-starrocks",
		DefaultTarget: external.TargetStarRocks,
	}, args, stdout, stderr)
}
