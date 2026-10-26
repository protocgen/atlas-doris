package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestAtlasOlapMainRun(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := Run(context.Background(), []string{"version"}, &out, &errBuf); code != 0 {
		t.Fatalf("Run(version) failed: %d (%s)", code, errBuf.String())
	}
	if strings.TrimSpace(out.String()) != "atlas-olap v0.1.0" {
		t.Errorf("unexpected version output: %q", out.String())
	}
}
