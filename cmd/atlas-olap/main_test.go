package main

import (
	"bytes"
	"context"
	"os"
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

	origExit := exitFunc
	origArgs := os.Args
	defer func() {
		exitFunc = origExit
		os.Args = origArgs
	}()
	gotCode := -1
	exitFunc = func(c int) { gotCode = c }
	os.Args = []string{"atlas-olap", "version"}
	main()
	if gotCode != 0 {
		t.Errorf("expected exit code 0 from main(), got %d", gotCode)
	}
}
