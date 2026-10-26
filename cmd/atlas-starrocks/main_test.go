package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAtlasStarRocksMainRun(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := Run(context.Background(), []string{"version"}, &out, &errBuf); code != 0 {
		t.Fatalf("Run(version) failed: %d (%s)", code, errBuf.String())
	}
	if strings.TrimSpace(out.String()) != "atlas-starrocks v0.1.0" {
		t.Errorf("unexpected version output: %q", out.String())
	}

	tmpDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmpDir, "tbl_doris.sql"), []byte("CREATE TABLE t (id BIGINT NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;\n"), 0o644)
	_ = os.WriteFile(filepath.Join(tmpDir, "tbl_starrocks.sql"), []byte("CREATE TABLE t (id BIGINT NOT NULL) PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES (\"enable_persistent_index\" = \"true\");\n"), 0o644)

	out.Reset()
	errBuf.Reset()
	if code := Run(context.Background(), []string{"schema", "inspect", "--url", tmpDir}, &out, &errBuf); code != 0 {
		t.Fatalf("schema inspect failed: %s", errBuf.String())
	}
	if !strings.Contains(out.String(), "PRIMARY KEY(id)") || strings.Contains(out.String(), "UNIQUE KEY(id)") {
		t.Errorf("expected atlas-starrocks to inspect only StarRocks target by default, got:\n%s", out.String())
	}

	origExit := exitFunc
	origArgs := os.Args
	defer func() {
		exitFunc = origExit
		os.Args = origArgs
	}()
	gotCode := -1
	exitFunc = func(c int) { gotCode = c }
	os.Args = []string{"atlas-starrocks", "version"}
	main()
	if gotCode != 0 {
		t.Errorf("expected exit code 0 from main(), got %d", gotCode)
	}
}
