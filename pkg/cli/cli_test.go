package cli_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/protocgen/atlas-doris/pkg/cli"
	"github.com/protocgen/atlas-doris/pkg/doris"
	"github.com/protocgen/atlas-doris/pkg/external"
)

func TestCLI_MultiBinaryConfigsAndDefaultTargets(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	dorisSQL := `
CREATE TABLE IF NOT EXISTS internal.tenant_a.patients (
    patient_id VARCHAR(64) NOT NULL,
    notes TEXT,
    publication_generation BIGINT NOT NULL DEFAULT "0"
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS 8
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "publication_generation"
);`

	starrocksSQL := `
CREATE TABLE IF NOT EXISTS tenant_a.patients (
    patient_id VARCHAR(64) NOT NULL,
    notes STRING,
    publication_generation BIGINT NOT NULL DEFAULT "0"
)
PRIMARY KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS 8
PROPERTIES (
    "enable_persistent_index" = "true",
    "function_column.sequence_col" = "publication_generation"
);`

	if err := os.WriteFile(filepath.Join(tmpDir, "patients_doris.sql"), []byte(dorisSQL), 0o644); err != nil {
		t.Fatalf("write doris sql: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "patients_starrocks.sql"), []byte(starrocksSQL), 0o644); err != nil {
		t.Fatalf("write starrocks sql: %v", err)
	}

	// 1. Version output for each binary configuration (and empty default config)
	for _, tc := range []struct {
		cfg  cli.Config
		want string
	}{
		{cli.Config{}, "atlas-olap v0.1.0"},
		{cli.Config{BinaryName: "atlas-olap", DefaultTarget: external.TargetAll}, "atlas-olap v0.1.0"},
		{cli.Config{BinaryName: "atlas-doris", DefaultTarget: external.TargetAll}, "atlas-doris v0.1.0"},
		{cli.Config{BinaryName: "atlas-starrocks", DefaultTarget: external.TargetStarRocks}, "atlas-starrocks v0.1.0"},
	} {
		var out, errBuf bytes.Buffer
		if code := cli.Run(ctx, tc.cfg, []string{"version"}, &out, &errBuf); code != 0 {
			t.Fatalf("version failed for %+v: code=%d stderr=%s", tc.cfg, code, errBuf.String())
		}
		if strings.TrimSpace(out.String()) != tc.want {
			t.Errorf("version output = %q, want %q", strings.TrimSpace(out.String()), tc.want)
		}
	}

	// 2. DefaultTarget behavior on schema inspect and external-schema for atlas-starrocks
	srCfg := cli.Config{BinaryName: "atlas-starrocks", DefaultTarget: external.TargetStarRocks}
	var srOut, srErr bytes.Buffer
	if code := cli.Run(ctx, srCfg, []string{"schema", "inspect", "--url", tmpDir}, &srOut, &srErr); code != 0 {
		t.Fatalf("atlas-starrocks schema inspect failed: %s", srErr.String())
	}
	if !strings.Contains(srOut.String(), "PRIMARY KEY(patient_id)") || strings.Contains(srOut.String(), "UNIQUE KEY(patient_id)") {
		t.Errorf("expected atlas-starrocks schema inspect to default to TargetStarRocks, got:\n%s", srOut.String())
	}

	srOut.Reset()
	srErr.Reset()
	if code := cli.Run(ctx, srCfg, []string{"external-schema", "--sql-dir", tmpDir}, &srOut, &srErr); code != 0 {
		t.Fatalf("atlas-starrocks external-schema failed: %s", srErr.String())
	}
	if !strings.Contains(srOut.String(), "PRIMARY KEY(patient_id)") || strings.Contains(srOut.String(), "UNIQUE KEY(patient_id)") {
		t.Errorf("expected atlas-starrocks external-schema to default to TargetStarRocks, got:\n%s", srOut.String())
	}
}

func TestCLI_SchemaParity_PassAndFailureCases(t *testing.T) {
	ctx := context.Background()
	olapCfg := cli.Config{BinaryName: "atlas-olap", DefaultTarget: external.TargetAll}
	tmpDir := t.TempDir()

	dorisSQL := `
CREATE TABLE IF NOT EXISTS internal.tenant_a.patients (
    patient_id VARCHAR(64) NOT NULL,
    notes TEXT,
    payload JSON,
    publication_generation BIGINT NOT NULL DEFAULT "0"
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS 8
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "publication_generation"
);`

	starrocksSQL := `
CREATE TABLE IF NOT EXISTS default_catalog.tenant_a.patients (
    patient_id VARCHAR(64) NOT NULL,
    notes STRING,
    payload JSON,
    publication_generation BIGINT NOT NULL DEFAULT "0"
)
PRIMARY KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS 8
PROPERTIES (
    "enable_persistent_index" = "true",
    "function_column.sequence_col" = "publication_generation"
);`

	icebergSQL := `
CREATE TABLE IF NOT EXISTS azra_lake.tenant_a.patients (
    patient_id VARCHAR(64) NOT NULL,
    notes STRING,
    payload STRING,
    publication_generation BIGINT NOT NULL
)
PROPERTIES (
    "compression_codec" = "zstd"
);`

	dorisFile := filepath.Join(tmpDir, "patients_doris.sql")
	srFile := filepath.Join(tmpDir, "patients_starrocks.sql")
	iceFile := filepath.Join(tmpDir, "patients_iceberg.sql")
	if err := os.WriteFile(dorisFile, []byte(dorisSQL), 0o644); err != nil {
		t.Fatalf("write dorisFile: %v", err)
	}
	if err := os.WriteFile(srFile, []byte(starrocksSQL), 0o644); err != nil {
		t.Fatalf("write srFile: %v", err)
	}
	if err := os.WriteFile(iceFile, []byte(icebergSQL), 0o644); err != nil {
		t.Fatalf("write iceFile: %v", err)
	}

	// 1. Passing parity check via --dir (includes Doris, StarRocks, and Iceberg)
	var out, errBuf bytes.Buffer
	if code := cli.Run(ctx, olapCfg, []string{"schema", "parity", "--dir", tmpDir}, &out, &errBuf); code != 0 {
		t.Fatalf("schema parity --dir failed (code=%d): %s", code, errBuf.String())
	}
	if !strings.Contains(out.String(), "schema parity passed (1 table(s) verified)") {
		t.Errorf("unexpected schema parity stdout: %q", out.String())
	}

	// 2. Passing parity check via explicit --doris, --starrocks, --iceberg flags
	out.Reset()
	errBuf.Reset()
	if code := cli.Run(ctx, olapCfg, []string{"schema", "parity", "--doris", dorisFile, "--starrocks", srFile, "--iceberg", iceFile}, &out, &errBuf); code != 0 {
		t.Fatalf("schema parity explicit files failed (code=%d): %s", code, errBuf.String())
	}

	// 3. Passing parity check via --dir when no Iceberg file is present
	noIceDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(noIceDir, "patients_doris.sql"), []byte(dorisSQL), 0o644)
	_ = os.WriteFile(filepath.Join(noIceDir, "patients_starrocks.sql"), []byte(starrocksSQL), 0o644)
	out.Reset()
	errBuf.Reset()
	if code := cli.Run(ctx, olapCfg, []string{"schema", "parity", "--dir", noIceDir}, &out, &errBuf); code != 0 {
		t.Fatalf("schema parity without iceberg failed (code=%d): %s", code, errBuf.String())
	}

	// 4. Failing parity checks: column type mismatch, nullability mismatch, key column mismatch, sequence_col mismatch, missing table
	badSRFile := filepath.Join(t.TempDir(), "bad_starrocks.sql")
	badSRSQL := `
CREATE TABLE IF NOT EXISTS tenant_a.patients (
    patient_id VARCHAR(64) NOT NULL,
    notes BIGINT NOT NULL,
    payload JSON,
    extra_col VARCHAR(16),
    publication_generation BIGINT NOT NULL DEFAULT "0"
)
DUPLICATE KEY(patient_id, notes)
DISTRIBUTED BY HASH(patient_id) BUCKETS 8
PROPERTIES (
    "function_column.sequence_col" = "other_seq"
);
CREATE TABLE IF NOT EXISTS tenant_a.sr_only_table (
    id BIGINT NOT NULL
)
PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4;`
	_ = os.WriteFile(badSRFile, []byte(badSRSQL), 0o644)

	out.Reset()
	errBuf.Reset()
	if code := cli.Run(ctx, olapCfg, []string{"schema", "parity", "--doris", dorisFile, "--starrocks", badSRFile}, &out, &errBuf); code == 0 {
		t.Fatalf("expected schema parity to fail on mismatched StarRocks schema")
	}
	errStr := errBuf.String()
	for _, wantSub := range []string{
		"column count mismatch",
		"type mismatch",
		"nullability mismatch",
		"incompatible key models",
		"key columns mismatch",
		"sequence_col mismatch",
		"sr_only_table present in StarRocks schema but missing in Doris schema",
	} {
		if !strings.Contains(errStr, wantSub) {
			t.Errorf("expected parity error output to contain %q, got:\n%s", wantSub, errStr)
		}
	}

	// 5. Direct unit test of CheckSchemaParity edge cases (nil schemas, empty schemas, column order mismatch, Iceberg mismatches)
	if m := cli.CheckSchemaParity(nil, nil, nil); len(m) == 0 {
		t.Errorf("expected mismatch for nil schemas")
	}
	if m := cli.CheckSchemaParity(doris.NewSchema(), doris.NewSchema(), nil); len(m) == 0 {
		t.Errorf("expected mismatch for empty schemas")
	}

	sDoris, _ := doris.ParseSchema(dorisSQL)
	sReorderedSR, _ := doris.ParseSchema(`
CREATE TABLE IF NOT EXISTS tenant_a.patients (
    patient_id VARCHAR(64) NOT NULL,
    payload JSON,
    notes STRING,
    publication_generation BIGINT NOT NULL DEFAULT "0"
)
PRIMARY KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS 8
PROPERTIES (
    "enable_persistent_index" = "true",
    "function_column.sequence_col" = "publication_generation"
);`)
	sBadIceberg, _ := doris.ParseSchema(`
CREATE TABLE IF NOT EXISTS azra_lake.tenant_a.patients (
    patient_id VARCHAR(64) NOT NULL,
    payload BIGINT,
    notes STRING
)
PROPERTIES (
    "compression_codec" = "zstd"
);
CREATE TABLE IF NOT EXISTS azra_lake.tenant_a.ice_only (
    id BIGINT NOT NULL
)
PROPERTIES (
    "compression_codec" = "zstd"
);`)
	mismatches := cli.CheckSchemaParity(sDoris, sReorderedSR, sBadIceberg)
	joined := strings.Join(mismatches, "\n")
	for _, want := range []string{
		"order/name mismatch",
		"Iceberg column count mismatch",
		"Iceberg type mismatch",
		"ice_only present in Iceberg schema but missing in Doris schema",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("expected CheckSchemaParity to report %q, got:\n%s", want, joined)
		}
	}
}

func TestRealProto2Type_SchemaParity(t *testing.T) {
	proto2typeDir := filepath.Clean(filepath.Join("..", "..", "..", "proto2type"))
	if _, err := os.Stat(proto2typeDir); os.IsNotExist(err) {
		proto2typeDir = filepath.Clean(filepath.Join("..", "..", "proto2type"))
	}
	protoFile := filepath.Join(proto2typeDir, "testdata", "doris_tenant_profile.proto")
	if _, err := os.Stat(protoFile); os.IsNotExist(err) {
		t.Skip("skipping real proto2type parity test: doris_tenant_profile.proto not found")
	}

	ctx := context.Background()
	olapCfg := cli.Config{BinaryName: "atlas-olap", DefaultTarget: external.TargetAll}
	var out, errBuf bytes.Buffer
	code := cli.Run(ctx, olapCfg, []string{
		"schema", "parity",
		"--doris", "proto://" + protoFile,
		"--starrocks", "proto://" + protoFile,
		"--iceberg", "proto://" + protoFile,
		"--include", proto2typeDir,
	}, &out, &errBuf)
	if code != 0 {
		t.Fatalf("schema parity on real proto2type failed (code=%d): %s", code, errBuf.String())
	}
	if !strings.Contains(out.String(), "schema parity passed") {
		t.Errorf("unexpected schema parity stdout: %q", out.String())
	}
}
