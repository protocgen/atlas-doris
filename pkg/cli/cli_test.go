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

func TestCLI_FullMigrationWorkflowAndErrorPaths(t *testing.T) {
	ctx := context.Background()
	cfg := cli.Config{BinaryName: "atlas-olap", DefaultTarget: external.TargetAll}
	runCLI := func(args ...string) (string, string, int) {
		var stdout, stderr bytes.Buffer
		c := cli.Run(ctx, cfg, args, &stdout, &stderr)
		return stdout.String(), stderr.String(), c
	}

	tmpDir := t.TempDir()
	migDir := filepath.Join(tmpDir, "migrations")
	if err := os.MkdirAll(migDir, 0o755); err != nil {
		t.Fatalf("mkdir migrations: %v", err)
	}
	if err := os.WriteFile(filepath.Join(migDir, "00000000000000_tenant.sql"), nil, 0o644); err != nil {
		t.Fatalf("write baseline: %v", err)
	}

	// 1. migrate hash
	out, errOut, code := runCLI("migrate", "hash", "--dir", "file://"+migDir)
	if code != 0 || !strings.Contains(out, "atlas.sum updated") {
		t.Fatalf("migrate hash failed (code=%d): %s", code, errOut)
	}

	// 2. Write desired v1 schema SQL file
	v1File := filepath.Join(tmpDir, "schema_v1_doris.sql")
	v1SQL := `
CREATE DATABASE IF NOT EXISTS internal.tenant_${TENANT_SLUG};

CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patients (
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(32) NOT NULL DEFAULT "",
    publication_generation BIGINT NOT NULL DEFAULT "0",
    INDEX idx_patients_mrn (mrn) USING INVERTED
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS ${TENANT_BUCKETS}
PROPERTIES (
    "storage_vault_name" = "azra_lakehouse_vault",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "publication_generation",
    "colocate_with" = "azra_patient_group"
);`
	if err := os.WriteFile(v1File, []byte(v1SQL), 0o644); err != nil {
		t.Fatalf("write v1 schema: %v", err)
	}

	// 3. migrate diff
	out, errOut, code = runCLI("migrate", "diff", "initial_schema", "--dir", "file://"+migDir, "--to", "file://"+v1File, "--version", "20261024100000")
	if code != 0 || !strings.Contains(out, "Created migration 20261024100000_initial_schema.sql") {
		t.Fatalf("migrate diff v1 failed (code=%d): %s", code, errOut)
	}

	// 4. migrate validate & lint
	if out, errOut, code = runCLI("migrate", "validate", "--dir", "file://"+migDir); code != 0 || !strings.Contains(out, "migrations directory is valid") {
		t.Fatalf("migrate validate failed (code=%d): %s", code, errOut)
	}
	if out, errOut, code = runCLI("migrate", "lint", "--dir", "file://"+migDir, "--to", "file://"+v1File, "--include", tmpDir); code != 0 || !strings.Contains(out, "migrate lint passed") {
		t.Fatalf("migrate lint failed (code=%d): %s", code, errOut)
	}

	// 5. Write v2 schema
	v2File := filepath.Join(tmpDir, "schema_v2_doris.sql")
	v2SQL := strings.Replace(v1SQL, "mrn VARCHAR(32) NOT NULL DEFAULT \"\",", "mrn VARCHAR(64) NOT NULL DEFAULT \"\",", 1)
	v2SQL = strings.Replace(v2SQL, "publication_generation BIGINT NOT NULL DEFAULT \"0\",", "publication_generation BIGINT NOT NULL DEFAULT \"0\",\n    status VARCHAR(32) NOT NULL DEFAULT \"ACTIVE\",", 1)
	if err := os.WriteFile(v2File, []byte(v2SQL), 0o644); err != nil {
		t.Fatalf("write v2 schema: %v", err)
	}

	// 6. schema diff & migrate diff v2
	if out, errOut, code = runCLI("schema", "diff", "--from", "file://"+v1File, "--to", "file://"+v2File); code != 0 || !strings.Contains(out, "ADD COLUMN status") {
		t.Fatalf("schema diff failed (code=%d): %s", code, errOut)
	}
	if _, errOut, code = runCLI("migrate", "diff", "widen_mrn_add_status", "--dir", "file://"+migDir, "--to", "file://"+v2File, "--version", "20261024100001"); code != 0 {
		t.Fatalf("migrate diff v2 failed (code=%d): %s", code, errOut)
	}

	// 7. migrate apply --dry-run
	if out, errOut, code = runCLI("migrate", "apply", "--dir", "file://"+migDir, "--dry-run", "--baseline", "00000000000000", "--var", "TENANT_SLUG=akron_general", "--var", "TENANT_BUCKETS=8"); code != 0 || !strings.Contains(out, "BUCKETS 8") {
		t.Fatalf("migrate apply --dry-run failed (code=%d): %s", code, errOut)
	}

	// 8. schema inspect, external-schema, help, no-ops
	outInspect, errOut, code := runCLI("schema", "inspect", "--url", "file://"+migDir)
	if code != 0 {
		t.Fatalf("schema inspect failed (code=%d): %s", code, errOut)
	}
	outExt, errOut, code := runCLI("external-schema", "--sql-file", v2File)
	if code != 0 || outInspect != outExt {
		t.Fatalf("external-schema mismatch (code=%d): %s", code, errOut)
	}
	outInspectHCL, errOut, code := runCLI("schema", "inspect", "--url", "file://"+migDir, "--format", "hcl")
	if code != 0 || !strings.Contains(outInspectHCL, `table "patients"`) {
		t.Fatalf("schema inspect --format=hcl failed (code=%d): %s", code, errOut)
	}
	hclFile := filepath.Join(tmpDir, "schema_v2_doris.hcl")
	_ = os.WriteFile(hclFile, []byte(outInspectHCL), 0o644)
	if outExtHCL, errOut, code := runCLI("external-schema", "--hcl-file", hclFile); code != 0 || outExtHCL != outInspect {
		t.Fatalf("external-schema --hcl-file failed (code=%d): %s", code, errOut)
	}
	if outHelp, _, code := runCLI("help"); code != 0 || !strings.Contains(outHelp, "Usage:") {
		t.Errorf("unexpected help output: %q (code=%d)", outHelp, code)
	}
	if outNoop, _, code := runCLI("migrate", "diff", "noop_sync", "--dir", "file://"+migDir, "--to", "file://"+v2File); code != 0 || !strings.Contains(outNoop, "no changes to be made") {
		t.Errorf("expected no changes on synced migrate diff, got code=%d out=%q", code, outNoop)
	}
	if outDiffNoop, _, code := runCLI("schema", "diff", "--from", "file://"+v2File, "--to", "file://"+v2File); code != 0 || !strings.Contains(outDiffNoop, "Schemas are identical") {
		t.Errorf("expected Schemas are identical, got code=%d out=%q", code, outDiffNoop)
	}

	// 9. Lint violation & diff error paths (destructive drop without --allow-destructive)
	emptyFile := filepath.Join(tmpDir, "empty_doris.sql")
	_ = os.WriteFile(emptyFile, []byte("CREATE DATABASE IF NOT EXISTS internal.tenant_${TENANT_SLUG};\n"), 0o644)
	if _, _, c := runCLI("schema", "diff", "--from", "file://"+v2File, "--to", "file://"+emptyFile); c == 0 {
		t.Errorf("expected schema diff to fail on destructive drop without --allow-destructive")
	}
	if _, _, c := runCLI("migrate", "diff", "drop_it", "--dir", "file://"+migDir, "--to", "file://"+emptyFile); c == 0 {
		t.Errorf("expected migrate diff to fail on destructive drop without --allow-destructive")
	}
	if _, _, c := runCLI("migrate", "lint", "--dir", "file://"+migDir, "--to", "file://"+emptyFile); c == 0 {
		t.Errorf("expected migrate lint to fail on destructive drop without --allow-destructive")
	}

	// 10. All CLI validation and error paths
	errorCases := [][]string{
		{},
		{"unknown-cmd"},
		{"migrate"},
		{"migrate", "unknown-sub"},
		{"migrate", "diff"},
		{"migrate", "diff", "--unknown-flag"},
		{"migrate", "diff", "bad", "--dir", "file://" + migDir, "--to", "file:///nonexistent.sql", "--", "extra"},
		{"migrate", "validate"},
		{"migrate", "validate", "--unknown-flag"},
		{"migrate", "validate", "--dir", "file:///nonexistent_dir"},
		{"migrate", "hash"},
		{"migrate", "hash", "--unknown-flag"},
		{"migrate", "hash", "--dir", "file:///nonexistent_dir"},
		{"migrate", "lint"},
		{"migrate", "lint", "--unknown-flag"},
		{"migrate", "lint", "--dir", "file:///nonexistent_dir"},
		{"migrate", "lint", "--dir", "file://" + migDir, "--to", "file:///nonexistent.sql"},
		{"migrate", "status"},
		{"migrate", "status", "--unknown-flag"},
		{"migrate", "status", "--dir", "file://" + migDir, "--url", "invalid://url"},
		{"migrate", "apply"},
		{"migrate", "apply", "--unknown-flag"},
		{"migrate", "apply", "--dir", "file://" + migDir},
		{"migrate", "apply", "--dir", "file://" + migDir, "--url", "invalid://url"},
		{"migrate", "apply", "--dir", "file:///nonexistent_dir", "--dry-run"},
		{"migrate", "apply", "--dir", "file://" + migDir, "--dry-run", "--var", "BAD_VAR_NO_EQUALS"},
		{"schema"},
		{"schema", "unknown-sub"},
		{"schema", "inspect"},
		{"schema", "inspect", "--unknown-flag"},
		{"schema", "inspect", "--url", "file:///nonexistent.sql"},
		{"schema", "inspect", "--url", "file://" + v2File, "--format", "invalid"},
		{"schema", "diff"},
		{"schema", "diff", "--unknown-flag"},
		{"schema", "diff", "--from", "file:///nonexistent.sql", "--to", "file://" + v2File},
		{"schema", "diff", "--from", "file://" + v2File, "--to", "file:///nonexistent.sql"},
		{"schema", "parity"},
		{"schema", "parity", "--unknown-flag"},
		{"schema", "parity", "--doris", "/nonexistent.sql", "--starrocks", v2File},
		{"schema", "parity", "--doris", v2File, "--starrocks", "/nonexistent.sql"},
		{"schema", "parity", "--doris", v2File, "--starrocks", v2File, "--iceberg", "/nonexistent.sql"},
		{"external-schema"},
		{"external-schema", "--unknown-flag"},
	}
	for _, tc := range errorCases {
		if _, _, c := runCLI(tc...); c == 0 {
			t.Errorf("expected non-zero exit code for args %v", tc)
		}
	}
}
