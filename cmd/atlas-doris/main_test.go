package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLI_FullMigrationWorkflow(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	migDir := filepath.Join(tmpDir, "migrations")
	if err := os.MkdirAll(migDir, 0o755); err != nil {
		t.Fatalf("mkdir migrations: %v", err)
	}

	// Write empty baseline 00000000000000_tenant.sql
	if err := os.WriteFile(filepath.Join(migDir, "00000000000000_tenant.sql"), nil, 0o644); err != nil {
		t.Fatalf("write baseline: %v", err)
	}

	// 1. migrate hash
	out, errOut, code := runCLI(ctx, "migrate", "hash", "--dir", "file://"+migDir)
	if code != 0 {
		t.Fatalf("migrate hash failed (code=%d): %s", code, errOut)
	}
	if !strings.Contains(out, "atlas.sum updated") {
		t.Errorf("unexpected migrate hash output: %q", out)
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

	// 3. migrate diff (positional name before flags)
	out, errOut, code = runCLI(ctx, "migrate", "diff", "initial_schema", "--dir", "file://"+migDir, "--to", "file://"+v1File, "--version", "20261024100000")
	if code != 0 {
		t.Fatalf("migrate diff v1 failed (code=%d): %s", code, errOut)
	}
	if !strings.Contains(out, "Created migration 20261024100000_initial_schema.sql") {
		t.Errorf("unexpected migrate diff output: %q", out)
	}

	// 4. migrate validate
	out, errOut, code = runCLI(ctx, "migrate", "validate", "--dir", "file://"+migDir)
	if code != 0 {
		t.Fatalf("migrate validate failed (code=%d): %s", code, errOut)
	}
	if !strings.Contains(out, "migrations directory is valid") {
		t.Errorf("unexpected migrate validate output: %q", out)
	}

	// 5. migrate lint
	out, errOut, code = runCLI(ctx, "migrate", "lint", "--dir", "file://"+migDir, "--to", "file://"+v1File)
	if code != 0 {
		t.Fatalf("migrate lint failed (code=%d): %s", code, errOut)
	}
	if !strings.Contains(out, "migrate lint passed") {
		t.Errorf("unexpected migrate lint output: %q", out)
	}

	// 6. Write v2 schema (widen mrn -> VARCHAR(64) and add status column)
	v2File := filepath.Join(tmpDir, "schema_v2_doris.sql")
	v2SQL := strings.Replace(v1SQL, "mrn VARCHAR(32) NOT NULL DEFAULT \"\",", "mrn VARCHAR(64) NOT NULL DEFAULT \"\",", 1)
	v2SQL = strings.Replace(v2SQL, "publication_generation BIGINT NOT NULL DEFAULT \"0\",", "publication_generation BIGINT NOT NULL DEFAULT \"0\",\n    status VARCHAR(32) NOT NULL DEFAULT \"ACTIVE\",", 1)
	if err := os.WriteFile(v2File, []byte(v2SQL), 0o644); err != nil {
		t.Fatalf("write v2 schema: %v", err)
	}

	// 7. schema diff between v1 and v2
	out, errOut, code = runCLI(ctx, "schema", "diff", "--from", "file://"+v1File, "--to", "file://"+v2File)
	if code != 0 {
		t.Fatalf("schema diff failed (code=%d): %s", code, errOut)
	}
	if !strings.Contains(out, "ALTER TABLE internal.tenant_${TENANT_SLUG}.patients ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT \"ACTIVE\";") ||
		!strings.Contains(out, "ALTER TABLE internal.tenant_${TENANT_SLUG}.patients MODIFY COLUMN mrn VARCHAR(64) NOT NULL DEFAULT \"\";") {
		t.Errorf("unexpected schema diff output:\n%s", out)
	}

	// 8. migrate diff v2
	out, errOut, code = runCLI(ctx, "migrate", "diff", "widen_mrn_add_status", "--dir", "file://"+migDir, "--to", "file://"+v2File, "--version", "20261024100001")
	if code != 0 {
		t.Fatalf("migrate diff v2 failed (code=%d): %s", code, errOut)
	}

	// 9. migrate apply --dry-run with --var substitution
	out, errOut, code = runCLI(ctx, "migrate", "apply", "--dir", "file://"+migDir, "--dry-run", "--baseline", "00000000000000", "--var", "TENANT_SLUG=akron_general", "--var", "TENANT_BUCKETS=8")
	if code != 0 {
		t.Fatalf("migrate apply --dry-run failed (code=%d): %s", code, errOut)
	}
	if !strings.Contains(out, "CREATE TABLE IF NOT EXISTS internal.tenant_akron_general.patients") ||
		!strings.Contains(out, "BUCKETS 8") ||
		!strings.Contains(out, "ALTER TABLE internal.tenant_akron_general.patients ADD COLUMN status") {
		t.Errorf("unexpected migrate apply --dry-run output:\n%s", out)
	}

	// 10. schema inspect and external-schema (both SQL and HCL formats)
	outInspect, errOut, code := runCLI(ctx, "schema", "inspect", "--url", "file://"+migDir)
	if code != 0 {
		t.Fatalf("schema inspect failed (code=%d): %s", code, errOut)
	}
	outExt, errOut, code := runCLI(ctx, "external-schema", "--sql-file", v2File)
	if code != 0 {
		t.Fatalf("external-schema failed (code=%d): %s", code, errOut)
	}
	if outInspect != outExt {
		t.Errorf("replayed migration chain schema != desired v2 schema:\ninspect:\n%s\nexternal:\n%s", outInspect, outExt)
	}

	outInspectHCL, errOut, code := runCLI(ctx, "schema", "inspect", "--url", "file://"+migDir, "--format", "hcl")
	if code != 0 {
		t.Fatalf("schema inspect --format=hcl failed (code=%d): %s", code, errOut)
	}
	if !strings.Contains(outInspectHCL, `table "patients"`) {
		t.Errorf("expected HCL table block in schema inspect --format=hcl output:\n%s", outInspectHCL)
	}

	hclFile := filepath.Join(tmpDir, "schema_v2_doris.hcl")
	if err := os.WriteFile(hclFile, []byte(outInspectHCL), 0o644); err != nil {
		t.Fatalf("write hclFile: %v", err)
	}
	outExtFromHCL, errOut, code := runCLI(ctx, "external-schema", "--hcl-file", hclFile)
	if code != 0 {
		t.Fatalf("external-schema --hcl-file failed (code=%d): %s", code, errOut)
	}
	if outExtFromHCL != outInspect {
		t.Errorf("external-schema --hcl-file SQL != inspected SQL:\ngot:\n%s\nwant:\n%s", outExtFromHCL, outInspect)
	}

	// 11. version command
	outVer, _, code := runCLI(ctx, "version")
	if code != 0 || !strings.Contains(outVer, "atlas-doris") {
		t.Errorf("unexpected version output: %q (code=%d)", outVer, code)
	}

	// 12. help command
	outHelp, _, code := runCLI(ctx, "help")
	if code != 0 || !strings.Contains(outHelp, "Usage:") {
		t.Errorf("unexpected help output: %q (code=%d)", outHelp, code)
	}

	// 13. migrate diff no-op when already synced
	outNoop, errOut, code := runCLI(ctx, "migrate", "diff", "noop_sync", "--dir", "file://"+migDir, "--to", "file://"+v2File)
	if code != 0 || !strings.Contains(outNoop, "no changes to be made") {
		t.Errorf("expected no changes output on synced migrate diff, got code=%d out=%q err=%q", code, outNoop, errOut)
	}

	// 14. schema diff no-op when identical
	outDiffNoop, errOut, code := runCLI(ctx, "schema", "diff", "--from", "file://"+v2File, "--to", "file://"+v2File)
	if code != 0 || !strings.Contains(outDiffNoop, "Schemas are identical") {
		t.Errorf("expected Schemas are identical, got code=%d out=%q err=%q", code, outDiffNoop, errOut)
	}

	// 15. CLI validation and error paths
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
		{"migrate", "lint", "--dir", "file://" + migDir, "--to", "file:///nonexistent.sql"},
		{"migrate", "status"},
		{"migrate", "status", "--unknown-flag"},
		{"migrate", "status", "--dir", "file://" + migDir, "--url", "invalid://url"},
		{"migrate", "apply"},
		{"migrate", "apply", "--unknown-flag"},
		{"migrate", "apply", "--dir", "file://" + migDir},
		{"migrate", "apply", "--dir", "file://" + migDir, "--url", "invalid://url"},
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
		{"external-schema"},
		{"external-schema", "--unknown-flag"},
	}
	for _, tc := range errorCases {
		if _, _, c := runCLI(ctx, tc...); c == 0 {
			t.Errorf("expected non-zero exit code for args %v", tc)
		}
	}
}

func runCLI(ctx context.Context, args ...string) (string, string, int) {
	var stdout, stderr bytes.Buffer
	code := Run(ctx, args, &stdout, &stderr)
	return stdout.String(), stderr.String(), code
}
