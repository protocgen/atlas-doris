package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/protocgen/atlas-doris/pkg/doris"
)

func TestCLI_FullMigrationWorkflowAndErrorPaths(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	migDir := filepath.Join(tmpDir, "migrations")
	if err := os.MkdirAll(migDir, 0o755); err != nil {
		t.Fatalf("mkdir migrations: %v", err)
	}
	if err := os.WriteFile(filepath.Join(migDir, "00000000000000_tenant.sql"), nil, 0o644); err != nil {
		t.Fatalf("write baseline: %v", err)
	}

	// 1. migrate hash
	out, errOut, code := runCLI(ctx, "migrate", "hash", "--dir", "file://"+migDir)
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
	out, errOut, code = runCLI(ctx, "migrate", "diff", "initial_schema", "--dir", "file://"+migDir, "--to", "file://"+v1File, "--version", "20261024100000")
	if code != 0 || !strings.Contains(out, "Created migration 20261024100000_initial_schema.sql") {
		t.Fatalf("migrate diff v1 failed (code=%d): %s", code, errOut)
	}

	// 4. migrate validate & lint
	if out, errOut, code = runCLI(ctx, "migrate", "validate", "--dir", "file://"+migDir); code != 0 || !strings.Contains(out, "migrations directory is valid") {
		t.Fatalf("migrate validate failed (code=%d): %s", code, errOut)
	}
	if out, errOut, code = runCLI(ctx, "migrate", "lint", "--dir", "file://"+migDir, "--to", "file://"+v1File, "--include", tmpDir); code != 0 || !strings.Contains(out, "migrate lint passed") {
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
	if out, errOut, code = runCLI(ctx, "schema", "diff", "--from", "file://"+v1File, "--to", "file://"+v2File); code != 0 || !strings.Contains(out, "ADD COLUMN status") {
		t.Fatalf("schema diff failed (code=%d): %s", code, errOut)
	}
	if _, errOut, code = runCLI(ctx, "migrate", "diff", "widen_mrn_add_status", "--dir", "file://"+migDir, "--to", "file://"+v2File, "--version", "20261024100001"); code != 0 {
		t.Fatalf("migrate diff v2 failed (code=%d): %s", code, errOut)
	}

	// 7. migrate apply --dry-run
	if out, errOut, code = runCLI(ctx, "migrate", "apply", "--dir", "file://"+migDir, "--dry-run", "--baseline", "00000000000000", "--var", "TENANT_SLUG=akron_general", "--var", "TENANT_BUCKETS=8"); code != 0 || !strings.Contains(out, "BUCKETS 8") {
		t.Fatalf("migrate apply --dry-run failed (code=%d): %s", code, errOut)
	}

	// 8. schema inspect, external-schema, help, version, no-ops
	outInspect, errOut, code := runCLI(ctx, "schema", "inspect", "--url", "file://"+migDir)
	if code != 0 {
		t.Fatalf("schema inspect failed (code=%d): %s", code, errOut)
	}
	outExt, errOut, code := runCLI(ctx, "external-schema", "--sql-file", v2File)
	if code != 0 || outInspect != outExt {
		t.Fatalf("external-schema mismatch (code=%d): %s", code, errOut)
	}
	outInspectHCL, errOut, code := runCLI(ctx, "schema", "inspect", "--url", "file://"+migDir, "--format", "hcl")
	if code != 0 || !strings.Contains(outInspectHCL, `table "patients"`) {
		t.Fatalf("schema inspect --format=hcl failed (code=%d): %s", code, errOut)
	}
	hclFile := filepath.Join(tmpDir, "schema_v2_doris.hcl")
	_ = os.WriteFile(hclFile, []byte(outInspectHCL), 0o644)
	if outExtHCL, errOut, code := runCLI(ctx, "external-schema", "--hcl-file", hclFile); code != 0 || outExtHCL != outInspect {
		t.Fatalf("external-schema --hcl-file failed (code=%d): %s", code, errOut)
	}
	if outVer, _, code := runCLI(ctx, "version"); code != 0 || !strings.Contains(outVer, "atlas-doris") {
		t.Errorf("unexpected version output: %q (code=%d)", outVer, code)
	}
	if outHelp, _, code := runCLI(ctx, "help"); code != 0 || !strings.Contains(outHelp, "Usage:") {
		t.Errorf("unexpected help output: %q (code=%d)", outHelp, code)
	}
	if outNoop, _, code := runCLI(ctx, "migrate", "diff", "noop_sync", "--dir", "file://"+migDir, "--to", "file://"+v2File); code != 0 || !strings.Contains(outNoop, "no changes to be made") {
		t.Errorf("expected no changes on synced migrate diff, got code=%d out=%q", code, outNoop)
	}
	if outDiffNoop, _, code := runCLI(ctx, "schema", "diff", "--from", "file://"+v2File, "--to", "file://"+v2File); code != 0 || !strings.Contains(outDiffNoop, "Schemas are identical") {
		t.Errorf("expected Schemas are identical, got code=%d out=%q", code, outDiffNoop)
	}

	// 9. Lint violation & diff error paths (destructive drop without --allow-destructive)
	emptyFile := filepath.Join(tmpDir, "empty_doris.sql")
	_ = os.WriteFile(emptyFile, []byte("CREATE DATABASE IF NOT EXISTS internal.tenant_${TENANT_SLUG};\n"), 0o644)
	if _, _, c := runCLI(ctx, "schema", "diff", "--from", "file://"+v2File, "--to", "file://"+emptyFile); c == 0 {
		t.Errorf("expected schema diff to fail on destructive drop without --allow-destructive")
	}
	if _, _, c := runCLI(ctx, "migrate", "diff", "drop_it", "--dir", "file://"+migDir, "--to", "file://"+emptyFile); c == 0 {
		t.Errorf("expected migrate diff to fail on destructive drop without --allow-destructive")
	}
	if _, _, c := runCLI(ctx, "migrate", "lint", "--dir", "file://"+migDir, "--to", "file://"+emptyFile); c == 0 {
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
		{"schema", "parity", "--doris", "/nonexistent.sql", "--iceberg", v2File},
		{"schema", "parity", "--doris", v2File, "--iceberg", "/nonexistent.sql"},
		{"external-schema"},
		{"external-schema", "--unknown-flag"},
	}
	for _, tc := range errorCases {
		if _, _, c := runCLI(ctx, tc...); c == 0 {
			t.Errorf("expected non-zero exit code for args %v", tc)
		}
	}
}

func TestCLI_SchemaParity_PassAndFailureCases(t *testing.T) {
	ctx := context.Background()
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
	iceFile := filepath.Join(tmpDir, "patients_iceberg.sql")
	if err := os.WriteFile(dorisFile, []byte(dorisSQL), 0o644); err != nil {
		t.Fatalf("write dorisFile: %v", err)
	}
	if err := os.WriteFile(iceFile, []byte(icebergSQL), 0o644); err != nil {
		t.Fatalf("write iceFile: %v", err)
	}

	// 1. Passing parity check via --dir
	out, errOut, code := runCLI(ctx, "schema", "parity", "--dir", tmpDir)
	if code != 0 {
		t.Fatalf("schema parity --dir failed (code=%d): %s", code, errOut)
	}
	if !strings.Contains(out, "schema parity passed (1 table(s) verified)") {
		t.Errorf("unexpected schema parity stdout: %q", out)
	}

	// 2. Passing parity check via explicit --doris and --iceberg flags
	if _, errOut, code = runCLI(ctx, "schema", "parity", "--doris", dorisFile, "--iceberg", iceFile); code != 0 {
		t.Fatalf("schema parity explicit files failed (code=%d): %s", code, errOut)
	}

	// 3. Failing parity check via --dir when a malformed *_iceberg.sql file is present in --dir
	badIceDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(badIceDir, "patients_doris.sql"), []byte(dorisSQL), 0o644)
	_ = os.WriteFile(filepath.Join(badIceDir, "bad_iceberg.sql"), []byte("CREATE TABLE broken (\n"), 0o644)
	if _, errOut, code = runCLI(ctx, "schema", "parity", "--dir", badIceDir); code == 0 {
		t.Fatalf("expected schema parity --dir to fail when malformed bad_iceberg.sql is present in directory")
	} else if !strings.Contains(errOut, "load Iceberg schema") {
		t.Errorf("expected 'load Iceberg schema' error message, got: %s", errOut)
	}

	// 4. Failing parity checks: column count, type, nullability, and missing table
	badIceFile := filepath.Join(t.TempDir(), "bad_iceberg.sql")
	badIceSQL := `
CREATE TABLE IF NOT EXISTS azra_lake.tenant_a.patients (
    patient_id VARCHAR(64) NOT NULL,
    notes BIGINT NOT NULL,
    payload STRING,
    extra_col VARCHAR(16),
    publication_generation BIGINT NOT NULL
)
PROPERTIES (
    "compression_codec" = "zstd"
);
CREATE TABLE IF NOT EXISTS azra_lake.tenant_a.ice_only_table (
    id BIGINT NOT NULL
)
PROPERTIES (
    "compression_codec" = "zstd"
);`
	_ = os.WriteFile(badIceFile, []byte(badIceSQL), 0o644)

	_, errOut, code = runCLI(ctx, "schema", "parity", "--doris", dorisFile, "--iceberg", badIceFile)
	if code == 0 {
		t.Fatalf("expected schema parity to fail on mismatched Iceberg schema")
	}
	for _, wantSub := range []string{
		"Iceberg column count mismatch",
		"Iceberg type mismatch",
		"nullability mismatch",
		"ice_only_table present in Iceberg schema but missing in Doris schema",
	} {
		if !strings.Contains(errOut, wantSub) {
			t.Errorf("expected parity error output to contain %q, got:\n%s", wantSub, errOut)
		}
	}

	// 5. Direct unit test of CheckSchemaParity edge cases
	if m := CheckSchemaParity(nil, nil); len(m) == 0 {
		t.Errorf("expected mismatch for nil schemas")
	}
	if m := CheckSchemaParity(doris.NewSchema(), doris.NewSchema()); len(m) == 0 {
		t.Errorf("expected mismatch for empty schemas")
	}

	sDoris, _ := doris.ParseSchema(dorisSQL)
	sReorderedIce, _ := doris.ParseSchema(`
CREATE TABLE IF NOT EXISTS azra_lake.tenant_a.patients (
    patient_id VARCHAR(64) NOT NULL,
    payload STRING,
    notes STRING,
    publication_generation BIGINT NOT NULL
)
PROPERTIES (
    "compression_codec" = "zstd"
);`)
	mismatches := CheckSchemaParity(sDoris, sReorderedIce)
	if !strings.Contains(strings.Join(mismatches, "\n"), "order/name mismatch") {
		t.Errorf("expected CheckSchemaParity to report order/name mismatch, got: %v", mismatches)
	}
}

func TestProto2TypeCrossRepoGoldenContract(t *testing.T) {
	dorisSQL := `
CREATE DATABASE IF NOT EXISTS internal.tenant_${TENANT_SLUG};

CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patient_profiles (
    tenant_id VARCHAR(64) NOT NULL,
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(64) NOT NULL DEFAULT "",
    full_name VARCHAR(128) NOT NULL DEFAULT "",
    status VARCHAR(32) NOT NULL DEFAULT "ACTIVE",
    clinical_notes TEXT,
    profile_embedding ARRAY<FLOAT> NOT NULL,
    publication_generation BIGINT NOT NULL DEFAULT "0",
    INDEX idx_status (status) USING INVERTED,
    INDEX idx_clinical_notes (clinical_notes) USING INVERTED PROPERTIES("parser" = "english"),
    INDEX idx_ngram_full_name (full_name) USING NGRAM_BF PROPERTIES("gram_size" = "3", "bf_size" = "256"),
    INDEX idx_ann_profile_embedding (profile_embedding) USING ANN PROPERTIES("index_type" = "hnsw", "metric_type" = "cosine_distance", "dim" = "768")
)
UNIQUE KEY(tenant_id, patient_id)
PARTITION BY LIST (tenant_id) ()
DISTRIBUTED BY HASH(patient_id) BUCKETS ${TENANT_BUCKETS}
PROPERTIES (
    "storage_vault_name" = "azra_lakehouse_vault",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "publication_generation",
    "colocate_with" = "azra_patient_group",
    "bloom_filter_columns" = "patient_id, mrn"
);

CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patient_note_chunks (
    tenant_id VARCHAR(64) NOT NULL,
    patient_id VARCHAR(64) NOT NULL,
    chunk_id VARCHAR(64) NOT NULL,
    event_date DATE NOT NULL,
    chunk_text TEXT,
    chunk_embedding ARRAY<FLOAT> NOT NULL,
    INDEX idx_chunk_text (chunk_text) USING INVERTED PROPERTIES("parser" = "english"),
    INDEX idx_chunk_embedding (chunk_embedding) USING ANN PROPERTIES("index_type" = "hnsw", "metric_type" = "l2_distance", "dim" = "768")
)
DUPLICATE KEY(tenant_id, patient_id, chunk_id)
PARTITION BY RANGE (event_date) ()
DISTRIBUTED BY HASH(patient_id) BUCKETS ${TENANT_BUCKETS}
PROPERTIES (
    "storage_vault_name" = "azra_lakehouse_vault",
    "colocate_with" = "azra_patient_group"
);`

	dorisHCL := `
schema "tenant_${TENANT_SLUG}" {
  catalog = "internal"
}

table "patient_profiles" {
  schema = "schema.tenant_${TENANT_SLUG}"
  column "tenant_id" {
    type = varchar(64)
    null = false
  }
  column "patient_id" {
    type = varchar(64)
    null = false
  }
  column "mrn" {
    type = varchar(64)
    null = false
    default = ""
  }
  column "full_name" {
    type = varchar(128)
    null = false
    default = ""
  }
  column "status" {
    type = varchar(32)
    null = false
    default = "ACTIVE"
  }
  column "clinical_notes" {
    type = text
    null = true
  }
  column "profile_embedding" {
    type = array<float>
    null = false
  }
  column "publication_generation" {
    type = bigint
    null = false
    default = "0"
  }
  unique_key {
    columns = [column.tenant_id, column.patient_id]
    merge_on_write = true
    sequence_col = column.publication_generation
  }
  partition_by_list {
    columns = [column.tenant_id]
  }
  distributed_by_hash {
    columns = [column.patient_id]
    buckets = "${TENANT_BUCKETS}"
  }
  index "idx_status" {
    columns = [column.status]
    using = "INVERTED"
  }
  index "idx_clinical_notes" {
    columns = [column.clinical_notes]
    using = "INVERTED"
    properties = {
      parser = "english"
    }
  }
  index "idx_ngram_full_name" {
    columns = [column.full_name]
    using = "NGRAM_BF"
    properties = {
      gram_size = "3"
      bf_size = "256"
    }
  }
  index "idx_ann_profile_embedding" {
    columns = [column.profile_embedding]
    using = "ANN"
    properties = {
      index_type = "hnsw"
      metric_type = "cosine_distance"
      dim = "768"
    }
  }
  properties = {
    storage_vault_name = "azra_lakehouse_vault"
    enable_unique_key_merge_on_write = "true"
    "function_column.sequence_col" = "publication_generation"
    colocate_with = "azra_patient_group"
    bloom_filter_columns = "patient_id, mrn"
  }
}

table "patient_note_chunks" {
  schema = "schema.tenant_${TENANT_SLUG}"
  column "tenant_id" {
    type = varchar(64)
    null = false
  }
  column "patient_id" {
    type = varchar(64)
    null = false
  }
  column "chunk_id" {
    type = varchar(64)
    null = false
  }
  column "event_date" {
    type = date
    null = false
  }
  column "chunk_text" {
    type = text
    null = true
  }
  column "chunk_embedding" {
    type = array<float>
    null = false
  }
  duplicate_key {
    columns = [column.tenant_id, column.patient_id, column.chunk_id]
  }
  partition_by_range {
    columns = [column.event_date]
  }
  distributed_by_hash {
    columns = [column.patient_id]
    buckets = "${TENANT_BUCKETS}"
  }
  index "idx_chunk_text" {
    columns = [column.chunk_text]
    using = "INVERTED"
    properties = {
      parser = "english"
    }
  }
  index "idx_chunk_embedding" {
    columns = [column.chunk_embedding]
    using = "ANN"
    properties = {
      index_type = "hnsw"
      metric_type = "l2_distance"
      dim = "768"
    }
  }
  properties = {
    storage_vault_name = "azra_lakehouse_vault"
    colocate_with = "azra_patient_group"
  }
}`

	icebergSQL := `
CREATE DATABASE IF NOT EXISTS azra_lake.tenant_${TENANT_SLUG};

CREATE TABLE IF NOT EXISTS azra_lake.tenant_${TENANT_SLUG}.patient_profiles (
    tenant_id VARCHAR(64) NOT NULL,
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(64) NOT NULL,
    full_name VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    clinical_notes STRING,
    profile_embedding ARRAY<FLOAT> NOT NULL,
    publication_generation BIGINT NOT NULL
)
PARTITION BY LIST (tenant_id) ()
PROPERTIES (
    "compression_codec" = "zstd"
);

CREATE TABLE IF NOT EXISTS azra_lake.tenant_${TENANT_SLUG}.patient_note_chunks (
    tenant_id VARCHAR(64) NOT NULL,
    patient_id VARCHAR(64) NOT NULL,
    chunk_id VARCHAR(64) NOT NULL,
    event_date DATE NOT NULL,
    chunk_text STRING,
    chunk_embedding ARRAY<FLOAT> NOT NULL
)
PARTITION BY RANGE (event_date) ()
PROPERTIES (
    "compression_codec" = "zstd"
);`

	icebergHCL := `
schema "tenant_${TENANT_SLUG}" {
  catalog = "azra_lake"
}

table "patient_profiles" {
  catalog = "azra_lake"
  schema = "schema.tenant_${TENANT_SLUG}"
  column "tenant_id" {
    type = varchar(64)
    null = false
  }
  column "patient_id" {
    type = varchar(64)
    null = false
  }
  column "mrn" {
    type = varchar(64)
    null = false
  }
  column "full_name" {
    type = varchar(128)
    null = false
  }
  column "status" {
    type = varchar(32)
    null = false
  }
  column "clinical_notes" {
    type = string
    null = true
  }
  column "profile_embedding" {
    type = array<float>
    null = false
  }
  column "publication_generation" {
    type = bigint
    null = false
  }
  partition_by_list {
    columns = [column.tenant_id]
  }
  properties = {
    compression_codec = "zstd"
  }
}

table "patient_note_chunks" {
  catalog = "azra_lake"
  schema = "schema.tenant_${TENANT_SLUG}"
  column "tenant_id" {
    type = varchar(64)
    null = false
  }
  column "patient_id" {
    type = varchar(64)
    null = false
  }
  column "chunk_id" {
    type = varchar(64)
    null = false
  }
  column "event_date" {
    type = date
    null = false
  }
  column "chunk_text" {
    type = string
    null = true
  }
  column "chunk_embedding" {
    type = array<float>
    null = false
  }
  partition_by_range {
    columns = [column.event_date]
  }
  properties = {
    compression_codec = "zstd"
  }
}`

	sDorisSQL, err := doris.ParseSchemaAuto(dorisSQL, "profiles_doris.sql")
	if err != nil {
		t.Fatalf("ParseSchemaAuto(dorisSQL): %v", err)
	}
	sDorisHCL, err := doris.ParseSchemaAuto(dorisHCL, "profiles_doris.hcl")
	if err != nil {
		t.Fatalf("ParseSchemaAuto(dorisHCL): %v", err)
	}
	sIcebergSQL, err := doris.ParseSchemaAuto(icebergSQL, "profiles_iceberg.sql")
	if err != nil {
		t.Fatalf("ParseSchemaAuto(icebergSQL): %v", err)
	}
	sIcebergHCL, err := doris.ParseSchemaAuto(icebergHCL, "profiles_iceberg.hcl")
	if err != nil {
		t.Fatalf("ParseSchemaAuto(icebergHCL): %v", err)
	}

	// 1. SQL <-> HCL equivalence across Doris and Iceberg
	for _, pair := range []struct {
		name string
		sqlS *doris.Schema
		hclS *doris.Schema
	}{
		{"doris", sDorisSQL, sDorisHCL},
		{"iceberg", sIcebergSQL, sIcebergHCL},
	} {
		stmts, err := doris.Diff(pair.sqlS, pair.hclS)
		if err != nil || len(stmts) != 0 {
			t.Fatalf("%s SQL <-> HCL diff produced %d stmts (err=%v):\n%s", pair.name, len(stmts), err, doris.FormatStatements(stmts))
		}
		if v := doris.LintSchema(pair.sqlS); len(v) != 0 {
			t.Fatalf("%s SQL LintSchema failed: %v", pair.name, v)
		}
		if v := doris.LintSchema(pair.hclS); len(v) != 0 {
			t.Fatalf("%s HCL LintSchema failed: %v", pair.name, v)
		}
	}

	// 2. FormatHCL round-trip on Doris HCL
	formattedDorisHCL := doris.FormatHCL(sDorisHCL)
	reParsedDorisHCL, err := doris.ParseHCL(formattedDorisHCL)
	if err != nil {
		t.Fatalf("ParseHCL(formattedDorisHCL): %v", err)
	}
	if doris.FormatHCL(reParsedDorisHCL) != formattedDorisHCL {
		t.Fatalf("FormatHCL not idempotent on Doris golden HCL")
	}

	// 3. CheckSchemaParity passes with 0 mismatches on both SQL and HCL golden sets
	if m := CheckSchemaParity(sDorisSQL, sIcebergSQL); len(m) != 0 {
		t.Fatalf("CheckSchemaParity(SQL) failed: %v", m)
	}
	if m := CheckSchemaParity(sDorisHCL, sIcebergHCL); len(m) != 0 {
		t.Fatalf("CheckSchemaParity(HCL) failed: %v", m)
	}

	// 4. Verify CheckSchemaParity catches Iceberg partition column drift
	driftIce := sIcebergSQL.Clone()
	driftIce.Tables[0].Partition.Columns = []string{"patient_id"}
	driftMismatches := strings.Join(CheckSchemaParity(sDorisSQL, driftIce), "\n")
	if !strings.Contains(driftMismatches, "Iceberg partition columns mismatch") {
		t.Errorf("expected drift CheckSchemaParity to report Iceberg partition columns mismatch, got:\n%s", driftMismatches)
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
	out, errOut, code := runCLI(
		ctx,
		"schema", "parity",
		"--doris", "proto://"+protoFile,
		"--iceberg", "proto://"+protoFile,
		"--include", proto2typeDir,
	)
	if code != 0 {
		t.Fatalf("schema parity on real proto2type failed (code=%d): %s", code, errOut)
	}
	if !strings.Contains(out, "schema parity passed") {
		t.Errorf("unexpected schema parity stdout: %q", out)
	}
}

func runCLI(ctx context.Context, args ...string) (string, string, int) {
	var stdout, stderr bytes.Buffer
	code := Run(ctx, args, &stdout, &stderr)
	return stdout.String(), stderr.String(), code
}

func TestMainEntrypoint(t *testing.T) {
	origExit := exitFunc
	origArgs := os.Args
	defer func() {
		exitFunc = origExit
		os.Args = origArgs
	}()
	gotCode := -1
	exitFunc = func(c int) { gotCode = c }
	os.Args = []string{"atlas-doris", "version"}
	main()
	if gotCode != 0 {
		t.Errorf("expected exit code 0 from main(), got %d", gotCode)
	}
}
