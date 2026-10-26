package external_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/protocgen/atlas-doris/pkg/doris"
	"github.com/protocgen/atlas-doris/pkg/external"
	"github.com/protocgen/atlas-doris/pkg/migrate"
)

func TestExternalSchema_SQLDirAndTargetFilters(t *testing.T) {
	tmpDir := t.TempDir()

	dorisSQL := `
CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patients (
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(64) NOT NULL DEFAULT "",
    publication_generation BIGINT NOT NULL DEFAULT "0"
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS ${TENANT_BUCKETS}
PROPERTIES (
    "storage_vault_name" = "azra_lakehouse_vault",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "publication_generation"
);`

	icebergSQL := `
CREATE TABLE IF NOT EXISTS azra_lake.tenant_${TENANT_SLUG}.patients (
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(64) NOT NULL,
    publication_generation BIGINT NOT NULL
)
PROPERTIES (
    "compression_codec" = "zstd"
);`

	if err := os.WriteFile(filepath.Join(tmpDir, "patient_profile_doris.sql"), []byte(dorisSQL), 0o644); err != nil {
		t.Fatalf("write doris sql: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "patient_profile_iceberg.sql"), []byte(icebergSQL), 0o644); err != nil {
		t.Fatalf("write iceberg sql: %v", err)
	}

	ctx := context.Background()

	// 1. TargetAll loads both tables
	allOut, err := external.ExternalSchemaSQL(ctx, external.ExternalSchemaOptions{
		SQLDir: tmpDir,
		Target: external.TargetAll,
	})
	if err != nil {
		t.Fatalf("ExternalSchemaSQL(TargetAll) failed: %v", err)
	}
	if !strings.Contains(allOut, "internal.tenant_${TENANT_SLUG}.patients") || !strings.Contains(allOut, "azra_lake.tenant_${TENANT_SLUG}.patients") {
		t.Errorf("TargetAll output missing expected tables:\n%s", allOut)
	}

	// 2. TargetDoris loads only Doris table
	dorisOut, err := external.ExternalSchemaSQL(ctx, external.ExternalSchemaOptions{
		SQLDir: tmpDir,
		Target: external.TargetDoris,
	})
	if err != nil {
		t.Fatalf("ExternalSchemaSQL(TargetDoris) failed: %v", err)
	}
	if !strings.Contains(dorisOut, "internal.tenant_${TENANT_SLUG}.patients") || strings.Contains(dorisOut, "azra_lake.tenant_${TENANT_SLUG}.patients") {
		t.Errorf("TargetDoris output unexpected:\n%s", dorisOut)
	}

	// 3. TargetIceberg loads only Iceberg table
	iceOut, err := external.ExternalSchemaSQL(ctx, external.ExternalSchemaOptions{
		SQLDir: tmpDir,
		Target: external.TargetIceberg,
	})
	if err != nil {
		t.Fatalf("ExternalSchemaSQL(TargetIceberg) failed: %v", err)
	}
	if !strings.Contains(iceOut, "azra_lake.tenant_${TENANT_SLUG}.patients") || strings.Contains(iceOut, "internal.tenant_${TENANT_SLUG}.patients") {
		t.Errorf("TargetIceberg output unexpected:\n%s", iceOut)
	}

	// 4. LoadSchema with file:// and target query parameter
	schemaDoris, err := external.LoadSchema(ctx, "file://"+tmpDir+"?target=doris", external.LoadOptions{})
	if err != nil {
		t.Fatalf("LoadSchema(file://...?target=doris) failed: %v", err)
	}
	if len(schemaDoris.Tables) != 1 || schemaDoris.Tables[0].FullName != "internal.tenant_${TENANT_SLUG}.patients" {
		t.Errorf("LoadSchema(file://?target=doris) tables = %d, want 1", len(schemaDoris.Tables))
	}

	// 5. Add *_doris.hcl and *_iceberg.hcl (simulating doris_mode=all directory) and verify clean deduplication + target filtering + HCL output
	dorisHCL := `
table "patients" {
  schema = "schema.tenant_${TENANT_SLUG}"
  column "patient_id" {
    type = varchar(64)
    null = false
  }
  column "mrn" {
    type = varchar(64)
    null = false
    default = ""
  }
  column "publication_generation" {
    type = bigint
    null = false
    default = "0"
  }
  unique_key {
    columns = [column.patient_id]
    merge_on_write = true
    sequence_col = column.publication_generation
  }
  distributed_by_hash {
    columns = [column.patient_id]
    buckets = "${TENANT_BUCKETS}"
  }
  properties = {
    storage_vault_name = "azra_lakehouse_vault"
    enable_unique_key_merge_on_write = "true"
    "function_column.sequence_col" = "publication_generation"
  }
}`
	icebergHCL := `
table "patients" {
  catalog = "azra_lake"
  schema = "schema.tenant_${TENANT_SLUG}"
  column "patient_id" {
    type = varchar(64)
    null = false
  }
  column "mrn" {
    type = varchar(64)
    null = false
  }
  column "publication_generation" {
    type = bigint
    null = false
  }
  properties = {
    compression_codec = "zstd"
  }
}`
	dorisHCLPath := filepath.Join(tmpDir, "patient_profile_doris.hcl")
	icebergHCLPath := filepath.Join(tmpDir, "patient_profile_iceberg.hcl")
	if err := os.WriteFile(dorisHCLPath, []byte(dorisHCL), 0o644); err != nil {
		t.Fatalf("write doris hcl: %v", err)
	}
	if err := os.WriteFile(icebergHCLPath, []byte(icebergHCL), 0o644); err != nil {
		t.Fatalf("write iceberg hcl: %v", err)
	}

	schemaAllMode, err := external.LoadFromPath(tmpDir, external.TargetAll)
	if err != nil {
		t.Fatalf("LoadFromPath(doris_mode=all dir): %v", err)
	}
	if len(schemaAllMode.Tables) != 2 {
		t.Fatalf("expected 2 deduplicated tables from doris_mode=all dir, got %d", len(schemaAllMode.Tables))
	}
	if len(external.FilterSchemaByTarget(schemaAllMode, external.TargetDoris).Tables) != 1 {
		t.Errorf("expected FilterSchemaByTarget(TargetDoris) to keep 1 table")
	}
	if len(external.FilterSchemaByTarget(schemaAllMode, external.TargetIceberg).Tables) != 1 {
		t.Errorf("expected FilterSchemaByTarget(TargetIceberg) to keep 1 table")
	}
	if len(external.FilterSchemaByTarget(schemaAllMode, external.TargetAll).Tables) != 2 {
		t.Errorf("expected FilterSchemaByTarget(TargetAll) to keep 2 tables")
	}

	// Test --hcl-file and Format="hcl"
	hclRendered, err := external.ExternalSchemaSQL(ctx, external.ExternalSchemaOptions{
		HCLFiles: []string{dorisHCLPath},
		Format:   "hcl",
	})
	if err != nil {
		t.Fatalf("ExternalSchemaSQL(HCLFiles, Format=hcl) failed: %v", err)
	}
	if !strings.Contains(hclRendered, `table "patients"`) || !strings.Contains(hclRendered, `unique_key`) {
		t.Errorf("unexpected HCL output:\n%s", hclRendered)
	}

	// Test --hcl-dir, --sql-file, --url, and invalid Format rejection
	hclDirRendered, err := external.ExternalSchemaSQL(ctx, external.ExternalSchemaOptions{
		HCLDir:   tmpDir,
		SQLFiles: []string{filepath.Join(tmpDir, "patient_profile_doris.sql"), ""},
		HCLFiles: []string{""},
		URL:      "file://" + tmpDir + "?target=doris",
		Target:   external.TargetDoris,
		Format:   "sql",
	})
	if err != nil {
		t.Fatalf("ExternalSchemaSQL(HCLDir+SQLFiles+URL) failed: %v", err)
	}
	if !strings.Contains(hclDirRendered, "internal.tenant_${TENANT_SLUG}.patients") {
		t.Errorf("unexpected HCLDir+URL output:\n%s", hclDirRendered)
	}

	if _, err := external.ExternalSchemaSQL(ctx, external.ExternalSchemaOptions{
		SQLDir: tmpDir,
		Format: "invalid_format",
	}); err == nil {
		t.Errorf("expected error for unsupported Format")
	}

	if _, err := external.ExternalSchemaSQL(ctx, external.ExternalSchemaOptions{}); err == nil {
		t.Errorf("expected error for empty ExternalSchemaOptions")
	}
	if _, err := external.LoadSchema(ctx, "", external.LoadOptions{}); err == nil {
		t.Errorf("expected error for empty LoadSchema URL")
	}
}

func TestLoadFromPath_MigrationDirAndProtoURL(t *testing.T) {
	ctx := context.Background()

	// 1. Directory containing atlas.sum replays migrations
	migDir := t.TempDir()
	migSQL := "CREATE TABLE IF NOT EXISTS internal.tenant_test.encounters (encounter_id VARCHAR(64) NOT NULL) UNIQUE KEY(encounter_id) DISTRIBUTED BY HASH(encounter_id) BUCKETS 4;\n"
	if err := os.WriteFile(filepath.Join(migDir, "20261025000001_init.sql"), []byte(migSQL), 0o644); err != nil {
		t.Fatalf("write migration file: %v", err)
	}
	if err := migrate.HashDir(migDir); err != nil {
		t.Fatalf("HashDir: %v", err)
	}
	replayedMig, err := external.LoadFromPath(migDir, external.TargetAll)
	if err != nil {
		t.Fatalf("LoadFromPath(migDir with atlas.sum): %v", err)
	}
	if len(replayedMig.Tables) != 1 {
		t.Errorf("expected 1 table from migDir replay, got %d", len(replayedMig.Tables))
	}

	// Nonexistent path / empty dir / invalid SQL file errors
	if _, err := external.LoadFromPath(filepath.Join(migDir, "does_not_exist"), external.TargetAll); err == nil {
		t.Errorf("expected error for nonexistent path")
	}
	emptyDir := t.TempDir()
	if _, err := external.LoadFromPath(emptyDir, external.TargetAll); err == nil {
		t.Errorf("expected error on directory with no .sql/.hcl files")
	}
	badSQLFile := filepath.Join(emptyDir, "bad.sql")
	if err := os.WriteFile(badSQLFile, []byte("CREATE TABLE broken ("), 0o644); err != nil {
		t.Fatalf("write bad.sql: %v", err)
	}
	if _, err := external.LoadFromPath(badSQLFile, external.TargetAll); err == nil {
		t.Errorf("expected error on invalid SQL file")
	}
	if _, err := external.LoadFromPath(emptyDir, external.TargetAll); err == nil {
		t.Errorf("expected error on directory containing invalid SQL file")
	}

	// 2. Test proto:// URL loading using a fake protoc binary in PATH + fake plugin binary
	binDir := t.TempDir()
	fakePlugin := filepath.Join(binDir, "protoc-gen-proto2type")
	if err := os.WriteFile(fakePlugin, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write fake plugin: %v", err)
	}
	fakeProtoc := filepath.Join(binDir, "protoc")
	fakeProtocScript := `#!/bin/sh
for arg in "$@"; do
  case "$arg" in
    --proto2type_out=*)
      out_dir="${arg#--proto2type_out=}"
      cat << 'EOF' > "$out_dir/fake_doris.hcl"
table "proto_items" {
  schema = schema.tenant_proto
  column "item_id" {
    type = varchar(64)
    null = false
  }
  unique_key {
    columns = [column.item_id]
  }
  distributed_by_hash {
    columns = [column.item_id]
    buckets = 4
  }
}
EOF
      cat << 'EOF' > "$out_dir/fake_iceberg.hcl"
table "proto_items" {
  catalog = "azra_lake"
  schema = schema.tenant_proto
  column "item_id" {
    type = varchar(64)
    null = false
  }
  properties = {
    compression_codec = "zstd"
  }
}
EOF
      ;;
  esac
done
exit 0
`
	if err := os.WriteFile(fakeProtoc, []byte(fakeProtocScript), 0o755); err != nil {
		t.Fatalf("write fake protoc: %v", err)
	}

	protoDir := t.TempDir()
	protoFile := filepath.Join(protoDir, "item.proto")
	if err := os.WriteFile(protoFile, []byte("syntax = \"proto3\";\npackage test;\n"), 0o644); err != nil {
		t.Fatalf("write proto file: %v", err)
	}

	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	protoRendered, err := external.ExternalSchemaSQL(ctx, external.ExternalSchemaOptions{
		ProtoPath:     protoDir + "?mode=hcl&target=doris&include=" + protoDir + "&plugin=" + fakePlugin,
		ProtoIncludes: []string{protoDir},
		Format:        "hcl",
	})
	if err != nil {
		t.Fatalf("ExternalSchemaSQL(proto:// dir) failed: %v", err)
	}
	if !strings.Contains(protoRendered, `table "proto_items"`) {
		t.Errorf("expected table proto_items from proto:// loader, got:\n%s", protoRendered)
	}

	// Single proto file with PATH lookup for protoc-gen-proto2type
	sSingle, err := external.LoadSchema(ctx, "proto://"+protoFile+"?doris_mode=both", external.LoadOptions{
		Target: external.TargetDoris,
	})
	if err != nil {
		t.Fatalf("LoadSchema(proto:// single file) failed: %v", err)
	}
	if len(sSingle.Tables) != 1 {
		t.Errorf("expected 1 table from single proto file, got %d", len(sSingle.Tables))
	}
	for _, iceQuery := range []string{"", "?mode=ddl", "?mode=hcl", "?mode=both"} {
		if sIce, err := external.LoadSchema(ctx, "proto://"+protoFile+iceQuery, external.LoadOptions{Target: external.TargetIceberg}); err != nil || len(sIce.Tables) != 1 {
			t.Errorf("LoadSchema(TargetIceberg, %q) = (%+v, %v)", iceQuery, sIce, err)
		}
	}

	// Proto error paths: missing plugin, empty proto dir, nonexistent proto path
	if _, err := external.LoadSchema(ctx, "proto://"+protoFile+"?plugin=/nonexistent/plugin", external.LoadOptions{}); err == nil {
		t.Errorf("expected error for nonexistent explicit plugin")
	}
	if _, err := external.LoadSchema(ctx, "proto://"+t.TempDir(), external.LoadOptions{}); err == nil {
		t.Errorf("expected error for directory with no .proto files")
	}
	if _, err := external.LoadSchema(ctx, "proto:///nonexistent/proto/dir", external.LoadOptions{}); err == nil {
		t.Errorf("expected error for nonexistent proto path")
	}
	if _, err := external.LoadSchema(ctx, "doris://invalid_url_missing_host", external.LoadOptions{}); err == nil {
		t.Errorf("expected error for invalid doris:// URL")
	}

	// Test PROTO2TYPE_DIR fallback when protoc-gen-proto2type is not in binDir
	_ = os.Remove(fakePlugin)
	fakeRepoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(fakeRepoDir, "protoc-gen-proto2type"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write fake repo plugin: %v", err)
	}
	t.Setenv("PROTO2TYPE_DIR", fakeRepoDir)
	sFromBuiltPlugin, err := external.LoadSchema(ctx, "proto://"+protoFile, external.LoadOptions{
		Target: external.TargetDoris,
	})
	if err != nil {
		t.Fatalf("LoadSchema with PROTO2TYPE_DIR failed: %v", err)
	}
	if len(sFromBuiltPlugin.Tables) != 1 {
		t.Errorf("expected 1 table from PROTO2TYPE_DIR, got %d", len(sFromBuiltPlugin.Tables))
	}

	// Test FilterSchemaByTarget with nil schema and unknown target
	if external.FilterSchemaByTarget(nil, external.TargetDoris) != nil {
		t.Errorf("expected nil from FilterSchemaByTarget(nil)")
	}
	if len(external.FilterSchemaByTarget(sFromBuiltPlugin, external.TargetFilter("custom")).Tables) != 1 {
		t.Errorf("expected custom target filter to retain table")
	}

	// Test invalid proto:// query string and failing protoc execution
	if _, err := external.LoadSchema(ctx, "proto://"+protoFile+"?%zz", external.LoadOptions{}); err == nil {
		t.Errorf("expected error for invalid query string in proto:// URL")
	}
	if err := os.WriteFile(fakeProtoc, []byte("#!/bin/sh\necho 'protoc boom' >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatalf("write failing fake protoc: %v", err)
	}
	if _, err := external.LoadSchema(ctx, "proto://"+protoFile, external.LoadOptions{}); err == nil {
		t.Errorf("expected error when protoc fails")
	}

	// Test ExternalSchemaSQL error propagation across all source options
	for _, badOpts := range []external.ExternalSchemaOptions{
		{URL: "file:///nonexistent/path.sql"},
		{SQLDir: "/nonexistent/sql_dir"},
		{SQLFiles: []string{"/nonexistent/file.sql"}},
		{HCLDir: "/nonexistent/hcl_dir"},
		{HCLFiles: []string{"/nonexistent/file.hcl"}},
		{ProtoPath: "/nonexistent/proto_dir"},
	} {
		if _, err := external.ExternalSchemaSQL(ctx, badOpts); err == nil {
			t.Errorf("expected error for ExternalSchemaSQL(%+v)", badOpts)
		}
	}
}

func TestMultiDialectDirectory_SameTableName_DiffAndFilter(t *testing.T) {
	tmpDir := t.TempDir()

	dorisSQL := `
CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patients (
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(64) NOT NULL DEFAULT "",
    publication_generation BIGINT NOT NULL DEFAULT "0"
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS ${TENANT_BUCKETS}
PROPERTIES (
    "storage_vault_name" = "azra_lakehouse_vault",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "publication_generation"
);`

	icebergSQL := `
CREATE TABLE IF NOT EXISTS azra_lake.tenant_${TENANT_SLUG}.patients (
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(64) NOT NULL,
    publication_generation BIGINT NOT NULL
)
PROPERTIES (
    "compression_codec" = "zstd"
);`

	dorisHCL := `
table "patients" {
  schema = "schema.tenant_${TENANT_SLUG}"
  column "patient_id" {
    type = varchar(64)
    null = false
  }
  column "mrn" {
    type = varchar(64)
    null = false
    default = ""
  }
  column "publication_generation" {
    type = bigint
    null = false
    default = "0"
  }
  unique_key {
    columns = [column.patient_id]
    merge_on_write = true
    sequence_col = column.publication_generation
  }
  distributed_by_hash {
    columns = [column.patient_id]
    buckets = "${TENANT_BUCKETS}"
  }
  properties = {
    storage_vault_name = "azra_lakehouse_vault"
  }
}`

	icebergHCL := `
table "patients" {
  catalog = "azra_lake"
  schema = "schema.tenant_${TENANT_SLUG}"
  column "patient_id" {
    type = varchar(64)
    null = false
  }
  column "mrn" {
    type = varchar(64)
    null = false
  }
  column "publication_generation" {
    type = bigint
    null = false
  }
  properties = {
    compression_codec = "zstd"
  }
}`

	dorisDupSQL := `
CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.audit_events (
    event_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(32) NOT NULL DEFAULT ""
)
DUPLICATE KEY(event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 8;`

	dorisDupHCL := `
table "audit_events" {
  schema = "schema.tenant_${TENANT_SLUG}"
  column "event_id" {
    type = varchar(64)
    null = false
  }
  column "event_type" {
    type = varchar(32)
    null = false
    default = ""
  }
  duplicate_key {
    columns = [column.event_id]
  }
  distributed_by_hash {
    columns = [column.event_id]
    buckets = 8
  }
}`

	for name, content := range map[string]string{
		"patient_profile_doris.sql":   dorisSQL,
		"patient_profile_iceberg.sql": icebergSQL,
		"patient_profile_doris.hcl":   dorisHCL,
		"patient_profile_iceberg.hcl": icebergHCL,
		"audit_events_doris.sql":      dorisDupSQL,
		"audit_events_doris.hcl":      dorisDupHCL,
	} {
		if err := os.WriteFile(filepath.Join(tmpDir, name), []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	sAll, err := external.LoadFromPath(tmpDir, external.TargetAll)
	if err != nil {
		t.Fatalf("LoadFromPath(TargetAll): %v", err)
	}
	// Expect 3 deduplicated tables: 2 Doris (patients + audit_events), 1 Iceberg (patients)
	if len(sAll.Tables) != 3 {
		t.Fatalf("expected 3 deduplicated tables in TargetAll, got %d", len(sAll.Tables))
	}

	// Self-diff on multi-dialect schema must produce 0 statements and nil error (no DORIS001 collision)
	diffStmts, err := doris.Diff(sAll, sAll)
	if err != nil {
		t.Fatalf("doris.Diff(sAll, sAll) failed on multi-dialect schema: %v", err)
	}
	if len(diffStmts) != 0 {
		t.Fatalf("expected 0 diff statements on self-diff, got %d: %v", len(diffStmts), doris.FormatStatements(diffStmts))
	}

	sDorisFiltered := external.FilterSchemaByTarget(sAll, external.TargetDoris)
	if len(sDorisFiltered.Tables) != 2 {
		t.Errorf("expected TargetDoris filter to retain 2 Doris tables (patients + audit_events), got %d", len(sDorisFiltered.Tables))
	}
	sIceFiltered := external.FilterSchemaByTarget(sAll, external.TargetIceberg)
	if len(sIceFiltered.Tables) != 1 {
		t.Errorf("expected TargetIceberg filter to retain 1 Iceberg table, got %d", len(sIceFiltered.Tables))
	}
}

func TestRealProto2TypeIntegration_MultiTarget(t *testing.T) {
	proto2typeDir := filepath.Clean(filepath.Join("..", "proto2type"))
	protoFile := filepath.Join(proto2typeDir, "testdata", "doris_tenant_profile.proto")
	if _, err := os.Stat(protoFile); os.IsNotExist(err) {
		t.Skip("skipping real proto2type integration test: ../proto2type/testdata/doris_tenant_profile.proto not found")
	}

	ctx := context.Background()
	sDoris, err := external.LoadSchema(ctx, "proto://"+protoFile, external.LoadOptions{
		Target:        external.TargetDoris,
		ProtoIncludes: []string{proto2typeDir},
	})
	if err != nil {
		t.Fatalf("LoadSchema(TargetDoris) failed: %v", err)
	}
	sIceberg, err := external.LoadSchema(ctx, "proto://"+protoFile, external.LoadOptions{
		Target:        external.TargetIceberg,
		ProtoIncludes: []string{proto2typeDir},
	})
	if err != nil {
		t.Fatalf("LoadSchema(TargetIceberg) failed: %v", err)
	}
	sAll, err := external.LoadSchema(ctx, "proto://"+protoFile+"?doris_mode=both", external.LoadOptions{
		Target:        external.TargetAll,
		ProtoIncludes: []string{proto2typeDir},
	})
	if err != nil {
		t.Fatalf("LoadSchema(TargetAll, doris_mode=both) failed: %v", err)
	}

	if len(sDoris.Tables) == 0 || len(sDoris.Tables) != len(sIceberg.Tables) {
		t.Fatalf("table count mismatch: doris=%d iceberg=%d", len(sDoris.Tables), len(sIceberg.Tables))
	}
	if stmts, err := doris.Diff(sAll, sAll); err != nil || len(stmts) != 0 {
		t.Fatalf("Diff(sAll, sAll) on proto2type doris_mode=both failed: stmts=%d err=%v", len(stmts), err)
	}
}
