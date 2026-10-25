package external_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/protocgen/atlas-doris/pkg/external"
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
}
