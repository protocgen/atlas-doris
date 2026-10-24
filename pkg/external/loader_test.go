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
}
