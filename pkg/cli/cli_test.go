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

	// 3b. Failing parity check via --dir when a malformed *_iceberg.sql file is present in --dir
	badIceDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(badIceDir, "patients_doris.sql"), []byte(dorisSQL), 0o644)
	_ = os.WriteFile(filepath.Join(badIceDir, "patients_starrocks.sql"), []byte(starrocksSQL), 0o644)
	_ = os.WriteFile(filepath.Join(badIceDir, "bad_iceberg.sql"), []byte("CREATE TABLE broken (\n"), 0o644)
	out.Reset()
	errBuf.Reset()
	if code := cli.Run(ctx, olapCfg, []string{"schema", "parity", "--dir", badIceDir}, &out, &errBuf); code == 0 {
		t.Fatalf("expected schema parity --dir to fail when malformed bad_iceberg.sql is present in directory")
	}
	if !strings.Contains(errBuf.String(), "load Iceberg schema") {
		t.Errorf("expected 'load Iceberg schema' error message, got: %s", errBuf.String())
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

	starrocksSQL := `
CREATE DATABASE IF NOT EXISTS internal.tenant_${TENANT_SLUG};

CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patient_profiles (
    tenant_id VARCHAR(64) NOT NULL,
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(64) NOT NULL DEFAULT "",
    full_name VARCHAR(128) NOT NULL DEFAULT "",
    status VARCHAR(32) NOT NULL DEFAULT "ACTIVE",
    clinical_notes STRING,
    profile_embedding ARRAY<FLOAT> NOT NULL,
    publication_generation BIGINT NOT NULL DEFAULT "0",
    INDEX idx_status (status) USING BITMAP,
    INDEX idx_clinical_notes (clinical_notes) USING GIN("parser" = "english"),
    INDEX idx_ngram_full_name (full_name) USING NGRAMBF ("gram_num" = "3", "bloom_filter_fpp" = "0.05")
)
PRIMARY KEY(tenant_id, patient_id)
PARTITION BY LIST (tenant_id) ()
DISTRIBUTED BY HASH(patient_id) BUCKETS ${TENANT_BUCKETS}
ORDER BY (tenant_id, patient_id, mrn)
PROPERTIES (
    "storage_volume" = "azra_sr_volume",
    "enable_persistent_index" = "true",
    "function_column.sequence_col" = "publication_generation",
    "colocate_with" = "azra_patient_group",
    "bloom_filter_columns" = "patient_id, mrn"
);

CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patient_note_chunks (
    tenant_id VARCHAR(64) NOT NULL,
    patient_id VARCHAR(64) NOT NULL,
    chunk_id VARCHAR(64) NOT NULL,
    event_date DATE NOT NULL,
    chunk_text STRING,
    chunk_embedding ARRAY<FLOAT> NOT NULL,
    INDEX idx_chunk_text (chunk_text) USING GIN("parser" = "english"),
    INDEX idx_chunk_embedding (chunk_embedding) USING VECTOR ("index_type" = "hnsw", "metric_type" = "l2_distance", "dim" = "768")
)
DUPLICATE KEY(tenant_id, patient_id, chunk_id)
PARTITION BY RANGE (event_date) ()
DISTRIBUTED BY HASH(patient_id) BUCKETS ${TENANT_BUCKETS}
PROPERTIES (
    "storage_volume" = "azra_sr_volume",
    "colocate_with" = "azra_patient_group"
);`

	starrocksHCL := `
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
    default = "0"
  }
  primary_key {
    columns = [column.tenant_id, column.patient_id]
    enable_persistent_index = true
    sequence_col = column.publication_generation
  }
  partition_by_list {
    columns = [column.tenant_id]
  }
  distributed_by_hash {
    columns = [column.patient_id]
    buckets = "${TENANT_BUCKETS}"
  }
  order_by {
    columns = [column.tenant_id, column.patient_id, column.mrn]
  }
  index "idx_status" {
    columns = [column.status]
    using = "BITMAP"
  }
  index "idx_clinical_notes" {
    columns = [column.clinical_notes]
    using = "GIN"
    properties = {
      parser = "english"
    }
  }
  index "idx_ngram_full_name" {
    columns = [column.full_name]
    using = "NGRAMBF"
    properties = {
      gram_num = "3"
      bloom_filter_fpp = "0.05"
    }
  }
  properties = {
    storage_volume = "azra_sr_volume"
    enable_persistent_index = "true"
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
    type = string
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
    using = "GIN"
    properties = {
      parser = "english"
    }
  }
  index "idx_chunk_embedding" {
    columns = [column.chunk_embedding]
    using = "VECTOR"
    properties = {
      index_type = "hnsw"
      metric_type = "l2_distance"
      dim = "768"
    }
  }
  properties = {
    storage_volume = "azra_sr_volume"
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
	sStarRocksSQL, err := doris.ParseSchemaAuto(starrocksSQL, "profiles_starrocks.sql")
	if err != nil {
		t.Fatalf("ParseSchemaAuto(starrocksSQL): %v", err)
	}
	sStarRocksHCL, err := doris.ParseSchemaAuto(starrocksHCL, "profiles_starrocks.hcl")
	if err != nil {
		t.Fatalf("ParseSchemaAuto(starrocksHCL): %v", err)
	}
	sIcebergSQL, err := doris.ParseSchemaAuto(icebergSQL, "profiles_iceberg.sql")
	if err != nil {
		t.Fatalf("ParseSchemaAuto(icebergSQL): %v", err)
	}
	sIcebergHCL, err := doris.ParseSchemaAuto(icebergHCL, "profiles_iceberg.hcl")
	if err != nil {
		t.Fatalf("ParseSchemaAuto(icebergHCL): %v", err)
	}

	// 1. SQL <-> HCL equivalence across all 3 dialects
	for _, pair := range []struct {
		name string
		sqlS *doris.Schema
		hclS *doris.Schema
	}{
		{"doris", sDorisSQL, sDorisHCL},
		{"starrocks", sStarRocksSQL, sStarRocksHCL},
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

	// 2. FormatHCL round-trip on StarRocks HCL
	formattedSRHCL := doris.FormatHCL(sStarRocksHCL)
	reParsedSRHCL, err := doris.ParseHCL(formattedSRHCL)
	if err != nil {
		t.Fatalf("ParseHCL(formattedSRHCL): %v", err)
	}
	if doris.FormatHCL(reParsedSRHCL) != formattedSRHCL {
		t.Fatalf("FormatHCL not idempotent on StarRocks golden HCL")
	}

	// 3. CheckSchemaParity passes with 0 mismatches on both SQL and HCL golden sets
	if m := cli.CheckSchemaParity(sDorisSQL, sStarRocksSQL, sIcebergSQL); len(m) != 0 {
		t.Fatalf("CheckSchemaParity(SQL) failed: %v", m)
	}
	if m := cli.CheckSchemaParity(sDorisHCL, sStarRocksHCL, sIcebergHCL); len(m) != 0 {
		t.Fatalf("CheckSchemaParity(HCL) failed: %v", m)
	}

	// 4. Verify CheckSchemaParity catches distribution, colocate_with, bloom_filter_columns, partition, and index drift
	driftSR := sStarRocksSQL.Clone()
	driftSR.Tables[0].Distribution.Columns = []string{"tenant_id"}
	driftSR.Tables[0].SetProperty("colocate_with", "wrong_group")
	driftSR.Tables[0].SetProperty("bloom_filter_columns", "patient_id")
	driftSR.Tables[0].Partition.Columns = []string{"patient_id"}
	// Change GIN parser on patient_profiles and drop NGRAMBF index
	driftSR.Tables[0].Indexes[1].Properties = []doris.Property{{Key: "parser", Value: "chinese"}}
	driftSR.Tables[0].Indexes = driftSR.Tables[0].Indexes[:2]
	// Add extra index on patient_note_chunks in StarRocks
	driftSR.Tables[1].Indexes = append(driftSR.Tables[1].Indexes, &doris.IndexDef{
		Name:    "idx_extra_bm",
		Columns: []string{"tenant_id"},
		Using:   "BITMAP",
	})

	driftIce := sIcebergSQL.Clone()
	driftIce.Tables[0].Partition.Columns = []string{"patient_id"}

	driftMismatches := strings.Join(cli.CheckSchemaParity(sDorisSQL, driftSR, driftIce), "\n")
	for _, want := range []string{
		"distribution columns mismatch",
		"colocate_with mismatch",
		"bloom_filter_columns mismatch",
		"partition mismatch between Doris and StarRocks",
		"Iceberg partition columns mismatch",
		"index mismatch on FULLTEXT(clinical_notes): properties differ",
		"index mismatch: NGRAM(full_name) present in Doris but missing in StarRocks",
		"index mismatch: SECONDARY(tenant_id) present in StarRocks but missing in Doris",
	} {
		if !strings.Contains(driftMismatches, want) {
			t.Errorf("expected drift CheckSchemaParity to report %q, got:\n%s", want, driftMismatches)
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
