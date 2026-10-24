package doris_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/protocgen/atlas-doris/pkg/doris"
)

func TestParseRealPlatformDorisAndIcebergDDLs(t *testing.T) {
	schemasDir := "/Users/ab/code/azra/platform/schemas/iceberg"
	if _, err := os.Stat(schemasDir); os.IsNotExist(err) {
		t.Skipf("platform schemas directory %s not present", schemasDir)
	}

	t.Run("doris_tenant_profile_ddl.sql (54 Doris tables)", func(t *testing.T) {
		raw, err := os.ReadFile(filepath.Join(schemasDir, "doris_tenant_profile_ddl.sql"))
		if err != nil {
			t.Fatalf("read doris_tenant_profile_ddl.sql: %v", err)
		}
		schema, err := doris.ParseSchema(string(raw))
		if err != nil {
			t.Fatalf("ParseSchema(doris_tenant_profile_ddl.sql) failed: %v", err)
		}
		if len(schema.Databases) != 1 || schema.Databases[0].CanonicalName() != "internal.tenant_${TENANT_SLUG}" {
			t.Fatalf("expected 1 database internal.tenant_${TENANT_SLUG}, got %+v", schema.Databases)
		}
		if len(schema.Tables) != 54 {
			t.Fatalf("expected 54 tables in doris_tenant_profile_ddl.sql, got %d", len(schema.Tables))
		}

		// Verify static linter passes with zero violations
		if violations := doris.LintSchema(schema); len(violations) > 0 {
			t.Fatalf("unexpected lint violations on doris_tenant_profile_ddl.sql: %v", violations)
		}

		// Verify round-trip FormatSchema -> ParseSchema -> FormatSchema idempotence
		formatted1 := doris.FormatSchema(schema)
		reParsed, err := doris.ParseSchema(formatted1)
		if err != nil {
			t.Fatalf("re-parse formatted doris_tenant_profile_ddl.sql failed: %v", err)
		}
		formatted2 := doris.FormatSchema(reParsed)
		if formatted1 != formatted2 {
			t.Fatalf("FormatSchema is not idempotent on doris_tenant_profile_ddl.sql")
		}

		// Diff(schema, reParsed) must be empty
		diffStmts, err := doris.Diff(schema, reParsed)
		if err != nil {
			t.Fatalf("Diff(schema, reParsed) error: %v", err)
		}
		if len(diffStmts) != 0 {
			t.Fatalf("expected 0 diff statements between parsed and formatted schema, got %d: %v", len(diffStmts), doris.FormatStatements(diffStmts))
		}
	})

	t.Run("iceberg_tenant_profile_ddl.sql (54 Iceberg tables)", func(t *testing.T) {
		raw, err := os.ReadFile(filepath.Join(schemasDir, "iceberg_tenant_profile_ddl.sql"))
		if err != nil {
			t.Fatalf("read iceberg_tenant_profile_ddl.sql: %v", err)
		}
		schema, err := doris.ParseSchema(string(raw))
		if err != nil {
			t.Fatalf("ParseSchema(iceberg_tenant_profile_ddl.sql) failed: %v", err)
		}
		if len(schema.Databases) != 1 || schema.Databases[0].CanonicalName() != "azra_lake.tenant_${TENANT_SLUG}" {
			t.Fatalf("expected 1 database azra_lake.tenant_${TENANT_SLUG}, got %+v", schema.Databases)
		}
		if len(schema.Tables) != 54 {
			t.Fatalf("expected 54 tables in iceberg_tenant_profile_ddl.sql, got %d", len(schema.Tables))
		}

		partitionedCount := 0
		for _, tbl := range schema.Tables {
			if !tbl.IsIcebergTable() {
				t.Errorf("expected table %s to be recognized as Iceberg table", tbl.CanonicalName())
			}
			if tbl.Partition != nil {
				partitionedCount++
			}
		}
		if partitionedCount != 13 {
			t.Errorf("expected 13 Tier-B partitioned Iceberg tables, got %d", partitionedCount)
		}

		formatted1 := doris.FormatSchema(schema)
		reParsed, err := doris.ParseSchema(formatted1)
		if err != nil {
			t.Fatalf("re-parse formatted iceberg_tenant_profile_ddl.sql failed: %v", err)
		}
		formatted2 := doris.FormatSchema(reParsed)
		if formatted1 != formatted2 {
			t.Fatalf("FormatSchema is not idempotent on iceberg_tenant_profile_ddl.sql")
		}
	})

	t.Run("doris_setup.sql (vault, workload groups, catalogs, terminology, RAG)", func(t *testing.T) {
		raw, err := os.ReadFile(filepath.Join(schemasDir, "doris_setup.sql"))
		if err != nil {
			t.Fatalf("read doris_setup.sql: %v", err)
		}
		schema, err := doris.ParseSchema(string(raw))
		if err != nil {
			t.Fatalf("ParseSchema(doris_setup.sql) failed: %v", err)
		}
		if len(schema.PlatformObjects) < 9 {
			t.Errorf("expected at least 9 platform objects (vault, default vault, 5 workload groups, policy, 2 catalogs), got %d", len(schema.PlatformObjects))
		}
		if len(schema.Tables) != 42 {
			t.Errorf("expected 42 tables in doris_setup.sql (32 Iceberg terminology + 8 Doris rollup + 1 telemetry + 1 RAG), got %d", len(schema.Tables))
		}

		// Check loinc_codes has `system` column properly parsed and backtick-formatted
		loinc, ok := schema.TableByName("azra_lake.terminology.loinc_codes")
		if !ok {
			t.Fatalf("missing azra_lake.terminology.loinc_codes")
		}
		if _, ok := loinc.ColumnByName("system"); !ok {
			t.Errorf("expected column 'system' in loinc_codes")
		}
		if !strings.Contains(doris.FormatCreateTable(loinc), "`system` VARCHAR(256)") {
			t.Errorf("expected backtick-quoted `system` in formatted loinc_codes DDL")
		}

		// Check clinical_concept_search has DUPLICATE KEY and ANN index
		rag, ok := schema.TableByName("internal.terminology_rag.clinical_concept_search")
		if !ok {
			t.Fatalf("missing internal.terminology_rag.clinical_concept_search")
		}
		if rag.Key == nil || rag.Key.Kind != doris.KeyDuplicate {
			t.Errorf("expected DUPLICATE KEY on clinical_concept_search, got %+v", rag.Key)
		}
		annIdx, ok := rag.IndexByName("idx_embedding_hnsw")
		if !ok || annIdx.Using != "ANN" {
			t.Errorf("expected ANN index idx_embedding_hnsw on clinical_concept_search, got %+v", annIdx)
		}
	})

	t.Run("vw_patient_summaries.sql (CREATE OR REPLACE VIEW)", func(t *testing.T) {
		raw, err := os.ReadFile(filepath.Join(schemasDir, "vw_patient_summaries.sql"))
		if err != nil {
			t.Fatalf("read vw_patient_summaries.sql: %v", err)
		}
		schema, err := doris.ParseSchema(string(raw))
		if err != nil {
			t.Fatalf("ParseSchema(vw_patient_summaries.sql) failed: %v", err)
		}
		if len(schema.Views) != 1 {
			t.Fatalf("expected 1 view in vw_patient_summaries.sql, got %d", len(schema.Views))
		}
		if schema.Views[0].CanonicalName() != "internal.tenant_${TENANT_SLUG}.vw_patient_summaries" {
			t.Errorf("unexpected view canonical name: %q", schema.Views[0].CanonicalName())
		}
	})
}

func TestLintRules_AllSevenRules(t *testing.T) {
	baseSQL := `
CREATE TABLE IF NOT EXISTS internal.tenant_test.patients (
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(32) NOT NULL DEFAULT "",
    age TINYINT,
    score FLOAT,
    publication_generation BIGINT NOT NULL DEFAULT "0",
    INDEX idx_patients_mrn (mrn) USING INVERTED
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS 4
PROPERTIES (
    "storage_vault_name" = "azra_lakehouse_vault",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "publication_generation",
    "colocate_with" = "azra_patient_group"
);`

	baseSchema, err := doris.ParseSchema(baseSQL)
	if err != nil {
		t.Fatalf("parse baseSQL: %v", err)
	}

	t.Run("DORIS001_IMMUTABLE_KEY", func(t *testing.T) {
		after := baseSchema.Clone()
		after.Tables[0].Key.Columns = []string{"patient_id", "mrn"}
		violations := doris.LintSchemaEvolution(baseSchema, after, doris.LintOptions{})
		assertHasRule(t, violations, doris.DORIS001_IMMUTABLE_KEY)
	})

	t.Run("DORIS002_IMMUTABLE_SEQUENCE", func(t *testing.T) {
		after := baseSchema.Clone()
		after.Tables[0].SetProperty("function_column.sequence_col", "age")
		violations := doris.LintSchemaEvolution(baseSchema, after, doris.LintOptions{})
		assertHasRule(t, violations, doris.DORIS002_IMMUTABLE_SEQUENCE)
	})

	t.Run("DORIS003_IMMUTABLE_DISTRIBUTION", func(t *testing.T) {
		after := baseSchema.Clone()
		after.Tables[0].Distribution.Columns = []string{"mrn"}
		violations := doris.LintSchemaEvolution(baseSchema, after, doris.LintOptions{})
		assertHasRule(t, violations, doris.DORIS003_IMMUTABLE_DISTRIBUTION)
	})

	t.Run("DORIS004_NO_COLUMN_DROP", func(t *testing.T) {
		after := baseSchema.Clone()
		// Drop "score" column
		after.Tables[0].Columns = append(after.Tables[0].Columns[:3], after.Tables[0].Columns[4:]...)
		violations := doris.LintSchemaEvolution(baseSchema, after, doris.LintOptions{AllowDestructive: false})
		assertHasRule(t, violations, doris.DORIS004_NO_COLUMN_DROP)

		// Allowed when AllowDestructive is true
		allowed := doris.LintSchemaEvolution(baseSchema, after, doris.LintOptions{AllowDestructive: true})
		if len(allowed) != 0 {
			t.Fatalf("expected 0 violations with AllowDestructive=true, got %v", allowed)
		}
	})

	t.Run("DORIS005_NON_NULL_SCALAR_DEFAULT", func(t *testing.T) {
		after := baseSchema.Clone()
		after.Tables[0].Columns = append(after.Tables[0].Columns, &doris.ColumnDef{
			Name:       "status",
			Type:       "VARCHAR(32)",
			Nullable:   false,
			HasDefault: false,
		})
		violations := doris.LintSchemaEvolution(baseSchema, after, doris.LintOptions{})
		assertHasRule(t, violations, doris.DORIS005_NON_NULL_SCALAR_DEFAULT)

		// Adding NOT NULL ARRAY<VARCHAR(32)> without default or scalar with default is allowed
		afterValid := baseSchema.Clone()
		afterValid.Tables[0].Columns = append(afterValid.Tables[0].Columns, &doris.ColumnDef{
			Name:       "status",
			Type:       "VARCHAR(32)",
			Nullable:   false,
			HasDefault: true,
			Default:    "ACTIVE",
		})
		if v := doris.LintSchemaEvolution(baseSchema, afterValid, doris.LintOptions{}); len(v) != 0 {
			t.Fatalf("expected 0 violations when DEFAULT is provided, got %v", v)
		}
	})

	t.Run("DORIS006_SAFE_TYPE_EVOLUTION", func(t *testing.T) {
		// Narrowing VARCHAR(32) -> VARCHAR(16) is rejected
		afterNarrow := baseSchema.Clone()
		afterNarrow.Tables[0].Columns[1].Type = "VARCHAR(16)"
		violations := doris.LintSchemaEvolution(baseSchema, afterNarrow, doris.LintOptions{})
		assertHasRule(t, violations, doris.DORIS006_SAFE_TYPE_EVOLUTION)

		// Widening VARCHAR(32)->VARCHAR(64), TINYINT->INT, FLOAT->DOUBLE is allowed
		afterWide := baseSchema.Clone()
		afterWide.Tables[0].Columns[1].Type = "VARCHAR(64)"
		afterWide.Tables[0].Columns[2].Type = "INT"
		afterWide.Tables[0].Columns[3].Type = "DOUBLE"
		if v := doris.LintSchemaEvolution(baseSchema, afterWide, doris.LintOptions{}); len(v) != 0 {
			t.Fatalf("expected 0 violations on safe widening, got %v", v)
		}
	})

	t.Run("DORIS007_FORBIDDEN_TYPE", func(t *testing.T) {
		for _, forbidden := range []string{"JSONB", "VARIANT", "ARRAY<JSONB>", "ARRAY<VARIANT>"} {
			after := baseSchema.Clone()
			after.Tables[0].Columns = append(after.Tables[0].Columns, &doris.ColumnDef{
				Name:     "raw_payload",
				Type:     forbidden,
				Nullable: true,
			})
			violations := doris.LintSchemaEvolution(baseSchema, after, doris.LintOptions{})
			assertHasRule(t, violations, doris.DORIS007_FORBIDDEN_TYPE)
		}
	})
}

func TestDiffAndReplay_EndToEnd(t *testing.T) {
	beforeSQL := `
CREATE DATABASE IF NOT EXISTS internal.tenant_duke;

CREATE TABLE IF NOT EXISTS internal.tenant_duke.patients (
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(32) NOT NULL DEFAULT "",
    age SMALLINT,
    publication_generation BIGINT NOT NULL DEFAULT "0",
    INDEX idx_patients_mrn (mrn) USING INVERTED
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS 4
PROPERTIES (
    "storage_vault_name" = "azra_lakehouse_vault",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "publication_generation",
    "group_commit_interval_ms" = "200"
);`

	afterSQL := `
CREATE DATABASE IF NOT EXISTS internal.tenant_duke;

CREATE TABLE IF NOT EXISTS internal.tenant_duke.patients (
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(64) NOT NULL DEFAULT "",
    age INT,
    publication_generation BIGINT NOT NULL DEFAULT "0",
    status VARCHAR(32) NOT NULL DEFAULT "ACTIVE",
    INDEX idx_patients_status (status) USING INVERTED
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS 4
PROPERTIES (
    "storage_vault_name" = "azra_lakehouse_vault",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "publication_generation",
    "group_commit_interval_ms" = "500",
    "file_cache_ttl_seconds" = "604800"
);`

	s1, err := doris.ParseSchema(beforeSQL)
	if err != nil {
		t.Fatalf("ParseSchema(beforeSQL): %v", err)
	}
	s2, err := doris.ParseSchema(afterSQL)
	if err != nil {
		t.Fatalf("ParseSchema(afterSQL): %v", err)
	}

	diffStmts, err := doris.Diff(s1, s2)
	if err != nil {
		t.Fatalf("Diff(s1, s2) failed: %v", err)
	}

	diffText := doris.FormatStatements(diffStmts)
	expectedSubstrings := []string{
		`ALTER TABLE internal.tenant_duke.patients ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT "ACTIVE";`,
		`ALTER TABLE internal.tenant_duke.patients MODIFY COLUMN mrn VARCHAR(64) NOT NULL DEFAULT "";`,
		`ALTER TABLE internal.tenant_duke.patients MODIFY COLUMN age INT;`,
		`ALTER TABLE internal.tenant_duke.patients DROP INDEX idx_patients_mrn;`,
		`ALTER TABLE internal.tenant_duke.patients ADD INDEX idx_patients_status (status) USING INVERTED;`,
		`BUILD INDEX idx_patients_status ON internal.tenant_duke.patients;`,
		`ALTER TABLE internal.tenant_duke.patients SET ("group_commit_interval_ms" = "500");`,
		`ALTER TABLE internal.tenant_duke.patients SET ("file_cache_ttl_seconds" = "604800");`,
	}
	for _, want := range expectedSubstrings {
		if !strings.Contains(diffText, want) {
			t.Errorf("diff SQL missing expected statement %q\nGot:\n%s", want, diffText)
		}
	}

	// Replay initial + diff statements and verify it matches s2
	initialStmts, err := doris.ParseStatements(beforeSQL)
	if err != nil {
		t.Fatalf("ParseStatements(beforeSQL): %v", err)
	}
	replayed, err := doris.ReplayStatements(append(initialStmts, diffStmts...))
	if err != nil {
		t.Fatalf("ReplayStatements failed: %v", err)
	}
	if doris.FormatSchema(replayed) != doris.FormatSchema(s2) {
		t.Fatalf("replayed schema mismatch:\nwant:\n%s\ngot:\n%s", doris.FormatSchema(s2), doris.FormatSchema(replayed))
	}
}

func assertHasRule(t *testing.T, violations []doris.Violation, want doris.LintRule) {
	t.Helper()
	for _, v := range violations {
		if v.Rule == want {
			return
		}
	}
	t.Fatalf("expected lint violation rule %s, got %v", want, violations)
}
