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
		if len(schema.Tables) < 42 {
			t.Errorf("expected at least 42 tables in doris_setup.sql, got %d", len(schema.Tables))
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

		// Static LintTable rejects FLOAT / DOUBLE key columns
		floatKeyTbl := baseSchema.Tables[0].Clone()
		floatKeyTbl.Key.Columns = []string{"score"}
		assertHasRule(t, doris.LintTable(floatKeyTbl), doris.DORIS001_IMMUTABLE_KEY)
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

		// Mutating colocate_with on an existing table is also blocked by DORIS003
		afterColocate := baseSchema.Clone()
		afterColocate.Tables[0].SetProperty("colocate_with", "other_group")
		assertHasRule(t, doris.LintSchemaEvolution(baseSchema, afterColocate, doris.LintOptions{}), doris.DORIS003_IMMUTABLE_DISTRIBUTION)
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

		// Widening VARCHAR(32)->VARCHAR(64), TINYINT->LARGEINT, FLOAT->DOUBLE is allowed
		afterWide := baseSchema.Clone()
		afterWide.Tables[0].Columns[1].Type = "VARCHAR(64)"
		afterWide.Tables[0].Columns[2].Type = "LARGEINT"
		afterWide.Tables[0].Columns[3].Type = "DOUBLE"
		if v := doris.LintSchemaEvolution(baseSchema, afterWide, doris.LintOptions{}); len(v) != 0 {
			t.Fatalf("expected 0 violations on safe widening, got %v", v)
		}

		// DECIMAL / DECIMALV3 precision widening with identical scale is safe; scale change or precision narrowing is rejected
		if !doris.IsSafeTypeEvolution("DECIMAL(10, 2)", "DECIMAL(18, 2)") {
			t.Errorf("expected DECIMAL(10, 2) -> DECIMAL(18, 2) to be safe")
		}
		if !doris.IsSafeTypeEvolution("DECIMALV3(12, 4)", "DECIMALV3(18, 4)") {
			t.Errorf("expected DECIMALV3(12, 4) -> DECIMALV3(18, 4) to be safe")
		}
		if doris.IsSafeTypeEvolution("DECIMALV3(18, 4)", "DECIMALV3(12, 4)") {
			t.Errorf("expected DECIMALV3(18, 4) -> DECIMALV3(12, 4) narrowing to be rejected")
		}
		if doris.IsSafeTypeEvolution("DECIMALV3(12, 2)", "DECIMALV3(18, 4)") {
			t.Errorf("expected DECIMALV3(12, 2) -> DECIMALV3(18, 4) scale change to be rejected")
		}
		if !doris.IsSafeTypeEvolution("BIGINT", "LARGEINT") {
			t.Errorf("expected BIGINT -> LARGEINT to be safe")
		}

		// ANN index invariants on LintTable
		badANN := &doris.TableDef{
			Name: "bad_ann",
			Key:  &doris.KeyClause{Kind: doris.KeyUnique, Columns: []string{"id"}},
			Columns: []*doris.ColumnDef{
				{Name: "id", Type: "VARCHAR(64)", Nullable: false},
				{Name: "embedding", Type: "ARRAY<DOUBLE>", Nullable: true},
			},
			Indexes: []*doris.IndexDef{
				{
					Name:    "idx_emb",
					Columns: []string{"embedding"},
					Using:   "ANN",
					Properties: []doris.Property{
						{Key: "index_type", Value: "flat"},
						{Key: "dim", Value: "0"},
					},
				},
			},
		}
		assertHasRule(t, doris.LintTable(badANN), doris.DORIS006_SAFE_TYPE_EVOLUTION)

		// Valid ANN index on UNIQUE KEY table with merge-on-write enabled and IVF / HNSW
		goodUniqueMoWANN := &doris.TableDef{
			Name: "good_unique_mow_ann",
			Key:  &doris.KeyClause{Kind: doris.KeyUnique, Columns: []string{"id"}},
			Columns: []*doris.ColumnDef{
				{Name: "id", Type: "VARCHAR(64)", Nullable: false},
				{Name: "embedding", Type: "ARRAY<FLOAT>", Nullable: false},
			},
			Properties: []doris.Property{
				{Key: "enable_unique_key_merge_on_write", Value: "true"},
			},
			Indexes: []*doris.IndexDef{
				{
					Name:    "idx_emb_ivf",
					Columns: []string{"embedding"},
					Using:   "ANN",
					Properties: []doris.Property{
						{Key: "index_type", Value: "ivf"},
						{Key: "metric_type", Value: "inner_product"},
						{Key: "dim", Value: "768"},
					},
				},
			},
		}
		if v := doris.LintTable(goodUniqueMoWANN); len(v) != 0 {
			t.Fatalf("expected 0 violations on UNIQUE KEY MoW table with valid IVF ANN index, got %v", v)
		}

		// Missing metric_type is rejected
		missingMetricANN := goodUniqueMoWANN.Clone()
		missingMetricANN.Indexes[0].Properties = []doris.Property{
			{Key: "index_type", Value: "hnsw"},
			{Key: "dim", Value: "768"},
		}
		assertHasRule(t, doris.LintTable(missingMetricANN), doris.DORIS006_SAFE_TYPE_EVOLUTION)
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

	// Verify Iceberg ALTER TABLE ADD COLUMN strips NOT NULL and DEFAULT
	iceBefore, err := doris.ParseSchema(`
CREATE TABLE IF NOT EXISTS azra_lake.tenant_duke.patients (
    patient_id VARCHAR(64) NOT NULL
)
PROPERTIES (
    "compression_codec" = "zstd"
);`)
	if err != nil {
		t.Fatalf("parse iceBefore: %v", err)
	}
	iceAfter, err := doris.ParseSchema(`
CREATE TABLE IF NOT EXISTS azra_lake.tenant_duke.patients (
    patient_id VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT "ACTIVE"
)
PROPERTIES (
    "compression_codec" = "zstd"
);`)
	if err != nil {
		t.Fatalf("parse iceAfter: %v", err)
	}
	iceStmts, err := doris.Diff(iceBefore, iceAfter)
	if err != nil {
		t.Fatalf("Diff(iceBefore, iceAfter) failed: %v", err)
	}
	iceDiffSQL := doris.FormatStatements(iceStmts)
	if !strings.Contains(iceDiffSQL, "ALTER TABLE azra_lake.tenant_duke.patients ADD COLUMN status VARCHAR(32);") {
		t.Errorf("expected Iceberg ADD COLUMN without NOT NULL or DEFAULT, got:\n%s", iceDiffSQL)
	}
	iceInitStmts, err := doris.ParseStatements(`
CREATE TABLE IF NOT EXISTS azra_lake.tenant_duke.patients (
    patient_id VARCHAR(64) NOT NULL
)
PROPERTIES (
    "compression_codec" = "zstd"
);`)
	if err != nil {
		t.Fatalf("ParseStatements(iceBefore): %v", err)
	}
	iceReplayed, err := doris.ReplayStatements(append(iceInitStmts, iceStmts...))
	if err != nil {
		t.Fatalf("ReplayStatements(ice): %v", err)
	}
	iceFollowUp, err := doris.Diff(iceReplayed, iceAfter)
	if err != nil {
		t.Fatalf("Diff(iceReplayed, iceAfter): %v", err)
	}
	if len(iceFollowUp) != 0 {
		t.Fatalf("expected 0 follow-up statements after Iceberg ADD COLUMN, got:\n%s", doris.FormatStatements(iceFollowUp))
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

func TestCanonicalMatching_TwoPartSQL_HCL_LiveInspect(t *testing.T) {
	twoPartSQL := `
CREATE DATABASE IF NOT EXISTS terminology;

CREATE TABLE IF NOT EXISTS terminology.concepts (
    concept_id VARCHAR(64) NOT NULL,
    code VARCHAR(64) NOT NULL DEFAULT "",
    display VARCHAR(256)
)
UNIQUE KEY(concept_id)
DISTRIBUTED BY HASH(concept_id) BUCKETS 8
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "replication_num" = "1"
);

CREATE OR REPLACE VIEW terminology.vw_concepts AS
SELECT concept_id, code, display FROM terminology.concepts;
`

	hclText := `
schema "terminology" {
  catalog = "internal"
}

table "concepts" {
  schema = schema.terminology
  column "concept_id" {
    type = varchar(64)
    null = false
  }
  column "code" {
    type = varchar(64)
    null = false
    default = ""
  }
  column "display" {
    type = varchar(256)
    null = true
  }
  unique_key {
    columns = [column.concept_id]
    merge_on_write = true
  }
  distributed_by_hash {
    columns = [column.concept_id]
    buckets = 8
  }
  properties = {
    enable_unique_key_merge_on_write = "true"
    replication_num = "1"
  }
}

view "vw_concepts" {
  schema = schema.terminology
  or_replace = true
  as = "SELECT concept_id, code, display FROM terminology.concepts"
}
`

	sqlSchema, err := doris.ParseSchema(twoPartSQL)
	if err != nil {
		t.Fatalf("ParseSchema(twoPartSQL): %v", err)
	}
	hclSchema, err := doris.ParseHCL(hclText)
	if err != nil {
		t.Fatalf("ParseHCL(hclText): %v", err)
	}

	// Verify TableByName, DatabaseByName, ViewByName work with both 2-part and 3-part names on both schemas
	for _, s := range []*doris.Schema{sqlSchema, hclSchema} {
		if _, ok := s.DatabaseByName("terminology"); !ok {
			t.Errorf("DatabaseByName(terminology) failed")
		}
		if _, ok := s.DatabaseByName("internal.terminology"); !ok {
			t.Errorf("DatabaseByName(internal.terminology) failed")
		}
		if _, ok := s.TableByName("terminology.concepts"); !ok {
			t.Errorf("TableByName(terminology.concepts) failed")
		}
		if _, ok := s.TableByName("internal.terminology.concepts"); !ok {
			t.Errorf("TableByName(internal.terminology.concepts) failed")
		}
		if _, ok := s.ViewByName("terminology.vw_concepts"); !ok {
			t.Errorf("ViewByName(terminology.vw_concepts) failed")
		}
		if _, ok := s.ViewByName("internal.terminology.vw_concepts"); !ok {
			t.Errorf("ViewByName(internal.terminology.vw_concepts) failed")
		}
	}

	// Diff between 2-part SQL and 3-part internal HCL in both directions must be 0 statements
	stmts1, err := doris.Diff(sqlSchema, hclSchema)
	if err != nil || len(stmts1) != 0 {
		t.Fatalf("Diff(sqlSchema, hclSchema) = (%v, %v), want 0 stmts", doris.FormatStatements(stmts1), err)
	}
	stmts2, err := doris.Diff(hclSchema, sqlSchema)
	if err != nil || len(stmts2) != 0 {
		t.Fatalf("Diff(hclSchema, sqlSchema) = (%v, %v), want 0 stmts", doris.FormatStatements(stmts2), err)
	}

	// FormatDatabaseHCL and FormatTableHCL on 2-part SQL must match FormatDatabaseHCL and FormatTableHCL on 3-part HCL
	if doris.FormatDatabaseHCL(sqlSchema.Databases[0]) != doris.FormatDatabaseHCL(hclSchema.Databases[0]) {
		t.Fatalf("FormatDatabaseHCL mismatch:\n--- sqlSchema ---\n%s\n--- hclSchema ---\n%s",
			doris.FormatDatabaseHCL(sqlSchema.Databases[0]), doris.FormatDatabaseHCL(hclSchema.Databases[0]))
	}
	if doris.FormatTableHCL(sqlSchema.Tables[0]) != doris.FormatTableHCL(hclSchema.Tables[0]) {
		t.Fatalf("FormatTableHCL mismatch:\n--- sqlSchema ---\n%s\n--- hclSchema ---\n%s",
			doris.FormatTableHCL(sqlSchema.Tables[0]), doris.FormatTableHCL(hclSchema.Tables[0]))
	}

	// Schema.Merge deduplicates 2-part SQL and 3-part HCL
	merged := doris.NewSchema()
	merged.Merge(sqlSchema)
	merged.Merge(hclSchema)
	if len(merged.Databases) != 1 || len(merged.Tables) != 1 || len(merged.Views) != 1 {
		t.Fatalf("expected 1 db, 1 table, 1 view after Merge, got dbs=%d tables=%d views=%d", len(merged.Databases), len(merged.Tables), len(merged.Views))
	}
}

func TestDiffTable_AddColumnFirstAndAfterOrder(t *testing.T) {
	beforeSQL := `
CREATE TABLE IF NOT EXISTS internal.tenant_duke.patients (
    patient_id VARCHAR(64) NOT NULL,
    legacy_col VARCHAR(16),
    created_at DATETIMEV2(6) NOT NULL DEFAULT "1970-01-01 00:00:00"
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS 4;`

	afterSQL := `
CREATE TABLE IF NOT EXISTS internal.tenant_duke.patients (
    tenant_id VARCHAR(64) NOT NULL DEFAULT "default_tenant",
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(64) NOT NULL DEFAULT "unassigned",
    status VARCHAR(32) NOT NULL DEFAULT "ACTIVE",
    created_at DATETIMEV2(6) NOT NULL DEFAULT "1970-01-01 00:00:00",
    updated_at DATETIMEV2(6) NOT NULL DEFAULT "1970-01-01 00:00:00"
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS 4;`

	sBefore, err := doris.ParseSchema(beforeSQL)
	if err != nil {
		t.Fatalf("ParseSchema(beforeSQL): %v", err)
	}
	sAfter, err := doris.ParseSchema(afterSQL)
	if err != nil {
		t.Fatalf("ParseSchema(afterSQL): %v", err)
	}

	diffStmts, err := doris.DiffWithOptions(sBefore, sAfter, doris.DiffOptions{
		LintOptions: doris.LintOptions{AllowDestructive: true},
	})
	if err != nil {
		t.Fatalf("DiffWithOptions: %v", err)
	}
	diffText := doris.FormatStatements(diffStmts)
	for _, want := range []string{
		`ALTER TABLE internal.tenant_duke.patients DROP COLUMN legacy_col;`,
		`ALTER TABLE internal.tenant_duke.patients ADD COLUMN tenant_id VARCHAR(64) NOT NULL DEFAULT "default_tenant" FIRST;`,
		`ALTER TABLE internal.tenant_duke.patients ADD COLUMN mrn VARCHAR(64) NOT NULL DEFAULT "unassigned" AFTER patient_id;`,
		`ALTER TABLE internal.tenant_duke.patients ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT "ACTIVE" AFTER mrn;`,
		`ALTER TABLE internal.tenant_duke.patients ADD COLUMN updated_at DATETIMEV2(6) NOT NULL DEFAULT "1970-01-01 00:00:00";`,
	} {
		if !strings.Contains(diffText, want) {
			t.Errorf("diff SQL missing %q\nGot:\n%s", want, diffText)
		}
	}

	initStmts, err := doris.ParseStatements(beforeSQL)
	if err != nil {
		t.Fatalf("ParseStatements(beforeSQL): %v", err)
	}
	replayed, err := doris.ReplayStatements(append(initStmts, diffStmts...))
	if err != nil {
		t.Fatalf("ReplayStatements: %v", err)
	}
	if doris.FormatSchema(replayed) != doris.FormatSchema(sAfter) {
		t.Fatalf("replayed schema does not match desired column order:\nwant:\n%s\ngot:\n%s", doris.FormatSchema(sAfter), doris.FormatSchema(replayed))
	}
}

func TestLiveDorisShowCreateTable_NormalizationAndDiff(t *testing.T) {
	liveShowCreateTable := "CREATE TABLE `patients` (\n" +
		"  `patient_id` varchar(64) NOT NULL,\n" +
		"  `active` tinyint(1) NOT NULL DEFAULT \"1\",\n" +
		"  `age` tinyint(4) NULL,\n" +
		"  `visit_count` int(11) NOT NULL DEFAULT \"0\",\n" +
		"  `total_events` bigint(20) NOT NULL DEFAULT \"0\",\n" +
		"  `huge_id` largeint(40) NULL,\n" +
		"  `score` double precision NULL,\n" +
		"  `code` character varying(32) NOT NULL DEFAULT \"\"\n" +
		") ENGINE=OLAP\n" +
		"UNIQUE KEY(`patient_id`)\n" +
		"DISTRIBUTED BY HASH(`patient_id`) BUCKETS 8\n" +
		"PROPERTIES (\n" +
		"\"replication_allocation\" = \"tag.location.default: 1\",\n" +
		"\"enable_unique_key_merge_on_write\" = \"true\"\n" +
		");"

	liveSchema, err := doris.ParseSchema(liveShowCreateTable)
	if err != nil {
		t.Fatalf("ParseSchema(liveShowCreateTable): %v", err)
	}
	liveSchema.Tables[0].Catalog = "internal"
	liveSchema.Tables[0].Database = "tenant_duke"
	liveSchema.Tables[0].FullName = liveSchema.Tables[0].CanonicalName()

	canonicalSQL := `
CREATE TABLE IF NOT EXISTS internal.tenant_duke.patients (
    patient_id VARCHAR(64) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT "1",
    age TINYINT,
    visit_count INT NOT NULL DEFAULT "0",
    total_events BIGINT NOT NULL DEFAULT "0",
    huge_id LARGEINT,
    score DOUBLE,
    code VARCHAR(32) NOT NULL DEFAULT ""
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);`
	desiredSchema, err := doris.ParseSchema(canonicalSQL)
	if err != nil {
		t.Fatalf("ParseSchema(canonicalSQL): %v", err)
	}

	stmts, err := doris.Diff(liveSchema, desiredSchema)
	if err != nil {
		t.Fatalf("Diff(liveSchema, desiredSchema): %v", err)
	}
	if len(stmts) != 0 {
		t.Fatalf("expected 0 diff statements between live SHOW CREATE TABLE and canonical schema, got:\n%s", doris.FormatStatements(stmts))
	}

	// Changing replication_num from 1 to 3 must still produce an ALTER TABLE SET statement
	desiredRep3 := desiredSchema.Clone()
	desiredRep3.Tables[0].SetProperty("replication_num", "3")
	stmtsRep3, err := doris.Diff(liveSchema, desiredRep3)
	if err != nil || len(stmtsRep3) != 1 {
		t.Fatalf("expected 1 ALTER TABLE SET statement when changing replication_num to 3, got %d (err=%v)", len(stmtsRep3), err)
	}
}

func TestNormalizeType_StructFieldCaseAndMultiWordTypes(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{
			raw:  "struct<patientId:varchar(64), visitCount:int(11), isActive:tinyint(1)>",
			want: "STRUCT<patientId:VARCHAR(64), visitCount:INT, isActive:BOOLEAN>",
		},
		{
			raw:  "STRUCT<`streetName` varchar(128), `zipCode` integer, nested:struct<innerField:decimalv3(12,4)>>",
			want: "STRUCT<streetName:VARCHAR(128), zipCode:INT, nested:STRUCT<innerField:DECIMALV3(12, 4)>>",
		},
		{
			raw:  "array<struct<conceptCode:varchar(64), weight:double precision>>",
			want: "ARRAY<STRUCT<conceptCode:VARCHAR(64), weight:DOUBLE>>",
		},
		{
			raw:  "map<varchar(64), struct<itemCount:bigint(20), smallVal:smallint(6)>>",
			want: "MAP<VARCHAR(64), STRUCT<itemCount:BIGINT, smallVal:SMALLINT>>",
		},
		{
			raw:  "bool",
			want: "BOOLEAN",
		},
	}
	for _, tc := range cases {
		if got := doris.NormalizeType(tc.raw); got != tc.want {
			t.Errorf("NormalizeType(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}

	// Verify HCL round-trip preserves camelCase struct field names
	structSQL := `
CREATE TABLE IF NOT EXISTS internal.tenant_duke.complex_events (
    event_id VARCHAR(64) NOT NULL,
    patient_meta STRUCT<patientId:VARCHAR(64), visitCount:INT, isActive:BOOLEAN>,
    items ARRAY<STRUCT<itemCode:VARCHAR(32), score:DOUBLE>>,
    lookup MAP<VARCHAR(32), STRUCT<tagLabel:VARCHAR(64)>>
)
UNIQUE KEY(event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 4;`

	s1, err := doris.ParseSchema(structSQL)
	if err != nil {
		t.Fatalf("ParseSchema(structSQL): %v", err)
	}
	hcl1 := doris.FormatHCL(s1)
	if !strings.Contains(hcl1, "struct<patientId:varchar(64), visitCount:int, isActive:boolean>") {
		t.Fatalf("FormatHCL did not preserve struct field casing:\n%s", hcl1)
	}
	s2, err := doris.ParseHCL(hcl1)
	if err != nil {
		t.Fatalf("ParseHCL(hcl1): %v", err)
	}
	if doris.FormatSchema(s1) != doris.FormatSchema(s2) {
		t.Fatalf("FormatSchema mismatch after struct HCL round-trip:\nwant:\n%s\ngot:\n%s", doris.FormatSchema(s1), doris.FormatSchema(s2))
	}
}

func FuzzParseAndRoundTripSchema(f *testing.F) {
	seeds := []string{
		`CREATE DATABASE IF NOT EXISTS internal.tenant_fuzz;
CREATE TABLE IF NOT EXISTS internal.tenant_fuzz.patients (
    patient_id VARCHAR(64) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT "true",
    tags ARRAY<VARCHAR(32)> NOT NULL DEFAULT [],
    meta STRUCT<firstName:VARCHAR(64), visitCount:INT>,
    INDEX idx_patients_id (patient_id) USING INVERTED
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS 4
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true"
);`,
		`schema "tenant_fuzz" {
  catalog = "internal"
}
table "events" {
  schema = schema.tenant_fuzz
  column "id" {
    type = varchar(64)
    null = false
  }
  column "payload" {
    type = text
    null = true
  }
  duplicate_key {
    columns = [column.id]
  }
  distributed_by_hash {
    columns = [column.id]
    buckets = 8
  }
}`,
	}
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		s, err := doris.ParseSchema(input)
		if err != nil || s == nil {
			return
		}
		sql1 := doris.FormatSchema(s)
		if sql1 == "" {
			return
		}
		s2, err := doris.ParseSchema(sql1)
		if err != nil {
			t.Fatalf("re-parse formatted SQL failed:\n%s\nerr: %v", sql1, err)
		}
		sql2 := doris.FormatSchema(s2)
		if sql1 != sql2 {
			t.Fatalf("FormatSchema not idempotent:\n--- 1 ---\n%s\n--- 2 ---\n%s", sql1, sql2)
		}
	})
}

func TestParserAndAST_ErrorAndEdgeBranches(t *testing.T) {
	// Exercise parser error branches
	badSQLs := []string{
		"SELECT 1;",
		"CREATE TABLE;",
		"CREATE TABLE t ();",
		"CREATE TABLE t (id );",
		"CREATE TABLE t (id VARCHAR(64) UNKNOWN_MODIFIER);",
		"CREATE TABLE t (id VARCHAR(64), INDEX () USING INVERTED);",
		"CREATE TABLE t (id VARCHAR(64), INDEX idx1 () USING INVERTED);",
		"CREATE TABLE t (id VARCHAR(64)) UNIQUE KEY();",
		"CREATE TABLE t (id VARCHAR(64)) DISTRIBUTED BY INVALID;",
		"CREATE TABLE t (id VARCHAR(64)) UNEXPECTED_TAIL;",
		"CREATE VIEW v SELECT 1;",
		"ALTER TABLE t UNKNOWN_ACTION;",
		"ALTER TABLE t DROP COLUMN;",
		"ALTER TABLE t DROP INDEX;",
		"ALTER TABLE t SET bad;",
		"BUILD INDEX idx1 t;",
		"DROP TABLE;",
		"CREATE TABLE t (id VARCHAR(64) COMMENT 'unterminated);",
		"CREATE TABLE t (id VARCHAR(64) /* unclosed comment );",
		"CREATE TABLE t (id VARCHAR(64)); )",
	}
	for _, bad := range badSQLs {
		if _, err := doris.ParseSchema(bad); err == nil {
			t.Errorf("expected error for ParseSchema(%q)", bad)
		}
	}

	// Exercise ApplyStatement error branches
	s := doris.NewSchema()
	for _, badStmt := range []doris.Statement{
		{Kind: doris.StmtCreateDatabase},
		{Kind: doris.StmtCreateTable},
		{Kind: doris.StmtDropTable, TargetTable: "missing_tbl"},
		{Kind: doris.StmtAlterTableAddColumn, TargetTable: "missing_tbl"},
		{Kind: doris.StmtAlterTableDropColumn, TargetTable: "missing_tbl"},
		{Kind: doris.StmtAlterTableModifyColumn, TargetTable: "missing_tbl"},
		{Kind: doris.StmtAlterTableAddIndex, TargetTable: "missing_tbl"},
		{Kind: doris.StmtAlterTableDropIndex, TargetTable: "missing_tbl"},
		{Kind: doris.StmtBuildIndex, TargetTable: "missing_tbl"},
		{Kind: doris.StmtAlterTableSetProperties, TargetTable: "missing_tbl"},
		{Kind: doris.StmtCreateOrReplaceView},
		{Kind: doris.StmtPlatformObject},
		{Kind: doris.StatementKind("UNKNOWN")},
	} {
		if err := doris.ApplyStatement(s, badStmt); err == nil {
			t.Errorf("expected error from ApplyStatement(%+v)", badStmt)
		}
	}
	if err := doris.ApplyStatement(nil, doris.Statement{}); err == nil {
		t.Errorf("expected error from ApplyStatement(nil, ...)")
	}

	// Exercise nil AST receivers
	var (
		nilTbl  *doris.TableDef
		nilDB   *doris.DatabaseDef
		nilView *doris.ViewDef
		nilPlat *doris.PlatformObject
		nilCol  *doris.ColumnDef
		nilIdx  *doris.IndexDef
		nilKey  *doris.KeyClause
		nilPart *doris.PartitionClause
		nilDist *doris.DistributionClause
	)
	if nilTbl.CanonicalName() != "" || nilTbl.LookupKey() != "" || nilTbl.IsIcebergTable() || nilTbl.Clone() != nil {
		t.Errorf("unexpected non-zero values on nil TableDef")
	}
	if _, ok := nilTbl.ColumnByName("x"); ok {
		t.Errorf("expected false on nilTbl.ColumnByName")
	}
	if _, ok := nilTbl.IndexByName("x"); ok {
		t.Errorf("expected false on nilTbl.IndexByName")
	}
	if _, ok := nilTbl.GetProperty("x"); ok {
		t.Errorf("expected false on nilTbl.GetProperty")
	}
	if nilDB.CanonicalName() != "" || nilDB.LookupKey() != "" || nilDB.Clone() != nil {
		t.Errorf("unexpected non-zero values on nil DatabaseDef")
	}
	if nilView.CanonicalName() != "" || nilView.LookupKey() != "" || nilView.Clone() != nil {
		t.Errorf("unexpected non-zero values on nil ViewDef")
	}
	if nilPlat.Clone() != nil || nilCol.Clone() != nil || nilIdx.Clone() != nil || nilKey.Clone() != nil || nilPart.Clone() != nil || nilDist.Clone() != nil {
		t.Errorf("expected nil from Clone() on nil AST pointers")
	}
	if doris.FormatColumnDef(nil) != "" || doris.FormatIndexDefInline(nil) != "" || doris.FormatCreateDatabase(nil) != "" ||
		doris.FormatCreateTable(nil) != "" || doris.FormatView(nil) != "" || doris.FormatPlatformObject(nil) != "" ||
		doris.FormatStatements(nil) != "" {
		t.Errorf("expected empty string from Format* on nil/empty inputs")
	}
}
