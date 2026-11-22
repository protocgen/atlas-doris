package doris_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/protocgen/atlas-doris/pkg/doris"
)

func TestValidateTenantChainStatement(t *testing.T) {
	seenTables := make(map[string]bool)
	seenViews := make(map[string]bool)
	var ownedTables []string

	// Valid CREATE TABLE registers table without duplicating ownedTables
	createPatient := "CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patients (id VARCHAR(64) NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;"
	if err := doris.ValidateTenantChainStatement(createPatient, seenTables, seenViews, &ownedTables); err != nil {
		t.Fatalf("unexpected error on CREATE TABLE: %v", err)
	}
	if err := doris.ValidateTenantChainStatement(createPatient, seenTables, seenViews, &ownedTables); err != nil {
		t.Fatalf("unexpected error on duplicate CREATE TABLE: %v", err)
	}
	if len(ownedTables) != 1 || ownedTables[0] != "patients" {
		t.Fatalf("expected ownedTables=[patients], got %v", ownedTables)
	}

	// Valid Iceberg CREATE TABLE, view creation, view alter, DML, and index operations on owned objects
	icebergTbl := "CREATE TABLE IF NOT EXISTS `azra_lake`.`tenant_${TENANT_SLUG}`.`events` (id STRING) ENGINE=iceberg;"
	if err := doris.ValidateTenantChainStatement(icebergTbl, seenTables, seenViews, &ownedTables); err != nil {
		t.Fatalf("unexpected error on Iceberg CREATE TABLE: %v", err)
	}
	createView := "CREATE OR REPLACE VIEW internal.tenant_${TENANT_SLUG}.vw_patients AS SELECT id FROM internal.tenant_${TENANT_SLUG}.patients;"
	if err := doris.ValidateTenantChainStatement(createView, seenTables, seenViews, &ownedTables); err != nil {
		t.Fatalf("unexpected error on CREATE OR REPLACE VIEW: %v", err)
	}
	alterView := "ALTER VIEW internal.tenant_${TENANT_SLUG}.vw_patients AS SELECT id FROM internal.tenant_${TENANT_SLUG}.patients;"
	if err := doris.ValidateTenantChainStatement(alterView, seenTables, seenViews, &ownedTables); err != nil {
		t.Fatalf("unexpected error on ALTER VIEW: %v", err)
	}
	for _, validStmt := range []string{
		"-- GRANT comment should not trigger\n# hash comment REVOKE\n/* block GRANT */ ALTER TABLE internal.tenant_${TENANT_SLUG}.patients ADD COLUMN note VARCHAR(64) DEFAULT 'GRANT ALL';",
		"BUILD INDEX idx_note ON internal.tenant_${TENANT_SLUG}.patients;",
		"CREATE INDEX idx_note2 ON internal.tenant_${TENANT_SLUG}.patients (id) USING INVERTED;",
		"DROP INDEX idx_note ON internal.tenant_${TENANT_SLUG}.patients;",
		"INSERT INTO internal.tenant_${TENANT_SLUG}.patients SELECT id FROM azra_lake.tenant_${TENANT_SLUG}.events;",
		"TRUNCATE TABLE internal.tenant_${TENANT_SLUG}.patients;",
		"DELETE FROM internal.tenant_${TENANT_SLUG}.patients WHERE id = 'escaped\\'GRANT';",
		"DROP VIEW IF EXISTS internal.tenant_${TENANT_SLUG}.vw_patients;",
	} {
		if err := doris.ValidateTenantChainStatement(validStmt, seenTables, seenViews, &ownedTables); err != nil {
			t.Errorf("expected statement to validate cleanly: %q\nerr=%v", validStmt, err)
		}
	}

	// Invalid statements
	longIdent := strings.Repeat("a", 64)
	for _, bad := range []struct {
		name string
		stmt string
	}{
		{"grant", "GRANT ALL ON internal.tenant_${TENANT_SLUG}.patients TO root;"},
		{"revoke", "REVOKE SELECT ON internal.tenant_${TENANT_SLUG}.patients FROM root;"},
		{"create_db", "CREATE DATABASE IF NOT EXISTS internal.tenant_${TENANT_SLUG};"},
		{"drop_db", "DROP DATABASE internal.tenant_${TENANT_SLUG};"},
		{"alter_catalog", "ALTER CATALOG azra_lake RENAME foo;"},
		{"set_var", "SET enable_profile = true;"},
		{"use_db", "USE tenant_${TENANT_SLUG};"},
		{"show", "SHOW TABLES;"},
		{"unowned_alter_table", "ALTER TABLE internal.tenant_${TENANT_SLUG}.unknown_tbl ADD COLUMN a INT;"},
		{"unowned_alter_view", "ALTER VIEW internal.tenant_${TENANT_SLUG}.unknown_vw AS SELECT 1;"},
		{"unowned_build_index", "BUILD INDEX idx_x ON internal.tenant_${TENANT_SLUG}.unknown_tbl;"},
		{"unowned_join_ref", "INSERT INTO internal.tenant_${TENANT_SLUG}.patients SELECT id FROM internal.tenant_${TENANT_SLUG}.foreign_tbl;"},
		{"disallowed_catalog", "CREATE TABLE IF NOT EXISTS mysql.tenant_${TENANT_SLUG}.patients (id INT);"},
		{"disallowed_db", "CREATE TABLE IF NOT EXISTS internal.other_db.patients (id INT);"},
		{"wrong_db_in_ref", "INSERT INTO internal.tenant_${TENANT_SLUG}.patients SELECT id FROM internal.other_tenant.patients;"},
		{"missing_three_part", "SELECT 1;"},
		{"too_long_table_name", "CREATE TABLE internal.tenant_${TENANT_SLUG}." + longIdent + " (id INT);"},
		{"too_long_view_name", "CREATE VIEW internal.tenant_${TENANT_SLUG}." + longIdent + " AS SELECT 1;"},
	} {
		t.Run(bad.name, func(t *testing.T) {
			if err := doris.ValidateTenantChainStatement(bad.stmt, seenTables, seenViews, &ownedTables); err == nil {
				t.Errorf("expected ValidateTenantChainStatement to reject %q", bad.stmt)
			}
		})
	}

	// Nil map resilience
	if err := doris.ValidateTenantChainStatement(createPatient, nil, nil, nil); err != nil {
		t.Fatalf("unexpected error with nil tracking maps: %v", err)
	}
}

func TestIsCoupledModeUnknownPropertiesErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unrelated", errors.New("syntax error near x"), false},
		{"missing_close_brace", errors.New("Unknown properties: {storage_vault_name"), false},
		{"empty_braces", errors.New("Unknown properties: {   }"), false},
		{"storage_vault_only", errors.New("Error 1105: errCode = 2, detailMessage = Unknown properties: {storage_vault_name=azra_vault}"), true},
		{"file_cache_only", errors.New("Unknown properties: { FILE_CACHE_TTL_SECONDS = 3600 }"), true},
		{"both_coupled_props", errors.New("Unknown properties: {storage_vault_name=v1, file_cache_ttl_seconds=86400}"), true},
		{"missing_equals", errors.New("Unknown properties: {storage_vault_name}"), false},
		{"mixed_prop", errors.New("Unknown properties: {storage_vault_name=v1, replication_num=1}"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := doris.IsCoupledModeUnknownPropertiesErr(tc.err); got != tc.want {
				t.Errorf("IsCoupledModeUnknownPropertiesErr(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestStripCoupledModeCreateTableProperties(t *testing.T) {
	t.Run("preserves_other_properties_multiline", func(t *testing.T) {
		in := `CREATE TABLE IF NOT EXISTS internal.tenant_duke.patients (
    patient_id VARCHAR(64) NOT NULL
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS 8
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "storage_vault_name" = "azra_vault",
    "file_cache_ttl_seconds" = "3600",
    "replication_num" = "1"
);`
		got := doris.StripCoupledModeCreateTableProperties(in)
		if strings.Contains(got, "storage_vault_name") || strings.Contains(got, "file_cache_ttl_seconds") {
			t.Fatalf("coupled properties were not stripped:\n%s", got)
		}
		if !strings.Contains(got, `"enable_unique_key_merge_on_write" = "true"`) || !strings.Contains(got, `"replication_num" = "1"`) {
			t.Fatalf("other properties missing:\n%s", got)
		}
	})

	t.Run("strips_entire_properties_block_when_only_coupled", func(t *testing.T) {
		in := `CREATE TABLE internal.tenant_duke.events (
    id BIGINT NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES (
    "storage_vault_name" = "azra_vault",
    "file_cache_ttl_seconds" = "86400"
);`
		got := doris.StripCoupledModeCreateTableProperties(in)
		if strings.Contains(strings.ToUpper(got), "PROPERTIES") {
			t.Fatalf("expected entire PROPERTIES clause to be removed, got:\n%s", got)
		}
		if !strings.HasSuffix(strings.TrimSpace(got), ";") {
			t.Fatalf("expected trailing semicolon preserved, got:\n%s", got)
		}
	})

	t.Run("single_line_properties_preserve_formatting", func(t *testing.T) {
		in := `CREATE TABLE internal.tenant_duke.t (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("storage_vault_name" = "v", "replication_num" = "1");`
		got := doris.StripCoupledModeCreateTableProperties(in)
		if strings.Contains(got, "storage_vault_name") || !strings.Contains(got, `PROPERTIES ("replication_num" = "1")`) {
			t.Fatalf("unexpected single-line strip result:\n%s", got)
		}
	})

	t.Run("non_create_or_no_properties_unchanged", func(t *testing.T) {
		for _, stmt := range []string{
			`ALTER TABLE internal.tenant_duke.t SET ("storage_vault_name" = "v");`,
			`CREATE TABLE internal.tenant_duke.t (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1;`,
			`-- just a comment`,
			`CREATE TABLE internal.tenant_duke.t (id INT) PROPERTIES ("enable_mow" = "true");`,
		} {
			if got := doris.StripCoupledModeCreateTableProperties(stmt); got != stmt {
				t.Errorf("expected unchanged output:\nwant: %s\ngot:  %s", stmt, got)
			}
		}
	})

	t.Run("comments_and_escaped_quotes_in_properties", func(t *testing.T) {
		in := "CREATE TABLE `internal`.`tenant_duke`.`t` (id INT) PROPERTIES ( /* keep */ 'storage_vault_name' = 'v\\'1', 'k' = 'x,y');\n"
		got := doris.StripCoupledModeCreateTableProperties(in)
		if strings.Contains(got, "storage_vault_name") || !strings.Contains(got, "'k' = 'x,y'") {
			t.Fatalf("unexpected strip with quotes/comments: %s", got)
		}
	})
}

func TestNormalizeIcebergCatalogDDL(t *testing.T) {
	t.Run("widens_create_and_alter_only_on_iceberg_catalog", func(t *testing.T) {
		dorisCreate := "-- leading TINYINT comment\n" +
			"CREATE TABLE IF NOT EXISTS internal.tenant_duke.doris_tbl (\n" +
			"    flag TINYINT NOT NULL DEFAULT \"0\",\n" +
			"    code SMALLINT\n" +
			") UNIQUE KEY(flag) DISTRIBUTED BY HASH(flag) BUCKETS 4;"
		iceCreate := "CREATE TABLE IF NOT EXISTS azra_lake.tenant_duke.ice_tbl (\n" +
			"    flag TINYINT NOT NULL COMMENT 'TINYINT literal stays',\n" +
			"    small_id SMALLINT(6) NULL,\n" +
			"    ok BIGINT NOT NULL,\n" +
			"    UNIQUE KEY (flag)\n" +
			")\nENGINE=iceberg\nPROPERTIES (\"format-version\" = \"2\");"
		iceAlter := "ALTER TABLE azra_lake.tenant_duke.ice_tbl ADD COLUMN extra tinyint(1), MODIFY COLUMN small_id smallint NULL, ADD COLUMN (c1 TINYINT, c2 SMALLINT(4)), DROP COLUMN obsolete;"
		customCreate := "CREATE TABLE IF NOT EXISTS custom_lake.tenant_duke.other (a TINYINT) ENGINE=iceberg;"

		if got := doris.NormalizeIcebergCatalogDDL(dorisCreate, ""); got != dorisCreate {
			t.Errorf("internal Doris table TINYINT should remain unchanged:\n%s", got)
		}
		gotIceCreate := doris.NormalizeIcebergCatalogDDL(iceCreate, "")
		if !strings.Contains(gotIceCreate, "flag INT NOT NULL COMMENT 'TINYINT literal stays'") {
			t.Errorf("Iceberg CREATE TABLE TINYINT should widen to INT preserving comment literal:\n%s", gotIceCreate)
		}
		if !strings.Contains(gotIceCreate, "small_id INT NULL") {
			t.Errorf("Iceberg CREATE TABLE SMALLINT(6) should widen to INT:\n%s", gotIceCreate)
		}
		gotIceAlter := doris.NormalizeIcebergCatalogDDL(iceAlter, "")
		if !strings.Contains(gotIceAlter, "ADD COLUMN extra INT") ||
			!strings.Contains(gotIceAlter, "MODIFY COLUMN small_id INT NULL") ||
			!strings.Contains(gotIceAlter, "c1 INT, c2 INT") {
			t.Errorf("Iceberg ALTER TABLE clauses did not widen expected integer types:\n%s", gotIceAlter)
		}
		if got := doris.NormalizeIcebergCatalogDDL(customCreate, ""); got != customCreate {
			t.Errorf("non-matching catalog custom_lake should remain untouched under default azra_lake:\n%s", got)
		}

		// Backtick-wrapped custom catalog
		gotCustom := doris.NormalizeIcebergCatalogDDL(customCreate, " `custom_lake` ")
		if !strings.Contains(gotCustom, "custom_lake.tenant_duke.other (a INT)") {
			t.Errorf("custom_lake should widen when configured:\n%s", gotCustom)
		}
	})

	t.Run("non_table_statements_and_semicolons_in_strings", func(t *testing.T) {
		in := "SELECT 'a;b'; CREATE DATABASE IF NOT EXISTS azra_lake.db1; ALTER TABLE azra_lake.db1.t SET ('k' = 'v');"
		if got := doris.NormalizeIcebergCatalogDDL(in, "azra_lake"); got != in {
			t.Errorf("expected no rewrite for non-column DDL:\ngot:  %s\nwant: %s", got, in)
		}
	})
}

func TestCreateViewDetectionAndNereidsOrderKeys(t *testing.T) {
	if !doris.IsCreateOrAlterViewStatement("  -- header\n  CREATE OR REPLACE VIEW internal.db.v AS SELECT 1;") {
		t.Errorf("expected CREATE OR REPLACE VIEW to be detected")
	}
	if !doris.IsCreateOrAlterViewStatement("/* c */ ALTER VIEW internal.db.v AS SELECT 1;") {
		t.Errorf("expected ALTER VIEW to be detected")
	}
	if doris.IsCreateOrAlterViewStatement("CREATE TABLE internal.db.t (id INT);") {
		t.Errorf("CREATE TABLE must not be treated as view statement")
	}
	if doris.IsCreateOrAlterViewStatement("   ") {
		t.Errorf("whitespace must not be treated as view statement")
	}

	viewSQL := `CREATE OR REPLACE VIEW internal.tenant_duke.vw_latest AS
SELECT
    patient_id,
    MAX_BY(status, STRUCT(event_time, seq_no -- ordering tie-breaker
)) AS latest_status,
    MIN_BY(CONCAT(status, ',', MAX_BY(code, STRUCT(rank_a, rank_b))), STRUCT(priority, id)) AS nested_min
FROM internal.tenant_duke.events
GROUP BY patient_id;`

	got := doris.NormalizeNereidsViewOrderKeys(viewSQL)
	if strings.Contains(got, "STRUCT(event_time") || strings.Contains(got, "STRUCT(priority") || strings.Contains(got, "STRUCT(rank_a") {
		t.Fatalf("expected STRUCT order keys to be rewritten to CONCAT_WS:\n%s", got)
	}
	if !strings.Contains(got, "CONCAT_WS(CHAR(1),") || !strings.Contains(got, "LPAD(CAST(") {
		t.Fatalf("expected CONCAT_WS + LPAD composite encoding in rewritten view SQL:\n%s", got)
	}

	// Non-composite MAX_BY remains untouched, as do struct literals outside MAX_BY/MIN_BY order key
	unchanged := `SELECT STRUCT(a, b), MAX_BY(v, ts) FROM tbl;`
	if res := doris.NormalizeNereidsViewOrderKeys(unchanged); res != unchanged {
		t.Errorf("unexpected modification on scalar MAX_BY:\ngot:  %s\nwant: %s", res, unchanged)
	}

	// Empty field or malformed comment / parens safely leave unchanged or handle line comment terminator
	commentInExpr := `CREATE VIEW IF NOT EXISTS internal.db.v AS SELECT MAX_BY(v, STRUCT(a -- comment
, b)) FROM internal.db.t;`
	gotComment := doris.NormalizeNereidsViewOrderKeys(commentInExpr)
	if !strings.Contains(gotComment, "CONCAT_WS(CHAR(1)") {
		t.Errorf("expected line-commented composite STRUCT to normalize cleanly:\n%s", gotComment)
	}

	for _, s := range []string{
		"CREATE VIEW v AS SELECT MAX_BY(v, STRUCT( )) FROM t;",
		"CREATE VIEW v AS SELECT obj.MAX_BY(v, STRUCT(a, b)) FROM t;",
		"ALTER TABLE IF EXISTS `azra_lake`.`tenant_${TENANT_SLUG}`.`t` ADD COLUMN a TINYINT, ADD COLUMN b SMALLINT;",
	} {
		_ = doris.NormalizeNereidsViewOrderKeys(s)
		_ = doris.NormalizeIcebergCatalogDDL(s, "azra_lake")
	}

	if got := doris.NormalizeIcebergCatalogDDL(
		"ALTER TABLE IF EXISTS `azra_lake`.`tenant_${TENANT_SLUG}`.`t` ADD COLUMN (a TINYINT, `b` SMALLINT(5));",
		"",
	); !strings.Contains(got, "a INT") || !strings.Contains(got, "`b` INT") {
		t.Errorf("expected ALTER TABLE IF EXISTS with backticks and parens to widen:\n%s", got)
	}
}

func TestStage1B_NormalizeSecurityAndEdgeCases(t *testing.T) {
	t.Run("validate_tenant_chain_security_hardening", func(t *testing.T) {
		seenTables := map[string]bool{"patients": true}
		seenViews := map[string]bool{"vw_patients": true}
		var ownedTables = []string{"patients"}

		badCases := []struct {
			name string
			stmt string
		}{
			{"mysql_conditional_comment", "/*!40101 GRANT ALL ON *.* TO root */;"},
			{"trailing_conditional_comment", "CREATE TABLE internal.tenant_${TENANT_SLUG}.t (id INT); /*!50000 DROP DATABASE x */"},
			{"unterminated_block_comment", "CREATE TABLE internal.tenant_${TENANT_SLUG}.t (id INT) /* open"},
			{"unterminated_single_quote", "INSERT INTO internal.tenant_${TENANT_SLUG}.patients VALUES ('abc);"},
			{"unterminated_double_quote", `INSERT INTO internal.tenant_${TENANT_SLUG}.patients VALUES ("abc);`},
			{"unterminated_backtick", "CREATE TABLE internal.tenant_${TENANT_SLUG}.`bad (id INT);"},
			{"multi_statement_smuggling", "CREATE TABLE internal.tenant_${TENANT_SLUG}.t (id INT); DROP DATABASE other_db;"},
			{"double_semicolon", "CREATE TABLE internal.tenant_${TENANT_SLUG}.t (id INT);;"},
			{"alter_table_rename", "ALTER TABLE internal.tenant_${TENANT_SLUG}.patients RENAME other_patients;"},
			{"alter_table_replace_with_table", "ALTER TABLE internal.tenant_${TENANT_SLUG}.patients REPLACE WITH TABLE internal.tenant_${TENANT_SLUG}.patients;"},
			{"two_part_after_from", "CREATE VIEW internal.tenant_${TENANT_SLUG}.v AS SELECT id FROM other_db.secret;"},
			{"two_part_after_join", "CREATE VIEW internal.tenant_${TENANT_SLUG}.v AS SELECT p.id FROM internal.tenant_${TENANT_SLUG}.patients p JOIN other_db.secret s ON p.id = s.id;"},
			{"two_part_after_into", "INSERT INTO other_db.secret SELECT id FROM internal.tenant_${TENANT_SLUG}.patients;"},
			{"two_part_after_like", "CREATE TABLE internal.tenant_${TENANT_SLUG}.c LIKE other_db.secret;"},
			{"two_part_after_using", "DELETE FROM internal.tenant_${TENANT_SLUG}.patients USING other_db.secret;"},
			{"two_part_comma_from_list", "CREATE VIEW internal.tenant_${TENANT_SLUG}.v AS SELECT p.id FROM internal.tenant_${TENANT_SLUG}.patients p, other_db.secret s;"},
			{"external_tvf_s3", `INSERT INTO internal.tenant_${TENANT_SLUG}.patients SELECT * FROM s3("uri" = "s3://bucket/k");`},
			{"external_tvf_hdfs", `CREATE VIEW internal.tenant_${TENANT_SLUG}.v AS SELECT * FROM HDFS("uri" = "hdfs://x");`},
			{"external_tvf_local", `CREATE VIEW internal.tenant_${TENANT_SLUG}.v AS SELECT * FROM local("file_path" = "/etc/passwd");`},
			{"external_tvf_http_stream", `CREATE VIEW internal.tenant_${TENANT_SLUG}.v AS SELECT * FROM http_stream("format" = "csv");`},
			{"backticked_other_tenant", "CREATE VIEW internal.tenant_${TENANT_SLUG}.v AS SELECT id FROM internal.`tenant-other`.patients;"},
			{"non_tenant_slug_placeholder", "CREATE VIEW internal.tenant_${TENANT_SLUG}.v AS SELECT id FROM internal.${OTHER}.patients;"},
			{"four_part_ref", "CREATE VIEW internal.tenant_${TENANT_SLUG}.v AS SELECT id FROM foo.internal.tenant_${TENANT_SLUG}.patients;"},
		}

		for _, tc := range badCases {
			t.Run(tc.name, func(t *testing.T) {
				beforeTables := len(seenTables)
				beforeViews := len(seenViews)
				beforeOwned := len(ownedTables)
				if err := doris.ValidateTenantChainStatement(tc.stmt, seenTables, seenViews, &ownedTables); err == nil {
					t.Errorf("expected ValidateTenantChainStatement to reject %q", tc.stmt)
				}
				if len(seenTables) != beforeTables || len(seenViews) != beforeViews || len(ownedTables) != beforeOwned {
					t.Errorf("failed statement %s mutated tracking state (tables %d->%d, views %d->%d, owned %d->%d)",
						tc.name, beforeTables, len(seenTables), beforeViews, len(seenViews), beforeOwned, len(ownedTables))
				}
			})
		}

		// Valid trailing semicolon + comment, backticked column rename name, and 2-part alias.col must pass
		for _, goodStmt := range []string{
			"CREATE TABLE internal.tenant_${TENANT_SLUG}.new_tbl (id VARCHAR(64), `rename` INT); -- trailing comment\n",
			"CREATE VIEW internal.tenant_${TENANT_SLUG}.vw_alias AS SELECT p.id, v.id FROM internal.tenant_${TENANT_SLUG}.patients p JOIN internal.tenant_${TENANT_SLUG}.vw_patients v ON p.id = v.id WHERE p.id = 'ok;semi';",
		} {
			if err := doris.ValidateTenantChainStatement(goodStmt, seenTables, seenViews, &ownedTables); err != nil {
				t.Errorf("expected valid statement to succeed: %q\nerr=%v", goodStmt, err)
			}
		}
	})

	t.Run("coupled_err_unicode_safe", func(t *testing.T) {
		// Turkish uppercase İ expands byte length under strings.ToLower
		unicodeErr := errors.New("İİİİ Unknown properties: {storage_vault_name=v1}")
		if !doris.IsCoupledModeUnknownPropertiesErr(unicodeErr) {
			t.Errorf("expected Unicode-prefixed Unknown properties error to match safely")
		}
		badUnicodeErr := errors.New("KK Unknown properties: {other_prop=1}")
		if doris.IsCoupledModeUnknownPropertiesErr(badUnicodeErr) {
			t.Errorf("expected non-coupled property with Unicode prefix to return false")
		}
	})

	t.Run("strip_coupled_comments_preserved", func(t *testing.T) {
		commentBeforeProps := "CREATE TABLE internal.tenant_a.t (\n    id INT\n) -- trailing comment\nPROPERTIES (\n    \"storage_vault_name\" = \"v1\"\n);"
		got := doris.StripCoupledModeCreateTableProperties(commentBeforeProps)
		if !strings.Contains(got, "-- trailing comment\n;") {
			t.Errorf("expected newline after line comment before semicolon to be preserved:\n%s", got)
		}

		commentBeforeComma := "CREATE TABLE internal.tenant_a.t (\n    id INT\n)\nPROPERTIES (\n    \"replication_num\" = \"1\" -- keep comment\n    , \"storage_vault_name\" = \"v1\",\n    \"enable_unique_key_merge_on_write\" = \"true\"\n);"
		gotComma := doris.StripCoupledModeCreateTableProperties(commentBeforeComma)
		if !strings.Contains(gotComma, "-- keep comment\n    , ") {
			t.Errorf("expected comma after line comment not to be swallowed into comment:\n%s", gotComma)
		}
	})

	t.Run("iceberg_struct_map_and_if_not_exists", func(t *testing.T) {
		ddl := "CREATE TABLE azra_lake.tenant_a.t (id BIGINT, payload STRUCT<smallint:INT, cnt:SMALLINT(6), m:MAP<STRING, TINYINT>>, tags ARRAY<TINYINT(1)>);"
		got := doris.NormalizeIcebergCatalogDDL(ddl, "azra_lake")
		wantSubstrs := []string{
			"smallint:INT",
			"cnt:INT",
			"m:MAP<STRING, INT>",
			"ARRAY<INT>",
		}
		for _, w := range wantSubstrs {
			if !strings.Contains(got, w) {
				t.Errorf("expected %q in normalized Iceberg DDL:\n%s", w, got)
			}
		}

		alter := "ALTER TABLE azra_lake.tenant_a.t ADD COLUMN IF NOT EXISTS flag TINYINT(1), MODIFY COLUMN cnt SMALLINT;"
		gotAlter := doris.NormalizeIcebergCatalogDDL(alter, "")
		if !strings.Contains(gotAlter, "flag INT") || !strings.Contains(gotAlter, "cnt INT") {
			t.Errorf("expected ADD COLUMN IF NOT EXISTS and MODIFY COLUMN to widen:\n%s", gotAlter)
		}
	})

	t.Run("nereids_comparison_operators_and_depth_cap", func(t *testing.T) {
		cmpView := "CREATE VIEW internal.db.v AS SELECT MAX_BY(v, CASE WHEN a < b AND c > d THEN STRUCT(ts, id) ELSE NULL END) FROM internal.db.t;"
		gotCmp := doris.NormalizeNereidsViewOrderKeys(cmpView)
		if !strings.Contains(gotCmp, "a < b AND c > d") || !strings.Contains(gotCmp, "CONCAT_WS(CHAR(1)") {
			t.Errorf("expected comparison operators inside MAX_BY not to break splitting:\n%s", gotCmp)
		}

		// Deep nesting > depth cap (4) stays bounded cleanly
		deep := "STRUCT(a, STRUCT(b, STRUCT(c, STRUCT(d, STRUCT(e, f)))))"
		deepView := "CREATE VIEW internal.db.v AS SELECT MAX_BY(v, " + deep + ") FROM internal.db.t;"
		gotDeep := doris.NormalizeNereidsViewOrderKeys(deepView)
		if !strings.Contains(gotDeep, "CONCAT_WS(CHAR(1)") {
			t.Errorf("expected outer levels of deep nested STRUCT to rewrite cleanly:\n%s", gotDeep)
		}
		// Unbalanced or malformed inputs remain safe no-ops
		for _, mal := range []string{
			"CREATE TABLE internal.t.p (id INT) /* open",
			"CREATE TABLE internal.t.p (id INT) PROPERTIES (\"storage_vault_name\" = 'unclosed);",
			"CREATE VIEW internal.db.v AS SELECT MAX_BY(v, STRUCT(a, b) FROM internal.db.t;",
		} {
			_ = doris.StripCoupledModeCreateTableProperties(mal)
			_ = doris.NormalizeNereidsViewOrderKeys(mal)
		}
	})
}
