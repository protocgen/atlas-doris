package doris_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/protocgen/atlas-doris/pkg/doris"
	"hegel.dev/go/hegel"
)

func drawIdent(ht *hegel.T, prefix string) string {
	raw := hegel.Draw(ht, hegel.Text().MinSize(1).MaxSize(12))
	var b strings.Builder
	for i, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			b.WriteRune(r + ('a' - 'A')*((r-'A')/26)) // keep ascii
		} else if i > 0 && r >= '0' && r <= '9' {
			b.WriteRune(r)
		} else if i > 0 && r == '_' {
			b.WriteRune('_')
		}
	}
	cleaned := strings.ToLower(b.String())
	// Filter to strict ascii a-z0-9_
	var strict strings.Builder
	for i := 0; i < len(cleaned); i++ {
		ch := cleaned[i]
		if (ch >= 'a' && ch <= 'z') || (i > 0 && ((ch >= '0' && ch <= '9') || ch == '_')) {
			strict.WriteByte(ch)
		}
	}
	if strict.Len() == 0 {
		return prefix
	}
	return prefix + "_" + strict.String()
}

func drawGeneratedTable(ht *hegel.T, tblIdx int) *doris.TableDef {
	colPool := []string{"patient_id", "record_id", "system", "order", "mrn", "status", "count_val", "amount", "tags", "created_at", "publication_generation"}
	typePool := []string{
		"VARCHAR(32)", "VARCHAR(64)", "TINYINT", "SMALLINT", "INT", "BIGINT",
		"FLOAT", "DOUBLE", "BOOLEAN", "DATEV2", "DATETIMEV2(6)", "DECIMALV3(10, 2)",
		"ARRAY<VARCHAR(32)>",
	}

	numExtraCols := hegel.Draw(ht, hegel.Integers(2, 5))
	cols := []*doris.ColumnDef{
		{
			Name:     "patient_id",
			Type:     "VARCHAR(64)",
			Nullable: false,
		},
	}
	usedNames := map[string]bool{"patient_id": true}
	for i := 0; i < numExtraCols; i++ {
		cand := colPool[(i+hegel.Draw(ht, hegel.Integers(1, len(colPool)-1)))%len(colPool)]
		if usedNames[cand] {
			cand = fmt.Sprintf("col_%d", i)
		}
		usedNames[cand] = true
		colType := typePool[hegel.Draw(ht, hegel.Integers(0, len(typePool)-1))]
		nullable := hegel.Draw(ht, hegel.Booleans())
		hasDef := false
		defVal := ""
		if !nullable {
			hasDef = true
			if strings.HasPrefix(colType, "ARRAY<") {
				defVal = "[]"
			} else if colType == "BOOLEAN" {
				defVal = "false"
			} else {
				defVal = "0"
			}
		}
		cols = append(cols, &doris.ColumnDef{
			Name:       cand,
			Type:       colType,
			Nullable:   nullable,
			HasDefault: hasDef,
			Default:    defVal,
		})
	}

	var indexes []*doris.IndexDef
	if hegel.Draw(ht, hegel.Booleans()) && len(cols) > 1 {
		targetCol := cols[1].Name
		indexes = append(indexes, &doris.IndexDef{
			Name:    "idx_" + targetCol,
			Columns: []string{targetCol},
			Using:   "INVERTED",
		})
	}

	keyKind := doris.KeyUnique
	if hegel.Draw(ht, hegel.Booleans()) {
		keyKind = doris.KeyDuplicate
	}

	buckets := strconv.Itoa(hegel.Draw(ht, hegel.Integers(1, 16)))
	if hegel.Draw(ht, hegel.Booleans()) {
		buckets = "${TENANT_BUCKETS}"
	}

	props := []doris.Property{
		{Key: "storage_vault_name", Value: "azra_lakehouse_vault"},
		{Key: "compression", Value: "zstd"},
	}
	if keyKind == doris.KeyUnique {
		props = append(props, doris.Property{Key: "enable_unique_key_merge_on_write", Value: "true"})
		props = append(props, doris.Property{Key: "function_column.sequence_col", Value: "publication_generation"})
	}
	props = append(props, doris.Property{Key: "group_commit_interval_ms", Value: "200"})

	tblName := fmt.Sprintf("tbl_%d_%s", tblIdx, drawIdent(ht, "t"))
	return &doris.TableDef{
		FullName:    "internal.tenant_${TENANT_SLUG}." + tblName,
		Catalog:     "internal",
		Database:    "tenant_${TENANT_SLUG}",
		Name:        tblName,
		IfNotExists: true,
		Columns:     cols,
		Indexes:     indexes,
		Key: &doris.KeyClause{
			Kind:    keyKind,
			Columns: []string{"patient_id"},
		},
		Distribution: &doris.DistributionClause{
			Kind:    "HASH",
			Columns: []string{"patient_id"},
			Buckets: buckets,
		},
		Properties: props,
	}
}

func drawGeneratedSchema(ht *hegel.T) *doris.Schema {
	numTables := hegel.Draw(ht, hegel.Integers(1, 3))
	s := doris.NewSchema()
	s.Databases = []*doris.DatabaseDef{
		{
			FullName:    "internal.tenant_${TENANT_SLUG}",
			Catalog:     "internal",
			Name:        "tenant_${TENANT_SLUG}",
			IfNotExists: true,
		},
	}
	for i := 0; i < numTables; i++ {
		s.Tables = append(s.Tables, drawGeneratedTable(ht, i))
	}
	return s
}

// Property 1: Parser never panics on arbitrary fuzz inputs.
func TestProperty_ParserNeverPanics(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		raw := hegel.Draw(ht, hegel.Text().MaxSize(256))
		_, _ = doris.StripCommentsAndSplitStatements(raw)
		_, _ = doris.ParseStatements(raw)
		_, _ = doris.ParseSchema(raw)
	})
}

// Property 2: Formatting and parsing any valid schema is a lossless, idempotent round-trip.
func TestProperty_FormatParseRoundTrip(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		s1 := drawGeneratedSchema(ht)
		sql1 := doris.FormatSchema(s1)
		parsed, err := doris.ParseSchema(sql1)
		if err != nil {
			ht.Fatalf("ParseSchema failed on generated SQL:\n%s\nerr: %v", sql1, err)
		}
		sql2 := doris.FormatSchema(parsed)
		if sql1 != sql2 {
			ht.Fatalf("FormatSchema round-trip mismatch:\n--- sql1 ---\n%s\n--- sql2 ---\n%s", sql1, sql2)
		}
	})
}

// Property 3: Diff(S, S) is always empty (idempotency).
func TestProperty_DiffIdempotency(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		s := drawGeneratedSchema(ht)
		stmts, err := doris.Diff(s, s.Clone())
		if err != nil {
			ht.Fatalf("Diff(S, S) returned error: %v", err)
		}
		if len(stmts) != 0 {
			ht.Fatalf("expected 0 statements for Diff(S, S), got %d:\n%s", len(stmts), doris.FormatStatements(stmts))
		}
	})
}

// Property 4: Replay(Create(S1) + Diff(S1, S2)) == S2 for all valid schema evolutions.
func TestProperty_ReplayEquivalence(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		s1 := drawGeneratedSchema(ht)
		s2 := s1.Clone()

		// Apply random valid evolutions to s2.Tables[0]
		t0 := s2.Tables[0]

		// 1. Widen an existing column if eligible
		for _, c := range t0.Columns {
			if c.Name == "patient_id" {
				continue
			}
			switch c.Type {
			case "VARCHAR(32)":
				if hegel.Draw(ht, hegel.Booleans()) {
					c.Type = "VARCHAR(64)"
				}
			case "TINYINT":
				if hegel.Draw(ht, hegel.Booleans()) {
					c.Type = "SMALLINT"
				}
			case "SMALLINT":
				if hegel.Draw(ht, hegel.Booleans()) {
					c.Type = "INT"
				}
			case "INT":
				if hegel.Draw(ht, hegel.Booleans()) {
					c.Type = "BIGINT"
				}
			case "FLOAT":
				if hegel.Draw(ht, hegel.Booleans()) {
					c.Type = "DOUBLE"
				}
			}
		}

		// 2. Optionally add a new column with default
		if hegel.Draw(ht, hegel.Booleans()) {
			newColName := "added_col_" + drawIdent(ht, "c")
			t0.Columns = append(t0.Columns, &doris.ColumnDef{
				Name:       newColName,
				Type:       "VARCHAR(128)",
				Nullable:   false,
				HasDefault: true,
				Default:    "INIT",
			})
			if hegel.Draw(ht, hegel.Booleans()) {
				t0.Indexes = append(t0.Indexes, &doris.IndexDef{
					Name:    "idx_" + newColName,
					Columns: []string{newColName},
					Using:   "INVERTED",
				})
			}
		}

		// 3. Optionally mutate mutable property group_commit_interval_ms
		if hegel.Draw(ht, hegel.Booleans()) {
			t0.SetProperty("group_commit_interval_ms", "500")
		}

		// 4. Optionally add a new table
		if hegel.Draw(ht, hegel.Booleans()) {
			s2.Tables = append(s2.Tables, drawGeneratedTable(ht, 99))
		}

		diffStmts, err := doris.Diff(s1, s2)
		if err != nil {
			ht.Fatalf("Diff(s1, s2) failed on valid evolution: %v", err)
		}

		createStmts, err := doris.ParseStatements(doris.FormatSchema(s1))
		if err != nil {
			ht.Fatalf("ParseStatements(FormatSchema(s1)) failed: %v", err)
		}

		allStmts := append(createStmts, diffStmts...)
		// Verify that formatting and re-parsing the migration statements also works cleanly
		migrationSQL := doris.FormatStatements(allStmts)
		parsedAllStmts, err := doris.ParseStatements(migrationSQL)
		if err != nil {
			ht.Fatalf("ParseStatements(migrationSQL) failed: %v\nSQL:\n%s", err, migrationSQL)
		}

		replayed, err := doris.ReplayStatements(parsedAllStmts)
		if err != nil {
			ht.Fatalf("ReplayStatements failed: %v", err)
		}

		wantSQL := doris.FormatSchema(s2)
		gotSQL := doris.FormatSchema(replayed)
		if wantSQL != gotSQL {
			ht.Fatalf("Replay(Create(S1) + Diff(S1, S2)) != S2:\n--- want ---\n%s\n--- got ---\n%s", wantSQL, gotSQL)
		}
	})
}

// Property 5: Linter always catches forbidden mutations and forbidden column types.
func TestProperty_LinterInvariants(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		s1 := drawGeneratedSchema(ht)
		mutationKind := hegel.Draw(ht, hegel.Integers(1, 7))
		s2 := s1.Clone()
		t0 := s2.Tables[0]

		var expectedRule doris.LintRule
		switch mutationKind {
		case 1: // Mutate Key
			t0.Key.Columns = append(t0.Key.Columns, "extra_key_col")
			expectedRule = doris.DORIS001_IMMUTABLE_KEY
		case 2: // Mutate sequence_col
			t0.SetProperty("function_column.sequence_col", "mutated_seq_col")
			expectedRule = doris.DORIS002_IMMUTABLE_SEQUENCE
		case 3: // Mutate distribution hash column
			t0.Distribution.Columns = []string{"mutated_dist_col"}
			expectedRule = doris.DORIS003_IMMUTABLE_DISTRIBUTION
		case 4: // Drop column without AllowDestructive
			t0.Columns = t0.Columns[:len(t0.Columns)-1]
			expectedRule = doris.DORIS004_NO_COLUMN_DROP
		case 5: // Add NOT NULL scalar column without Default
			t0.Columns = append(t0.Columns, &doris.ColumnDef{
				Name:       "bad_non_null_scalar",
				Type:       "VARCHAR(64)",
				Nullable:   false,
				HasDefault: false,
			})
			expectedRule = doris.DORIS005_NON_NULL_SCALAR_DEFAULT
		case 6: // Unsafe type narrowing
			t0.Columns[0].Type = "VARCHAR(8)" // narrowed from VARCHAR(64)
			expectedRule = doris.DORIS006_SAFE_TYPE_EVOLUTION
		case 7: // Forbidden JSONB / VARIANT type
			forbidden := "JSONB"
			if hegel.Draw(ht, hegel.Booleans()) {
				forbidden = "VARIANT"
			}
			t0.Columns = append(t0.Columns, &doris.ColumnDef{
				Name:     "forbidden_col",
				Type:     forbidden,
				Nullable: true,
			})
			expectedRule = doris.DORIS007_FORBIDDEN_TYPE
		}

		violations := doris.LintSchemaEvolution(s1, s2, doris.LintOptions{AllowDestructive: false})
		found := false
		for _, v := range violations {
			if v.Rule == expectedRule {
				found = true
				break
			}
		}
		if !found {
			ht.Fatalf("mutation %d expected rule %s, but got violations: %v", mutationKind, expectedRule, violations)
		}
	})
}
