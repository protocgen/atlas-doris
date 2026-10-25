package doris

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// LintRule identifies a compile-time or migration-time Apache Doris 4.x /
// Apache Iceberg v2 schema & evolution safety rule.
type LintRule string

const (
	// DORIS001_IMMUTABLE_KEY blocks mutating UNIQUE KEY, DUPLICATE KEY, or AGGREGATE KEY columns on an existing table.
	DORIS001_IMMUTABLE_KEY LintRule = "DORIS001_IMMUTABLE_KEY"
	// DORIS002_IMMUTABLE_SEQUENCE blocks mutating "function_column.sequence_col" on an existing table.
	DORIS002_IMMUTABLE_SEQUENCE LintRule = "DORIS002_IMMUTABLE_SEQUENCE"
	// DORIS003_IMMUTABLE_DISTRIBUTION blocks mutating DISTRIBUTED BY HASH columns on an existing table.
	DORIS003_IMMUTABLE_DISTRIBUTION LintRule = "DORIS003_IMMUTABLE_DISTRIBUTION"
	// DORIS004_NO_COLUMN_DROP blocks dropping existing columns (or tables) unless AllowDestructive is set.
	DORIS004_NO_COLUMN_DROP LintRule = "DORIS004_NO_COLUMN_DROP"
	// DORIS005_NON_NULL_SCALAR_DEFAULT requires an explicit DEFAULT on newly added NOT NULL scalar columns on existing tables.
	DORIS005_NON_NULL_SCALAR_DEFAULT LintRule = "DORIS005_NON_NULL_SCALAR_DEFAULT"
	// DORIS006_SAFE_TYPE_EVOLUTION validates column type changes against Doris 4.x LSC & Iceberg v2 widening rules.
	DORIS006_SAFE_TYPE_EVOLUTION LintRule = "DORIS006_SAFE_TYPE_EVOLUTION"
	// DORIS007_FORBIDDEN_TYPE rejects JSONB and VARIANT column types across all tables (ADR-0008).
	DORIS007_FORBIDDEN_TYPE LintRule = "DORIS007_FORBIDDEN_TYPE"
)

// LintOptions configures schema and evolution lint behavior.
type LintOptions struct {
	AllowDestructive bool
}

// Violation represents a single lint rule violation.
type Violation struct {
	Rule       LintRule
	TableName  string
	ColumnName string
	Message    string
}

// String formats the violation as a deterministic human-readable diagnostic.
func (v Violation) String() string {
	if v.ColumnName != "" {
		return fmt.Sprintf("[%s] table %s column %s: %s", v.Rule, v.TableName, v.ColumnName, v.Message)
	}
	return fmt.Sprintf("[%s] table %s: %s", v.Rule, v.TableName, v.Message)
}

// Error implements the error interface.
func (v Violation) Error() string {
	return v.String()
}

// ViolationsToError converts a slice of Violation into a single joined error, or nil if empty.
func ViolationsToError(violations []Violation) error {
	if len(violations) == 0 {
		return nil
	}
	errs := make([]error, len(violations))
	for i, v := range violations {
		errs[i] = v
	}
	return errors.Join(errs...)
}

// LintSchema runs static lint checks (such as DORIS007_FORBIDDEN_TYPE) across all tables in s.
func LintSchema(s *Schema) []Violation {
	if s == nil {
		return nil
	}
	var violations []Violation
	for _, t := range s.Tables {
		violations = append(violations, LintTable(t)...)
	}
	return violations
}

// LintTable runs static lint checks on a single table definition.
func LintTable(t *TableDef) []Violation {
	if t == nil {
		return nil
	}
	var violations []Violation
	tblName := t.CanonicalName()
	colByName := make(map[string]*ColumnDef, len(t.Columns))
	for _, c := range t.Columns {
		colByName[c.Name] = c
		if IsForbiddenColumnType(c.Type) {
			violations = append(violations, Violation{
				Rule:       DORIS007_FORBIDDEN_TYPE,
				TableName:  tblName,
				ColumnName: c.Name,
				Message:    fmt.Sprintf("column %q uses forbidden type %q (JSONB and VARIANT are prohibited by ADR-0008)", c.Name, c.Type),
			})
		}
	}
	violations = append(violations, lintTableKeyAndANN(t, colByName)...)
	return violations
}

func lintTableKeyAndANN(t *TableDef, colByName map[string]*ColumnDef) []Violation {
	if t == nil {
		return nil
	}
	var violations []Violation
	tblName := t.CanonicalName()

	if t.Key != nil {
		for _, kc := range t.Key.Columns {
			col, ok := colByName[kc]
			if !ok {
				continue
			}
			upperType := NormalizeType(col.Type)
			if upperType == "FLOAT" || upperType == "DOUBLE" {
				violations = append(violations, Violation{
					Rule:       DORIS001_IMMUTABLE_KEY,
					TableName:  tblName,
					ColumnName: kc,
					Message:    fmt.Sprintf("key column %q uses floating-point type %s (prohibited by Doris)", kc, col.Type),
				})
			}
		}
	}

	for _, idx := range t.Indexes {
		if !strings.EqualFold(strings.TrimSpace(idx.Using), "ANN") {
			continue
		}
		if t.Key != nil && t.Key.Kind != KeyDuplicate {
			violations = append(violations, Violation{
				Rule:      DORIS006_SAFE_TYPE_EVOLUTION,
				TableName: tblName,
				Message:   fmt.Sprintf("ANN index %q is only supported on DUPLICATE KEY tables in Doris 4.x (found %s KEY)", idx.Name, t.Key.Kind),
			})
		}
		if len(idx.Columns) != 1 {
			violations = append(violations, Violation{
				Rule:      DORIS006_SAFE_TYPE_EVOLUTION,
				TableName: tblName,
				Message:   fmt.Sprintf("ANN index %q must target exactly 1 column", idx.Name),
			})
		} else if col, ok := colByName[idx.Columns[0]]; !ok {
			violations = append(violations, Violation{
				Rule:       DORIS006_SAFE_TYPE_EVOLUTION,
				TableName:  tblName,
				ColumnName: idx.Columns[0],
				Message:    fmt.Sprintf("ANN index %q references unknown column %q", idx.Name, idx.Columns[0]),
			})
		} else if NormalizeType(col.Type) != "ARRAY<FLOAT>" || col.Nullable {
			violations = append(violations, Violation{
				Rule:       DORIS006_SAFE_TYPE_EVOLUTION,
				TableName:  tblName,
				ColumnName: col.Name,
				Message:    fmt.Sprintf("ANN index %q column %q must be ARRAY<FLOAT> NOT NULL (got %s, nullable=%v)", idx.Name, col.Name, col.Type, col.Nullable),
			})
		}
		idxProps := propertiesToMap(idx.Properties)
		if !strings.EqualFold(strings.TrimSpace(idxProps["index_type"]), "hnsw") {
			violations = append(violations, Violation{
				Rule:      DORIS006_SAFE_TYPE_EVOLUTION,
				TableName: tblName,
				Message:   fmt.Sprintf("ANN index %q must set \"index_type\" = \"hnsw\"", idx.Name),
			})
		}
		if dim, err := strconv.Atoi(strings.TrimSpace(idxProps["dim"])); err != nil || dim <= 0 {
			violations = append(violations, Violation{
				Rule:      DORIS006_SAFE_TYPE_EVOLUTION,
				TableName: tblName,
				Message:   fmt.Sprintf("ANN index %q must specify positive integer \"dim\"", idx.Name),
			})
		}
	}
	return violations
}

// LintTableEvolution validates evolving an existing table definition from before to after.
func LintTableEvolution(before, after *TableDef, opts LintOptions) []Violation {
	if before == nil && after == nil {
		return nil
	}
	if before == nil {
		return LintTable(after)
	}
	if after == nil {
		if !opts.AllowDestructive {
			return []Violation{{
				Rule:      DORIS004_NO_COLUMN_DROP,
				TableName: before.CanonicalName(),
				Message:   fmt.Sprintf("dropping existing table %q is forbidden without --allow-destructive", before.CanonicalName()),
			}}
		}
		return nil
	}

	tblName := after.CanonicalName()
	if tblName == "" {
		tblName = before.CanonicalName()
	}

	var violations []Violation

	// DORIS001_IMMUTABLE_KEY: Block mutating UNIQUE KEY, DUPLICATE KEY, or AGGREGATE KEY columns.
	if !keysEqual(before.Key, after.Key) {
		violations = append(violations, Violation{
			Rule:      DORIS001_IMMUTABLE_KEY,
			TableName: tblName,
			Message:   fmt.Sprintf("table key clause cannot be mutated on existing table (before=%s, after=%s)", formatKeyDebug(before.Key), formatKeyDebug(after.Key)),
		})
	}

	// DORIS002_IMMUTABLE_SEQUENCE: Block mutating "function_column.sequence_col".
	beforeSeq := strings.TrimSpace(before.SequenceCol())
	afterSeq := strings.TrimSpace(after.SequenceCol())
	if beforeSeq != afterSeq {
		seqCol := afterSeq
		if seqCol == "" {
			seqCol = beforeSeq
		}
		violations = append(violations, Violation{
			Rule:       DORIS002_IMMUTABLE_SEQUENCE,
			TableName:  tblName,
			ColumnName: seqCol,
			Message:    fmt.Sprintf("function_column.sequence_col cannot be mutated on existing table (before=%q, after=%q)", beforeSeq, afterSeq),
		})
	}

	// DORIS003_IMMUTABLE_DISTRIBUTION: Block mutating DISTRIBUTED BY HASH columns or colocate_with group.
	if !distributionHashEqual(before.Distribution, after.Distribution) {
		distCol := ""
		if after.Distribution != nil && len(after.Distribution.Columns) > 0 {
			distCol = after.Distribution.Columns[0]
		} else if before.Distribution != nil && len(before.Distribution.Columns) > 0 {
			distCol = before.Distribution.Columns[0]
		}
		violations = append(violations, Violation{
			Rule:       DORIS003_IMMUTABLE_DISTRIBUTION,
			TableName:  tblName,
			ColumnName: distCol,
			Message:    fmt.Sprintf("DISTRIBUTED BY HASH columns cannot be mutated on existing table (before=%v, after=%v)", distCols(before.Distribution), distCols(after.Distribution)),
		})
	}
	beforeColocate := strings.TrimSpace(before.ColocateGroup())
	afterColocate := strings.TrimSpace(after.ColocateGroup())
	if beforeColocate != afterColocate {
		violations = append(violations, Violation{
			Rule:      DORIS003_IMMUTABLE_DISTRIBUTION,
			TableName: tblName,
			Message:   fmt.Sprintf("colocate_with group cannot be mutated on existing table (before=%q, after=%q)", beforeColocate, afterColocate),
		})
	}

	beforeColByName := make(map[string]*ColumnDef, len(before.Columns))
	for _, c := range before.Columns {
		beforeColByName[c.Name] = c
	}
	afterColByName := make(map[string]*ColumnDef, len(after.Columns))
	for _, c := range after.Columns {
		afterColByName[c.Name] = c
	}

	// DORIS004_NO_COLUMN_DROP: Block removing existing columns unless AllowDestructive is true.
	if !opts.AllowDestructive {
		for _, bc := range before.Columns {
			if _, ok := afterColByName[bc.Name]; !ok {
				violations = append(violations, Violation{
					Rule:       DORIS004_NO_COLUMN_DROP,
					TableName:  tblName,
					ColumnName: bc.Name,
					Message:    fmt.Sprintf("dropping existing column %q is forbidden without --allow-destructive", bc.Name),
				})
			}
		}
	}

	// DORIS005, DORIS006, and DORIS007 on after columns.
	for _, ac := range after.Columns {
		if IsForbiddenColumnType(ac.Type) {
			violations = append(violations, Violation{
				Rule:       DORIS007_FORBIDDEN_TYPE,
				TableName:  tblName,
				ColumnName: ac.Name,
				Message:    fmt.Sprintf("column %q uses forbidden type %q (JSONB and VARIANT are prohibited by ADR-0008)", ac.Name, ac.Type),
			})
		}

		bc, existed := beforeColByName[ac.Name]
		if !existed {
			// Newly added column: check DORIS005_NON_NULL_SCALAR_DEFAULT.
			if !ac.Nullable && !IsNonScalarColumnType(ac.Type) && (!ac.HasDefault || strings.TrimSpace(ac.Default) == "") {
				violations = append(violations, Violation{
					Rule:       DORIS005_NON_NULL_SCALAR_DEFAULT,
					TableName:  tblName,
					ColumnName: ac.Name,
					Message:    fmt.Sprintf("newly added NOT NULL scalar column %q (%s) requires an explicit DEFAULT value", ac.Name, ac.Type),
				})
			}
			continue
		}

		// Existing column: check DORIS006_SAFE_TYPE_EVOLUTION.
		if !IsSafeTypeEvolution(bc.Type, ac.Type) {
			violations = append(violations, Violation{
				Rule:       DORIS006_SAFE_TYPE_EVOLUTION,
				TableName:  tblName,
				ColumnName: ac.Name,
				Message:    fmt.Sprintf("unsafe type evolution on column %q from %q to %q", ac.Name, bc.Type, ac.Type),
			})
		}
	}

	violations = append(violations, lintTableKeyAndANN(after, afterColByName)...)

	return violations
}

// LintSchemaEvolution validates evolving current Schema into desired Schema.
func LintSchemaEvolution(current, desired *Schema, opts LintOptions) []Violation {
	if current == nil {
		return LintSchema(desired)
	}
	if desired == nil {
		return nil
	}

	var violations []Violation
	currentByName := make(map[string]*TableDef, len(current.Tables))
	for _, t := range current.Tables {
		currentByName[t.CanonicalName()] = t
	}
	desiredByName := make(map[string]*TableDef, len(desired.Tables))
	for _, t := range desired.Tables {
		desiredByName[t.CanonicalName()] = t
	}

	// Check dropped tables if destructive changes are disallowed.
	if !opts.AllowDestructive {
		for _, ct := range current.Tables {
			if _, ok := desiredByName[ct.CanonicalName()]; !ok {
				violations = append(violations, Violation{
					Rule:      DORIS004_NO_COLUMN_DROP,
					TableName: ct.CanonicalName(),
					Message:   fmt.Sprintf("dropping existing table %q is forbidden without --allow-destructive", ct.CanonicalName()),
				})
			}
		}
	}

	for _, dt := range desired.Tables {
		if ct, existed := currentByName[dt.CanonicalName()]; existed {
			violations = append(violations, LintTableEvolution(ct, dt, opts)...)
		} else {
			violations = append(violations, LintTable(dt)...)
		}
	}
	return violations
}

// IsSafeTypeEvolution reports whether evolving a column from beforeType to
// afterType is either a no-op or a safe widening type change supported by both
// Apache Doris 4.x Light Schema Change (LSC) and Apache Iceberg v2:
//   - VARCHAR(m) -> VARCHAR(n) where n >= m
//   - TINYINT -> SMALLINT, INT, BIGINT, or LARGEINT
//   - SMALLINT -> INT, BIGINT, or LARGEINT
//   - INT -> BIGINT or LARGEINT
//   - BIGINT -> LARGEINT
//   - FLOAT -> DOUBLE
//   - DECIMAL(p1, s) -> DECIMAL(p2, s) and DECIMALV3(p1, s) -> DECIMALV3(p2, s) where p2 >= p1
func IsSafeTypeEvolution(beforeType, afterType string) bool {
	bt := NormalizeType(beforeType)
	at := NormalizeType(afterType)
	if bt == "" || at == "" {
		return false
	}
	if bt == at {
		return true
	}

	if m, okB := parseVarcharLength(bt); okB {
		if n, okA := parseVarcharLength(at); okA {
			return n >= m
		}
		return false
	}

	if p1, s1, okB := parseDecimalPrecisionScale(bt); okB {
		if p2, s2, okA := parseDecimalPrecisionScale(at); okA {
			return p2 >= p1 && s1 == s2
		}
		return false
	}

	switch bt {
	case "TINYINT":
		return at == "SMALLINT" || at == "INT" || at == "BIGINT" || at == "LARGEINT"
	case "SMALLINT":
		return at == "INT" || at == "BIGINT" || at == "LARGEINT"
	case "INT":
		return at == "BIGINT" || at == "LARGEINT"
	case "BIGINT":
		return at == "LARGEINT"
	case "FLOAT":
		return at == "DOUBLE"
	default:
		return false
	}
}

// IsForbiddenColumnType reports whether rawType is or contains JSONB or VARIANT.
func IsForbiddenColumnType(rawType string) bool {
	dt := NormalizeType(rawType)
	if dt == "JSONB" || dt == "VARIANT" {
		return true
	}
	if strings.HasPrefix(dt, "ARRAY<") && strings.HasSuffix(dt, ">") {
		inner := strings.TrimSuffix(strings.TrimPrefix(dt, "ARRAY<"), ">")
		return IsForbiddenColumnType(inner)
	}
	return false
}

// IsNonScalarColumnType reports whether rawType is a complex or semi-structured
// Doris column type (ARRAY, MAP, STRUCT, JSON, JSONB, VARIANT).
func IsNonScalarColumnType(rawType string) bool {
	dt := NormalizeType(rawType)
	switch dt {
	case "JSON", "JSONB", "VARIANT":
		return true
	}
	return strings.HasPrefix(dt, "ARRAY<") ||
		strings.HasPrefix(dt, "MAP<") ||
		strings.HasPrefix(dt, "STRUCT<")
}

func parseVarcharLength(normType string) (int, bool) {
	if !strings.HasPrefix(normType, "VARCHAR(") || !strings.HasSuffix(normType, ")") {
		return 0, false
	}
	inner := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(normType, "VARCHAR("), ")"))
	if inner == "" {
		return 0, false
	}
	n, err := strconv.Atoi(inner)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

func parseDecimalPrecisionScale(normType string) (precision int, scale int, ok bool) {
	var inner string
	switch {
	case strings.HasPrefix(normType, "DECIMALV3(") && strings.HasSuffix(normType, ")"):
		inner = strings.TrimSuffix(strings.TrimPrefix(normType, "DECIMALV3("), ")")
	case strings.HasPrefix(normType, "DECIMAL(") && strings.HasSuffix(normType, ")"):
		inner = strings.TrimSuffix(strings.TrimPrefix(normType, "DECIMAL("), ")")
	default:
		return 0, 0, false
	}
	parts := strings.Split(inner, ",")
	if len(parts) != 1 && len(parts) != 2 {
		return 0, 0, false
	}
	p, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || p <= 0 {
		return 0, 0, false
	}
	s := 0
	if len(parts) == 2 {
		s, err = strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil || s < 0 || s > p {
			return 0, 0, false
		}
	}
	return p, s, true
}

func keysEqual(a, b *KeyClause) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Kind == b.Kind && stringSlicesEqual(a.Columns, b.Columns)
}

func formatKeyDebug(k *KeyClause) string {
	if k == nil {
		return "<none>"
	}
	return fmt.Sprintf("%s(%s)", k.Kind, strings.Join(k.Columns, ","))
}

func distributionHashEqual(a, b *DistributionClause) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return strings.EqualFold(a.Kind, b.Kind) && stringSlicesEqual(a.Columns, b.Columns)
}

func distCols(d *DistributionClause) []string {
	if d == nil {
		return nil
	}
	return d.Columns
}
