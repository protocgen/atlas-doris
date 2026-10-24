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
	for _, c := range t.Columns {
		if IsForbiddenColumnType(c.Type) {
			violations = append(violations, Violation{
				Rule:       DORIS007_FORBIDDEN_TYPE,
				TableName:  tblName,
				ColumnName: c.Name,
				Message:    fmt.Sprintf("column %q uses forbidden type %q (JSONB and VARIANT are prohibited by ADR-0008)", c.Name, c.Type),
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

	// DORIS003_IMMUTABLE_DISTRIBUTION: Block mutating DISTRIBUTED BY HASH columns.
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
//   - VARCHAR(m) -> VARCHAR(n) where n > m
//   - TINYINT -> SMALLINT, INT, or BIGINT
//   - SMALLINT -> INT or BIGINT
//   - INT -> BIGINT
//   - FLOAT -> DOUBLE
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
			return n > m
		}
		return false
	}

	switch bt {
	case "TINYINT":
		return at == "SMALLINT" || at == "INT" || at == "BIGINT"
	case "SMALLINT":
		return at == "INT" || at == "BIGINT"
	case "INT":
		return at == "BIGINT"
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
