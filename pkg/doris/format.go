package doris

import (
	"fmt"
	"strings"
)

// IsReservedKeyword reports whether colName is a reserved SQL keyword in
// Apache Doris 4.x that must be backtick-quoted in DDL statements.
func IsReservedKeyword(colName string) bool {
	switch strings.ToLower(strings.TrimSpace(colName)) {
	case "system", "key", "keys", "order", "range", "values", "index", "rank",
		"group", "table", "tables", "database", "databases", "schema", "schemas",
		"partition", "partitions", "buckets", "desc", "describe", "asc", "default",
		"primary", "unique", "duplicate", "aggregate", "check", "references",
		"foreign", "constraint", "select", "from", "where", "join", "inner",
		"left", "right", "full", "outer", "cross", "on", "using", "in", "is",
		"null", "not", "and", "or", "xor", "as", "by", "limit", "offset",
		"having", "union", "intersect", "except", "all", "distinct", "case",
		"when", "then", "else", "end", "exists", "between", "like", "ilike",
		"regexp", "rlike", "div", "mod", "interval", "function", "procedure",
		"trigger", "view", "grant", "revoke", "show", "explain", "use", "set",
		"kill", "load", "export", "backup", "restore", "alter", "create", "drop",
		"truncate", "rename", "replace", "insert", "update", "delete", "merge",
		"into", "over", "window", "rows", "preceding", "following", "current",
		"true", "false", "properties", "inverted", "ngram_bf", "ann", "bitmap":
		return true
	default:
		return false
	}
}

// FormatIdent formats a single SQL column/object identifier, quoting it with
// backticks if it is a Doris reserved keyword.
func FormatIdent(ident string) string {
	clean := strings.Trim(strings.TrimSpace(ident), "`")
	if IsReservedKeyword(clean) {
		return "`" + clean + "`"
	}
	return clean
}

// FormatIdentList formats a slice of column identifiers as a comma-separated list.
func FormatIdentList(cols []string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = FormatIdent(c)
	}
	return strings.Join(out, ", ")
}

// NormalizeType normalizes a Doris or Iceberg SQL type string into canonical
// uppercase form with standardized comma spacing (e.g. "DECIMALV3(12, 4)", "ARRAY<VARCHAR(64)>").
func NormalizeType(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			continue
		}
		if ch >= 'a' && ch <= 'z' {
			ch = ch - 'a' + 'A'
		}
		if ch == ',' {
			b.WriteString(", ")
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

// FormatColumnDef formats a single ColumnDef for use inside CREATE TABLE or ALTER TABLE.
func FormatColumnDef(c *ColumnDef) string {
	if c == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString(FormatIdent(c.Name))
	b.WriteByte(' ')
	b.WriteString(NormalizeType(c.Type))
	if c.AggType != "" {
		b.WriteByte(' ')
		b.WriteString(strings.ToUpper(strings.TrimSpace(c.AggType)))
	}
	if !c.Nullable {
		b.WriteString(" NOT NULL")
	}
	if c.HasDefault {
		b.WriteString(fmt.Sprintf(" DEFAULT %q", c.Default))
	}
	if c.Comment != "" {
		b.WriteString(fmt.Sprintf(" COMMENT %q", c.Comment))
	}
	return b.String()
}

// FormatIndexDefInline formats an inline INDEX definition.
func FormatIndexDefInline(idx *IndexDef) string {
	if idx == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("INDEX ")
	b.WriteString(idx.Name)
	b.WriteString(" (")
	b.WriteString(FormatIdentList(idx.Columns))
	b.WriteString(") USING ")
	b.WriteString(strings.ToUpper(strings.TrimSpace(idx.Using)))
	if len(idx.Properties) > 0 {
		pairs := make([]string, len(idx.Properties))
		for i, p := range idx.Properties {
			pairs[i] = fmt.Sprintf("%q = %q", p.Key, p.Value)
		}
		b.WriteString(" PROPERTIES (")
		b.WriteString(strings.Join(pairs, ", "))
		b.WriteByte(')')
	}
	if idx.Comment != "" {
		b.WriteString(fmt.Sprintf(" COMMENT %q", idx.Comment))
	}
	return b.String()
}

// FormatCreateDatabase renders a canonical CREATE DATABASE / CREATE SCHEMA statement.
func FormatCreateDatabase(d *DatabaseDef) string {
	if d == nil {
		return ""
	}
	kw := "DATABASE"
	if d.IsSchemaKeyword {
		kw = "SCHEMA"
	}
	if d.IfNotExists {
		return fmt.Sprintf("CREATE %s IF NOT EXISTS %s;", kw, d.CanonicalName())
	}
	return fmt.Sprintf("CREATE %s %s;", kw, d.CanonicalName())
}

// FormatCreateTable renders a deterministic, canonical CREATE TABLE statement for t.
func FormatCreateTable(t *TableDef) string {
	if t == nil {
		return ""
	}
	var bodyLines []string
	for _, c := range t.Columns {
		bodyLines = append(bodyLines, "    "+FormatColumnDef(c))
	}
	for _, idx := range t.Indexes {
		bodyLines = append(bodyLines, "    "+FormatIndexDefInline(idx))
	}

	var b strings.Builder
	b.WriteString("CREATE TABLE ")
	if t.IfNotExists {
		b.WriteString("IF NOT EXISTS ")
	}
	b.WriteString(t.CanonicalName())
	b.WriteString(" (\n")
	b.WriteString(strings.Join(bodyLines, ",\n"))
	b.WriteString("\n)")

	if t.Key != nil && len(t.Key.Columns) > 0 {
		b.WriteByte('\n')
		b.WriteString(string(t.Key.Kind))
		b.WriteString(" KEY(")
		b.WriteString(FormatIdentList(t.Key.Columns))
		b.WriteByte(')')
	}

	if t.Comment != "" {
		b.WriteByte('\n')
		b.WriteString(fmt.Sprintf("COMMENT %q", t.Comment))
	}

	if t.Partition != nil {
		b.WriteByte('\n')
		switch strings.ToUpper(t.Partition.Kind) {
		case "LIST", "RANGE":
			b.WriteString("PARTITION BY ")
			b.WriteString(strings.ToUpper(t.Partition.Kind))
			b.WriteString(" (")
			b.WriteString(FormatIdentList(t.Partition.Columns))
			b.WriteByte(')')
			raw := strings.TrimSpace(t.Partition.PartitionsRaw)
			if raw == "" {
				raw = "()"
			}
			b.WriteByte(' ')
			b.WriteString(raw)
		case "EXPR":
			b.WriteString("PARTITION BY (")
			b.WriteString(strings.TrimSpace(t.Partition.RawExpr))
			b.WriteByte(')')
		}
	}

	if len(t.OrderBy) > 0 {
		b.WriteByte('\n')
		b.WriteString("ORDER BY (")
		b.WriteString(FormatIdentList(t.OrderBy))
		b.WriteByte(')')
	}

	if t.Distribution != nil {
		b.WriteByte('\n')
		if strings.EqualFold(t.Distribution.Kind, "RANDOM") {
			b.WriteString("DISTRIBUTED BY RANDOM BUCKETS ")
			b.WriteString(t.Distribution.Buckets)
		} else {
			b.WriteString("DISTRIBUTED BY HASH(")
			b.WriteString(FormatIdentList(t.Distribution.Columns))
			b.WriteString(") BUCKETS ")
			b.WriteString(t.Distribution.Buckets)
		}
	}

	if len(t.Properties) > 0 {
		b.WriteString("\nPROPERTIES (\n")
		propLines := make([]string, len(t.Properties))
		for i, p := range t.Properties {
			propLines[i] = fmt.Sprintf("    %q = %q", p.Key, p.Value)
		}
		b.WriteString(strings.Join(propLines, ",\n"))
		b.WriteString("\n)")
	}

	b.WriteByte(';')
	return b.String()
}

// FormatView renders a CREATE [OR REPLACE] VIEW statement.
func FormatView(v *ViewDef) string {
	if v == nil {
		return ""
	}
	if v.RawSQL != "" {
		raw := strings.TrimSpace(v.RawSQL)
		if !strings.HasSuffix(raw, ";") {
			raw += ";"
		}
		return raw
	}
	kw := "CREATE VIEW"
	if v.OrReplace {
		kw = "CREATE OR REPLACE VIEW"
	}
	return fmt.Sprintf("%s %s AS\n%s;", kw, v.CanonicalName(), strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(v.AsSQL), ";")))
}

// FormatPlatformObject renders a platform-level DDL statement.
func FormatPlatformObject(p *PlatformObject) string {
	if p == nil {
		return ""
	}
	if p.RawSQL != "" {
		raw := strings.TrimSpace(p.RawSQL)
		if !strings.HasSuffix(raw, ";") {
			raw += ";"
		}
		return raw
	}
	switch p.Kind {
	case "SET_DEFAULT_STORAGE_VAULT":
		return fmt.Sprintf("SET %s AS DEFAULT STORAGE VAULT;", p.Name)
	case "STORAGE_VAULT", "WORKLOAD_GROUP", "CATALOG", "RESOURCE":
		kw := strings.ReplaceAll(p.Kind, "_", " ")
		ine := ""
		if p.IfNotExists {
			ine = "IF NOT EXISTS "
		}
		var b strings.Builder
		b.WriteString(fmt.Sprintf("CREATE %s %s%s", kw, ine, p.Name))
		if len(p.Properties) > 0 {
			b.WriteString("\nPROPERTIES (\n")
			lines := make([]string, len(p.Properties))
			for i, prop := range p.Properties {
				lines[i] = fmt.Sprintf("    %q = %q", prop.Key, prop.Value)
			}
			b.WriteString(strings.Join(lines, ",\n"))
			b.WriteString("\n)")
		}
		b.WriteByte(';')
		return b.String()
	case "ALTER_WORKLOAD_GROUP":
		var b strings.Builder
		b.WriteString(fmt.Sprintf("ALTER WORKLOAD GROUP %s", p.Name))
		if len(p.Properties) > 0 {
			b.WriteString("\nPROPERTIES (\n")
			lines := make([]string, len(p.Properties))
			for i, prop := range p.Properties {
				lines[i] = fmt.Sprintf("    %q = %q", prop.Key, prop.Value)
			}
			b.WriteString(strings.Join(lines, ",\n"))
			b.WriteString("\n)")
		}
		b.WriteByte(';')
		return b.String()
	}
	return ""
}

// FormatSchema renders the entire Schema into deterministic canonical SQL.
func FormatSchema(s *Schema) string {
	if s == nil {
		return ""
	}
	var blocks []string
	for _, p := range s.PlatformObjects {
		if sql := FormatPlatformObject(p); sql != "" {
			blocks = append(blocks, sql)
		}
	}
	for _, d := range s.Databases {
		if sql := FormatCreateDatabase(d); sql != "" {
			blocks = append(blocks, sql)
		}
	}
	for _, t := range s.Tables {
		if sql := FormatCreateTable(t); sql != "" {
			blocks = append(blocks, sql)
		}
	}
	for _, v := range s.Views {
		if sql := FormatView(v); sql != "" {
			blocks = append(blocks, sql)
		}
	}
	if len(blocks) == 0 {
		return ""
	}
	return strings.Join(blocks, "\n\n") + "\n"
}

// FormatStatements renders a slice of Statements into deterministic SQL text.
func FormatStatements(stmts []Statement) string {
	if len(stmts) == 0 {
		return ""
	}
	var blocks []string
	for _, st := range stmts {
		if sql := st.SQL(); sql != "" {
			blocks = append(blocks, sql)
		}
	}
	if len(blocks) == 0 {
		return ""
	}
	return strings.Join(blocks, "\n\n") + "\n"
}
