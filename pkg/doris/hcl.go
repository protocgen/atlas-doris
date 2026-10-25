package doris

import (
	"fmt"
	"strconv"
	"strings"
)

// IsHCLContent reports whether raw text appears to be an Atlas HCL schema document
// (starting with a top-level `schema` or `table` block after stripping comments).
func IsHCLContent(raw string) bool {
	s := skipLeadingCommentsAndSpace(raw)
	if s == "" {
		return false
	}
	tok, rest := readLeadingWord(s)
	if tok != "schema" && tok != "table" {
		return false
	}
	rest = strings.TrimSpace(rest)
	return rest == "" || rest[0] == '"' || rest[0] == '{'
}

// ParseSchemaAuto parses content as Atlas HCL if filename ends with .hcl or
// IsHCLContent(content) is true; otherwise parses content as Doris/Iceberg SQL DDL.
func ParseSchemaAuto(content, filename string) (*Schema, error) {
	if strings.HasSuffix(strings.ToLower(strings.TrimSpace(filename)), ".hcl") || IsHCLContent(content) {
		return ParseHCL(content)
	}
	return ParseSchema(content)
}

// ParseHCL parses an Atlas HCL schema document (`schema "<name>"` and `table "<name>"` blocks,
// including proto2type-generated *_doris.hcl and *_iceberg.hcl files) into a *Schema.
func ParseHCL(hclText string) (*Schema, error) {
	cleaned, err := stripAtlasHCLComments(hclText)
	if err != nil {
		return nil, err
	}

	s := NewSchema()
	schemaByName := make(map[string]*DatabaseDef)
	var defaultSchema *DatabaseDef

	pos := 0
	for pos < len(cleaned) {
		for pos < len(cleaned) && isHCLSpace(cleaned[pos]) {
			pos++
		}
		if pos >= len(cleaned) {
			break
		}

		openRel := indexUnquotedHCLBrace(cleaned[pos:])
		if openRel < 0 {
			return nil, fmt.Errorf("unexpected trailing content in Atlas HCL: %q", strings.TrimSpace(cleaned[pos:]))
		}
		openAbs := pos + openRel
		header := strings.TrimSpace(cleaned[pos:openAbs])
		closeAbs, err := findMatchingHCLBrace(cleaned, openAbs)
		if err != nil {
			return nil, err
		}
		body := cleaned[openAbs+1 : closeAbs]
		pos = closeAbs + 1

		blockKind, labels, err := parseHCLBlockHeader(header)
		if err != nil {
			return nil, err
		}

		switch blockKind {
		case "schema":
			dbDef, err := parseHCLSchemaBlock(labels, body)
			if err != nil {
				return nil, err
			}
			if dbDef.Name != "" {
				schemaByName[dbDef.Name] = dbDef
				if defaultSchema == nil {
					defaultSchema = dbDef
				}
				s.Databases = append(s.Databases, dbDef)
			}

		case "table":
			tbl, err := parseHCLTableBlock(labels, body)
			if err != nil {
				return nil, err
			}
			if tbl.Database == "" && defaultSchema != nil {
				tbl.Database = defaultSchema.Name
				if tbl.Catalog == "" && defaultSchema.Catalog != "" {
					tbl.Catalog = defaultSchema.Catalog
				}
			}
			if tbl.Database != "" && tbl.Catalog == "" {
				if sd, ok := schemaByName[tbl.Database]; ok && sd.Catalog != "" {
					tbl.Catalog = sd.Catalog
				} else if tbl.IsIcebergTable() {
					tbl.Catalog = "iceberg"
				} else {
					tbl.Catalog = "internal"
				}
			}
			if tbl.Database != "" && tbl.Catalog != "" {
				if sd, ok := schemaByName[tbl.Database]; ok && sd.Catalog == "" {
					sd.Catalog = tbl.Catalog
					sd.FullName = sd.CanonicalName()
				}
			}
			tbl.FullName = tbl.CanonicalName()
			tbl.IfNotExists = true
			s.Tables = append(s.Tables, tbl)

		default:
			return nil, fmt.Errorf("unexpected top-level HCL block %q (expected schema or table)", header)
		}
	}

	for _, d := range s.Databases {
		if d.Catalog == "" && d.Name != "" {
			d.Catalog = "internal"
		}
		d.FullName = d.CanonicalName()
	}

	return s, nil
}

// FormatHCL renders the entire Schema into deterministic Atlas HCL.
func FormatHCL(s *Schema) string {
	if s == nil {
		return ""
	}
	var blocks []string
	for _, d := range s.Databases {
		if blk := FormatDatabaseHCL(d); blk != "" {
			blocks = append(blocks, blk)
		}
	}
	for _, t := range s.Tables {
		if blk := FormatTableHCL(t); blk != "" {
			blocks = append(blocks, blk)
		}
	}
	if len(blocks) == 0 {
		return ""
	}
	return strings.Join(blocks, "\n\n") + "\n"
}

// FormatDatabaseHCL renders a single DatabaseDef as an Atlas HCL `schema "<name>"` block.
func FormatDatabaseHCL(d *DatabaseDef) string {
	if d == nil || d.Name == "" {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "schema %q {\n", d.Name)
	if d.Catalog != "" {
		fmt.Fprintf(&b, "  catalog = %q\n", d.Catalog)
	}
	b.WriteString("}")
	return b.String()
}

// FormatTableHCL renders a single TableDef as a deterministic Atlas HCL `table "<name>"` block
// matching proto2type's BuildDorisAtlasHCL / BuildIcebergAtlasHCL output format.
func FormatTableHCL(t *TableDef) string {
	if t == nil || t.Name == "" {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "table %q {\n", t.Name)

	if t.Catalog != "" && (t.IsIcebergTable() || !strings.EqualFold(t.Catalog, "internal")) {
		fmt.Fprintf(&b, "  catalog = %q\n", t.Catalog)
	}
	if t.Database != "" {
		if strings.Contains(t.Database, "${") || !isValidHCLIdent(t.Database) {
			fmt.Fprintf(&b, "  schema = %q\n", "schema."+t.Database)
		} else {
			fmt.Fprintf(&b, "  schema = schema.%s\n", t.Database)
		}
	}
	if t.Comment != "" {
		fmt.Fprintf(&b, "  comment = %q\n", t.Comment)
	}

	for _, c := range t.Columns {
		fmt.Fprintf(&b, "  column %q {\n", c.Name)
		fmt.Fprintf(&b, "    type = %s\n", dorisTypeToHCLType(c.Type))
		fmt.Fprintf(&b, "    null = %t\n", c.Nullable)
		if c.AggType != "" {
			fmt.Fprintf(&b, "    agg_type = %q\n", strings.ToUpper(strings.TrimSpace(c.AggType)))
		}
		if c.HasDefault {
			fmt.Fprintf(&b, "    default = %q\n", c.Default)
		}
		if c.Comment != "" {
			fmt.Fprintf(&b, "    comment = %q\n", c.Comment)
		}
		b.WriteString("  }\n")
	}

	if t.Key != nil && len(t.Key.Columns) > 0 {
		blockName := "unique_key"
		switch t.Key.Kind {
		case KeyDuplicate:
			blockName = "duplicate_key"
		case KeyAggregate:
			blockName = "aggregate_key"
		}
		fmt.Fprintf(&b, "  %s {\n", blockName)
		fmt.Fprintf(&b, "    columns = %s\n", formatHCLColRefList(t.Key.Columns))
		if t.Key.Kind == KeyUnique {
			if mow, ok := t.GetProperty("enable_unique_key_merge_on_write"); ok && strings.EqualFold(mow, "true") {
				b.WriteString("    merge_on_write = true\n")
			}
			if seq := strings.TrimSpace(t.SequenceCol()); seq != "" {
				fmt.Fprintf(&b, "    sequence_col = column.%s\n", seq)
			}
		}
		b.WriteString("  }\n")
	}

	if t.Distribution != nil {
		if strings.EqualFold(t.Distribution.Kind, "RANDOM") {
			b.WriteString("  distribution {\n")
			b.WriteString("    type = \"RANDOM\"\n")
			if len(t.Distribution.Columns) > 0 {
				fmt.Fprintf(&b, "    columns = %s\n", formatHCLColRefList(t.Distribution.Columns))
			}
			fmt.Fprintf(&b, "    buckets = %s\n", formatHCLBuckets(t.Distribution.Buckets))
			b.WriteString("  }\n")
		} else {
			b.WriteString("  distributed_by_hash {\n")
			fmt.Fprintf(&b, "    columns = %s\n", formatHCLColRefList(t.Distribution.Columns))
			fmt.Fprintf(&b, "    buckets = %s\n", formatHCLBuckets(t.Distribution.Buckets))
			b.WriteString("  }\n")
		}
	}

	if t.Partition != nil {
		switch strings.ToUpper(t.Partition.Kind) {
		case "LIST":
			b.WriteString("  partition_by_list {\n")
			fmt.Fprintf(&b, "    columns = %s\n", formatHCLColRefList(t.Partition.Columns))
			if raw := strings.TrimSpace(t.Partition.PartitionsRaw); raw != "" && raw != "()" {
				fmt.Fprintf(&b, "    partitions = %q\n", raw)
			}
			b.WriteString("  }\n")
		case "RANGE":
			b.WriteString("  partition_by_range {\n")
			fmt.Fprintf(&b, "    columns = %s\n", formatHCLColRefList(t.Partition.Columns))
			if raw := strings.TrimSpace(t.Partition.PartitionsRaw); raw != "" && raw != "()" {
				fmt.Fprintf(&b, "    partitions = %q\n", raw)
			}
			b.WriteString("  }\n")
		case "EXPR":
			b.WriteString("  partition_by {\n")
			fmt.Fprintf(&b, "    spec = %q\n", strings.TrimSpace(t.Partition.RawExpr))
			b.WriteString("  }\n")
		}
	}

	if len(t.OrderBy) > 0 {
		b.WriteString("  order_by {\n")
		fmt.Fprintf(&b, "    columns = %s\n", formatHCLColRefList(t.OrderBy))
		b.WriteString("  }\n")
	}

	for _, idx := range t.Indexes {
		using := strings.ToUpper(strings.TrimSpace(idx.Using))
		if using == "" {
			using = "INVERTED"
		}
		fmt.Fprintf(&b, "  index %q {\n", idx.Name)
		fmt.Fprintf(&b, "    columns = %s\n", formatHCLColRefList(idx.Columns))
		fmt.Fprintf(&b, "    using = %q\n", using)
		if len(idx.Properties) > 0 {
			b.WriteString("    properties = {\n")
			for _, p := range idx.Properties {
				fmt.Fprintf(&b, "      %s = %q\n", formatHCLPropKey(p.Key), p.Value)
			}
			b.WriteString("    }\n")
		}
		if idx.Comment != "" {
			fmt.Fprintf(&b, "    comment = %q\n", idx.Comment)
		}
		b.WriteString("  }\n")
	}

	if len(t.Properties) > 0 {
		b.WriteString("  properties = {\n")
		for _, p := range t.Properties {
			fmt.Fprintf(&b, "    %s = %q\n", formatHCLPropKey(p.Key), p.Value)
		}
		b.WriteString("  }\n")
	}

	b.WriteString("}")
	return b.String()
}

type hclKV struct {
	Key   string
	Value string
}

type hclBodyItem struct {
	IsBlock     bool
	Key         string
	Value       string
	BlockHeader string
	BlockBody   string
}

func parseHCLSchemaBlock(labels []string, body string) (*DatabaseDef, error) {
	rawName := ""
	if len(labels) > 0 {
		rawName = labels[len(labels)-1]
	}
	fullName, catalog, db, short := splitQualifiedName(rawName)
	if db != "" && catalog == "" {
		catalog = db
	}

	items, err := parseHCLBodyItems(body)
	if err != nil {
		return nil, fmt.Errorf("schema %q: %w", rawName, err)
	}
	for _, it := range items {
		if it.IsBlock {
			continue
		}
		switch it.Key {
		case "catalog":
			catalog = unquoteHCLString(it.Value)
		case "charset", "collate", "comment":
			// Accepted Atlas schema metadata attributes.
		default:
			return nil, fmt.Errorf("schema %q: unsupported attribute %q", rawName, it.Key)
		}
	}

	dbDef := &DatabaseDef{
		FullName:    fullName,
		Catalog:     catalog,
		Name:        short,
		IfNotExists: true,
	}
	dbDef.FullName = dbDef.CanonicalName()
	return dbDef, nil
}

func parseHCLTableBlock(labels []string, body string) (*TableDef, error) {
	if len(labels) == 0 || strings.TrimSpace(labels[len(labels)-1]) == "" {
		return nil, fmt.Errorf("table block missing table name label")
	}
	rawTableName := strings.TrimSpace(labels[len(labels)-1])
	_, catalog, db, shortName := splitQualifiedName(rawTableName)
	if len(labels) == 2 && db == "" {
		db = strings.TrimPrefix(strings.TrimSpace(labels[0]), "schema.")
	}
	if !isValidHCLIdent(shortName) {
		return nil, fmt.Errorf("invalid table name %q in Atlas HCL", shortName)
	}

	tbl := &TableDef{
		Catalog:     catalog,
		Database:    db,
		Name:        shortName,
		IfNotExists: true,
	}

	items, err := parseHCLBodyItems(body)
	if err != nil {
		return nil, fmt.Errorf("table %q: %w", shortName, err)
	}

	seenCols := make(map[string]bool)
	var keyMoW bool
	var keySeqCol string

	for _, it := range items {
		if !it.IsBlock {
			switch it.Key {
			case "schema":
				rawSchema := unquoteHCLString(it.Value)
				rawSchema = strings.TrimPrefix(rawSchema, "schema.")
				_, sCat, sDB, sShort := splitQualifiedName(rawSchema)
				if sDB != "" && sCat == "" {
					sCat = sDB
				}
				if sCat != "" && tbl.Catalog == "" {
					tbl.Catalog = sCat
				}
				tbl.Database = sShort
			case "catalog":
				tbl.Catalog = unquoteHCLString(it.Value)
			case "engine":
				tbl.Engine = unquoteHCLString(it.Value)
			case "comment":
				tbl.Comment = unquoteHCLString(it.Value)
			case "order_by":
				cols, err := parseHCLColumnRefList(it.Value)
				if err != nil {
					return nil, fmt.Errorf("table %q order_by: %w", shortName, err)
				}
				tbl.OrderBy = cols
			default:
				return nil, fmt.Errorf("table %q: unsupported attribute %q", shortName, it.Key)
			}
			continue
		}

		bKind, bLabels, err := parseHCLBlockHeader(it.BlockHeader)
		if err != nil {
			return nil, fmt.Errorf("table %q: %w", shortName, err)
		}

		switch bKind {
		case "column":
			if len(bLabels) != 1 || bLabels[0] == "" {
				return nil, fmt.Errorf("table %q: column block requires a name label", shortName)
			}
			colName := bLabels[0]
			if !isValidHCLIdent(colName) {
				return nil, fmt.Errorf("table %q: invalid column name %q", shortName, colName)
			}
			if seenCols[colName] {
				return nil, fmt.Errorf("table %q: duplicate column %q", shortName, colName)
			}
			seenCols[colName] = true
			col, err := parseHCLColumnBlock(colName, it.BlockBody)
			if err != nil {
				return nil, fmt.Errorf("table %q: %w", shortName, err)
			}
			tbl.Columns = append(tbl.Columns, col)

		case "primary_key", "unique_key", "duplicate_key", "aggregate_key":
			kc, mow, seqCol, err := parseHCLKeyBlock(bKind, it.BlockBody)
			if err != nil {
				return nil, fmt.Errorf("table %q: %w", shortName, err)
			}
			tbl.Key = kc
			if mow {
				keyMoW = true
			}
			if seqCol != "" {
				keySeqCol = seqCol
			}

		case "distributed_by_hash", "distributed_by_random", "distribution":
			dist, err := parseHCLDistributionBlock(bKind, it.BlockBody)
			if err != nil {
				return nil, fmt.Errorf("table %q: %w", shortName, err)
			}
			tbl.Distribution = dist

		case "partition_by_list", "partition_by_range", "partition_by", "partition":
			part, err := parseHCLPartitionBlock(bKind, it.BlockBody)
			if err != nil {
				return nil, fmt.Errorf("table %q: %w", shortName, err)
			}
			tbl.Partition = part

		case "order_by":
			cols, err := parseHCLOrderByBlock(it.BlockBody)
			if err != nil {
				return nil, fmt.Errorf("table %q: %w", shortName, err)
			}
			tbl.OrderBy = cols

		case "index":
			if len(bLabels) != 1 || bLabels[0] == "" {
				return nil, fmt.Errorf("table %q: index block requires a name label", shortName)
			}
			idxName := bLabels[0]
			if !isValidHCLIdent(idxName) {
				return nil, fmt.Errorf("table %q: invalid index name %q", shortName, idxName)
			}
			idx, err := parseHCLIndexBlock(idxName, it.BlockBody)
			if err != nil {
				return nil, fmt.Errorf("table %q: %w", shortName, err)
			}
			tbl.Indexes = append(tbl.Indexes, idx)

		case "properties":
			kvs, err := parseHCLKeyValueSlice(it.BlockBody)
			if err != nil {
				return nil, fmt.Errorf("table %q properties: %w", shortName, err)
			}
			for _, kv := range kvs {
				tbl.SetProperty(kv.Key, unquoteHCLString(kv.Value))
			}

		default:
			return nil, fmt.Errorf("table %q: unsupported HCL block %q", shortName, it.BlockHeader)
		}
	}

	if len(tbl.Columns) == 0 {
		return nil, fmt.Errorf("table %q defines zero columns", shortName)
	}

	if keyMoW {
		if _, ok := tbl.GetProperty("enable_unique_key_merge_on_write"); !ok {
			tbl.SetProperty("enable_unique_key_merge_on_write", "true")
		}
	}
	if keySeqCol != "" {
		if _, ok := tbl.GetProperty("function_column.sequence_col"); !ok {
			tbl.SetProperty("function_column.sequence_col", keySeqCol)
		}
	}

	return tbl, nil
}

func parseHCLColumnBlock(colName, body string) (*ColumnDef, error) {
	kvs, err := parseHCLKeyValueSlice(body)
	if err != nil {
		return nil, fmt.Errorf("column %q: %w", colName, err)
	}
	col := &ColumnDef{
		Name:     colName,
		Nullable: false,
	}
	for _, kv := range kvs {
		switch kv.Key {
		case "type":
			col.Type = NormalizeType(unquoteHCLString(kv.Value))
		case "null":
			b, err := strconv.ParseBool(unquoteHCLString(kv.Value))
			if err != nil {
				return nil, fmt.Errorf("column %q: invalid null attribute %q", colName, kv.Value)
			}
			col.Nullable = b
		case "default":
			col.HasDefault = true
			col.Default = unquoteHCLString(kv.Value)
		case "comment":
			col.Comment = unquoteHCLString(kv.Value)
		case "agg_type", "aggregate":
			col.AggType = strings.ToUpper(strings.TrimSpace(unquoteHCLString(kv.Value)))
		default:
			return nil, fmt.Errorf("column %q: unsupported attribute %q", colName, kv.Key)
		}
	}
	if col.Type == "" {
		return nil, fmt.Errorf("column %q missing type attribute", colName)
	}
	return col, nil
}

func parseHCLKeyBlock(blockKind, body string) (*KeyClause, bool, string, error) {
	kind := KeyUnique
	switch blockKind {
	case "duplicate_key":
		kind = KeyDuplicate
	case "aggregate_key":
		kind = KeyAggregate
	}

	kvs, err := parseHCLKeyValueSlice(body)
	if err != nil {
		return nil, false, "", fmt.Errorf("%s: %w", blockKind, err)
	}

	var cols []string
	hasCols := false
	mow := false
	seqCol := ""

	for _, kv := range kvs {
		switch kv.Key {
		case "columns":
			parsedCols, err := parseHCLColumnRefList(kv.Value)
			if err != nil {
				return nil, false, "", fmt.Errorf("%s: %w", blockKind, err)
			}
			cols = parsedCols
			hasCols = true
		case "merge_on_write":
			b, err := strconv.ParseBool(unquoteHCLString(kv.Value))
			if err != nil {
				return nil, false, "", fmt.Errorf("%s: invalid merge_on_write %q", blockKind, kv.Value)
			}
			mow = b
		case "sequence_col":
			sc := strings.TrimPrefix(unquoteHCLString(kv.Value), "column.")
			sc = strings.TrimSpace(sc)
			if !isValidHCLIdent(sc) {
				return nil, false, "", fmt.Errorf("%s: invalid sequence_col %q", blockKind, kv.Value)
			}
			seqCol = sc
		default:
			return nil, false, "", fmt.Errorf("%s: unsupported attribute %q", blockKind, kv.Key)
		}
	}

	if !hasCols || len(cols) == 0 {
		return nil, false, "", fmt.Errorf("%s missing columns attribute", blockKind)
	}
	return &KeyClause{Kind: kind, Columns: cols}, mow, seqCol, nil
}

func parseHCLDistributionBlock(blockKind, body string) (*DistributionClause, error) {
	dist := &DistributionClause{
		Kind:    "HASH",
		Buckets: "AUTO",
	}
	if blockKind == "distributed_by_random" {
		dist.Kind = "RANDOM"
	}

	kvs, err := parseHCLKeyValueSlice(body)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", blockKind, err)
	}

	for _, kv := range kvs {
		switch kv.Key {
		case "type", "kind":
			k := strings.ToUpper(strings.TrimSpace(unquoteHCLString(kv.Value)))
			if k != "HASH" && k != "RANDOM" {
				return nil, fmt.Errorf("%s: unsupported distribution type %q", blockKind, kv.Value)
			}
			dist.Kind = k
		case "columns":
			cols, err := parseHCLColumnRefList(kv.Value)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", blockKind, err)
			}
			dist.Columns = cols
		case "buckets", "buckets_symbol":
			bVal := strings.TrimSpace(unquoteHCLString(kv.Value))
			if strings.EqualFold(bVal, "auto") {
				dist.Buckets = "AUTO"
			} else if strings.HasPrefix(bVal, "${") && strings.HasSuffix(bVal, "}") {
				dist.Buckets = bVal
			} else {
				n, err := strconv.Atoi(bVal)
				if err != nil || n <= 0 {
					return nil, fmt.Errorf("%s: invalid buckets %q", blockKind, kv.Value)
				}
				dist.Buckets = strconv.Itoa(n)
			}
		default:
			return nil, fmt.Errorf("%s: unsupported attribute %q", blockKind, kv.Key)
		}
	}

	if dist.Kind == "HASH" && len(dist.Columns) == 0 {
		return nil, fmt.Errorf("%s: HASH distribution requires columns", blockKind)
	}
	return dist, nil
}

func parseHCLPartitionBlock(blockKind, body string) (*PartitionClause, error) {
	kvs, err := parseHCLKeyValueSlice(body)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", blockKind, err)
	}

	var kind, spec, partitions string
	var cols []string
	switch blockKind {
	case "partition_by_list":
		kind = "LIST"
	case "partition_by_range":
		kind = "RANGE"
	}

	for _, kv := range kvs {
		switch kv.Key {
		case "kind", "type":
			kind = strings.ToUpper(strings.TrimSpace(unquoteHCLString(kv.Value)))
		case "columns":
			c, err := parseHCLColumnRefList(kv.Value)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", blockKind, err)
			}
			cols = c
		case "spec", "expr":
			spec = strings.TrimSpace(unquoteHCLString(kv.Value))
		case "partitions":
			partitions = strings.TrimSpace(unquoteHCLString(kv.Value))
		default:
			return nil, fmt.Errorf("%s: unsupported attribute %q", blockKind, kv.Key)
		}
	}

	switch strings.ToUpper(kind) {
	case "LIST", "RANGE":
		if len(cols) == 0 && spec != "" {
			cols = parseIdentList(spec)
		}
		if len(cols) == 0 {
			return nil, fmt.Errorf("%s: missing partition columns", blockKind)
		}
		if partitions == "" {
			partitions = "()"
		}
		return &PartitionClause{
			Kind:          strings.ToUpper(kind),
			Columns:       cols,
			PartitionsRaw: partitions,
		}, nil
	default:
		if spec == "" && len(cols) > 0 {
			if blockKind == "partition" {
				if partitions == "" {
					partitions = "()"
				}
				return &PartitionClause{
					Kind:          "LIST",
					Columns:       cols,
					PartitionsRaw: partitions,
				}, nil
			}
			spec = strings.Join(cols, ", ")
		}
		if spec == "" {
			return nil, fmt.Errorf("%s: missing partition spec or columns", blockKind)
		}
		return &PartitionClause{
			Kind:    "EXPR",
			RawExpr: spec,
		}, nil
	}
}

func parseHCLOrderByBlock(body string) ([]string, error) {
	kvs, err := parseHCLKeyValueSlice(body)
	if err != nil {
		return nil, fmt.Errorf("order_by: %w", err)
	}
	for _, kv := range kvs {
		if kv.Key == "columns" {
			return parseHCLColumnRefList(kv.Value)
		}
		return nil, fmt.Errorf("order_by: unsupported attribute %q", kv.Key)
	}
	return nil, fmt.Errorf("order_by: missing columns attribute")
}

func parseHCLIndexBlock(idxName, body string) (*IndexDef, error) {
	items, err := parseHCLBodyItems(body)
	if err != nil {
		return nil, fmt.Errorf("index %q: %w", idxName, err)
	}

	idx := &IndexDef{
		Name:  idxName,
		Using: "INVERTED",
	}

	for _, it := range items {
		if it.IsBlock {
			bKind, _, err := parseHCLBlockHeader(it.BlockHeader)
			if err != nil {
				return nil, fmt.Errorf("index %q: %w", idxName, err)
			}
			if bKind != "properties" {
				return nil, fmt.Errorf("index %q: unsupported sub-block %q", idxName, it.BlockHeader)
			}
			props, err := parseHCLKeyValueSlice(it.BlockBody)
			if err != nil {
				return nil, fmt.Errorf("index %q properties: %w", idxName, err)
			}
			for _, p := range props {
				idx.Properties = append(idx.Properties, Property{
					Key:   p.Key,
					Value: unquoteHCLString(p.Value),
				})
			}
			continue
		}

		switch it.Key {
		case "columns":
			cols, err := parseHCLColumnRefList(it.Value)
			if err != nil {
				return nil, fmt.Errorf("index %q: %w", idxName, err)
			}
			idx.Columns = cols
		case "using", "type":
			idx.Using = strings.ToUpper(strings.TrimSpace(unquoteHCLString(it.Value)))
		case "comment":
			idx.Comment = unquoteHCLString(it.Value)
		default:
			return nil, fmt.Errorf("index %q: unsupported attribute %q", idxName, it.Key)
		}
	}

	if len(idx.Columns) == 0 {
		return nil, fmt.Errorf("index %q: missing columns attribute", idxName)
	}
	return idx, nil
}

// parseHCLBodyItems scans a block body into ordered key=value attributes and nested {...} blocks.
// It supports both multi-line and single-line (space- or comma-separated) HCL syntax.
func parseHCLBodyItems(body string) ([]hclBodyItem, error) {
	var items []hclBodyItem
	n := len(body)
	i := 0

	for i < n {
		for i < n && (isHCLSpace(body[i]) || body[i] == ',') {
			i++
		}
		if i >= n {
			break
		}

		// Read key or block header up to '=' or '{'
		start := i
		inQuotes := false
		var sep byte
		for i < n {
			ch := body[i]
			if inQuotes {
				if ch == '\\' && i+1 < n {
					i += 2
					continue
				}
				if ch == '"' {
					inQuotes = false
				}
				i++
				continue
			}
			if ch == '"' {
				inQuotes = true
				i++
				continue
			}
			if ch == '=' || ch == '{' {
				sep = ch
				break
			}
			if ch == '\n' || ch == '\r' {
				return nil, fmt.Errorf("unexpected statement %q", strings.TrimSpace(body[start:i]))
			}
			i++
		}
		if sep == 0 {
			return nil, fmt.Errorf("unexpected statement %q", strings.TrimSpace(body[start:]))
		}

		left := strings.TrimSpace(body[start:i])
		if left == "" {
			return nil, fmt.Errorf("missing key or block name before %q", string(sep))
		}

		if sep == '{' {
			closeAbs, err := findMatchingHCLBrace(body, i)
			if err != nil {
				return nil, err
			}
			items = append(items, hclBodyItem{
				IsBlock:     true,
				BlockHeader: left,
				BlockBody:   body[i+1 : closeAbs],
			})
			i = closeAbs + 1
			continue
		}

		// sep == '=': advance past '=' and inspect value
		i++
		for i < n && (body[i] == ' ' || body[i] == '\t') {
			i++
		}
		if i < n && body[i] == '{' {
			closeAbs, err := findMatchingHCLBrace(body, i)
			if err != nil {
				return nil, err
			}
			items = append(items, hclBodyItem{
				IsBlock:     true,
				BlockHeader: left,
				BlockBody:   body[i+1 : closeAbs],
			})
			i = closeAbs + 1
			continue
		}

		val, nextIdx, err := readHCLValueToken(body, i)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", left, err)
		}
		if val == "" {
			return nil, fmt.Errorf("attribute %q has empty value", left)
		}
		items = append(items, hclBodyItem{
			IsBlock: false,
			Key:     unquoteHCLString(left),
			Value:   val,
		})
		i = nextIdx
	}

	return items, nil
}

func parseHCLKeyValueSlice(body string) ([]hclKV, error) {
	items, err := parseHCLBodyItems(body)
	if err != nil {
		return nil, err
	}
	out := make([]hclKV, 0, len(items))
	for _, it := range items {
		if it.IsBlock {
			return nil, fmt.Errorf("unexpected block %q where key = value assignment was expected", it.BlockHeader)
		}
		if it.Key == "" || it.Value == "" {
			return nil, fmt.Errorf("invalid key = value assignment (%q = %q)", it.Key, it.Value)
		}
		out = append(out, hclKV{Key: it.Key, Value: it.Value})
	}
	return out, nil
}

func readHCLValueToken(s string, start int) (string, int, error) {
	n := len(s)
	i := start
	for i < n && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	if i >= n {
		return "", i, fmt.Errorf("missing value")
	}

	// Double-quoted string literal
	if s[i] == '"' {
		valStart := i
		i++
		for i < n {
			ch := s[i]
			if ch == '\\' && i+1 < n {
				i += 2
				continue
			}
			if ch == '"' {
				i++
				return s[valStart:i], i, nil
			}
			i++
		}
		return "", n, fmt.Errorf("unterminated string literal")
	}

	// Bracketed list [...]
	if s[i] == '[' {
		valStart := i
		depth := 0
		inQuotes := false
		for i < n {
			ch := s[i]
			if inQuotes {
				if ch == '\\' && i+1 < n {
					i += 2
					continue
				}
				if ch == '"' {
					inQuotes = false
				}
				i++
				continue
			}
			switch ch {
			case '"':
				inQuotes = true
			case '[':
				depth++
			case ']':
				depth--
				if depth == 0 {
					i++
					return s[valStart:i], i, nil
				}
			}
			i++
		}
		return "", n, fmt.Errorf("unbalanced '[' in list value")
	}

	// Unquoted expression (e.g. varchar(64), decimalv3(18, 4), array<float>, schema.tenant_acme, true, 16)
	valStart := i
	parenDepth := 0
	angleDepth := 0
	for i < n {
		ch := s[i]
		switch ch {
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		case '<':
			angleDepth++
		case '>':
			if angleDepth > 0 {
				angleDepth--
			}
		case ' ', '\t', '\n', '\r', ',':
			if parenDepth == 0 && angleDepth == 0 {
				return strings.TrimSpace(s[valStart:i]), i, nil
			}
		}
		i++
	}
	if parenDepth != 0 || angleDepth != 0 {
		return "", n, fmt.Errorf("unbalanced type/expression %q", strings.TrimSpace(s[valStart:]))
	}
	return strings.TrimSpace(s[valStart:n]), n, nil
}

func parseHCLBlockHeader(header string) (string, []string, error) {
	h := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(header), "="))
	if h == "" {
		return "", nil, fmt.Errorf("empty HCL block header")
	}
	kind, rest := readLeadingWord(h)
	if kind == "" {
		return "", nil, fmt.Errorf("invalid HCL block header %q", header)
	}
	var labels []string
	rem := strings.TrimSpace(rest)
	for rem != "" {
		if rem[0] == '"' {
			tok, nextIdx, err := readHCLValueToken(rem, 0)
			if err != nil {
				return "", nil, err
			}
			labels = append(labels, unquoteHCLString(tok))
			rem = strings.TrimSpace(rem[nextIdx:])
		} else {
			w, next := readLeadingWord(rem)
			if w == "" {
				return "", nil, fmt.Errorf("unexpected token in block header %q", header)
			}
			labels = append(labels, w)
			rem = strings.TrimSpace(next)
		}
	}
	return kind, labels, nil
}

func parseHCLColumnRefList(raw string) ([]string, error) {
	s := strings.TrimSpace(raw)
	if !strings.HasPrefix(s, "[") || !strings.HasSuffix(s, "]") {
		return nil, fmt.Errorf("expected bracketed column list, got %q", raw)
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return nil, nil
	}
	parts := strings.Split(inner, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		item := unquoteHCLString(p)
		item = strings.TrimPrefix(item, "column.")
		item = strings.Trim(strings.TrimSpace(item), "`")
		if item == "" || !isValidHCLIdent(item) {
			return nil, fmt.Errorf("invalid column reference %q in %q", p, raw)
		}
		out = append(out, item)
	}
	return out, nil
}

// stripAtlasHCLComments removes #, //, and /* ... */ comments outside of double-quoted strings.
func stripAtlasHCLComments(src string) (string, error) {
	var b strings.Builder
	b.Grow(len(src))
	inQuotes := false
	for i := 0; i < len(src); i++ {
		ch := src[i]
		if inQuotes {
			b.WriteByte(ch)
			if ch == '\\' && i+1 < len(src) {
				i++
				b.WriteByte(src[i])
				continue
			}
			if ch == '"' {
				inQuotes = false
			}
			continue
		}
		if ch == '"' {
			inQuotes = true
			b.WriteByte(ch)
			continue
		}
		if ch == '#' || (ch == '/' && i+1 < len(src) && src[i+1] == '/') {
			for i < len(src) && src[i] != '\n' {
				i++
			}
			if i < len(src) {
				b.WriteByte('\n')
			}
			continue
		}
		if ch == '/' && i+1 < len(src) && src[i+1] == '*' {
			i += 2
			closed := false
			for i+1 < len(src) {
				if src[i] == '*' && src[i+1] == '/' {
					i++
					closed = true
					break
				}
				if src[i] == '\n' {
					b.WriteByte('\n')
				}
				i++
			}
			if !closed {
				return "", fmt.Errorf("unterminated block comment in Atlas HCL")
			}
			continue
		}
		b.WriteByte(ch)
	}
	if inQuotes {
		return "", fmt.Errorf("unterminated string literal in Atlas HCL")
	}
	return b.String(), nil
}

func indexUnquotedHCLBrace(s string) int {
	inQuotes := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuotes {
			if ch == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if ch == '"' {
				inQuotes = false
			}
			continue
		}
		switch ch {
		case '"':
			inQuotes = true
		case '{':
			return i
		}
	}
	return -1
}

func findMatchingHCLBrace(s string, openIdx int) (int, error) {
	depth := 0
	inQuotes := false
	for i := openIdx; i < len(s); i++ {
		ch := s[i]
		if inQuotes {
			if ch == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if ch == '"' {
				inQuotes = false
			}
			continue
		}
		switch ch {
		case '"':
			inQuotes = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return -1, fmt.Errorf("unbalanced '{' in Atlas HCL")
}

func unquoteHCLString(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		if unq, err := strconv.Unquote(s); err == nil {
			return unq
		}
		return s[1 : len(s)-1]
	}
	return s
}

func dorisTypeToHCLType(dorisType string) string {
	norm := NormalizeType(dorisType)
	if norm == "" {
		return "varchar(256)"
	}
	return strings.ToLower(norm)
}

func formatHCLColRefList(cols []string) string {
	refs := make([]string, len(cols))
	for i, c := range cols {
		refs[i] = "column." + c
	}
	return "[" + strings.Join(refs, ", ") + "]"
}

func formatHCLPropKey(k string) string {
	if isValidHCLIdent(k) {
		return k
	}
	return fmt.Sprintf("%q", k)
}

func formatHCLBuckets(b string) string {
	trimmed := strings.TrimSpace(b)
	if trimmed == "" || strings.EqualFold(trimmed, "auto") {
		return `"auto"`
	}
	if n, err := strconv.Atoi(trimmed); err == nil && n > 0 {
		return strconv.Itoa(n)
	}
	return fmt.Sprintf("%q", trimmed)
}

func isValidHCLIdent(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isAlpha := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
		isDigit := c >= '0' && c <= '9'
		if i == 0 && !isAlpha {
			return false
		}
		if !isAlpha && !isDigit {
			return false
		}
	}
	return true
}

func isHCLSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func skipLeadingCommentsAndSpace(s string) string {
	i := 0
	n := len(s)
	for i < n {
		if isHCLSpace(s[i]) {
			i++
			continue
		}
		if s[i] == '#' || (s[i] == '/' && i+1 < n && s[i+1] == '/') || (s[i] == '-' && i+1 < n && s[i+1] == '-') {
			for i < n && s[i] != '\n' {
				i++
			}
			continue
		}
		if s[i] == '/' && i+1 < n && s[i+1] == '*' {
			i += 2
			for i+1 < n && !(s[i] == '*' && s[i+1] == '/') {
				i++
			}
			if i+1 < n {
				i += 2
			} else {
				return ""
			}
			continue
		}
		break
	}
	return s[i:]
}

func readLeadingWord(s string) (string, string) {
	s = strings.TrimSpace(s)
	i := 0
	for i < len(s) {
		ch := s[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' {
			i++
		} else {
			break
		}
	}
	return s[:i], s[i:]
}
