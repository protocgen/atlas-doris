package doris

import (
	"fmt"
	"strings"
)

// ParseSchema parses a SQL or Atlas HCL document containing one or more Doris /
// Iceberg DDL or table definitions and returns the resulting Schema state.
func ParseSchema(sqlText string) (*Schema, error) {
	if IsHCLContent(sqlText) {
		return ParseHCL(sqlText)
	}
	stmts, err := ParseStatements(sqlText)
	if err != nil {
		return nil, err
	}
	return ReplayStatements(stmts)
}

// ReplayStatements replays a sequence of DDL statements in order to construct
// the resulting Schema state.
func ReplayStatements(stmts []Statement) (*Schema, error) {
	s := NewSchema()
	for i, st := range stmts {
		if err := ApplyStatement(s, st); err != nil {
			return nil, fmt.Errorf("statement %d (%s): %w", i+1, st.Kind, err)
		}
	}
	return s, nil
}

// ApplyStatement applies a single DDL statement to Schema s in-place.
func ApplyStatement(s *Schema, st Statement) error {
	if s == nil {
		return fmt.Errorf("nil schema")
	}
	switch st.Kind {
	case StmtCreateDatabase:
		if st.Database == nil {
			return fmt.Errorf("CREATE DATABASE statement has nil DatabaseDef")
		}
		if _, exists := s.DatabaseByName(st.Database.CanonicalName()); !exists {
			s.Databases = append(s.Databases, st.Database.Clone())
		}
		return nil

	case StmtCreateTable:
		if st.Table == nil {
			return fmt.Errorf("CREATE TABLE statement has nil TableDef")
		}
		for i, existing := range s.Tables {
			if existing.CanonicalName() == st.Table.CanonicalName() {
				if st.Table.IfNotExists {
					return nil
				}
				s.Tables[i] = st.Table.Clone()
				return nil
			}
		}
		s.Tables = append(s.Tables, st.Table.Clone())
		return nil

	case StmtDropTable:
		target := normalizeQualifiedName(st.TargetTable)
		for i, existing := range s.Tables {
			if existing.CanonicalName() == target || existing.Name == target {
				s.Tables = append(s.Tables[:i], s.Tables[i+1:]...)
				return nil
			}
		}
		if st.IfExists {
			return nil
		}
		return fmt.Errorf("DROP TABLE target %q does not exist", st.TargetTable)

	case StmtAlterTableAddColumn:
		tbl, ok := s.TableByName(st.TargetTable)
		if !ok {
			return fmt.Errorf("ALTER TABLE ADD COLUMN target %q does not exist", st.TargetTable)
		}
		if st.Column == nil {
			return fmt.Errorf("ALTER TABLE ADD COLUMN has nil ColumnDef")
		}
		if _, exists := tbl.ColumnByName(st.Column.Name); exists {
			return fmt.Errorf("column %q already exists on table %q", st.Column.Name, tbl.CanonicalName())
		}
		newCol := st.Column.Clone()
		if st.First {
			tbl.Columns = append([]*ColumnDef{newCol}, tbl.Columns...)
			return nil
		}
		if st.AfterColumn != "" {
			afterIdx := -1
			for i, c := range tbl.Columns {
				if c.Name == st.AfterColumn {
					afterIdx = i
					break
				}
			}
			if afterIdx < 0 {
				return fmt.Errorf("AFTER column %q does not exist on table %q", st.AfterColumn, tbl.CanonicalName())
			}
			updated := make([]*ColumnDef, 0, len(tbl.Columns)+1)
			updated = append(updated, tbl.Columns[:afterIdx+1]...)
			updated = append(updated, newCol)
			updated = append(updated, tbl.Columns[afterIdx+1:]...)
			tbl.Columns = updated
			return nil
		}
		tbl.Columns = append(tbl.Columns, newCol)
		return nil

	case StmtAlterTableDropColumn:
		tbl, ok := s.TableByName(st.TargetTable)
		if !ok {
			return fmt.Errorf("ALTER TABLE DROP COLUMN target %q does not exist", st.TargetTable)
		}
		for i, c := range tbl.Columns {
			if c.Name == st.DropColumnName {
				tbl.Columns = append(tbl.Columns[:i], tbl.Columns[i+1:]...)
				return nil
			}
		}
		return fmt.Errorf("column %q does not exist on table %q", st.DropColumnName, tbl.CanonicalName())

	case StmtAlterTableModifyColumn:
		tbl, ok := s.TableByName(st.TargetTable)
		if !ok {
			return fmt.Errorf("ALTER TABLE MODIFY COLUMN target %q does not exist", st.TargetTable)
		}
		if st.Column == nil {
			return fmt.Errorf("ALTER TABLE MODIFY COLUMN has nil ColumnDef")
		}
		colIdx := -1
		for i, c := range tbl.Columns {
			if c.Name == st.Column.Name {
				colIdx = i
				break
			}
		}
		if colIdx < 0 {
			return fmt.Errorf("column %q does not exist on table %q", st.Column.Name, tbl.CanonicalName())
		}
		modCol := st.Column.Clone()
		tbl.Columns[colIdx] = modCol
		if st.First {
			without := append([]*ColumnDef(nil), tbl.Columns[:colIdx]...)
			without = append(without, tbl.Columns[colIdx+1:]...)
			tbl.Columns = append([]*ColumnDef{modCol}, without...)
			return nil
		}
		if st.AfterColumn != "" {
			without := append([]*ColumnDef(nil), tbl.Columns[:colIdx]...)
			without = append(without, tbl.Columns[colIdx+1:]...)
			afterIdx := -1
			for i, c := range without {
				if c.Name == st.AfterColumn {
					afterIdx = i
					break
				}
			}
			if afterIdx < 0 {
				return fmt.Errorf("AFTER column %q does not exist on table %q", st.AfterColumn, tbl.CanonicalName())
			}
			reordered := make([]*ColumnDef, 0, len(without)+1)
			reordered = append(reordered, without[:afterIdx+1]...)
			reordered = append(reordered, modCol)
			reordered = append(reordered, without[afterIdx+1:]...)
			tbl.Columns = reordered
		}
		return nil

	case StmtAlterTableAddIndex:
		tbl, ok := s.TableByName(st.TargetTable)
		if !ok {
			return fmt.Errorf("ALTER TABLE ADD INDEX target %q does not exist", st.TargetTable)
		}
		if st.Index == nil {
			return fmt.Errorf("ALTER TABLE ADD INDEX has nil IndexDef")
		}
		for i, idx := range tbl.Indexes {
			if idx.Name == st.Index.Name {
				tbl.Indexes[i] = st.Index.Clone()
				return nil
			}
		}
		tbl.Indexes = append(tbl.Indexes, st.Index.Clone())
		return nil

	case StmtAlterTableDropIndex:
		tbl, ok := s.TableByName(st.TargetTable)
		if !ok {
			return fmt.Errorf("ALTER TABLE DROP INDEX target %q does not exist", st.TargetTable)
		}
		for i, idx := range tbl.Indexes {
			if idx.Name == st.DropIndexName {
				tbl.Indexes = append(tbl.Indexes[:i], tbl.Indexes[i+1:]...)
				return nil
			}
		}
		return fmt.Errorf("index %q does not exist on table %q", st.DropIndexName, tbl.CanonicalName())

	case StmtBuildIndex:
		tbl, ok := s.TableByName(st.TargetTable)
		if !ok {
			return fmt.Errorf("BUILD INDEX target table %q does not exist", st.TargetTable)
		}
		if _, ok := tbl.IndexByName(st.DropIndexName); !ok {
			return fmt.Errorf("BUILD INDEX target index %q does not exist on table %q", st.DropIndexName, tbl.CanonicalName())
		}
		return nil

	case StmtAlterTableSetProperties:
		tbl, ok := s.TableByName(st.TargetTable)
		if !ok {
			return fmt.Errorf("ALTER TABLE SET target %q does not exist", st.TargetTable)
		}
		for _, p := range st.Properties {
			tbl.SetProperty(p.Key, p.Value)
		}
		return nil

	case StmtCreateOrReplaceView:
		if st.View == nil {
			return fmt.Errorf("CREATE VIEW statement has nil ViewDef")
		}
		for i, v := range s.Views {
			if v.CanonicalName() == st.View.CanonicalName() {
				s.Views[i] = st.View.Clone()
				return nil
			}
		}
		s.Views = append(s.Views, st.View.Clone())
		return nil

	case StmtPlatformObject:
		if st.Platform == nil {
			return fmt.Errorf("PLATFORM_OBJECT statement has nil PlatformObject")
		}
		if st.Platform.Kind == "ALTER_WORKLOAD_GROUP" {
			for _, existing := range s.PlatformObjects {
				if existing.Kind == "WORKLOAD_GROUP" && existing.Name == st.Platform.Name {
					propMap := propertiesToMap(existing.Properties)
					for _, np := range st.Platform.Properties {
						if _, ok := propMap[np.Key]; ok {
							for idx := range existing.Properties {
								if existing.Properties[idx].Key == np.Key {
									existing.Properties[idx].Value = np.Value
								}
							}
						} else {
							existing.Properties = append(existing.Properties, np)
						}
					}
					existing.RawSQL = ""
					return nil
				}
			}
		}
		for i, existing := range s.PlatformObjects {
			if existing.Kind == st.Platform.Kind && existing.Name == st.Platform.Name && st.Platform.Name != "" {
				s.PlatformObjects[i] = st.Platform.Clone()
				return nil
			}
		}
		s.PlatformObjects = append(s.PlatformObjects, st.Platform.Clone())
		return nil

	default:
		return fmt.Errorf("unsupported statement kind %q", st.Kind)
	}
}

// ParseStatements strips SQL comments, splits sqlText into individual statements,
// and parses each into a Statement AST node.
func ParseStatements(sqlText string) ([]Statement, error) {
	rawStmts, err := StripCommentsAndSplitStatements(sqlText)
	if err != nil {
		return nil, err
	}
	var out []Statement
	for _, raw := range rawStmts {
		st, err := parseSingleStatement(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}

// StripCommentsAndSplitStatements removes -- line comments and /* */ block comments
// (outside string literals and backtick identifiers) and splits the input on top-level semicolons.
func StripCommentsAndSplitStatements(sqlText string) ([]string, error) {
	var stmts []string
	var cur strings.Builder
	var inSingleQuote, inDoubleQuote, inBacktick bool
	parenDepth := 0

	n := len(sqlText)
	for i := 0; i < n; i++ {
		ch := sqlText[i]

		if inSingleQuote {
			cur.WriteByte(ch)
			if ch == '\\' && i+1 < n {
				i++
				cur.WriteByte(sqlText[i])
				continue
			}
			if ch == '\'' {
				if i+1 < n && sqlText[i+1] == '\'' {
					i++
					cur.WriteByte(sqlText[i])
					continue
				}
				inSingleQuote = false
			}
			continue
		}

		if inDoubleQuote {
			cur.WriteByte(ch)
			if ch == '\\' && i+1 < n {
				i++
				cur.WriteByte(sqlText[i])
				continue
			}
			if ch == '"' {
				if i+1 < n && sqlText[i+1] == '"' {
					i++
					cur.WriteByte(sqlText[i])
					continue
				}
				inDoubleQuote = false
			}
			continue
		}

		if inBacktick {
			cur.WriteByte(ch)
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		// Line comment --
		if ch == '-' && i+1 < n && sqlText[i+1] == '-' {
			i += 2
			for i < n && sqlText[i] != '\n' {
				i++
			}
			cur.WriteByte('\n')
			continue
		}

		// Block comment /* ... */
		if ch == '/' && i+1 < n && sqlText[i+1] == '*' {
			i += 2
			closed := false
			for i+1 < n {
				if sqlText[i] == '*' && sqlText[i+1] == '/' {
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated block comment")
			}
			cur.WriteByte(' ')
			continue
		}

		switch ch {
		case '\'':
			inSingleQuote = true
			cur.WriteByte(ch)
		case '"':
			inDoubleQuote = true
			cur.WriteByte(ch)
		case '`':
			inBacktick = true
			cur.WriteByte(ch)
		case '(':
			parenDepth++
			cur.WriteByte(ch)
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
			cur.WriteByte(ch)
		case ';':
			if parenDepth == 0 {
				s := strings.TrimSpace(cur.String())
				if s != "" {
					stmts = append(stmts, s)
				}
				cur.Reset()
			} else {
				cur.WriteByte(ch)
			}
		default:
			cur.WriteByte(ch)
		}
	}

	if inSingleQuote || inDoubleQuote || inBacktick {
		return nil, fmt.Errorf("unterminated quote in SQL input")
	}
	if parenDepth != 0 {
		return nil, fmt.Errorf("unbalanced parentheses in SQL input")
	}

	tail := strings.TrimSpace(cur.String())
	if tail != "" {
		stmts = append(stmts, tail)
	}
	return stmts, nil
}

func parseSingleStatement(raw string) (Statement, error) {
	trimmed := strings.TrimSpace(raw)
	tokens := scanLeadingKeywords(trimmed, 6)
	if len(tokens) == 0 {
		return Statement{}, fmt.Errorf("empty SQL statement")
	}

	w0 := strings.ToUpper(tokens[0])
	w1 := ""
	if len(tokens) > 1 {
		w1 = strings.ToUpper(tokens[1])
	}
	w2 := ""
	if len(tokens) > 2 {
		w2 = strings.ToUpper(tokens[2])
	}
	w3 := ""
	if len(tokens) > 3 {
		w3 = strings.ToUpper(tokens[3])
	}

	switch w0 {
	case "CREATE":
		switch w1 {
		case "DATABASE", "SCHEMA":
			db, err := parseCreateDatabase(trimmed)
			if err != nil {
				return Statement{}, err
			}
			return Statement{Kind: StmtCreateDatabase, Database: db, RawSQL: trimmed}, nil
		case "TABLE":
			tbl, err := parseCreateTable(trimmed)
			if err != nil {
				return Statement{}, err
			}
			return Statement{Kind: StmtCreateTable, Table: tbl, RawSQL: trimmed}, nil
		case "VIEW":
			v, err := parseCreateView(trimmed, false)
			if err != nil {
				return Statement{}, err
			}
			return Statement{Kind: StmtCreateOrReplaceView, View: v, RawSQL: trimmed}, nil
		case "OR":
			if w2 == "REPLACE" && w3 == "VIEW" {
				v, err := parseCreateView(trimmed, true)
				if err != nil {
					return Statement{}, err
				}
				return Statement{Kind: StmtCreateOrReplaceView, View: v, RawSQL: trimmed}, nil
			}
		case "STORAGE":
			if w2 == "VAULT" {
				po, err := parsePlatformCreate(trimmed, "STORAGE_VAULT", "CREATE STORAGE VAULT")
				if err != nil {
					return Statement{}, err
				}
				return Statement{Kind: StmtPlatformObject, Platform: po, RawSQL: trimmed}, nil
			}
		case "WORKLOAD":
			if w2 == "GROUP" {
				po, err := parsePlatformCreate(trimmed, "WORKLOAD_GROUP", "CREATE WORKLOAD GROUP")
				if err != nil {
					return Statement{}, err
				}
				return Statement{Kind: StmtPlatformObject, Platform: po, RawSQL: trimmed}, nil
			}
			if w2 == "POLICY" {
				po, err := parseWorkloadPolicy(trimmed)
				if err != nil {
					return Statement{}, err
				}
				return Statement{Kind: StmtPlatformObject, Platform: po, RawSQL: trimmed}, nil
			}
		case "CATALOG":
			po, err := parsePlatformCreate(trimmed, "CATALOG", "CREATE CATALOG")
			if err != nil {
				return Statement{}, err
			}
			return Statement{Kind: StmtPlatformObject, Platform: po, RawSQL: trimmed}, nil
		case "RESOURCE":
			po, err := parsePlatformCreate(trimmed, "RESOURCE", "CREATE RESOURCE")
			if err != nil {
				return Statement{}, err
			}
			return Statement{Kind: StmtPlatformObject, Platform: po, RawSQL: trimmed}, nil
		}

	case "DROP":
		if w1 == "TABLE" {
			return parseDropTable(trimmed)
		}

	case "ALTER":
		if w1 == "TABLE" {
			return parseAlterTable(trimmed)
		}
		if w1 == "WORKLOAD" && w2 == "GROUP" {
			po, err := parsePlatformAlterWorkloadGroup(trimmed)
			if err != nil {
				return Statement{}, err
			}
			return Statement{Kind: StmtPlatformObject, Platform: po, RawSQL: trimmed}, nil
		}

	case "BUILD":
		if w1 == "INDEX" {
			return parseBuildIndex(trimmed)
		}

	case "SET":
		// SET <vault> AS DEFAULT STORAGE VAULT
		if len(tokens) >= 6 &&
			strings.EqualFold(tokens[2], "AS") &&
			strings.EqualFold(tokens[3], "DEFAULT") &&
			strings.EqualFold(tokens[4], "STORAGE") &&
			strings.EqualFold(tokens[5], "VAULT") {
			vaultName := strings.Trim(tokens[1], "`")
			po := &PlatformObject{
				Kind:   "SET_DEFAULT_STORAGE_VAULT",
				Name:   vaultName,
				RawSQL: trimmed,
			}
			return Statement{Kind: StmtPlatformObject, Platform: po, RawSQL: trimmed}, nil
		}
	}

	return Statement{}, fmt.Errorf("unsupported or unrecognized Doris DDL statement: %q", truncateForError(trimmed, 80))
}

func parseCreateDatabase(stmt string) (*DatabaseDef, error) {
	rest := consumeKeywordCI(stmt, "CREATE")
	isSchema := false
	if after := consumeKeywordCI(rest, "SCHEMA"); after != rest {
		isSchema = true
		rest = after
	} else if after := consumeKeywordCI(rest, "DATABASE"); after != rest {
		rest = after
	} else {
		return nil, fmt.Errorf("expected DATABASE or SCHEMA after CREATE")
	}

	ifNotExists := false
	if after := consumePhraseCI(rest, "IF", "NOT", "EXISTS"); after != rest {
		ifNotExists = true
		rest = after
	}

	nameRaw := strings.TrimSpace(rest)
	if nameRaw == "" {
		return nil, fmt.Errorf("missing database name in CREATE DATABASE")
	}
	fullName, catalog, db, short := splitQualifiedName(nameRaw)
	if db != "" && catalog == "" {
		catalog = db
	}
	return &DatabaseDef{
		FullName:        fullName,
		Catalog:         catalog,
		Name:            short,
		IfNotExists:     ifNotExists,
		IsSchemaKeyword: isSchema,
	}, nil
}

func parseDropTable(stmt string) (Statement, error) {
	rest := consumePhraseCI(stmt, "DROP", "TABLE")
	ifExists := false
	if after := consumePhraseCI(rest, "IF", "EXISTS"); after != rest {
		ifExists = true
		rest = after
	}
	nameRaw := strings.TrimSpace(rest)
	if nameRaw == "" {
		return Statement{}, fmt.Errorf("missing table name in DROP TABLE")
	}
	fullName, _, _, _ := splitQualifiedName(nameRaw)
	return Statement{
		Kind:        StmtDropTable,
		TargetTable: fullName,
		IfExists:    ifExists,
		RawSQL:      stmt,
	}, nil
}

func parseBuildIndex(stmt string) (Statement, error) {
	rest := consumePhraseCI(stmt, "BUILD", "INDEX")
	idxName, rest := nextToken(rest)
	if idxName == "" {
		return Statement{}, fmt.Errorf("missing index name in BUILD INDEX")
	}
	afterOn := consumeKeywordCI(rest, "ON")
	if afterOn == rest {
		return Statement{}, fmt.Errorf("expected ON in BUILD INDEX statement: %q", stmt)
	}
	tblRaw := strings.TrimSpace(afterOn)
	if tblRaw == "" {
		return Statement{}, fmt.Errorf("missing table name in BUILD INDEX")
	}
	fullName, _, _, _ := splitQualifiedName(tblRaw)
	return Statement{
		Kind:          StmtBuildIndex,
		TargetTable:   fullName,
		DropIndexName: strings.Trim(idxName, "`"),
		RawSQL:        stmt,
	}, nil
}

func parseAlterTable(stmt string) (Statement, error) {
	rest := consumePhraseCI(stmt, "ALTER", "TABLE")
	rest = strings.TrimSpace(rest)

	// Read qualified table name up to action keyword (ADD, DROP, MODIFY, SET)
	tblToken, afterTbl := readQualifiedTableToken(rest)
	if tblToken == "" {
		return Statement{}, fmt.Errorf("missing table name in ALTER TABLE: %q", stmt)
	}
	targetTable, _, _, _ := splitQualifiedName(tblToken)
	afterTbl = strings.TrimSpace(afterTbl)

	if after := consumePhraseCI(afterTbl, "ADD", "COLUMN"); after != afterTbl {
		after = consumePhraseCI(after, "IF", "NOT", "EXISTS")
		col, isFirst, afterCol, err := parseColumnDefWithPosition(strings.TrimSpace(after))
		if err != nil {
			return Statement{}, fmt.Errorf("ALTER TABLE %s ADD COLUMN: %w", targetTable, err)
		}
		return Statement{
			Kind:        StmtAlterTableAddColumn,
			TargetTable: targetTable,
			Column:      col,
			First:       isFirst,
			AfterColumn: afterCol,
			RawSQL:      stmt,
		}, nil
	}

	if after := consumePhraseCI(afterTbl, "DROP", "COLUMN"); after != afterTbl {
		after = consumePhraseCI(after, "IF", "EXISTS")
		colName := strings.Trim(strings.TrimSpace(after), "`")
		if colName == "" {
			return Statement{}, fmt.Errorf("ALTER TABLE %s DROP COLUMN: missing column name", targetTable)
		}
		return Statement{
			Kind:           StmtAlterTableDropColumn,
			TargetTable:    targetTable,
			DropColumnName: colName,
			RawSQL:         stmt,
		}, nil
	}

	if after := consumePhraseCI(afterTbl, "MODIFY", "COLUMN"); after != afterTbl {
		col, isFirst, afterCol, err := parseColumnDefWithPosition(strings.TrimSpace(after))
		if err != nil {
			return Statement{}, fmt.Errorf("ALTER TABLE %s MODIFY COLUMN: %w", targetTable, err)
		}
		return Statement{
			Kind:        StmtAlterTableModifyColumn,
			TargetTable: targetTable,
			Column:      col,
			First:       isFirst,
			AfterColumn: afterCol,
			RawSQL:      stmt,
		}, nil
	}

	if after := consumePhraseCI(afterTbl, "ADD", "INDEX"); after != afterTbl {
		idx, err := parseIndexDef("INDEX " + strings.TrimSpace(after))
		if err != nil {
			return Statement{}, fmt.Errorf("ALTER TABLE %s ADD INDEX: %w", targetTable, err)
		}
		return Statement{
			Kind:        StmtAlterTableAddIndex,
			TargetTable: targetTable,
			Index:       idx,
			RawSQL:      stmt,
		}, nil
	}

	if after := consumePhraseCI(afterTbl, "DROP", "INDEX"); after != afterTbl {
		after = consumePhraseCI(after, "IF", "EXISTS")
		idxName := strings.Trim(strings.TrimSpace(after), "`")
		if idxName == "" {
			return Statement{}, fmt.Errorf("ALTER TABLE %s DROP INDEX: missing index name", targetTable)
		}
		return Statement{
			Kind:          StmtAlterTableDropIndex,
			TargetTable:   targetTable,
			DropIndexName: idxName,
			RawSQL:        stmt,
		}, nil
	}

	if after := consumeKeywordCI(afterTbl, "SET"); after != afterTbl {
		after = strings.TrimSpace(after)
		openIdx := strings.IndexByte(after, '(')
		if openIdx != 0 {
			return Statement{}, fmt.Errorf("ALTER TABLE %s SET: expected '('", targetTable)
		}
		inside, _, ok := extractBalancedParens(after, 0)
		if !ok {
			return Statement{}, fmt.Errorf("ALTER TABLE %s SET: unbalanced parentheses", targetTable)
		}
		props, err := parsePropertiesBody(inside)
		if err != nil {
			return Statement{}, fmt.Errorf("ALTER TABLE %s SET: %w", targetTable, err)
		}
		return Statement{
			Kind:        StmtAlterTableSetProperties,
			TargetTable: targetTable,
			Properties:  props,
			RawSQL:      stmt,
		}, nil
	}

	return Statement{}, fmt.Errorf("unsupported ALTER TABLE action on %s: %q", targetTable, afterTbl)
}

func parseCreateTable(stmt string) (*TableDef, error) {
	rest := consumePhraseCI(stmt, "CREATE", "TABLE")
	ifNotExists := false
	if after := consumePhraseCI(rest, "IF", "NOT", "EXISTS"); after != rest {
		ifNotExists = true
		rest = after
	}
	rest = strings.TrimSpace(rest)

	openIdx := findFirstOutsideQuotes(rest, '(')
	if openIdx <= 0 {
		return nil, fmt.Errorf("CREATE TABLE missing column list '(' in: %q", truncateForError(stmt, 80))
	}

	nameRaw := strings.TrimSpace(rest[:openIdx])
	fullName, catalog, db, shortName := splitQualifiedName(nameRaw)
	if shortName == "" {
		return nil, fmt.Errorf("CREATE TABLE missing table name")
	}

	bodyContent, afterBodyIdx, ok := extractBalancedParens(rest, openIdx)
	if !ok {
		return nil, fmt.Errorf("CREATE TABLE %s has unbalanced parentheses", fullName)
	}

	items := splitTopLevelComma(bodyContent)
	if len(items) == 0 {
		return nil, fmt.Errorf("CREATE TABLE %s has empty body", fullName)
	}

	tbl := &TableDef{
		FullName:    fullName,
		Catalog:     catalog,
		Database:    db,
		Name:        shortName,
		IfNotExists: ifNotExists,
	}

	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if isInlineIndexItem(item) {
			idx, err := parseIndexDef(item)
			if err != nil {
				return nil, fmt.Errorf("table %s index: %w", fullName, err)
			}
			tbl.Indexes = append(tbl.Indexes, idx)
		} else {
			col, err := parseColumnDef(item)
			if err != nil {
				return nil, fmt.Errorf("table %s column: %w", fullName, err)
			}
			tbl.Columns = append(tbl.Columns, col)
		}
	}

	if len(tbl.Columns) == 0 {
		return nil, fmt.Errorf("table %s defines zero columns", fullName)
	}

	tail := strings.TrimSpace(rest[afterBodyIdx:])
	if err := parseCreateTableTail(tbl, tail); err != nil {
		return nil, fmt.Errorf("table %s clauses: %w", fullName, err)
	}

	return tbl, nil
}

func isInlineIndexItem(item string) bool {
	if strings.HasPrefix(item, "`") {
		return false
	}
	after := consumeKeywordCI(item, "INDEX")
	if after == item {
		return false
	}
	// An inline index has "INDEX <name> (<cols>) USING ..."
	openParen := findFirstOutsideQuotes(after, '(')
	return openParen > 0
}

func parseIndexDef(item string) (*IndexDef, error) {
	rest := consumeKeywordCI(strings.TrimSpace(item), "INDEX")
	idxName, rest := nextToken(rest)
	idxName = strings.Trim(idxName, "`")
	if idxName == "" {
		return nil, fmt.Errorf("missing index name in %q", item)
	}

	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "(") {
		return nil, fmt.Errorf("index %s: expected '(' for indexed columns", idxName)
	}
	colsInside, afterColsIdx, ok := extractBalancedParens(rest, 0)
	if !ok {
		return nil, fmt.Errorf("index %s: unbalanced column parentheses", idxName)
	}
	cols := parseIdentList(colsInside)
	if len(cols) == 0 {
		return nil, fmt.Errorf("index %s: empty column list", idxName)
	}

	rest = strings.TrimSpace(rest[afterColsIdx:])
	usingType := "INVERTED"
	if afterUsing := consumeKeywordCI(rest, "USING"); afterUsing != rest {
		uTok, rem := nextToken(afterUsing)
		if uTok == "" {
			return nil, fmt.Errorf("index %s: missing index type after USING", idxName)
		}
		usingType = strings.ToUpper(uTok)
		rest = strings.TrimSpace(rem)
	}

	var props []Property
	var comment string
	for rest != "" {
		if afterProp := consumeKeywordCI(rest, "PROPERTIES"); afterProp != rest {
			afterProp = strings.TrimSpace(afterProp)
			if !strings.HasPrefix(afterProp, "(") {
				return nil, fmt.Errorf("index %s: expected '(' after PROPERTIES", idxName)
			}
			propInside, afterPropIdx, ok := extractBalancedParens(afterProp, 0)
			if !ok {
				return nil, fmt.Errorf("index %s: unbalanced PROPERTIES parentheses", idxName)
			}
			parsedProps, err := parsePropertiesBody(propInside)
			if err != nil {
				return nil, fmt.Errorf("index %s properties: %w", idxName, err)
			}
			props = append(props, parsedProps...)
			rest = strings.TrimSpace(afterProp[afterPropIdx:])
			continue
		}
		if afterComment := consumeKeywordCI(rest, "COMMENT"); afterComment != rest {
			val, rem, err := consumeQuotedOrToken(afterComment)
			if err != nil {
				return nil, fmt.Errorf("index %s COMMENT: %w", idxName, err)
			}
			comment = val
			rest = strings.TrimSpace(rem)
			continue
		}
		return nil, fmt.Errorf("index %s: unexpected trailing clause %q", idxName, rest)
	}

	return &IndexDef{
		Name:       idxName,
		Columns:    cols,
		Using:      usingType,
		Properties: props,
		Comment:    comment,
	}, nil
}

func parseColumnDef(item string) (*ColumnDef, error) {
	col, _, _, err := parseColumnDefWithPosition(item)
	return col, err
}

func parseColumnDefWithPosition(item string) (*ColumnDef, bool, string, error) {
	colName, rest, err := consumeIdent(item)
	if err != nil || colName == "" {
		return nil, false, "", fmt.Errorf("invalid column name in %q", item)
	}

	sqlType, rest, err := consumeSQLType(rest)
	if err != nil || sqlType == "" {
		return nil, false, "", fmt.Errorf("column %s: invalid SQL type in %q: %v", colName, item, err)
	}

	col := &ColumnDef{
		Name:     colName,
		Type:     NormalizeType(sqlType),
		Nullable: true,
	}
	var isFirst bool
	var afterColumn string

	rest = strings.TrimSpace(rest)
	for rest != "" {
		if after := consumePhraseCI(rest, "NOT", "NULL"); after != rest {
			col.Nullable = false
			rest = strings.TrimSpace(after)
			continue
		}
		if after := consumeKeywordCI(rest, "NULL"); after != rest {
			col.Nullable = true
			rest = strings.TrimSpace(after)
			continue
		}
		if after := consumeKeywordCI(rest, "DEFAULT"); after != rest {
			val, rem, err := consumeQuotedOrToken(after)
			if err != nil {
				return nil, false, "", fmt.Errorf("column %s DEFAULT: %w", colName, err)
			}
			col.HasDefault = true
			col.Default = val
			rest = strings.TrimSpace(rem)
			continue
		}
		if after := consumeKeywordCI(rest, "COMMENT"); after != rest {
			val, rem, err := consumeQuotedOrToken(after)
			if err != nil {
				return nil, false, "", fmt.Errorf("column %s COMMENT: %w", colName, err)
			}
			col.Comment = val
			rest = strings.TrimSpace(rem)
			continue
		}
		if after := consumeKeywordCI(rest, "AUTO_INCREMENT"); after != rest {
			rest = strings.TrimSpace(after)
			continue
		}
		if after := consumeKeywordCI(rest, "FIRST"); after != rest {
			isFirst = true
			rest = strings.TrimSpace(after)
			continue
		}
		if after := consumeKeywordCI(rest, "AFTER"); after != rest {
			refCol, rem, err := consumeIdent(after)
			if err != nil || refCol == "" {
				return nil, false, "", fmt.Errorf("column %s AFTER: invalid column name in %q", colName, after)
			}
			afterColumn = refCol
			rest = strings.TrimSpace(rem)
			continue
		}
		// Check aggregate keywords for AGGREGATE KEY tables
		tok, rem := nextToken(rest)
		switch strings.ToUpper(tok) {
		case "SUM", "REPLACE", "MAX", "MIN", "REPLACE_IF_NOT_NULL", "HLL_UNION", "BITMAP_UNION":
			col.AggType = strings.ToUpper(tok)
			rest = strings.TrimSpace(rem)
			continue
		}
		return nil, false, "", fmt.Errorf("column %s: unexpected token %q in %q", colName, tok, item)
	}

	return col, isFirst, afterColumn, nil
}

func parseCreateTableTail(tbl *TableDef, tail string) error {
	rest := strings.TrimSpace(tail)
	for rest != "" {
		// ENGINE = OLAP / iceberg
		if after := consumeKeywordCI(rest, "ENGINE"); after != rest {
			after = strings.TrimSpace(after)
			after = strings.TrimPrefix(after, "=")
			engTok, rem := nextToken(after)
			tbl.Engine = engTok
			rest = strings.TrimSpace(rem)
			continue
		}

		// UNIQUE KEY(...) / DUPLICATE KEY(...) / AGGREGATE KEY(...)
		if keyKind, afterKey, matched := consumeKeyClausePrefix(rest); matched {
			afterKey = strings.TrimSpace(afterKey)
			if !strings.HasPrefix(afterKey, "(") {
				return fmt.Errorf("expected '(' after %s KEY", keyKind)
			}
			inside, afterIdx, ok := extractBalancedParens(afterKey, 0)
			if !ok {
				return fmt.Errorf("unbalanced parentheses in %s KEY", keyKind)
			}
			cols := parseIdentList(inside)
			if len(cols) == 0 {
				return fmt.Errorf("empty column list in %s KEY", keyKind)
			}
			tbl.Key = &KeyClause{
				Kind:    keyKind,
				Columns: cols,
			}
			rest = strings.TrimSpace(afterKey[afterIdx:])
			continue
		}

		// COMMENT "..."
		if after := consumeKeywordCI(rest, "COMMENT"); after != rest {
			val, rem, err := consumeQuotedOrToken(after)
			if err != nil {
				return fmt.Errorf("COMMENT: %w", err)
			}
			tbl.Comment = val
			rest = strings.TrimSpace(rem)
			continue
		}

		// PARTITION BY ...
		if after := consumePhraseCI(rest, "PARTITION", "BY"); after != rest {
			after = strings.TrimSpace(after)
			partKind := "EXPR"
			if afterList := consumeKeywordCI(after, "LIST"); afterList != after {
				partKind = "LIST"
				after = strings.TrimSpace(afterList)
			} else if afterRange := consumeKeywordCI(after, "RANGE"); afterRange != after {
				partKind = "RANGE"
				after = strings.TrimSpace(afterRange)
			}

			if !strings.HasPrefix(after, "(") {
				return fmt.Errorf("expected '(' in PARTITION BY clause")
			}
			inside, afterIdx, ok := extractBalancedParens(after, 0)
			if !ok {
				return fmt.Errorf("unbalanced parentheses in PARTITION BY clause")
			}
			rest = strings.TrimSpace(after[afterIdx:])

			pc := &PartitionClause{Kind: partKind}
			if partKind == "LIST" || partKind == "RANGE" {
				pc.Columns = parseIdentList(inside)
				if strings.HasPrefix(rest, "(") {
					partBody, afterPartBodyIdx, ok := extractBalancedParens(rest, 0)
					if !ok {
						return fmt.Errorf("unbalanced partition body parentheses in PARTITION BY %s", partKind)
					}
					pc.PartitionsRaw = "(" + strings.TrimSpace(partBody) + ")"
					rest = strings.TrimSpace(rest[afterPartBodyIdx:])
				} else {
					pc.PartitionsRaw = "()"
				}
			} else {
				pc.RawExpr = strings.TrimSpace(inside)
			}
			tbl.Partition = pc
			continue
		}

		// ORDER BY (...)
		if after := consumePhraseCI(rest, "ORDER", "BY"); after != rest {
			after = strings.TrimSpace(after)
			if !strings.HasPrefix(after, "(") {
				return fmt.Errorf("expected '(' after ORDER BY")
			}
			inside, afterIdx, ok := extractBalancedParens(after, 0)
			if !ok {
				return fmt.Errorf("unbalanced parentheses in ORDER BY")
			}
			tbl.OrderBy = parseIdentList(inside)
			rest = strings.TrimSpace(after[afterIdx:])
			continue
		}

		// DISTRIBUTED BY HASH(...) BUCKETS ... or DISTRIBUTED BY RANDOM BUCKETS ...
		if after := consumePhraseCI(rest, "DISTRIBUTED", "BY"); after != rest {
			after = strings.TrimSpace(after)
			if afterHash := consumeKeywordCI(after, "HASH"); afterHash != after {
				afterHash = strings.TrimSpace(afterHash)
				if !strings.HasPrefix(afterHash, "(") {
					return fmt.Errorf("expected '(' after DISTRIBUTED BY HASH")
				}
				inside, afterIdx, ok := extractBalancedParens(afterHash, 0)
				if !ok {
					return fmt.Errorf("unbalanced parentheses in DISTRIBUTED BY HASH")
				}
				cols := parseIdentList(inside)
				rem := strings.TrimSpace(afterHash[afterIdx:])
				buckets := "AUTO"
				if afterBuckets := consumeKeywordCI(rem, "BUCKETS"); afterBuckets != rem {
					bTok, remAfterB := nextToken(afterBuckets)
					if bTok == "" {
						return fmt.Errorf("missing bucket count after BUCKETS")
					}
					buckets = bTok
					rem = strings.TrimSpace(remAfterB)
				}
				tbl.Distribution = &DistributionClause{
					Kind:    "HASH",
					Columns: cols,
					Buckets: buckets,
				}
				rest = rem
				continue
			}
			if afterRand := consumeKeywordCI(after, "RANDOM"); afterRand != after {
				rem := strings.TrimSpace(afterRand)
				buckets := "AUTO"
				if afterBuckets := consumeKeywordCI(rem, "BUCKETS"); afterBuckets != rem {
					bTok, remAfterB := nextToken(afterBuckets)
					if bTok == "" {
						return fmt.Errorf("missing bucket count after BUCKETS")
					}
					buckets = bTok
					rem = strings.TrimSpace(remAfterB)
				}
				tbl.Distribution = &DistributionClause{
					Kind:    "RANDOM",
					Buckets: buckets,
				}
				rest = rem
				continue
			}
			return fmt.Errorf("unsupported DISTRIBUTED BY clause: %q", after)
		}

		// PROPERTIES (...)
		if after := consumeKeywordCI(rest, "PROPERTIES"); after != rest {
			after = strings.TrimSpace(after)
			if !strings.HasPrefix(after, "(") {
				return fmt.Errorf("expected '(' after PROPERTIES")
			}
			inside, afterIdx, ok := extractBalancedParens(after, 0)
			if !ok {
				return fmt.Errorf("unbalanced parentheses in PROPERTIES")
			}
			props, err := parsePropertiesBody(inside)
			if err != nil {
				return err
			}
			tbl.Properties = append(tbl.Properties, props...)
			rest = strings.TrimSpace(after[afterIdx:])
			continue
		}

		return fmt.Errorf("unexpected clause in CREATE TABLE tail: %q", truncateForError(rest, 60))
	}
	return nil
}

func consumeKeyClausePrefix(s string) (KeyKind, string, bool) {
	if after := consumePhraseCI(s, "UNIQUE", "KEY"); after != s {
		return KeyUnique, after, true
	}
	if after := consumePhraseCI(s, "DUPLICATE", "KEY"); after != s {
		return KeyDuplicate, after, true
	}
	if after := consumePhraseCI(s, "AGGREGATE", "KEY"); after != s {
		return KeyAggregate, after, true
	}
	return "", s, false
}

func parseCreateView(stmt string, orReplace bool) (*ViewDef, error) {
	var rest string
	if orReplace {
		rest = consumePhraseCI(stmt, "CREATE", "OR", "REPLACE", "VIEW")
	} else {
		rest = consumePhraseCI(stmt, "CREATE", "VIEW")
	}
	rest = consumePhraseCI(rest, "IF", "NOT", "EXISTS")
	rest = strings.TrimSpace(rest)

	// Read view name before AS
	nameTok, rem := readQualifiedTableToken(rest)
	if nameTok == "" {
		return nil, fmt.Errorf("missing view name in CREATE VIEW")
	}
	fullName, catalog, db, short := splitQualifiedName(nameTok)

	afterAs := consumeKeywordCI(rem, "AS")
	if afterAs == rem {
		return nil, fmt.Errorf("CREATE VIEW %s missing AS keyword", fullName)
	}
	asSQL := strings.TrimSpace(afterAs)
	if asSQL == "" {
		return nil, fmt.Errorf("CREATE VIEW %s has empty SELECT query", fullName)
	}

	return &ViewDef{
		FullName:  fullName,
		Catalog:   catalog,
		Database:  db,
		Name:      short,
		OrReplace: orReplace,
		AsSQL:     asSQL,
		RawSQL:    stmt,
	}, nil
}

func parsePlatformCreate(stmt, kind, prefixPhrase string) (*PlatformObject, error) {
	words := strings.Fields(prefixPhrase)
	rest := consumePhraseCI(stmt, words...)
	ifNotExists := false
	if after := consumePhraseCI(rest, "IF", "NOT", "EXISTS"); after != rest {
		ifNotExists = true
		rest = after
	}
	nameTok, rem := nextToken(rest)
	nameTok = strings.Trim(nameTok, "`")
	if nameTok == "" {
		return nil, fmt.Errorf("missing name in %s", prefixPhrase)
	}
	rem = strings.TrimSpace(rem)
	var props []Property
	if afterProp := consumeKeywordCI(rem, "PROPERTIES"); afterProp != rem {
		afterProp = strings.TrimSpace(afterProp)
		if !strings.HasPrefix(afterProp, "(") {
			return nil, fmt.Errorf("%s %s: expected '(' after PROPERTIES", prefixPhrase, nameTok)
		}
		inside, _, ok := extractBalancedParens(afterProp, 0)
		if !ok {
			return nil, fmt.Errorf("%s %s: unbalanced PROPERTIES parentheses", prefixPhrase, nameTok)
		}
		var err error
		props, err = parsePropertiesBody(inside)
		if err != nil {
			return nil, err
		}
	}
	return &PlatformObject{
		Kind:        kind,
		Name:        nameTok,
		IfNotExists: ifNotExists,
		Properties:  props,
		RawSQL:      stmt,
	}, nil
}

func parsePlatformAlterWorkloadGroup(stmt string) (*PlatformObject, error) {
	rest := consumePhraseCI(stmt, "ALTER", "WORKLOAD", "GROUP")
	nameTok, rem := nextToken(rest)
	nameTok = strings.Trim(nameTok, "`")
	if nameTok == "" {
		return nil, fmt.Errorf("missing workload group name in ALTER WORKLOAD GROUP")
	}
	rem = strings.TrimSpace(rem)
	var props []Property
	if afterProp := consumeKeywordCI(rem, "PROPERTIES"); afterProp != rem {
		afterProp = strings.TrimSpace(afterProp)
		if !strings.HasPrefix(afterProp, "(") {
			return nil, fmt.Errorf("ALTER WORKLOAD GROUP %s: expected '(' after PROPERTIES", nameTok)
		}
		inside, _, ok := extractBalancedParens(afterProp, 0)
		if !ok {
			return nil, fmt.Errorf("ALTER WORKLOAD GROUP %s: unbalanced PROPERTIES parentheses", nameTok)
		}
		var err error
		props, err = parsePropertiesBody(inside)
		if err != nil {
			return nil, err
		}
	}
	return &PlatformObject{
		Kind:       "ALTER_WORKLOAD_GROUP",
		Name:       nameTok,
		Properties: props,
		RawSQL:     stmt,
	}, nil
}

func parseWorkloadPolicy(stmt string) (*PlatformObject, error) {
	rest := consumePhraseCI(stmt, "CREATE", "WORKLOAD", "POLICY")
	ifNotExists := false
	if after := consumePhraseCI(rest, "IF", "NOT", "EXISTS"); after != rest {
		ifNotExists = true
		rest = after
	}
	nameTok, _ := nextToken(rest)
	nameTok = strings.Trim(nameTok, "`")
	if nameTok == "" {
		return nil, fmt.Errorf("missing policy name in CREATE WORKLOAD POLICY")
	}
	return &PlatformObject{
		Kind:        "WORKLOAD_POLICY",
		Name:        nameTok,
		IfNotExists: ifNotExists,
		RawSQL:      stmt,
	}, nil
}

func parsePropertiesBody(body string) ([]Property, error) {
	items := splitTopLevelComma(body)
	var props []Property
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		eqIdx := findFirstOutsideQuotes(item, '=')
		if eqIdx <= 0 {
			return nil, fmt.Errorf("invalid property entry (expected \"key\" = \"value\"): %q", item)
		}
		kRaw := strings.TrimSpace(item[:eqIdx])
		vRaw := strings.TrimSpace(item[eqIdx+1:])
		k, _, err := consumeQuotedOrToken(kRaw)
		if err != nil {
			return nil, fmt.Errorf("invalid property key %q: %w", kRaw, err)
		}
		v, _, err := consumeQuotedOrToken(vRaw)
		if err != nil {
			return nil, fmt.Errorf("invalid property value %q: %w", vRaw, err)
		}
		props = append(props, Property{Key: k, Value: v})
	}
	return props, nil
}

func splitQualifiedName(raw string) (fullName, catalog, db, name string) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", "", "", ""
	}
	var parts []string
	var cur strings.Builder
	inBacktick := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '`' {
			inBacktick = !inBacktick
			continue
		}
		if ch == '.' && !inBacktick {
			parts = append(parts, strings.TrimSpace(cur.String()))
			cur.Reset()
			continue
		}
		cur.WriteByte(ch)
	}
	parts = append(parts, strings.TrimSpace(cur.String()))

	switch len(parts) {
	case 3:
		catalog = parts[0]
		db = parts[1]
		name = parts[2]
		fullName = catalog + "." + db + "." + name
	case 2:
		db = parts[0]
		name = parts[1]
		fullName = db + "." + name
	case 1:
		name = parts[0]
		fullName = name
	default:
		name = parts[len(parts)-1]
		fullName = strings.Join(parts, ".")
	}
	return fullName, catalog, db, name
}

func normalizeQualifiedName(raw string) string {
	full, _, _, _ := splitQualifiedName(raw)
	return full
}

func readQualifiedTableToken(s string) (string, string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	inBacktick := false
	braceDepth := 0
	end := 0
	for end < len(s) {
		ch := s[end]
		if ch == '`' {
			inBacktick = !inBacktick
			end++
			continue
		}
		if inBacktick {
			end++
			continue
		}
		if ch == '{' {
			braceDepth++
			end++
			continue
		}
		if ch == '}' && braceDepth > 0 {
			braceDepth--
			end++
			continue
		}
		if braceDepth == 0 && (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '(' || ch == ';') {
			break
		}
		end++
	}
	return s[:end], strings.TrimSpace(s[end:])
}

func consumeIdent(s string) (string, string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", "", fmt.Errorf("unexpected empty identifier")
	}
	if s[0] == '`' {
		end := strings.IndexByte(s[1:], '`')
		if end < 0 {
			return "", "", fmt.Errorf("unterminated backtick identifier in %q", s)
		}
		ident := s[1 : end+1]
		return ident, strings.TrimSpace(s[end+2:]), nil
	}
	tok, rem := nextToken(s)
	return tok, rem, nil
}

func consumeSQLType(s string) (string, string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", "", fmt.Errorf("missing SQL type")
	}
	// Read base type word
	i := 0
	for i < len(s) {
		ch := s[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
			i++
		} else {
			break
		}
	}
	if i == 0 {
		return "", "", fmt.Errorf("invalid type start in %q", s)
	}
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		// Only consume whitespace if followed by '(' or '<'
		j := i
		for j < len(s) && (s[j] == ' ' || s[j] == '\t') {
			j++
		}
		if j < len(s) && (s[j] == '(' || s[j] == '<') {
			i = j
		}
		break
	}

	if i < len(s) && s[i] == '(' {
		_, afterIdx, ok := extractBalancedParens(s, i)
		if !ok {
			return "", "", fmt.Errorf("unbalanced '(' in type %q", s)
		}
		i = afterIdx
	} else if i < len(s) && s[i] == '<' {
		_, afterIdx, ok := extractBalancedAngles(s, i)
		if !ok {
			return "", "", fmt.Errorf("unbalanced '<' in type %q", s)
		}
		i = afterIdx
	}

	return strings.TrimSpace(s[:i]), strings.TrimSpace(s[i:]), nil
}

func extractBalancedParens(s string, openIdx int) (string, int, bool) {
	if openIdx < 0 || openIdx >= len(s) || s[openIdx] != '(' {
		return "", 0, false
	}
	depth := 0
	var inSingle, inDouble, inBacktick bool
	for i := openIdx; i < len(s); i++ {
		ch := s[i]
		if inSingle {
			if ch == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if ch == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return s[openIdx+1 : i], i + 1, true
			}
		}
	}
	return "", 0, false
}

func extractBalancedAngles(s string, openIdx int) (string, int, bool) {
	if openIdx < 0 || openIdx >= len(s) || s[openIdx] != '<' {
		return "", 0, false
	}
	depth := 0
	for i := openIdx; i < len(s); i++ {
		ch := s[i]
		if ch == '<' {
			depth++
		} else if ch == '>' {
			depth--
			if depth == 0 {
				return s[openIdx+1 : i], i + 1, true
			}
		}
	}
	return "", 0, false
}

func splitTopLevelComma(s string) []string {
	var out []string
	var cur strings.Builder
	var inSingle, inDouble, inBacktick bool
	parenDepth := 0
	angleDepth := 0

	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inSingle {
			cur.WriteByte(ch)
			if ch == '\\' && i+1 < len(s) {
				i++
				cur.WriteByte(s[i])
				continue
			}
			if ch == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			cur.WriteByte(ch)
			if ch == '\\' && i+1 < len(s) {
				i++
				cur.WriteByte(s[i])
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			cur.WriteByte(ch)
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		switch ch {
		case '\'':
			inSingle = true
			cur.WriteByte(ch)
		case '"':
			inDouble = true
			cur.WriteByte(ch)
		case '`':
			inBacktick = true
			cur.WriteByte(ch)
		case '(':
			parenDepth++
			cur.WriteByte(ch)
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
			cur.WriteByte(ch)
		case '<':
			angleDepth++
			cur.WriteByte(ch)
		case '>':
			if angleDepth > 0 {
				angleDepth--
			}
			cur.WriteByte(ch)
		case ',':
			if parenDepth == 0 && angleDepth == 0 {
				item := strings.TrimSpace(cur.String())
				if item != "" {
					out = append(out, item)
				}
				cur.Reset()
				continue
			}
			cur.WriteByte(ch)
		default:
			cur.WriteByte(ch)
		}
	}
	tail := strings.TrimSpace(cur.String())
	if tail != "" {
		out = append(out, tail)
	}
	return out
}

func parseIdentList(inside string) []string {
	parts := splitTopLevelComma(inside)
	var cols []string
	for _, p := range parts {
		clean := strings.Trim(strings.TrimSpace(p), "`")
		if clean != "" {
			cols = append(cols, clean)
		}
	}
	return cols
}

func consumeQuotedOrToken(s string) (string, string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", "", fmt.Errorf("expected quoted string or token")
	}
	quote := s[0]
	if quote == '[' {
		closeIdx := strings.IndexByte(s, ']')
		if closeIdx < 0 {
			return "", "", fmt.Errorf("unterminated bracket literal in %q", s)
		}
		inner := strings.TrimSpace(s[1:closeIdx])
		return "[" + inner + "]", strings.TrimSpace(s[closeIdx+1:]), nil
	}
	if quote == '"' || quote == '\'' {
		var b strings.Builder
		for i := 1; i < len(s); i++ {
			ch := s[i]
			if ch == '\\' && i+1 < len(s) {
				i++
				b.WriteByte(s[i])
				continue
			}
			if ch == quote {
				if i+1 < len(s) && s[i+1] == quote {
					i++
					b.WriteByte(s[i])
					continue
				}
				return b.String(), strings.TrimSpace(s[i+1:]), nil
			}
			b.WriteByte(ch)
		}
		return "", "", fmt.Errorf("unterminated string literal in %q", s)
	}
	tok, rem := nextToken(s)
	return tok, rem, nil
}

func findFirstOutsideQuotes(s string, target byte) int {
	var inSingle, inDouble, inBacktick bool
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inSingle {
			if ch == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if ch == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		default:
			if ch == target {
				return i
			}
		}
	}
	return -1
}

func scanLeadingKeywords(s string, maxWords int) []string {
	var words []string
	rem := strings.TrimSpace(s)
	for len(words) < maxWords && rem != "" {
		tok, next := nextToken(rem)
		if tok == "" {
			break
		}
		words = append(words, tok)
		rem = next
	}
	return words
}

func nextToken(s string) (string, string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	i := 0
	inBacktick := false
	for i < len(s) {
		ch := s[i]
		if ch == '`' {
			inBacktick = !inBacktick
			i++
			continue
		}
		if !inBacktick && (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '(' || ch == ')' || ch == ',' || ch == ';') {
			break
		}
		i++
	}
	return s[:i], strings.TrimSpace(s[i:])
}

func consumeKeywordCI(s, kw string) string {
	trimmed := strings.TrimSpace(s)
	if len(trimmed) < len(kw) {
		return s
	}
	if !strings.EqualFold(trimmed[:len(kw)], kw) {
		return s
	}
	if len(trimmed) > len(kw) {
		next := trimmed[len(kw)]
		if (next >= 'a' && next <= 'z') || (next >= 'A' && next <= 'Z') || (next >= '0' && next <= '9') || next == '_' {
			return s
		}
	}
	return strings.TrimSpace(trimmed[len(kw):])
}

func consumePhraseCI(s string, kws ...string) string {
	cur := s
	for _, kw := range kws {
		next := consumeKeywordCI(cur, kw)
		if next == cur {
			return s
		}
		cur = next
	}
	return cur
}

func truncateForError(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
