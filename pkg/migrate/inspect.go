package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/protocgen/atlas-doris/pkg/doris"
)

// InspectLiveSchema connects to a live Doris / StarRocks / MySQL-protocol database URL
// (doris://..., starrocks://..., or mysql://...) and inspects its schema into a *doris.Schema.
func InspectLiveSchema(ctx context.Context, rawURL string) (*doris.Schema, error) {
	dsn, dbName, err := ParseDorisDSN(rawURL)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(DefaultSQLDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open doris connection: %w", err)
	}
	defer func() { _ = db.Close() }()
	s, err := InspectSchemaFromDB(ctx, db, dbName)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(rawURL)), "starrocks://") {
		for _, t := range s.Tables {
			if !t.IsIcebergTable() && t.Dialect == "" {
				t.Dialect = "starrocks"
			}
		}
	}
	return s, nil
}

// InspectSchemaFromDB inspects the tables and views in defaultDB from an open *sql.DB
// connection using SHOW FULL TABLES / SHOW CREATE TABLE / SHOW CREATE VIEW, with fallback
// to information_schema.columns + SHOW INDEX FROM when SHOW CREATE TABLE is unavailable.
func InspectSchemaFromDB(ctx context.Context, db *sql.DB, defaultDB string) (*doris.Schema, error) {
	if db == nil {
		return nil, fmt.Errorf("cannot inspect nil database connection")
	}

	s := doris.NewSchema()
	catalog := "internal"
	shortDB := strings.TrimSpace(defaultDB)
	if shortDB != "" {
		parts := strings.Split(shortDB, ".")
		if len(parts) == 2 {
			catalog = parts[0]
			shortDB = parts[1]
		}
		if !isSafeQualifiedIdentifier(shortDB) || !isSafeQualifiedIdentifier(catalog) {
			return nil, fmt.Errorf("invalid database identifier %q", defaultDB)
		}
		dbDef := &doris.DatabaseDef{
			Catalog:     catalog,
			Name:        shortDB,
			IfNotExists: true,
		}
		dbDef.FullName = dbDef.CanonicalName()
		s.Databases = append(s.Databases, dbDef)
	}

	showTablesQuery := "SHOW FULL TABLES;"
	if shortDB != "" {
		if catalog != "" && !strings.EqualFold(catalog, "internal") && !strings.EqualFold(catalog, "default_catalog") {
			showTablesQuery = fmt.Sprintf("SHOW FULL TABLES FROM %s.%s;", catalog, shortDB)
		} else {
			showTablesQuery = fmt.Sprintf("SHOW FULL TABLES FROM %s;", shortDB)
		}
	}

	rows, err := db.QueryContext(ctx, showTablesQuery)
	if err != nil {
		return nil, fmt.Errorf("query tables (%s): %w", showTablesQuery, err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	type tableEntry struct {
		name      string
		tableType string
	}
	var entries []tableEntry
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		if len(vals) == 0 || strings.TrimSpace(vals[0].String) == "" {
			continue
		}
		tName := strings.TrimSpace(vals[0].String)
		if strings.EqualFold(tName, RevisionsTableName) {
			continue
		}
		tType := "BASE TABLE"
		if len(vals) >= 2 && strings.TrimSpace(vals[1].String) != "" {
			tType = strings.TrimSpace(vals[1].String)
		}
		entries = append(entries, tableEntry{name: tName, tableType: tType})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if !isSafeQualifiedIdentifier(entry.name) {
			continue
		}
		qualified := entry.name
		if shortDB != "" {
			if catalog != "" {
				qualified = fmt.Sprintf("%s.%s.%s", catalog, shortDB, entry.name)
			} else {
				qualified = fmt.Sprintf("%s.%s", shortDB, entry.name)
			}
		}

		if strings.EqualFold(entry.tableType, "VIEW") {
			vwDDL, err := queryCreateStatement(ctx, db, fmt.Sprintf("SHOW CREATE VIEW %s;", qualified))
			if err != nil {
				return nil, fmt.Errorf("SHOW CREATE VIEW %s: %w", qualified, err)
			}
			parsed, err := doris.ParseSchema(vwDDL)
			if err != nil {
				return nil, fmt.Errorf("parse SHOW CREATE VIEW %s: %w", qualified, err)
			}
			for _, v := range parsed.Views {
				if v.Database == "" && shortDB != "" {
					v.Database = shortDB
					v.RawSQL = ""
				}
				if v.Catalog == "" && catalog != "" {
					v.Catalog = catalog
					v.RawSQL = ""
				}
				v.FullName = v.CanonicalName()
				s.Views = append(s.Views, v)
			}
			continue
		}

		tblDDL, err := queryCreateStatement(ctx, db, fmt.Sprintf("SHOW CREATE TABLE %s;", qualified))
		if err == nil && strings.TrimSpace(tblDDL) != "" {
			parsed, parseErr := doris.ParseSchema(tblDDL)
			if parseErr == nil && len(parsed.Tables) > 0 {
				for _, t := range parsed.Tables {
					if t.Database == "" && shortDB != "" {
						t.Database = shortDB
					}
					if t.Catalog == "" && catalog != "" {
						t.Catalog = catalog
					}
					if strings.EqualFold(catalog, "default_catalog") && !t.IsIcebergTable() && t.Dialect == "" {
						t.Dialect = "starrocks"
					}
					t.FullName = t.CanonicalName()
					t.IfNotExists = true
					s.Tables = append(s.Tables, t)
				}
				continue
			}
		}

		// Fallback to information_schema.columns + SHOW INDEX FROM
		tblDef, infoErr := InspectTableFromInfoSchema(ctx, db, catalog, shortDB, entry.name)
		if infoErr != nil {
			if err != nil {
				return nil, fmt.Errorf("inspect table %s: %w", qualified, err)
			}
			return nil, infoErr
		}
		s.Tables = append(s.Tables, tblDef)
	}

	return s, nil
}

func queryCreateStatement(ctx context.Context, db *sql.DB, query string) (string, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if len(cols) < 2 {
		return "", fmt.Errorf("unexpected column count %d from %s", len(cols), query)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", err
		}
		return "", fmt.Errorf("no rows returned from %s", query)
	}
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return "", err
	}
	return vals[1].String, rows.Err()
}

// InspectTableFromInfoSchema inspects a single Doris/Iceberg table via information_schema.columns
// and SHOW INDEX FROM when SHOW CREATE TABLE is unavailable.
func InspectTableFromInfoSchema(ctx context.Context, db *sql.DB, catalog, schemaName, tableName string) (*doris.TableDef, error) {
	if catalog == "" {
		catalog = "internal"
	}
	if !isSafeQualifiedIdentifier(catalog) || !isSafeQualifiedIdentifier(schemaName) || !isSafeQualifiedIdentifier(tableName) {
		return nil, fmt.Errorf("invalid table identifier %s.%s.%s", catalog, schemaName, tableName)
	}
	tbl := &doris.TableDef{
		Catalog:     catalog,
		Database:    schemaName,
		Name:        tableName,
		IfNotExists: true,
	}
	if strings.EqualFold(catalog, "default_catalog") {
		tbl.Dialect = "starrocks"
	}
	tbl.FullName = tbl.CanonicalName()

	colQuery := fmt.Sprintf(
		"SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, COLUMN_COMMENT "+
			"FROM `%s`.`information_schema`.`columns` "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "+
			"ORDER BY ORDINAL_POSITION ASC",
		catalog,
	)
	rows, err := db.QueryContext(ctx, colQuery, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("inspect columns for %s: %w", tbl.FullName, err)
	}
	defer func() { _ = rows.Close() }()

	var keyCols []string
	var keyKind doris.KeyKind
	for rows.Next() {
		var (
			name, colType, isNullable, colKey string
			colDefault, colComment            sql.NullString
		)
		if err := rows.Scan(&name, &colType, &isNullable, &colKey, &colDefault, &colComment); err != nil {
			return nil, err
		}
		normType := doris.NormalizeType(colType)

		col := &doris.ColumnDef{
			Name:       name,
			Type:       normType,
			Nullable:   strings.EqualFold(isNullable, "YES"),
			HasDefault: colDefault.Valid,
			Default:    colDefault.String,
			Comment:    colComment.String,
		}
		tbl.Columns = append(tbl.Columns, col)

		switch strings.ToUpper(strings.TrimSpace(colKey)) {
		case "PRI":
			keyKind = doris.KeyPrimary
			if tbl.Dialect == "" {
				tbl.Dialect = "starrocks"
			}
			keyCols = append(keyCols, name)
		case "UNI":
			keyKind = doris.KeyUnique
			keyCols = append(keyCols, name)
		case "DUP":
			if keyKind == "" {
				keyKind = doris.KeyDuplicate
			}
			keyCols = append(keyCols, name)
		case "AGG":
			if keyKind == "" {
				keyKind = doris.KeyAggregate
			}
			keyCols = append(keyCols, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(tbl.Columns) == 0 {
		return nil, fmt.Errorf("table not found: %s", tbl.FullName)
	}
	if len(keyCols) > 0 {
		tbl.Key = &doris.KeyClause{
			Kind:    keyKind,
			Columns: keyCols,
		}
	}
	return tbl, nil
}
