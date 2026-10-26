package doris

import (
	"strconv"
	"strings"
)

// DiffOptions configures schema diff computation and lint enforcement.
type DiffOptions struct {
	LintOptions
	SkipLint bool
}

// Diff computes the deterministic migration statements required to evolve
// current Schema into desired Schema, enforcing standard non-destructive lint rules.
func Diff(current, desired *Schema) ([]Statement, error) {
	return DiffWithOptions(current, desired, DiffOptions{})
}

// DiffWithOptions computes the deterministic migration statements required to
// evolve current Schema into desired Schema with custom DiffOptions.
func DiffWithOptions(current, desired *Schema, opts DiffOptions) ([]Statement, error) {
	if current == nil {
		current = NewSchema()
	}
	if desired == nil {
		desired = NewSchema()
	}

	if !opts.SkipLint {
		violations := LintSchemaEvolution(current, desired, opts.LintOptions)
		if len(violations) > 0 {
			return nil, ViolationsToError(violations)
		}
	}

	var stmts []Statement

	// 1. Platform objects (storage vaults, workload groups, catalogs, resources).
	stmts = append(stmts, diffPlatformObjects(current.PlatformObjects, desired.PlatformObjects)...)

	// 2. Missing databases.
	for _, db := range desired.Databases {
		if _, exists := current.DatabaseByName(db.CanonicalName()); !exists && !schemaHasDatabaseObjects(current, db) {
			dbCopy := db.Clone()
			dbCopy.IfNotExists = true
			stmts = append(stmts, Statement{
				Kind:     StmtCreateDatabase,
				Database: dbCopy,
			})
		}
	}

	// 3. Tables (create new, alter existing, or drop removed when AllowDestructive is set).
	currentForDesired, desiredForCurrent := pairSchemaTables(current.Tables, desired.Tables)

	for _, dt := range desired.Tables {
		ct, existed := currentForDesired[dt]
		if !existed {
			tblCopy := dt.Clone()
			tblCopy.IfNotExists = true
			stmts = append(stmts, Statement{
				Kind:  StmtCreateTable,
				Table: tblCopy,
			})
			continue
		}
		stmts = append(stmts, diffTable(ct, dt, opts)...)
	}

	if opts.AllowDestructive {
		for _, ct := range current.Tables {
			if _, ok := desiredForCurrent[ct]; !ok {
				stmts = append(stmts, Statement{
					Kind:        StmtDropTable,
					TargetTable: ct.CanonicalName(),
					IfExists:    true,
				})
			}
		}
	}

	// 4. Views (new or changed).
	for _, dv := range desired.Views {
		cv, existed := current.ViewByName(dv.CanonicalName())
		if !existed || normalizeViewSQL(cv) != normalizeViewSQL(dv) {
			vCopy := dv.Clone()
			vCopy.OrReplace = true
			stmts = append(stmts, Statement{
				Kind: StmtCreateOrReplaceView,
				View: vCopy,
			})
		}
	}

	return stmts, nil
}

func diffTable(current, desired *TableDef, opts DiffOptions) []Statement {
	var stmts []Statement
	targetName := desired.CanonicalName()
	if current != nil && current.Database != "" && isDefaultDorisCatalog(current.Catalog) && isDefaultDorisCatalog(desired.Catalog) {
		targetName = current.CanonicalName()
	}

	currentColByName := make(map[string]*ColumnDef, len(current.Columns))
	for _, c := range current.Columns {
		currentColByName[c.Name] = c
	}
	desiredColByName := make(map[string]*ColumnDef, len(desired.Columns))
	for _, c := range desired.Columns {
		desiredColByName[c.Name] = c
	}

	// 1. Dropped columns (if AllowDestructive).
	evolvingCols := make([]string, 0, len(current.Columns)+len(desired.Columns))
	for _, cc := range current.Columns {
		if opts.AllowDestructive {
			if _, ok := desiredColByName[cc.Name]; !ok {
				stmts = append(stmts, Statement{
					Kind:           StmtAlterTableDropColumn,
					TargetTable:    targetName,
					DropColumnName: cc.Name,
				})
				continue
			}
		}
		evolvingCols = append(evolvingCols, cc.Name)
	}

	// 2. Added and modified columns in desired order.
	isIceberg := desired.IsIcebergTable()
	isIcebergV3 := isIceberg && strings.TrimSpace(desired.PropertyMap()["format-version"]) == "3"
	for i, dc := range desired.Columns {
		cc, existed := currentColByName[dc.Name]
		if !existed {
			colToAdd := dc.Clone()
			if isIceberg && !isIcebergV3 {
				colToAdd.Nullable = true
				colToAdd.HasDefault = false
				colToAdd.Default = ""
			}
			var isFirst bool
			var afterCol string
			if i == 0 {
				if len(evolvingCols) > 0 {
					isFirst = true
				}
				evolvingCols = append([]string{dc.Name}, evolvingCols...)
			} else {
				prevName := desired.Columns[i-1].Name
				if len(evolvingCols) > 0 && evolvingCols[len(evolvingCols)-1] == prevName {
					evolvingCols = append(evolvingCols, dc.Name)
				} else {
					afterCol = prevName
					evolvingCols = insertAfterCol(evolvingCols, prevName, dc.Name)
				}
			}
			stmts = append(stmts, Statement{
				Kind:        StmtAlterTableAddColumn,
				TargetTable: targetName,
				Column:      colToAdd,
				First:       isFirst,
				AfterColumn: afterCol,
			})
			continue
		}
		targetCol := dc.Clone()
		if isIceberg {
			// Iceberg schema evolution cannot tighten a nullable column to NOT NULL
			// ("Cannot change optional to required") or modify column defaults
			// ("Modifying default values is not supported for Iceberg columns").
			// Preserve current nullability (when nullable) and default attributes so a
			// column added via Iceberg v2 ADD COLUMN does not trigger a spurious MODIFY COLUMN
			// on subsequent diffs.
			if cc.Nullable {
				targetCol.Nullable = true
			}
			targetCol.HasDefault = cc.HasDefault
			targetCol.Default = cc.Default
		}
		if !cc.Equal(targetCol) {
			stmts = append(stmts, Statement{
				Kind:        StmtAlterTableModifyColumn,
				TargetTable: targetName,
				Column:      targetCol,
			})
		}
	}

	// 3. Indexes: dropped or modified indexes first, then added/modified indexes.
	currentIdxByName := make(map[string]*IndexDef, len(current.Indexes))
	for _, idx := range current.Indexes {
		currentIdxByName[idx.Name] = idx
	}
	desiredIdxByName := make(map[string]*IndexDef, len(desired.Indexes))
	for _, idx := range desired.Indexes {
		desiredIdxByName[idx.Name] = idx
	}

	for _, cIdx := range current.Indexes {
		dIdx, ok := desiredIdxByName[cIdx.Name]
		if !ok || !cIdx.Equal(dIdx) {
			stmts = append(stmts, Statement{
				Kind:          StmtAlterTableDropIndex,
				TargetTable:   targetName,
				DropIndexName: cIdx.Name,
			})
		}
	}

	for _, dIdx := range desired.Indexes {
		cIdx, ok := currentIdxByName[dIdx.Name]
		if !ok || !cIdx.Equal(dIdx) {
			stmts = append(stmts, Statement{
				Kind:        StmtAlterTableAddIndex,
				TargetTable: targetName,
				Index:       dIdx.Clone(),
			})
			if desired.IsDorisInternalTable() && shouldBuildIndexOnline(dIdx.Using) {
				stmts = append(stmts, Statement{
					Kind:          StmtBuildIndex,
					TargetTable:   targetName,
					DropIndexName: dIdx.Name,
				})
			}
		}
	}

	// 4. Changed mutable table properties.
	curProps := current.PropertyMap()
	for _, dp := range desired.Properties {
		if !isMutableTableProperty(dp.Key) {
			continue
		}
		if tablePropertyMatches(curProps, dp.Key, dp.Value) {
			continue
		}
		stmts = append(stmts, Statement{
			Kind:        StmtAlterTableSetProperties,
			TargetTable: targetName,
			Properties:  []Property{{Key: dp.Key, Value: dp.Value}},
		})
	}

	return stmts
}

func insertAfterCol(cols []string, afterName, newName string) []string {
	for i, c := range cols {
		if c == afterName {
			out := make([]string, 0, len(cols)+1)
			out = append(out, cols[:i+1]...)
			out = append(out, newName)
			out = append(out, cols[i+1:]...)
			return out
		}
	}
	return append(cols, newName)
}

func schemaHasDatabaseObjects(s *Schema, db *DatabaseDef) bool {
	if s == nil || db == nil {
		return false
	}
	targetKey := db.LookupKey()
	if targetKey == "" {
		return false
	}
	for _, t := range s.Tables {
		if t != nil && t.Database != "" {
			tblDB := DatabaseDef{Catalog: t.Catalog, Name: t.Database}
			if tblDB.LookupKey() == targetKey {
				return true
			}
		}
	}
	for _, v := range s.Views {
		if v != nil && v.Database != "" {
			vwDB := DatabaseDef{Catalog: v.Catalog, Name: v.Database}
			if vwDB.LookupKey() == targetKey {
				return true
			}
		}
	}
	return false
}

func tablePropertyMatches(curProps map[string]string, key, desiredVal string) bool {
	if curVal, exists := curProps[key]; exists {
		return curVal == desiredVal
	}
	// Live Doris SHOW CREATE TABLE renders "replication_num" = "N" as
	// "replication_allocation" = "tag.location.default: N".
	if key == "replication_num" {
		if alloc, ok := curProps["replication_allocation"]; ok {
			if count, ok := replicationCountFromAllocation(alloc); ok && count == strings.TrimSpace(desiredVal) {
				return true
			}
		}
	}
	return false
}

func replicationCountFromAllocation(alloc string) (string, bool) {
	parts := strings.Split(alloc, ",")
	total := 0
	found := false
	for _, p := range parts {
		colonIdx := strings.LastIndexByte(p, ':')
		if colonIdx < 0 {
			return "", false
		}
		n, err := strconv.Atoi(strings.TrimSpace(p[colonIdx+1:]))
		if err != nil || n <= 0 {
			return "", false
		}
		total += n
		found = true
	}
	if !found {
		return "", false
	}
	return strconv.Itoa(total), true
}

func shouldBuildIndexOnline(using string) bool {
	switch strings.ToUpper(strings.TrimSpace(using)) {
	case "INVERTED", "NGRAM_BF", "ANN":
		return true
	default:
		return false
	}
}

func isMutableTableProperty(key string) bool {
	switch key {
	case "file_cache_ttl_seconds",
		"group_commit_interval_ms",
		"bloom_filter_columns",
		"colocate_with",
		"compression_codec",
		"replication_num",
		"enable_single_replica_compaction":
		return true
	}
	return strings.HasPrefix(key, "write.parquet.")
}

func diffPlatformObjects(current, desired []*PlatformObject) []Statement {
	var stmts []Statement
	currentByKey := make(map[string]*PlatformObject, len(current))
	for _, p := range current {
		currentByKey[p.Kind+":"+p.Name] = p
	}

	for _, dp := range desired {
		cp, existed := currentByKey[dp.Kind+":"+dp.Name]
		if !existed {
			stmts = append(stmts, Statement{
				Kind:     StmtPlatformObject,
				Platform: dp.Clone(),
			})
			continue
		}
		if !platformObjectsEqual(cp, dp) {
			if dp.Kind == "WORKLOAD_GROUP" && len(dp.Properties) > 0 {
				stmts = append(stmts, Statement{
					Kind: StmtPlatformObject,
					Platform: &PlatformObject{
						Kind:       "ALTER_WORKLOAD_GROUP",
						Name:       dp.Name,
						Properties: append([]Property(nil), dp.Properties...),
					},
				})
			} else {
				stmts = append(stmts, Statement{
					Kind:     StmtPlatformObject,
					Platform: dp.Clone(),
				})
			}
		}
	}
	return stmts
}

func platformObjectsEqual(a, b *PlatformObject) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Kind != b.Kind || a.Name != b.Name {
		return false
	}
	if len(a.Properties) != len(b.Properties) {
		return false
	}
	mapA := propertiesToMap(a.Properties)
	mapB := propertiesToMap(b.Properties)
	if len(mapA) != len(mapB) {
		return false
	}
	for k, v := range mapA {
		if vb, ok := mapB[k]; !ok || v != vb {
			return false
		}
	}
	if len(a.Properties) == 0 {
		return strings.TrimSpace(a.RawSQL) == strings.TrimSpace(b.RawSQL)
	}
	return true
}

func normalizeViewSQL(v *ViewDef) string {
	if v == nil {
		return ""
	}
	return strings.Join(strings.Fields(strings.TrimSuffix(strings.TrimSpace(v.AsSQL), ";")), " ")
}
