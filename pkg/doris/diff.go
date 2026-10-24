package doris

import (
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
		if _, exists := current.DatabaseByName(db.CanonicalName()); !exists {
			dbCopy := db.Clone()
			dbCopy.IfNotExists = true
			stmts = append(stmts, Statement{
				Kind:     StmtCreateDatabase,
				Database: dbCopy,
			})
		}
	}

	// 3. Tables (create new, alter existing, or drop removed when AllowDestructive is set).
	currentByName := make(map[string]*TableDef, len(current.Tables))
	for _, t := range current.Tables {
		currentByName[t.CanonicalName()] = t
	}
	desiredByName := make(map[string]*TableDef, len(desired.Tables))
	for _, t := range desired.Tables {
		desiredByName[t.CanonicalName()] = t
	}

	for _, dt := range desired.Tables {
		ct, existed := currentByName[dt.CanonicalName()]
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
			if _, ok := desiredByName[ct.CanonicalName()]; !ok {
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

	currentColByName := make(map[string]*ColumnDef, len(current.Columns))
	for _, c := range current.Columns {
		currentColByName[c.Name] = c
	}
	desiredColByName := make(map[string]*ColumnDef, len(desired.Columns))
	for _, c := range desired.Columns {
		desiredColByName[c.Name] = c
	}

	// 1. Dropped columns (if AllowDestructive).
	if opts.AllowDestructive {
		for _, cc := range current.Columns {
			if _, ok := desiredColByName[cc.Name]; !ok {
				stmts = append(stmts, Statement{
					Kind:           StmtAlterTableDropColumn,
					TargetTable:    targetName,
					DropColumnName: cc.Name,
				})
			}
		}
	}

	// 2. Added and modified columns in desired order.
	for _, dc := range desired.Columns {
		cc, existed := currentColByName[dc.Name]
		if !existed {
			stmts = append(stmts, Statement{
				Kind:        StmtAlterTableAddColumn,
				TargetTable: targetName,
				Column:      dc.Clone(),
			})
			continue
		}
		if !cc.Equal(dc) {
			stmts = append(stmts, Statement{
				Kind:        StmtAlterTableModifyColumn,
				TargetTable: targetName,
				Column:      dc.Clone(),
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
		if curVal, exists := curProps[dp.Key]; !exists || curVal != dp.Value {
			stmts = append(stmts, Statement{
				Kind:        StmtAlterTableSetProperties,
				TargetTable: targetName,
				Properties:  []Property{{Key: dp.Key, Value: dp.Value}},
			})
		}
	}

	return stmts
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
