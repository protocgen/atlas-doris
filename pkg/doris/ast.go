// Package doris implements an AST, parser, canonical formatter, schema differ,
// and evolution linter for Apache Doris 4.x and Apache Iceberg v2 DDL statements,
// including proto2type-generated *_doris.sql and *_iceberg.sql files.
package doris

import (
	"fmt"
	"path/filepath"
	"strings"
)

// KeyKind identifies the Apache Doris / StarRocks table key model.
type KeyKind string

const (
	KeyPrimary   KeyKind = "PRIMARY"
	KeyUnique    KeyKind = "UNIQUE"
	KeyDuplicate KeyKind = "DUPLICATE"
	KeyAggregate KeyKind = "AGGREGATE"
)

// KeyClause represents a PRIMARY KEY(...), UNIQUE KEY(...), DUPLICATE KEY(...), or AGGREGATE KEY(...) clause.
type KeyClause struct {
	Kind    KeyKind
	Columns []string
}

// Clone returns a deep copy of KeyClause.
func (k *KeyClause) Clone() *KeyClause {
	if k == nil {
		return nil
	}
	return &KeyClause{
		Kind:    k.Kind,
		Columns: append([]string(nil), k.Columns...),
	}
}

// PartitionClause represents a PARTITION BY LIST / RANGE / expression clause.
type PartitionClause struct {
	Kind          string   // "LIST", "RANGE", or "EXPR"
	Columns       []string // Columns for LIST / RANGE
	RawExpr       string   // Raw expression inside (...) for EXPR (e.g. "tenant_id, day(ingested_at)")
	PartitionsRaw string   // Optional partition body e.g. "()"
}

// Clone returns a deep copy of PartitionClause.
func (p *PartitionClause) Clone() *PartitionClause {
	if p == nil {
		return nil
	}
	return &PartitionClause{
		Kind:          p.Kind,
		Columns:       append([]string(nil), p.Columns...),
		RawExpr:       p.RawExpr,
		PartitionsRaw: p.PartitionsRaw,
	}
}

// Equal reports whether two PartitionClause values are semantically equal.
func (p *PartitionClause) Equal(other *PartitionClause) bool {
	if p == nil && other == nil {
		return true
	}
	if p == nil || other == nil {
		return false
	}
	return p.Kind == other.Kind &&
		stringSlicesEqual(p.Columns, other.Columns) &&
		p.RawExpr == other.RawExpr &&
		p.PartitionsRaw == other.PartitionsRaw
}

// DistributionClause represents a DISTRIBUTED BY HASH(...) or RANDOM BUCKETS ... clause.
type DistributionClause struct {
	Kind    string   // "HASH" or "RANDOM"
	Columns []string // Hash columns
	Buckets string   // Positive integer, "AUTO", or template placeholder "${TENANT_BUCKETS}"
}

// Clone returns a deep copy of DistributionClause.
func (d *DistributionClause) Clone() *DistributionClause {
	if d == nil {
		return nil
	}
	return &DistributionClause{
		Kind:    d.Kind,
		Columns: append([]string(nil), d.Columns...),
		Buckets: d.Buckets,
	}
}

// Equal reports whether two DistributionClause values are semantically equal.
func (d *DistributionClause) Equal(other *DistributionClause) bool {
	if d == nil && other == nil {
		return true
	}
	if d == nil || other == nil {
		return false
	}
	return d.Kind == other.Kind &&
		stringSlicesEqual(d.Columns, other.Columns) &&
		d.Buckets == other.Buckets
}

// Property represents a single ("key" = "value") entry in a PROPERTIES block.
type Property struct {
	Key   string
	Value string
}

// ColumnDef describes a column in a Doris or Iceberg table.
type ColumnDef struct {
	Name       string
	Type       string
	AggType    string // Optional aggregate type for AGGREGATE KEY tables (e.g. SUM, REPLACE, MAX, MIN)
	Nullable   bool
	HasDefault bool
	Default    string
	Comment    string
}

// Clone returns a deep copy of ColumnDef.
func (c *ColumnDef) Clone() *ColumnDef {
	if c == nil {
		return nil
	}
	cp := *c
	return &cp
}

// Equal reports whether two ColumnDef values have identical definition attributes.
func (c *ColumnDef) Equal(other *ColumnDef) bool {
	if c == nil && other == nil {
		return true
	}
	if c == nil || other == nil {
		return false
	}
	return c.Name == other.Name &&
		TypesSemanticallyEqual(c.Type, other.Type) &&
		c.AggType == other.AggType &&
		c.Nullable == other.Nullable &&
		c.HasDefault == other.HasDefault &&
		c.Default == other.Default &&
		c.Comment == other.Comment
}

// RollupDef describes a Doris rollup index definition on a table.
type RollupDef struct {
	Name    string
	Columns []string
}

// Clone returns a deep copy of RollupDef.
func (r *RollupDef) Clone() *RollupDef {
	if r == nil {
		return nil
	}
	return &RollupDef{
		Name:    r.Name,
		Columns: append([]string(nil), r.Columns...),
	}
}

// Equal reports whether two RollupDef values are semantically equal.
func (r *RollupDef) Equal(other *RollupDef) bool {
	if r == nil && other == nil {
		return true
	}
	if r == nil || other == nil {
		return false
	}
	return r.Name == other.Name && stringSlicesEqual(r.Columns, other.Columns)
}

// IndexDef describes an inline Doris index (USING INVERTED, NGRAM_BF, ANN, or BITMAP).
type IndexDef struct {
	Name       string
	Columns    []string
	Using      string // "INVERTED", "NGRAM_BF", "ANN", "BITMAP"
	Properties []Property
	Comment    string
}

// Clone returns a deep copy of IndexDef.
func (idx *IndexDef) Clone() *IndexDef {
	if idx == nil {
		return nil
	}
	return &IndexDef{
		Name:       idx.Name,
		Columns:    append([]string(nil), idx.Columns...),
		Using:      idx.Using,
		Properties: append([]Property(nil), idx.Properties...),
		Comment:    idx.Comment,
	}
}

// Equal reports whether two IndexDef values are semantically identical.
func (idx *IndexDef) Equal(other *IndexDef) bool {
	if idx == nil && other == nil {
		return true
	}
	if idx == nil || other == nil {
		return false
	}
	if idx.Name != other.Name ||
		!stringSlicesEqual(idx.Columns, other.Columns) ||
		!strings.EqualFold(idx.Using, other.Using) ||
		idx.Comment != other.Comment ||
		len(idx.Properties) != len(other.Properties) {
		return false
	}
	pmA := propertiesToMap(idx.Properties)
	pmB := propertiesToMap(other.Properties)
	if len(pmA) != len(pmB) {
		return false
	}
	for k, v := range pmA {
		if vb, ok := pmB[k]; !ok || v != vb {
			return false
		}
	}
	return true
}

// TableDef describes a parsed Apache Doris 4.x, StarRocks 4.x, or Apache Iceberg table definition.
type TableDef struct {
	FullName     string
	Catalog      string
	Database     string
	Name         string
	Dialect      string
	IfNotExists  bool
	Columns      []*ColumnDef
	Indexes      []*IndexDef
	Rollups      []*RollupDef
	Engine       string
	Key          *KeyClause
	Comment      string
	Partition    *PartitionClause
	OrderBy      []string
	Distribution *DistributionClause
	Properties   []Property
}

// CanonicalName returns the normalized qualified name of the table.
func (t *TableDef) CanonicalName() string {
	if t == nil {
		return ""
	}
	if t.Catalog != "" && t.Database != "" {
		return t.Catalog + "." + t.Database + "." + t.Name
	}
	if t.Database != "" {
		return t.Database + "." + t.Name
	}
	if t.FullName != "" {
		return t.FullName
	}
	return t.Name
}

// LookupKey returns the catalog-normalized matching key for the table, treating
// an omitted catalog and the default Doris "internal" / StarRocks "default_catalog" catalogs as equivalent.
func (t *TableDef) LookupKey() string {
	if t == nil {
		return ""
	}
	return normalizeTableLookupKey(t.CanonicalName())
}

// DialectLookupKey returns a dialect-scoped lookup key ("iceberg:<key>", "starrocks:<key>", or "doris:<key>")
// so multi-target schemas containing Doris, StarRocks, and Iceberg variants of the same logical table do not collide.
func (t *TableDef) DialectLookupKey() string {
	if t == nil {
		return ""
	}
	key := t.LookupKey()
	if t.IsIcebergTable() {
		return "iceberg:" + key
	}
	if t.IsStarRocksTable() {
		return "starrocks:" + key
	}
	return "doris:" + key
}

// IsIcebergTable reports whether the table is an Apache Iceberg catalog table
// rather than an Apache Doris internal OLAP table.
func (t *TableDef) IsIcebergTable() bool {
	if t == nil {
		return false
	}
	cat := strings.ToLower(t.Catalog)
	if cat == "azra_lake" || cat == "lake" || cat == "iceberg" {
		return true
	}
	if strings.EqualFold(t.Engine, "iceberg") {
		return true
	}
	if eng, ok := t.GetProperty("engine"); ok && strings.EqualFold(eng, "iceberg") {
		return true
	}
	if _, ok := t.GetProperty("compression_codec"); ok {
		return true
	}
	for _, p := range t.Properties {
		if strings.HasPrefix(p.Key, "write.parquet.") {
			return true
		}
	}
	return t.Key == nil && t.Distribution == nil
}

// IsDorisInternalTable reports whether the table is an Apache Doris / StarRocks internal OLAP table.
func (t *TableDef) IsDorisInternalTable() bool {
	return !t.IsIcebergTable()
}

// IsStarRocksTable reports whether the table is a StarRocks 4.x native table
// (identified by Dialect == "starrocks", PRIMARY KEY model, StarRocks-specific
// properties such as enable_persistent_index, storage_volume, or datacache.enable,
// or StarRocks GIN / VECTOR indexes).
func (t *TableDef) IsStarRocksTable() bool {
	if t == nil || t.IsIcebergTable() {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(t.Dialect), "starrocks") {
		return true
	}
	if t.Key != nil && t.Key.Kind == KeyPrimary {
		return true
	}
	for _, p := range t.Properties {
		switch p.Key {
		case "enable_persistent_index", "storage_volume", "datacache.enable", "datacache.partition_duration":
			return true
		}
	}
	for _, idx := range t.Indexes {
		u := strings.ToUpper(strings.TrimSpace(idx.Using))
		if u == "GIN" || u == "VECTOR" || u == "NGRAMBF" {
			return true
		}
	}
	return false
}

// HasPersistentIndex reports whether "enable_persistent_index" is set to "true".
func (t *TableDef) HasPersistentIndex() bool {
	v, ok := t.GetProperty("enable_persistent_index")
	return ok && strings.EqualFold(strings.TrimSpace(v), "true")
}

// ColumnByName looks up a column by its unquoted identifier.
func (t *TableDef) ColumnByName(name string) (*ColumnDef, bool) {
	if t == nil {
		return nil, false
	}
	for _, c := range t.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return nil, false
}

// IndexByName looks up an inline index by name.
func (t *TableDef) IndexByName(name string) (*IndexDef, bool) {
	if t == nil {
		return nil, false
	}
	for _, idx := range t.Indexes {
		if idx.Name == name {
			return idx, true
		}
	}
	return nil, false
}

// GetProperty returns the value of table property key k if present.
func (t *TableDef) GetProperty(k string) (string, bool) {
	if t == nil {
		return "", false
	}
	for _, p := range t.Properties {
		if p.Key == k {
			return p.Value, true
		}
	}
	return "", false
}

// SetProperty sets or updates a table property in deterministic insertion order.
func (t *TableDef) SetProperty(k, v string) {
	for i := range t.Properties {
		if t.Properties[i].Key == k {
			t.Properties[i].Value = v
			return
		}
	}
	t.Properties = append(t.Properties, Property{Key: k, Value: v})
}

// PropertyMap returns a map of all table properties.
func (t *TableDef) PropertyMap() map[string]string {
	return propertiesToMap(t.Properties)
}

// SequenceCol returns the "function_column.sequence_col" property if configured.
func (t *TableDef) SequenceCol() string {
	v, _ := t.GetProperty("function_column.sequence_col")
	return v
}

// ColocateGroup returns the "colocate_with" property if configured.
func (t *TableDef) ColocateGroup() string {
	v, _ := t.GetProperty("colocate_with")
	return v
}

// HasUniqueKeyMergeOnWrite reports whether "enable_unique_key_merge_on_write" is set to "true".
func (t *TableDef) HasUniqueKeyMergeOnWrite() bool {
	v, ok := t.GetProperty("enable_unique_key_merge_on_write")
	return ok && strings.EqualFold(strings.TrimSpace(v), "true")
}

// Clone returns a deep copy of TableDef.
func (t *TableDef) Clone() *TableDef {
	if t == nil {
		return nil
	}
	cols := make([]*ColumnDef, len(t.Columns))
	for i, c := range t.Columns {
		cols[i] = c.Clone()
	}
	idxs := make([]*IndexDef, len(t.Indexes))
	for i, idx := range t.Indexes {
		idxs[i] = idx.Clone()
	}
	rollups := make([]*RollupDef, len(t.Rollups))
	for i, r := range t.Rollups {
		rollups[i] = r.Clone()
	}
	return &TableDef{
		FullName:     t.FullName,
		Catalog:      t.Catalog,
		Database:     t.Database,
		Name:         t.Name,
		Dialect:      t.Dialect,
		IfNotExists:  t.IfNotExists,
		Columns:      cols,
		Indexes:      idxs,
		Rollups:      rollups,
		Engine:       t.Engine,
		Key:          t.Key.Clone(),
		Comment:      t.Comment,
		Partition:    t.Partition.Clone(),
		OrderBy:      append([]string(nil), t.OrderBy...),
		Distribution: t.Distribution.Clone(),
		Properties:   append([]Property(nil), t.Properties...),
	}
}

// DatabaseDef represents a CREATE DATABASE / CREATE SCHEMA statement.
type DatabaseDef struct {
	FullName        string
	Catalog         string
	Name            string
	IfNotExists     bool
	IsSchemaKeyword bool
}

// CanonicalName returns the normalized [catalog.]database identifier.
func (d *DatabaseDef) CanonicalName() string {
	if d == nil {
		return ""
	}
	if d.Catalog != "" {
		return d.Catalog + "." + d.Name
	}
	if d.FullName != "" {
		return d.FullName
	}
	return d.Name
}

// LookupKey returns the catalog-normalized matching key for the database, treating
// an omitted catalog and the default Doris "internal" catalog as equivalent.
func (d *DatabaseDef) LookupKey() string {
	if d == nil {
		return ""
	}
	return normalizeDatabaseLookupKey(d.CanonicalName())
}

// Clone returns a deep copy of DatabaseDef.
func (d *DatabaseDef) Clone() *DatabaseDef {
	if d == nil {
		return nil
	}
	cp := *d
	return &cp
}

// ViewDef represents a CREATE [OR REPLACE] VIEW statement.
type ViewDef struct {
	FullName  string
	Catalog   string
	Database  string
	Name      string
	OrReplace bool
	AsSQL     string
	RawSQL    string
}

// CanonicalName returns the normalized view name.
func (v *ViewDef) CanonicalName() string {
	if v == nil {
		return ""
	}
	if v.Catalog != "" && v.Database != "" {
		return v.Catalog + "." + v.Database + "." + v.Name
	}
	if v.Database != "" {
		return v.Database + "." + v.Name
	}
	if v.FullName != "" {
		return v.FullName
	}
	return v.Name
}

// LookupKey returns the catalog-normalized matching key for the view, treating
// an omitted catalog and the default Doris "internal" catalog as equivalent.
func (v *ViewDef) LookupKey() string {
	if v == nil {
		return ""
	}
	return normalizeTableLookupKey(v.CanonicalName())
}

// Clone returns a deep copy of ViewDef.
func (v *ViewDef) Clone() *ViewDef {
	if v == nil {
		return nil
	}
	cp := *v
	return &cp
}

// PlatformObject represents cluster/catalog-level Doris DDL such as
// CREATE STORAGE VAULT, CREATE WORKLOAD GROUP, ALTER WORKLOAD GROUP,
// CREATE WORKLOAD POLICY, CREATE CATALOG, CREATE RESOURCE, or SET ... AS DEFAULT STORAGE VAULT.
type PlatformObject struct {
	Kind        string // "STORAGE_VAULT", "SET_DEFAULT_STORAGE_VAULT", "WORKLOAD_GROUP", "ALTER_WORKLOAD_GROUP", "WORKLOAD_POLICY", "CATALOG", "RESOURCE"
	Name        string
	IfNotExists bool
	Properties  []Property
	RawSQL      string
}

// Clone returns a deep copy of PlatformObject.
func (p *PlatformObject) Clone() *PlatformObject {
	if p == nil {
		return nil
	}
	return &PlatformObject{
		Kind:        p.Kind,
		Name:        p.Name,
		IfNotExists: p.IfNotExists,
		Properties:  append([]Property(nil), p.Properties...),
		RawSQL:      p.RawSQL,
	}
}

// Schema represents a complete Doris / Iceberg schema state.
type Schema struct {
	Databases       []*DatabaseDef
	Tables          []*TableDef
	Views           []*ViewDef
	PlatformObjects []*PlatformObject
}

// NewSchema creates an empty Schema.
func NewSchema() *Schema {
	return &Schema{}
}

// Clone returns a deep copy of Schema.
func (s *Schema) Clone() *Schema {
	if s == nil {
		return nil
	}
	out := &Schema{
		Databases:       make([]*DatabaseDef, len(s.Databases)),
		Tables:          make([]*TableDef, len(s.Tables)),
		Views:           make([]*ViewDef, len(s.Views)),
		PlatformObjects: make([]*PlatformObject, len(s.PlatformObjects)),
	}
	for i, d := range s.Databases {
		out.Databases[i] = d.Clone()
	}
	for i, t := range s.Tables {
		out.Tables[i] = t.Clone()
	}
	for i, v := range s.Views {
		out.Views[i] = v.Clone()
	}
	for i, p := range s.PlatformObjects {
		out.PlatformObjects[i] = p.Clone()
	}
	return out
}

// TableByName looks up a table in Schema by its CanonicalName(), LookupKey(), or short Name (when unambiguous).
func (s *Schema) TableByName(name string) (*TableDef, bool) {
	if s == nil {
		return nil, false
	}
	clean := normalizeQualifiedName(name)
	lookup := normalizeTableLookupKey(name)
	for _, t := range s.Tables {
		if t.CanonicalName() == clean || t.FullName == clean || (lookup != "" && t.LookupKey() == lookup) {
			return t, true
		}
	}
	// Fallback to short table name match if only one table has that short name.
	var match *TableDef
	count := 0
	for _, t := range s.Tables {
		if t.Name == clean {
			match = t
			count++
		}
	}
	if count == 1 {
		return match, true
	}
	return nil, false
}

// DatabaseByName looks up a database by its canonical name or LookupKey().
func (s *Schema) DatabaseByName(name string) (*DatabaseDef, bool) {
	if s == nil {
		return nil, false
	}
	clean := normalizeQualifiedName(name)
	lookup := normalizeDatabaseLookupKey(name)
	for _, d := range s.Databases {
		if d.CanonicalName() == clean || d.Name == clean || (lookup != "" && d.LookupKey() == lookup) {
			return d, true
		}
	}
	return nil, false
}

// ViewByName looks up a view by its canonical name or LookupKey().
func (s *Schema) ViewByName(name string) (*ViewDef, bool) {
	if s == nil {
		return nil, false
	}
	clean := normalizeQualifiedName(name)
	lookup := normalizeTableLookupKey(name)
	for _, v := range s.Views {
		if v.CanonicalName() == clean || v.Name == clean || (lookup != "" && v.LookupKey() == lookup) {
			return v, true
		}
	}
	return nil, false
}

// Merge appends objects from other into s, updating or replacing any existing objects with the same canonical key.
func (s *Schema) Merge(other *Schema) {
	if s == nil || other == nil {
		return
	}
	for _, d := range other.Databases {
		if _, ok := s.DatabaseByName(d.CanonicalName()); !ok {
			s.Databases = append(s.Databases, d.Clone())
		}
	}
	for _, t := range other.Tables {
		replaced := false
		for i, existing := range s.Tables {
			if existing.DialectLookupKey() == t.DialectLookupKey() {
				s.Tables[i] = t.Clone()
				replaced = true
				break
			}
		}
		if !replaced {
			s.Tables = append(s.Tables, t.Clone())
		}
	}
	for _, v := range other.Views {
		replaced := false
		for i, existing := range s.Views {
			if existing.LookupKey() == v.LookupKey() {
				s.Views[i] = v.Clone()
				replaced = true
				break
			}
		}
		if !replaced {
			s.Views = append(s.Views, v.Clone())
		}
	}
	for _, p := range other.PlatformObjects {
		replaced := false
		for i, existing := range s.PlatformObjects {
			if existing.Kind == p.Kind && existing.Name == p.Name && p.Name != "" {
				s.PlatformObjects[i] = p.Clone()
				replaced = true
				break
			}
		}
		if !replaced {
			s.PlatformObjects = append(s.PlatformObjects, p.Clone())
		}
	}
}

func isDefaultDorisCatalog(cat string) bool {
	c := strings.TrimSpace(cat)
	return c == "" || strings.EqualFold(c, "internal") || strings.EqualFold(c, "default_catalog")
}

// pairSchemaTables pairs tables between current and desired schemas:
//  1. First by exact DialectLookupKey() so multi-target schemas (Doris + StarRocks + Iceberg)
//     pair each table with its matching dialect counterpart without colliding.
//  2. Second, any remaining unmatched non-Iceberg tables with the same LookupKey() are paired
//     so single-dialect migrations that illegally mutate a table's key model (e.g., UNIQUE KEY
//     to PRIMARY KEY) still trigger DORIS001_IMMUTABLE_KEY.
func pairSchemaTables(current, desired []*TableDef) (map[*TableDef]*TableDef, map[*TableDef]*TableDef) {
	currentForDesired := make(map[*TableDef]*TableDef, len(desired))
	desiredForCurrent := make(map[*TableDef]*TableDef, len(current))

	currentByDialect := make(map[string]*TableDef, len(current))
	for _, ct := range current {
		currentByDialect[ct.DialectLookupKey()] = ct
	}
	for _, dt := range desired {
		if ct, ok := currentByDialect[dt.DialectLookupKey()]; ok {
			if _, alreadyUsed := desiredForCurrent[ct]; !alreadyUsed {
				currentForDesired[dt] = ct
				desiredForCurrent[ct] = dt
			}
		}
	}

	for _, dt := range desired {
		if _, matched := currentForDesired[dt]; matched || dt.IsIcebergTable() {
			continue
		}
		for _, ct := range current {
			if _, cMatched := desiredForCurrent[ct]; cMatched || ct.IsIcebergTable() {
				continue
			}
			if ct.LookupKey() == dt.LookupKey() {
				currentForDesired[dt] = ct
				desiredForCurrent[ct] = dt
				break
			}
		}
	}
	return currentForDesired, desiredForCurrent
}

func inferDialectHint(content, filename string) string {
	trimmed := strings.TrimSpace(filename)
	if trimmed != "" {
		lowerBase := strings.ToLower(filepath.Base(trimmed))
		switch {
		case strings.Contains(lowerBase, "starrocks"):
			return "starrocks"
		case strings.Contains(lowerBase, "iceberg"):
			return "iceberg"
		case strings.Contains(lowerBase, "doris"):
			return "doris"
		}
	}
	lowerContent := strings.ToLower(content)
	switch {
	case strings.Contains(lowerContent, "backend: starrocks"), strings.Contains(lowerContent, "backend=starrocks"):
		return "starrocks"
	case strings.Contains(lowerContent, "backend: iceberg"), strings.Contains(lowerContent, "backend=iceberg"):
		return "iceberg"
	case strings.Contains(lowerContent, "backend: doris"), strings.Contains(lowerContent, "backend=doris"):
		return "doris"
	}
	return ""
}

func applySchemaDialectHint(s *Schema, dialect string) {
	if s == nil || dialect == "" {
		return
	}
	for _, t := range s.Tables {
		if t.IsIcebergTable() {
			t.Dialect = "iceberg"
			continue
		}
		if t.Dialect == "" {
			t.Dialect = dialect
		}
	}
}

func normalizeTableLookupKey(raw string) string {
	full, cat, db, short := splitQualifiedName(raw)
	if short == "" {
		return full
	}
	if !isDefaultDorisCatalog(cat) && db != "" {
		return cat + "." + db + "." + short
	}
	if db != "" {
		return db + "." + short
	}
	return short
}

func normalizeDatabaseLookupKey(raw string) string {
	full, cat, db, short := splitQualifiedName(raw)
	if short == "" {
		return full
	}
	if cat == "" && db != "" {
		cat = db
	}
	if !isDefaultDorisCatalog(cat) {
		return cat + "." + short
	}
	return short
}

// StatementKind classifies a parsed DDL / migration SQL statement.
type StatementKind string

const (
	StmtCreateDatabase          StatementKind = "CREATE_DATABASE"
	StmtCreateTable             StatementKind = "CREATE_TABLE"
	StmtDropTable               StatementKind = "DROP_TABLE"
	StmtAlterTableAddColumn     StatementKind = "ALTER_TABLE_ADD_COLUMN"
	StmtAlterTableDropColumn    StatementKind = "ALTER_TABLE_DROP_COLUMN"
	StmtAlterTableModifyColumn  StatementKind = "ALTER_TABLE_MODIFY_COLUMN"
	StmtAlterTableAddIndex      StatementKind = "ALTER_TABLE_ADD_INDEX"
	StmtAlterTableDropIndex     StatementKind = "ALTER_TABLE_DROP_INDEX"
	StmtBuildIndex              StatementKind = "BUILD_INDEX"
	StmtAlterTableSetProperties StatementKind = "ALTER_TABLE_SET_PROPERTIES"
	StmtCreateOrReplaceView     StatementKind = "CREATE_OR_REPLACE_VIEW"
	StmtPlatformObject          StatementKind = "PLATFORM_OBJECT"
)

// Statement represents a single parsed or diff-generated DDL statement.
type Statement struct {
	Kind           StatementKind
	Database       *DatabaseDef
	Table          *TableDef
	TargetTable    string
	IfExists       bool
	Column         *ColumnDef
	First          bool
	AfterColumn    string
	DropColumnName string
	Index          *IndexDef
	DropIndexName  string
	Properties     []Property
	View           *ViewDef
	Platform       *PlatformObject
	RawSQL         string
}

// SQL renders the canonical SQL text for this statement (including trailing semicolon).
func (st Statement) SQL() string {
	switch st.Kind {
	case StmtCreateDatabase:
		if st.Database != nil {
			return FormatCreateDatabase(st.Database)
		}
	case StmtCreateTable:
		if st.Table != nil {
			return FormatCreateTable(st.Table)
		}
	case StmtDropTable:
		if st.IfExists {
			return fmt.Sprintf("DROP TABLE IF EXISTS %s;", st.TargetTable)
		}
		return fmt.Sprintf("DROP TABLE %s;", st.TargetTable)
	case StmtAlterTableAddColumn:
		if st.Column != nil {
			posClause := ""
			if st.First {
				posClause = " FIRST"
			} else if st.AfterColumn != "" {
				posClause = " AFTER " + FormatIdent(st.AfterColumn)
			}
			return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s%s;", st.TargetTable, FormatColumnDef(st.Column), posClause)
		}
	case StmtAlterTableDropColumn:
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", st.TargetTable, FormatIdent(st.DropColumnName))
	case StmtAlterTableModifyColumn:
		if st.Column != nil {
			posClause := ""
			if st.First {
				posClause = " FIRST"
			} else if st.AfterColumn != "" {
				posClause = " AFTER " + FormatIdent(st.AfterColumn)
			}
			return fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s%s;", st.TargetTable, FormatColumnDef(st.Column), posClause)
		}
	case StmtAlterTableAddIndex:
		if st.Index != nil {
			return fmt.Sprintf("ALTER TABLE %s ADD %s;", st.TargetTable, FormatIndexDefInline(st.Index))
		}
	case StmtAlterTableDropIndex:
		return fmt.Sprintf("ALTER TABLE %s DROP INDEX %s;", st.TargetTable, st.DropIndexName)
	case StmtBuildIndex:
		return fmt.Sprintf("BUILD INDEX %s ON %s;", st.DropIndexName, st.TargetTable)
	case StmtAlterTableSetProperties:
		if len(st.Properties) > 0 {
			pairs := make([]string, len(st.Properties))
			for i, p := range st.Properties {
				pairs[i] = fmt.Sprintf("%q = %q", p.Key, p.Value)
			}
			return fmt.Sprintf("ALTER TABLE %s SET (%s);", st.TargetTable, strings.Join(pairs, ", "))
		}
	case StmtCreateOrReplaceView:
		if st.View != nil {
			return FormatView(st.View)
		}
	case StmtPlatformObject:
		if st.Platform != nil {
			return FormatPlatformObject(st.Platform)
		}
	}
	if st.RawSQL != "" {
		raw := strings.TrimSpace(st.RawSQL)
		if !strings.HasSuffix(raw, ";") {
			raw += ";"
		}
		return raw
	}
	return ""
}

func propertiesToMap(props []Property) map[string]string {
	out := make(map[string]string, len(props))
	for _, p := range props {
		out[p.Key] = p.Value
	}
	return out
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
