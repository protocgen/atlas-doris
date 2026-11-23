package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/protocgen/atlas-doris/pkg/doris"
)

// OperatorVersion is recorded in atlas_schema_revisions.operator_version.
const OperatorVersion = "atlas-doris/v0.3.0"

// DefaultLegacyRevisionsTable is the pre-Atlas tenant-worker revisions table checked
// during scoped cutover when atlas_schema_revisions has no rows for the chain scope.
const DefaultLegacyRevisionsTable = "schema_revisions"

const (
	asyncPendingErrorPrefix = "ASYNC_PENDING:"
	inProgressErrorPrefix   = "IN_PROGRESS:"
	maxChainScopeLen        = 128
	maxStoredVersionLen     = 255
	checkpointWriteTimeout  = 5 * time.Second
)

// ErrAsyncJobPending is returned in NonBlocking mode when an asynchronous Doris
// schema change or index build job is still in progress (PENDING, WAITING_TXN, or RUNNING).
var ErrAsyncJobPending = errors.New("doris async job still pending")

// ErrUnscopedRevisionsExist is returned when a scoped migration chain runs with no scoped
// rows in atlas_schema_revisions while matching unscoped version rows already exist.
var ErrUnscopedRevisionsExist = errors.New("unscoped rows already exist in atlas_schema_revisions matching this migration chain")

// ErrLegacyRevisionsExist is returned when a scoped migration chain runs with no scoped
// rows in atlas_schema_revisions while pre-existing <scope>:<ver>@<hash> rows exist in
// the legacy schema_revisions table.
var ErrLegacyRevisionsExist = errors.New("legacy schema_revisions rows already exist for this migration chain")

// AsyncJobFailedError describes a failed or cancelled asynchronous Doris schema change
// or index build job detected during migration polling.
type AsyncJobFailedError struct {
	Table   string
	Index   string
	State   string
	Message string
}

// Error implements the error interface.
func (e *AsyncJobFailedError) Error() string {
	if e == nil {
		return "doris async job failed"
	}
	target := e.Table
	if e.Index != "" {
		target += "." + e.Index
	}
	if e.Message != "" {
		return fmt.Sprintf("doris async schema change job for %q was %s: %s", target, e.State, e.Message)
	}
	return fmt.Sprintf("doris async schema change job for %q was %s", target, e.State)
}

// QueryRows abstracts *sql.Rows for callers integrating custom query hooks or connection wrappers.
type QueryRows interface {
	Columns() ([]string, error)
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// QueryExecutor abstracts SQL execution and row queries so callers can supply
// custom wrappers (e.g., tenant isolation query hooks) or delegate to *sql.DB.
type QueryExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowsContext(ctx context.Context, query string, args ...any) (QueryRows, error)
}

type sqlDBQuerier struct {
	db *sql.DB
}

func (q sqlDBQuerier) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return q.db.ExecContext(ctx, query, args...)
}

func (q sqlDBQuerier) QueryRowsContext(ctx context.Context, query string, args ...any) (QueryRows, error) {
	return q.db.QueryContext(ctx, query, args...)
}

// RevisionsTableName is the standard Atlas revisions table name.
const RevisionsTableName = "atlas_schema_revisions"

// RevisionsTableDDL is the canonical Apache Doris 4.x Merge-on-Write DDL for atlas_schema_revisions.
const RevisionsTableDDL = `CREATE TABLE IF NOT EXISTS atlas_schema_revisions (
    version VARCHAR(255) NOT NULL,
    description VARCHAR(255) NOT NULL,
    type INT NOT NULL DEFAULT "2",
    applied INT NOT NULL DEFAULT "0",
    total INT NOT NULL DEFAULT "0",
    executed_at DATETIMEV2(6) NOT NULL,
    execution_time BIGINT NOT NULL DEFAULT "0",
    error TEXT NULL,
    error_stmt TEXT NULL,
    hash VARCHAR(255) NOT NULL,
    partial_hashes ARRAY<VARCHAR(255)> NOT NULL DEFAULT "[]",
    operator_version VARCHAR(255) NOT NULL
)
UNIQUE KEY(version)
DISTRIBUTED BY HASH(version) BUCKETS 1
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true"
);`

// RevisionRecord represents a row in atlas_schema_revisions.
type RevisionRecord struct {
	Version         string
	Description     string
	Type            int
	Applied         int
	Total           int
	ExecutedAt      time.Time
	ExecutionTimeNs int64
	Error           string
	ErrorStmt       string
	Hash            string
	OperatorVersion string
}

// MigrationStatus reports the current state of a migration directory against Doris.
type MigrationStatus struct {
	Status  string // "OK" or "PENDING"
	Current string // Latest applied version (or "No migration applied yet")
	Next    string // Next pending version (or "")
	Applied []RevisionRecord
	Pending []MigrationFile
}

// ApplyOptions configures Executor.Apply.
type ApplyOptions struct {
	DirPath              string
	FS                   fs.FS
	Vars                 map[string]string
	DryRun               bool
	BaselineVersion      string
	RevisionsSchema      string
	ChainScope           string
	IcebergCatalog       string
	LegacyRevisionsTable string
	SkipCutoverCheck     bool
	NonBlocking          bool
	AllowDestructive     bool
	PollInterval         time.Duration
	PollTimeout          time.Duration
}

// ApplyResult summarizes the migrations and SQL statements applied (or planned in DryRun).
type ApplyResult struct {
	AppliedFiles      []string
	ExecutedSQL       []string
	SkippedBaseline   []string
	AlreadyUpToDate   bool
	TotalExecutionDur time.Duration
}

// ExecutorOption customizes an Executor created by NewExecutorWithQuerier.
type ExecutorOption func(*Executor)

// WithChainScope scopes revision reads, writes, and deletes to "<scope>:<version>".
func WithChainScope(scope string) ExecutorOption {
	return func(e *Executor) {
		e.chainScope = strings.TrimSpace(scope)
	}
}

// WithNonBlocking configures Executor to return ErrAsyncJobPending instead of sleeping on unfinished Doris jobs.
func WithNonBlocking(nonBlocking bool) ExecutorOption {
	return func(e *Executor) {
		e.nonBlocking = nonBlocking
	}
}

// WithIcebergCatalog configures the Iceberg catalog name used by NormalizeIcebergCatalogDDL.
func WithIcebergCatalog(catalog string) ExecutorOption {
	return func(e *Executor) {
		e.icebergCatalog = strings.TrimSpace(catalog)
	}
}

// WithPollConfig overrides async job poll interval and timeout.
func WithPollConfig(interval, timeout time.Duration) ExecutorOption {
	return func(e *Executor) {
		if interval > 0 {
			e.pollInterval = interval
		}
		if timeout > 0 {
			e.pollTimeout = timeout
		}
	}
}

// WithLegacyRevisionsTable sets the legacy revisions table checked before cutting over
// a scoped chain when atlas_schema_revisions has no rows for that scope. Passing ""
// disables the legacy table check.
func WithLegacyRevisionsTable(tableName string) ExecutorOption {
	return func(e *Executor) {
		e.legacyRevisionsTable = strings.TrimSpace(tableName)
	}
}

// Executor manages migration status and execution against an Apache Doris FE endpoint.
type Executor struct {
	db                   *sql.DB
	querier              QueryExecutor
	revisionsSchema      string
	chainScope           string
	icebergCatalog       string
	legacyRevisionsTable string
	nonBlocking          bool
	coupledMode          *atomic.Bool
	pollInterval         time.Duration
	pollTimeout          time.Duration
}

// NewExecutor creates an Executor wrapping an existing *sql.DB connection.
func NewExecutor(db *sql.DB, revisionsSchema string) *Executor {
	var q QueryExecutor
	if db != nil {
		q = sqlDBQuerier{db: db}
	}
	return &Executor{
		db:                   db,
		querier:              q,
		revisionsSchema:      strings.TrimSpace(revisionsSchema),
		legacyRevisionsTable: DefaultLegacyRevisionsTable,
		coupledMode:          &atomic.Bool{},
		pollInterval:         200 * time.Millisecond,
		pollTimeout:          2 * time.Minute,
	}
}

// NewExecutorWithQuerier creates an Executor backed by a custom QueryExecutor.
func NewExecutorWithQuerier(q QueryExecutor, revisionsSchema string, opts ...ExecutorOption) *Executor {
	exec := &Executor{
		querier:              q,
		revisionsSchema:      strings.TrimSpace(revisionsSchema),
		legacyRevisionsTable: DefaultLegacyRevisionsTable,
		coupledMode:          &atomic.Bool{},
		pollInterval:         200 * time.Millisecond,
		pollTimeout:          2 * time.Minute,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(exec)
		}
	}
	return exec
}

// DefaultSQLDriver is the database/sql driver name used by DialExecutor and InspectLiveSchema.
var DefaultSQLDriver = "mysql"

// DialExecutor parses rawURL ("doris://user:pass@host:port/db" or "mysql://..."),
// opens a MySQL wire-protocol connection to Apache Doris FE, and returns an Executor.
func DialExecutor(rawURL, revisionsSchema string) (*Executor, error) {
	return DialExecutorWithOptions(rawURL, revisionsSchema)
}

// DialExecutorWithOptions opens a MySQL wire-protocol connection to Apache Doris FE
// and configures the resulting Executor with opts.
func DialExecutorWithOptions(rawURL, revisionsSchema string, opts ...ExecutorOption) (*Executor, error) {
	dsn, _, err := ParseDorisDSN(rawURL)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(DefaultSQLDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open doris connection: %w", err)
	}
	exec := NewExecutor(db, revisionsSchema)
	for _, opt := range opts {
		if opt != nil {
			opt(exec)
		}
	}
	return exec, nil
}

// Close closes the underlying database connection if present.
func (e *Executor) Close() error {
	if e == nil || e.db == nil {
		return nil
	}
	return e.db.Close()
}

// ParseDorisDSN converts a doris:// or mysql:// connection URL into a Go MySQL driver DSN.
func ParseDorisDSN(rawURL string) (dsn string, dbName string, err error) {
	s := strings.TrimSpace(rawURL)
	if s == "" {
		return "", "", fmt.Errorf("empty Doris URL")
	}
	if !strings.Contains(s, "://") {
		// Already a Go MySQL DSN e.g. user:pass@tcp(host:port)/db
		if cfg, parseErr := mysql.ParseDSN(s); parseErr == nil {
			return s, cfg.DBName, nil
		}
		if slashIdx := strings.IndexByte(s, '/'); slashIdx >= 0 {
			rest := s[slashIdx+1:]
			if qIdx := strings.IndexByte(rest, '?'); qIdx >= 0 {
				dbName = rest[:qIdx]
			} else {
				dbName = rest
			}
		}
		return s, dbName, nil
	}

	u, err := url.Parse(s)
	if err != nil {
		if scheme := safeURLScheme(s); scheme != "" {
			return "", "", fmt.Errorf("invalid %s URL: malformed URL", scheme)
		}
		return "", "", fmt.Errorf("invalid Doris URL: malformed URL")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "doris" && scheme != "mysql" {
		return "", "", fmt.Errorf("unsupported URL scheme %q in %q (expected doris:// or mysql://)", u.Scheme, u.Redacted())
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("missing host in Doris URL %q", u.Redacted())
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":9030"
	}

	user := "root"
	pass := ""
	if u.User != nil {
		if u.User.Username() != "" {
			user = u.User.Username()
		}
		pass, _ = u.User.Password()
	}

	dbName = strings.TrimPrefix(u.Path, "/")
	q := u.Query()
	cfg := mysql.NewConfig()
	if len(q) > 0 {
		parsedCfg, parseErr := mysql.ParseDSN("tcp(127.0.0.1:9030)/?" + q.Encode())
		if parseErr != nil {
			return "", "", fmt.Errorf("invalid Doris URL query options in %q: %w", u.Redacted(), parseErr)
		}
		cfg = parsedCfg
	}
	cfg.User = user
	cfg.Passwd = pass
	cfg.Net = "tcp"
	cfg.Addr = host
	cfg.DBName = dbName
	if !q.Has("parseTime") {
		cfg.ParseTime = true
	}
	if !q.Has("interpolateParams") {
		cfg.InterpolateParams = true
	}
	return cfg.FormatDSN(), dbName, nil
}

func safeURLScheme(s string) string {
	schemeSep := strings.Index(s, "://")
	if schemeSep <= 0 {
		return ""
	}
	scheme := s[:schemeSep]
	for i := 0; i < len(scheme); i++ {
		c := scheme[i]
		isAlpha := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		isDigit := c >= '0' && c <= '9'
		if i == 0 && !isAlpha {
			return ""
		}
		if !isAlpha && !isDigit && c != '+' && c != '.' && c != '-' {
			return ""
		}
	}
	return scheme
}

// RevisionsTableQualifiedName returns the qualified name for atlas_schema_revisions.
func (e *Executor) RevisionsTableQualifiedName() string {
	if e != nil && e.revisionsSchema != "" {
		return e.revisionsSchema + "." + RevisionsTableName
	}
	return RevisionsTableName
}

// RevisionsTableCreateSQL returns the CREATE TABLE IF NOT EXISTS DDL for atlas_schema_revisions.
func (e *Executor) RevisionsTableCreateSQL() string {
	qName := e.RevisionsTableQualifiedName()
	if qName == RevisionsTableName {
		return RevisionsTableDDL
	}
	return strings.Replace(RevisionsTableDDL, "CREATE TABLE IF NOT EXISTS "+RevisionsTableName, "CREATE TABLE IF NOT EXISTS "+qName, 1)
}

// EnsureRevisionsTable creates the revisions schema (if configured) and the
// atlas_schema_revisions table in Apache Doris if it does not already exist.
func (e *Executor) EnsureRevisionsTable(ctx context.Context) error {
	if e == nil || e.querier == nil {
		return fmt.Errorf("executor has no database connection")
	}
	if e.revisionsSchema != "" {
		if !isSafeQualifiedIdentifier(e.revisionsSchema) {
			return fmt.Errorf("invalid revisions schema identifier %q", e.revisionsSchema)
		}
		createDB := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", e.revisionsSchema)
		if _, err := e.querier.ExecContext(ctx, createDB); err != nil {
			return fmt.Errorf("create revisions database %q: %w", e.revisionsSchema, err)
		}
	}
	if _, err := e.querier.ExecContext(ctx, e.RevisionsTableCreateSQL()); err != nil {
		return fmt.Errorf("create %s: %w", e.RevisionsTableQualifiedName(), err)
	}
	return nil
}

// AppliedRevisions performs a read-only query of atlas_schema_revisions ordered by version ascending.
// If the revisions database or table does not yet exist, it returns an empty slice without mutating the database.
func (e *Executor) AppliedRevisions(ctx context.Context) ([]RevisionRecord, error) {
	if e == nil || e.querier == nil {
		return nil, fmt.Errorf("executor has no database connection")
	}
	return e.queryAppliedRevisions(ctx, true)
}

func (e *Executor) queryAppliedRevisions(ctx context.Context, allowMissing bool) ([]RevisionRecord, error) {
	if e == nil || e.querier == nil {
		return nil, nil
	}
	if e.revisionsSchema != "" && !isSafeQualifiedIdentifier(e.revisionsSchema) {
		return nil, fmt.Errorf("invalid revisions schema identifier %q", e.revisionsSchema)
	}
	if err := validateChainScope(e.chainScope); err != nil {
		return nil, err
	}

	var (
		query string
		args  []any
	)
	if e.chainScope != "" {
		query = fmt.Sprintf(
			"SELECT version, description, `type`, applied, total, executed_at, execution_time, COALESCE(error, ''), COALESCE(error_stmt, ''), hash, operator_version FROM %s WHERE version >= ? AND version < ? ORDER BY version ASC;",
			e.RevisionsTableQualifiedName(),
		)
		args = []any{e.chainScope + ":", e.chainScope + ";"}
	} else {
		query = fmt.Sprintf(
			"SELECT version, description, `type`, applied, total, executed_at, execution_time, COALESCE(error, ''), COALESCE(error_stmt, ''), hash, operator_version FROM %s ORDER BY version ASC;",
			e.RevisionsTableQualifiedName(),
		)
	}

	rows, err := e.querier.QueryRowsContext(ctx, query, args...)
	if err != nil {
		if allowMissing && isMissingTableOrDatabaseError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("query %s: %w", e.RevisionsTableQualifiedName(), err)
	}
	if rows == nil {
		return nil, fmt.Errorf("query %s returned nil rows", e.RevisionsTableQualifiedName())
	}
	defer func() { _ = rows.Close() }()

	var records []RevisionRecord
	for rows.Next() {
		var r RevisionRecord
		var execAtRaw any
		if err := rows.Scan(
			&r.Version,
			&r.Description,
			&r.Type,
			&r.Applied,
			&r.Total,
			&execAtRaw,
			&r.ExecutionTimeNs,
			&r.Error,
			&r.ErrorStmt,
			&r.Hash,
			&r.OperatorVersion,
		); err != nil {
			return nil, fmt.Errorf("scan %s row: %w", e.RevisionsTableQualifiedName(), err)
		}
		unscopedVer, ok := parseScopedVersion(e.chainScope, r.Version)
		if !ok {
			continue
		}
		r.Version = unscopedVer
		r.ExecutedAt = parseScannedTime(execAtRaw)
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

// RecordRevision inserts or updates a revision record in atlas_schema_revisions.
func (e *Executor) RecordRevision(ctx context.Context, r RevisionRecord) error {
	if e == nil || e.querier == nil {
		return fmt.Errorf("executor has no database connection")
	}
	if e.revisionsSchema != "" && !isSafeQualifiedIdentifier(e.revisionsSchema) {
		return fmt.Errorf("invalid revisions schema identifier %q", e.revisionsSchema)
	}
	if err := validateChainScope(e.chainScope); err != nil {
		return err
	}
	storedVersion, err := validateStoredVersion(e.chainScope, r.Version)
	if err != nil {
		return err
	}
	execAt := r.ExecutedAt
	if execAt.IsZero() {
		execAt = time.Now().UTC()
	}
	opVer := r.OperatorVersion
	if opVer == "" {
		opVer = OperatorVersion
	}
	recType := r.Type
	if recType == 0 {
		recType = 2
	}

	errVal := sql.NullString{String: r.Error, Valid: r.Error != ""}
	errStmtVal := sql.NullString{String: r.ErrorStmt, Valid: r.ErrorStmt != ""}

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (version, description, `type`, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', ?);",
		e.RevisionsTableQualifiedName(),
	)
	_, err = e.querier.ExecContext(
		ctx,
		insertSQL,
		storedVersion,
		r.Description,
		recType,
		r.Applied,
		r.Total,
		execAt.UTC().Format("2006-01-02 15:04:05.000000"),
		r.ExecutionTimeNs,
		errVal,
		errStmtVal,
		r.Hash,
		opVer,
	)
	if err != nil {
		return fmt.Errorf("insert revision %s into %s: %w", storedVersion, e.RevisionsTableQualifiedName(), err)
	}
	return nil
}

// DeleteRevision removes a revision record matching version (within e.chainScope when configured)
// from atlas_schema_revisions.
func (e *Executor) DeleteRevision(ctx context.Context, version string) error {
	if e == nil || e.querier == nil {
		return fmt.Errorf("executor has no database connection")
	}
	if e.revisionsSchema != "" && !isSafeQualifiedIdentifier(e.revisionsSchema) {
		return fmt.Errorf("invalid revisions schema identifier %q", e.revisionsSchema)
	}
	if err := validateChainScope(e.chainScope); err != nil {
		return err
	}
	storedVersion, err := validateStoredVersion(e.chainScope, version)
	if err != nil {
		return err
	}
	delSQL := fmt.Sprintf("DELETE FROM %s WHERE version = ?;", e.RevisionsTableQualifiedName())
	if _, err := e.querier.ExecContext(ctx, delSQL, storedVersion); err != nil {
		return fmt.Errorf("delete revision %s from %s: %w", storedVersion, e.RevisionsTableQualifiedName(), err)
	}
	return nil
}

// StatusFS validates the migration fs.FS and computes applied vs pending migrations without mutating the database.
func (e *Executor) StatusFS(ctx context.Context, fsys fs.FS) (*MigrationStatus, error) {
	if e == nil || e.querier == nil {
		return nil, fmt.Errorf("executor has no database connection")
	}
	_, files, err := validateAndReplayFS(fsys, doris.LintOptions{AllowDestructive: true})
	if err != nil {
		return nil, err
	}
	return e.statusFromFiles(ctx, files)
}

// Status validates the migration directory and computes applied vs pending migrations without mutating the database.
func (e *Executor) Status(ctx context.Context, dirPath string) (*MigrationStatus, error) {
	if e == nil || e.querier == nil {
		return nil, fmt.Errorf("executor has no database connection")
	}
	_, files, err := validateAndReplayDir(dirPath, doris.LintOptions{AllowDestructive: true})
	if err != nil {
		return nil, err
	}
	return e.statusFromFiles(ctx, files)
}

func (e *Executor) statusFromFiles(ctx context.Context, files []MigrationFile) (*MigrationStatus, error) {
	if e == nil || e.querier == nil {
		return nil, fmt.Errorf("executor has no database connection")
	}
	if err := validateChainScope(e.chainScope); err != nil {
		return nil, err
	}
	applied, err := e.queryAppliedRevisions(ctx, true)
	if err != nil {
		return nil, err
	}

	appliedByVersion := make(map[string]RevisionRecord, len(applied))
	maxAppliedVersion := ""
	for _, r := range applied {
		appliedByVersion[r.Version] = r
		if r.Error == "" && r.Version > maxAppliedVersion {
			maxAppliedVersion = r.Version
		}
	}

	var pending []MigrationFile
	for _, f := range files {
		if rev, ok := appliedByVersion[f.Version]; ok {
			if rev.Hash != "" && f.Hash != "" && rev.Hash != f.Hash {
				return nil, fmt.Errorf("applied migration %s (%s) checksum mismatch: db has %s, atlas.sum has %s", f.Version, f.Name, rev.Hash, f.Hash)
			}
			if rev.Error == "" {
				continue
			}
		} else if isEmptyTenantBaseline(f) {
			continue
		}
		pending = append(pending, f)
	}

	st := &MigrationStatus{
		Status:  "OK",
		Current: "No migration applied yet",
		Applied: applied,
		Pending: pending,
	}
	if maxAppliedVersion != "" {
		st.Current = maxAppliedVersion
	}
	if len(pending) > 0 {
		st.Status = "PENDING"
		st.Next = pending[0].Version
	}
	return st, nil
}

func (e *Executor) withApplyOptions(opts ApplyOptions) (*Executor, error) {
	var cfg Executor
	if e != nil {
		cfg = *e
	} else {
		cfg.legacyRevisionsTable = DefaultLegacyRevisionsTable
	}
	if cfg.coupledMode == nil {
		cfg.coupledMode = &atomic.Bool{}
	}
	if s := strings.TrimSpace(opts.RevisionsSchema); s != "" {
		cfg.revisionsSchema = s
	}
	if s := strings.TrimSpace(opts.ChainScope); s != "" {
		cfg.chainScope = s
	}
	if s := strings.TrimSpace(opts.IcebergCatalog); s != "" {
		cfg.icebergCatalog = s
	}
	if s := strings.TrimSpace(opts.LegacyRevisionsTable); s != "" {
		cfg.legacyRevisionsTable = s
	}
	if opts.NonBlocking {
		cfg.nonBlocking = true
	}
	if opts.PollInterval > 0 {
		cfg.pollInterval = opts.PollInterval
	}
	if opts.PollTimeout > 0 {
		cfg.pollTimeout = opts.PollTimeout
	}
	if err := validateChainScope(cfg.chainScope); err != nil {
		return nil, err
	}
	if cfg.revisionsSchema != "" && !isSafeQualifiedIdentifier(cfg.revisionsSchema) {
		return nil, fmt.Errorf("invalid revisions schema identifier %q", cfg.revisionsSchema)
	}
	if cfg.legacyRevisionsTable != "" && !isSafeQualifiedTableIdentifier(cfg.legacyRevisionsTable) {
		return nil, fmt.Errorf("invalid legacy revisions table identifier %q", cfg.legacyRevisionsTable)
	}
	return &cfg, nil
}

// Apply validates atlas.sum, substitutes template variables, executes pending migrations
// in version order against Doris FE, polls async schema change / index build jobs, and
// records each applied migration in atlas_schema_revisions.
func (e *Executor) Apply(ctx context.Context, opts ApplyOptions) (*ApplyResult, error) {
	execCfg, err := e.withApplyOptions(opts)
	if err != nil {
		return nil, err
	}

	lintOpts := doris.LintOptions{AllowDestructive: opts.AllowDestructive}
	var files []MigrationFile
	if opts.FS != nil {
		_, files, err = validateAndReplayFS(opts.FS, lintOpts)
		if err != nil {
			return nil, err
		}
	} else {
		_, files, err = validateAndReplayDir(opts.DirPath, lintOpts)
		if err != nil {
			return nil, err
		}
	}

	// Preflight all stored revision keys before EnsureRevisionsTable or executing any migration SQL.
	for _, f := range files {
		if isEmptyTenantBaseline(f) && (opts.BaselineVersion == "" || f.Version > opts.BaselineVersion) {
			continue
		}
		if _, err := validateStoredVersion(execCfg.chainScope, f.Version); err != nil {
			return nil, err
		}
	}

	appliedByVersion := make(map[string]RevisionRecord)
	if !opts.DryRun {
		if execCfg.querier == nil {
			return nil, fmt.Errorf("migrate apply requires a live Doris connection unless --dry-run is used")
		}
		if err := execCfg.EnsureRevisionsTable(ctx); err != nil {
			return nil, err
		}
		revs, err := execCfg.queryAppliedRevisions(ctx, false)
		if err != nil {
			return nil, err
		}
		if execCfg.chainScope != "" && len(revs) == 0 && !opts.SkipCutoverCheck {
			if err := execCfg.verifyScopedCutoverSafety(ctx, files); err != nil {
				return nil, err
			}
		}
		for _, r := range revs {
			appliedByVersion[r.Version] = r
		}
	} else if execCfg.querier != nil {
		revs, err := execCfg.queryAppliedRevisions(ctx, true)
		if err != nil {
			return nil, err
		}
		for _, r := range revs {
			appliedByVersion[r.Version] = r
		}
	}

	res := &ApplyResult{}
	startAll := time.Now()

	for _, f := range files {
		resumeOffset := 0
		pendingAsyncWait := false
		var pendingBaselineJobID int64
		hasPersistedAsyncMarker := false
		if rev, ok := appliedByVersion[f.Version]; ok {
			if rev.Hash != "" && f.Hash != "" && rev.Hash != f.Hash {
				return nil, fmt.Errorf("applied migration %s (%s) checksum mismatch: db has %s, atlas.sum has %s", f.Version, f.Name, rev.Hash, f.Hash)
			}
			if rev.Error == "" {
				continue
			}
			if markerIdx, jobID, ok := parseAsyncPendingMarker(rev.Error); ok {
				resumeOffset = markerIdx
				pendingAsyncWait = true
				pendingBaselineJobID = jobID
				hasPersistedAsyncMarker = true
			} else {
				if rev.Applied > 0 {
					resumeOffset = rev.Applied
				}
				pendingAsyncWait = isPendingAsyncError(rev.Error)
			}
		}

		// Explicit --baseline skips and records the baseline version.
		if opts.BaselineVersion != "" && f.Version <= opts.BaselineVersion {
			res.SkippedBaseline = append(res.SkippedBaseline, f.Name)
			if !opts.DryRun {
				if err := execCfg.recordRevisionCheckpoint(ctx, RevisionRecord{
					Version:         f.Version,
					Description:     f.Description,
					Type:            2,
					Applied:         0,
					Total:           len(f.Statements),
					ExecutedAt:      time.Now().UTC(),
					ExecutionTimeNs: 0,
					Hash:            f.Hash,
					OperatorVersion: OperatorVersion,
				}); err != nil {
					return nil, err
				}
			}
			continue
		}

		// Skip empty 00000000000000_tenant.sql without executing or recording in atlas_schema_revisions.
		if isEmptyTenantBaseline(f) {
			res.SkippedBaseline = append(res.SkippedBaseline, f.Name)
			continue
		}

		renderedSQL, err := SubstituteVars(f.SQL, opts.Vars)
		if err != nil {
			return nil, fmt.Errorf("migration %s variable substitution: %w", f.Name, err)
		}
		stmts, err := doris.ParseStatements(renderedSQL)
		if err != nil {
			return nil, fmt.Errorf("migration %s rendered SQL parse error: %w", f.Name, err)
		}
		if resumeOffset > len(stmts) {
			resumeOffset = len(stmts)
		}

		if opts.DryRun {
			res.AppliedFiles = append(res.AppliedFiles, f.Name)
			for idx, st := range stmts[resumeOffset:] {
				if pendingAsyncWait && idx == 0 && execCfg.requiresAsyncJobWait(st) {
					pendingAsyncWait = false
					continue
				}
				stmtSQL := execCfg.normalizeRuntimeStatement(statementRuntimeSQL(st))
				if stmtSQL != "" {
					res.ExecutedSQL = append(res.ExecutedSQL, stmtSQL)
				}
			}
			continue
		}

		fileStart := time.Now()
		for stmtIdx := resumeOffset; stmtIdx < len(stmts); stmtIdx++ {
			st := stmts[stmtIdx]
			stmtSQL := execCfg.normalizeRuntimeStatement(statementRuntimeSQL(st))
			if stmtSQL == "" {
				continue
			}

			isAsyncStmt := execCfg.requiresAsyncJobWait(st)
			if pendingAsyncWait && stmtIdx == resumeOffset && isAsyncStmt {
				pendingAsyncWait = false
				if !hasPersistedAsyncMarker {
					if cpErr := execCfg.recordRevisionCheckpoint(ctx, RevisionRecord{
						Version:         f.Version,
						Description:     f.Description,
						Type:            2,
						Applied:         stmtIdx,
						Total:           len(stmts),
						ExecutedAt:      time.Now().UTC(),
						ExecutionTimeNs: time.Since(fileStart).Nanoseconds(),
						Error:           formatAsyncPendingMarker(stmtIdx, pendingBaselineJobID),
						ErrorStmt:       stmtSQL,
						Hash:            f.Hash,
						OperatorVersion: OperatorVersion,
					}); cpErr != nil {
						return nil, cpErr
					}
				}
				if waitErr := execCfg.waitForStatementJobCompletion(ctx, st, pendingBaselineJobID); waitErr != nil {
					if isTerminalAsyncError(waitErr) {
						if cpErr := execCfg.recordRevisionCheckpoint(ctx, RevisionRecord{
							Version:         f.Version,
							Description:     f.Description,
							Type:            2,
							Applied:         stmtIdx,
							Total:           len(stmts),
							ExecutedAt:      time.Now().UTC(),
							ExecutionTimeNs: time.Since(fileStart).Nanoseconds(),
							Error:           waitErr.Error(),
							ErrorStmt:       stmtSQL,
							Hash:            f.Hash,
							OperatorVersion: OperatorVersion,
						}); cpErr != nil {
							return nil, errors.Join(fmt.Errorf("migration %s async schema change on %s: %w", f.Name, st.TargetTable, waitErr), cpErr)
						}
					}
					return nil, fmt.Errorf("migration %s async schema change on %s: %w", f.Name, st.TargetTable, waitErr)
				}
				if stmtIdx < len(stmts)-1 {
					if cpErr := execCfg.recordRevisionCheckpoint(ctx, RevisionRecord{
						Version:         f.Version,
						Description:     f.Description,
						Type:            2,
						Applied:         stmtIdx + 1,
						Total:           len(stmts),
						ExecutedAt:      time.Now().UTC(),
						ExecutionTimeNs: time.Since(fileStart).Nanoseconds(),
						Error:           formatInProgressMarker(stmtIdx + 1),
						Hash:            f.Hash,
						OperatorVersion: OperatorVersion,
					}); cpErr != nil {
						return nil, cpErr
					}
				}
				continue
			}

			var baselineJobID int64
			if isAsyncStmt {
				var snapErr error
				baselineJobID, snapErr = execCfg.snapshotStatementBaselineJobID(ctx, st)
				if snapErr != nil {
					return nil, fmt.Errorf("migration %s pre-exec job watermark on %s: %w", f.Name, st.TargetTable, snapErr)
				}
			}

			_, execErr := execCfg.querier.ExecContext(ctx, stmtSQL)
			if execErr != nil && doris.IsCoupledModeUnknownPropertiesErr(execErr) {
				stripped := doris.StripCoupledModeCreateTableProperties(stmtSQL)
				if stripped != stmtSQL {
					if execCfg.coupledMode != nil {
						execCfg.coupledMode.Store(true)
					}
					stmtSQL = stripped
					_, execErr = execCfg.querier.ExecContext(ctx, stmtSQL)
				}
			}
			if execErr != nil {
				failErr := fmt.Errorf("migration %s failed on %q: %w", f.Name, stmtSQL, execErr)
				if cpErr := execCfg.recordRevisionCheckpoint(ctx, RevisionRecord{
					Version:         f.Version,
					Description:     f.Description,
					Type:            2,
					Applied:         stmtIdx,
					Total:           len(stmts),
					ExecutedAt:      time.Now().UTC(),
					ExecutionTimeNs: time.Since(fileStart).Nanoseconds(),
					Error:           execErr.Error(),
					ErrorStmt:       stmtSQL,
					Hash:            f.Hash,
					OperatorVersion: OperatorVersion,
				}); cpErr != nil {
					return nil, errors.Join(failErr, cpErr)
				}
				return nil, failErr
			}
			res.ExecutedSQL = append(res.ExecutedSQL, stmtSQL)

			if isAsyncStmt {
				// Persist explicit ASYNC_PENDING marker immediately after submitting the async DDL
				// so transient poll failures, timeouts, or NonBlocking returns never re-execute DDL.
				if cpErr := execCfg.recordRevisionCheckpoint(ctx, RevisionRecord{
					Version:         f.Version,
					Description:     f.Description,
					Type:            2,
					Applied:         stmtIdx,
					Total:           len(stmts),
					ExecutedAt:      time.Now().UTC(),
					ExecutionTimeNs: time.Since(fileStart).Nanoseconds(),
					Error:           formatAsyncPendingMarker(stmtIdx, baselineJobID),
					ErrorStmt:       stmtSQL,
					Hash:            f.Hash,
					OperatorVersion: OperatorVersion,
				}); cpErr != nil {
					return nil, cpErr
				}
				if waitErr := execCfg.waitForStatementJobCompletion(ctx, st, baselineJobID); waitErr != nil {
					if isTerminalAsyncError(waitErr) {
						if cpErr := execCfg.recordRevisionCheckpoint(ctx, RevisionRecord{
							Version:         f.Version,
							Description:     f.Description,
							Type:            2,
							Applied:         stmtIdx,
							Total:           len(stmts),
							ExecutedAt:      time.Now().UTC(),
							ExecutionTimeNs: time.Since(fileStart).Nanoseconds(),
							Error:           waitErr.Error(),
							ErrorStmt:       stmtSQL,
							Hash:            f.Hash,
							OperatorVersion: OperatorVersion,
						}); cpErr != nil {
							return nil, errors.Join(fmt.Errorf("migration %s async schema change on %s: %w", f.Name, st.TargetTable, waitErr), cpErr)
						}
					}
					return nil, fmt.Errorf("migration %s async schema change on %s: %w", f.Name, st.TargetTable, waitErr)
				}
			}

			if stmtIdx < len(stmts)-1 {
				if cpErr := execCfg.recordRevisionCheckpoint(ctx, RevisionRecord{
					Version:         f.Version,
					Description:     f.Description,
					Type:            2,
					Applied:         stmtIdx + 1,
					Total:           len(stmts),
					ExecutedAt:      time.Now().UTC(),
					ExecutionTimeNs: time.Since(fileStart).Nanoseconds(),
					Error:           formatInProgressMarker(stmtIdx + 1),
					Hash:            f.Hash,
					OperatorVersion: OperatorVersion,
				}); cpErr != nil {
					return nil, cpErr
				}
			}
		}

		if err := execCfg.recordRevisionCheckpoint(ctx, RevisionRecord{
			Version:         f.Version,
			Description:     f.Description,
			Type:            2,
			Applied:         len(stmts),
			Total:           len(stmts),
			ExecutedAt:      time.Now().UTC(),
			ExecutionTimeNs: time.Since(fileStart).Nanoseconds(),
			Hash:            f.Hash,
			OperatorVersion: OperatorVersion,
		}); err != nil {
			return nil, err
		}
		res.AppliedFiles = append(res.AppliedFiles, f.Name)
	}

	res.TotalExecutionDur = time.Since(startAll)
	res.AlreadyUpToDate = len(res.AppliedFiles) == 0
	return res, nil
}

func (e *Executor) recordRevisionCheckpoint(ctx context.Context, r RevisionRecord) error {
	if ctx == nil {
		ctx = context.Background()
	}
	cpCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), checkpointWriteTimeout)
	defer cancel()
	return e.RecordRevision(cpCtx, r)
}

func statementRuntimeSQL(st doris.Statement) string {
	sqlText := strings.TrimSpace(st.RawSQL)
	if sqlText == "" {
		sqlText = strings.TrimSpace(st.SQL())
	}
	if sqlText != "" && !strings.HasSuffix(sqlText, ";") {
		sqlText += ";"
	}
	return sqlText
}

func isTerminalAsyncError(err error) bool {
	var asyncFail *AsyncJobFailedError
	return errors.As(err, &asyncFail)
}

func formatAsyncPendingMarker(stmtIdx int, baselineJobID int64) string {
	return fmt.Sprintf("%s%d:%d", asyncPendingErrorPrefix, stmtIdx, baselineJobID)
}

func formatInProgressMarker(nextStmtIdx int) string {
	return fmt.Sprintf("%s%d", inProgressErrorPrefix, nextStmtIdx)
}

func parseAsyncPendingMarker(errStr string) (stmtIdx int, baselineJobID int64, ok bool) {
	if !strings.HasPrefix(errStr, asyncPendingErrorPrefix) {
		return 0, 0, false
	}
	rest := strings.TrimPrefix(errStr, asyncPendingErrorPrefix)
	parts := strings.Split(rest, ":")
	if len(parts) < 2 {
		return 0, 0, false
	}
	idx, err1 := strconv.Atoi(parts[0])
	jobID, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil || idx < 0 {
		return 0, 0, false
	}
	return idx, jobID, true
}

func (e *Executor) verifyScopedCutoverSafety(ctx context.Context, files []MigrationFile) error {
	versionSet := make(map[string]bool, len(files))
	versions := make([]string, 0, len(files))
	for _, f := range files {
		if isEmptyTenantBaseline(f) || f.Version == "" || versionSet[f.Version] {
			continue
		}
		versionSet[f.Version] = true
		versions = append(versions, f.Version)
	}

	if len(versions) > 0 {
		placeholders := strings.Repeat("?,", len(versions))
		placeholders = strings.TrimSuffix(placeholders, ",")
		args := make([]any, len(versions))
		for i, v := range versions {
			args[i] = v
		}
		query := fmt.Sprintf("SELECT version FROM %s WHERE version IN (%s);", e.RevisionsTableQualifiedName(), placeholders)
		rows, err := e.querier.QueryRowsContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("cutover check against %s: %w", e.RevisionsTableQualifiedName(), err)
		}
		if rows == nil {
			return fmt.Errorf("cutover check against %s returned nil rows", e.RevisionsTableQualifiedName())
		}
		foundVer, scanErr := scanMatchingVersion(rows, func(v string) bool {
			return !strings.Contains(v, ":") && versionSet[v]
		})
		_ = rows.Close()
		if scanErr != nil {
			return fmt.Errorf("scan cutover check against %s: %w", e.RevisionsTableQualifiedName(), scanErr)
		}
		if foundVer != "" {
			return fmt.Errorf("%w: version %q exists unscoped in %s (backfill chain scope %q or pass SkipCutoverCheck)", ErrUnscopedRevisionsExist, foundVer, e.RevisionsTableQualifiedName(), e.chainScope)
		}
	}

	if e.legacyRevisionsTable != "" {
		legacyQualified := e.legacyRevisionsTable
		if !strings.Contains(legacyQualified, ".") && e.revisionsSchema != "" {
			legacyQualified = e.revisionsSchema + "." + legacyQualified
		}
		legacyQuery := fmt.Sprintf("SELECT version FROM %s WHERE version >= ? AND version < ? LIMIT 1;", legacyQualified)
		rows, err := e.querier.QueryRowsContext(ctx, legacyQuery, e.chainScope+":", e.chainScope+";")
		if err != nil {
			if isMissingTableOrDatabaseError(err) {
				return nil
			}
			return fmt.Errorf("legacy cutover check against %s: %w", legacyQualified, err)
		}
		if rows == nil {
			return fmt.Errorf("legacy cutover check against %s returned nil rows", legacyQualified)
		}
		prefix := e.chainScope + ":"
		foundLegacy, scanErr := scanMatchingVersion(rows, func(v string) bool {
			return strings.HasPrefix(v, prefix)
		})
		_ = rows.Close()
		if scanErr != nil {
			return fmt.Errorf("scan legacy cutover check against %s: %w", legacyQualified, scanErr)
		}
		if foundLegacy != "" {
			return fmt.Errorf("%w: found %q in %s for scope %q (backfill or pass SkipCutoverCheck)", ErrLegacyRevisionsExist, foundLegacy, legacyQualified, e.chainScope)
		}
	}

	return nil
}

func scanMatchingVersion(rows QueryRows, match func(string) bool) (string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if len(cols) == 0 {
		return "", nil
	}
	verIdx := 0
	for i, c := range cols {
		if strings.EqualFold(c, "version") {
			verIdx = i
			break
		}
	}
	vals := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range vals {
		scanArgs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return "", err
		}
		v := strings.TrimSpace(string(vals[verIdx]))
		if v != "" && match(v) {
			return v, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return "", nil
}

func (e *Executor) normalizeRuntimeStatement(stmtSQL string) string {
	s := strings.TrimSpace(stmtSQL)
	if s == "" {
		return ""
	}
	s = doris.NormalizeIcebergCatalogDDL(s, e.icebergCatalog)
	if doris.IsCreateOrAlterViewStatement(s) {
		s = doris.NormalizeNereidsViewOrderKeys(s)
	}
	if e.coupledMode != nil && e.coupledMode.Load() {
		s = doris.StripCoupledModeCreateTableProperties(s)
	}
	return s
}

func (e *Executor) requiresAsyncJobWait(st doris.Statement) bool {
	if isNonInternalCatalogTarget(st.TargetTable, func() string {
		if e != nil {
			return e.icebergCatalog
		}
		return ""
	}()) {
		return false
	}
	switch st.Kind {
	case doris.StmtAlterTableAddColumn,
		doris.StmtAlterTableDropColumn,
		doris.StmtAlterTableModifyColumn,
		doris.StmtAlterTableAddIndex,
		doris.StmtAlterTableDropIndex,
		doris.StmtBuildIndex:
		return true
	default:
		return false
	}
}

func isNonInternalCatalogTarget(targetTable, icebergCatalog string) bool {
	parts := strings.Split(targetTable, ".")
	if len(parts) < 3 {
		return false
	}
	cat := strings.Trim(strings.TrimSpace(parts[0]), "`")
	if cat == "" {
		return false
	}
	if icebergCatalog != "" && strings.EqualFold(cat, strings.Trim(strings.TrimSpace(icebergCatalog), "`")) {
		return true
	}
	return !strings.EqualFold(cat, "internal")
}

func statementIndexName(st doris.Statement) string {
	if st.DropIndexName != "" {
		return st.DropIndexName
	}
	if st.Index != nil {
		return st.Index.Name
	}
	return ""
}

func (e *Executor) showQueryForStatement(ctx context.Context, st doris.Statement) (showPrefix, indexName, query string, err error) {
	if st.Kind == doris.StmtBuildIndex {
		showPrefix = "SHOW BUILD INDEX"
	} else {
		showPrefix = "SHOW ALTER TABLE COLUMN"
	}
	indexName = statementIndexName(st)
	query, err = e.buildShowJobQueryContext(ctx, st.TargetTable, showPrefix)
	return showPrefix, indexName, query, err
}

func (e *Executor) resolveActiveDatabase(ctx context.Context) string {
	if e == nil || e.querier == nil {
		return ""
	}
	rows, err := e.querier.QueryRowsContext(ctx, "SELECT DATABASE();")
	if err != nil || rows == nil {
		return ""
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return ""
	}
	var raw sql.RawBytes
	if err := rows.Scan(&raw); err != nil {
		return ""
	}
	db := strings.TrimSpace(string(raw))
	if db == "" || db == "<nil>" || strings.EqualFold(db, "NULL") {
		return ""
	}
	parts := strings.Split(db, ".")
	if len(parts) == 0 || len(parts) > 2 {
		return ""
	}
	for i := range parts {
		parts[i] = strings.Trim(strings.TrimSpace(parts[i]), "`")
		if parts[i] == "" || !isSafeQualifiedIdentifier(parts[i]) {
			return ""
		}
	}
	return strings.Join(parts, ".")
}

func (e *Executor) buildShowJobQueryContext(ctx context.Context, fullTableName, showPrefix string) (string, error) {
	rawParts := strings.Split(fullTableName, ".")
	if len(rawParts) == 0 || len(rawParts) > 3 {
		return "", fmt.Errorf("invalid target table %q", fullTableName)
	}
	cleanParts := make([]string, len(rawParts))
	for i, rp := range rawParts {
		seg := strings.Trim(strings.TrimSpace(rp), "`")
		if seg == "" || !isSafeQualifiedIdentifier(seg) {
			return "", fmt.Errorf("unsafe table identifier segment %q in %q", rp, fullTableName)
		}
		cleanParts[i] = seg
	}
	shortTable := strings.ReplaceAll(cleanParts[len(cleanParts)-1], "'", "''")

	var dbPrefix string
	if showPrefix == "SHOW BUILD INDEX" {
		var dbName string
		if len(cleanParts) >= 2 {
			dbName = cleanParts[len(cleanParts)-2]
		} else {
			dbSource := e.resolveActiveDatabase(ctx)
			if dbSource == "" && e != nil {
				dbSource = e.revisionsSchema
			}
			if dbSource != "" {
				revParts := strings.Split(dbSource, ".")
				dbName = strings.Trim(strings.TrimSpace(revParts[len(revParts)-1]), "`")
				if dbName != "" && !isSafeQualifiedIdentifier(dbName) {
					return "", fmt.Errorf("invalid revisions schema segment %q", dbName)
				}
			}
		}
		if dbName != "" {
			dbPrefix = " FROM `" + dbName + "`"
		}
	} else {
		switch len(cleanParts) {
		case 3:
			dbPrefix = " FROM `" + cleanParts[0] + "`.`" + cleanParts[1] + "`"
		case 2:
			dbPrefix = " FROM `" + cleanParts[0] + "`"
		default:
			dbSource := e.resolveActiveDatabase(ctx)
			if dbSource == "" && e != nil {
				dbSource = e.revisionsSchema
			}
			if dbSource != "" {
				revParts := strings.Split(dbSource, ".")
				quoted := make([]string, 0, len(revParts))
				for _, rp := range revParts {
					cleanRP := strings.Trim(strings.TrimSpace(rp), "`")
					if cleanRP == "" || !isSafeQualifiedIdentifier(cleanRP) {
						return "", fmt.Errorf("invalid revisions schema segment %q", rp)
					}
					quoted = append(quoted, "`"+cleanRP+"`")
				}
				if len(quoted) > 0 {
					dbPrefix = " FROM " + strings.Join(quoted, ".")
				}
			}
		}
	}
	return fmt.Sprintf("%s%s WHERE TableName = '%s';", showPrefix, dbPrefix, shortTable), nil
}

func (e *Executor) snapshotStatementBaselineJobID(ctx context.Context, st doris.Statement) (int64, error) {
	if e == nil || e.querier == nil {
		return 0, nil
	}
	showPrefix, indexName, query, err := e.showQueryForStatement(ctx, st)
	if err != nil {
		return 0, err
	}
	stStatus, err := e.queryLatestAlterState(ctx, query, showPrefix, indexName, 0, true)
	if err != nil {
		if isUnsupportedShowQueryError(err) {
			return 0, nil
		}
		return 0, err
	}
	return stStatus.maxJobID, nil
}

func (e *Executor) waitForStatementJobCompletion(ctx context.Context, st doris.Statement, baselineJobID int64) error {
	if st.Kind == doris.StmtBuildIndex {
		return e.waitForAsyncDorisJob(ctx, st.TargetTable, statementIndexName(st), "SHOW BUILD INDEX", baselineJobID)
	}
	return e.waitForAsyncDorisJob(ctx, st.TargetTable, statementIndexName(st), "SHOW ALTER TABLE COLUMN", baselineJobID)
}

func (e *Executor) waitForAsyncDorisJob(ctx context.Context, fullTableName, indexName, showPrefix string, baselineJobID int64) error {
	if e == nil || e.querier == nil {
		return nil
	}
	query, err := e.buildShowJobQueryContext(ctx, fullTableName, showPrefix)
	if err != nil {
		return err
	}

	interval := e.pollInterval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	timeout := e.pollTimeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	deadline := time.Now().Add(timeout)

	for {
		status, err := e.queryLatestAlterState(ctx, query, showPrefix, indexName, baselineJobID, false)
		if err != nil {
			if isUnsupportedShowQueryError(err) {
				return nil
			}
			return err
		}
		upperState := strings.ToUpper(strings.TrimSpace(status.state))
		switch upperState {
		case "", "FINISHED":
			return nil
		case "CANCELLED", "CANCELED", "FAILED":
			idx := indexName
			if idx == "" {
				idx = status.indexName
			}
			return &AsyncJobFailedError{
				Table:   fullTableName,
				Index:   idx,
				State:   upperState,
				Message: status.msg,
			}
		}
		if e.nonBlocking {
			return fmt.Errorf("%w: table %q state=%s", ErrAsyncJobPending, fullTableName, upperState)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for Doris schema change job on %q (last state=%q)", fullTableName, status.state)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func isUnsupportedShowQueryError(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return false
	}
	var mysqlErr *mysql.MySQLError
	isMySQLErr := errors.As(err, &mysqlErr)
	msg := strings.ToLower(err.Error())

	// Never treat TLS/network/auth/context errors as unsupported SHOW commands.
	for _, reject := range []string{
		"tls:", "x509:", "certificate", "handshake", "dial ", "connection",
		"timeout", "eof", "access denied", "error 1045",
		"context canceled", "deadline exceeded",
	} {
		if strings.Contains(msg, reject) {
			return false
		}
	}

	hasUnsupportedToken := strings.Contains(msg, "unsupported") ||
		strings.Contains(msg, "not supported") ||
		strings.Contains(msg, "does not support") ||
		strings.Contains(msg, "doesn't support") ||
		strings.Contains(msg, "unknown command") ||
		strings.Contains(msg, "unrecognized")
	if !hasUnsupportedToken {
		return false
	}

	return isMySQLErr ||
		strings.Contains(msg, "show ") ||
		strings.Contains(msg, "build index") ||
		strings.Contains(msg, "alter table") ||
		strings.Contains(msg, "command") ||
		strings.Contains(msg, "statement") ||
		strings.Contains(msg, "errcode") ||
		strings.Contains(msg, "sql")
}

func isMissingTableOrDatabaseError(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && (mysqlErr.Number == 1049 || mysqlErr.Number == 1146) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "error 1049") ||
		strings.Contains(msg, "error 1146") ||
		strings.Contains(msg, "unknown database") ||
		strings.Contains(msg, "unknown table") {
		return true
	}
	if strings.Contains(msg, "doesn't exist") || strings.Contains(msg, "does not exist") {
		return strings.Contains(msg, "table ") ||
			strings.Contains(msg, "database ") ||
			strings.Contains(msg, "schema ")
	}
	return false
}

func isEmptyTenantBaseline(f MigrationFile) bool {
	return f.Name == "00000000000000_tenant.sql" && len(f.Statements) == 0
}

func isPendingAsyncError(errStr string) bool {
	if strings.HasPrefix(errStr, asyncPendingErrorPrefix) {
		return true
	}
	return strings.Contains(strings.ToLower(errStr), strings.ToLower(ErrAsyncJobPending.Error()))
}

var chainScopeRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
var genericTemplateVarValueRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func validateChainScope(scope string) error {
	if scope == "" {
		return nil
	}
	if len(scope) > maxChainScopeLen {
		return fmt.Errorf("chain scope %q exceeds maximum length %d", scope, maxChainScopeLen)
	}
	if !chainScopeRe.MatchString(scope) {
		return fmt.Errorf("invalid chain scope %q: must match ^[A-Za-z0-9._-]+$", scope)
	}
	return nil
}

func validateStoredVersion(chainScope, version string) (string, error) {
	cleanVer := strings.TrimSpace(version)
	if cleanVer == "" {
		return "", fmt.Errorf("revision version cannot be empty")
	}
	if chainScope == "" && strings.Contains(cleanVer, ":") {
		return "", fmt.Errorf("unscoped revision version %q cannot contain ':'", cleanVer)
	}
	storedVersion := formatScopedVersion(chainScope, cleanVer)
	if len(storedVersion) > maxStoredVersionLen {
		return "", fmt.Errorf("stored revision version %q exceeds maximum length %d", storedVersion, maxStoredVersionLen)
	}
	return storedVersion, nil
}

func formatScopedVersion(scope, version string) string {
	if scope == "" {
		return version
	}
	prefix := scope + ":"
	if strings.HasPrefix(version, prefix) {
		return version
	}
	return prefix + version
}

func parseScopedVersion(scope, stored string) (string, bool) {
	if scope == "" {
		if strings.Contains(stored, ":") {
			return "", false
		}
		return stored, true
	}
	prefix := scope + ":"
	if !strings.HasPrefix(stored, prefix) {
		return "", false
	}
	return strings.TrimPrefix(stored, prefix), true
}

type asyncJobStatus struct {
	state     string
	indexName string
	msg       string
	maxJobID  int64
}

func (e *Executor) queryLatestAlterState(ctx context.Context, query, showPrefix, expectedIndex string, minJobID int64, snapshotOnly bool) (asyncJobStatus, error) {
	rows, err := e.querier.QueryRowsContext(ctx, query)
	if err != nil {
		return asyncJobStatus{}, err
	}
	if rows == nil {
		return asyncJobStatus{}, fmt.Errorf("SHOW query returned nil rows")
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return asyncJobStatus{}, err
	}
	stateIdx := -1
	jobIDIdx := -1
	msgIdx := -1
	indexNameIdx := -1
	alterInvertedIdx := -1
	for i, c := range cols {
		switch {
		case strings.EqualFold(c, "State") || strings.EqualFold(c, "Status"):
			stateIdx = i
		case strings.EqualFold(c, "JobId"):
			jobIDIdx = i
		case strings.EqualFold(c, "Msg") || strings.EqualFold(c, "Message"):
			msgIdx = i
		case strings.EqualFold(c, "IndexName"):
			indexNameIdx = i
		case strings.EqualFold(c, "AlterInvertedIndexes") || strings.EqualFold(c, "Alter Inverted Index"):
			alterInvertedIdx = i
		}
	}

	type jobRow struct {
		jobID         string
		parsedJobID   int64
		hasNumericID  bool
		state         string
		indexName     string
		alterInverted string
		msg           string
	}
	var allRows []jobRow
	vals := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range vals {
		scanArgs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return asyncJobStatus{}, err
		}
		jid := ""
		var parsedID int64
		hasNum := false
		if jobIDIdx >= 0 {
			jid = strings.TrimSpace(string(vals[jobIDIdx]))
			if n, parseErr := strconv.ParseInt(jid, 10, 64); parseErr == nil {
				parsedID = n
				hasNum = true
			}
		}
		idxName := ""
		if indexNameIdx >= 0 {
			idxName = strings.TrimSpace(string(vals[indexNameIdx]))
		}
		altIdx := ""
		if alterInvertedIdx >= 0 {
			altIdx = strings.TrimSpace(string(vals[alterInvertedIdx]))
		}
		msg := ""
		if msgIdx >= 0 {
			msg = strings.TrimSpace(string(vals[msgIdx]))
		}
		stVal := ""
		if stateIdx >= 0 {
			stVal = strings.TrimSpace(string(vals[stateIdx]))
		}
		allRows = append(allRows, jobRow{
			jobID:         jid,
			parsedJobID:   parsedID,
			hasNumericID:  hasNum,
			state:         stVal,
			indexName:     idxName,
			alterInverted: altIdx,
			msg:           msg,
		})
	}
	if err := rows.Err(); err != nil {
		return asyncJobStatus{}, err
	}
	if len(allRows) == 0 {
		return asyncJobStatus{}, nil
	}
	if !snapshotOnly && stateIdx < 0 {
		return asyncJobStatus{}, fmt.Errorf("SHOW result returned %d row(s) without State or Status column (cols=%v)", len(allRows), cols)
	}

	// Filter rows by target index name for SHOW BUILD INDEX when index metadata columns are populated.
	filteredRows := allRows
	if showPrefix == "SHOW BUILD INDEX" && expectedIndex != "" && (indexNameIdx >= 0 || alterInvertedIdx >= 0) {
		anyIndexMeta := false
		for _, r := range allRows {
			if r.indexName != "" || r.alterInverted != "" {
				anyIndexMeta = true
				break
			}
		}
		if anyIndexMeta {
			var idxMatches []jobRow
			for _, r := range allRows {
				if rowMatchesIndexName(r.indexName, r.alterInverted, expectedIndex) {
					idxMatches = append(idxMatches, r)
				}
			}
			filteredRows = idxMatches
		}
	}

	var maxJobID int64
	for _, r := range filteredRows {
		if r.hasNumericID && r.parsedJobID > maxJobID {
			maxJobID = r.parsedJobID
		}
	}
	if snapshotOnly {
		return asyncJobStatus{maxJobID: maxJobID}, nil
	}

	// Apply baseline JobId watermark filter when > 0 and numeric JobIds exist.
	if minJobID > 0 && jobIDIdx >= 0 {
		var newer []jobRow
		for _, r := range filteredRows {
			if r.hasNumericID && r.parsedJobID > minJobID {
				newer = append(newer, r)
			}
		}
		filteredRows = newer
	}
	if len(filteredRows) == 0 {
		// No newer async job after baselineJobID -> synchronous or no-op completion.
		return asyncJobStatus{state: "FINISHED", maxJobID: maxJobID}, nil
	}

	// Inspect all matching post-baseline rows across all partitions/jobs so multi-partition
	// BUILD INDEX jobs with distinct per-partition JobIds never mask an in-progress or failed job.
	var nonFinishedStatus asyncJobStatus
	var finishedStatus asyncJobStatus
	hasFinished := false
	var latestStatus asyncJobStatus
	for _, r := range filteredRows {
		st := asyncJobStatus{state: r.state, indexName: r.indexName, msg: r.msg, maxJobID: maxJobID}
		latestStatus = st
		upper := strings.ToUpper(strings.TrimSpace(r.state))
		switch upper {
		case "CANCELLED", "CANCELED", "FAILED":
			return st, nil
		case "FINISHED":
			finishedStatus = st
			hasFinished = true
		case "":
		default:
			if nonFinishedStatus.state == "" {
				nonFinishedStatus = st
			}
		}
	}
	if nonFinishedStatus.state != "" {
		return nonFinishedStatus, nil
	}
	if hasFinished {
		return finishedStatus, nil
	}
	return latestStatus, nil
}

func rowMatchesIndexName(indexName, alterInverted, expected string) bool {
	if strings.EqualFold(strings.Trim(strings.TrimSpace(indexName), "`"), expected) {
		return true
	}
	if alterInverted == "" {
		return false
	}
	return containsExactIdentToken(alterInverted, expected)
}

func containsExactIdentToken(text, target string) bool {
	lowerText := strings.ToLower(text)
	lowerTarget := strings.ToLower(strings.Trim(strings.TrimSpace(target), "`"))
	if lowerTarget == "" {
		return false
	}
	start := 0
	for {
		idx := strings.Index(lowerText[start:], lowerTarget)
		if idx < 0 {
			return false
		}
		pos := start + idx
		end := pos + len(lowerTarget)
		leftOK := pos == 0 || !isIdentChar(lowerText[pos-1])
		rightOK := end == len(lowerText) || !isIdentChar(lowerText[end])
		if leftOK && rightOK {
			return true
		}
		start = pos + 1
	}
}

func isIdentChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_'
}

var templateVarPlaceholderRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// SubstituteVars replaces all ${KEY} template placeholders in sqlText using vars,
// enforcing strict SQL injection validation on all variable names and values.
// Returns an error if any placeholder remains unresolved or any value is unsafe.
func SubstituteVars(sqlText string, vars map[string]string) (string, error) {
	normalizedVars := make(map[string]string, len(vars))
	for k, v := range vars {
		key := strings.TrimSpace(k)
		if !isValidVarName(key) {
			return "", fmt.Errorf("invalid template variable name %q", k)
		}
		cleanVal, err := ValidateAndNormalizeVarValue(key, v)
		if err != nil {
			return "", err
		}
		normalizedVars[key] = cleanVal
	}

	var unresolved []string
	result := templateVarPlaceholderRe.ReplaceAllStringFunc(sqlText, func(match string) string {
		key := match[2 : len(match)-1]
		if val, ok := normalizedVars[key]; ok {
			return val
		}
		unresolved = append(unresolved, key)
		return match
	})

	if len(unresolved) > 0 {
		return "", fmt.Errorf("unresolved template variable(s): %s", strings.Join(dedupStrings(unresolved), ", "))
	}
	if strings.Contains(result, "${") {
		return "", fmt.Errorf("malformed or unresolved template placeholder in SQL")
	}
	return result, nil
}

// ValidateAndNormalizeVarValue validates a template variable value against SQL injection
// and normalizes special variables such as TENANT_SLUG and TENANT_BUCKETS.
func ValidateAndNormalizeVarValue(key, val string) (string, error) {
	v := strings.TrimSpace(val)
	if v == "" {
		return "", fmt.Errorf("template variable %s cannot be empty", key)
	}
	for i := 0; i < len(v); i++ {
		ch := v[i]
		if ch < 0x20 || ch > 0x7e {
			return "", fmt.Errorf("template variable %s contains non-printable character", key)
		}
		switch ch {
		case '"', '\'', '`', '\\', ';', '#':
			return "", fmt.Errorf("template variable %s contains forbidden SQL metacharacter %q", key, string(ch))
		}
	}
	if strings.Contains(v, "--") || strings.Contains(v, "/*") || strings.Contains(v, "*/") {
		return "", fmt.Errorf("template variable %s contains forbidden SQL comment token", key)
	}

	switch strings.ToUpper(key) {
	case "TENANT_SLUG":
		slug := strings.ReplaceAll(v, "-", "_")
		if !isValidVarName(slug) {
			return "", fmt.Errorf("TENANT_SLUG %q must be a valid SQL identifier ([a-zA-Z_][a-zA-Z0-9_-]*)", val)
		}
		return slug, nil
	case "TENANT_BUCKETS":
		if strings.EqualFold(v, "AUTO") {
			return "AUTO", nil
		}
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return "", fmt.Errorf("TENANT_BUCKETS %q must be a positive integer or AUTO", val)
		}
		return strconv.Itoa(n), nil
	default:
		if !genericTemplateVarValueRe.MatchString(v) {
			return "", fmt.Errorf("template variable %s value %q must match ^[A-Za-z0-9._-]+$", key, val)
		}
	}
	return v, nil
}

func isValidVarName(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		isAlpha := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
		isDigit := ch >= '0' && ch <= '9'
		if i == 0 && !isAlpha {
			return false
		}
		if !isAlpha && !isDigit {
			return false
		}
	}
	return true
}

func isSafeQualifiedIdentifier(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) == 0 || len(parts) > 2 {
		return false
	}
	for _, p := range parts {
		if !isValidVarName(p) {
			return false
		}
	}
	return true
}

func isSafeQualifiedTableIdentifier(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) == 0 || len(parts) > 3 {
		return false
	}
	for _, p := range parts {
		if !isValidVarName(p) {
			return false
		}
	}
	return true
}

func parseScannedTime(raw any) time.Time {
	switch v := raw.(type) {
	case time.Time:
		return v.UTC()
	case []byte:
		return parseTimeString(string(v))
	case string:
		return parseTimeString(v)
	default:
		return time.Time{}
	}
}

func parseTimeString(s string) time.Time {
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func dedupStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
