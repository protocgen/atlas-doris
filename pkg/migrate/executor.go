package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/protocgen/atlas-doris/pkg/doris"
)

// OperatorVersion is recorded in atlas_schema_revisions.operator_version.
const OperatorVersion = "atlas-doris/v0.3.0"

// ErrAsyncJobPending is returned in NonBlocking mode when an asynchronous Doris
// schema change or index build job is still in progress (PENDING, WAITING_TXN, or RUNNING).
var ErrAsyncJobPending = errors.New("doris async job still pending")

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
	DirPath          string
	FS               fs.FS
	Vars             map[string]string
	DryRun           bool
	BaselineVersion  string
	RevisionsSchema  string
	ChainScope       string
	IcebergCatalog   string
	NonBlocking      bool
	AllowDestructive bool
	PollInterval     time.Duration
	PollTimeout      time.Duration
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

// Executor manages migration status and execution against an Apache Doris FE endpoint.
type Executor struct {
	db              *sql.DB
	querier         QueryExecutor
	revisionsSchema string
	chainScope      string
	icebergCatalog  string
	nonBlocking     bool
	coupledMode     *atomic.Bool
	pollInterval    time.Duration
	pollTimeout     time.Duration
}

// NewExecutor creates an Executor wrapping an existing *sql.DB connection.
func NewExecutor(db *sql.DB, revisionsSchema string) *Executor {
	var q QueryExecutor
	if db != nil {
		q = sqlDBQuerier{db: db}
	}
	return &Executor{
		db:              db,
		querier:         q,
		revisionsSchema: strings.TrimSpace(revisionsSchema),
		coupledMode:     &atomic.Bool{},
		pollInterval:    200 * time.Millisecond,
		pollTimeout:     2 * time.Minute,
	}
}

// NewExecutorWithQuerier creates an Executor backed by a custom QueryExecutor.
func NewExecutorWithQuerier(q QueryExecutor, revisionsSchema string, opts ...ExecutorOption) *Executor {
	exec := &Executor{
		querier:         q,
		revisionsSchema: strings.TrimSpace(revisionsSchema),
		coupledMode:     &atomic.Bool{},
		pollInterval:    200 * time.Millisecond,
		pollTimeout:     2 * time.Minute,
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
		return "", "", fmt.Errorf("invalid Doris URL %q: %w", rawURL, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "doris" && scheme != "mysql" {
		return "", "", fmt.Errorf("unsupported URL scheme %q (expected doris:// or mysql://)", u.Scheme)
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("missing host in Doris URL %q", rawURL)
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
	if q.Get("parseTime") == "" {
		q.Set("parseTime", "true")
	}
	if q.Get("interpolateParams") == "" {
		q.Set("interpolateParams", "true")
	}

	authPart := user
	if pass != "" {
		authPart = user + ":" + pass
	}
	dsn = fmt.Sprintf("%s@tcp(%s)/%s", authPart, host, dbName)
	if encoded := q.Encode(); encoded != "" {
		dsn += "?" + encoded
	}
	return dsn, dbName, nil
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
	return e.queryAppliedRevisions(ctx)
}

func (e *Executor) queryAppliedRevisions(ctx context.Context) ([]RevisionRecord, error) {
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
			"SELECT version, description, `type`, applied, total, executed_at, execution_time, COALESCE(error, ''), COALESCE(error_stmt, ''), hash, operator_version FROM %s WHERE version LIKE ? ORDER BY version ASC;",
			e.RevisionsTableQualifiedName(),
		)
		args = []any{e.chainScope + ":%"}
	} else {
		query = fmt.Sprintf(
			"SELECT version, description, `type`, applied, total, executed_at, execution_time, COALESCE(error, ''), COALESCE(error_stmt, ''), hash, operator_version FROM %s ORDER BY version ASC;",
			e.RevisionsTableQualifiedName(),
		)
	}

	rows, err := e.querier.QueryRowsContext(ctx, query, args...)
	if err != nil {
		if isMissingTableOrDatabaseError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("query %s: %w", e.RevisionsTableQualifiedName(), err)
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
	if strings.TrimSpace(r.Version) == "" {
		return fmt.Errorf("revision version cannot be empty")
	}

	storedVersion := formatScopedVersion(e.chainScope, r.Version)
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
	_, err := e.querier.ExecContext(
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
	cleanVer := strings.TrimSpace(version)
	if cleanVer == "" {
		return fmt.Errorf("revision version cannot be empty")
	}
	storedVersion := formatScopedVersion(e.chainScope, cleanVer)
	delSQL := fmt.Sprintf("DELETE FROM %s WHERE version = ?;", e.RevisionsTableQualifiedName())
	if _, err := e.querier.ExecContext(ctx, delSQL, storedVersion); err != nil {
		return fmt.Errorf("delete revision %s from %s: %w", storedVersion, e.RevisionsTableQualifiedName(), err)
	}
	return nil
}

// StatusFS validates the migration fs.FS and computes applied vs pending migrations without mutating the database.
func (e *Executor) StatusFS(ctx context.Context, fsys fs.FS) (*MigrationStatus, error) {
	if err := ValidateFS(fsys, doris.LintOptions{AllowDestructive: true}); err != nil {
		return nil, err
	}
	_, files, err := ReplayFS(fsys)
	if err != nil {
		return nil, err
	}
	return e.statusFromFiles(ctx, files)
}

// Status validates the migration directory and computes applied vs pending migrations without mutating the database.
func (e *Executor) Status(ctx context.Context, dirPath string) (*MigrationStatus, error) {
	if err := ValidateDir(dirPath, doris.LintOptions{AllowDestructive: true}); err != nil {
		return nil, err
	}
	_, files, err := ReplayDir(dirPath)
	if err != nil {
		return nil, err
	}
	return e.statusFromFiles(ctx, files)
}

func (e *Executor) statusFromFiles(ctx context.Context, files []MigrationFile) (*MigrationStatus, error) {
	var (
		applied []RevisionRecord
		err     error
	)
	if e != nil {
		if err := validateChainScope(e.chainScope); err != nil {
			return nil, err
		}
		if e.querier != nil {
			applied, err = e.queryAppliedRevisions(ctx)
			if err != nil {
				return nil, err
			}
		}
	}

	appliedByVersion := make(map[string]RevisionRecord, len(applied))
	maxAppliedVersion := ""
	for _, r := range applied {
		appliedByVersion[r.Version] = r
		if r.Error == "" && r.Version > maxAppliedVersion {
			maxAppliedVersion = r.Version
		}
	}

	chainScope := ""
	if e != nil {
		chainScope = e.chainScope
	}

	var pending []MigrationFile
	for _, f := range files {
		if rev, ok := appliedByVersion[f.Version]; ok {
			if rev.Error == "" && rev.Hash != "" && f.Hash != "" && rev.Hash != f.Hash {
				return nil, fmt.Errorf("applied migration %s (%s) checksum mismatch: db has %s, atlas.sum has %s", f.Version, f.Name, rev.Hash, f.Hash)
			}
			if rev.Error == "" {
				continue
			}
		} else if isEmptyTenantBaseline(f) && (chainScope != "" || maxAppliedVersion > f.Version) {
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
	if len(applied) > 0 {
		st.Current = applied[len(applied)-1].Version
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
	}
	if cfg.coupledMode == nil {
		cfg.coupledMode = &atomic.Bool{}
	}
	if s := strings.TrimSpace(opts.RevisionsSchema); s != "" && cfg.revisionsSchema == "" {
		cfg.revisionsSchema = s
	}
	if s := strings.TrimSpace(opts.ChainScope); s != "" {
		cfg.chainScope = s
	}
	if s := strings.TrimSpace(opts.IcebergCatalog); s != "" {
		cfg.icebergCatalog = s
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
		if err := ValidateFS(opts.FS, lintOpts); err != nil {
			return nil, err
		}
		_, files, err = ReplayFS(opts.FS)
		if err != nil {
			return nil, err
		}
	} else {
		if err := ValidateDir(opts.DirPath, lintOpts); err != nil {
			return nil, err
		}
		_, files, err = ReplayDir(opts.DirPath)
		if err != nil {
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
		revs, err := execCfg.queryAppliedRevisions(ctx)
		if err != nil {
			return nil, err
		}
		for _, r := range revs {
			appliedByVersion[r.Version] = r
		}
	} else if execCfg.querier != nil {
		revs, err := execCfg.queryAppliedRevisions(ctx)
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
		if rev, ok := appliedByVersion[f.Version]; ok {
			if rev.Hash != "" && f.Hash != "" && rev.Hash != f.Hash {
				return nil, fmt.Errorf("applied migration %s (%s) checksum mismatch: db has %s, atlas.sum has %s", f.Version, f.Name, rev.Hash, f.Hash)
			}
			if rev.Error == "" {
				continue
			}
			if rev.Applied > 0 {
				resumeOffset = rev.Applied
			}
			pendingAsyncWait = isPendingAsyncError(rev.Error)
		}

		// Explicit --baseline skips and records the baseline version.
		if opts.BaselineVersion != "" && f.Version <= opts.BaselineVersion {
			res.SkippedBaseline = append(res.SkippedBaseline, f.Name)
			if !opts.DryRun {
				if err := execCfg.RecordRevision(ctx, RevisionRecord{
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
				if pendingAsyncWait && idx == 0 && requiresAsyncJobWait(st) {
					pendingAsyncWait = false
					continue
				}
				stmtSQL := execCfg.normalizeRuntimeStatement(st.SQL())
				if stmtSQL != "" {
					res.ExecutedSQL = append(res.ExecutedSQL, stmtSQL)
				}
			}
			continue
		}

		fileStart := time.Now()
		appliedCount := resumeOffset
		for idx, st := range stmts[resumeOffset:] {
			stmtSQL := execCfg.normalizeRuntimeStatement(st.SQL())
			if stmtSQL == "" {
				continue
			}
			if pendingAsyncWait && idx == 0 && requiresAsyncJobWait(st) {
				pendingAsyncWait = false
				if err := execCfg.waitForStatementJobCompletion(ctx, st); err != nil {
					_ = execCfg.RecordRevision(ctx, RevisionRecord{
						Version:         f.Version,
						Description:     f.Description,
						Type:            2,
						Applied:         appliedCount,
						Total:           len(stmts),
						ExecutedAt:      time.Now().UTC(),
						ExecutionTimeNs: time.Since(fileStart).Nanoseconds(),
						Error:           err.Error(),
						ErrorStmt:       stmtSQL,
						Hash:            f.Hash,
						OperatorVersion: OperatorVersion,
					})
					return nil, fmt.Errorf("migration %s async schema change on %s: %w", f.Name, st.TargetTable, err)
				}
				appliedCount++
				continue
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
				_ = execCfg.RecordRevision(ctx, RevisionRecord{
					Version:         f.Version,
					Description:     f.Description,
					Type:            2,
					Applied:         appliedCount,
					Total:           len(stmts),
					ExecutedAt:      time.Now().UTC(),
					ExecutionTimeNs: time.Since(fileStart).Nanoseconds(),
					Error:           execErr.Error(),
					ErrorStmt:       stmtSQL,
					Hash:            f.Hash,
					OperatorVersion: OperatorVersion,
				})
				return nil, fmt.Errorf("migration %s failed on %q: %w", f.Name, stmtSQL, execErr)
			}
			appliedCount++
			res.ExecutedSQL = append(res.ExecutedSQL, stmtSQL)

			if requiresAsyncJobWait(st) {
				if err := execCfg.waitForStatementJobCompletion(ctx, st); err != nil {
					_ = execCfg.RecordRevision(ctx, RevisionRecord{
						Version:         f.Version,
						Description:     f.Description,
						Type:            2,
						Applied:         appliedCount - 1,
						Total:           len(stmts),
						ExecutedAt:      time.Now().UTC(),
						ExecutionTimeNs: time.Since(fileStart).Nanoseconds(),
						Error:           err.Error(),
						ErrorStmt:       stmtSQL,
						Hash:            f.Hash,
						OperatorVersion: OperatorVersion,
					})
					return nil, fmt.Errorf("migration %s async schema change on %s: %w", f.Name, st.TargetTable, err)
				}
			}
		}

		if err := execCfg.RecordRevision(ctx, RevisionRecord{
			Version:         f.Version,
			Description:     f.Description,
			Type:            2,
			Applied:         appliedCount,
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

func requiresAsyncJobWait(st doris.Statement) bool {
	switch st.Kind {
	case doris.StmtAlterTableModifyColumn, doris.StmtAlterTableAddIndex, doris.StmtAlterTableDropIndex, doris.StmtBuildIndex:
		return true
	default:
		return false
	}
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

func (e *Executor) waitForStatementJobCompletion(ctx context.Context, st doris.Statement) error {
	if st.Kind == doris.StmtBuildIndex {
		return e.waitForAsyncDorisJob(ctx, st.TargetTable, statementIndexName(st), "SHOW BUILD INDEX")
	}
	return e.waitForAsyncDorisJob(ctx, st.TargetTable, statementIndexName(st), "SHOW ALTER TABLE COLUMN")
}

func (e *Executor) waitForAsyncDorisJob(ctx context.Context, fullTableName, indexName, showPrefix string) error {
	if e == nil || e.querier == nil {
		return nil
	}
	parts := strings.Split(fullTableName, ".")
	shortTable := strings.Trim(strings.TrimSpace(parts[len(parts)-1]), "`")
	dbPrefix := ""
	if len(parts) >= 2 {
		if showPrefix == "SHOW BUILD INDEX" {
			dbName := strings.Trim(strings.TrimSpace(parts[len(parts)-2]), "`")
			if dbName != "" {
				dbPrefix = " FROM " + dbName
			}
		} else {
			dbPrefix = " FROM " + strings.Join(parts[:len(parts)-1], ".")
		}
	}
	query := fmt.Sprintf("%s%s WHERE TableName = '%s';", showPrefix, dbPrefix, shortTable)

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
		status, err := e.queryLatestAlterState(ctx, query)
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
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unsupported") ||
		strings.Contains(msg, "not supported") ||
		strings.Contains(msg, "unknown command") ||
		strings.Contains(msg, "unrecognized")
}

func isMissingTableOrDatabaseError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "doesn't exist") ||
		strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "unknown database") ||
		strings.Contains(msg, "unknown table") ||
		strings.Contains(msg, "error 1146") ||
		strings.Contains(msg, "error 1049")
}

func isEmptyTenantBaseline(f MigrationFile) bool {
	return f.Name == "00000000000000_tenant.sql" && len(f.Statements) == 0
}

func isPendingAsyncError(errStr string) bool {
	return strings.Contains(strings.ToLower(errStr), strings.ToLower(ErrAsyncJobPending.Error()))
}

var chainScopeRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func validateChainScope(scope string) error {
	if scope == "" {
		return nil
	}
	if !chainScopeRe.MatchString(scope) {
		return fmt.Errorf("invalid chain scope %q: must match ^[A-Za-z0-9._-]+$", scope)
	}
	return nil
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
}

func (e *Executor) queryLatestAlterState(ctx context.Context, query string) (asyncJobStatus, error) {
	rows, err := e.querier.QueryRowsContext(ctx, query)
	if err != nil {
		return asyncJobStatus{}, err
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
	for i, c := range cols {
		switch {
		case strings.EqualFold(c, "State"):
			stateIdx = i
		case strings.EqualFold(c, "JobId"):
			jobIDIdx = i
		case strings.EqualFold(c, "Msg"):
			msgIdx = i
		case strings.EqualFold(c, "IndexName"):
			indexNameIdx = i
		}
	}
	if stateIdx < 0 {
		return asyncJobStatus{state: "FINISHED"}, nil
	}

	type jobRow struct {
		jobID     string
		state     string
		indexName string
		msg       string
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
		if jobIDIdx >= 0 {
			jid = strings.TrimSpace(string(vals[jobIDIdx]))
		}
		idxName := ""
		if indexNameIdx >= 0 {
			idxName = strings.TrimSpace(string(vals[indexNameIdx]))
		}
		msg := ""
		if msgIdx >= 0 {
			msg = strings.TrimSpace(string(vals[msgIdx]))
		}
		allRows = append(allRows, jobRow{
			jobID:     jid,
			state:     strings.TrimSpace(string(vals[stateIdx])),
			indexName: idxName,
			msg:       msg,
		})
	}
	if err := rows.Err(); err != nil {
		return asyncJobStatus{}, err
	}
	if len(allRows) == 0 {
		return asyncJobStatus{}, nil
	}

	targetJobID := allRows[len(allRows)-1].jobID
	if jobIDIdx >= 0 {
		var maxJobNum int64 = -1
		for _, r := range allRows {
			if n, err := strconv.ParseInt(r.jobID, 10, 64); err == nil {
				if n >= maxJobNum {
					maxJobNum = n
					targetJobID = r.jobID
				}
			} else if maxJobNum < 0 && r.jobID != "" {
				targetJobID = r.jobID
			}
		}
	}

	var nonFinishedStatus asyncJobStatus
	var latestStatus asyncJobStatus
	for _, r := range allRows {
		if jobIDIdx >= 0 && targetJobID != "" && r.jobID != targetJobID {
			continue
		}
		st := asyncJobStatus{state: r.state, indexName: r.indexName, msg: r.msg}
		latestStatus = st
		upper := strings.ToUpper(r.state)
		switch upper {
		case "CANCELLED", "CANCELED", "FAILED":
			return st, nil
		case "FINISHED":
		default:
			if nonFinishedStatus.state == "" {
				nonFinishedStatus = st
			}
		}
	}
	if nonFinishedStatus.state != "" {
		return nonFinishedStatus, nil
	}
	return latestStatus, nil
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
		case '"', '\'', '`', '\\', ';':
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
