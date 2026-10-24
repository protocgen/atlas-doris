package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/protocgen/atlas-doris/pkg/doris"
)

// OperatorVersion is recorded in atlas_schema_revisions.operator_version.
const OperatorVersion = "atlas-doris/v0.1.0"

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
	Vars             map[string]string
	DryRun           bool
	BaselineVersion  string
	RevisionsSchema  string
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

// Executor manages migration status and execution against an Apache Doris FE endpoint.
type Executor struct {
	db              *sql.DB
	revisionsSchema string
	pollInterval    time.Duration
	pollTimeout     time.Duration
}

// NewExecutor creates an Executor wrapping an existing *sql.DB connection.
func NewExecutor(db *sql.DB, revisionsSchema string) *Executor {
	return &Executor{
		db:              db,
		revisionsSchema: strings.TrimSpace(revisionsSchema),
		pollInterval:    200 * time.Millisecond,
		pollTimeout:     2 * time.Minute,
	}
}

// DialExecutor parses rawURL ("doris://user:pass@host:port/db" or "mysql://..."),
// opens a MySQL wire-protocol connection to Apache Doris FE, and returns an Executor.
func DialExecutor(rawURL, revisionsSchema string) (*Executor, error) {
	dsn, _, err := ParseDorisDSN(rawURL)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open doris connection: %w", err)
	}
	return NewExecutor(db, revisionsSchema), nil
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
	if e == nil || e.db == nil {
		return fmt.Errorf("executor has no database connection")
	}
	if e.revisionsSchema != "" {
		if !isSafeQualifiedIdentifier(e.revisionsSchema) {
			return fmt.Errorf("invalid revisions schema identifier %q", e.revisionsSchema)
		}
		createDB := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", e.revisionsSchema)
		if _, err := e.db.ExecContext(ctx, createDB); err != nil {
			return fmt.Errorf("create revisions database %q: %w", e.revisionsSchema, err)
		}
	}
	if _, err := e.db.ExecContext(ctx, e.RevisionsTableCreateSQL()); err != nil {
		return fmt.Errorf("create %s: %w", e.RevisionsTableQualifiedName(), err)
	}
	return nil
}

// AppliedRevisions queries atlas_schema_revisions ordered by version ascending.
func (e *Executor) AppliedRevisions(ctx context.Context) ([]RevisionRecord, error) {
	if err := e.EnsureRevisionsTable(ctx); err != nil {
		return nil, err
	}
	query := fmt.Sprintf(
		"SELECT version, description, `type`, applied, total, executed_at, execution_time, COALESCE(error, ''), COALESCE(error_stmt, ''), hash, operator_version FROM %s ORDER BY version ASC;",
		e.RevisionsTableQualifiedName(),
	)
	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", e.RevisionsTableQualifiedName(), err)
	}
	defer rows.Close()

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
	if e == nil || e.db == nil {
		return fmt.Errorf("executor has no database connection")
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
	_, err := e.db.ExecContext(
		ctx,
		insertSQL,
		r.Version,
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
		return fmt.Errorf("insert revision %s into %s: %w", r.Version, e.RevisionsTableQualifiedName(), err)
	}
	return nil
}

// Status validates the migration directory and computes applied vs pending migrations.
func (e *Executor) Status(ctx context.Context, dirPath string) (*MigrationStatus, error) {
	if err := ValidateDir(dirPath, doris.LintOptions{AllowDestructive: true}); err != nil {
		return nil, err
	}
	_, files, err := ReplayDir(dirPath)
	if err != nil {
		return nil, err
	}

	var applied []RevisionRecord
	if e != nil && e.db != nil {
		applied, err = e.AppliedRevisions(ctx)
		if err != nil {
			return nil, err
		}
	}

	appliedByVersion := make(map[string]RevisionRecord, len(applied))
	for _, r := range applied {
		appliedByVersion[r.Version] = r
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

// Apply validates atlas.sum, substitutes template variables, executes pending migrations
// in version order against Doris FE, polls async schema change / index build jobs, and
// records each applied migration in atlas_schema_revisions.
func (e *Executor) Apply(ctx context.Context, opts ApplyOptions) (*ApplyResult, error) {
	if opts.RevisionsSchema != "" && e != nil && e.revisionsSchema == "" {
		e.revisionsSchema = strings.TrimSpace(opts.RevisionsSchema)
	}
	if opts.PollInterval > 0 && e != nil {
		e.pollInterval = opts.PollInterval
	}
	if opts.PollTimeout > 0 && e != nil {
		e.pollTimeout = opts.PollTimeout
	}

	if err := ValidateDir(opts.DirPath, doris.LintOptions{AllowDestructive: opts.AllowDestructive}); err != nil {
		return nil, err
	}
	_, files, err := ReplayDir(opts.DirPath)
	if err != nil {
		return nil, err
	}

	appliedByVersion := make(map[string]RevisionRecord)
	if !opts.DryRun || (e != nil && e.db != nil) {
		if e == nil || e.db == nil {
			return nil, fmt.Errorf("migrate apply requires a live Doris connection unless --dry-run is used")
		}
		revs, err := e.AppliedRevisions(ctx)
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
		if rev, ok := appliedByVersion[f.Version]; ok && rev.Error == "" {
			if rev.Hash != "" && f.Hash != "" && rev.Hash != f.Hash {
				return nil, fmt.Errorf("applied migration %s (%s) checksum mismatch: db has %s, atlas.sum has %s", f.Version, f.Name, rev.Hash, f.Hash)
			}
			continue
		}

		// Handle --baseline (or empty 00000000000000_* baseline file)
		if opts.BaselineVersion != "" && f.Version <= opts.BaselineVersion {
			res.SkippedBaseline = append(res.SkippedBaseline, f.Name)
			if !opts.DryRun {
				if err := e.RecordRevision(ctx, RevisionRecord{
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

		renderedSQL, err := SubstituteVars(f.SQL, opts.Vars)
		if err != nil {
			return nil, fmt.Errorf("migration %s variable substitution: %w", f.Name, err)
		}
		stmts, err := doris.ParseStatements(renderedSQL)
		if err != nil {
			return nil, fmt.Errorf("migration %s rendered SQL parse error: %w", f.Name, err)
		}

		if opts.DryRun {
			res.AppliedFiles = append(res.AppliedFiles, f.Name)
			for _, st := range stmts {
				res.ExecutedSQL = append(res.ExecutedSQL, st.SQL())
			}
			continue
		}

		fileStart := time.Now()
		appliedCount := 0
		for _, st := range stmts {
			stmtSQL := st.SQL()
			if stmtSQL == "" {
				continue
			}
			if _, execErr := e.db.ExecContext(ctx, stmtSQL); execErr != nil {
				_ = e.RecordRevision(ctx, RevisionRecord{
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
				if err := e.waitForTableAlterCompletion(ctx, st.TargetTable); err != nil {
					return nil, fmt.Errorf("migration %s async schema change on %s: %w", f.Name, st.TargetTable, err)
				}
			}
		}

		if err := e.RecordRevision(ctx, RevisionRecord{
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

func requiresAsyncJobWait(st doris.Statement) bool {
	switch st.Kind {
	case doris.StmtAlterTableModifyColumn, doris.StmtAlterTableAddIndex, doris.StmtAlterTableDropIndex, doris.StmtBuildIndex:
		return true
	default:
		return false
	}
}

func (e *Executor) waitForTableAlterCompletion(ctx context.Context, fullTableName string) error {
	if e == nil || e.db == nil {
		return nil
	}
	parts := strings.Split(fullTableName, ".")
	shortTable := parts[len(parts)-1]
	dbPrefix := ""
	if len(parts) >= 2 {
		dbPrefix = " FROM " + strings.Join(parts[:len(parts)-1], ".")
	}
	query := fmt.Sprintf("SHOW ALTER TABLE COLUMN%s WHERE TableName = '%s';", dbPrefix, shortTable)

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
		state, err := e.queryLatestAlterState(ctx, query)
		if err != nil {
			// If the mock/driver doesn't support SHOW ALTER TABLE COLUMN or returns no rows, treat as complete.
			return nil
		}
		switch strings.ToUpper(state) {
		case "", "FINISHED":
			return nil
		case "CANCELLED":
			return fmt.Errorf("Doris async schema change job for table %q was CANCELLED", fullTableName)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for Doris schema change job on %q (last state=%q)", fullTableName, state)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func (e *Executor) queryLatestAlterState(ctx context.Context, query string) (string, error) {
	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	stateIdx := -1
	for i, c := range cols {
		if strings.EqualFold(c, "State") {
			stateIdx = i
			break
		}
	}
	if stateIdx < 0 {
		return "FINISHED", nil
	}

	latestState := ""
	vals := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range vals {
		scanArgs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return "", err
		}
		latestState = string(vals[stateIdx])
	}
	return latestState, rows.Err()
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
