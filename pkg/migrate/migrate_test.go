package migrate_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/protocgen/atlas-doris/pkg/doris"
	"github.com/protocgen/atlas-doris/pkg/migrate"
	"hegel.dev/go/hegel"
)

func TestAtlasSumByteForByteCompatibility(t *testing.T) {
	t.Run("standalone_baseline_vector", func(t *testing.T) {
		tmpDir := t.TempDir()
		if err := os.WriteFile(filepath.Join(tmpDir, "00000000000000_tenant.sql"), nil, 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if err := migrate.HashDir(tmpDir); err != nil {
			t.Fatalf("HashDir: %v", err)
		}
		gotSum, err := os.ReadFile(filepath.Join(tmpDir, "atlas.sum"))
		if err != nil {
			t.Fatalf("read atlas.sum: %v", err)
		}
		const wantLine = "00000000000000_tenant.sql h1:evZW1ZIPit+1wfVGyGJrH+pY/64U2PRFFkrjAMO93wg="
		if !strings.Contains(string(gotSum), wantLine) {
			t.Fatalf("atlas.sum missing expected line %q:\ngot:\n%s", wantLine, string(gotSum))
		}
	})

	realChainDir := "/Users/ab/code/azra/platform/schemas/tenant/directoryservice/migrations"
	if _, err := os.Stat(realChainDir); os.IsNotExist(err) {
		return
	}

	tmpDir := t.TempDir()
	entries, err := os.ReadDir(realChainDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", realChainDir, err)
	}
	var expectedSum []byte
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(realChainDir, e.Name()))
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", e.Name(), err)
		}
		if e.Name() == "atlas.sum" {
			expectedSum = data
			continue
		}
		if err := os.WriteFile(filepath.Join(tmpDir, e.Name()), data, 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", e.Name(), err)
		}
	}

	if err := migrate.HashDir(tmpDir); err != nil {
		t.Fatalf("HashDir(%s) failed: %v", tmpDir, err)
	}
	gotSum, err := os.ReadFile(filepath.Join(tmpDir, "atlas.sum"))
	if err != nil {
		t.Fatalf("read generated atlas.sum: %v", err)
	}
	if string(gotSum) != string(expectedSum) {
		t.Fatalf("atlas.sum is not byte-for-byte identical to standard atlas CLI:\nwant:\n%s\ngot:\n%s", string(expectedSum), string(gotSum))
	}
}

func TestDiffAndWrite_Validate_TamperDetection_AndLint(t *testing.T) {
	migDir := t.TempDir()

	// Create empty baseline 00000000000000_init.sql
	if err := os.WriteFile(filepath.Join(migDir, "00000000000000_init.sql"), nil, 0o644); err != nil {
		t.Fatalf("write baseline: %v", err)
	}
	if err := migrate.HashDir(migDir); err != nil {
		t.Fatalf("HashDir baseline: %v", err)
	}

	v1SQL := `
CREATE DATABASE IF NOT EXISTS internal.tenant_${TENANT_SLUG};

CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patients (
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(32) NOT NULL DEFAULT "",
    publication_generation BIGINT NOT NULL DEFAULT "0",
    INDEX idx_patients_mrn (mrn) USING INVERTED
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS ${TENANT_BUCKETS}
PROPERTIES (
    "storage_vault_name" = "azra_lakehouse_vault",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "publication_generation",
    "colocate_with" = "azra_patient_group"
);`

	s1, err := doris.ParseSchema(v1SQL)
	if err != nil {
		t.Fatalf("ParseSchema(v1SQL): %v", err)
	}

	f1, stmts1, err := migrate.DiffAndWrite(migDir, "initial_patients", s1, migrate.DiffWriteOptions{
		Version: "20261024000001",
	})
	if err != nil {
		t.Fatalf("DiffAndWrite v1 failed: %v", err)
	}
	if f1 != "20261024000001_initial_patients.sql" || len(stmts1) != 2 {
		t.Fatalf("unexpected v1 migration result: file=%q stmts=%d", f1, len(stmts1))
	}

	// Re-diffing against identical s1 must produce 0 statements and no new file
	fNoop, stmtsNoop, err := migrate.DiffAndWrite(migDir, "noop", s1, migrate.DiffWriteOptions{})
	if err != nil {
		t.Fatalf("DiffAndWrite noop failed: %v", err)
	}
	if fNoop != "" || len(stmtsNoop) != 0 {
		t.Fatalf("expected noop diff, got file=%q stmts=%d", fNoop, len(stmtsNoop))
	}

	// ValidateDir must succeed
	if err := migrate.ValidateDir(migDir, doris.LintOptions{}); err != nil {
		t.Fatalf("ValidateDir failed: %v", err)
	}

	// Evolve to v2: widen mrn to VARCHAR(64), add status column with default and inverted index
	v2SQL := `
CREATE DATABASE IF NOT EXISTS internal.tenant_${TENANT_SLUG};

CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patients (
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(64) NOT NULL DEFAULT "",
    publication_generation BIGINT NOT NULL DEFAULT "0",
    patient_status VARCHAR(32) NOT NULL DEFAULT "ACTIVE",
    INDEX idx_patients_mrn (mrn) USING INVERTED,
    INDEX idx_patients_status (patient_status) USING INVERTED
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS ${TENANT_BUCKETS}
PROPERTIES (
    "storage_vault_name" = "azra_lakehouse_vault",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "publication_generation",
    "colocate_with" = "azra_patient_group"
);`
	s2, err := doris.ParseSchema(v2SQL)
	if err != nil {
		t.Fatalf("ParseSchema(v2SQL): %v", err)
	}

	f2, stmts2, err := migrate.DiffAndWrite(migDir, "add_patient_status", s2, migrate.DiffWriteOptions{
		Version: "20261024000002",
	})
	if err != nil {
		t.Fatalf("DiffAndWrite v2 failed: %v", err)
	}
	if f2 != "20261024000002_add_patient_status.sql" || len(stmts2) != 4 {
		t.Fatalf("expected 4 statements (ADD COLUMN, MODIFY COLUMN, ADD INDEX, BUILD INDEX), got %d in %s", len(stmts2), f2)
	}

	if err := migrate.ValidateDir(migDir, doris.LintOptions{}); err != nil {
		t.Fatalf("ValidateDir after v2 failed: %v", err)
	}

	// Tamper with v1 file without updating atlas.sum -> ValidateDir must fail
	v1Path := filepath.Join(migDir, f1)
	origV1, err := os.ReadFile(v1Path)
	if err != nil {
		t.Fatalf("read v1: %v", err)
	}
	if err := os.WriteFile(v1Path, append(origV1, []byte("\n-- tampered\n")...), 0o644); err != nil {
		t.Fatalf("tamper v1: %v", err)
	}
	if err := migrate.ValidateDir(migDir, doris.LintOptions{}); err == nil {
		t.Fatalf("expected ValidateDir to fail after tampering with %s", f1)
	}
	// Restore v1
	if err := os.WriteFile(v1Path, origV1, 0o644); err != nil {
		t.Fatalf("restore v1: %v", err)
	}
	if err := migrate.ValidateDir(migDir, doris.LintOptions{}); err != nil {
		t.Fatalf("ValidateDir after restore failed: %v", err)
	}
}

func TestSubstituteVars_AndPropertyInjectionSafety(t *testing.T) {
	sqlTpl := "CREATE DATABASE IF NOT EXISTS internal.tenant_${TENANT_SLUG};\nCREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patients (patient_id VARCHAR(64) NOT NULL) UNIQUE KEY(patient_id) DISTRIBUTED BY HASH(patient_id) BUCKETS ${TENANT_BUCKETS};"

	rendered, err := migrate.SubstituteVars(sqlTpl, map[string]string{
		"TENANT_SLUG":    "duke-health",
		"TENANT_BUCKETS": "16",
	})
	if err != nil {
		t.Fatalf("SubstituteVars failed: %v", err)
	}
	if !strings.Contains(rendered, "internal.tenant_duke_health.patients") || !strings.Contains(rendered, "BUCKETS 16") {
		t.Fatalf("unexpected rendered SQL: %s", rendered)
	}

	// Missing var fails
	if _, err := migrate.SubstituteVars(sqlTpl, map[string]string{"TENANT_SLUG": "duke"}); err == nil {
		t.Fatalf("expected error when TENANT_BUCKETS is missing")
	}

	// Property test: SubstituteVars never allows injection metacharacters
	hegel.Test(t, func(ht *hegel.T) {
		val := hegel.Draw(ht, hegel.Text().MaxSize(64))
		res, err := migrate.ValidateAndNormalizeVarValue("TENANT_SLUG", val)
		if err == nil {
			if strings.ContainsAny(res, ";'\"`\\\n\r") {
				ht.Fatalf("ValidateAndNormalizeVarValue accepted injection char in %q -> %q", val, res)
			}
		}
	})
}

func TestParseDorisDSN(t *testing.T) {
	dsn, db, err := migrate.ParseDorisDSN("doris://admin:secret@doris-fe.internal:9030/tenant_duke")
	if err != nil {
		t.Fatalf("ParseDorisDSN failed: %v", err)
	}
	if db != "tenant_duke" {
		t.Errorf("expected db=tenant_duke, got %q", db)
	}
	if !strings.HasPrefix(dsn, "admin:secret@tcp(doris-fe.internal:9030)/tenant_duke?") {
		t.Errorf("unexpected DSN: %q", dsn)
	}
}

// In-process mock SQL driver to test Executor end-to-end (revisions table, async polling, status, apply)
type mockDorisDriver struct {
	mu                       sync.Mutex
	executedSQL              []string
	revisions                []migrate.RevisionRecord
	alterPolls               int
	buildIndexPolls          int
	alterCancel              bool
	alterState               string
	buildIndexState          string
	failOnSubstr             string
	showPollError            string
	multiPartitionBuildIndex bool
	disableShowCreateTable   bool
}

var (
	registerMockOnce sync.Once
	activeMock       *mockDorisDriver
)

func setupMockDB(t *testing.T) (*sql.DB, *mockDorisDriver) {
	t.Helper()
	registerMockOnce.Do(func() {
		sql.Register("mockdoris", &mockDriverEntry{})
	})
	m := &mockDorisDriver{}
	activeMock = m
	db, err := sql.Open("mockdoris", "mock")
	if err != nil {
		t.Fatalf("sql.Open(mockdoris): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, m
}

type mockDriverEntry struct{}

func (d *mockDriverEntry) Open(name string) (driver.Conn, error) {
	return &mockConn{m: activeMock}, nil
}

type mockConn struct {
	m *mockDorisDriver
}

func (c *mockConn) Prepare(query string) (driver.Stmt, error) {
	return &mockStmt{c: c, query: query}, nil
}
func (c *mockConn) Close() error              { return nil }
func (c *mockConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("not supported") }

type mockStmt struct {
	c     *mockConn
	query string
}

func (s *mockStmt) Close() error  { return nil }
func (s *mockStmt) NumInput() int { return -1 }

func (s *mockStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.c.m.mu.Lock()
	defer s.c.m.mu.Unlock()

	q := strings.TrimSpace(s.query)
	s.c.m.executedSQL = append(s.c.m.executedSQL, q)

	if s.c.m.failOnSubstr != "" && strings.Contains(q, s.c.m.failOnSubstr) {
		return nil, fmt.Errorf("simulated Doris execution failure on %s", s.c.m.failOnSubstr)
	}

	if strings.HasPrefix(q, "INSERT INTO ") && strings.Contains(q, "atlas_schema_revisions") && len(args) >= 10 {
		rec := migrate.RevisionRecord{
			Version:         fmt.Sprint(args[0]),
			Description:     fmt.Sprint(args[1]),
			Type:            int(args[2].(int64)),
			Applied:         int(args[3].(int64)),
			Total:           int(args[4].(int64)),
			ExecutedAt:      time.Now().UTC(),
			ExecutionTimeNs: args[6].(int64),
			Hash:            fmt.Sprint(args[9]),
			OperatorVersion: fmt.Sprint(args[10]),
		}
		if args[7] != nil {
			rec.Error = fmt.Sprint(args[7])
		}
		if args[8] != nil {
			rec.ErrorStmt = fmt.Sprint(args[8])
		}
		// Upsert by Version
		replaced := false
		for i, existing := range s.c.m.revisions {
			if existing.Version == rec.Version {
				s.c.m.revisions[i] = rec
				replaced = true
				break
			}
		}
		if !replaced {
			s.c.m.revisions = append(s.c.m.revisions, rec)
		}
	}
	return driver.RowsAffected(1), nil
}

func (s *mockStmt) Query(args []driver.Value) (driver.Rows, error) {
	s.c.m.mu.Lock()
	defer s.c.m.mu.Unlock()

	q := strings.TrimSpace(s.query)
	s.c.m.executedSQL = append(s.c.m.executedSQL, q)

	if strings.HasPrefix(q, "SHOW ALTER TABLE COLUMN") {
		s.c.m.alterPolls++
		state := "FINISHED"
		if s.c.m.alterState != "" {
			state = s.c.m.alterState
		} else if s.c.m.alterCancel {
			state = "CANCELLED"
		} else if s.c.m.alterPolls == 1 {
			state = "RUNNING"
		}
		return &mockRows{
			cols: []string{"JobId", "TableName", "State"},
			rows: [][]driver.Value{{"1001", "patients", []byte(state)}},
		}, nil
	}

	if strings.HasPrefix(q, "SHOW BUILD INDEX") {
		s.c.m.buildIndexPolls++
		if s.c.m.showPollError != "" {
			return nil, fmt.Errorf("%s", s.c.m.showPollError)
		}
		if s.c.m.multiPartitionBuildIndex {
			p1State := "RUNNING"
			if s.c.m.buildIndexPolls >= 2 {
				p1State = "FINISHED"
			}
			return &mockRows{
				cols: []string{"JobId", "TableName", "PartitionName", "State"},
				rows: [][]driver.Value{
					{"2000", "patients", "p_old", []byte("FINISHED")},
					{"2001", "patients", "p1", []byte(p1State)},
					{"2001", "patients", "p2", []byte("FINISHED")},
				},
			}, nil
		}
		state := "FINISHED"
		if s.c.m.buildIndexState != "" {
			state = s.c.m.buildIndexState
		} else if s.c.m.buildIndexPolls == 1 {
			state = "RUNNING"
		}
		return &mockRows{
			cols: []string{"JobId", "TableName", "State"},
			rows: [][]driver.Value{{"2001", "patients", []byte(state)}},
		}, nil
	}

	if strings.HasPrefix(q, "SHOW FULL TABLES") {
		return &mockRows{
			cols: []string{"Tables_in_tenant_duke", "Table_type"},
			rows: [][]driver.Value{
				{"atlas_schema_revisions", "BASE TABLE"},
				{"patients", "BASE TABLE"},
				{"vw_patients", "VIEW"},
			},
		}, nil
	}

	if strings.HasPrefix(q, "SHOW CREATE VIEW") {
		return &mockRows{
			cols: []string{"View", "Create View"},
			rows: [][]driver.Value{
				{"vw_patients", "CREATE OR REPLACE VIEW internal.tenant_duke.vw_patients AS SELECT patient_id FROM internal.tenant_duke.patients;"},
			},
		}, nil
	}

	if strings.HasPrefix(q, "SHOW CREATE TABLE") {
		if s.c.m.disableShowCreateTable {
			return nil, fmt.Errorf("SHOW CREATE TABLE disabled")
		}
		return &mockRows{
			cols: []string{"Table", "Create Table"},
			rows: [][]driver.Value{
				{"patients", "CREATE TABLE IF NOT EXISTS internal.tenant_duke.patients (patient_id VARCHAR(64) NOT NULL, mrn VARCHAR(32) NOT NULL DEFAULT \"\") UNIQUE KEY(patient_id) DISTRIBUTED BY HASH(patient_id) BUCKETS 4;"},
			},
		}, nil
	}

	if strings.Contains(q, "information_schema") && strings.Contains(q, "columns") {
		return &mockRows{
			cols: []string{"COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_KEY", "COLUMN_DEFAULT", "COLUMN_COMMENT"},
			rows: [][]driver.Value{
				{"patient_id", "VARCHAR(64)", "NO", "UNI", nil, "PK"},
				{"active", "TINYINT(1)", "NO", "", "1", "Active flag"},
			},
		}, nil
	}

	if strings.Contains(q, "FROM") && strings.Contains(q, "atlas_schema_revisions") {
		var rows [][]driver.Value
		for _, r := range s.c.m.revisions {
			rows = append(rows, []driver.Value{
				r.Version,
				r.Description,
				int64(r.Type),
				int64(r.Applied),
				int64(r.Total),
				r.ExecutedAt.Format("2006-01-02 15:04:05.000000"),
				r.ExecutionTimeNs,
				r.Error,
				r.ErrorStmt,
				r.Hash,
				r.OperatorVersion,
			})
		}
		return &mockRows{
			cols: []string{"version", "description", "type", "applied", "total", "executed_at", "execution_time", "error", "error_stmt", "hash", "operator_version"},
			rows: rows,
		}, nil
	}

	return &mockRows{cols: []string{"ok"}}, nil
}

type mockRows struct {
	cols []string
	rows [][]driver.Value
	idx  int
}

func (r *mockRows) Columns() []string { return r.cols }
func (r *mockRows) Close() error      { return nil }
func (r *mockRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

func TestExecutor_StatusAndApply_WithAsyncPolling(t *testing.T) {
	migDir := t.TempDir()

	// Empty baseline
	if err := os.WriteFile(filepath.Join(migDir, "00000000000000_tenant.sql"), nil, 0o644); err != nil {
		t.Fatalf("write baseline: %v", err)
	}
	if err := migrate.HashDir(migDir); err != nil {
		t.Fatalf("hash baseline: %v", err)
	}

	s1, err := doris.ParseSchema(`
CREATE DATABASE IF NOT EXISTS internal.tenant_${TENANT_SLUG};
CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patients (
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(32) NOT NULL DEFAULT "",
    publication_generation BIGINT NOT NULL DEFAULT "0"
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS ${TENANT_BUCKETS}
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "publication_generation"
);`)
	if err != nil {
		t.Fatalf("parse s1: %v", err)
	}
	if _, _, err := migrate.DiffAndWrite(migDir, "init_patients", s1, migrate.DiffWriteOptions{Version: "20261024010000"}); err != nil {
		t.Fatalf("DiffAndWrite s1: %v", err)
	}

	s2, err := doris.ParseSchema(`
CREATE DATABASE IF NOT EXISTS internal.tenant_${TENANT_SLUG};
CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patients (
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(64) NOT NULL DEFAULT "",
    publication_generation BIGINT NOT NULL DEFAULT "0",
    INDEX idx_patients_mrn (mrn) USING INVERTED
)
UNIQUE KEY(patient_id)
DISTRIBUTED BY HASH(patient_id) BUCKETS ${TENANT_BUCKETS}
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "publication_generation"
);`)
	if err != nil {
		t.Fatalf("parse s2: %v", err)
	}
	if _, _, err := migrate.DiffAndWrite(migDir, "widen_and_index_mrn", s2, migrate.DiffWriteOptions{Version: "20261024020000"}); err != nil {
		t.Fatalf("DiffAndWrite s2: %v", err)
	}

	db, mock := setupMockDB(t)
	exec := migrate.NewExecutor(db, "internal")

	ctx := context.Background()
	stBefore, err := exec.Status(ctx, migDir)
	if err != nil {
		t.Fatalf("Status before apply: %v", err)
	}
	if stBefore.Status != "PENDING" || len(stBefore.Pending) != 3 {
		t.Fatalf("expected PENDING with 3 files, got %+v", stBefore)
	}

	res, err := exec.Apply(ctx, migrate.ApplyOptions{
		DirPath:         migDir,
		BaselineVersion: "00000000000000",
		Vars: map[string]string{
			"TENANT_SLUG":    "duke_health",
			"TENANT_BUCKETS": "16",
		},
		PollInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if len(res.AppliedFiles) != 2 || len(res.SkippedBaseline) != 1 {
		t.Fatalf("expected 2 applied files and 1 baseline skip, got %+v", res)
	}
	if mock.alterPolls < 2 {
		t.Errorf("expected async SHOW ALTER TABLE COLUMN polling (RUNNING -> FINISHED), got %d polls", mock.alterPolls)
	}
	if mock.buildIndexPolls < 2 {
		t.Errorf("expected async SHOW BUILD INDEX polling (RUNNING -> FINISHED), got %d polls", mock.buildIndexPolls)
	}

	stAfter, err := exec.Status(ctx, migDir)
	if err != nil {
		t.Fatalf("Status after apply: %v", err)
	}
	if stAfter.Status != "OK" || len(stAfter.Pending) != 0 || stAfter.Current != "20261024020000" {
		t.Fatalf("expected OK status with Current=20261024020000, got %+v", stAfter)
	}

	// Re-running Apply when already up-to-date is a clean no-op
	resNoop, err := exec.Apply(ctx, migrate.ApplyOptions{
		DirPath: migDir,
		Vars: map[string]string{
			"TENANT_SLUG":    "duke_health",
			"TENANT_BUCKETS": "16",
		},
	})
	if err != nil {
		t.Fatalf("second Apply failed: %v", err)
	}
	if !resNoop.AlreadyUpToDate {
		t.Errorf("expected AlreadyUpToDate=true on second Apply")
	}

	// Verify immediate failure on CANCELLED, CANCELED, and FAILED states
	for _, terminalState := range []string{"CANCELLED", "CANCELED", "FAILED"} {
		t.Run("terminal_state_"+terminalState, func(t *testing.T) {
			dbFail, mockFail := setupMockDB(t)
			mockFail.alterState = terminalState
			execFail := migrate.NewExecutor(dbFail, "internal")
			_, err := execFail.Apply(ctx, migrate.ApplyOptions{
				DirPath:         migDir,
				BaselineVersion: "00000000000000",
				Vars: map[string]string{
					"TENANT_SLUG":    "duke_health",
					"TENANT_BUCKETS": "16",
				},
				PollInterval: 5 * time.Millisecond,
				PollTimeout:  50 * time.Millisecond,
			})
			if err == nil || !strings.Contains(err.Error(), terminalState) {
				t.Fatalf("expected immediate error containing %s, got %v", terminalState, err)
			}
			if mockFail.alterPolls != 1 {
				t.Errorf("expected exactly 1 poll before failing on %s, got %d", terminalState, mockFail.alterPolls)
			}
		})
	}

	// Verify multi-partition SHOW BUILD INDEX waits for all partitions of latest JobId
	t.Run("multi_partition_build_index", func(t *testing.T) {
		dbMP, mockMP := setupMockDB(t)
		mockMP.multiPartitionBuildIndex = true
		execMP := migrate.NewExecutor(dbMP, "internal")
		_, err := execMP.Apply(ctx, migrate.ApplyOptions{
			DirPath:         migDir,
			BaselineVersion: "00000000000000",
			Vars: map[string]string{
				"TENANT_SLUG":    "duke_health",
				"TENANT_BUCKETS": "16",
			},
			PollInterval: 5 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("multi-partition BUILD INDEX Apply failed: %v", err)
		}
		if mockMP.buildIndexPolls < 2 {
			t.Errorf("expected at least 2 polls for multi-partition BUILD INDEX, got %d", mockMP.buildIndexPolls)
		}
	})

	// Verify real SHOW BUILD INDEX polling errors propagate while unsupported query errors complete
	t.Run("show_build_index_polling_errors", func(t *testing.T) {
		dbErr, mockErr := setupMockDB(t)
		mockErr.showPollError = "connection reset by peer"
		execErr := migrate.NewExecutor(dbErr, "internal")
		_, err := execErr.Apply(ctx, migrate.ApplyOptions{
			DirPath:         migDir,
			BaselineVersion: "00000000000000",
			Vars: map[string]string{
				"TENANT_SLUG":    "duke_health",
				"TENANT_BUCKETS": "16",
			},
			PollInterval: 5 * time.Millisecond,
		})
		if err == nil || !strings.Contains(err.Error(), "connection reset by peer") {
			t.Fatalf("expected connection reset polling error, got %v", err)
		}

		dbUnsup, mockUnsup := setupMockDB(t)
		mockUnsup.showPollError = "syntax error: unsupported SHOW command"
		execUnsup := migrate.NewExecutor(dbUnsup, "internal")
		if _, err := execUnsup.Apply(ctx, migrate.ApplyOptions{
			DirPath:         migDir,
			BaselineVersion: "00000000000000",
			Vars: map[string]string{
				"TENANT_SLUG":    "duke_health",
				"TENANT_BUCKETS": "16",
			},
			PollInterval: 5 * time.Millisecond,
		}); err != nil {
			t.Fatalf("expected unsupported SHOW command to be treated as complete, got %v", err)
		}
	})
}

func TestExecutor_PartialMigrationFailureRetry(t *testing.T) {
	migDir := t.TempDir()
	migSQL := strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS internal.tenant_duke.patients (patient_id VARCHAR(64) NOT NULL, mrn VARCHAR(32) NOT NULL DEFAULT \"\") UNIQUE KEY(patient_id) DISTRIBUTED BY HASH(patient_id) BUCKETS 4;",
		"ALTER TABLE internal.tenant_duke.patients ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT \"ACTIVE\";",
		"ALTER TABLE internal.tenant_duke.patients ADD COLUMN notes TEXT;",
	}, "\n\n") + "\n"
	if err := os.WriteFile(filepath.Join(migDir, "20261025000001_three_steps.sql"), []byte(migSQL), 0o644); err != nil {
		t.Fatalf("write migration: %v", err)
	}
	if err := migrate.HashDir(migDir); err != nil {
		t.Fatalf("HashDir: %v", err)
	}

	db, mock := setupMockDB(t)
	// Fail on the 2nd statement ("ADD COLUMN status")
	mock.failOnSubstr = "ADD COLUMN status"
	exec := migrate.NewExecutor(db, "internal")
	ctx := context.Background()

	_, err := exec.Apply(ctx, migrate.ApplyOptions{
		DirPath:      migDir,
		PollInterval: 5 * time.Millisecond,
	})
	if err == nil {
		t.Fatalf("expected Apply to fail on statement 2")
	}
	if len(mock.revisions) != 1 || mock.revisions[0].Applied != 1 || mock.revisions[0].Error == "" {
		t.Fatalf("expected partial revision with Applied=1 and Error!=empty, got %+v", mock.revisions)
	}

	// Clear failure and retry: statement 1 (CREATE TABLE) must be skipped, executing only statements 2 and 3
	mock.failOnSubstr = ""
	resRetry, err := exec.Apply(ctx, migrate.ApplyOptions{
		DirPath:      migDir,
		PollInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("retry Apply failed: %v", err)
	}
	if len(resRetry.ExecutedSQL) != 2 {
		t.Fatalf("expected retry to execute only remaining 2 statements, got %d: %v", len(resRetry.ExecutedSQL), resRetry.ExecutedSQL)
	}
	if strings.Contains(resRetry.ExecutedSQL[0], "CREATE TABLE") {
		t.Errorf("retry unexpectedly re-executed statement 1: %s", resRetry.ExecutedSQL[0])
	}
	if len(mock.revisions) != 1 || mock.revisions[0].Applied != 3 || mock.revisions[0].Error != "" {
		t.Fatalf("expected final revision with Applied=3 and Error='', got %+v", mock.revisions)
	}
}

func TestInspectSchemaFromDB_AndInfoSchemaFallback(t *testing.T) {
	ctx := context.Background()
	db, mock := setupMockDB(t)

	// 1. Inspect via SHOW FULL TABLES + SHOW CREATE TABLE + SHOW CREATE VIEW
	s, err := migrate.InspectSchemaFromDB(ctx, db, "internal.tenant_duke")
	if err != nil {
		t.Fatalf("InspectSchemaFromDB failed: %v", err)
	}
	if len(s.Databases) != 1 || len(s.Tables) != 1 || len(s.Views) != 1 {
		t.Fatalf("expected 1 db, 1 table, 1 view; got dbs=%d tables=%d views=%d", len(s.Databases), len(s.Tables), len(s.Views))
	}
	if s.Tables[0].CanonicalName() != "internal.tenant_duke.patients" {
		t.Errorf("unexpected table canonical name: %q", s.Tables[0].CanonicalName())
	}

	// 2. Fallback to information_schema.columns when SHOW CREATE TABLE is disabled
	mock.disableShowCreateTable = true
	sFallback, err := migrate.InspectSchemaFromDB(ctx, db, "tenant_duke")
	if err != nil {
		t.Fatalf("InspectSchemaFromDB fallback failed: %v", err)
	}
	if len(sFallback.Tables) != 1 || len(sFallback.Tables[0].Columns) != 2 {
		t.Fatalf("expected fallback table with 2 columns, got %+v", sFallback.Tables)
	}
	if sFallback.Tables[0].Columns[1].Type != "BOOLEAN" {
		t.Errorf("expected TINYINT(1) to normalize to BOOLEAN, got %q", sFallback.Tables[0].Columns[1].Type)
	}

	// 3. Test InspectLiveSchema and DialExecutor using DefaultSQLDriver = "mockdoris"
	origDriver := migrate.DefaultSQLDriver
	migrate.DefaultSQLDriver = "mockdoris"
	t.Cleanup(func() { migrate.DefaultSQLDriver = origDriver })

	mock.disableShowCreateTable = false
	sLive, err := migrate.InspectLiveSchema(ctx, "doris://root@localhost/tenant_duke")
	if err != nil {
		t.Fatalf("InspectLiveSchema failed: %v", err)
	}
	if len(sLive.Tables) != 1 || len(sLive.Views) != 1 {
		t.Errorf("unexpected InspectLiveSchema counts: tables=%d views=%d", len(sLive.Tables), len(sLive.Views))
	}

	dialedExec, err := migrate.DialExecutor("mysql://root:pass@localhost:9030/tenant_duke", "internal.tenant_duke")
	if err != nil {
		t.Fatalf("DialExecutor failed: %v", err)
	}
	_ = dialedExec.Close()

	// 4. Test ParseDorisDSN edge cases
	dsnRaw, dbRaw, err := migrate.ParseDorisDSN("root:pass@tcp(127.0.0.1:9030)/my_db?parseTime=true")
	if err != nil || dbRaw != "my_db" || dsnRaw == "" {
		t.Errorf("ParseDorisDSN(raw DSN) = (%q, %q, %v)", dsnRaw, dbRaw, err)
	}
	for _, badDSN := range []string{"", "postgres://localhost/db", "doris:///no_host"} {
		if _, _, err := migrate.ParseDorisDSN(badDSN); err == nil {
			t.Errorf("expected error for ParseDorisDSN(%q)", badDSN)
		}
	}

	// 5. Test ValidateAndNormalizeVarValue & ResolvePath edge cases
	if _, err := migrate.ValidateAndNormalizeVarValue("TENANT_BUCKETS", "0"); err == nil {
		t.Errorf("expected error for TENANT_BUCKETS=0")
	}
	if _, err := migrate.ValidateAndNormalizeVarValue("TENANT_BUCKETS", "AUTO"); err != nil {
		t.Errorf("expected TENANT_BUCKETS=AUTO to succeed, got %v", err)
	}
	if _, err := migrate.SubstituteVars("SELECT 1", map[string]string{"123_BAD": "val"}); err == nil {
		t.Errorf("expected error for invalid var name")
	}
	if _, err := migrate.ResolvePath(""); err == nil {
		t.Errorf("expected error for empty ResolvePath")
	}
	if _, err := migrate.ResolvePath("http://example.com/schema.sql"); err == nil {
		t.Errorf("expected error for unsupported URL scheme in ResolvePath")
	}

	// 6. Test LintDir per-statement rules (DROP TABLE, DROP COLUMN, ADD COLUMN without default, unsafe MODIFY COLUMN) and DiffAndWrite auto-version
	lintDir := t.TempDir()
	badMigSQL := strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS internal.tenant_duke.patients (patient_id VARCHAR(64) NOT NULL, mrn VARCHAR(64) NOT NULL DEFAULT \"\", old_col INT) UNIQUE KEY(patient_id) DISTRIBUTED BY HASH(patient_id) BUCKETS 4;",
		"ALTER TABLE internal.tenant_duke.patients ADD COLUMN bad_scalar VARCHAR(32) NOT NULL;",
		"ALTER TABLE internal.tenant_duke.patients MODIFY COLUMN mrn VARCHAR(16) NOT NULL DEFAULT \"\";",
		"ALTER TABLE internal.tenant_duke.patients DROP COLUMN old_col;",
		"DROP TABLE internal.tenant_duke.patients;",
	}, "\n\n") + "\n"
	if err := os.WriteFile(filepath.Join(lintDir, "20261025000001_bad_ops.sql"), []byte(badMigSQL), 0o644); err != nil {
		t.Fatalf("write bad_ops.sql: %v", err)
	}
	violations, err := migrate.LintDir(lintDir, sLive, doris.LintOptions{AllowDestructive: false})
	if err != nil {
		t.Fatalf("LintDir failed: %v", err)
	}
	if len(violations) < 4 {
		t.Errorf("expected at least 4 lint violations from bad_ops.sql, got %d: %v", len(violations), violations)
	}

	autoDir := t.TempDir()
	fName, stmtsAuto, err := migrate.DiffAndWrite(autoDir, "My Upper-Case & Special Name!", sLive, migrate.DiffWriteOptions{})
	if err != nil || fName == "" || len(stmtsAuto) == 0 {
		t.Fatalf("DiffAndWrite auto-version failed: fName=%q err=%v", fName, err)
	}
	// Calling DiffAndWrite again with identical desired schema returns empty fileName
	fNoop, stmtsNoop, err := migrate.DiffAndWrite(autoDir, "", sLive, migrate.DiffWriteOptions{})
	if err != nil || fNoop != "" || len(stmtsNoop) != 0 {
		t.Errorf("expected no-op DiffAndWrite when already synced, got f=%q stmts=%d err=%v", fNoop, len(stmtsNoop), err)
	}

	// 7. Test InspectTableFromInfoSchema error paths and Executor without revisionsSchema
	if _, err := migrate.InspectTableFromInfoSchema(ctx, db, "bad;cat", "tenant_duke", "patients"); err == nil {
		t.Errorf("expected error for invalid catalog identifier in InspectTableFromInfoSchema")
	}
	if _, err := migrate.InspectSchemaFromDB(ctx, nil, "tenant_duke"); err == nil {
		t.Errorf("expected error for nil db in InspectSchemaFromDB")
	}
	if _, err := migrate.InspectSchemaFromDB(ctx, db, "bad;db"); err == nil {
		t.Errorf("expected error for invalid db identifier in InspectSchemaFromDB")
	}

	execDefaultSchema := migrate.NewExecutor(db, "")
	if execDefaultSchema.RevisionsTableQualifiedName() != migrate.RevisionsTableName {
		t.Errorf("unexpected default revisions table name: %s", execDefaultSchema.RevisionsTableQualifiedName())
	}
	if !strings.Contains(execDefaultSchema.RevisionsTableCreateSQL(), migrate.RevisionsTableName) {
		t.Errorf("unexpected default RevisionsTableCreateSQL")
	}
	resDry, err := execDefaultSchema.Apply(ctx, migrate.ApplyOptions{
		DirPath: autoDir,
		DryRun:  true,
	})
	if err != nil || len(resDry.ExecutedSQL) == 0 {
		t.Errorf("Apply(DryRun=true) failed: res=%+v err=%v", resDry, err)
	}

	// 8. Test Executor error paths (nil db, invalid revisions schema, checksum mismatch in Status & Apply)
	nilExec := migrate.NewExecutor(nil, "internal")
	if err := nilExec.EnsureRevisionsTable(ctx); err == nil {
		t.Errorf("expected error on EnsureRevisionsTable with nil db")
	}
	if err := nilExec.RecordRevision(ctx, migrate.RevisionRecord{Version: "1"}); err == nil {
		t.Errorf("expected error on RecordRevision with nil db")
	}
	if _, err := nilExec.Apply(ctx, migrate.ApplyOptions{DirPath: autoDir, DryRun: false}); err == nil {
		t.Errorf("expected error on Apply(DryRun=false) with nil db")
	}

	badSchemaExec := migrate.NewExecutor(db, "bad;schema")
	if err := badSchemaExec.EnsureRevisionsTable(ctx); err == nil {
		t.Errorf("expected error on EnsureRevisionsTable with invalid schema identifier")
	}

	// Record a revision with mismatched hash for fName's version and verify Status and Apply detect checksum mismatch
	verPrefix := strings.Split(fName, "_")[0]
	_ = execDefaultSchema.RecordRevision(ctx, migrate.RevisionRecord{
		Version:     verPrefix,
		Description: "test",
		Hash:        "h1:mismatched_hash",
	})
	if _, err := execDefaultSchema.Status(ctx, autoDir); err == nil {
		t.Errorf("expected checksum mismatch error in Status")
	}
	if _, err := execDefaultSchema.Apply(ctx, migrate.ApplyOptions{DirPath: autoDir}); err == nil {
		t.Errorf("expected checksum mismatch error in Apply")
	}
}
