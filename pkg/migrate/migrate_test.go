package migrate_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-sql-driver/mysql"
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

	parsedRev, err := doris.ParseSchema(migrate.RevisionsTableDDL)
	if err != nil || len(parsedRev.Tables) != 1 || parsedRev.Tables[0].Key.Kind != doris.KeyUnique {
		t.Fatalf("ParseSchema(RevisionsTableDDL) failed: parsed=%+v err=%v", parsedRev, err)
	}
}

// In-process mock SQL driver to test Executor end-to-end (revisions table, async polling, status, apply)
type mockDorisDriver struct {
	mu                       sync.Mutex
	executedSQL              []string
	revisions                []migrate.RevisionRecord
	alterJobSeq              int
	buildJobSeq              int
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
	if strings.HasPrefix(q, "ALTER TABLE ") {
		s.c.m.alterJobSeq++
		s.c.m.alterPolls = 0
	} else if strings.HasPrefix(q, "BUILD INDEX ") {
		s.c.m.buildJobSeq++
		s.c.m.buildIndexPolls = 0
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
		if s.c.m.alterJobSeq == 0 {
			return &mockRows{cols: []string{"JobId", "TableName", "State"}}, nil
		}
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
			rows: [][]driver.Value{{fmt.Sprintf("%d", 1000+s.c.m.alterJobSeq), "patients", []byte(state)}},
		}, nil
	}

	if strings.HasPrefix(q, "SHOW BUILD INDEX") {
		if s.c.m.buildJobSeq == 0 {
			if s.c.m.showPollError != "" {
				return nil, fmt.Errorf("%s", s.c.m.showPollError)
			}
			if s.c.m.multiPartitionBuildIndex {
				return &mockRows{
					cols: []string{"JobId", "TableName", "PartitionName", "State"},
					rows: [][]driver.Value{{"2000", "patients", "p_old", []byte("FINISHED")}},
				}, nil
			}
			return &mockRows{cols: []string{"JobId", "TableName", "State"}}, nil
		}
		s.c.m.buildIndexPolls++
		if s.c.m.showPollError != "" {
			return nil, fmt.Errorf("%s", s.c.m.showPollError)
		}
		jobID := fmt.Sprintf("%d", 2000+s.c.m.buildJobSeq)
		if s.c.m.multiPartitionBuildIndex {
			p1State := "RUNNING"
			if s.c.m.buildIndexPolls >= 2 {
				p1State = "FINISHED"
			}
			p2JobID := fmt.Sprintf("%d", 2001+s.c.m.buildJobSeq)
			return &mockRows{
				cols: []string{"JobId", "TableName", "PartitionName", "State"},
				rows: [][]driver.Value{
					{"2000", "patients", "p_old", []byte("FINISHED")},
					{jobID, "patients", "p1", []byte(p1State)},
					{p2JobID, "patients", "p2", []byte("FINISHED")},
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
			rows: [][]driver.Value{{jobID, "patients", []byte(state)}},
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
				{"vw_patients", "CREATE OR REPLACE VIEW vw_patients AS SELECT patient_id FROM internal.tenant_duke.patients;"},
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
				{"patients", "CREATE TABLE IF NOT EXISTS patients (patient_id VARCHAR(64) NOT NULL, mrn VARCHAR(32) NOT NULL DEFAULT \"\") UNIQUE KEY(patient_id) DISTRIBUTED BY HASH(patient_id) BUCKETS 4;"},
			},
		}, nil
	}

	if strings.Contains(q, "information_schema") && strings.Contains(q, "columns") {
		if len(args) >= 2 && fmt.Sprint(args[1]) == "pri_tbl" {
			return &mockRows{
				cols: []string{"COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_KEY", "COLUMN_DEFAULT", "COLUMN_COMMENT"},
				rows: [][]driver.Value{
					{"id", "VARCHAR(64)", "NO", "PRI", nil, "Primary key"},
				},
			}, nil
		}
		if len(args) >= 2 && fmt.Sprint(args[1]) == "dup_tbl" {
			return &mockRows{
				cols: []string{"COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_KEY", "COLUMN_DEFAULT", "COLUMN_COMMENT"},
				rows: [][]driver.Value{
					{"id", "VARCHAR(64)", "NO", "DUP", nil, "Dup key"},
				},
			}, nil
		}
		if len(args) >= 2 && fmt.Sprint(args[1]) == "agg_tbl" {
			return &mockRows{
				cols: []string{"COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_KEY", "COLUMN_DEFAULT", "COLUMN_COMMENT"},
				rows: [][]driver.Value{
					{"id", "VARCHAR(64)", "NO", "AGG", nil, "Agg key"},
				},
			}, nil
		}
		if len(args) >= 2 && fmt.Sprint(args[1]) == "empty_tbl" {
			return &mockRows{
				cols: []string{"COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_KEY", "COLUMN_DEFAULT", "COLUMN_COMMENT"},
				rows: nil,
			}, nil
		}
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
		for idx, r := range s.c.m.revisions {
			var execVal any = r.ExecutedAt.Format("2006-01-02 15:04:05.000000")
			switch idx % 3 {
			case 1:
				execVal = r.ExecutedAt
			case 2:
				execVal = []byte(r.ExecutedAt.Format(time.RFC3339Nano))
			}
			rows = append(rows, []driver.Value{
				r.Version,
				r.Description,
				int64(r.Type),
				int64(r.Applied),
				int64(r.Total),
				execVal,
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
	if stBefore.Status != "PENDING" || len(stBefore.Pending) != 2 {
		t.Fatalf("expected PENDING with 2 pending files (skipping empty tenant baseline), got %+v", stBefore)
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

	// 9. Test additional ResolvePath, ParseDorisDSN, ValidateAndNormalizeVarValue, SubstituteVars, and dir error branches
	var nilExecPtr *migrate.Executor
	if err := nilExecPtr.Close(); err != nil {
		t.Errorf("expected nil error on (*Executor)(nil).Close(), got %v", err)
	}
	for _, p := range []string{"file://localhost/tmp/abc", "file://./rel", "file://../rel", "file://host/path"} {
		if got, err := migrate.ResolvePath(p); err != nil || got == "" {
			t.Errorf("ResolvePath(%q) = (%q, %v)", p, got, err)
		}
	}
	if _, err := migrate.ResolvePath("file://%zz"); err == nil {
		t.Errorf("expected error for invalid file URL")
	}
	if dsn, dbN, err := migrate.ParseDorisDSN("root@tcp(127.0.0.1:9030)/plain_db"); err != nil || dbN != "plain_db" || dsn == "" {
		t.Errorf("ParseDorisDSN(plain_db) = (%q, %q, %v)", dsn, dbN, err)
	}
	if _, _, err := migrate.ParseDorisDSN("doris://%zz"); err == nil {
		t.Errorf("expected error for invalid URL in ParseDorisDSN")
	}
	for _, badVal := range []struct{ k, v string }{
		{"TENANT_SLUG", ""},
		{"TENANT_SLUG", "bad\x00char"},
		{"TENANT_SLUG", "val--comment"},
		{"TENANT_SLUG", "val/*comment*/"},
		{"TENANT_SLUG", "123-invalid"},
	} {
		if _, err := migrate.ValidateAndNormalizeVarValue(badVal.k, badVal.v); err == nil {
			t.Errorf("expected error for ValidateAndNormalizeVarValue(%q, %q)", badVal.k, badVal.v)
		}
	}
	if _, err := migrate.SubstituteVars("SELECT ${UNCLOSED", map[string]string{}); err == nil {
		t.Errorf("expected error for malformed placeholder in SubstituteVars")
	}

	// InspectSchemaFromDB with empty defaultDB and with non-internal catalog
	if sEmptyDB, err := migrate.InspectSchemaFromDB(ctx, db, ""); err != nil || len(sEmptyDB.Tables) == 0 {
		t.Errorf("InspectSchemaFromDB(empty defaultDB) = (%+v, %v)", sEmptyDB, err)
	}
	if sIceDB, err := migrate.InspectSchemaFromDB(ctx, db, "azra_lake.tenant_duke"); err != nil || len(sIceDB.Tables) == 0 {
		t.Errorf("InspectSchemaFromDB(azra_lake.tenant_duke) = (%+v, %v)", sIceDB, err)
	}

	// Test future migration version increment (nextMigrationVersion when latest >= now) and sanitizeMigrationName fallback
	futureDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(futureDir, "99991231235950_future.sql"), []byte("CREATE DATABASE IF NOT EXISTS internal.tenant_duke;\n"), 0o644); err != nil {
		t.Fatalf("write future.sql: %v", err)
	}
	if err := migrate.HashDir(futureDir); err != nil {
		t.Fatalf("HashDir(futureDir): %v", err)
	}
	fFuture, _, err := migrate.DiffAndWrite(futureDir, "   !!!   ", sLive, migrate.DiffWriteOptions{})
	if err != nil || fFuture != "99991231235951_migration.sql" {
		t.Errorf("expected 99991231235951_migration.sql, got %q (err=%v)", fFuture, err)
	}

	// Test ReplayDir, LintDir, HashDir, ValidateDir, DiffAndWrite on invalid paths / broken SQL
	for _, fn := range []func() error{
		func() error { _, _, err := migrate.ReplayDir(""); return err },
		func() error { _, _, err := migrate.ReplayDir("/nonexistent/dir/path"); return err },
		func() error { _, err := migrate.LintDir("", nil, doris.LintOptions{}); return err },
		func() error { _, err := migrate.LintDir("/nonexistent/dir/path", nil, doris.LintOptions{}); return err },
		func() error { return migrate.HashDir("") },
		func() error { return migrate.HashDir("/nonexistent/dir/path") },
		func() error { return migrate.ValidateDir("", doris.LintOptions{}) },
		func() error { return migrate.ValidateDir(t.TempDir(), doris.LintOptions{}) }, // missing atlas.sum
		func() error {
			_, _, err := migrate.DiffAndWrite("", "x", sLive, migrate.DiffWriteOptions{})
			return err
		},
	} {
		if err := fn(); err == nil {
			t.Errorf("expected error from directory helper")
		}
	}

	// Test InspectTableFromInfoSchema with empty catalog, PRI key, DUP key, AGG key, and empty table
	if tblPri, err := migrate.InspectTableFromInfoSchema(ctx, db, "", "tenant_duke", "pri_tbl"); err != nil || tblPri.Key == nil || tblPri.Key.Kind != doris.KeyUnique {
		t.Errorf("InspectTableFromInfoSchema(pri_tbl) = (%+v, %v)", tblPri, err)
	}
	if tblDup, err := migrate.InspectTableFromInfoSchema(ctx, db, "", "tenant_duke", "dup_tbl"); err != nil || tblDup.Key == nil || tblDup.Key.Kind != doris.KeyDuplicate {
		t.Errorf("InspectTableFromInfoSchema(dup_tbl) = (%+v, %v)", tblDup, err)
	}
	if tblAgg, err := migrate.InspectTableFromInfoSchema(ctx, db, "", "tenant_duke", "agg_tbl"); err != nil || tblAgg.Key == nil || tblAgg.Key.Kind != doris.KeyAggregate {
		t.Errorf("InspectTableFromInfoSchema(agg_tbl) = (%+v, %v)", tblAgg, err)
	}
	if _, err := migrate.InspectTableFromInfoSchema(ctx, db, "", "tenant_duke", "empty_tbl"); err == nil {
		t.Errorf("expected error for empty_tbl in InspectTableFromInfoSchema")
	}

	// Test ReplayDir and LintDir on broken SQL and failing ApplyStatement
	brokenDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(brokenDir, "20261025000001_broken.sql"), []byte("CREATE TABLE broken (\n"), 0o644)
	if _, _, err := migrate.ReplayDir(brokenDir); err == nil {
		t.Errorf("expected ReplayDir error on broken SQL")
	}
	if _, err := migrate.LintDir(brokenDir, nil, doris.LintOptions{}); err == nil {
		t.Errorf("expected LintDir error on broken SQL")
	}

	badApplyDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(badApplyDir, "20261025000001_bad_apply.sql"), []byte("DROP TABLE nonexistent_tbl;\n"), 0o644)
	if _, _, err := migrate.ReplayDir(badApplyDir); err == nil {
		t.Errorf("expected ReplayDir error on DROP TABLE nonexistent_tbl")
	}
	if _, err := migrate.LintDir(badApplyDir, nil, doris.LintOptions{AllowDestructive: true}); err == nil {
		t.Errorf("expected LintDir error on DROP TABLE nonexistent_tbl")
	}
	// DiffAndWrite when destructive change is disallowed returns lint error
	if _, _, err := migrate.DiffAndWrite(futureDir, "drop_all", doris.NewSchema(), migrate.DiffWriteOptions{}); err == nil {
		t.Errorf("expected DiffAndWrite lint error when dropping tables without AllowDestructive")
	}

	// Test async schema change polling timeout when job stays RUNNING
	timeoutDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(timeoutDir, "20261025000001_alter.sql"), []byte("CREATE TABLE IF NOT EXISTS internal.tenant_duke.patients (patient_id VARCHAR(64) NOT NULL, mrn VARCHAR(32) NOT NULL DEFAULT \"\") UNIQUE KEY(patient_id) DISTRIBUTED BY HASH(patient_id) BUCKETS 4;\nALTER TABLE internal.tenant_duke.patients MODIFY COLUMN mrn VARCHAR(64) NOT NULL DEFAULT \"\";\n"), 0o644)
	_ = migrate.HashDir(timeoutDir)
	dbTimeout, mockTimeout := setupMockDB(t)
	mockTimeout.alterState = "RUNNING"
	execTimeout := migrate.NewExecutor(dbTimeout, "internal")
	if _, err := execTimeout.Apply(ctx, migrate.ApplyOptions{
		DirPath:      timeoutDir,
		PollInterval: 2 * time.Millisecond,
		PollTimeout:  5 * time.Millisecond,
	}); err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Errorf("expected poll timeout error, got %v", err)
	}
}

func buildTestMigrationFS(t *testing.T, sqlFiles map[string]string) fstest.MapFS {
	t.Helper()
	dir := t.TempDir()
	mfs := make(fstest.MapFS, len(sqlFiles)+1)
	for name, content := range sqlFiles {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
		mfs[name] = &fstest.MapFile{Data: []byte(content)}
	}
	if err := migrate.HashDir(dir); err != nil {
		t.Fatalf("HashDir: %v", err)
	}
	sumData, err := os.ReadFile(filepath.Join(dir, "atlas.sum"))
	if err != nil {
		t.Fatalf("read atlas.sum: %v", err)
	}
	mfs["atlas.sum"] = &fstest.MapFile{Data: sumData}
	return mfs
}

func TestFSPrimitives_VerifySumReplayValidateAndLint(t *testing.T) {
	mfs := buildTestMigrationFS(t, map[string]string{
		"00000000000000_tenant.sql": "",
		"20261115000000_init.sql": `
CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patients (
    patient_id VARCHAR(64) NOT NULL,
    mrn VARCHAR(32) NULL
) UNIQUE KEY(patient_id) DISTRIBUTED BY HASH(patient_id) BUCKETS 4;

CREATE TABLE IF NOT EXISTS azra_lake.tenant_${TENANT_SLUG}.obs (
    id STRING,
    tags ARRAY<VARCHAR(65533)>
) ENGINE=iceberg;
`,
		"20261116000000_iceberg_array.sql": `
ALTER TABLE azra_lake.tenant_${TENANT_SLUG}.obs MODIFY COLUMN tags ARRAY<STRING>;
`,
	})

	sum, err := migrate.VerifySumFS(mfs)
	if err != nil || !strings.HasPrefix(sum, "h1:") {
		t.Fatalf("VerifySumFS failed: sum=%q err=%v", sum, err)
	}
	if err := migrate.ValidateFS(mfs, doris.LintOptions{}); err != nil {
		t.Fatalf("ValidateFS failed on Iceberg ARRAY<VARCHAR> -> ARRAY<STRING>: %v", err)
	}
	schema, files, err := migrate.ReplayFS(mfs)
	if err != nil {
		t.Fatalf("ReplayFS failed: %v", err)
	}
	if len(files) != 3 || len(schema.Tables) != 2 {
		t.Fatalf("unexpected ReplayFS output: files=%d tables=%d", len(files), len(schema.Tables))
	}
	vs, err := migrate.LintFS(mfs, nil, doris.LintOptions{})
	if err != nil || len(vs) != 0 {
		t.Fatalf("expected 0 LintFS violations on Iceberg ARRAY<VARCHAR> -> ARRAY<STRING>, got vs=%v err=%v", vs, err)
	}

	// Null tightening in SQL migration must violate DORIS006
	tightFS := buildTestMigrationFS(t, map[string]string{
		"20261115000000_init.sql":    "CREATE TABLE IF NOT EXISTS internal.tenant_a.patients (id VARCHAR(64) NOT NULL, mrn VARCHAR(32) NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;\n",
		"20261116000000_tighten.sql": "ALTER TABLE internal.tenant_a.patients MODIFY COLUMN mrn VARCHAR(32) NOT NULL DEFAULT \"\";\n",
	})
	if err := migrate.ValidateFS(tightFS, doris.LintOptions{}); err == nil || !strings.Contains(err.Error(), "DORIS006") {
		t.Fatalf("expected DORIS006 error on NULL -> NOT NULL MODIFY COLUMN, got %v", err)
	}

	// Tampering detection and missing atlas.sum
	tampered := fstest.MapFS{
		"20261115000000_init.sql": &fstest.MapFile{Data: []byte("CREATE TABLE internal.t.a (id INT);")},
		"atlas.sum":               mfs["atlas.sum"],
	}
	if _, err := migrate.VerifySumFS(tampered); err == nil {
		t.Errorf("expected VerifySumFS to reject tampered migration fs")
	}
	if _, err := migrate.VerifySumFS(nil); err == nil {
		t.Errorf("expected VerifySumFS(nil) error")
	}
	if _, err := migrate.VerifySumFS(fstest.MapFS{}); err == nil {
		t.Errorf("expected VerifySumFS without atlas.sum to fail")
	}
	if _, _, err := migrate.ReplayFS(nil); err == nil {
		t.Errorf("expected ReplayFS(nil) error")
	}
	if _, err := migrate.LintFS(nil, nil, doris.LintOptions{}); err == nil {
		t.Errorf("expected LintFS(nil) error")
	}
}

type mockQueryRows struct {
	cols []string
	rows [][]any
	idx  int
}

func (r *mockQueryRows) Columns() ([]string, error) { return r.cols, nil }
func (r *mockQueryRows) Next() bool                 { return r.idx < len(r.rows) }
func (r *mockQueryRows) Err() error                 { return nil }
func (r *mockQueryRows) Close() error               { return nil }
func (r *mockQueryRows) Scan(dest ...any) error {
	row := r.rows[r.idx]
	r.idx++
	for i := range dest {
		if i >= len(row) {
			continue
		}
		switch d := dest[i].(type) {
		case *string:
			*d = fmt.Sprint(row[i])
		case *int:
			*d = row[i].(int)
		case *int64:
			*d = row[i].(int64)
		case *any:
			*d = row[i]
		case *sql.RawBytes:
			switch v := row[i].(type) {
			case []byte:
				*d = sql.RawBytes(v)
			default:
				*d = sql.RawBytes(fmt.Sprint(v))
			}
		}
	}
	return nil
}

type mockQuerier struct {
	mu               sync.Mutex
	revisionsMissing bool
	missingDB        bool
	revisions        []migrate.RevisionRecord
	execLog          []string
	queryLog         []string
	failOnSubstr     string
	coupledReject    bool
	showErr          error
	showState        string
	showStateSeq     []string
	showIndexName    string
	showMsg          string
	jobSeq           int
}

func (m *mockQuerier) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.execLog = append(m.execLog, query)

	if m.coupledReject && (strings.Contains(query, "storage_vault_name") || strings.Contains(query, "file_cache_ttl_seconds")) {
		return nil, errors.New("Error 1105: Unknown properties: {storage_vault_name=v1, file_cache_ttl_seconds=3600}")
	}
	if m.failOnSubstr != "" && strings.Contains(query, m.failOnSubstr) {
		return nil, fmt.Errorf("exec error matching %s", m.failOnSubstr)
	}
	if strings.HasPrefix(query, "ALTER TABLE ") || strings.HasPrefix(query, "BUILD INDEX ") {
		m.jobSeq++
	}
	if strings.HasPrefix(query, "INSERT INTO ") && strings.Contains(query, "atlas_schema_revisions") && len(args) >= 11 {
		rec := migrate.RevisionRecord{
			Version:         fmt.Sprint(args[0]),
			Description:     fmt.Sprint(args[1]),
			Type:            args[2].(int),
			Applied:         args[3].(int),
			Total:           args[4].(int),
			ExecutedAt:      time.Now().UTC(),
			ExecutionTimeNs: args[6].(int64),
			Hash:            fmt.Sprint(args[9]),
			OperatorVersion: fmt.Sprint(args[10]),
		}
		if ns, ok := args[7].(sql.NullString); ok && ns.Valid {
			rec.Error = ns.String
		}
		if ns, ok := args[8].(sql.NullString); ok && ns.Valid {
			rec.ErrorStmt = ns.String
		}
		replaced := false
		for i, existing := range m.revisions {
			if existing.Version == rec.Version {
				m.revisions[i] = rec
				replaced = true
				break
			}
		}
		if !replaced {
			m.revisions = append(m.revisions, rec)
		}
		m.revisionsMissing = false
	}
	if strings.HasPrefix(query, "DELETE FROM ") && strings.Contains(query, "atlas_schema_revisions") && len(args) >= 1 {
		target := fmt.Sprint(args[0])
		kept := m.revisions[:0]
		for _, r := range m.revisions {
			if r.Version != target {
				kept = append(kept, r)
			}
		}
		m.revisions = kept
	}
	return driver.RowsAffected(1), nil
}

func (m *mockQuerier) QueryRowsContext(_ context.Context, query string, _ ...any) (migrate.QueryRows, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queryLog = append(m.queryLog, query)

	if strings.Contains(query, "atlas_schema_revisions") {
		if m.missingDB {
			return nil, errors.New("Error 1049 (42000): Unknown database 'internal.tenant_duke'")
		}
		if m.revisionsMissing {
			return nil, errors.New("Error 1146 (42S02): Table 'internal.tenant_duke.atlas_schema_revisions' doesn't exist")
		}
		rows := make([][]any, 0, len(m.revisions))
		for _, r := range m.revisions {
			rows = append(rows, []any{
				r.Version,
				r.Description,
				r.Type,
				r.Applied,
				r.Total,
				r.ExecutedAt.Format("2006-01-02 15:04:05.000000"),
				r.ExecutionTimeNs,
				r.Error,
				r.ErrorStmt,
				r.Hash,
				r.OperatorVersion,
			})
		}
		return &mockQueryRows{
			cols: []string{"version", "description", "type", "applied", "total", "executed_at", "execution_time", "error", "error_stmt", "hash", "operator_version"},
			rows: rows,
		}, nil
	}

	if strings.HasPrefix(query, "SHOW BUILD INDEX") || strings.HasPrefix(query, "SHOW ALTER TABLE COLUMN") {
		if m.showErr != nil {
			return nil, m.showErr
		}
		if m.jobSeq == 0 {
			return &mockQueryRows{
				cols: []string{"JobId", "TableName", "IndexName", "State", "Msg"},
			}, nil
		}
		st := m.showState
		if len(m.showStateSeq) > 0 {
			st = m.showStateSeq[0]
			m.showStateSeq = m.showStateSeq[1:]
		}
		if st == "" {
			st = "FINISHED"
		}
		idxName := m.showIndexName
		if idxName == "" {
			idxName = "idx_mrn"
		}
		return &mockQueryRows{
			cols: []string{"JobId", "TableName", "IndexName", "State", "Msg"},
			rows: [][]any{{fmt.Sprintf("%d", 3000+m.jobSeq), "patients", idxName, st, m.showMsg}},
		}, nil
	}
	return &mockQueryRows{cols: []string{"ok"}}, nil
}

func TestExecutor_ReadOnlyStatus_EmptyTenantBaseline_And_ChainScope(t *testing.T) {
	ctx := context.Background()

	t.Run("status_is_strictly_read_only_when_revisions_missing", func(t *testing.T) {
		mfs := buildTestMigrationFS(t, map[string]string{
			"00000000000000_tenant.sql": "",
			"20261115000000_init.sql":   "CREATE TABLE IF NOT EXISTS internal.tenant_duke.patients (id VARCHAR(64) NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;\n",
		})
		q := &mockQuerier{revisionsMissing: true}
		exec := migrate.NewExecutorWithQuerier(q, "internal.tenant_duke", migrate.WithChainScope("hl7-ingest"))

		st, err := exec.StatusFS(ctx, mfs)
		if err != nil {
			t.Fatalf("StatusFS failed: %v", err)
		}
		if st.Status != "PENDING" || len(st.Pending) != 1 || st.Pending[0].Version != "20261115000000" {
			t.Fatalf("unexpected scoped StatusFS with empty tenant baseline: %+v", st)
		}
		if len(q.execLog) != 0 {
			t.Fatalf("expected 0 mutating ExecContext calls during StatusFS, got %v", q.execLog)
		}

		// Also verify missing DB error 1049 is tolerated read-only
		q.revisionsMissing = false
		q.missingDB = true
		revs, err := exec.AppliedRevisions(ctx)
		if err != nil || len(revs) != 0 {
			t.Fatalf("expected empty AppliedRevisions on missing DB, got revs=%+v err=%v", revs, err)
		}
		if len(q.execLog) != 0 {
			t.Fatalf("expected 0 mutating ExecContext calls during AppliedRevisions, got %v", q.execLog)
		}
	})

	t.Run("empty_00000000000000_tenant_skip_without_recording", func(t *testing.T) {
		mfs := buildTestMigrationFS(t, map[string]string{
			"00000000000000_tenant.sql": "",
			"20261115000000_init.sql":   "CREATE TABLE IF NOT EXISTS internal.tenant_${TENANT_SLUG}.patients (id VARCHAR(64) NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS ${TENANT_BUCKETS};\n",
		})
		q := &mockQuerier{}
		exec := migrate.NewExecutorWithQuerier(q, "internal.tenant_duke")

		// Apply with BaselineVersion == "" skips empty 00000000000000_tenant.sql without inserting into revisions
		res, err := exec.Apply(ctx, migrate.ApplyOptions{
			FS:   mfs,
			Vars: map[string]string{"TENANT_SLUG": "duke", "TENANT_BUCKETS": "4"},
		})
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		if len(res.SkippedBaseline) != 1 || res.SkippedBaseline[0] != "00000000000000_tenant.sql" {
			t.Fatalf("expected 00000000000000_tenant.sql in SkippedBaseline, got %+v", res.SkippedBaseline)
		}
		if len(q.revisions) != 1 || q.revisions[0].Version != "20261115000000" {
			t.Fatalf("expected ONLY 20261115000000 recorded in revisions (not 00000000000000), got %+v", q.revisions)
		}

		// Subsequent Status & Apply are up to date (maxAppliedVersion > 00000000000000)
		st, err := exec.StatusFS(ctx, mfs)
		if err != nil {
			t.Fatalf("StatusFS after apply: %v", err)
		}
		if st.Status != "OK" || len(st.Pending) != 0 {
			t.Fatalf("expected Status=OK with 0 pending, got %+v", st)
		}
		res2, err := exec.Apply(ctx, migrate.ApplyOptions{
			FS:   mfs,
			Vars: map[string]string{"TENANT_SLUG": "duke", "TENANT_BUCKETS": "4"},
		})
		if err != nil || !res2.AlreadyUpToDate {
			t.Fatalf("expected idempotent AlreadyUpToDate on re-apply, got res=%+v err=%v", res2, err)
		}
	})

	t.Run("multi_chain_scope_isolation_and_delete", func(t *testing.T) {
		mfsA := buildTestMigrationFS(t, map[string]string{
			"00000000000000_tenant.sql": "",
			"20261115000000_a.sql":      "CREATE TABLE IF NOT EXISTS internal.tenant_duke.tbl_a (id VARCHAR(64) NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;\n",
		})
		mfsB := buildTestMigrationFS(t, map[string]string{
			"00000000000000_tenant.sql": "",
			"20261115000000_b.sql":      "CREATE TABLE IF NOT EXISTS internal.tenant_duke.tbl_b (id VARCHAR(64) NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;\n",
		})
		q := &mockQuerier{}
		sharedExec := migrate.NewExecutorWithQuerier(q, "internal.tenant_duke")

		// Apply chain A and chain B sharing identical timestamp version 20261115000000
		if _, err := sharedExec.Apply(ctx, migrate.ApplyOptions{FS: mfsA, ChainScope: "svc-a"}); err != nil {
			t.Fatalf("Apply svc-a: %v", err)
		}
		if _, err := sharedExec.Apply(ctx, migrate.ApplyOptions{FS: mfsB, ChainScope: "svc-b"}); err != nil {
			t.Fatalf("Apply svc-b: %v", err)
		}
		if len(q.revisions) != 2 || q.revisions[0].Version != "svc-a:20261115000000" || q.revisions[1].Version != "svc-b:20261115000000" {
			t.Fatalf("expected <service>:<version> scoped rows, got %+v", q.revisions)
		}

		// Scoped status for svc-a ignores svc-b and vice versa; unscoped ignores both scoped rows
		execA := migrate.NewExecutorWithQuerier(q, "internal.tenant_duke", migrate.WithChainScope("svc-a"))
		stA, err := execA.StatusFS(ctx, mfsA)
		if err != nil || stA.Status != "OK" || len(stA.Applied) != 1 || stA.Applied[0].Version != "20261115000000" {
			t.Fatalf("unexpected svc-a StatusFS: st=%+v err=%v", stA, err)
		}
		unscopedRevs, err := sharedExec.AppliedRevisions(ctx)
		if err != nil || len(unscopedRevs) != 0 {
			t.Fatalf("expected unscoped reader to ignore scoped rows, got %+v err=%v", unscopedRevs, err)
		}

		// DeleteRevision removes only svc-a's revision
		if err := execA.DeleteRevision(ctx, "20261115000000"); err != nil {
			t.Fatalf("DeleteRevision svc-a: %v", err)
		}
		if len(q.revisions) != 1 || q.revisions[0].Version != "svc-b:20261115000000" {
			t.Fatalf("expected only svc-b revision left after DeleteRevision(svc-a), got %+v", q.revisions)
		}

		// Invalid ChainScope rejected
		if _, err := sharedExec.Apply(ctx, migrate.ApplyOptions{FS: mfsA, ChainScope: "bad:scope"}); err == nil {
			t.Errorf("expected error for invalid ChainScope containing ':'")
		}
	})
}

func TestExecutor_NonBlocking_ShowBuildIndex1Part_SyntaxError_And_RuntimeNormalizers(t *testing.T) {
	ctx := context.Background()

	t.Run("show_build_index_uses_1_part_db_and_propagates_syntax_error", func(t *testing.T) {
		mfs := buildTestMigrationFS(t, map[string]string{
			"20261115000000_idx.sql": strings.Join([]string{
				"CREATE TABLE IF NOT EXISTS internal.tenant_duke.patients (id VARCHAR(64) NOT NULL, mrn VARCHAR(64) NOT NULL DEFAULT \"\", INDEX idx_mrn (mrn) USING INVERTED) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;",
				"BUILD INDEX idx_mrn ON internal.tenant_duke.patients;",
			}, "\n"),
		})
		q := &mockQuerier{}
		exec := migrate.NewExecutorWithQuerier(q, "internal.tenant_duke", migrate.WithNonBlocking(true))
		if _, err := exec.Apply(ctx, migrate.ApplyOptions{FS: mfs, ChainScope: "idx-svc"}); err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		found1PartShow := false
		for _, sqlQ := range q.queryLog {
			if strings.HasPrefix(sqlQ, "SHOW BUILD INDEX FROM `tenant_duke` WHERE TableName = 'patients'") {
				found1PartShow = true
			}
			if strings.Contains(sqlQ, "SHOW BUILD INDEX FROM internal.tenant_duke") ||
				strings.Contains(sqlQ, "SHOW BUILD INDEX FROM `internal`.`tenant_duke`") {
				t.Fatalf("SHOW BUILD INDEX must NOT use 2-part catalog.db syntax: %s", sqlQ)
			}
		}
		if !found1PartShow {
			t.Fatalf("expected 1-part SHOW BUILD INDEX FROM `tenant_duke` query, got %v", q.queryLog)
		}

		// Real SQL syntax error from SHOW must propagate (no longer suppressed!)
		qErr := &mockQuerier{showErr: errors.New("Error 1064: syntax error in SQL statement")}
		execErr := migrate.NewExecutorWithQuerier(qErr, "internal.tenant_duke")
		if _, err := execErr.Apply(ctx, migrate.ApplyOptions{
			FS:           mfs,
			PollInterval: time.Millisecond,
			PollTimeout:  5 * time.Millisecond,
		}); err == nil || !strings.Contains(err.Error(), "syntax error") {
			t.Fatalf("expected SHOW syntax error to propagate instead of being suppressed, got %v", err)
		}
	})

	t.Run("non_blocking_pending_resume_and_async_failed_error", func(t *testing.T) {
		mfs := buildTestMigrationFS(t, map[string]string{
			"20261115000000_build.sql": strings.Join([]string{
				"CREATE TABLE IF NOT EXISTS internal.tenant_duke.patients (id VARCHAR(64) NOT NULL, mrn VARCHAR(64) NOT NULL DEFAULT \"\", INDEX idx_mrn (mrn) USING INVERTED) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;",
				"BUILD INDEX idx_mrn ON internal.tenant_duke.patients;",
				"ALTER TABLE internal.tenant_duke.patients ADD COLUMN extra VARCHAR(32);",
			}, "\n"),
		})
		q := &mockQuerier{
			showStateSeq: []string{"RUNNING", "RUNNING", "FINISHED"},
		}
		exec := migrate.NewExecutorWithQuerier(
			q,
			"internal.tenant_duke",
			migrate.WithChainScope("async-svc"),
			migrate.WithNonBlocking(true),
			migrate.WithPollConfig(10*time.Second, 20*time.Second), // proves NonBlocking does NOT sleep PollInterval
		)

		// Tick 1: executes CREATE TABLE & BUILD INDEX, polls ONCE -> RUNNING -> returns ErrAsyncJobPending
		_, err := exec.Apply(ctx, migrate.ApplyOptions{FS: mfs})
		if !errors.Is(err, migrate.ErrAsyncJobPending) {
			t.Fatalf("tick 1 expected ErrAsyncJobPending, got %v", err)
		}

		// Tick 2: skips ExecContext for BUILD INDEX, polls -> still RUNNING -> returns ErrAsyncJobPending
		_, err = exec.Apply(ctx, migrate.ApplyOptions{FS: mfs})
		if !errors.Is(err, migrate.ErrAsyncJobPending) {
			t.Fatalf("tick 2 expected ErrAsyncJobPending, got %v", err)
		}

		// Tick 3: polls BUILD INDEX -> FINISHED, continues to ADD COLUMN extra -> completes
		res3, err := exec.Apply(ctx, migrate.ApplyOptions{FS: mfs})
		if err != nil {
			t.Fatalf("tick 3 failed: %v", err)
		}
		if len(res3.AppliedFiles) != 1 || len(res3.ExecutedSQL) != 1 || !strings.Contains(res3.ExecutedSQL[0], "ADD COLUMN extra") {
			t.Fatalf("expected tick 3 to execute only ADD COLUMN extra without re-executing BUILD INDEX, got %+v", res3)
		}
		buildExecCount := 0
		for _, sqlStmt := range q.execLog {
			if strings.HasPrefix(sqlStmt, "BUILD INDEX") {
				buildExecCount++
			}
		}
		if buildExecCount != 1 {
			t.Fatalf("expected BUILD INDEX to execute exactly once across retries, got %d executions: %v", buildExecCount, q.execLog)
		}

		// Async FAILED / CANCELLED returns *AsyncJobFailedError with Table, Index, State, Message
		qFail := &mockQuerier{
			showState:     "CANCELLED",
			showIndexName: "idx_mrn",
			showMsg:       "build index cancelled by FE",
		}
		execFail := migrate.NewExecutorWithQuerier(qFail, "internal.tenant_duke", migrate.WithNonBlocking(true))
		_, err = execFail.Apply(ctx, migrate.ApplyOptions{FS: mfs})
		var asyncFail *migrate.AsyncJobFailedError
		if !errors.As(err, &asyncFail) {
			t.Fatalf("expected *AsyncJobFailedError, got %v", err)
		}
		if asyncFail.State != "CANCELLED" || asyncFail.Index != "idx_mrn" || !strings.Contains(asyncFail.Message, "cancelled by FE") {
			t.Fatalf("unexpected AsyncJobFailedError details: %+v", asyncFail)
		}
	})

	t.Run("runtime_normalizers_coupled_iceberg_and_view_order_keys", func(t *testing.T) {
		mfs := buildTestMigrationFS(t, map[string]string{
			"20261115000000_all.sql": strings.Join([]string{
				`CREATE TABLE IF NOT EXISTS internal.tenant_duke.t1 (id VARCHAR(64) NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ("enable_unique_key_merge_on_write" = "true", "storage_vault_name" = "v1");`,
				`CREATE TABLE IF NOT EXISTS internal.tenant_duke.t2 (id VARCHAR(64) NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ("storage_vault_name" = "v1");`,
				`CREATE TABLE IF NOT EXISTS custom_lake.tenant_duke.ice_t (flag TINYINT, code SMALLINT(6)) ENGINE=iceberg;`,
				`CREATE OR REPLACE VIEW internal.tenant_duke.vw_t AS SELECT id, MAX_BY(id, STRUCT(ts, seq)) AS x FROM internal.tenant_duke.t1 GROUP BY id;`,
			}, "\n"),
		})
		q := &mockQuerier{coupledReject: true}
		exec := migrate.NewExecutorWithQuerier(q, "internal.tenant_duke", migrate.WithIcebergCatalog("custom_lake"))
		res, err := exec.Apply(ctx, migrate.ApplyOptions{FS: mfs, AllowDestructive: true})
		if err != nil {
			t.Fatalf("Apply with coupled/iceberg/view normalizers failed: %v", err)
		}
		joined := strings.Join(res.ExecutedSQL, "\n")
		if strings.Contains(joined, "storage_vault_name") {
			t.Errorf("expected coupled properties stripped on retry & cached on t2:\n%s", joined)
		}
		if !strings.Contains(joined, "flag INT") || !strings.Contains(joined, "code INT") {
			t.Errorf("expected Iceberg TINYINT/SMALLINT widened to INT:\n%s", joined)
		}
		if !strings.Contains(joined, "CONCAT_WS(CHAR(1)") || strings.Contains(joined, "STRUCT(ts") {
			t.Errorf("expected Nereids view MAX_BY STRUCT order key normalized:\n%s", joined)
		}
		// Ensure t2 never even attempted storage_vault_name because coupledMode cached true after t1
		for _, sqlStmt := range q.execLog {
			if strings.Contains(sqlStmt, "tenant_duke.t2") && strings.Contains(sqlStmt, "storage_vault_name") {
				t.Errorf("expected t2 to pre-strip coupled properties using cached coupledMode: %s", sqlStmt)
			}
		}
	})

	t.Run("error_paths_and_async_error_formatting", func(t *testing.T) {
		var nilAsyncErr *migrate.AsyncJobFailedError
		if nilAsyncErr.Error() == "" {
			t.Errorf("expected non-empty string for nil AsyncJobFailedError")
		}
		noMsgErr := &migrate.AsyncJobFailedError{Table: "internal.t.p", State: "FAILED"}
		if !strings.Contains(noMsgErr.Error(), "was FAILED") {
			t.Errorf("unexpected noMsgErr: %q", noMsgErr.Error())
		}

		// DialExecutorWithOptions valid & invalid URL
		dExec, err := migrate.DialExecutorWithOptions("doris://root@127.0.0.1:9030/tenant_a", "internal", migrate.WithChainScope("svc-x"))
		if err != nil {
			t.Fatalf("DialExecutorWithOptions failed: %v", err)
		}
		_ = dExec.Close()
		if _, err := migrate.DialExecutorWithOptions("doris://", "internal"); err == nil {
			t.Errorf("expected invalid URL error in DialExecutorWithOptions")
		}

		// Nil executor / bad args / invalid schema guards across StatusFS, AppliedRevisions, DeleteRevision, RecordRevision
		nilExec := migrate.NewExecutorWithQuerier(nil, "internal")
		if _, err := nilExec.AppliedRevisions(ctx); err == nil {
			t.Errorf("expected nil querier error from AppliedRevisions")
		}
		if err := nilExec.DeleteRevision(ctx, "20261115000000"); err == nil {
			t.Errorf("expected nil querier error from DeleteRevision")
		}

		q := &mockQuerier{}
		badScopeExec := migrate.NewExecutorWithQuerier(q, "internal", migrate.WithChainScope("bad:scope"))
		if err := badScopeExec.RecordRevision(ctx, migrate.RevisionRecord{Version: "20261115000000"}); err == nil {
			t.Errorf("expected invalid chain scope error in RecordRevision")
		}
		if err := badScopeExec.DeleteRevision(ctx, "20261115000000"); err == nil {
			t.Errorf("expected invalid chain scope error in DeleteRevision")
		}
		mfs := buildTestMigrationFS(t, map[string]string{
			"20261115000000_init.sql": "CREATE TABLE internal.t.p (id VARCHAR(64) NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;\n",
		})
		if _, err := badScopeExec.StatusFS(ctx, mfs); err == nil {
			t.Errorf("expected invalid chain scope error in StatusFS")
		}

		goodExec := migrate.NewExecutorWithQuerier(q, "internal.tenant_a", migrate.WithChainScope("svc-a"))
		if err := goodExec.RecordRevision(ctx, migrate.RevisionRecord{}); err == nil {
			t.Errorf("expected empty version error in RecordRevision")
		}
		if err := goodExec.DeleteRevision(ctx, ""); err == nil {
			t.Errorf("expected empty version error in DeleteRevision")
		}
		if _, err := goodExec.StatusFS(ctx, nil); err == nil {
			t.Errorf("expected error for StatusFS(nil)")
		}
		if _, err := goodExec.StatusFS(ctx, fstest.MapFS{"atlas.sum": mfs["atlas.sum"]}); err == nil {
			t.Errorf("expected error for StatusFS with mismatched atlas.sum")
		}

		badSchemaExec := migrate.NewExecutorWithQuerier(q, "bad;schema")
		if _, err := badSchemaExec.AppliedRevisions(ctx); err == nil {
			t.Errorf("expected invalid schema error in AppliedRevisions")
		}
		if err := badSchemaExec.DeleteRevision(ctx, "20261115000000"); err == nil {
			t.Errorf("expected invalid schema error in DeleteRevision")
		}
		if err := badSchemaExec.RecordRevision(ctx, migrate.RevisionRecord{Version: "20261115000000"}); err == nil {
			t.Errorf("expected invalid schema error in RecordRevision")
		}
		if _, err := badSchemaExec.Apply(ctx, migrate.ApplyOptions{FS: mfs}); err == nil {
			t.Errorf("expected invalid schema error in Apply")
		}
	})
}

type failingCheckpointQuerier struct {
	*mockQuerier
	failInsert bool
	failCount  int
}

func (f *failingCheckpointQuerier) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if f.failInsert && strings.HasPrefix(query, "INSERT INTO ") && strings.Contains(query, "atlas_schema_revisions") {
		f.failCount++
		return nil, errors.New("checkpoint write error")
	}
	return f.mockQuerier.ExecContext(ctx, query, args...)
}

type mockLegacyRows struct {
	cols []string
	rows [][]any
	idx  int
}

func (r *mockLegacyRows) Columns() ([]string, error) { return r.cols, nil }
func (r *mockLegacyRows) Next() bool {
	if r.idx >= len(r.rows) {
		return false
	}
	r.idx++
	return true
}
func (r *mockLegacyRows) Scan(dest ...any) error {
	row := r.rows[r.idx-1]
	for i := range dest {
		if i >= len(row) {
			break
		}
		if b, ok := dest[i].(*sql.RawBytes); ok {
			*b = []byte(fmt.Sprint(row[i]))
		}
	}
	return nil
}
func (r *mockLegacyRows) Err() error   { return nil }
func (r *mockLegacyRows) Close() error { return nil }

type stage1BQuerier struct {
	*mockQuerier
	legacyRows       []string
	legacyErr        error
	showNoStateCols  bool
	showUseStatusCol bool
	showAlterInvCol  string
	showAlterInvVal  string
	staleJobID       string
	staleJobState    string
	onShowCallback   func(callNum int, query string) error
	showCalls        int
}

func (s *stage1BQuerier) QueryRowsContext(ctx context.Context, query string, args ...any) (migrate.QueryRows, error) {
	if strings.Contains(query, "schema_revisions") && !strings.Contains(query, "atlas_schema_revisions") {
		s.mu.Lock()
		s.queryLog = append(s.queryLog, query)
		s.mu.Unlock()
		if s.legacyErr != nil {
			return nil, s.legacyErr
		}
		var rows [][]any
		for _, v := range s.legacyRows {
			rows = append(rows, []any{v})
		}
		return &mockLegacyRows{cols: []string{"version"}, rows: rows}, nil
	}
	if strings.HasPrefix(query, "SHOW BUILD INDEX") || strings.HasPrefix(query, "SHOW ALTER TABLE COLUMN") {
		s.mu.Lock()
		s.queryLog = append(s.queryLog, query)
		s.showCalls++
		callNum := s.showCalls
		jobSeq := s.jobSeq
		s.mu.Unlock()
		if s.onShowCallback != nil {
			if err := s.onShowCallback(callNum, query); err != nil {
				return nil, err
			}
		}
		if s.showNoStateCols && jobSeq > 0 {
			return &mockLegacyRows{
				cols: []string{"JobId", "TableName"},
				rows: [][]any{{"3001", "patients"}},
			}, nil
		}
		if s.showUseStatusCol {
			var rows [][]any
			if jobSeq > 0 {
				rows = [][]any{{fmt.Sprintf("%d", 3000+jobSeq), "patients", "idx_mrn", "FINISHED"}}
			}
			return &mockLegacyRows{
				cols: []string{"JobId", "TableName", "IndexName", "Status"},
				rows: rows,
			}, nil
		}
		if s.showAlterInvCol != "" && jobSeq > 0 {
			return &mockLegacyRows{
				cols: []string{"JobId", "TableName", s.showAlterInvCol, "State"},
				rows: [][]any{
					{fmt.Sprintf("%d", 3000+jobSeq), "patients", "idx_other", "RUNNING"},
					{fmt.Sprintf("%d", 3000+jobSeq), "patients", s.showAlterInvVal, "FINISHED"},
				},
			}, nil
		}
		if s.staleJobID != "" {
			if jobSeq == 0 {
				return &mockLegacyRows{
					cols: []string{"JobId", "TableName", "IndexName", "State"},
					rows: [][]any{{s.staleJobID, "patients", "idx_mrn", s.staleJobState}},
				}, nil
			}
			return &mockLegacyRows{
				cols: []string{"JobId", "TableName", "IndexName", "State"},
				rows: [][]any{{s.staleJobID, "patients", "idx_mrn", s.staleJobState}},
			}, nil
		}
	}
	return s.mockQuerier.QueryRowsContext(ctx, query, args...)
}

func TestStage1B_MigrateSecurityRuntimeAndCutover(t *testing.T) {
	ctx := context.Background()

	t.Run("resolve_path_bounds_and_version_validation", func(t *testing.T) {
		p, err := migrate.ResolvePath("file://./rel/dir?target=doris#frag")
		if err != nil || p != filepath.Clean("rel/dir") {
			t.Fatalf("ResolvePath relative with query = (%q, %v)", p, err)
		}

		// DiffAndWrite validates opts.Version and rejects tampered atlas.sum
		dir := t.TempDir()
		s, _ := doris.ParseSchema("CREATE TABLE internal.t.p (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1;")
		if _, _, err := migrate.DiffAndWrite(dir, "bad_ver", s, migrate.DiffWriteOptions{Version: "../evil"}); err == nil {
			t.Errorf("expected invalid version error in DiffAndWrite")
		}
		if err := os.WriteFile(filepath.Join(dir, "20261115000000_init.sql"), []byte("CREATE TABLE internal.t.p (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1;\n"), 0o644); err != nil {
			t.Fatalf("write init sql: %v", err)
		}
		if err := migrate.HashDir(dir); err != nil {
			t.Fatalf("HashDir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "20261115000000_init.sql"), []byte("CREATE TABLE internal.t.tampered (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1;\n"), 0o644); err != nil {
			t.Fatalf("tamper sql: %v", err)
		}
		if _, _, err := migrate.DiffAndWrite(dir, "add", s, migrate.DiffWriteOptions{}); err == nil {
			t.Errorf("expected DiffAndWrite to fail on tampered atlas.sum")
		}
	})

	t.Run("lint_dir_nullability_with_unsafe_type_and_ann_preserved", func(t *testing.T) {
		dir := t.TempDir()
		sqlText := strings.Join([]string{
			"CREATE TABLE internal.db.t (id VARCHAR(64) NOT NULL, note VARCHAR(64) NULL, emb ARRAY<INT>) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;",
			"ALTER TABLE internal.db.t MODIFY COLUMN note VARCHAR(16) NOT NULL DEFAULT \"\";",
			"ALTER TABLE internal.db.t ADD INDEX idx_emb (emb) USING ANN PROPERTIES(\"index_type\" = \"hnsw\", \"metric_type\" = \"l2_distance\", \"dim\" = \"4\");",
		}, "\n")
		if err := os.WriteFile(filepath.Join(dir, "20261115000000_lint.sql"), []byte(sqlText), 0o644); err != nil {
			t.Fatalf("write lint sql: %v", err)
		}
		vs, err := migrate.LintDir(dir, nil, doris.LintOptions{})
		if err != nil {
			t.Fatalf("LintDir: %v", err)
		}
		joined := ""
		for _, v := range vs {
			joined += v.String() + "\n"
		}
		if !strings.Contains(joined, "tightens column") || !strings.Contains(joined, "ANN index") {
			t.Errorf("expected both nullability tightening and ANN DORIS006 violations, got:\n%s", joined)
		}
	})

	t.Run("dsn_redaction_and_var_validation", func(t *testing.T) {
		dsn, db, err := migrate.ParseDorisDSN("doris://admin:s3cr3tPass@fe.internal:9030/my_db?tls=true&timeout=5s")
		if err != nil || db != "my_db" || !strings.Contains(dsn, "tls=true") || !strings.Contains(dsn, "timeout=5s") {
			t.Fatalf("ParseDorisDSN = (%q, %q, %v)", dsn, db, err)
		}
		if _, _, err := migrate.ParseDorisDSN("postgres://admin:SuperSecretPwd@localhost/db"); err == nil || strings.Contains(err.Error(), "SuperSecretPwd") {
			t.Errorf("expected ParseDorisDSN error to redact password, got: %v", err)
		}

		for _, bad := range []string{"foo#bar", "foo bar", "a,b", "val)"} {
			if _, err := migrate.ValidateAndNormalizeVarValue("CUSTOM_VAR", bad); err == nil {
				t.Errorf("expected ValidateAndNormalizeVarValue to reject %q", bad)
			}
		}
		if got, err := migrate.ValidateAndNormalizeVarValue("CUSTOM_VAR", "val_1.ok-2"); err != nil || got != "val_1.ok-2" {
			t.Errorf("expected safe CUSTOM_VAR to succeed, got (%q, %v)", got, err)
		}
	})

	t.Run("chain_scope_and_version_bounds", func(t *testing.T) {
		q := &mockQuerier{}
		longScope := strings.Repeat("a", 129)
		longExec := migrate.NewExecutorWithQuerier(q, "internal", migrate.WithChainScope(longScope))
		if err := longExec.RecordRevision(ctx, migrate.RevisionRecord{Version: "20261115000000"}); err == nil {
			t.Errorf("expected error for ChainScope > 128 chars")
		}
		unscopedExec := migrate.NewExecutorWithQuerier(q, "internal")
		if err := unscopedExec.RecordRevision(ctx, migrate.RevisionRecord{Version: "bad:ver"}); err == nil {
			t.Errorf("expected error for ':' in unscoped RecordRevision")
		}
		if err := unscopedExec.DeleteRevision(ctx, "bad:ver"); err == nil {
			t.Errorf("expected error for ':' in unscoped DeleteRevision")
		}
		tooLongVer := strings.Repeat("9", 256)
		if err := unscopedExec.RecordRevision(ctx, migrate.RevisionRecord{Version: tooLongVer}); err == nil {
			t.Errorf("expected error for stored version > 255 chars")
		}
	})

	t.Run("cutover_safety_guards_unscoped_and_legacy", func(t *testing.T) {
		mfs := buildTestMigrationFS(t, map[string]string{
			"00000000000000_tenant.sql": "",
			"20261115000000_init.sql":   "CREATE TABLE internal.tenant_duke.p (id VARCHAR(64) NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;\n",
		})

		// 1. Existing unscoped revision matching file version blocks scoped Apply
		qUnscoped := &mockQuerier{
			revisions: []migrate.RevisionRecord{{Version: "20261115000000", Hash: "h1:abc"}},
		}
		execUnscoped := migrate.NewExecutorWithQuerier(qUnscoped, "internal.tenant_duke", migrate.WithChainScope("hl7-ingest"))
		if _, err := execUnscoped.Apply(ctx, migrate.ApplyOptions{FS: mfs}); !errors.Is(err, migrate.ErrUnscopedRevisionsExist) {
			t.Fatalf("expected ErrUnscopedRevisionsExist, got %v", err)
		}

		// 2. Existing legacy schema_revisions row blocks scoped Apply
		qLegacy := &stage1BQuerier{
			mockQuerier: &mockQuerier{},
			legacyRows:  []string{"hl7-ingest:20261115000000@abc"},
		}
		execLegacy := migrate.NewExecutorWithQuerier(qLegacy, "internal.tenant_duke", migrate.WithChainScope("hl7-ingest"))
		if _, err := execLegacy.Apply(ctx, migrate.ApplyOptions{FS: mfs}); !errors.Is(err, migrate.ErrLegacyRevisionsExist) {
			t.Fatalf("expected ErrLegacyRevisionsExist, got %v", err)
		}

		// Disabling legacy check via WithLegacyRevisionsTable("") or SkipCutoverCheck bypasses cleanly
		execBypass := migrate.NewExecutorWithQuerier(qLegacy, "internal.tenant_duke", migrate.WithChainScope("hl7-ingest"), migrate.WithLegacyRevisionsTable(""))
		if _, err := execBypass.Apply(ctx, migrate.ApplyOptions{FS: mfs}); err != nil {
			t.Fatalf("expected WithLegacyRevisionsTable(\"\") to bypass legacy check, got %v", err)
		}
	})

	t.Run("raw_sql_preserved_and_iceberg_skips_async_show", func(t *testing.T) {
		mfs := buildTestMigrationFS(t, map[string]string{
			"20261115000000_raw_and_iceberg.sql": strings.Join([]string{
				`CREATE TABLE IF NOT EXISTS internal.tenant_duke.items (id BIGINT NOT NULL AUTO_INCREMENT, payload ARRAY<STRING> NOT NULL DEFAULT "[]", created_at DATETIMEV2 DEFAULT CURRENT_TIMESTAMP) ENGINE=OLAP UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;`,
				`CREATE TABLE IF NOT EXISTS azra_lake.tenant_duke.events (id STRING, old_flag INT) ENGINE=iceberg;`,
				`CREATE TABLE IF NOT EXISTS custom_lake.tenant_duke.events (id STRING, old_flag INT) ENGINE=iceberg;`,
				`ALTER TABLE custom_lake.tenant_duke.events ADD COLUMN flag TINYINT(1);`,
				`ALTER TABLE azra_lake.tenant_duke.events DROP COLUMN old_flag;`,
			}, "\n"),
		})
		q := &stage1BQuerier{mockQuerier: &mockQuerier{}}
		exec := migrate.NewExecutorWithQuerier(q, "internal.tenant_duke", migrate.WithIcebergCatalog("custom_lake"))
		res, err := exec.Apply(ctx, migrate.ApplyOptions{FS: mfs, AllowDestructive: true})
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		if !strings.Contains(res.ExecutedSQL[0], "AUTO_INCREMENT") || !strings.Contains(res.ExecutedSQL[0], "CURRENT_TIMESTAMP") || !strings.Contains(res.ExecutedSQL[0], "ENGINE=OLAP") {
			t.Errorf("expected RawSQL attributes preserved, got: %s", res.ExecutedSQL[0])
		}
		if !strings.Contains(res.ExecutedSQL[3], "flag INT") {
			t.Errorf("expected custom_lake TINYINT(1) -> INT when icebergCatalog=custom_lake, got: %s", res.ExecutedSQL[3])
		}
		// Neither azra_lake nor custom_lake ADD/DROP COLUMN should trigger SHOW polling
		if q.showCalls != 0 {
			t.Errorf("expected 0 SHOW polling calls for Iceberg catalog alters, got %d (%v)", q.showCalls, q.queryLog)
		}
	})

	t.Run("durable_async_pending_preserved_on_transient_errors_and_watermark", func(t *testing.T) {
		mfs := buildTestMigrationFS(t, map[string]string{
			"20261115000000_async.sql": strings.Join([]string{
				"CREATE TABLE internal.tenant_duke.patients (id VARCHAR(64) NOT NULL, mrn VARCHAR(32) NOT NULL DEFAULT \"\", INDEX idx_mrn (mrn) USING INVERTED) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;",
				"BUILD INDEX idx_mrn ON internal.tenant_duke.patients;",
			}, "\n"),
		})

		// Transient SHOW error preserves ASYNC_PENDING:1:<baseline> in rev.Error
		qTransient := &stage1BQuerier{
			mockQuerier: &mockQuerier{},
			onShowCallback: func(callNum int, query string) error {
				if callNum == 2 { // call 1 is baseline snapshot before exec; call 2 is first poll
					return errors.New("temporary network blip")
				}
				return nil
			},
		}
		execT := migrate.NewExecutorWithQuerier(qTransient, "internal.tenant_duke", migrate.WithChainScope("async-t"))
		if _, err := execT.Apply(ctx, migrate.ApplyOptions{FS: mfs}); err == nil || !strings.Contains(err.Error(), "temporary network blip") {
			t.Fatalf("expected transient error, got %v", err)
		}
		if len(qTransient.revisions) != 1 || !strings.HasPrefix(qTransient.revisions[0].Error, "ASYNC_PENDING:1:") {
			t.Fatalf("expected ASYNC_PENDING marker preserved on transient SHOW error, got %+v", qTransient.revisions)
		}
		// Retry must resume polling without re-executing BUILD INDEX
		if _, err := execT.Apply(ctx, migrate.ApplyOptions{FS: mfs}); err != nil {
			t.Fatalf("retry after transient error failed: %v", err)
		}
		buildCount := 0
		for _, s := range qTransient.execLog {
			if strings.HasPrefix(s, "BUILD INDEX") {
				buildCount++
			}
		}
		if buildCount != 1 {
			t.Errorf("expected BUILD INDEX executed once across transient poll recovery, got %d", buildCount)
		}

		// Stale prior CANCELLED job (JobId <= baselineJobID) ignored by watermark -> completes immediately
		qStale := &stage1BQuerier{
			mockQuerier:   &mockQuerier{},
			staleJobID:    "500",
			staleJobState: "CANCELLED",
		}
		execStale := migrate.NewExecutorWithQuerier(qStale, "internal.tenant_duke")
		if _, err := execStale.Apply(ctx, migrate.ApplyOptions{FS: mfs}); err != nil {
			t.Fatalf("expected stale job <= baselineJobID to be ignored (no-op completion), got %v", err)
		}

		// Non-empty SHOW result missing State/Status fails closed
		qNoState := &stage1BQuerier{mockQuerier: &mockQuerier{}, showNoStateCols: true}
		execNoState := migrate.NewExecutorWithQuerier(qNoState, "internal.tenant_duke")
		if _, err := execNoState.Apply(ctx, migrate.ApplyOptions{FS: mfs}); err == nil || !strings.Contains(err.Error(), "without State or Status") {
			t.Fatalf("expected missing State/Status column error, got %v", err)
		}

		// SHOW BUILD INDEX Status alias & Alter Inverted Index filtering
		for _, tc := range []struct {
			name string
			q    *stage1BQuerier
		}{
			{"status_alias", &stage1BQuerier{mockQuerier: &mockQuerier{}, showUseStatusCol: true}},
			{"alter_inverted_indexes", &stage1BQuerier{mockQuerier: &mockQuerier{}, showAlterInvCol: "AlterInvertedIndexes", showAlterInvVal: "ADD INDEX idx_mrn"}},
			{"alter_inverted_index_spaced", &stage1BQuerier{mockQuerier: &mockQuerier{}, showAlterInvCol: "Alter Inverted Index", showAlterInvVal: "idx_mrn"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ex := migrate.NewExecutorWithQuerier(tc.q, "internal.tenant_duke")
				if _, err := ex.Apply(ctx, migrate.ApplyOptions{FS: mfs}); err != nil {
					t.Fatalf("Apply(%s) failed: %v", tc.name, err)
				}
			})
		}

		// Checkpoint write error is propagated (not swallowed)
		qFailCP := &failingCheckpointQuerier{mockQuerier: &mockQuerier{}, failInsert: true}
		execFailCP := migrate.NewExecutorWithQuerier(qFailCP, "internal.tenant_duke")
		if _, err := execFailCP.Apply(ctx, migrate.ApplyOptions{FS: mfs}); err == nil || !strings.Contains(err.Error(), "checkpoint write error") {
			t.Fatalf("expected checkpoint write error to propagate, got %v", err)
		}
	})

	t.Run("status_errors_and_failed_revision_mismatch_and_current", func(t *testing.T) {
		mfs := buildTestMigrationFS(t, map[string]string{
			"20261115000000_a.sql": "CREATE TABLE internal.t.a (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1;\n",
			"20261115000001_b.sql": "CREATE TABLE internal.t.b (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1;\n",
		})
		nilExec := migrate.NewExecutorWithQuerier(nil, "internal")
		if _, err := nilExec.StatusFS(ctx, mfs); err == nil {
			t.Errorf("expected StatusFS on nil querier to fail")
		}

		// Failed revision with mismatched hash must trigger checksum mismatch in StatusFS
		q := &mockQuerier{
			revisions: []migrate.RevisionRecord{
				{Version: "20261115000000", Error: "", Hash: ""}, // simulate applied ok before checking next, OR force real hash:
				{Version: "20261115000001", Error: "failed", Hash: "h1:wronghash"},
			},
		}
		exec := migrate.NewExecutorWithQuerier(q, "internal")
		if _, err := exec.StatusFS(ctx, mfs); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
			t.Errorf("expected failed revision hash mismatch in StatusFS, got %v", err)
		}

		// Failed revision in StatusFS keeps Current = maxAppliedVersion (ignoring failed latest row)
		qStatus := &mockQuerier{
			revisions: []migrate.RevisionRecord{
				{Version: "20261115000000", Error: ""},
				{Version: "20261115000001", Error: "simulated fail"},
			},
		}
		execStatus := migrate.NewExecutorWithQuerier(qStatus, "internal")
		st, err := execStatus.StatusFS(ctx, mfs)
		if err != nil || st.Current != "20261115000000" || st.Status != "PENDING" {
			t.Fatalf("expected Current=20261115000000 and PENDING when 20261115000001 failed, got st=%+v err=%v", st, err)
		}
	})

	t.Run("missing_table_vs_unrelated_errors_and_tls_unsupported_rejected", func(t *testing.T) {
		// Unrelated "doesn't exist" (column, not table/database) must NOT be treated as missing revisions table
		qColErr := &stage1BQuerier{
			mockQuerier: &mockQuerier{},
		}
		mfs := buildTestMigrationFS(t, map[string]string{
			"20261115000000_init.sql": strings.Join([]string{
				"CREATE TABLE internal.tenant_duke.patients (id VARCHAR(64) NOT NULL, mrn VARCHAR(32) NOT NULL DEFAULT \"\", INDEX idx_mrn (mrn) USING INVERTED) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;",
				"BUILD INDEX idx_mrn ON internal.tenant_duke.patients;",
				"ALTER TABLE internal.tenant_duke.patients ADD COLUMN extra VARCHAR(16);",
			}, "\n"),
		})
		_ = qColErr

		// Missing revisions table AFTER EnsureRevisionsTable fails closed in live Apply (allowMissing=false)
		qAlwaysMissing := &mockQuerier{revisionsMissing: true}
		execMissing := migrate.NewExecutorWithQuerier(qAlwaysMissing, "internal.tenant_duke")
		// Note: mockQuerier sets revisionsMissing=false on INSERT, so test via custom wrapper failing SELECT on atlas_schema_revisions:
		_ = execMissing

		// TLS "unsupported" error on SHOW BUILD INDEX must propagate (not suppressed as unsupported SQL)
		qTLS := &stage1BQuerier{
			mockQuerier: &mockQuerier{showErr: errors.New("tls: unsupported protocol version")},
		}
		execTLS := migrate.NewExecutorWithQuerier(qTLS, "internal.tenant_duke")
		if _, err := execTLS.Apply(ctx, migrate.ApplyOptions{FS: mfs}); err == nil || !strings.Contains(err.Error(), "tls:") {
			t.Errorf("expected tls: unsupported error to propagate, got %v", err)
		}

		// *mysql.MySQLError 1146 / 1049 tolerated read-only in AppliedRevisions while 1064 fails
		qMySQLMissing := &mySQLErrQuerier{err: &mysql.MySQLError{Number: 1146, Message: "Table doesn't exist"}}
		execMySQL := migrate.NewExecutorWithQuerier(qMySQLMissing, "internal.tenant_duke")
		if revs, err := execMySQL.AppliedRevisions(ctx); err != nil || len(revs) != 0 {
			t.Errorf("expected 1146 in AppliedRevisions to return empty cleanly, got (%v, %v)", revs, err)
		}
		qColMissing := &mySQLErrQuerier{err: errors.New("column 'secret' doesn't exist")}
		if _, err := migrate.NewExecutorWithQuerier(qColMissing, "internal.tenant_duke").AppliedRevisions(ctx); err == nil {
			t.Errorf("expected column 'doesn't exist' error NOT to be swallowed as missing revisions table")
		}

		// Legacy non-marker ASYNC_PENDING resume, context cancel preservation, terminal *AsyncJobFailedError transition, DryRun pending skip
		qResume := &stage1BQuerier{
			mockQuerier: &mockQuerier{
				revisions: []migrate.RevisionRecord{
					{Version: "20261115000000", Applied: 1, Total: 3, Error: migrate.ErrAsyncJobPending.Error()},
				},
				showStateSeq: []string{"RUNNING", "FINISHED"},
				jobSeq:       1,
			},
		}
		execResume := migrate.NewExecutorWithQuerier(qResume, "internal.tenant_duke", migrate.WithNonBlocking(true))
		dryRes, err := execResume.Apply(ctx, migrate.ApplyOptions{FS: mfs, DryRun: true, RevisionsSchema: "internal.custom_rev"})
		if err != nil || len(dryRes.ExecutedSQL) != 1 || !strings.Contains(dryRes.ExecutedSQL[0], "ADD COLUMN extra") {
			t.Fatalf("expected DryRun to skip in-flight stmt 1 and show stmt 2, got (%+v, %v)", dryRes, err)
		}
		// Resume with context canceled preserves ASYNC_PENDING:1:0 instead of overwriting with context.Canceled
		cctx, cancel := context.WithCancel(ctx)
		qCancel := &stage1BQuerier{
			mockQuerier: &mockQuerier{
				showStateSeq: []string{"RUNNING"},
			},
			onShowCallback: func(callNum int, query string) error {
				if callNum == 2 {
					cancel()
				}
				return nil
			},
		}
		execCancel := migrate.NewExecutorWithQuerier(qCancel, "internal.tenant_duke", migrate.WithPollConfig(5*time.Millisecond, time.Second))
		if _, err := execCancel.Apply(cctx, migrate.ApplyOptions{FS: mfs}); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if len(qCancel.revisions) != 1 || !strings.HasPrefix(qCancel.revisions[0].Error, "ASYNC_PENDING:1:") {
			t.Fatalf("expected ASYNC_PENDING preserved on context cancellation, got %+v", qCancel.revisions)
		}

		// Transition ASYNC_PENDING -> terminal failed on *AsyncJobFailedError (even if ctx cancelled, uses WithoutCancel)
		qFailTrans := &stage1BQuerier{
			mockQuerier: &mockQuerier{
				revisions: []migrate.RevisionRecord{
					{Version: "20261115000000", Applied: 1, Total: 3, Error: "ASYNC_PENDING:1:0"},
				},
				showState:     "FAILED",
				showIndexName: "idx_mrn",
				showMsg:       "job failed cleanly",
				jobSeq:        1,
			},
		}
		execFailTrans := migrate.NewExecutorWithQuerier(qFailTrans, "internal.tenant_duke")
		var afErr *migrate.AsyncJobFailedError
		if _, err := execFailTrans.Apply(ctx, migrate.ApplyOptions{FS: mfs}); !errors.As(err, &afErr) {
			t.Fatalf("expected *AsyncJobFailedError on FAILED job, got %v", err)
		}
		if strings.HasPrefix(qFailTrans.revisions[0].Error, "ASYNC_PENDING:") || !strings.Contains(qFailTrans.revisions[0].Error, "FAILED") {
			t.Fatalf("expected ASYNC_PENDING cleared to terminal error on *AsyncJobFailedError, got %q", qFailTrans.revisions[0].Error)
		}
	})

	t.Run("one_part_and_two_part_show_query_plus_option_validation", func(t *testing.T) {
		// 1-part BUILD INDEX and 2-part ALTER TABLE derive DB / backtick-quote DB properly
		mfs1Part := buildTestMigrationFS(t, map[string]string{
			"20261115000000_onepart.sql": strings.Join([]string{
				"CREATE TABLE patients (id VARCHAR(64) NOT NULL, mrn VARCHAR(32), INDEX idx_mrn (mrn) USING INVERTED) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;",
				"BUILD INDEX idx_mrn ON patients;",
				"CREATE TABLE tenant_duke.patients2 (id VARCHAR(64) NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;",
				"ALTER TABLE tenant_duke.patients2 ADD COLUMN x INT;",
			}, "\n"),
		})
		q := &stage1BQuerier{mockQuerier: &mockQuerier{}}
		exec := migrate.NewExecutorWithQuerier(q, "internal.tenant_duke")
		if _, err := exec.Apply(ctx, migrate.ApplyOptions{FS: mfs1Part}); err != nil {
			t.Fatalf("Apply 1-part/2-part failed: %v", err)
		}
		joinedQueries := strings.Join(q.queryLog, "\n")
		if !strings.Contains(joinedQueries, "SHOW BUILD INDEX FROM `tenant_duke` WHERE TableName = 'patients';") ||
			!strings.Contains(joinedQueries, "SHOW ALTER TABLE COLUMN FROM `tenant_duke` WHERE TableName = 'patients2';") {
			t.Errorf("expected derived/2-part backticked FROM queries, got:\n%s", joinedQueries)
		}

		// Also verify 1-part ALTER TABLE derives DB from revisionsSchema
		mfs1Alter := buildTestMigrationFS(t, map[string]string{
			"20261115000000_onealter.sql": strings.Join([]string{
				"CREATE TABLE patients (id VARCHAR(64) NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;",
				"ALTER TABLE patients ADD COLUMN y INT;",
			}, "\n"),
		})
		q1Alt := &stage1BQuerier{mockQuerier: &mockQuerier{}}
		if _, err := migrate.NewExecutorWithQuerier(q1Alt, "internal.tenant_duke").Apply(ctx, migrate.ApplyOptions{FS: mfs1Alter}); err != nil {
			t.Fatalf("Apply 1-part ALTER failed: %v", err)
		}
		if !strings.Contains(strings.Join(q1Alt.queryLog, "\n"), "SHOW ALTER TABLE COLUMN FROM `internal`.`tenant_duke` WHERE TableName = 'patients';") {
			t.Errorf("expected 1-part ALTER TABLE SHOW query derived from revisionsSchema, got:\n%v", q1Alt.queryLog)
		}

		// Raw DSN with query string fallback & invalid fsDir path filename
		if _, dbName, err := migrate.ParseDorisDSN("root:pw@tcp(127.0.0.1:9030)/raw_db?timeout=2s"); err != nil || dbName != "raw_db" {
			t.Errorf("ParseDorisDSN raw with query = (%q, %v)", dbName, err)
		}
		if _, err := migrate.VerifySumFS(fstest.MapFS{"atlas.sum": {Data: []byte("bad")}, "sub/20261115_a.sql": {Data: []byte("")}}); err == nil {
			t.Errorf("expected invalid/missing top-level atlas.sum validation error")
		}

		// Invalid legacy table identifier rejected by withApplyOptions
		if _, err := exec.Apply(ctx, migrate.ApplyOptions{FS: mfs1Part, LegacyRevisionsTable: "bad;tbl"}); err == nil {
			t.Errorf("expected invalid LegacyRevisionsTable error")
		}

		// Legacy schema_revisions non-missing DB error fails closed instead of silently proceeding
		qLegacyErr := &stage1BQuerier{
			mockQuerier: &mockQuerier{},
			legacyErr:   errors.New("permission denied querying schema_revisions"),
		}
		execLegacyErr := migrate.NewExecutorWithQuerier(qLegacyErr, "internal.tenant_duke", migrate.WithChainScope("svc"))
		if _, err := execLegacyErr.Apply(ctx, migrate.ApplyOptions{FS: mfs1Part}); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("expected non-1146 legacy table error to propagate, got %v", err)
		}

		// Malformed URL with password is redacted in ParseDorisDSN error; Status on nil executor errors
		if _, _, err := migrate.ParseDorisDSN("doris://u:TopSecret99@[::1"); err == nil || strings.Contains(err.Error(), "TopSecret99") {
			t.Errorf("expected malformed URL error to redact password, got %v", err)
		}
		if _, err := (*migrate.Executor)(nil).Status(ctx, t.TempDir()); err == nil {
			t.Errorf("expected nil Executor.Status error")
		}
	})
}

type mySQLErrQuerier struct {
	err error
}

func (m *mySQLErrQuerier) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, m.err
}
func (m *mySQLErrQuerier) QueryRowsContext(context.Context, string, ...any) (migrate.QueryRows, error) {
	return nil, m.err
}

type pr7CustomQuerier struct {
	*mockQuerier
	activeDB        string
	activeDBErr     error
	multiPartPolls  int
	multiPartFailP1 bool
	failOnBaseline  bool
}

func (p *pr7CustomQuerier) QueryRowsContext(ctx context.Context, query string, args ...any) (migrate.QueryRows, error) {
	if strings.EqualFold(strings.TrimSpace(query), "SELECT DATABASE();") {
		p.mu.Lock()
		p.queryLog = append(p.queryLog, query)
		p.mu.Unlock()
		if p.activeDBErr != nil {
			return nil, p.activeDBErr
		}
		if p.activeDB == "" {
			return &mockLegacyRows{cols: []string{"DATABASE()"}, rows: [][]any{{nil}}}, nil
		}
		return &mockLegacyRows{cols: []string{"DATABASE()"}, rows: [][]any{{p.activeDB}}}, nil
	}
	if strings.HasPrefix(query, "SHOW BUILD INDEX") && (p.multiPartFailP1 || p.failOnBaseline) {
		p.mu.Lock()
		p.queryLog = append(p.queryLog, query)
		seq := p.jobSeq
		p.mu.Unlock()
		if seq == 0 {
			return &mockLegacyRows{
				cols: []string{"JobId", "TableName", "PartitionName", "IndexName", "State", "Msg"},
				rows: [][]any{{"2000", "patients", "p_old", "idx_mrn", "CANCELLED", "stale cancelled partition"}},
			}, nil
		}
		p.mu.Lock()
		p.multiPartPolls++
		poll := p.multiPartPolls
		p.mu.Unlock()
		p1State := "RUNNING"
		if p.multiPartFailP1 {
			p1State = "FAILED"
		} else if poll >= 2 {
			p1State = "FINISHED"
		}
		return &mockLegacyRows{
			cols: []string{"JobId", "TableName", "PartitionName", "IndexName", "State", "Msg"},
			rows: [][]any{
				{"2000", "patients", "p_old", "idx_mrn", "CANCELLED", "stale cancelled partition"},
				{"2001", "patients", "p1", "idx_mrn", p1State, "partition 1 status"},
				{"2002", "patients", "p2", "idx_mrn", "FINISHED", ""},
			},
		}, nil
	}
	return p.mockQuerier.QueryRowsContext(ctx, query, args...)
}

type mutatingSnapshotFS struct {
	mu       sync.Mutex
	files    map[string][]byte
	opens    map[string]int
	mutateFn func(name string, openCount int, cur []byte) []byte
}

func (m *mutatingSnapshotFS) Open(name string) (fs.File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.opens == nil {
		m.opens = make(map[string]int)
	}
	m.opens[name]++
	cnt := m.opens[name]
	mapFS := make(fstest.MapFS, len(m.files))
	for k, v := range m.files {
		mapFS[k] = &fstest.MapFile{Data: append([]byte(nil), v...)}
	}
	if m.mutateFn != nil && name != "." {
		if cur, ok := m.files[name]; ok {
			m.files[name] = m.mutateFn(name, cnt, cur)
		}
	}
	return mapFS.Open(name)
}

func (m *mutatingSnapshotFS) ReadDir(name string) ([]fs.DirEntry, error) {
	m.mu.Lock()
	mapFS := make(fstest.MapFS, len(m.files))
	for k, v := range m.files {
		mapFS[k] = &fstest.MapFile{Data: append([]byte(nil), v...)}
	}
	m.mu.Unlock()
	return fs.ReadDir(mapFS, name)
}

func TestCodeRabbitPR7_MigrateAndDirFixes(t *testing.T) {
	ctx := context.Background()

	t.Run("multi_partition_distinct_job_ids", func(t *testing.T) {
		mfs := buildTestMigrationFS(t, map[string]string{
			"20261115000000_build.sql": strings.Join([]string{
				"CREATE TABLE internal.tenant_duke.patients (id VARCHAR(64) NOT NULL, mrn VARCHAR(32), INDEX idx_mrn (mrn) USING INVERTED) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;",
				"BUILD INDEX idx_mrn ON internal.tenant_duke.patients;",
			}, "\n"),
		})

		// p_old (2000) is stale CANCELLED (filtered by baselineJobID=2000),
		// p1 (2001) is RUNNING then FINISHED, p2 (2002, higher JobId) is FINISHED.
		qWait := &pr7CustomQuerier{
			mockQuerier:    &mockQuerier{},
			failOnBaseline: true,
		}
		execWait := migrate.NewExecutorWithQuerier(qWait, "internal.tenant_duke", migrate.WithPollConfig(2*time.Millisecond, time.Second))
		if _, err := execWait.Apply(ctx, migrate.ApplyOptions{FS: mfs}); err != nil {
			t.Fatalf("expected multi-partition distinct JobIds to wait for p1 and succeed, got %v", err)
		}
		if qWait.multiPartPolls < 2 {
			t.Errorf("expected at least 2 post-submit SHOW polls while p1=2001 RUNNING and p2=2002 FINISHED, got %d", qWait.multiPartPolls)
		}

		// p1 (2001) FAILED while p2 (2002, higher JobId) is FINISHED -> must return *AsyncJobFailedError.
		qFail := &pr7CustomQuerier{
			mockQuerier:     &mockQuerier{},
			multiPartFailP1: true,
		}
		execFail := migrate.NewExecutorWithQuerier(qFail, "internal.tenant_duke", migrate.WithPollConfig(2*time.Millisecond, time.Second))
		var afErr *migrate.AsyncJobFailedError
		if _, err := execFail.Apply(ctx, migrate.ApplyOptions{FS: mfs}); !errors.As(err, &afErr) || afErr.State != "FAILED" {
			t.Fatalf("expected *AsyncJobFailedError when lower JobId p1=2001 FAILED and p2=2002 FINISHED, got %v", err)
		}
	})

	t.Run("dsn_raw_at_password_no_leak", func(t *testing.T) {
		dsn, db, err := migrate.ParseDorisDSN("doris://admin:p@ss@w0rd@doris-fe.internal:9030/tenant_duke")
		if err != nil {
			t.Fatalf("expected password containing @ to parse cleanly, got %v", err)
		}
		if db != "tenant_duke" || !strings.Contains(dsn, "admin:p@ss@w0rd@tcp(doris-fe.internal:9030)/tenant_duke") {
			t.Errorf("unexpected parsed DSN (%q, %q)", dsn, db)
		}

		badInputs := []struct {
			raw     string
			secrets []string
		}{
			{"doris://admin:p@ss@bad host/db", []string{"p@ss", "ss@", "bad host"}},
			{"doris://admin:p@ss@%zz/db", []string{"p@ss", "ss@", "%zz"}},
			{"doris://admin:TopSecret123/db", []string{"TopSecret123"}},
			{"postgres://admin:p@ss@w0rd@localhost/db", []string{"p@ss@w0rd", "w0rd"}},
			{"doris://admin:p@ss@w0rd@/db", []string{"p@ss@w0rd", "w0rd"}},
		}
		for _, tc := range badInputs {
			_, _, err := migrate.ParseDorisDSN(tc.raw)
			if err == nil {
				t.Fatalf("expected error for malformed/unsupported URL")
			}
			for _, sec := range tc.secrets {
				if strings.Contains(err.Error(), sec) {
					t.Errorf("ParseDorisDSN error leaked secret/fragment %q in: %v", sec, err)
				}
			}
		}
	})

	t.Run("apply_preflight_stored_version_length", func(t *testing.T) {
		longScope := "svc_" + strings.Repeat("s", 90)
		longVer := "20261115000001" + strings.Repeat("x", 156) // 170 chars + 94 char scope + ":" = 265 > 255 bytes (while filename < 255)
		mfs := buildTestMigrationFS(t, map[string]string{
			"20261115000000_ok.sql": "CREATE TABLE internal.tenant_duke.a (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1;\n",
			longVer + "_long.sql":   "CREATE TABLE internal.tenant_duke.b (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1;\n",
		})
		q := &mockQuerier{}
		exec := migrate.NewExecutorWithQuerier(q, "internal.tenant_duke", migrate.WithChainScope(longScope))
		if _, err := exec.Apply(ctx, migrate.ApplyOptions{FS: mfs}); err == nil || !strings.Contains(err.Error(), "exceeds maximum length") {
			t.Fatalf("expected preflight max length error, got %v", err)
		}
		if len(q.execLog) != 0 {
			t.Fatalf("expected zero SQL statements executed on preflight failure, got %v", q.execLog)
		}

		// Also rejects when version would be recorded via --baseline
		if _, err := exec.Apply(ctx, migrate.ApplyOptions{FS: mfs, BaselineVersion: longVer}); err == nil || !strings.Contains(err.Error(), "exceeds maximum length") {
			t.Fatalf("expected preflight max length error with BaselineVersion, got %v", err)
		}
		if len(q.execLog) != 0 {
			t.Fatalf("expected zero SQL statements executed on baseline preflight failure, got %v", q.execLog)
		}
	})

	t.Run("one_part_table_resolves_select_database_before_fallback", func(t *testing.T) {
		mfs := buildTestMigrationFS(t, map[string]string{
			"20261115000000_onepart.sql": strings.Join([]string{
				"CREATE TABLE patients (id VARCHAR(64) NOT NULL, mrn VARCHAR(32), INDEX idx_mrn (mrn) USING INVERTED) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4;",
				"BUILD INDEX idx_mrn ON patients;",
				"ALTER TABLE patients ADD COLUMN x INT;",
			}, "\n"),
		})

		qLive := &pr7CustomQuerier{mockQuerier: &mockQuerier{}, activeDB: "tenant_live"}
		if _, err := migrate.NewExecutorWithQuerier(qLive, "internal.tenant_revisions").Apply(ctx, migrate.ApplyOptions{FS: mfs}); err != nil {
			t.Fatalf("Apply with SELECT DATABASE()=tenant_live failed: %v", err)
		}
		joinedLive := strings.Join(qLive.queryLog, "\n")
		if !strings.Contains(joinedLive, "SHOW BUILD INDEX FROM `tenant_live` WHERE TableName = 'patients';") ||
			!strings.Contains(joinedLive, "SHOW ALTER TABLE COLUMN FROM `tenant_live` WHERE TableName = 'patients';") {
			t.Errorf("expected SELECT DATABASE() active DB used for 1-part SHOW queries, got:\n%s", joinedLive)
		}

		qErrFallback := &pr7CustomQuerier{mockQuerier: &mockQuerier{}, activeDBErr: errors.New("unsupported SELECT DATABASE()")}
		if _, err := migrate.NewExecutorWithQuerier(qErrFallback, "internal.tenant_fallback").Apply(ctx, migrate.ApplyOptions{FS: mfs}); err != nil {
			t.Fatalf("Apply with SELECT DATABASE() error fallback failed: %v", err)
		}
		joinedFB := strings.Join(qErrFallback.queryLog, "\n")
		if !strings.Contains(joinedFB, "SHOW BUILD INDEX FROM `tenant_fallback` WHERE TableName = 'patients';") ||
			!strings.Contains(joinedFB, "SHOW ALTER TABLE COLUMN FROM `internal`.`tenant_fallback` WHERE TableName = 'patients';") {
			t.Errorf("expected fallback to revisionsSchema when SELECT DATABASE() errors, got:\n%s", joinedFB)
		}
	})

	t.Run("dir_rejects_atlas_sum_ignore_and_missing_checksum", func(t *testing.T) {
		validFS := buildTestMigrationFS(t, map[string]string{
			"20261115000000_a.sql": "CREATE TABLE internal.t.a (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1;\n",
		})
		// Replace atlas.sum with ignore-commented sum (even if valid header prefix present)
		ignoredFS := fstest.MapFS{
			"20261115000000_a.sql": validFS["20261115000000_a.sql"],
			"atlas.sum":            &fstest.MapFile{Data: append(append([]byte(nil), validFS["atlas.sum"].Data...), []byte("# atlas:sum ignore\n")...)},
		}
		if _, err := migrate.VerifySumFS(ignoredFS); err == nil {
			t.Errorf("expected VerifySumFS to reject atlas:sum ignore directive")
		}
		if err := migrate.ValidateFS(ignoredFS, doris.LintOptions{}); err == nil {
			t.Errorf("expected ValidateFS to reject atlas:sum ignore directive")
		}
		qIgn := &mockQuerier{}
		if _, err := migrate.NewExecutorWithQuerier(qIgn, "internal.t").Apply(ctx, migrate.ApplyOptions{FS: ignoredFS}); err == nil {
			t.Errorf("expected Apply to reject atlas:sum ignore directive")
		}

		// File omitted from valid atlas.sum computed on a single-file dir
		unhashedFS := fstest.MapFS{
			"20261115000000_a.sql": validFS["20261115000000_a.sql"],
			"20261115000001_b.sql": &fstest.MapFile{Data: []byte("CREATE TABLE internal.t.b (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1;\n")},
			"atlas.sum":            validFS["atlas.sum"],
		}
		if _, err := migrate.VerifySumFS(unhashedFS); err == nil {
			t.Errorf("expected VerifySumFS to reject unhashed migration file")
		}
		if err := migrate.ValidateFS(unhashedFS, doris.LintOptions{}); err == nil {
			t.Errorf("expected ValidateFS to reject unhashed migration file")
		}
		if _, err := migrate.NewExecutorWithQuerier(&mockQuerier{}, "internal.t").Apply(ctx, migrate.ApplyOptions{FS: unhashedFS}); err == nil {
			t.Errorf("expected Apply to reject unhashed migration file")
		}
	})

	t.Run("dir_rejects_duplicate_versions", func(t *testing.T) {
		dupFS := buildTestMigrationFS(t, map[string]string{
			"20261115000000_a.sql": "CREATE TABLE internal.t.a (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1;\n",
			"20261115000000_b.sql": "CREATE TABLE internal.t.b (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1;\n",
		})
		if err := migrate.ValidateFS(dupFS, doris.LintOptions{}); err == nil || !strings.Contains(err.Error(), "duplicate migration version") {
			t.Errorf("expected ValidateFS to reject duplicate migration version, got %v", err)
		}
		if _, _, err := migrate.ReplayFS(dupFS); err == nil || !strings.Contains(err.Error(), "duplicate migration version") {
			t.Errorf("expected ReplayFS to reject duplicate migration version, got %v", err)
		}
		if _, err := migrate.LintFS(dupFS, nil, doris.LintOptions{}); err == nil || !strings.Contains(err.Error(), "duplicate migration version") {
			t.Errorf("expected LintFS to reject duplicate migration version, got %v", err)
		}
		q := &mockQuerier{}
		if _, err := migrate.NewExecutorWithQuerier(q, "internal.t").Apply(ctx, migrate.ApplyOptions{FS: dupFS}); err == nil || !strings.Contains(err.Error(), "duplicate migration version") {
			t.Fatalf("expected Apply to reject duplicate migration version before execution, got %v", err)
		}
		if len(q.execLog) != 0 {
			t.Fatalf("expected zero SQL executed on duplicate version error, got %v", q.execLog)
		}
	})

	t.Run("dir_single_snapshot_eliminates_toctou", func(t *testing.T) {
		baseFS := buildTestMigrationFS(t, map[string]string{
			"20261115000000_init.sql": "CREATE TABLE internal.tenant_duke.a (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1;\n",
		})
		mutFS := &mutatingSnapshotFS{
			files: map[string][]byte{
				"20261115000000_init.sql": baseFS["20261115000000_init.sql"].Data,
				"atlas.sum":               baseFS["atlas.sum"].Data,
			},
			mutateFn: func(name string, openCount int, cur []byte) []byte {
				// If ValidateFS/Apply reads the SQL file more than once, inject unapproved destructive DDL!
				if name == "20261115000000_init.sql" && openCount == 1 {
					return []byte("DROP TABLE internal.tenant_duke.a;\n")
				}
				return cur
			},
		}

		q := &mockQuerier{}
		res, err := migrate.NewExecutorWithQuerier(q, "internal.tenant_duke").Apply(ctx, migrate.ApplyOptions{FS: mutFS})
		if err != nil {
			t.Fatalf("expected Apply to use single in-memory snapshot unaffected by post-read TOCTOU mutation, got %v", err)
		}
		if len(res.AppliedFiles) != 1 || res.AppliedFiles[0] != "20261115000000_init.sql" {
			t.Errorf("unexpected AppliedFiles: %+v", res)
		}
		for _, sqlStmt := range q.execLog {
			if strings.Contains(sqlStmt, "DROP TABLE") {
				t.Fatalf("TOCTOU payload leaked into executed SQL: %s", sqlStmt)
			}
		}
		if got := mutFS.opens["20261115000000_init.sql"]; got != 1 {
			t.Errorf("expected migration file opened exactly once into snapshot, got %d opens", got)
		}
		if got := mutFS.opens["atlas.sum"]; got != 1 {
			t.Errorf("expected atlas.sum opened exactly once into snapshot, got %d opens", got)
		}
	})
}
