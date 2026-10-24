// Package migrate manages Atlas-compatible migration directories (atlas.sum),
// offline migration replay, diff generation, validation, and Doris FE execution.
package migrate

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"ariga.io/atlas/sql/migrate"
	"github.com/protocgen/atlas-doris/pkg/doris"
)

// MigrationFile represents a single versioned SQL migration file in a migration chain.
type MigrationFile struct {
	Name        string
	Version     string
	Description string
	Hash        string
	SQL         string
	Statements  []doris.Statement
}

// DiffWriteOptions configures DiffAndWrite behavior.
type DiffWriteOptions struct {
	doris.LintOptions
	Version string // Optional explicit 14-digit version string for deterministic tests
}

// ResolvePath converts a raw path or file:// URL into a clean local filesystem path.
func ResolvePath(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", fmt.Errorf("empty path")
	}
	if strings.HasPrefix(s, "file://") {
		u, err := url.Parse(s)
		if err != nil {
			return "", fmt.Errorf("invalid file URL %q: %w", raw, err)
		}
		p := u.Path
		if u.Host != "" && u.Host != "localhost" {
			p = filepath.Join(u.Host, u.Path)
		} else if strings.HasPrefix(s, "file://./") || strings.HasPrefix(s, "file://../") {
			p = strings.TrimPrefix(s, "file://")
		}
		return filepath.Clean(p), nil
	}
	return filepath.Clean(s), nil
}

// ReplayDir reads all migration .sql files from dirPath in lexical version order
// and replays them offline to construct the current doris.Schema state.
func ReplayDir(dirPath string) (*doris.Schema, []MigrationFile, error) {
	cleanPath, err := ResolvePath(dirPath)
	if err != nil {
		return nil, nil, err
	}
	localDir, err := migrate.NewLocalDir(cleanPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open migration directory %q: %w", cleanPath, err)
	}
	files, err := localDir.Files()
	if err != nil {
		return nil, nil, fmt.Errorf("read migration files in %q: %w", cleanPath, err)
	}
	hf, _ := localDir.Checksum()

	schema := doris.NewSchema()
	var migFiles []MigrationFile
	for _, f := range files {
		rawSQL := string(f.Bytes())
		stmts, err := doris.ParseStatements(rawSQL)
		if err != nil {
			return nil, nil, fmt.Errorf("migration %s: %w", f.Name(), err)
		}
		for i, st := range stmts {
			if err := doris.ApplyStatement(schema, st); err != nil {
				return nil, nil, fmt.Errorf("migration %s statement %d: %w", f.Name(), i+1, err)
			}
		}
		hashVal := ""
		if hf != nil {
			if h, err := hf.SumByName(f.Name()); err == nil {
				hashVal = "h1:" + h
			}
		}
		migFiles = append(migFiles, MigrationFile{
			Name:        f.Name(),
			Version:     f.Version(),
			Description: f.Desc(),
			Hash:        hashVal,
			SQL:         rawSQL,
			Statements:  stmts,
		})
	}
	return schema, migFiles, nil
}

// LintDir replays all migrations in dirPath step-by-step, validating each step
// (and optional desired target schema) against Doris/Iceberg evolution lint rules.
func LintDir(dirPath string, desired *doris.Schema, opts doris.LintOptions) ([]doris.Violation, error) {
	cleanPath, err := ResolvePath(dirPath)
	if err != nil {
		return nil, err
	}
	localDir, err := migrate.NewLocalDir(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("open migration directory %q: %w", cleanPath, err)
	}
	files, err := localDir.Files()
	if err != nil {
		return nil, fmt.Errorf("read migration files in %q: %w", cleanPath, err)
	}

	current := doris.NewSchema()
	var violations []doris.Violation

	for _, f := range files {
		before := current.Clone()
		stmts, err := doris.ParseStatements(string(f.Bytes()))
		if err != nil {
			return nil, fmt.Errorf("migration %s: %w", f.Name(), err)
		}
		for i, st := range stmts {
			if !opts.AllowDestructive {
				switch st.Kind {
				case doris.StmtDropTable:
					violations = append(violations, doris.Violation{
						Rule:      doris.DORIS004_NO_COLUMN_DROP,
						TableName: st.TargetTable,
						Message:   fmt.Sprintf("migration %s drops table %q without --allow-destructive", f.Name(), st.TargetTable),
					})
				case doris.StmtAlterTableDropColumn:
					violations = append(violations, doris.Violation{
						Rule:       doris.DORIS004_NO_COLUMN_DROP,
						TableName:  st.TargetTable,
						ColumnName: st.DropColumnName,
						Message:    fmt.Sprintf("migration %s drops column %q without --allow-destructive", f.Name(), st.DropColumnName),
					})
				}
			}
			if st.Kind == doris.StmtAlterTableAddColumn && st.Column != nil {
				if !st.Column.Nullable && !doris.IsNonScalarColumnType(st.Column.Type) && (!st.Column.HasDefault || strings.TrimSpace(st.Column.Default) == "") {
					violations = append(violations, doris.Violation{
						Rule:       doris.DORIS005_NON_NULL_SCALAR_DEFAULT,
						TableName:  st.TargetTable,
						ColumnName: st.Column.Name,
						Message:    fmt.Sprintf("migration %s adds NOT NULL scalar column %q (%s) without an explicit DEFAULT", f.Name(), st.Column.Name, st.Column.Type),
					})
				}
			}
			if st.Kind == doris.StmtAlterTableModifyColumn && st.Column != nil {
				if tbl, ok := current.TableByName(st.TargetTable); ok {
					if origCol, ok := tbl.ColumnByName(st.Column.Name); ok {
						if !doris.IsSafeTypeEvolution(origCol.Type, st.Column.Type) {
							violations = append(violations, doris.Violation{
								Rule:       doris.DORIS006_SAFE_TYPE_EVOLUTION,
								TableName:  st.TargetTable,
								ColumnName: st.Column.Name,
								Message:    fmt.Sprintf("migration %s performs unsafe type evolution on column %q from %q to %q", f.Name(), st.Column.Name, origCol.Type, st.Column.Type),
							})
						}
					}
				}
			}
			if err := doris.ApplyStatement(current, st); err != nil {
				return nil, fmt.Errorf("migration %s statement %d: %w", f.Name(), i+1, err)
			}
		}
		// Check static & evolution rules for this step (using AllowDestructive=true to avoid duplicate DORIS004/005/006 already recorded per-stmt)
		stepViolations := doris.LintSchemaEvolution(before, current, doris.LintOptions{AllowDestructive: true})
		for _, sv := range stepViolations {
			if sv.Rule == doris.DORIS005_NON_NULL_SCALAR_DEFAULT || sv.Rule == doris.DORIS006_SAFE_TYPE_EVOLUTION {
				continue
			}
			violations = append(violations, sv)
		}
	}

	if desired != nil {
		violations = append(violations, doris.LintSchemaEvolution(current, desired, opts)...)
	}

	return violations, nil
}

// HashDir recomputes and writes the Atlas-compatible atlas.sum file in dirPath.
func HashDir(dirPath string) error {
	cleanPath, err := ResolvePath(dirPath)
	if err != nil {
		return err
	}
	localDir, err := migrate.NewLocalDir(cleanPath)
	if err != nil {
		return fmt.Errorf("open migration directory %q: %w", cleanPath, err)
	}
	sum, err := localDir.Checksum()
	if err != nil {
		return fmt.Errorf("compute checksum for %q: %w", cleanPath, err)
	}
	if err := migrate.WriteSumFile(localDir, sum); err != nil {
		return fmt.Errorf("write atlas.sum in %q: %w", cleanPath, err)
	}
	return nil
}

// ValidateDir verifies the atlas.sum checksum integrity in dirPath and replays
// all migrations to validate SQL syntax and Doris/Iceberg lint rules.
func ValidateDir(dirPath string, opts doris.LintOptions) error {
	cleanPath, err := ResolvePath(dirPath)
	if err != nil {
		return err
	}
	sumPath := filepath.Join(cleanPath, migrate.HashFileName)
	if _, err := os.Stat(sumPath); err != nil {
		return fmt.Errorf("missing %s in %q: %w", migrate.HashFileName, cleanPath, err)
	}
	localDir, err := migrate.NewLocalDir(cleanPath)
	if err != nil {
		return fmt.Errorf("open migration directory %q: %w", cleanPath, err)
	}
	if err := migrate.Validate(localDir); err != nil {
		return fmt.Errorf("atlas.sum validation failed in %q: %w", cleanPath, err)
	}
	violations, err := LintDir(cleanPath, nil, opts)
	if err != nil {
		return err
	}
	return doris.ViolationsToError(violations)
}

// DiffAndWrite replays dirPath, computes the migration diff to reach desired,
// validates evolution safety via doris.LintSchemaEvolution, writes a new
// <YYYYMMDDHHMMSS>_<name>.sql file (if there are changes), and updates atlas.sum.
func DiffAndWrite(dirPath, migrationName string, desired *doris.Schema, opts DiffWriteOptions) (string, []doris.Statement, error) {
	cleanPath, err := ResolvePath(dirPath)
	if err != nil {
		return "", nil, err
	}
	if err := os.MkdirAll(cleanPath, 0o755); err != nil {
		return "", nil, fmt.Errorf("create migration directory %q: %w", cleanPath, err)
	}

	current, existingFiles, err := ReplayDir(cleanPath)
	if err != nil {
		return "", nil, err
	}

	stmts, err := doris.DiffWithOptions(current, desired, doris.DiffOptions{
		LintOptions: opts.LintOptions,
	})
	if err != nil {
		return "", nil, err
	}
	if len(stmts) == 0 {
		return "", nil, nil
	}

	localDir, err := migrate.NewLocalDir(cleanPath)
	if err != nil {
		return "", nil, fmt.Errorf("open migration directory %q: %w", cleanPath, err)
	}

	version := opts.Version
	if version == "" {
		version = nextMigrationVersion(existingFiles, time.Now().UTC())
	}
	desc := sanitizeMigrationName(migrationName)
	fileName := fmt.Sprintf("%s_%s.sql", version, desc)

	content := "-- Code generated by atlas-doris. DO NOT EDIT.\n\n" + doris.FormatStatements(stmts)
	if err := localDir.WriteFile(fileName, []byte(content)); err != nil {
		return "", nil, fmt.Errorf("write migration file %q: %w", fileName, err)
	}
	sum, err := localDir.Checksum()
	if err != nil {
		return "", nil, fmt.Errorf("compute checksum in %q: %w", cleanPath, err)
	}
	if err := migrate.WriteSumFile(localDir, sum); err != nil {
		return "", nil, fmt.Errorf("update atlas.sum in %q: %w", cleanPath, err)
	}
	return fileName, stmts, nil
}

func nextMigrationVersion(existing []MigrationFile, now time.Time) string {
	candidate := now.UTC().Format("20060102150405")
	if len(existing) == 0 {
		return candidate
	}
	latest := existing[len(existing)-1].Version
	if candidate > latest {
		return candidate
	}
	if n, err := strconv.ParseInt(latest, 10, 64); err == nil {
		return fmt.Sprintf("%014d", n+1)
	}
	return candidate
}

func sanitizeMigrationName(name string) string {
	s := strings.TrimSpace(name)
	if s == "" {
		return "migration"
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch >= '0' && ch <= '9':
			b.WriteByte(ch)
		case ch >= 'A' && ch <= 'Z':
			b.WriteByte(ch - 'A' + 'a')
		default:
			if b.Len() > 0 && b.String()[b.Len()-1] != '_' {
				b.WriteByte('_')
			}
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "migration"
	}
	return out
}
