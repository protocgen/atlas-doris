// Command atlas-doris provides an Atlas-compatible schema migration driver,
// differ, linter, executor, and external_schema plugin for Apache Doris 4.x
// and Apache Iceberg that consumes proto2type-generated *_doris.sql and *_iceberg.sql files.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/protocgen/atlas-doris/pkg/doris"
	"github.com/protocgen/atlas-doris/pkg/external"
	"github.com/protocgen/atlas-doris/pkg/migrate"
)

const versionString = "v0.1.0"

func main() {
	os.Exit(Run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

// Run executes the atlas-doris CLI with the given arguments and output writers.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 1
	}

	switch args[0] {
	case "version", "--version", "-v":
		fmt.Fprintf(stdout, "atlas-doris %s\n", versionString)
		return 0
	case "help", "--help", "-h":
		printUsage(stdout)
		return 0
	case "migrate":
		return runMigrate(ctx, args[1:], stdout, stderr)
	case "schema":
		return runSchema(ctx, args[1:], stdout, stderr)
	case "external-schema":
		return runExternalSchema(ctx, args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		printUsage(stderr)
		return 1
	}
}

func runMigrate(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "migrate subcommand required: diff | validate | hash | lint | status | apply")
		return 1
	}
	sub := args[0]
	rest := args[1:]

	switch sub {
	case "diff":
		return runMigrateDiff(ctx, rest, stdout, stderr)
	case "validate":
		return runMigrateValidate(rest, stdout, stderr)
	case "hash":
		return runMigrateHash(rest, stdout, stderr)
	case "lint":
		return runMigrateLint(ctx, rest, stdout, stderr)
	case "status":
		return runMigrateStatus(ctx, rest, stdout, stderr)
	case "apply":
		return runMigrateApply(ctx, rest, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown migrate subcommand %q\n", sub)
		return 1
	}
}

func runMigrateDiff(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("migrate diff", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var dirURL, toURL, target, pluginPath, versionOverride string
	var allowDestructive bool
	var includes multiStringFlag
	fs.StringVar(&dirURL, "dir", "", "migration directory URL (file://path/to/migrations)")
	fs.StringVar(&toURL, "to", "", "desired schema URL (file://path or proto://path)")
	fs.StringVar(&target, "target", "all", "target filter: all | doris | iceberg")
	fs.StringVar(&pluginPath, "plugin", "", "path to protoc-gen-proto2type binary")
	fs.StringVar(&versionOverride, "version", "", "optional explicit 14-digit migration version")
	fs.BoolVar(&allowDestructive, "allow-drop-column", false, "allow destructive column/table drops")
	fs.BoolVar(&allowDestructive, "allow-destructive", false, "allow destructive column/table drops")
	fs.Var(&includes, "include", "proto include directory (repeatable)")

	reordered := reorderFlagsFirst(args, map[string]bool{
		"--allow-drop-column": true,
		"--allow-destructive": true,
	})
	if err := fs.Parse(reordered); err != nil {
		return 1
	}
	if dirURL == "" || toURL == "" {
		fmt.Fprintln(stderr, "migrate diff requires --dir and --to")
		return 1
	}
	migName := "migration"
	if fs.NArg() > 0 {
		migName = fs.Arg(0)
	}

	desired, err := external.LoadSchema(ctx, toURL, external.LoadOptions{
		Target:        external.TargetFilter(strings.ToLower(target)),
		ProtoIncludes: includes,
		PluginPath:    pluginPath,
	})
	if err != nil {
		fmt.Fprintf(stderr, "load desired schema: %v\n", err)
		return 1
	}

	fileName, stmts, err := migrate.DiffAndWrite(dirURL, migName, desired, migrate.DiffWriteOptions{
		LintOptions: doris.LintOptions{AllowDestructive: allowDestructive},
		Version:     versionOverride,
	})
	if err != nil {
		fmt.Fprintf(stderr, "migrate diff failed: %v\n", err)
		return 1
	}
	if len(stmts) == 0 {
		fmt.Fprintln(stdout, "The migration directory issynced with the desired state, no changes to be made.")
		return 0
	}
	fmt.Fprintf(stdout, "Created migration %s (%d statement(s))\n", fileName, len(stmts))
	return 0
}

func runMigrateValidate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("migrate validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var dirURL string
	var allowDestructive bool
	fs.StringVar(&dirURL, "dir", "", "migration directory URL (file://path/to/migrations)")
	fs.BoolVar(&allowDestructive, "allow-drop-column", false, "allow destructive column/table drops")
	fs.BoolVar(&allowDestructive, "allow-destructive", false, "allow destructive column/table drops")

	reordered := reorderFlagsFirst(args, map[string]bool{
		"--allow-drop-column": true,
		"--allow-destructive": true,
	})
	if err := fs.Parse(reordered); err != nil {
		return 1
	}
	if dirURL == "" {
		fmt.Fprintln(stderr, "migrate validate requires --dir")
		return 1
	}
	if err := migrate.ValidateDir(dirURL, doris.LintOptions{AllowDestructive: allowDestructive}); err != nil {
		fmt.Fprintf(stderr, "migrate validate failed: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "migrations directory is valid")
	return 0
}

func runMigrateHash(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("migrate hash", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var dirURL string
	fs.StringVar(&dirURL, "dir", "", "migration directory URL (file://path/to/migrations)")

	if err := fs.Parse(reorderFlagsFirst(args, nil)); err != nil {
		return 1
	}
	if dirURL == "" {
		fmt.Fprintln(stderr, "migrate hash requires --dir")
		return 1
	}
	if err := migrate.HashDir(dirURL); err != nil {
		fmt.Fprintf(stderr, "migrate hash failed: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "atlas.sum updated")
	return 0
}

func runMigrateLint(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("migrate lint", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var dirURL, toURL, target, pluginPath string
	var allowDestructive bool
	var includes multiStringFlag
	fs.StringVar(&dirURL, "dir", "", "migration directory URL (file://path/to/migrations)")
	fs.StringVar(&toURL, "to", "", "optional desired target schema URL (file:// or proto://)")
	fs.StringVar(&target, "target", "all", "target filter: all | doris | iceberg")
	fs.StringVar(&pluginPath, "plugin", "", "path to protoc-gen-proto2type binary")
	fs.BoolVar(&allowDestructive, "allow-drop-column", false, "allow destructive column/table drops")
	fs.BoolVar(&allowDestructive, "allow-destructive", false, "allow destructive column/table drops")
	fs.Var(&includes, "include", "proto include directory (repeatable)")

	reordered := reorderFlagsFirst(args, map[string]bool{
		"--allow-drop-column": true,
		"--allow-destructive": true,
	})
	if err := fs.Parse(reordered); err != nil {
		return 1
	}
	if dirURL == "" {
		fmt.Fprintln(stderr, "migrate lint requires --dir")
		return 1
	}

	var desired *doris.Schema
	if toURL != "" {
		var err error
		desired, err = external.LoadSchema(ctx, toURL, external.LoadOptions{
			Target:        external.TargetFilter(strings.ToLower(target)),
			ProtoIncludes: includes,
			PluginPath:    pluginPath,
		})
		if err != nil {
			fmt.Fprintf(stderr, "load --to schema: %v\n", err)
			return 1
		}
	}

	violations, err := migrate.LintDir(dirURL, desired, doris.LintOptions{AllowDestructive: allowDestructive})
	if err != nil {
		fmt.Fprintf(stderr, "migrate lint error: %v\n", err)
		return 1
	}
	if len(violations) > 0 {
		for _, v := range violations {
			fmt.Fprintln(stderr, v.String())
		}
		return 1
	}
	fmt.Fprintln(stdout, "migrate lint passed (0 violations)")
	return 0
}

func runMigrateStatus(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("migrate status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var dirURL, dbURL, revisionsSchema string
	fs.StringVar(&dirURL, "dir", "", "migration directory URL (file://path/to/migrations)")
	fs.StringVar(&dbURL, "url", "", "Doris FE URL (doris://user:pass@host:9030/db)")
	fs.StringVar(&revisionsSchema, "revisions-table", "", "optional database/schema or table for atlas_schema_revisions")
	fs.StringVar(&revisionsSchema, "revisions-schema", "", "optional database/schema for atlas_schema_revisions")

	if err := fs.Parse(reorderFlagsFirst(args, nil)); err != nil {
		return 1
	}
	if dirURL == "" || dbURL == "" {
		fmt.Fprintln(stderr, "migrate status requires --dir and --url")
		return 1
	}

	exec, err := migrate.DialExecutor(dbURL, revisionsSchema)
	if err != nil {
		fmt.Fprintf(stderr, "connect to Doris: %v\n", err)
		return 1
	}
	defer exec.Close()

	st, err := exec.Status(ctx, dirURL)
	if err != nil {
		fmt.Fprintf(stderr, "migrate status failed: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "Migration Status: %s\n", st.Status)
	fmt.Fprintf(stdout, "  -- Current Version: %s\n", st.Current)
	if st.Next != "" {
		fmt.Fprintf(stdout, "  -- Next Version:    %s\n", st.Next)
	}
	fmt.Fprintf(stdout, "  -- Executed Files:  %d\n", len(st.Applied))
	fmt.Fprintf(stdout, "  -- Pending Files:   %d\n", len(st.Pending))
	return 0
}

func runMigrateApply(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("migrate apply", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var dirURL, dbURL, baseline, revisionsSchema string
	var dryRun, allowDestructive bool
	var varPairs multiStringFlag
	fs.StringVar(&dirURL, "dir", "", "migration directory URL (file://path/to/migrations)")
	fs.StringVar(&dbURL, "url", "", "Doris FE URL (doris://user:pass@host:9030/db)")
	fs.StringVar(&baseline, "baseline", "", "optional baseline version to mark as already applied")
	fs.StringVar(&revisionsSchema, "revisions-table", "", "optional database/schema or table for atlas_schema_revisions")
	fs.StringVar(&revisionsSchema, "revisions-schema", "", "optional database/schema for atlas_schema_revisions")
	fs.BoolVar(&dryRun, "dry-run", false, "validate and render SQL without executing against Doris")
	fs.BoolVar(&allowDestructive, "allow-drop-column", false, "allow destructive column/table drops")
	fs.BoolVar(&allowDestructive, "allow-destructive", false, "allow destructive column/table drops")
	fs.Var(&varPairs, "var", "template variable KEY=VALUE (repeatable)")

	reordered := reorderFlagsFirst(args, map[string]bool{
		"--dry-run":           true,
		"--allow-drop-column": true,
		"--allow-destructive": true,
	})
	if err := fs.Parse(reordered); err != nil {
		return 1
	}
	if dirURL == "" {
		fmt.Fprintln(stderr, "migrate apply requires --dir")
		return 1
	}
	if dbURL == "" && !dryRun {
		fmt.Fprintln(stderr, "migrate apply requires --url (or --dry-run)")
		return 1
	}

	vars, err := parseKeyValueFlags(varPairs)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --var flag: %v\n", err)
		return 1
	}

	var exec *migrate.Executor
	if dbURL != "" && !dryRun {
		exec, err = migrate.DialExecutor(dbURL, revisionsSchema)
		if err != nil {
			fmt.Fprintf(stderr, "connect to Doris: %v\n", err)
			return 1
		}
		defer exec.Close()
	} else {
		exec = migrate.NewExecutor(nil, revisionsSchema)
	}

	res, err := exec.Apply(ctx, migrate.ApplyOptions{
		DirPath:          dirURL,
		Vars:             vars,
		DryRun:           dryRun,
		BaselineVersion:  baseline,
		RevisionsSchema:  revisionsSchema,
		AllowDestructive: allowDestructive,
	})
	if err != nil {
		fmt.Fprintf(stderr, "migrate apply failed: %v\n", err)
		return 1
	}

	if dryRun {
		for _, sqlStmt := range res.ExecutedSQL {
			fmt.Fprintln(stdout, sqlStmt)
		}
		return 0
	}
	if res.AlreadyUpToDate {
		fmt.Fprintln(stdout, "No new migrations to execute")
		return 0
	}
	fmt.Fprintf(stdout, "Applied %d migration file(s) (%d statement(s))\n", len(res.AppliedFiles), len(res.ExecutedSQL))
	return 0
}

func runSchema(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "schema subcommand required: inspect | diff")
		return 1
	}
	sub := args[0]
	rest := args[1:]

	switch sub {
	case "inspect":
		return runSchemaInspect(ctx, rest, stdout, stderr)
	case "diff":
		return runSchemaDiff(ctx, rest, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown schema subcommand %q\n", sub)
		return 1
	}
}

func runSchemaInspect(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("schema inspect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var targetURL, target, pluginPath string
	var includes multiStringFlag
	fs.StringVar(&targetURL, "url", "", "schema URL (file://path or proto://path)")
	fs.StringVar(&targetURL, "u", "", "schema URL shorthand")
	fs.StringVar(&target, "target", "all", "target filter: all | doris | iceberg")
	fs.StringVar(&pluginPath, "plugin", "", "path to protoc-gen-proto2type binary")
	fs.Var(&includes, "include", "proto include directory (repeatable)")

	if err := fs.Parse(reorderFlagsFirst(args, nil)); err != nil {
		return 1
	}
	if targetURL == "" {
		fmt.Fprintln(stderr, "schema inspect requires --url")
		return 1
	}

	s, err := external.LoadSchema(ctx, targetURL, external.LoadOptions{
		Target:        external.TargetFilter(strings.ToLower(target)),
		ProtoIncludes: includes,
		PluginPath:    pluginPath,
	})
	if err != nil {
		fmt.Fprintf(stderr, "schema inspect failed: %v\n", err)
		return 1
	}
	fmt.Fprint(stdout, doris.FormatSchema(s))
	return 0
}

func runSchemaDiff(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("schema diff", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var fromURL, toURL, target, pluginPath string
	var allowDestructive bool
	var includes multiStringFlag
	fs.StringVar(&fromURL, "from", "", "source schema URL (file://path or proto://path)")
	fs.StringVar(&toURL, "to", "", "target schema URL (file://path or proto://path)")
	fs.StringVar(&target, "target", "all", "target filter: all | doris | iceberg")
	fs.StringVar(&pluginPath, "plugin", "", "path to protoc-gen-proto2type binary")
	fs.BoolVar(&allowDestructive, "allow-drop-column", false, "allow destructive column/table drops")
	fs.BoolVar(&allowDestructive, "allow-destructive", false, "allow destructive column/table drops")
	fs.Var(&includes, "include", "proto include directory (repeatable)")

	reordered := reorderFlagsFirst(args, map[string]bool{
		"--allow-drop-column": true,
		"--allow-destructive": true,
	})
	if err := fs.Parse(reordered); err != nil {
		return 1
	}
	if fromURL == "" || toURL == "" {
		fmt.Fprintln(stderr, "schema diff requires --from and --to")
		return 1
	}

	loadOpts := external.LoadOptions{
		Target:        external.TargetFilter(strings.ToLower(target)),
		ProtoIncludes: includes,
		PluginPath:    pluginPath,
	}
	fromSchema, err := external.LoadSchema(ctx, fromURL, loadOpts)
	if err != nil {
		fmt.Fprintf(stderr, "load --from schema: %v\n", err)
		return 1
	}
	toSchema, err := external.LoadSchema(ctx, toURL, loadOpts)
	if err != nil {
		fmt.Fprintf(stderr, "load --to schema: %v\n", err)
		return 1
	}

	stmts, err := doris.DiffWithOptions(fromSchema, toSchema, doris.DiffOptions{
		LintOptions: doris.LintOptions{AllowDestructive: allowDestructive},
	})
	if err != nil {
		fmt.Fprintf(stderr, "schema diff failed: %v\n", err)
		return 1
	}
	if len(stmts) == 0 {
		fmt.Fprintln(stdout, "-- Schemas are identical.")
		return 0
	}
	fmt.Fprint(stdout, doris.FormatStatements(stmts))
	return 0
}

func runExternalSchema(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("external-schema", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var sqlDir, protoPath, targetURL, target, pluginPath string
	var sqlFiles, includes multiStringFlag
	fs.StringVar(&sqlDir, "sql-dir", "", "directory containing *_doris.sql / *_iceberg.sql files")
	fs.Var(&sqlFiles, "sql-file", "path to SQL file (repeatable)")
	fs.StringVar(&protoPath, "proto", "", "path to .proto file or directory")
	fs.StringVar(&targetURL, "url", "", "schema URL (file:// or proto://)")
	fs.StringVar(&target, "target", "all", "target filter: all | doris | iceberg")
	fs.StringVar(&pluginPath, "plugin", "", "path to protoc-gen-proto2type binary")
	fs.Var(&includes, "include", "proto include directory (repeatable)")

	if err := fs.Parse(reorderFlagsFirst(args, nil)); err != nil {
		return 1
	}

	out, err := external.ExternalSchemaSQL(ctx, external.ExternalSchemaOptions{
		SQLDir:        sqlDir,
		SQLFiles:      sqlFiles,
		ProtoPath:     protoPath,
		URL:           targetURL,
		Target:        external.TargetFilter(strings.ToLower(target)),
		ProtoIncludes: includes,
		PluginPath:    pluginPath,
	})
	if err != nil {
		fmt.Fprintf(stderr, "external-schema failed: %v\n", err)
		return 1
	}
	fmt.Fprint(stdout, out)
	return 0
}

type multiStringFlag []string

func (m *multiStringFlag) String() string {
	return strings.Join(*m, ",")
}

func (m *multiStringFlag) Set(val string) error {
	*m = append(*m, val)
	return nil
}

func parseKeyValueFlags(pairs []string) (map[string]string, error) {
	out := make(map[string]string, len(pairs))
	for _, p := range pairs {
		eq := strings.IndexByte(p, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("expected KEY=VALUE, got %q", p)
		}
		k := strings.TrimSpace(p[:eq])
		v := strings.TrimSpace(p[eq+1:])
		out[k] = v
	}
	return out, nil
}

// reorderFlagsFirst places flag arguments before positional arguments so Go's
// flag.FlagSet parses flags regardless of whether positional args appear first.
func reorderFlagsFirst(args []string, boolFlags map[string]bool) []string {
	var flags []string
	var pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			pos = append(pos, args[i+1:]...)
			break
		}
		if strings.HasPrefix(a, "-") {
			flags = append(flags, a)
			if !strings.Contains(a, "=") && !boolFlags[a] && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
				flags = append(flags, args[i])
			}
		} else {
			pos = append(pos, a)
		}
	}
	return append(flags, pos...)
}

func printUsage(w io.Writer) {
	fmt.Fprint(w, `atlas-doris - Atlas-compatible schema migration driver, differ, linter, and executor for Apache Doris 4.x & Apache Iceberg

Usage:
  atlas-doris migrate diff [name] --dir file://... --to file://...
  atlas-doris migrate validate --dir file://...
  atlas-doris migrate hash --dir file://...
  atlas-doris migrate lint --dir file://... [--to file://...]
  atlas-doris migrate status --dir file://... --url doris://...
  atlas-doris migrate apply --dir file://... --url doris://... [--var K=V] [--dry-run]
  atlas-doris schema inspect --url file://...
  atlas-doris schema diff --from file://... --to file://...
  atlas-doris external-schema [--sql-dir ... | --sql-file ... | --proto ...]
  atlas-doris version
`)
}
