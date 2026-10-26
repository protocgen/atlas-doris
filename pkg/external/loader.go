// Package external implements the Atlas external_schema loader and proto2type
// runner for Apache Doris 4.x and Apache Iceberg schemas.
package external

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/protocgen/atlas-doris/pkg/doris"
	"github.com/protocgen/atlas-doris/pkg/migrate"
)

// TargetFilter controls which generated DDL files are loaded from a directory or proto run.
type TargetFilter string

const (
	TargetAll       TargetFilter = "all"
	TargetDoris     TargetFilter = "doris"
	TargetIceberg   TargetFilter = "iceberg"
	TargetStarRocks TargetFilter = "starrocks"
)

// LoadOptions configures how LoadSchema loads SQL or Protobuf schema definitions.
type LoadOptions struct {
	Target        TargetFilter
	ProtoIncludes []string
	PluginPath    string
}

// ExternalSchemaOptions configures the external-schema command output.
type ExternalSchemaOptions struct {
	SQLDir        string
	SQLFiles      []string
	HCLDir        string
	HCLFiles      []string
	ProtoPath     string
	URL           string
	Target        TargetFilter
	Format        string
	ProtoIncludes []string
	PluginPath    string
}

// LoadSchema loads a doris.Schema from a file:// URL, local file/directory path,
// doris:// or mysql:// live database URL, or proto:// URL.
func LoadSchema(ctx context.Context, targetURL string, opts LoadOptions) (*doris.Schema, error) {
	raw := strings.TrimSpace(targetURL)
	if raw == "" {
		return nil, fmt.Errorf("empty schema URL or path")
	}
	if strings.HasPrefix(raw, "proto://") {
		return loadFromProtoURL(ctx, raw, opts)
	}
	if strings.HasPrefix(raw, "doris://") || strings.HasPrefix(raw, "starrocks://") || strings.HasPrefix(raw, "mysql://") {
		s, err := migrate.InspectLiveSchema(ctx, raw)
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(strings.ToLower(raw), "starrocks://") {
			for _, t := range s.Tables {
				if !t.IsIcebergTable() && t.Dialect == "" {
					t.Dialect = "starrocks"
				}
			}
		}
		return FilterSchemaByTarget(s, opts.Target), nil
	}
	target := opts.Target
	if strings.HasPrefix(raw, "file://") {
		if u, err := url.Parse(raw); err == nil {
			if qTarget := strings.TrimSpace(u.Query().Get("target")); qTarget != "" && target == "" {
				target = TargetFilter(strings.ToLower(qTarget))
			}
		}
	}
	localPath, err := migrate.ResolvePath(raw)
	if err != nil {
		return nil, err
	}
	return LoadFromPath(localPath, target)
}

// FilterSchemaByTarget returns a copy of s retaining only tables that match target.
func FilterSchemaByTarget(s *doris.Schema, target TargetFilter) *doris.Schema {
	if s == nil || target == "" || target == TargetAll {
		return s
	}
	out := s.Clone()
	out.Tables = nil
	for _, t := range s.Tables {
		switch target {
		case TargetDoris:
			if t.IsDorisInternalTable() && !t.IsStarRocksTable() {
				out.Tables = append(out.Tables, t)
			}
		case TargetStarRocks:
			if t.IsStarRocksTable() {
				out.Tables = append(out.Tables, t)
			}
		case TargetIceberg:
			if t.IsIcebergTable() {
				out.Tables = append(out.Tables, t)
			}
		default:
			out.Tables = append(out.Tables, t)
		}
	}
	return out
}

// LoadFromPath loads a doris.Schema from a single .sql or .hcl file, a directory of
// .sql / .hcl files, or an Atlas migration directory (when atlas.sum is present).
func LoadFromPath(localPath string, target TargetFilter) (*doris.Schema, error) {
	info, err := os.Stat(localPath)
	if err != nil {
		return nil, fmt.Errorf("stat schema path %q: %w", localPath, err)
	}
	if !info.IsDir() {
		data, err := os.ReadFile(localPath)
		if err != nil {
			return nil, fmt.Errorf("read schema file %q: %w", localPath, err)
		}
		s, err := doris.ParseSchemaAuto(string(data), localPath)
		if err != nil {
			return nil, fmt.Errorf("parse schema file %q: %w", localPath, err)
		}
		return s, nil
	}

	// If the directory is an Atlas migration chain (contains atlas.sum), replay it.
	if _, err := os.Stat(filepath.Join(localPath, "atlas.sum")); err == nil {
		s, _, err := migrate.ReplayDir(localPath)
		if err != nil {
			return nil, err
		}
		return s, nil
	}

	var schemaFiles []string
	err = filepath.WalkDir(localPath, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		name := strings.ToLower(d.Name())
		if !strings.HasSuffix(name, ".sql") && !strings.HasSuffix(name, ".hcl") {
			return nil
		}
		if !matchesTargetFilter(name, target) {
			return nil
		}
		schemaFiles = append(schemaFiles, p)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan schema directory %q: %w", localPath, err)
	}
	sort.Strings(schemaFiles)
	if len(schemaFiles) == 0 {
		return nil, fmt.Errorf("no matching .sql or .hcl files found in %q (target=%q)", localPath, target)
	}

	merged := doris.NewSchema()
	for _, f := range schemaFiles {
		data, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", f, err)
		}
		sub, err := doris.ParseSchemaAuto(string(data), f)
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", f, err)
		}
		merged.Merge(sub)
	}
	return merged, nil
}

// ExternalSchemaSQL resolves the schema from ExternalSchemaOptions and formats it
// as deterministic canonical SQL suitable for Atlas data "external_schema".
func ExternalSchemaSQL(ctx context.Context, opts ExternalSchemaOptions) (string, error) {
	merged := doris.NewSchema()
	loadedAny := false

	if opts.URL != "" {
		s, err := LoadSchema(ctx, opts.URL, LoadOptions{
			Target:        opts.Target,
			ProtoIncludes: opts.ProtoIncludes,
			PluginPath:    opts.PluginPath,
		})
		if err != nil {
			return "", err
		}
		merged.Merge(s)
		loadedAny = true
	}

	if opts.SQLDir != "" {
		s, err := LoadFromPath(opts.SQLDir, opts.Target)
		if err != nil {
			return "", err
		}
		merged.Merge(s)
		loadedAny = true
	}

	for _, f := range opts.SQLFiles {
		if strings.TrimSpace(f) == "" {
			continue
		}
		s, err := LoadFromPath(f, opts.Target)
		if err != nil {
			return "", err
		}
		merged.Merge(s)
		loadedAny = true
	}

	if opts.HCLDir != "" {
		s, err := LoadFromPath(opts.HCLDir, opts.Target)
		if err != nil {
			return "", err
		}
		merged.Merge(s)
		loadedAny = true
	}

	for _, f := range opts.HCLFiles {
		if strings.TrimSpace(f) == "" {
			continue
		}
		s, err := LoadFromPath(f, opts.Target)
		if err != nil {
			return "", err
		}
		merged.Merge(s)
		loadedAny = true
	}

	if opts.ProtoPath != "" {
		protoURL := opts.ProtoPath
		if !strings.HasPrefix(protoURL, "proto://") {
			protoURL = "proto://" + protoURL
		}
		s, err := loadFromProtoURL(ctx, protoURL, LoadOptions{
			Target:        opts.Target,
			ProtoIncludes: opts.ProtoIncludes,
			PluginPath:    opts.PluginPath,
		})
		if err != nil {
			return "", err
		}
		merged.Merge(s)
		loadedAny = true
	}

	if !loadedAny {
		return "", fmt.Errorf("external-schema requires at least one of --sql-dir, --sql-file, --hcl-dir, --hcl-file, --proto, or --url")
	}

	switch strings.ToLower(strings.TrimSpace(opts.Format)) {
	case "", "sql":
		return doris.FormatSchema(merged), nil
	case "hcl":
		return doris.FormatHCL(merged), nil
	default:
		return "", fmt.Errorf("unsupported format %q (expected sql or hcl)", opts.Format)
	}
}

func loadFromProtoURL(ctx context.Context, rawURL string, opts LoadOptions) (*doris.Schema, error) {
	withoutScheme := strings.TrimPrefix(rawURL, "proto://")
	pathPart := withoutScheme
	var queryValues url.Values
	if qIdx := strings.IndexByte(withoutScheme, '?'); qIdx >= 0 {
		pathPart = withoutScheme[:qIdx]
		parsedQ, err := url.ParseQuery(withoutScheme[qIdx+1:])
		if err != nil {
			return nil, fmt.Errorf("invalid proto:// query parameters in %q: %w", rawURL, err)
		}
		queryValues = parsedQ
	}

	target := opts.Target
	if qTarget := queryValues.Get("target"); qTarget != "" {
		target = TargetFilter(strings.ToLower(qTarget))
	}
	pluginPath := opts.PluginPath
	if qPlugin := queryValues.Get("plugin"); qPlugin != "" {
		pluginPath = qPlugin
	}
	var includes []string
	includes = append(includes, opts.ProtoIncludes...)
	for _, inc := range queryValues["include"] {
		for _, part := range strings.Split(inc, ",") {
			if p := strings.TrimSpace(part); p != "" {
				includes = append(includes, p)
			}
		}
	}

	protoTarget := filepath.Clean(pathPart)
	info, err := os.Stat(protoTarget)
	if err != nil {
		return nil, fmt.Errorf("stat proto target %q: %w", protoTarget, err)
	}

	var protoFiles []string
	var rootInclude string
	if info.IsDir() {
		rootInclude = protoTarget
		err := filepath.WalkDir(protoTarget, func(p string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if !d.IsDir() && strings.HasSuffix(d.Name(), ".proto") {
				protoFiles = append(protoFiles, p)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk proto directory %q: %w", protoTarget, err)
		}
	} else {
		rootInclude = filepath.Dir(protoTarget)
		protoFiles = []string{protoTarget}
	}
	sort.Strings(protoFiles)
	if len(protoFiles) == 0 {
		return nil, fmt.Errorf("no .proto files found in %q", protoTarget)
	}

	resolvedPlugin, cleanupPlugin, err := resolveProto2TypePlugin(ctx, pluginPath)
	if err != nil {
		return nil, err
	}
	defer cleanupPlugin()

	tmpOut, err := os.MkdirTemp("", "atlas-doris-proto2type-*")
	if err != nil {
		return nil, fmt.Errorf("create temp output dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpOut) }()

	dorisMode := strings.ToLower(strings.TrimSpace(queryValues.Get("mode")))
	if dorisMode == "" {
		dorisMode = strings.ToLower(strings.TrimSpace(queryValues.Get("doris_mode")))
	}
	if dorisMode == "" {
		if target == TargetStarRocks {
			dorisMode = "starrocks"
		} else {
			dorisMode = "ddl"
		}
	} else if target == TargetStarRocks {
		switch dorisMode {
		case "ddl":
			dorisMode = "starrocks_ddl"
		case "hcl":
			dorisMode = "starrocks_hcl"
		case "both":
			dorisMode = "starrocks"
		}
	}

	var args []string
	args = append(args, "--plugin=protoc-gen-proto2type="+resolvedPlugin)
	args = append(args, "--proto2type_out="+tmpOut)
	args = append(args, "--proto2type_opt=lang=go,backend=doris,doris_mode="+dorisMode)
	seenInc := make(map[string]bool)
	for _, inc := range append([]string{rootInclude}, includes...) {
		cleanInc := filepath.Clean(inc)
		if !seenInc[cleanInc] {
			seenInc[cleanInc] = true
			args = append(args, "-I", cleanInc)
		}
	}
	args = append(args, protoFiles...)

	cmd := exec.CommandContext(ctx, "protoc", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("protoc + protoc-gen-proto2type failed: %w\n%s", err, strings.TrimSpace(stderr.String()))
	}

	return LoadFromPath(tmpOut, target)
}

func resolveProto2TypePlugin(ctx context.Context, explicitPath string) (string, func(), error) {
	noop := func() {}
	if explicitPath != "" {
		abs, err := filepath.Abs(explicitPath)
		if err != nil {
			return "", noop, err
		}
		if _, err := os.Stat(abs); err != nil {
			return "", noop, fmt.Errorf("protoc-gen-proto2type plugin not found at %q: %w", abs, err)
		}
		return abs, noop, nil
	}
	if p, err := exec.LookPath("protoc-gen-proto2type"); err == nil {
		return p, noop, nil
	}
	// Check PROTO2TYPE_DIR or sibling proto2type repository if present in workspace
	var candidates []string
	if envDir := strings.TrimSpace(os.Getenv("PROTO2TYPE_DIR")); envDir != "" {
		candidates = append(candidates, envDir)
	}
	candidates = append(candidates,
		filepath.Join("..", "proto2type"),
		filepath.Join("..", "..", "proto2type"),
		filepath.Join("..", "..", "..", "proto2type"),
	)
	for _, repoDir := range candidates {
		prebuilt := filepath.Join(repoDir, "protoc-gen-proto2type")
		if info, err := os.Stat(prebuilt); err == nil && !info.IsDir() {
			if abs, err := filepath.Abs(prebuilt); err == nil {
				return abs, noop, nil
			}
		}
		if info, err := os.Stat(filepath.Join(repoDir, "go.mod")); err == nil && !info.IsDir() {
			tmpBinDir, err := os.MkdirTemp("", "protoc-gen-proto2type-bin-*")
			if err != nil {
				continue
			}
			binPath := filepath.Join(tmpBinDir, "protoc-gen-proto2type")
			buildCmd := exec.CommandContext(ctx, "go", "build", "-workfile=off", "-o", binPath, ".")
			buildCmd.Dir = repoDir
			if err := buildCmd.Run(); err == nil {
				return binPath, func() { _ = os.RemoveAll(tmpBinDir) }, nil
			}
			_ = os.RemoveAll(tmpBinDir)
		}
	}
	return "", noop, fmt.Errorf("protoc-gen-proto2type binary not found in PATH (pass --plugin or install protoc-gen-proto2type)")
}

func matchesTargetFilter(lowerFileName string, target TargetFilter) bool {
	switch target {
	case TargetDoris:
		if strings.HasSuffix(lowerFileName, "_starrocks.sql") ||
			strings.HasSuffix(lowerFileName, "_starrocks.hcl") ||
			strings.Contains(lowerFileName, "starrocks") ||
			strings.HasSuffix(lowerFileName, "_iceberg.sql") ||
			strings.HasSuffix(lowerFileName, "_iceberg.hcl") ||
			strings.Contains(lowerFileName, "iceberg") {
			return false
		}
		return strings.HasSuffix(lowerFileName, "_doris.sql") ||
			strings.HasSuffix(lowerFileName, "_doris.hcl") ||
			strings.Contains(lowerFileName, "doris")
	case TargetStarRocks:
		return strings.HasSuffix(lowerFileName, "_starrocks.sql") ||
			strings.HasSuffix(lowerFileName, "_starrocks.hcl") ||
			strings.Contains(lowerFileName, "starrocks")
	case TargetIceberg:
		return strings.HasSuffix(lowerFileName, "_iceberg.sql") ||
			strings.HasSuffix(lowerFileName, "_iceberg.hcl") ||
			strings.Contains(lowerFileName, "iceberg")
	default:
		return true
	}
}
