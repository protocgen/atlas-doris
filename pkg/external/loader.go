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
	"regexp"
	"sort"
	"strings"

	"github.com/protocgen/atlas-doris/pkg/doris"
	"github.com/protocgen/atlas-doris/pkg/migrate"
)

var protoOptIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// TargetFilter controls which generated DDL files are loaded from a directory or proto run.
type TargetFilter string

const (
	TargetAll     TargetFilter = "all"
	TargetDoris   TargetFilter = "doris"
	TargetIceberg TargetFilter = "iceberg"
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

// LoadSchemas loads one or more schema sources (file paths, directories, file://,
// doris://, mysql://, or proto:// URLs) and merges them cleanly in order.
func LoadSchemas(ctx context.Context, targetURLs []string, opts LoadOptions) (*doris.Schema, error) {
	var cleaned []string
	for _, u := range targetURLs {
		if s := strings.TrimSpace(u); s != "" {
			cleaned = append(cleaned, s)
		}
	}
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("empty schema URL or path")
	}
	if len(cleaned) == 1 {
		return LoadSchema(ctx, cleaned[0], opts)
	}
	merged := doris.NewSchema()
	for _, raw := range cleaned {
		sub, err := LoadSchema(ctx, raw, opts)
		if err != nil {
			return nil, err
		}
		merged.Merge(sub)
	}
	return merged, nil
}

// LoadFromPaths loads and merges one or more local schema files, directories,
// or Atlas migration directories (with atlas.sum).
func LoadFromPaths(paths []string, target TargetFilter) (*doris.Schema, error) {
	var cleaned []string
	for _, p := range paths {
		if s := strings.TrimSpace(p); s != "" {
			cleaned = append(cleaned, s)
		}
	}
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("empty schema path")
	}
	if len(cleaned) == 1 {
		return LoadFromPath(cleaned[0], target)
	}
	merged := doris.NewSchema()
	for _, p := range cleaned {
		sub, err := LoadFromPath(p, target)
		if err != nil {
			return nil, err
		}
		merged.Merge(sub)
	}
	return merged, nil
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
	if strings.HasPrefix(raw, "doris://") || strings.HasPrefix(raw, "mysql://") {
		s, err := migrate.InspectLiveSchema(ctx, raw)
		if err != nil {
			return nil, err
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

// FilterSchemaByTarget returns a copy of s retaining only databases, tables, views,
// and platform objects that match target.
func FilterSchemaByTarget(s *doris.Schema, target TargetFilter) *doris.Schema {
	if s == nil || target == "" || target == TargetAll {
		return s
	}
	out := doris.NewSchema()

	for _, t := range s.Tables {
		if t == nil {
			continue
		}
		switch target {
		case TargetDoris:
			if t.IsDorisInternalTable() {
				out.Tables = append(out.Tables, t.Clone())
			}
		case TargetIceberg:
			if t.IsIcebergTable() {
				out.Tables = append(out.Tables, t.Clone())
			}
		default:
			out.Tables = append(out.Tables, t.Clone())
		}
	}

	for _, d := range s.Databases {
		if d == nil {
			continue
		}
		isInternalDB := d.Catalog == "" || strings.EqualFold(d.Catalog, "internal")
		switch target {
		case TargetDoris:
			if isInternalDB {
				out.Databases = append(out.Databases, d.Clone())
			}
		case TargetIceberg:
			if !isInternalDB {
				out.Databases = append(out.Databases, d.Clone())
			}
		default:
			out.Databases = append(out.Databases, d.Clone())
		}
	}

	switch target {
	case TargetIceberg:
		out.Views = nil
		out.PlatformObjects = nil
	case TargetDoris:
		for _, v := range s.Views {
			if v == nil {
				continue
			}
			if v.Catalog == "" || strings.EqualFold(v.Catalog, "internal") {
				out.Views = append(out.Views, v.Clone())
			}
		}
		for _, p := range s.PlatformObjects {
			if p != nil {
				out.PlatformObjects = append(out.PlatformObjects, p.Clone())
			}
		}
	default:
		for _, v := range s.Views {
			if v != nil {
				out.Views = append(out.Views, v.Clone())
			}
		}
		for _, p := range s.PlatformObjects {
			if p != nil {
				out.PlatformObjects = append(out.PlatformObjects, p.Clone())
			}
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
		return FilterSchemaByTarget(s, target), nil
	}

	// If the directory is an Atlas migration chain (contains atlas.sum), verify and replay it.
	if _, err := os.Stat(filepath.Join(localPath, "atlas.sum")); err == nil {
		if _, err := migrate.VerifySumFS(os.DirFS(localPath)); err != nil {
			return nil, err
		}
		s, _, err := migrate.ReplayDir(localPath)
		if err != nil {
			return nil, err
		}
		return FilterSchemaByTarget(s, target), nil
	}

	merged := doris.NewSchema()
	loadedSubdir := false
	matchedRootFile := false
	var filteredFiles []string
	var allFiles []string

	err = filepath.WalkDir(localPath, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if p != localPath {
				if _, statErr := os.Stat(filepath.Join(p, "atlas.sum")); statErr == nil {
					if _, err := migrate.VerifySumFS(os.DirFS(p)); err != nil {
						return err
					}
					sub, _, err := migrate.ReplayDir(p)
					if err != nil {
						return err
					}
					merged.Merge(sub)
					loadedSubdir = true
					return fs.SkipDir
				}
			}
			return nil
		}
		name := strings.ToLower(d.Name())
		if !strings.HasSuffix(name, ".sql") && !strings.HasSuffix(name, ".hcl") {
			return nil
		}
		allFiles = append(allFiles, p)
		if matchesTargetFilter(name, target) {
			matchedRootFile = true
			filteredFiles = append(filteredFiles, p)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan schema directory %q: %w", localPath, err)
	}

	schemaFiles := filteredFiles
	if !matchedRootFile && len(allFiles) > 0 {
		// Fall back to parsing all .sql/.hcl files and relying on FilterSchemaByTarget
		// when filenames in an explicit directory do not carry _doris/_iceberg suffixes.
		schemaFiles = allFiles
	}
	sort.Strings(schemaFiles)
	if len(schemaFiles) == 0 && !loadedSubdir {
		return nil, fmt.Errorf("no matching .sql or .hcl files found in %q (target=%q)", localPath, target)
	}

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
	return FilterSchemaByTarget(merged, target), nil
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

	if queryValues.Has("plugin") {
		return nil, fmt.Errorf("proto:// URL query parameter ?plugin= is not allowed (pass LoadOptions.PluginPath or --plugin)")
	}
	target := opts.Target
	if qTarget := strings.TrimSpace(queryValues.Get("target")); qTarget != "" && target == "" {
		target = TargetFilter(strings.ToLower(qTarget))
	}
	pluginPath := opts.PluginPath
	var includes []string
	includes = append(includes, opts.ProtoIncludes...)
	for _, inc := range queryValues["include"] {
		for _, part := range strings.Split(inc, ",") {
			if p := strings.TrimSpace(part); p != "" {
				includes = append(includes, p)
			}
		}
	}

	protoTarget, err := filepath.Abs(filepath.Clean(pathPart))
	if err != nil {
		return nil, fmt.Errorf("resolve proto target %q: %w", pathPart, err)
	}
	if strings.HasPrefix(protoTarget, "-") || strings.HasPrefix(protoTarget, "@") {
		return nil, fmt.Errorf("invalid proto target path %q", protoTarget)
	}
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
				absP, err := filepath.Abs(p)
				if err != nil {
					return err
				}
				if strings.HasPrefix(absP, "-") || strings.HasPrefix(absP, "@") {
					return fmt.Errorf("invalid proto file path %q", absP)
				}
				protoFiles = append(protoFiles, absP)
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
		dorisMode = "ddl"
	}
	switch dorisMode {
	case "ddl", "hcl", "both":
	default:
		return nil, fmt.Errorf("invalid doris_mode %q (expected ddl, hcl, or both)", dorisMode)
	}

	optParts := []string{
		"lang=go",
		"backend=doris",
		"doris_mode=" + dorisMode,
	}
	if vault := strings.TrimSpace(queryValues.Get("doris_storage_vault")); vault != "" {
		if !protoOptIdentRe.MatchString(vault) {
			return nil, fmt.Errorf("invalid doris_storage_vault %q (must match ^[a-zA-Z_][a-zA-Z0-9_]*$)", vault)
		}
		optParts = append(optParts, "doris_storage_vault="+vault)
	}
	if catalog := strings.TrimSpace(queryValues.Get("iceberg_catalog")); catalog != "" {
		if !protoOptIdentRe.MatchString(catalog) {
			return nil, fmt.Errorf("invalid iceberg_catalog %q (must match ^[a-zA-Z_][a-zA-Z0-9_]*$)", catalog)
		}
		optParts = append(optParts, "iceberg_catalog="+catalog)
	}

	var args []string
	args = append(args, "--plugin=protoc-gen-proto2type="+resolvedPlugin)
	args = append(args, "--proto2type_out="+tmpOut)
	args = append(args, "--proto2type_opt="+strings.Join(optParts, ","))
	seenInc := make(map[string]bool)
	for _, inc := range append([]string{rootInclude}, includes...) {
		absInc, err := filepath.Abs(filepath.Clean(inc))
		if err != nil {
			return nil, fmt.Errorf("resolve proto include %q: %w", inc, err)
		}
		if strings.HasPrefix(absInc, "-") || strings.HasPrefix(absInc, "@") {
			return nil, fmt.Errorf("invalid proto include path %q", absInc)
		}
		if !seenInc[absInc] {
			seenInc[absInc] = true
			args = append(args, "-I", absInc)
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
		if strings.HasPrefix(abs, "-") || strings.HasPrefix(abs, "@") {
			return "", noop, fmt.Errorf("invalid plugin path %q", abs)
		}
		if _, err := os.Stat(abs); err != nil {
			return "", noop, fmt.Errorf("protoc-gen-proto2type plugin not found at %q: %w", abs, err)
		}
		return abs, noop, nil
	}
	if p, err := exec.LookPath("protoc-gen-proto2type"); err == nil {
		return p, noop, nil
	}
	if envDir := strings.TrimSpace(os.Getenv("PROTO2TYPE_DIR")); envDir != "" {
		absDir, err := filepath.Abs(envDir)
		if err == nil {
			prebuilt := filepath.Join(absDir, "protoc-gen-proto2type")
			if info, err := os.Stat(prebuilt); err == nil && !info.IsDir() {
				return prebuilt, noop, nil
			}
			if info, err := os.Stat(filepath.Join(absDir, "go.mod")); err == nil && !info.IsDir() {
				tmpBinDir, err := os.MkdirTemp("", "protoc-gen-proto2type-bin-*")
				if err == nil {
					binPath := filepath.Join(tmpBinDir, "protoc-gen-proto2type")
					buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binPath, ".")
					buildCmd.Dir = absDir
					buildCmd.Env = append(os.Environ(), "GOWORK=off")
					if err := buildCmd.Run(); err == nil {
						return binPath, func() { _ = os.RemoveAll(tmpBinDir) }, nil
					}
					_ = os.RemoveAll(tmpBinDir)
				}
			}
		}
	}
	return "", noop, fmt.Errorf("protoc-gen-proto2type binary not found in PATH or PROTO2TYPE_DIR (pass --plugin or install protoc-gen-proto2type)")
}

func matchesTargetFilter(lowerFileName string, target TargetFilter) bool {
	switch target {
	case TargetDoris:
		if strings.HasSuffix(lowerFileName, "_iceberg.sql") ||
			strings.HasSuffix(lowerFileName, "_iceberg.hcl") ||
			strings.Contains(lowerFileName, "iceberg") {
			return false
		}
		return strings.HasSuffix(lowerFileName, "_doris.sql") ||
			strings.HasSuffix(lowerFileName, "_doris.hcl") ||
			strings.Contains(lowerFileName, "doris")
	case TargetIceberg:
		return strings.HasSuffix(lowerFileName, "_iceberg.sql") ||
			strings.HasSuffix(lowerFileName, "_iceberg.hcl") ||
			strings.Contains(lowerFileName, "iceberg")
	default:
		return true
	}
}
