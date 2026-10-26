# atlas-doris (`atlas-olap` / `atlas-doris` / `atlas-starrocks`)

[![CI](https://github.com/protocgen/atlas-doris/actions/workflows/ci.yml/badge.svg)](https://github.com/protocgen/atlas-doris/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/protocgen/atlas-doris)](https://goreportcard.com/report/github.com/protocgen/atlas-doris)

An Atlas-compatible schema migration driver, AST parser, canonical SQL & HCL formatter, schema differ, evolution linter, parity verifier, revision executor, and `external_schema` plugin for **Apache Doris 4.x**, **StarRocks 4.x**, and **Apache Iceberg v2** that consumes [proto2type](https://github.com/protocgen/proto2type)-generated `*_doris.sql`, `*_starrocks.sql`, `*_iceberg.sql`, `*_doris.hcl`, `*_starrocks.hcl`, and `*_iceberg.hcl` contracts.

## Why this exists

Standard [Atlas](https://atlasgo.io) supports PostgreSQL, MySQL, MariaDB, SQLite, ClickHouse, and Snowflake, but its MySQL dialect parser rejects Apache Doris 4.x, StarRocks 4.x, and Apache Iceberg table clauses (`UNIQUE KEY(...)`, `PRIMARY KEY(...)`, `DUPLICATE KEY(...)`, `DISTRIBUTED BY HASH(...) BUCKETS ...`, `INDEX ... USING INVERTED / NGRAM_BF / ANN / GIN / BITMAP / VECTOR`, `DATETIMEV2(6)`, `DECIMALV3(P, S)`, `DATEV2`, `ARRAY<T>`, and `PROPERTIES (...)`).

```
.proto contracts  ──►  proto2type (*_doris / *_starrocks / *_iceberg)  ──►  atlas-olap (migrations/ + atlas.sum)  ──►  Doris FE / StarRocks FE
```

**Three CLI binaries from a single shared architecture:**

| Binary | Default `--target` | Purpose |
|--------|--------------------|---------|
| `atlas-olap` | `all` | Unified OLAP schema CLI for Apache Doris 4.x, StarRocks 4.x, and Apache Iceberg (`schema parity`, `migrate`, `schema`, `external-schema`) |
| `atlas-doris` | `all` | Apache Doris 4.x & Apache Iceberg CLI (100% backwards-compatible) |
| `atlas-starrocks` | `starrocks` | StarRocks 4.x specialized CLI defaulting `--target` to `starrocks` |

**Key capabilities:**

- 🔐 **100% Byte-for-Byte `atlas.sum` Compatibility** — uses `ariga.io/atlas/sql/migrate` (`migrate.NewLocalDir`, `localDir.Checksum()`, `migrate.WriteSumFile`, `migrate.Validate`) so migration directories managed by `atlas-olap`, `atlas-doris`, or `atlas-starrocks` are byte-for-byte compatible with `atlas migrate validate` and `atlas migrate hash`
- 🧩 **Native Doris 4.x, StarRocks 4.x & Iceberg SQL + Atlas HCL Parser/Formatter** — natively parses and formats `*_doris.sql`, `*_starrocks.sql`, `*_iceberg.sql` DDL and declarative Atlas HCL (`*_doris.hcl`, `*_starrocks.hcl`, `*_iceberg.hcl`), including StarRocks `PRIMARY KEY(...)`, `enable_persistent_index`, `storage_volume`, `datacache.enable`, `USING GIN(...)`, `USING BITMAP`, `USING VECTOR(...)`, `CREATE STORAGE VOLUME`, and `CREATE EXTERNAL CATALOG`
- ⚖️ **Cross-Engine `schema parity` Verification** — verifies table, column, semantic type (`TEXT` ↔ `STRING`, `DATETIME(6)` ↔ `DATETIMEV2(6)`), nullability, key model (`UNIQUE KEY` ↔ `PRIMARY KEY`), key column, and `function_column.sequence_col` parity across Doris, StarRocks, and Iceberg schemas
- ⚡ **Zero-Cluster Offline Replay (`ReplayDir`)** — replays versioned migration files (`00000000000000_baseline.sql`, `<YYYYMMDDHHMMSS>_<name>.sql`) in lexical order to construct the current `*doris.Schema` state in memory, running `migrate diff`, `migrate validate`, and `migrate lint` in milliseconds in CI without spinning up a live FE+BE/CN cluster
- 🛡️ **Compile-Time & Migration-Time Evolution Linter (`DORIS001`–`DORIS007`)** — enforces Apache Doris 4.x & StarRocks 4.x Fast/Light Schema Change, Merge-on-Write (`enable_unique_key_merge_on_write`), Persistent Index (`enable_persistent_index`), `ANN` / `VECTOR` HNSW/IVFPQ index, and Apache Iceberg v2 schema evolution invariants
- 🏔️ **Doris FE & StarRocks FE Revision Executor** — connects to Apache Doris FE (`doris://user:pass@host:9030/db`) or StarRocks FE (`starrocks://user:pass@host:9030/db`), manages `atlas_schema_revisions` as a Doris `UNIQUE KEY(version)` Merge-on-Write table or StarRocks `PRIMARY KEY(version)` Persistent Index table, safely interpolates `${TENANT_SLUG}` and `${TENANT_BUCKETS}`, and polls `SHOW ALTER TABLE COLUMN` and `SHOW BUILD INDEX` until asynchronous schema changes reach `FINISHED`
- 🔌 **Atlas `external_schema` Provider** — loads desired schemas from `*_doris.sql`, `*_starrocks.sql`, `*_iceberg.sql`, `*_doris.hcl`, `*_starrocks.hcl`, `*_iceberg.hcl` files or directly from `.proto` files via `protoc-gen-proto2type`

## Architecture

```mermaid
flowchart LR
    P["Protobuf Contracts (orders.proto)"] -->|protoc-gen-proto2type| DDL["Generated DDL (*_doris / *_starrocks / *_iceberg)"]
    DDL -->|atlas-olap schema parity| PARITY["Cross-Engine Parity Gate"]
    DDL -->|atlas-olap migrate diff| CHAIN["Versioned Migrations (migrations/ + atlas.sum)"]
    CHAIN -->|CI Gate| LINT["atlas-olap migrate validate & lint"]
    CHAIN -->|CD / GitOps| EXEC["atlas-olap migrate apply (doris:// or starrocks://)"]
```

## Install

```bash
go install github.com/protocgen/atlas-doris/cmd/atlas-olap@latest
go install github.com/protocgen/atlas-doris/cmd/atlas-doris@latest
go install github.com/protocgen/atlas-doris/cmd/atlas-starrocks@latest
```

To use as a Go library:

```bash
go get github.com/protocgen/atlas-doris@latest
```

## Lint Rules

| Rule ID | Name | Description |
|---------|------|-------------|
| `DORIS001_IMMUTABLE_KEY` | Immutable Key Columns | Blocks mutating `UNIQUE KEY`, `DUPLICATE KEY`, `AGGREGATE KEY`, or `PRIMARY KEY` columns (or disabling `enable_persistent_index` on `PRIMARY KEY` tables) on an existing table. |
| `DORIS002_IMMUTABLE_SEQUENCE` | Immutable Sequence Column | Blocks mutating `"function_column.sequence_col"` on an existing table, and ensures `sequence_col` is only set on `UNIQUE KEY` or `PRIMARY KEY` tables. |
| `DORIS003_IMMUTABLE_DISTRIBUTION` | Immutable Hash Distribution | Blocks mutating `DISTRIBUTED BY HASH(...)` columns, `colocate_with`, or `PARTITION BY` keys on an existing table. |
| `DORIS004_NO_COLUMN_DROP` | Destructive Drop Guard | Blocks dropping existing columns or tables unless `--allow-drop-column` / `--allow-destructive` is explicitly passed. |
| `DORIS005_NON_NULL_SCALAR_DEFAULT` | Scalar `NOT NULL` Default | Requires an explicit `DEFAULT "..."` on newly added `NOT NULL` scalar columns on existing tables. |
| `DORIS006_SAFE_TYPE_EVOLUTION` | Safe LSC & Iceberg Widening | Restricts column type changes to safe widenings (`VARCHAR(m) -> VARCHAR(n)` for `n >= m`, `VARCHAR(m) -> STRING/TEXT`, `DATE -> DATETIME`, `TINYINT -> SMALLINT -> INT -> BIGINT -> LARGEINT`, `FLOAT -> DOUBLE`, `DECIMAL(P1, S) -> DECIMAL(P2, S)` for `P2 >= P1`) and validates `ANN` / `VECTOR` index invariants (`hnsw`, `ivf`, `ivf_on_disk`, `ivfpq`). |
| `DORIS007_FORBIDDEN_TYPE` | Forbidden Column Types | Rejects `JSONB` and `VARIANT` (including `ARRAY<JSONB>` / `ARRAY<VARIANT>`) across all tables (ADR-0008). |

## CLI Usage

### 1. Generate a Migration from `proto2type` SQL, Atlas HCL, or Protobufs

```bash
# Doris or StarRocks SQL / HCL files:
atlas-doris migrate diff add_order_status \
  --dir file://./migrations \
  --to file://./schema/tenant_doris.hcl

atlas-starrocks migrate diff add_order_status \
  --dir file://./migrations_starrocks \
  --to file://./schema/tenant_starrocks.hcl

# Or directly from .proto files via protoc + protoc-gen-proto2type:
atlas-olap migrate diff add_order_status \
  --dir file://./migrations_starrocks \
  --to "proto://./proto/acme/orders/v1?include=./proto&target=starrocks"
```

### 2. Verify Cross-Engine Schema Parity (`Doris` ↔ `StarRocks` ↔ `Iceberg`)

```bash
# Verify all generated *_doris.sql, *_starrocks.sql, and *_iceberg.sql in a directory:
atlas-olap schema parity --dir ./schema

# Or compare explicit files / proto:// URLs:
atlas-olap schema parity \
  --doris file://./schema/patients_doris.sql \
  --starrocks file://./schema/patients_starrocks.sql \
  --iceberg file://./schema/patients_iceberg.sql
```

### 3. Validate `atlas.sum` & Lint Migration Chain in CI

```bash
atlas-olap migrate validate --dir file://./migrations

atlas-olap migrate lint \
  --dir file://./migrations \
  --to file://./schema/tenant_doris.hcl
```

### 4. Inspect or Diff Schemas (SQL or Atlas HCL)

```bash
# Inspect as canonical SQL (default) or declarative Atlas HCL:
atlas-olap schema inspect --url file://./schema/tenant_starrocks.hcl --format sql
atlas-starrocks schema inspect --url file://./schema --format hcl

atlas-olap schema diff \
  --from file://./migrations \
  --to file://./schema/tenant_doris.hcl
```

### 5. Apply Migrations to Apache Doris FE or StarRocks FE

```bash
# Dry-run with template variable interpolation:
atlas-olap migrate apply \
  --dir file://./migrations \
  --dry-run \
  --baseline 00000000000000 \
  --var TENANT_SLUG=acme_corp \
  --var TENANT_BUCKETS=16

# Live execution against Apache Doris FE:
atlas-doris migrate apply \
  --dir file://./migrations \
  --url "doris://root:${DORIS_PASSWORD}@doris-fe:9030/internal" \
  --revisions-table internal.atlas_revisions \
  --baseline 00000000000000 \
  --var TENANT_SLUG=acme_corp \
  --var TENANT_BUCKETS=16

# Live execution against StarRocks FE:
atlas-starrocks migrate apply \
  --dir file://./migrations_starrocks \
  --url "starrocks://root:${STARROCKS_PASSWORD}@starrocks-fe:9030/default_catalog" \
  --revisions-table default_catalog.atlas_revisions \
  --baseline 00000000000000 \
  --var TENANT_SLUG=acme_corp \
  --var TENANT_BUCKETS=16
```

### 6. Use as an Atlas `external_schema` Provider in `atlas.hcl`

```hcl
data "external_schema" "starrocks_schema" {
  program = [
    "atlas-starrocks",
    "external-schema",
    "--hcl-dir", "./schema",
  ]
}
```

## Go Library Usage

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/protocgen/atlas-doris/pkg/doris"
	"github.com/protocgen/atlas-doris/pkg/migrate"
)

func main() {
	// 1. Validate atlas.sum integrity and lint the migration directory
	if err := migrate.ValidateDir("file://./migrations", doris.LintOptions{}); err != nil {
		log.Fatalf("invalid migrations: %v", err)
	}

	// 2. Replay the migration chain offline into an in-memory AST
	currentSchema, _, err := migrate.ReplayDir("file://./migrations")
	if err != nil {
		log.Fatalf("replay migrations: %v", err)
	}
	fmt.Print(doris.FormatSchema(currentSchema))

	// 3. Apply pending migrations against Apache Doris FE or StarRocks FE (doris:// or starrocks://)
	exec, err := migrate.DialExecutor("starrocks://root@127.0.0.1:9030/default_catalog", "default_catalog.atlas_revisions")
	if err != nil {
		log.Fatalf("dial starrocks: %v", err)
	}
	defer exec.Close()

	res, err := exec.Apply(context.Background(), migrate.ApplyOptions{
		DirPath: "file://./migrations",
		Vars: map[string]string{
			"TENANT_SLUG":    "acme_corp",
			"TENANT_BUCKETS": "16",
		},
	})
	if err != nil {
		log.Fatalf("apply migrations: %v", err)
	}
	fmt.Printf("Applied %d migration file(s)\n", len(res.AppliedFiles))
}
```

## Development

All commands run inside the Nix dev shell:

```bash
nix develop -c go test -race -cover ./...
nix develop -c golangci-lint run ./...
nix develop -c go build -o atlas-olap ./cmd/atlas-olap
nix develop -c go build -o atlas-doris ./cmd/atlas-doris
nix develop -c go build -o atlas-starrocks ./cmd/atlas-starrocks
```
