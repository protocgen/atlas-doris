# atlas-doris

[![CI](https://github.com/protocgen/atlas-doris/actions/workflows/ci.yml/badge.svg)](https://github.com/protocgen/atlas-doris/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/protocgen/atlas-doris)](https://goreportcard.com/report/github.com/protocgen/atlas-doris)

An Atlas-compatible schema migration driver, AST parser, canonical SQL & HCL formatter, schema differ, evolution linter, parity verifier, revision executor, and `external_schema` plugin for **Apache Doris 4.x** and **Apache Iceberg v2** that consumes [proto2type](https://github.com/protocgen/proto2type)-generated `*_doris.sql`, `*_iceberg.sql`, `*_doris.hcl`, and `*_iceberg.hcl` contracts.

## Why this exists

Standard [Atlas](https://atlasgo.io) supports PostgreSQL, MySQL, MariaDB, SQLite, ClickHouse, and Snowflake, but its MySQL dialect parser rejects Apache Doris 4.x and Apache Iceberg table clauses (`UNIQUE KEY(...)`, `DUPLICATE KEY(...)`, `DISTRIBUTED BY HASH(...) BUCKETS ...`, `INDEX ... USING INVERTED / NGRAM_BF / ANN`, `DATETIMEV2(6)`, `DECIMALV3(P, S)`, `DATEV2`, `ARRAY<T>`, and `PROPERTIES (...)`).

```
.proto contracts  ──►  proto2type (*_doris.sql|hcl / *_iceberg.sql|hcl)  ──►  atlas-doris (migrations/ + atlas.sum)  ──►  Apache Doris FE
```

**atlas-doris bridges `proto2type` and Atlas:**

- 🔐 **100% Byte-for-Byte `atlas.sum` Compatibility** — uses `ariga.io/atlas/sql/migrate` (`migrate.NewLocalDir`, `localDir.Checksum()`, `migrate.WriteSumFile`, `migrate.Validate`) so migration directories managed by `atlas-doris` are byte-for-byte compatible with `atlas migrate validate` and `atlas migrate hash`
- 🧩 **Native Doris 4.x & Iceberg SQL + Atlas HCL Parser/Formatter** — natively parses and formats `*_doris.sql` and `*_iceberg.sql` DDL as well as declarative Atlas HCL (`*_doris.hcl` and `*_iceberg.hcl`)
- ⚖️ **Doris ↔ Iceberg `schema parity` Verification** — verifies table, column, semantic type (`JSON` / `VARCHAR` ↔ `STRING`, `DATETIMEV2(6)` ↔ `DATETIMEV2`), nullability, and partition column parity across Doris and Iceberg schemas
- ⚡ **Zero-Cluster Offline Replay (`ReplayDir`)** — replays versioned migration files (`00000000000000_baseline.sql`, `<YYYYMMDDHHMMSS>_<name>.sql`) in lexical order to construct the current `*doris.Schema` state in memory, running `migrate diff`, `migrate validate`, and `migrate lint` in milliseconds in CI without spinning up a live Doris FE+BE cluster
- 🛡️ **Compile-Time & Migration-Time Evolution Linter (`DORIS001`–`DORIS007`)** — enforces Apache Doris 4.x Light Schema Change (LSC), Merge-on-Write (`enable_unique_key_merge_on_write`), `ANN` HNSW/IVF vector index, and Apache Iceberg v2 schema evolution invariants
- 🏔️ **Doris FE Revision Executor** — connects to Apache Doris FE over the MySQL wire protocol (`doris://user:pass@host:9030/db`), manages `atlas_schema_revisions` as a Doris `UNIQUE KEY(version)` Merge-on-Write table, safely interpolates `${TENANT_SLUG}` and `${TENANT_BUCKETS}`, and polls `SHOW ALTER TABLE COLUMN` and `SHOW BUILD INDEX` until asynchronous schema changes reach `FINISHED`
- 🔌 **Atlas `external_schema` Provider** — loads desired schemas from `*_doris.sql`, `*_iceberg.sql`, `*_doris.hcl`, `*_iceberg.hcl` files or directly from `.proto` files via `protoc-gen-proto2type`

## Architecture

```mermaid
flowchart LR
    P["Protobuf Contracts (orders.proto)"] -->|protoc-gen-proto2type| DDL["Generated DDL (*_doris / *_iceberg)"]
    DDL -->|atlas-doris schema parity| PARITY["Doris ↔ Iceberg Parity Gate"]
    DDL -->|atlas-doris migrate diff| CHAIN["Versioned Migrations (migrations/ + atlas.sum)"]
    CHAIN -->|CI Gate| LINT["atlas-doris migrate validate & lint"]
    CHAIN -->|CD / GitOps| EXEC["atlas-doris migrate apply (doris://)"]
```

## Install

```bash
go install github.com/protocgen/atlas-doris/cmd/atlas-doris@latest
```

To use as a Go library:

```bash
go get github.com/protocgen/atlas-doris@latest
```

## Lint Rules

| Rule ID | Name | Description |
|---------|------|-------------|
| `DORIS001_IMMUTABLE_KEY` | Immutable Key Columns | Blocks mutating `UNIQUE KEY`, `DUPLICATE KEY`, or `AGGREGATE KEY` columns on an existing table and validates key column existence and scalar types. |
| `DORIS002_IMMUTABLE_SEQUENCE` | Immutable Sequence Column | Blocks mutating `"function_column.sequence_col"` on an existing table, and ensures `sequence_col` is only set on `UNIQUE KEY` tables. |
| `DORIS003_IMMUTABLE_DISTRIBUTION` | Immutable Hash Distribution | Blocks mutating `DISTRIBUTED BY HASH(...)` columns, `colocate_with`, or `PARTITION BY` keys on an existing table and validates distribution column existence and types. |
| `DORIS004_NO_COLUMN_DROP` | Destructive Drop Guard | Blocks dropping existing columns or tables unless `--allow-drop-column` / `--allow-destructive` is explicitly passed. |
| `DORIS005_NON_NULL_SCALAR_DEFAULT` | Scalar `NOT NULL` Default | Requires an explicit `DEFAULT "..."` on newly added `NOT NULL` scalar columns on existing tables. |
| `DORIS006_SAFE_TYPE_EVOLUTION` | Safe LSC & Iceberg Widening | Restricts column type changes to safe widenings (`VARCHAR(m) -> VARCHAR(n)` for `n >= m`, `VARCHAR(m) -> STRING/TEXT`, `DATE -> DATETIME`, `DATETIME(p1) -> DATETIME(p2)` for `p2 >= p1`, `TINYINT -> SMALLINT -> INT -> BIGINT -> LARGEINT`, `FLOAT -> DOUBLE`, `DECIMAL(P1, S) -> DECIMAL(P2, S)` for `P2 >= P1`) and validates `ANN` index invariants (`hnsw`, `ivf`, `ivf_on_disk`, `ivfpq`). |
| `DORIS007_FORBIDDEN_TYPE` | Forbidden Column Types | Rejects `JSONB` and `VARIANT` (including `ARRAY<JSONB>` / `ARRAY<VARIANT>`) across all tables (ADR-0008). |

## CLI Usage

### 1. Generate a Migration from `proto2type` SQL, Atlas HCL, or Protobufs

```bash
# From proto2type-generated *_doris.sql / *_iceberg.sql or *_doris.hcl / *_iceberg.hcl files:
atlas-doris migrate diff add_order_status \
  --dir file://./migrations \
  --to file://./schema/tenant_doris.hcl

# Or directly from .proto files via protoc + protoc-gen-proto2type:
atlas-doris migrate diff add_order_status \
  --dir file://./migrations \
  --to "proto://./proto/acme/orders/v1?include=./proto&target=doris"
```

### 2. Verify Doris ↔ Iceberg Schema Parity

```bash
# Verify all generated *_doris.sql and *_iceberg.sql in a directory:
atlas-doris schema parity --dir ./schema

# Or compare explicit files / proto:// URLs:
atlas-doris schema parity \
  --doris file://./schema/patients_doris.sql \
  --iceberg file://./schema/patients_iceberg.sql
```

### 3. Validate `atlas.sum` & Lint Migration Chain in CI

```bash
atlas-doris migrate validate --dir file://./migrations

atlas-doris migrate lint \
  --dir file://./migrations \
  --to file://./schema/tenant_doris.hcl
```

### 4. Inspect or Diff Schemas (SQL or Atlas HCL)

```bash
# Inspect as canonical SQL (default) or declarative Atlas HCL:
atlas-doris schema inspect --url file://./schema/tenant_doris.hcl --format sql
atlas-doris schema inspect --url file://./migrations --format hcl

atlas-doris schema diff \
  --from file://./migrations \
  --to file://./schema/tenant_doris.hcl
```

### 5. Apply Migrations to Apache Doris FE

```bash
# Dry-run with template variable interpolation:
atlas-doris migrate apply \
  --dir file://./migrations \
  --dry-run \
  --baseline 00000000000000 \
  --var TENANT_SLUG=acme_corp \
  --var TENANT_BUCKETS=16

# Live execution against Apache Doris FE:
atlas-doris migrate apply \
  --dir file://./migrations \
  --url "doris://root:${DORIS_PASSWORD}@doris-fe:9030/internal" \
  --revisions-table internal \
  --baseline 00000000000000 \
  --var TENANT_SLUG=acme_corp \
  --var TENANT_BUCKETS=16
```

### 6. Use as an Atlas `external_schema` Provider in `atlas.hcl`

```hcl
data "external_schema" "doris_schema" {
  program = [
    "atlas-doris",
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

	// 3. Apply pending migrations against Apache Doris FE
	exec, err := migrate.DialExecutor("doris://root@127.0.0.1:9030/internal", "internal")
	if err != nil {
		log.Fatalf("dial doris: %v", err)
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
nix develop -c go build -o atlas-doris ./cmd/atlas-doris
```
