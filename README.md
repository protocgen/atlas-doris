# atlas-doris (`github.com/protocgen/atlas-doris`)

**`atlas-doris`** is a standalone Atlas-compatible schema migration driver, AST parser, canonical formatter, schema differ, evolution linter, revision executor, and `external_schema` plugin for **Apache Doris 4.x** and **Apache Iceberg v2** that directly consumes `proto2type`-generated `*_doris.sql` and `*_iceberg.sql` DDL contracts.

---

## Why `atlas-doris`?

Standard [Atlas](https://atlasgo.io) supports PostgreSQL, MySQL, MariaDB, SQLite, ClickHouse, and Snowflake, but its MySQL parser rejects Apache Doris 4.x / Apache Iceberg table clauses (`UNIQUE KEY(...)`, `DUPLICATE KEY(...)`, `DISTRIBUTED BY HASH(...) BUCKETS ...`, `INDEX ... USING INVERTED / NGRAM_BF / ANN`, `DATETIMEV2(6)`, `DECIMALV3(P, S)`, `DATEV2`, `ARRAY<T>`, `PROPERTIES (...)`).

`atlas-doris` bridges `proto2type` and Atlas:
1. **100% Byte-for-Byte `atlas.sum` Compatibility**: Uses `ariga.io/atlas/sql/migrate` (`migrate.NewLocalDir`, `migrate.HashDir`, `migrate.Validate`) so migration directories managed by `atlas-doris` are byte-for-byte compatible with standard `atlas migrate validate`, `atlas migrate hash`, and GitOps `ConfigMap` digest checks (`schema-chain`).
2. **Zero-Cluster Offline Replay (`ReplayDir`)**: Replays versioned migration files (`00000000000000_tenant.sql`, `<YYYYMMDDHHMMSS>_<name>.sql`) in lexical order to construct the current `*doris.Schema` state in memory, allowing `atlas-doris migrate diff`, `migrate validate`, and `migrate lint` to run in milliseconds in CI without spinning up a live Doris FE+BE cluster.
3. **Compile-Time & Migration-Time Evolution Linter (`DORIS001`–`DORIS007`)**: Enforces Apache Doris 4.x Light Schema Change (LSC), Merge-on-Write (`enable_unique_key_merge_on_write`), and Apache Iceberg v2 schema evolution invariants.
4. **Doris FE Revision Executor**: Connects to Apache Doris FE over the MySQL wire protocol (`doris://user:pass@host:9030/db`), manages `atlas_schema_revisions` as a Doris `UNIQUE KEY(version)` Merge-on-Write table, safely interpolates `${TENANT_SLUG}` and `${TENANT_BUCKETS}`, and polls `SHOW ALTER TABLE COLUMN` when asynchronous index builds or column type widenings are triggered.

---

## End-to-End Architecture (`proto2type` + `atlas-doris`)

```mermaid
flowchart LR
    P["Protobuf Contracts<br/>(patient_profile.proto)"] -->|protoc-gen-proto2type<br/>backend=doris,doris_mode=ddl| DDL["*_doris.sql<br/>*_iceberg.sql"]
    DDL -->|atlas-doris migrate diff| CHAIN["migrations/<br/>00000000000000_tenant.sql<br/>20261024000001_init.sql<br/>atlas.sum"]
    CHAIN -->|CI Gate| LINT["atlas-doris migrate validate<br/>atlas-doris migrate lint"]
    CHAIN -->|In-Cluster GitOps /<br/>provisioning-worker| EXEC["atlas-doris migrate apply<br/>--url doris://fe:9030<br/>--var TENANT_SLUG=...<br/>--var TENANT_BUCKETS=16"]
```

---

## Lint Rules (`pkg/doris`)

| Rule ID | Name | Description |
|---|---|---|
| `DORIS001_IMMUTABLE_KEY` | Immutable Key Columns | Blocks mutating `UNIQUE KEY`, `DUPLICATE KEY`, or `AGGREGATE KEY` columns on an existing table. |
| `DORIS002_IMMUTABLE_SEQUENCE` | Immutable Sequence Column | Blocks mutating `"function_column.sequence_col"` (e.g., `publication_generation`) on an existing table. |
| `DORIS003_IMMUTABLE_DISTRIBUTION` | Immutable Hash Distribution | Blocks mutating `DISTRIBUTED BY HASH(...)` columns on an existing table. |
| `DORIS004_NO_COLUMN_DROP` | Destructive Drop Guard | Blocks dropping existing columns or tables unless `--allow-destructive` is explicitly passed. |
| `DORIS005_NON_NULL_SCALAR_DEFAULT` | Scalar `NOT NULL` Default | Requires an explicit `DEFAULT "..."` on newly added `NOT NULL` scalar columns on existing tables. |
| `DORIS006_SAFE_TYPE_EVOLUTION` | Safe LSC & Iceberg Widening | Restricts column type changes to safe widenings (`VARCHAR(m)->VARCHAR(n)` for `n>m`, `TINYINT->SMALLINT/INT/BIGINT`, `SMALLINT->INT/BIGINT`, `INT->BIGINT`, `FLOAT->DOUBLE`). |
| `DORIS007_FORBIDDEN_TYPE` | Strict Relational Typing | Rejects `JSONB` and `VARIANT` (including `ARRAY<JSONB>` / `ARRAY<VARIANT>`) across all tables (ADR-0008). |

---

## CLI Usage

### 1. Generate a Migration from `proto2type` SQL or Protobufs
```bash
# From proto2type-generated *_doris.sql / *_iceberg.sql files:
atlas-doris migrate diff add_clinical_tags \
  --dir file://./doris/migrations \
  --to file://./iceberg/doris_tenant_profile_ddl.sql

# Or directly from .proto files via protoc + protoc-gen-proto2type:
atlas-doris migrate diff add_clinical_tags \
  --dir file://./doris/migrations \
  --to "proto://./proto/azra/clinical/v1?include=./proto&target=doris"
```

### 2. Validate `atlas.sum` & Lint Migration Chain in CI (`schema-chain`)
```bash
atlas-doris migrate validate --dir file://./doris/migrations
atlas-doris migrate lint \
  --dir file://./doris/migrations \
  --to file://./iceberg/doris_tenant_profile_ddl.sql
```

### 3. Inspect or Diff Schemas
```bash
atlas-doris schema inspect --url file://./iceberg/doris_tenant_profile_ddl.sql
atlas-doris schema diff \
  --from file://./doris/migrations \
  --to file://./iceberg/doris_tenant_profile_ddl.sql
```

### 4. Apply Migrations In-Cluster (`doris-schema` / `provisioning-worker`)
```bash
# Dry-run with tenant variable interpolation:
atlas-doris migrate apply \
  --dir file://./doris/migrations \
  --dry-run \
  --baseline 00000000000000 \
  --var TENANT_SLUG=duke_health \
  --var TENANT_BUCKETS=16

# Live execution against Apache Doris FE:
atlas-doris migrate apply \
  --dir file://./doris/migrations \
  --url "doris://root:${DORIS_PASSWORD}@doris-fe.doris.svc:9030/internal" \
  --revisions-schema internal.atlas_revisions \
  --baseline 00000000000000 \
  --var TENANT_SLUG=duke_health \
  --var TENANT_BUCKETS=16
```

### 5. Use as an Atlas `external_schema` Provider in `atlas.hcl`
```hcl
data "external_schema" "doris_tenant_profile" {
  program = [
    "atlas-doris",
    "external-schema",
    "--sql-file", "./iceberg/doris_tenant_profile_ddl.sql",
  ]
}
```

---

## Development

All commands run inside the Nix dev shell:
```bash
nix develop -c go test -v ./...
nix develop -c golangci-lint run
nix develop -c go build -o atlas-doris ./cmd/atlas-doris
```
