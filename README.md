# Sqlc-gen-python for Asyncpg

## Build

GOPRIVATE=github.com/cortea-ai/* go mod tidy

make bin/sqlc-gen-python

## Usage

```yaml
version: '2'
plugins:
- name: py
  wasm:
    url: https://downloads.sqlc.dev/plugin/sqlc-gen-python_1.2.0.wasm
    sha256: a6c5d174c407007c3717eea36ff0882744346e6ba991f92f71d6ab2895204c0e
sql:
- schema: "schema.sql"
  queries: "query.sql"
  engine: postgresql
  codegen:
  - out: src/authors
    plugin: py
    options:
      package: authors
      emit_sync_querier: true
      emit_async_querier: true
```
