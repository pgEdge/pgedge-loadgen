# Changelog

All notable changes to pgEdge Load Generator will be documented in this
file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-11-10

### Added

- New `--concurrency` flag for the `init` command, and matching
  `init.concurrency` configuration setting (default 4), controlling how
  many database connections are used whilst loading data.

### Changed

- Building from source now requires Go 1.26.10 or later.
- Updated third-party Go modules to their latest releases.

### Fixed

- The `init` command no longer opens a pool of up to 100 database
  connections regardless of its concurrency setting
  ([#2](https://github.com/pgEdge/pgedge-loadgen/issues/2)).
- An extensionless file named `pgedge-loadgen` in the configuration
  search path, such as the binary itself, is no longer mistaken for a
  YAML configuration file, and configuration errors now name the file
  that could not be read
  ([#1](https://github.com/pgEdge/pgedge-loadgen/issues/1)).

### Security

- Release binaries are built with Go 1.26.10, which no longer trusts the
  compromised eMudhra intermediate CAs (CVE-2026-111943).
- Updated `github.com/jackc/pgx/v5`, `golang.org/x/text` and
  `golang.org/x/sys` to versions without known vulnerabilities; the
  newer pgx no longer depends on `golang.org/x/crypto`
- Random strings are now drawn from the seeded Faker source rather than
  the global `math/rand` generator

## [1.0.0-beta1] - 2026-01-05

### Added

- Automatic size maintenance for the wholesale application: old orders are
  periodically deleted to maintain the database at the target size specified
  during initialisation. This prevents unbounded growth during long-running
  tests and simulates real-world data archival practices.
- New `--no-maintain-size` flag for the `run` command to disable automatic
  cleanup if unbounded growth is desired.
- `SizeMaintainer` interface allowing other applications to implement size
  maintenance in future.

## [1.0.0-alpha2] - 2025-12-12

### Changed

- Set `application_name` connection parameter for each worker connection,
  making it easier to identify load generator connections in
  `pg_stat_activity` (e.g., "pgedge-loadgen - client 1")
- Removed unnecessary long-lived connection pool from the run command;
  workers now create dedicated connections only

### Fixed

- Documentation links in README

## [1.0.0-alpha1] - 2025-12-11

### Added

- Initial release of pgedge-loadgen
- Seven fictional applications for load testing:

    - `wholesale` - TPC-C based OLTP workload
    - `analytics` - TPC-H based OLAP workload
    - `brokerage` - TPC-E based mixed workload
    - `retail` - TPC-DS based decision support
    - `ecommerce` - pgvector product catalog with semantic search
    - `knowledgebase` - pgvector knowledge base with article similarity
    - `docmgmt` - pgvector document management with content similarity

- Four usage profiles for temporal patterns:

    - `local-office` - Business hours with breaks
    - `global` - 24/7 global enterprise
    - `store-regional` - Regional e-commerce with evening peak
    - `store-global` - Global retail with multi-region peaks

- Two connection modes:

    - `pool` - Rapid connection reuse for web applications
    - `session` - User session simulation with think time

- Multiple embedding generation options for pgvector apps:

    - Random vectors (default)
    - OpenAI embeddings
    - Sentence Transformers
    - pgedge-vectorizer service

- YAML configuration file support
- Real-time statistics reporting
- Graceful shutdown with summary statistics
- Cross-platform binaries (Linux, macOS, Windows)
