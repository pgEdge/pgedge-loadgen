# Changelog

All notable changes to pgEdge Load Generator will be documented in this
file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
