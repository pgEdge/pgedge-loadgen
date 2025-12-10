# Changelog

All notable changes to pgEdge Load Generator will be documented in this
file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
