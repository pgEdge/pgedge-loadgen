# pgEdge Load Generator

A CLI tool that connects to PostgreSQL databases, creates schemas for
fictional applications, populates them with test data, and runs realistic
load simulations with temporal usage patterns.

## Overview

**pgedge-loadgen** is designed to generate realistic database workloads for
testing purposes. Unlike traditional benchmarks, it simulates actual user
behavior patterns including:

- Time-of-day activity variations
- Weekday/weekend differences
- User session patterns with think time
- Realistic query mixes based on industry-standard benchmarks

!!! note "Testing, Not Benchmarking"
    This tool is designed for generating realistic test workloads, not for
    producing benchmark scores. Use it to validate database configurations,
    test replication setups, or simulate production-like conditions.

## Quick Start

### 1. Install

Download the latest release for your platform from the
[releases page](https://github.com/pgEdge/pgedge-loadgen/releases), or build
from source:

```bash
git clone https://github.com/pgEdge/pgedge-loadgen.git
cd pgedge-loadgen
make build
```

### 2. Initialize a Database

Create the schema and populate it with test data:

```bash
pgedge-loadgen init \
    --app wholesale \
    --size 5GB \
    --connection "postgres://user:pass@localhost:5432/mydb"
```

### 3. Run Load Simulation

Start generating realistic workload:

```bash
pgedge-loadgen run \
    --app wholesale \
    --connections 50 \
    --profile local-office \
    --connection "postgres://user:pass@localhost:5432/mydb"
```

Press `Ctrl+C` to stop the simulation gracefully.

## Available Applications

pgedge-loadgen includes seven fictional applications, each with distinct
schema designs and query patterns:

### TPC-Based Applications

| Application | Based On | Workload Type | Description |
|------------|----------|---------------|-------------|
| `wholesale` | TPC-C | OLTP | Wholesale supplier with orders, inventory |
| `analytics` | TPC-H | OLAP | Analytical queries on sales data |
| `brokerage` | TPC-E | Mixed | Stock trading with customers and trades |
| `retail` | TPC-DS | Decision Support | Multi-channel retail analytics |

### pgvector Applications

These applications use PostgreSQL's pgvector extension for semantic search:

| Application | Description |
|------------|-------------|
| `ecommerce` | E-commerce with semantic product search |
| `knowledgebase` | Knowledge base with article similarity |
| `docmgmt` | Document management with content similarity |

See [Applications](applications.md) for detailed information about each
application's schema and query mix.

## Usage Profiles

Profiles simulate different patterns of database activity:

| Profile | Description |
|---------|-------------|
| `local-office` | Business hours with lunch/break dips |
| `global` | 24/7 operation following the sun |
| `store-regional` | Regional online store with evening peak |
| `store-global` | Global e-commerce with multi-region peaks |

See [Usage Profiles](profiles.md) for detailed timing patterns.

## Features

- **Realistic Data Generation**: Uses gofakeit to generate realistic names,
  addresses, and business data
- **Configurable Scale**: Target specific database sizes from megabytes to
  terabytes
- **Temporal Patterns**: Activity levels vary by time of day and day of week
- **Session Simulation**: Optional session mode simulates user think time
- **Live Statistics**: Real-time throughput and latency reporting
- **Graceful Shutdown**: Clean stop with final summary statistics
- **pgvector Support**: Semantic search applications with vector embeddings

## Next Steps

- [Installation](installation.md) - Detailed installation instructions
- [Applications](applications.md) - Learn about available applications
- [Usage Profiles](profiles.md) - Understand temporal patterns
- [Configuration](configuration.md) - Configure via file or CLI
- [CLI Reference](cli-reference.md) - Complete command reference
