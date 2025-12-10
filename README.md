# pgEdge Load Generator

[![CI](https://github.com/pgEdge/pgedge-loadgen/actions/workflows/ci.yml/badge.svg)](https://github.com/pgEdge/pgedge-loadgen/actions/workflows/ci.yml)

A CLI tool for generating realistic PostgreSQL workloads. Creates schemas
for fictional applications, populates them with test data, and runs load
simulations with temporal usage patterns.

## Features

- **7 Applications**: TPC-based (wholesale, analytics, brokerage, retail)
  and pgvector-based (ecommerce, knowledgebase, docmgmt)
- **4 Usage Profiles**: Simulate local office, global enterprise, and
  e-commerce traffic patterns
- **Realistic Patterns**: Time-of-day variations, weekend differences,
  session simulation
- **Configurable Scale**: Target specific database sizes (MB to TB)
- **Live Statistics**: Real-time throughput and latency reporting

## Quick Start

```bash
# Initialize database with 5GB of wholesale data
pgedge-loadgen init \
    --app wholesale \
    --size 5GB \
    --connection "postgres://user:pass@localhost:5432/mydb"

# Run load simulation with 50 connections
pgedge-loadgen run \
    --app wholesale \
    --connections 50 \
    --profile local-office \
    --connection "postgres://user:pass@localhost:5432/mydb"
```

## Installation

Download from [releases](https://github.com/pgEdge/pgedge-loadgen/releases)
or build from source:

```bash
git clone https://github.com/pgEdge/pgedge-loadgen.git
cd pgedge-loadgen
make build
```

## Applications

| Application | Based On | Workload | Description |
|-------------|----------|----------|-------------|
| `wholesale` | TPC-C | OLTP | Orders, inventory, payments |
| `analytics` | TPC-H | OLAP | Analytical queries on sales |
| `brokerage` | TPC-E | Mixed | Stock trading simulation |
| `retail` | TPC-DS | Decision Support | Multi-channel retail |
| `ecommerce` | pgvector | Semantic Search | Product catalog with AI search |
| `knowledgebase` | pgvector | Semantic Search | FAQ with article similarity |
| `docmgmt` | pgvector | Semantic Search | Document management |

## Usage Profiles

| Profile | Description |
|---------|-------------|
| `local-office` | Business hours (8AM-6PM) with lunch/break dips |
| `global` | 24/7 operation with rolling global peaks |
| `store-regional` | Regional e-commerce with evening peak |
| `store-global` | Global retail with multi-region peaks |

## Documentation

Full documentation: [https://pgedge.github.io/pgedge-loadgen/](https://pgedge.github.io/pgedge-loadgen/)

- [Installation](https://pgedge.github.io/pgedge-loadgen/installation/)
- [Applications](https://pgedge.github.io/pgedge-loadgen/applications/)
- [Usage Profiles](https://pgedge.github.io/pgedge-loadgen/profiles/)
- [Configuration](https://pgedge.github.io/pgedge-loadgen/configuration/)
- [CLI Reference](https://pgedge.github.io/pgedge-loadgen/cli-reference/)

## License

Copyright 2025 pgEdge, Inc. See [LICENSE](LICENSE) for details.
