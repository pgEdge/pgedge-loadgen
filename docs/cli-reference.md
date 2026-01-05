# CLI Reference

Complete reference for all pgedge-loadgen commands and options.

## Global Options

These options apply to all commands:

| Option | Description | Default |
|--------|-------------|---------|
| `--config` | Path to configuration file | Auto-detected |
| `--connection` | PostgreSQL connection string | From config |
| `--app` | Application type | From config |
| `--log-level` | Log verbosity (debug, info, warn, error) | `info` |

## Commands

### version

Display version information.

```bash
pgedge-loadgen version
```

**Output:**

```
pgedge-loadgen v1.0.0 (abc1234) built 2025-01-15T10:30:00Z
```

---

### apps

List available applications.

```bash
pgedge-loadgen apps
```

**Output:**

```
Available applications:

TPC-Based Applications:
  wholesale     - Wholesale supplier (TPC-C based) - OLTP workload
  analytics     - Analytics warehouse (TPC-H based) - OLAP workload
  brokerage     - Brokerage firm (TPC-E based) - Mixed workload
  retail        - Retail analytics (TPC-DS based) - Decision support

pgvector Applications:
  ecommerce     - E-commerce with semantic product search
  knowledgebase - Knowledge base with semantic article search
  docmgmt       - Document management with similarity search

Use 'pgedge-loadgen apps describe <app>' for details.
```

---

### profiles

List available usage profiles.

```bash
pgedge-loadgen profiles
```

**Output:**

```
Available usage profiles:

  local-office   - Local office hours (8AM-6PM, weekday focus)
  global         - Global enterprise (24/7 with rolling peaks)
  store-regional - Online store, regional (evening peak)
  store-global   - Online store, global (24/7 multi-region)

Profiles affect:
  - Query rate variations throughout the day
  - Weekend vs weekday activity levels
  - Break and lunch time reductions
```

---

### init

Initialize a database with schema and test data.

```bash
pgedge-loadgen init [options]
```

**Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `--size` | Target database size (e.g., 1GB, 500MB) | `1GB` |
| `--embedding-mode` | Embedding generation mode | `random` |
| `--embedding-dimensions` | Vector dimensions | `384` |
| `--vectorizer-url` | URL for vectorizer service | - |
| `--openai-api-key` | OpenAI API key | - |
| `--drop-existing` | Drop existing schema first | `false` |

**Embedding Modes:**

| Mode | Description |
|------|-------------|
| `random` | Generate random vectors (fast, no dependencies) |
| `openai` | Use OpenAI embeddings API |
| `sentence` | Use Sentence Transformers (local) |
| `vectorizer` | Use pgedge-vectorizer service |

**Examples:**

```bash
# Basic initialization
pgedge-loadgen init \
    --app wholesale \
    --size 5GB \
    --connection "postgres://user:pass@localhost/db"

# Initialize with specific embedding mode
pgedge-loadgen init \
    --app ecommerce \
    --size 1GB \
    --embedding-mode vectorizer \
    --vectorizer-url "http://localhost:8080" \
    --connection "postgres://user:pass@localhost/db"

# Reinitialize with different app
pgedge-loadgen init \
    --app analytics \
    --size 10GB \
    --drop-existing \
    --connection "postgres://user:pass@localhost/db"
```

**Database Protection:**

The init command stores metadata about which application was used. If you
try to run a different app against an initialized database, you'll get an
error:

```
Error: database was initialized for 'wholesale' but 'analytics' was specified;
use --drop-existing to reinitialize
```

---

### run

Run load simulation against an initialized database.

```bash
pgedge-loadgen run [options]
```

**Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `--connections` | Number of database connections | `10` |
| `--profile` | Usage profile | `local-office` |
| `--timezone` | Timezone for profile | `Local` |
| `--report-interval` | Stats interval in seconds | `60` |
| `--duration` | Run duration in minutes (0=indefinite) | `0` |
| `--connection-mode` | Connection mode (pool/session) | `pool` |
| `--session-min-duration` | Min session length (seconds) | `300` |
| `--session-max-duration` | Max session length (seconds) | `1800` |
| `--think-time-min` | Min think time (milliseconds) | `1000` |
| `--think-time-max` | Max think time (milliseconds) | `5000` |
| `--no-maintain-size` | Disable automatic cleanup of old data | `false` |

**Examples:**

```bash
# Basic run with pool mode
pgedge-loadgen run \
    --app wholesale \
    --connections 50 \
    --profile local-office \
    --connection "postgres://user:pass@localhost/db"

# Run for specific duration
pgedge-loadgen run \
    --app wholesale \
    --connections 50 \
    --duration 30 \
    --connection "postgres://user:pass@localhost/db"

# Session mode (desktop application simulation)
pgedge-loadgen run \
    --app brokerage \
    --connections 20 \
    --connection-mode session \
    --session-min-duration 600 \
    --session-max-duration 3600 \
    --think-time-min 2000 \
    --think-time-max 10000 \
    --connection "postgres://user:pass@localhost/db"

# Global profile with specific timezone
pgedge-loadgen run \
    --app analytics \
    --connections 100 \
    --profile global \
    --timezone "UTC" \
    --connection "postgres://user:pass@localhost/db"
```

**Output During Run:**

```
2025-01-15T10:30:00Z INF Starting load simulation app=wholesale connections=50 profile=local-office
2025-01-15T10:31:00Z INF Statistics queries=3250 qps=54.2 avg_latency=18.5ms p99_latency=45.2ms errors=0
2025-01-15T10:32:00Z INF Statistics queries=3180 qps=53.0 avg_latency=19.1ms p99_latency=48.7ms errors=0
```

**Graceful Shutdown:**

Press `Ctrl+C` to stop the simulation. A summary is printed:

```
2025-01-15T10:35:00Z INF Load simulation stopped
2025-01-15T10:35:00Z INF Summary total_queries=15750 total_time=5m0s avg_qps=52.5 avg_latency=18.8ms errors=0
```

---

## Connection String Format

The connection string follows the standard PostgreSQL URI format:

```
postgres://[user[:password]@]host[:port]/database[?parameters]
```

**Examples:**

```bash
# Basic
postgres://postgres@localhost/mydb

# With password
postgres://postgres:secret@localhost/mydb

# With port
postgres://postgres:secret@localhost:5432/mydb

# With SSL
postgres://postgres:secret@db.example.com:5432/mydb?sslmode=require

# Multiple hosts (for HA)
postgres://user:pass@host1:5432,host2:5432/mydb?target_session_attrs=read-write
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error |
| 130 | Interrupted (Ctrl+C) |

## Next Steps

- [Configuration](configuration.md) - Configuration file reference
- [Applications](applications.md) - Application details
- [Usage Profiles](profiles.md) - Profile details
