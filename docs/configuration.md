# Configuration

pgedge-loadgen can be configured via a YAML configuration file and/or
command-line flags. CLI flags take precedence over config file values.

!!! note "No Environment Variables"
    This tool deliberately does not support environment variables for
    configuration to keep the configuration model simple and predictable.

## Configuration File Locations

Configuration files are searched in this order:

1. Path specified by `--config` flag
2. `./pgedge-loadgen.yaml` (current directory)
3. `~/.config/pgedge-loadgen/config.yaml` (user config directory)

## Complete Configuration Reference

```yaml
# pgedge-loadgen.yaml

# PostgreSQL connection string (required)
connection: "postgres://user:password@localhost:5432/database"

# Application type (required)
# Options: wholesale, analytics, brokerage, retail,
#          ecommerce, knowledgebase, docmgmt
app: wholesale

# Log level (optional)
# Options: debug, info, warn, error
# Default: info
log_level: info

# Configuration for 'init' command
init:
    # Target database size
    # Supports: B, KB, MB, GB, TB
    # Default: 1GB
    size: 5GB

    # Embedding mode for pgvector applications
    # Options: random, openai, sentence, vectorizer
    # Default: random
    embedding_mode: random

    # Vector dimensions for embeddings
    # Default: 384
    embedding_dimensions: 384

    # URL for pgedge-vectorizer service (if embedding_mode: vectorizer)
    vectorizer_url: "http://localhost:8080"

    # OpenAI API key (if embedding_mode: openai)
    openai_api_key: ""

    # Drop existing schema before initialization
    # Default: false
    drop_existing: false

# Configuration for 'run' command
run:
    # Number of database connections
    # Default: 10
    connections: 50

    # Usage profile
    # Options: local-office, global, store-regional, store-global
    # Default: local-office
    profile: local-office

    # Timezone for profile calculations
    # Default: Local (system timezone)
    timezone: "America/New_York"

    # Statistics reporting interval in seconds
    # Default: 60
    report_interval: 60

    # Duration to run in minutes (0 = indefinite)
    # Default: 0
    duration: 0

    # Connection mode
    # Options: pool, session
    # Default: pool
    connection_mode: pool

    # Session mode settings (only used when connection_mode: session)
    # Minimum session duration in seconds
    # Default: 300 (5 minutes)
    session_min_duration: 300

    # Maximum session duration in seconds
    # Default: 1800 (30 minutes)
    session_max_duration: 1800

    # Minimum think time between queries in milliseconds
    # Default: 1000 (1 second)
    think_time_min: 1000

    # Maximum think time between queries in milliseconds
    # Default: 5000 (5 seconds)
    think_time_max: 5000
```

## Minimal Configuration Examples

### OLTP Workload

```yaml
# wholesale-test.yaml
connection: "postgres://postgres@localhost:5432/wholesale_test"
app: wholesale

init:
    size: 10GB

run:
    connections: 100
    profile: local-office
```

### Analytics Workload

```yaml
# analytics-test.yaml
connection: "postgres://postgres@localhost:5432/analytics_test"
app: analytics

init:
    size: 50GB

run:
    connections: 20
    profile: global
```

### E-commerce with Vectorizer

```yaml
# ecommerce-test.yaml
connection: "postgres://postgres@localhost:5432/ecommerce_test"
app: ecommerce

init:
    size: 5GB
    embedding_mode: vectorizer
    vectorizer_url: "http://localhost:8080"

run:
    connections: 50
    profile: store-regional
    timezone: "America/New_York"
```

### Session Mode (Desktop Application)

```yaml
# desktop-app.yaml
connection: "postgres://postgres@localhost:5432/desktop_test"
app: brokerage

run:
    connections: 30
    connection_mode: session
    session_min_duration: 600    # 10 minutes
    session_max_duration: 3600   # 1 hour
    think_time_min: 2000         # 2 seconds
    think_time_max: 10000        # 10 seconds
```

## Using Configuration Files

### Specify Config File

```bash
# Use specific config file
pgedge-loadgen init --config ./my-config.yaml

# Override config file values with CLI flags
pgedge-loadgen run --config ./my-config.yaml --connections 100
```

### Create Config in Default Location

{% raw %}
```bash
# Create user config directory
mkdir -p ~/.config/pgedge-loadgen

# Create config file
cat > ~/.config/pgedge-loadgen/config.yaml << 'EOF'
connection: "postgres://postgres@localhost:5432/loadgen"
app: wholesale

init:
    size: 1GB

run:
    connections: 10
    profile: local-office
EOF
```
{% endraw %}

## CLI Flag Precedence

CLI flags always override config file values:

```bash
# Config file has app: wholesale and size: 1GB
# CLI overrides both
pgedge-loadgen init \
    --config ./config.yaml \
    --app analytics \
    --size 5GB
```

## Connection Modes

### Pool Mode (Default)

Connections are shared and reused rapidly. Suitable for web applications
where requests are stateless and quick.

```yaml
run:
    connection_mode: pool
    connections: 50
```

### Session Mode

Workers simulate user sessions with think time between queries. Suitable for
desktop applications or long-running user sessions.

```yaml
run:
    connection_mode: session
    connections: 30
    session_min_duration: 300   # Minimum session length
    session_max_duration: 1800  # Maximum session length
    think_time_min: 1000        # Minimum pause between queries
    think_time_max: 5000        # Maximum pause between queries
```

## Data Size Guidelines

| Size | Use Case | Approximate Init Time |
|------|----------|----------------------|
| 100MB | Quick testing | < 1 minute |
| 1GB | Development | 2-5 minutes |
| 10GB | Integration testing | 20-60 minutes |
| 100GB | Performance testing | 3-6 hours |
| 1TB | Production simulation | 1-2 days |

!!! tip "Start Small"
    Start with smaller sizes for development and increase for production
    testing. The schema and queries are the same regardless of size.

## Next Steps

- [CLI Reference](cli-reference.md) - All command-line options
- [Applications](applications.md) - Application-specific settings
- [Usage Profiles](profiles.md) - Profile configuration details
