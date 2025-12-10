# Installation

## Requirements

- **PostgreSQL** 14 or later
- **pgvector extension** (optional, for semantic search applications)
- **Go 1.22+** (only if building from source)

## Pre-built Binaries

Download the latest release for your platform from the
[releases page](https://github.com/pgEdge/pgedge-loadgen/releases):

| Platform | Architecture | Download |
|----------|--------------|----------|
| Linux | amd64 | `pgedge-loadgen-linux-amd64` |
| Linux | arm64 | `pgedge-loadgen-linux-arm64` |
| macOS | amd64 (Intel) | `pgedge-loadgen-darwin-amd64` |
| macOS | arm64 (Apple Silicon) | `pgedge-loadgen-darwin-arm64` |
| Windows | amd64 | `pgedge-loadgen-windows-amd64.exe` |

### Linux/macOS

```bash
# Download (example for Linux amd64)
curl -LO https://github.com/pgEdge/pgedge-loadgen/releases/latest/download/pgedge-loadgen-linux-amd64

# Make executable
chmod +x pgedge-loadgen-linux-amd64

# Move to PATH (optional)
sudo mv pgedge-loadgen-linux-amd64 /usr/local/bin/pgedge-loadgen

# Verify installation
pgedge-loadgen version
```

### Windows

Download `pgedge-loadgen-windows-amd64.exe` and add it to your PATH, or run
it directly from the download location.

## Building from Source

### Prerequisites

- Go 1.22 or later
- Git
- Make (optional, but recommended)

### Build Steps

```bash
# Clone the repository
git clone https://github.com/pgEdge/pgedge-loadgen.git
cd pgedge-loadgen

# Build using Make (recommended)
make build

# Or build directly with Go
go build -o bin/pgedge-loadgen ./cmd/pgedge-loadgen

# Verify the build
./bin/pgedge-loadgen version
```

### Build Options

```bash
# Build with version information
make build VERSION=1.0.0

# Build for all platforms
make build-all

# Run tests
make test

# Run linting
make lint
```

## PostgreSQL Setup

### Basic Setup

Ensure PostgreSQL is running and accessible:

```bash
# Test connection
psql "postgres://user:pass@localhost:5432/mydb" -c "SELECT version();"
```

### pgvector Extension (Optional)

For the pgvector applications (ecommerce, knowledgebase, docmgmt), install
the pgvector extension:

```sql
-- As a superuser
CREATE EXTENSION IF NOT EXISTS vector;
```

If pgvector is not available in your PostgreSQL installation, see the
[pgvector installation guide](https://github.com/pgvector/pgvector#installation).

## Verify Installation

After installation, verify everything is working:

```bash
# Check version
pgedge-loadgen version

# List available applications
pgedge-loadgen apps

# List available profiles
pgedge-loadgen profiles
```

## Next Steps

- [Applications](applications.md) - Choose an application to run
- [Configuration](configuration.md) - Set up a configuration file
- [CLI Reference](cli-reference.md) - Learn about all commands
