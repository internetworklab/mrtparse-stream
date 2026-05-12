# mrtparse-stream

A Go-based system for parsing [MRT](https://tools.ietf.org/html/rfc6396) BGP routing data dumps and serving them through a streaming HTTP API backed by PostgreSQL.

Built as part of the [CloudPing](https://github.com/internetworklab/cloudping) project, **mrtparse-stream** ingests BGP RIB dumps from sources like [RIPE RIS](https://ris.ripe.net/) and [DN42](https://dn42.dev/), stores them in per-generation immutable PostgreSQL tables, and exposes a resumable, cursored streaming API for real-time querying.

## Architecture

```
MRT RIB Dumps (.gz / .bz2)
        │
        ▼
  ┌─────────────┐    stream parse     ┌──────────────────┐
  │ mrtparse-   │ ──────────────────► │   PostgreSQL     │
  │ ingest      │    batch write      │   generations    │
  └─────────────┘                     │   mrt_entries_N  │
                                      └───────┬──────────┘
                                              │  streaming query
                                      ┌───────▼──────────┐
                                      │  mrtparse-serve   │
                                      │  HTTP API (:8190) │
                                      └──────────────────┘
```

The system manages data as **immutable generations**: each ingestion run creates a new generation with its own table, marks it `ready` when complete, and prunes stale generations beyond a configurable threshold.

## Features

- **Streaming MRT parser** — memory-efficient, channel-based parsing of BGP RIB entries using [gobgp](https://github.com/osrg/gobgp)
- **Rich BGP attribute extraction** — prefix, peer IP/ASN, AS path, standard / large / extended communities, next hop
- **PostgreSQL storage** — per-generation tables with GiST (prefix) and GIN (AS path) indexes for fast lookups
- **Streaming HTTP API** — NDJSON responses with resumable cursors for large result sets
- **Flexible querying** — by origin ASN, AS path segments, neighbor ASN, IP address, or CIDR prefix
- **Authentication** — JWT and Cloudflare Zero Trust access control via [cloudping](https://github.com/internetworklab/cloudping) integration
- **Docker Compose** — ready-to-use stacks for production and testing environments
- **Systemd timer** — automated periodic ingestion (e.g. every 12 hours)

## Commands

### `mrtparse-ingest` — Ingest MRT Data

Parses an MRT data source (local file or remote URL) and writes entries to PostgreSQL or stdout.

```bash
# Ingest from a remote RIPE RIS RIB dump into PostgreSQL
mrtparse-ingest https://data.ris.ripe.net/rrc00/2026.05/bview.20260502.1600.gz \
  --provider ripe-ris-rrc00

# Ingest a local file, output as JSON lines (no database required)
mrtparse-ingest ./bview.20260502.1600.gz \
  --provider my-source \
  --sink json-stdout

# Dry-run (parse only, discard output)
mrtparse-ingest ./master4_latest.mrt.bz2 \
  --provider dn42-master4 \
  --sink null \
  --limit 10000
```

**Options:**

| Flag | Default | Description |
|------|---------|-------------|
| `--provider` | `ripe-ris` | Data source provider identifier |
| `--sink` | `postgres` | Output destination: `postgres`, `json-stdout`, or `null` |
| `--limit` | `0` | Max entries to process (0 = unlimited) |
| `--pg-user-env` | `TEST_PG_USER` | Env var name for PostgreSQL user |
| `--pg-pass-env` | `TEST_PG_PASSWORD` | Env var name for PostgreSQL password |
| `--pg-hostport-env` | `TEST_PG_HOSTPORT` | Env var name for PostgreSQL host:port |
| `--pg-dbname-env` | `TEST_PG_DBNAME` | Env var name for PostgreSQL database name |
| `--mrt-entries-table-prefix` | `mrt_entries` | Table name prefix for per-generation tables |

### `mrtparse-serve` — HTTP API Server

Serves queried MRT/BGP routing data from PostgreSQL over HTTP.

```bash
# Start the server with no authentication
mrtparse-serve --listen-address=:8190

# Start with JWT authentication
mrtparse-serve \
  --listen-address=:8190 \
  --authentication=jwt \
  --jwt-auth-secret-from-env=JWT_SECRET
```

**Options:**

| Flag | Default | Description |
|------|---------|-------------|
| `--listen-address` | `:8190` | Address to listen on |
| `--table-prefix` | `mrt_entries` | Table name prefix for per-generation tables |
| `--authentication` | `none` | Auth method: `none`, `jwt`, or `cloudflare` |
| `--jwt-auth-secret-from-env` | | Env var name containing the JWT secret |
| `--jwt-auth-secret-from-file` | | File path containing the JWT secret |
| `--cloudflare-team-name` | | Cloudflare Zero Trust team name |
| `--cloudflare-aud-env` | | Env var name for Cloudflare AUD tag |

## API Reference

### `GET /providers`

Lists all available data providers and their ingestion status.

```bash
curl http://localhost:8190/providers
```

```json
{
  "data": [
    { "Name": "ripe-ris-rrc00", "Status": "ready", "LastModified": "2026-05-02T16:00:00Z" },
    { "Name": "dn42-master4", "Status": "ready", "LastModified": "2026-05-01T00:00:00Z" }
  ]
}
```

### `GET /mrt_entries/query/{provider}`

Queries MRT entries for a specific provider. Returns a streaming NDJSON response.

**Query parameters (exactly one required):**

| Parameter | Description |
|-----------|-------------|
| `originAsn` | Filter by origin ASN (last AS in path) |
| `asSegments` | Filter by AS path segments (comma-separated, contains-match) |
| `neighborAsn` | Filter by neighbor ASN (first AS in path) |
| `ip` | Find entries whose prefix covers the given IP |
| `cidr` | Find entries whose prefix is a supernet of the given CIDR |

**Pagination parameters:**

| Parameter | Description |
|-----------|-------------|
| `page_size` | Max records per response (enables cursor-based pagination) |
| `cursor_id` | Resume from a previous cursor |
| `cursor_lifespan` | Cursor TTL (default: `30m`) |

**Examples:**

```bash
# Query entries originating from AS13335
curl http://localhost:8190/mrt_entries/query/ripe-ris-rrc00?originAsn=13335

# Query entries containing AS174 in their AS path
curl http://localhost:8190/mrt_entries/query/ripe-ris-rrc00?asSegments=174

# Query entries covering IP 1.1.1.1
curl http://localhost:8190/mrt_entries/query/ripe-ris-rrc00?ip=1.1.1.1

# Query entries for supernet of 192.168.0.0/24
curl http://localhost:8190/mrt_entries/query/ripe-ris-rrc00?cidr=192.168.0.0/24

# Paginated query (resume with cursor_id from response header)
curl http://localhost:8190/mrt_entries/query/ripe-ris-rrc00?originAsn=13335&page_size=100
```

Each line in the NDJSON stream is a `ResumableResponseStreamEvent`:

```json
{
  "data": {
    "Prefix": "1.1.1.0/24",
    "Peer": "10.0.0.8",
    "PeerAS": 64551,
    "ASPath": [64551, 64552, 64553, 64554, 64555, 64556],
    "Communities": ["1:2", "3:4"],
    "LargeCommunities": ["64551:77:88"],
    "ExtendedCommunities": ["1234567890123456789"],
    "NextHop": "172.16.0.8"
  },
  "cursor_id": "a-uuid-for-resumption"
}
```

### `GET /counter`

A resumable counter demo endpoint for testing cursor-based streaming. Accepts `cnt_max_val`, `cnt_tick_intv`, `cursor_lifespan`, and `page_size` query parameters.

## Database Schema

The system uses a PostgreSQL database with the following structure (see [`schema.sql`](schema.sql)):

- **`generations`** — tracks ingestion batches with a `provisioning` → `ready` lifecycle
- **`mrt_entries_{id}`** — per-generation tables containing the parsed BGP routing entries with GiST and GIN indexes

Each `mrt_entries` table stores:

| Column | Type | Description |
|--------|------|-------------|
| `id` | `bigserial` | Primary key |
| `prefix` | `cidr` | BGP prefix |
| `prefix_len` | `smallint` | Prefix length |
| `peer` | `inet` | Peer IP address |
| `next_hop` | `inet` | BGP next hop |
| `peer_as` | `int` | Peer ASN |
| `as_path` | `int[]` | AS path segments |
| `community_high` / `community_low` | `int[]` | Standard BGP communities (split for indexing) |
| `extended_community_high` / `extended_community_low` | `int[]` | Extended communities |
| `large_community_high` / `_mid` / `_low` | `int[]` | Large communities |

## Getting Started

### Prerequisites

- Go 1.25+
- PostgreSQL with `intarray` and `btree_gist` extensions

### Build from Source

```bash
go build -o bin/mrtparse-serve ./cmd/serve
go build -o bin/mrtparse-ingest ./cmd/ingest
```

### Build Docker Image

```bash
./build.sh
```

### Run with Docker Compose

```bash
# Testing environment
cd docker/testing
cp .env.example .env   # then edit .env with your PostgreSQL credentials
docker compose up -d

# Production environment
cd docker/prod
cp .env.example .env   # then edit .env
docker compose up -d
```

### Quick Local Test

1. Start PostgreSQL and create the database:
   ```bash
   psql -c "CREATE DATABASE cloudping;"
   psql -d cloudping -f schema.sql
   ```

2. Set environment variables:
   ```bash
   export TEST_PG_USER=postgres
   export TEST_PG_PASSWORD=postgres
   export TEST_PG_HOSTPORT=localhost:5432
   export TEST_PG_DBNAME=cloudping
   ```

3. Ingest an MRT file:
   ```bash
   go run ./cmd/ingest ./master4_latest.mrt.bz2 --provider dn42-master4
   ```

4. Start the server:
   ```bash
   go run ./cmd/serve
   ```

5. Query the API:
   ```bash
   curl http://localhost:8190/providers
   curl http://localhost:8190/mrt_entries/query/dn42-master4?originAsn=4242420001
   ```

## Automated Ingestion

A systemd timer is provided for periodic ingestion (every 12 hours by default):

```bash
cd jobs
sudo ./install.sh
```

This installs and enables the `ingest-mrt-data.timer` and `ingest-mrt-data.service` units. Customize the service file to point to your ingestion script.

## Project Structure

```
.
├── cmd/
│   ├── serve/          # HTTP API server
│   ├── ingest/         # MRT data ingestion CLI
│   ├── debug_mrt/      # MRT file debug/inspection tool
│   └── test-pgsql/     # PostgreSQL test utilities
├── pkg/
│   ├── db/             # Database layer (read/write, queries, table builder)
│   ├── handler/        # HTTP handlers (query, providers, counter, cursor)
│   ├── lister/         # Provider listing abstractions
│   ├── model/          # MRT entry model and streaming parser
│   ├── task/           # Ingest task orchestrators (DB, JSON, null sinks)
│   └── utils/          # Shared utilities (logging, source streams)
├── docker/
│   ├── prod/           # Production Docker Compose
│   └── testing/        # Testing Docker Compose
├── jobs/               # Systemd timer/service for automated ingestion
├── scripts/            # Helper shell scripts
├── schema.sql          # PostgreSQL schema
├── Dockerfile          # Multi-stage Docker build
└── build.sh            # Docker build script
```

## License

This project is developed by the [InterNetwork Lab](https://github.com/internetworklab).
