# Xsink

[Українська](README.md) | English

![Xsink: Redis Streams → mapping configuration → PostgreSQL](docs/xsink.webp)

A universal **Redis Streams → Postgres** pipeline with a configurable mapping.
One program, many kinds of data: what to write and where is defined in `mapping.json`,
with no code changes.

---

## Contents

- [What it does](#what-it-does)
- [Reliability guarantees](#reliability-guarantees)
- [Configuration files](#configuration-files)
- [mapping.json — database architecture](#mappingjson--database-architecture)
- [auto_migrate](#auto_migrate)
- [dead_letters](#dead_letters)
- [Secrets](#secrets)
- [HTTP and metrics](#http-and-metrics)
- [Build](#build)
- [Running locally](#running-locally-dev)
- [Running as a systemd service](#running-as-a-systemd-service-linux)
- [CLI flags](#cli-flags)
- [Security / what not to commit](#security--what-not-to-commit)

---

## What it does

1. Reads a SOCKS5 proxy list (a file with one `host:port` or `host:port:user:pass` per line).
2. Opens mTLS connections to one or more Redis sources through a proxy
   (with failover: a dead proxy is marked unhealthy and the dial moves on to another one).
3. In N parallel workers, for EACH stream:
   - `XREADGROUP GROUP <cg> <consumer_id> COUNT <batch> BLOCK <ms> STREAMS <stream> >`;
   - converts the entry fields according to the schema in `mapping.json`;
   - `INSERT INTO <table> ... ON CONFLICT ...` (one multi-row INSERT per batch);
   - on success → `XACK` + `XDEL` (removal from Redis);
   - on an **unrecoverable** error (broken format / constraint violation) → `dead_letters`;
   - on a **transient** failure (network, Postgres down) → leaves the entry in Redis for a retry.
4. Continuous polling with a rate limit (`poll_rate_per_second` per source).
5. Background tasks: `XAUTOCLAIM` reclaim of stuck messages, TTL cleanup of `dead_letters`,
   pool metrics collection.
6. Runs as a systemd service: `Type=notify`, `WatchdogSec`, journal logging, hardening.

**Multi-source:** `config.redis[]` is a list of Redis servers; each stream in the mapping
is bound to a source through `source`. One shared Postgres store.

---

## Reliability guarantees

- **At-least-once.** A message is removed from Redis (`XACK`+`XDEL`) ONLY after it has been
  durably stored: inserted into the table or written to `dead_letters`.
  Duplicates are suppressed by `ON CONFLICT` (see `on_conflict`).
- **Transient vs unrecoverable errors.** A transient failure (network drop, deadlock,
  Postgres unavailable, schema not created yet) does NOT discard data: the batch
  stays in the Redis PEL and is retried. Only genuinely broken data (SQLSTATE classes 22/23)
  and conversion errors end up in `dead_letters`.
- **Circuit breaker.** One per Redis source and one for Postgres: after N consecutive
  failures → Open for `open_timeout`, so a dependency that is already down is not hammered.
- **Backoff + jitter.** INSERT retries grow exponentially with random spread, so workers
  do not retry in lockstep.
- **Reclaim.** `XAUTOCLAIM` picks up stuck messages from the PEL of a dead consumer
  (worker crash) and **reprocesses** them through the same path (INSERT + XACK/XDEL).
- **Proxy failover.** A dial tries up to 3 proxies; a failing one is marked unhealthy for 30s.
- **Graceful shutdown.** SIGTERM → `sd_notify STOPPING` → context cancellation → drain →
  exit. Anything not yet stored stays in Redis (nothing is lost).

---

## Configuration files

| File | Purpose | In git |
|---|---|---|
| `config.yaml` | infrastructure: Redis+mTLS, proxies, Postgres DSN, workers, limits | **no** (.gitignore) |
| `mapping.json` | stream descriptions and field → column mapping (database architecture) | **no** (.gitignore) |
| `config.yaml.example` | template for config.yaml | yes |
| `mapping.json.example` | template for mapping.json | yes |

The real `config.yaml` and `mapping.json` are in `.gitignore`: they contain sensitive
information (passwords, IPs, SNI, table structure). Only `*.example` files live in the repository.

```
cp config.yaml.example config.yaml
cp mapping.json.example mapping.json
# edit for your environment
```

### Main sections of config.yaml

- `log` — `level` (DEBUG|INFO|WARN|ERROR), `sink` (`journal`|`stdout`).
- `proxy` — `list_path`, `strategy` (`random`|`round_robin`|`sticky`), `dial_timeout`.
- `redis[]` — list of sources: `name` (unique), `addr`, `tls.{ca,client_cert,client_key,server_name}`,
  `password`, `db`, `max_retries`, `poll_rate_per_second`, `dial_timeout`.
- `postgres` — `dsn` (through pgbouncer), `max_open_conns`, `connect_timeout`, `auto_migrate`.
- `pipeline` — `workers`, `insert_retries`, `insert_retry_backoff`, `dead_letter_table`,
  `dead_letter_retention`, `redact_dead_letter_payload`.
- `mapping.path` — path to mapping.json.
- `http.addr` — admin HTTP address (`""` disables it).
- `reclaim` — `enabled`, `interval`, `min_idle_time`, `batch_size`.
- `breaker` — `max_consecutive_failures`, `open_timeout`.

---

## mapping.json — database architecture

This is the main file where you describe **what to read from Redis and which table/columns to write it to**.
The root is a `streams` array; each element = one Redis stream → one table.

### Stream fields

| Field | Type | Description |
|---|---|---|
| `source` | string | name of the Redis source (`config.redis[].name`). Optional when there is a single source. |
| `name` | string | Redis stream name (e.g. `queue:orders`). Unique. |
| `consumer_group` | string | consumer group for `XREADGROUP` (created automatically). |
| `batch_size` | int | `COUNT` in XREADGROUP (default 100). |
| `block_ms` | int | `BLOCK` in XREADGROUP, ms (default 5000). |
| `table` | string | Postgres table. Unique. Identifier `[a-z_][a-z0-9_]*`. |
| `id_column` | string | column that receives the Redis stream id (e.g. `1701-0`). Optional. |
| `primary_key` | []string | PK columns, used for `ON CONFLICT`. |
| `on_conflict` | string | `do_nothing` \| `update` \| `error` (default `do_nothing`). |
| `fields` | []Field | redis-field → postgres-column mapping. |

### Field entry

| Key | Description |
|---|---|
| `redis` | field name in `XADD`. |
| `column` | Postgres column name (`[a-z_][a-z0-9_]*`). |
| `type` | SQL type (see below). |
| `not_null` | (optional) `NOT NULL` in the DDL at CREATE TABLE. |
| `encoding` | (optional, `bytea` only) `hex` (default) \| `base64`. |

### Supported types

| type | Postgres | Accepted Redis value (string) |
|---|---|---|
| `text` | TEXT | as is |
| `uuid` | UUID | as is |
| `integer` | INTEGER | integer, int32 |
| `bigint` | BIGINT | integer, int64 |
| `real` | REAL | float32 |
| `double` | DOUBLE PRECISION | float64 |
| `boolean` | BOOLEAN | `true/false/1/0/t/f` |
| `jsonb` | JSONB | valid JSON |
| `timestamptz` | TIMESTAMPTZ | RFC3339 / RFC3339Nano / unix seconds |
| `bytea` | BYTEA | hex (default) or base64 (`encoding`) |

An empty string → SQL `NULL` (when `not_null=false`). Every table gets an automatic
`ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()` column.

### on_conflict

- `do_nothing` — `ON CONFLICT DO NOTHING` (retry-safe, idempotent).
- `update` — `ON CONFLICT (<primary_key>) DO UPDATE SET ...` (upsert). Requires `primary_key`.
- `error` — no `ON CONFLICT`; a duplicate → SQL error → `dead_letters`.

For an example see [`mapping.json.example`](mapping.json.example).

---

## auto_migrate

`postgres.auto_migrate: true` — at startup the program creates missing tables and
adds missing columns: `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ADD COLUMN
IF NOT EXISTS`. It never drops old columns.

> **Important:** `NOT NULL` is applied only at the initial `CREATE TABLE`
> (empty table). When a column is added to an **existing** table it is added
> **nullable** (because `ALTER ADD COLUMN NOT NULL` fails on a non-empty table).
> If a new column of an existing table must be `NOT NULL`, do it as a separate
> controlled manual migration (backfill + `SET NOT NULL`).

`false` — the program simply attempts the INSERT; if the table/column is missing, the entry
stays in Redis and is retried (not lost), and the error goes to the log.

To see the DDL without connecting to the database:
```
make print-ddl
```

---

## dead_letters

Table for entries that could not be written for an **unrecoverable** reason:

```sql
CREATE TABLE dead_letters (
  id          BIGSERIAL PRIMARY KEY,
  stream      TEXT NOT NULL,
  redis_id    TEXT NOT NULL,
  raw_fields  JSONB NOT NULL,
  error       TEXT NOT NULL,
  failed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

- The `error` text does NOT contain raw values (only the field name/type; for Postgres,
  SQLSTATE+Message without Detail).
- `pipeline.dead_letter_retention` — TTL: background deletion of rows older than
  this duration (`0s` keeps them forever).
- `pipeline.redact_dead_letter_payload: true` — `raw_fields` stores only
  `{"_redacted":true,"sha256":...,"size":...}` instead of the values themselves (for
  sensitive data).

---

## Secrets

Prefer NOT to keep the Postgres password in `config.yaml`. An environment variable overrides `postgres.dsn`:

```
export XSINK_POSTGRES_DSN='postgres://xsink_user:REAL_PASS@127.0.0.1:6432/xsink?sslmode=disable&default_query_exec_mode=simple_protocol'
```

Under systemd, use `EnvironmentFile=-/etc/xsink/xsink.env` (chmod 600):
```
XSINK_POSTGRES_DSN=postgres://xsink_user:REAL_PASS@127.0.0.1:6432/xsink?sslmode=disable&default_query_exec_mode=simple_protocol
```

---

## HTTP and metrics

Admin HTTP (`http.addr`, default `127.0.0.1:9090`):

- `GET /healthz` — the process is alive (independent of dependencies).
- `GET /readyz` — 200 only when the pipeline is ready + ping to every Redis + Postgres succeeds.
- `GET /metrics` — Prometheus.

Key metrics:

| Metric | Meaning |
|---|---|
| `xsink_messages_read_total` | read via XREADGROUP |
| `xsink_messages_inserted_total` | inserted successfully |
| `xsink_messages_dead_letter_total` | sent to dead_letters (reason=convert\|insert) |
| `xsink_messages_deferred_total` | left in Redis for retry (reason=transient\|pg_open) |
| `xsink_messages_reclaimed_total` | picked up by XAUTOCLAIM |
| `xsink_dead_letters_purged_total` | deleted by TTL |
| `xsink_pending_count` | PEL length (XPENDING) |
| `xsink_stream_length` | XLEN (backlog) |
| `xsink_breaker_state` | 0=Closed 1=HalfOpen 2=Open |
| `xsink_insert_duration_seconds` / `xsink_ack_duration_seconds` | latency |
| `xsink_postgres_pool_in_use` / `_idle` | pgx pool |

Alerts are worth setting on sharp growth of `dead_letter`, `deferred`, and `stream_length`.

---

## Build

Dependencies: Go 1.25+. No CGO.

```
make build         # bin/xsink (for the current OS)
make build-linux   # bin/linux/xsink (cross-compilation for production deployment)
make print-ddl     # print the DDL from the mapping without connecting to the database
make test          # go test ./...
make tidy          # go mod tidy
make clean         # remove bin/
```

---

## Running locally (dev)

```
cp config.yaml.example config.yaml     # fill in Redis addr, mTLS paths, DSN
cp mapping.json.example mapping.json    # describe your streams/tables
# create keys/ with CA+client cert/key and proxies.txt with SOCKS5 proxies

./bin/xsink --config config.yaml
```

For local development `log.sink: stdout` is convenient.

---

## Running as a systemd service (Linux)

```
sudo cp deploy/systemd/xsink.service /etc/systemd/system/
sudo cp bin/linux/xsink /usr/local/bin/xsink
sudo mkdir -p /etc/xsink/keys
sudo cp config.yaml mapping.json /etc/xsink/
sudo cp keys/*.crt keys/*.key /etc/xsink/keys/
sudo touch /etc/xsink/proxies.txt   # fill in SOCKS5 proxies
# (optional) DSN secret:
echo 'XSINK_POSTGRES_DSN=postgres://...' | sudo tee /etc/xsink/xsink.env
sudo chmod 600 /etc/xsink/xsink.env
sudo useradd --system --no-create-home --shell /usr/sbin/nologin xsink
sudo chown -R xsink:xsink /etc/xsink
sudo systemctl daemon-reload
sudo systemctl enable --now xsink.service
sudo journalctl -u xsink -f
```

The unit delegates to systemd: restarts (`Restart=on-failure`), liveness
(`Type=notify` + `WatchdogSec=60s`), restart-storm protection
(`StartLimitIntervalSec=300`/`StartLimitBurst=10`), a drain window
(`TimeoutStopSec=120s`), the descriptor limit (`LimitNOFILE=65536`), journal logging,
start ordering, a service user, and full sandboxing/hardening.

**Memory/CPU limits** (`MemoryMax`/`CPUQuota`) are deliberately NOT set in the universal unit
(they depend on the specific hardware). If needed, use a per-host drop-in:
```
# /etc/systemd/system/xsink.service.d/limits.conf
[Service]
MemoryMax=2G
CPUQuota=200%
```

---

## CLI flags

- `--config <path>` — YAML config (default `config.yaml`).
- `--print-ddl` — print `CREATE TABLE`/`ALTER TABLE` from the mapping and exit.
- `--version` — version.

---

## Security / what not to commit

`.gitignore` excludes from git: `config.yaml`, `mapping.json` (table structure),
`keys/` and `*.crt`/`*.key`/`*.pem`, `proxies.txt`, `*.env`, `bin/`, `data/`,
backups (`*.tar.gz`). Only the `*.example` templates stay in the repository.

Additionally:
- mTLS to Redis (client cert + CA verification + SNI).
- SQL identifiers are validated (`[a-z_][a-z0-9_]*`); values go through
  pgx parameters (injection protection).
- The database password comes from `XSINK_POSTGRES_DSN`, not from a file.
- Error texts and `dead_letters.error` contain no raw values; optional payload redaction.
