# oxide

Oxide is a durable work-stream engine for backend platforms, written in Rust.

It stores opaque work payloads, leases work to pull-based workers, retries abandoned work, preserves optional per-key ordering, and exposes a native TCP protocol for high-throughput producers and consumers.

The server is WAL-backed. The data model includes streams, partitions, protocol versions, claims, fencing tokens, and idempotency keys so the core concepts can evolve without changing the application-facing model.

## Features

- durable named work streams
- opaque payload bytes with typed metadata
- delayed work and priority scheduling
- pull-based claims with lease expiry
- fencing tokens for ack, fail, release, and extend
- lane keys for per-key ordered execution
- idempotency keys and completion records
- segmented WAL with CRC checks and replay
- configurable fsync: `always`, `batch`, or `async`
- Oxide Wire Protocol v1 over TCP
- batch submit, batch claim, and batch ack operations
- stream clear, pause, resume, stats, health, and metrics
- CLI for server operation, stream administration, WAL inspection, and benchmarking

## Quick Start

Run a server:

```bash
cargo run -p oxide-cli -- serve --config examples/oxide.toml
```

Create a stream:

```bash
cargo run -p oxide-cli -- stream-create --name email
```

Submit work:

```bash
cargo run -p oxide-cli -- submit \
  --stream email \
  --payload "send welcome email" \
  --lane-key account-42 \
  --idempotency-key welcome-email-42
```

Claim work:

```bash
cargo run -p oxide-cli -- claim \
  --stream email \
  --group workers \
  --worker worker-1
```

Inspect the stream:

```bash
cargo run -p oxide-cli -- stream-info --name email
```

Install Oxide on a Linux server as a systemd service:

```bash
curl -fsSL https://raw.githubusercontent.com/oxide-queue/oxide/main/scripts/install.sh | sudo bash
```

Install a specific release:

```bash
curl -fsSL https://raw.githubusercontent.com/oxide-queue/oxide/main/scripts/install.sh \
  | sudo OXIDE_VERSION=v0.1.0 bash
```

Useful docs:

- [Architecture](docs/architecture.md)
- [Configuration](docs/configuration.md)
- [Wire Protocol](docs/protocol.md)

## Development

Build:

```bash
cargo build
```

Build release binary:

```bash
cargo build --release -p oxide-cli
```

Run the CLI help:

```bash
cargo run -p oxide-cli -- --help
```

Validate config:

```bash
cargo run -p oxide-cli -- validate-config --config examples/oxide.toml
```

## Tests

Run all Rust tests:

```bash
cargo test
```

Run focused Rust tests:

```bash
cargo test -p oxide-core -p oxide-server
```
