# Oxide Architecture

Oxide separates the system into five layers:

1. Core engine: deterministic stream, scheduler, claim, retry, and dedupe state transitions.
2. WAL: authoritative append-only mutation history with replay.
3. Wire protocol: versioned length-prefixed TCP frames.
4. Runtime: WAL-backed command execution and recovery.
5. CLI: operational entrypoint and protocol client.

## Work Model

Work is submitted into named streams. Each work item has opaque payload bytes plus metadata:

- stream name
- priority
- schedule time
- optional deadline
- time-to-run lease duration
- optional idempotency key
- optional lane key
- retry policy
- headers

Lane keys serialize related work without forcing strict total ordering for the whole stream. A hot customer, account, or aggregate can be protected from concurrent mutation while unrelated work remains parallel.

## Delivery Model

Workers pull batches with `WORK.CLAIM`. Each returned work item has a claim ID and fencing token. Follow-up operations such as `WORK.ACK`, `WORK.FAIL`, and `WORK.EXTEND` must present the current token.

This gives Oxide at-least-once transport delivery while making safe application retries practical.

Clients can use `WORK.RELEASE` to return a claimed item to ready or delayed state without marking it failed.

`STREAM.CLEAR` cancels ready and delayed work in a stream by default, with an explicit `include_claimed` option for operators that want to cancel live claims too.

## Durability Model

Mutating commands produce `EngineEvent` values. The runtime appends a non-empty event to the WAL before making the cloned engine state visible. Recovery replays all WAL events into a fresh engine.

The default fsync mode is `batch`. Operators can choose:

- `always`: sync every event
- `batch`: sync by byte threshold or time interval
- `async`: rely on the OS page cache

## Protocol Boundaries

The public model includes stream names, partition IDs, protocol versioning, and ownership boundaries so new runtime capabilities can evolve without requiring a conceptual rewrite.

If `auth_token` is configured, non-health protocol operations require a matching bearer token header. This is intentionally simple for v1 and should be deployed behind normal transport security in production.
