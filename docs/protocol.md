# Oxide Wire Protocol v1

Oxide uses a native length-prefixed TCP protocol. Payload bytes are opaque to the frame layer. JSON is used by the current CLI and server command payloads, but the frame format does not require application payloads to be JSON.

If `auth_token` is configured on the server, all operations except `HEALTH` require either `authorization = "Bearer <token>"` or `auth-token = "<token>"` in frame headers.

## Frame Layout

All integer fields are big-endian.

| Field | Bytes | Notes |
| --- | ---: | --- |
| magic | 4 | `OXD1` |
| version | 2 | currently `1` |
| request_id | 8 | echoed by responses |
| op_len | 2 | operation name byte length |
| headers_len | 4 | JSON object byte length |
| payload_len | 4 | opaque payload byte length |
| reserved | 4 | currently zero |
| op | `op_len` | UTF-8 operation name |
| headers | `headers_len` | JSON object of string keys/values |
| payload | `payload_len` | opaque bytes |

## Core Operations

- `STREAM.CREATE`: payload is `StreamConfig`
- `STREAM.UPDATE`: payload is `StreamConfig`
- `STREAM.INFO`: requires `stream` header
- `STREAM.CLEAR`: payload is `{ "stream": "...", "include_claimed": false }`
- `QUEUE.CLEAR`: alias for `STREAM.CLEAR`
- `WORK.SUBMIT`: payload is `SubmitWork`
- `WORK.CLAIM`: payload is `ClaimRequest`, optional `wait_ms` header
- `WORK.CLAIM_MANY`: payload is `ClaimManyRequest`, optional `wait_ms` header
- `WORK.ACK`: payload is `CompleteRequest`
- `WORK.FAIL`: payload is `FailRequest`
- `WORK.RELEASE`: payload is `ReleaseRequest`
- `WORK.EXTEND`: payload is `ExtendRequest`
- `WORK.CANCEL`: payload is `{ "work_id": 123 }`
- `WORK.INSPECT`: requires `work_id` header
- `GROUP.INFO`: requires `group` header
- `STATS`: no payload
- `HEALTH`: no payload
- `METRICS`: no payload, returns Prometheus-style text
- `PAUSE`: payload is `{ "stream": "...", "duration_ms": 1000 }` or `{ "stream": "...", "until_ms": 123 }`
- `RESUME`: requires `stream` header
- `DRAIN`: flushes WAL and returns stats

`WORK.CLAIM` and `WORK.CLAIM_MANY` lazily create missing streams from server
`stream_defaults` when `lazy_claim_stream_creation` is enabled. Other stream
and work operations still return an error for missing streams.

## Response Headers

Responses include a `status` header:

- `ok`: request succeeded
- `empty`: request succeeded but no work was available
- `error`: request failed; `error` header contains a human-readable message

## Stable JSON Responses

The public protocol returns stable JSON objects rather than internal engine event enum shapes.

Examples:

```json
{ "work_id": 123 }
```

```json
{
  "count": 1,
  "items": [
    {
      "claim": {
        "claim_id": 7,
        "work_id": 123,
        "stream": "email",
        "consumer_group": "workers",
        "worker_id": "worker-1",
        "fence_token": 9,
        "claimed_at_ms": 1780000000000,
        "expires_at_ms": 1780000030000,
        "lane_key": "account-42"
      },
      "work": {}
    }
  ]
}
```

`STREAM.INFO` includes stream counters:

- `ready` / `pending`
- `scheduled` / `delayed`
- `claimed` / `reserved`
- `oldest_pending_submitted_at_ms`

## Request Schemas

`ClaimManyRequest`:

```json
{
  "streams": ["high", "default", "low"],
  "consumer_group": "workers",
  "worker_id": "worker-1",
  "max": 1,
  "lease_ms": 90000
}
```

`ReleaseRequest`:

```json
{
  "claim_id": 7,
  "work_id": 123,
  "fence_token": 9,
  "delay_ms": 30000,
  "priority": null
}
```

`ClearStreamRequest`:

```json
{
  "stream": "default",
  "include_claimed": false
}
```
