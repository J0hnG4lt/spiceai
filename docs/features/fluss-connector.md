# Fluss Data Connector

[Apache Fluss](https://fluss.apache.org/) is a streaming storage system built for real-time analytics, designed as the streaming storage layer for lakehouse architectures. The Fluss connector streams data from Fluss tables into accelerated Spice datasets with efficient CDC-based refresh.

**Maturity**: targeting [Alpha criteria](../criteria/connectors/alpha.md). Feature flag: `fluss` (enabled by default in `spiced`).

## Features

- **Log tables** (`refresh_mode: append`): continuous ingestion of append-only records
- **Primary-key tables** (`refresh_mode: changes`): the table's changelog streams in as CDC operations — inserts (`c`), updates (`u`), deletes (`d`) — applied to the accelerated table by primary key
- **Offset checkpointing**: per-(partition, bucket) offsets persist in the dataset accelerator's sidecar; a restart resumes the stream instead of re-reading from the beginning
- **Partitioned tables**: partitions discovered at stream start; per-partition bucket subscription
- **Fault recovery**: poll errors back off; a checkpoint stranded past truncated log segments recovers automatically (see Delivery semantics)
- **Metrics**: `records_consumed_total`, `bytes_consumed_total`, `poll_errors_total`

## Requirements

- A running Apache Fluss cluster (0.8+; tested against 0.9.1-incubating)
- Datasets **must** be accelerated with `refresh_mode: append` (log tables) or `changes` (PK tables); the mode must match the table type
- File-mode acceleration (e.g. `engine: duckdb, mode: file`) is required for checkpoints to survive restarts; without it the connector logs a warning and streams from the beginning on every start

## Configuration

```yaml
datasets:
  # Log table (no primary key)
  - from: fluss:my_database.my_log_table
    name: my_log_table
    params:
      fluss_bootstrap_servers: "localhost:9123"
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      refresh_mode: append

  # Primary-key table (CDC)
  - from: fluss:my_database.my_pk_table
    name: my_pk_table
    params:
      fluss_bootstrap_servers: "localhost:9123"
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      refresh_mode: changes
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `fluss_bootstrap_servers` | string | Yes | The Fluss coordinator server address (`host:port`) |

The `from` field uses the format `fluss:<database>.<table>`.

## Bootstrap and readiness

At stream start the connector captures every (partition, bucket) high watermark and subscribes:

- With a persisted checkpoint: one past the last committed offset per bucket.
- Without: from `EARLIEST_OFFSET`. For PK tables this replays the changelog; idempotent upserts/deletes converge the accelerated table to the source's current state. **This requires the table's changelog retention (`table.log.ttl`, default 7 days) to cover its history** — rows whose changelog has expired will be missing. A KV-snapshot bootstrap will remove this caveat once the Rust client exposes snapshot reads.

The dataset reports Ready when all buckets are consumed up to their watermarks; a stream that starts already caught up (empty table, idle resume) announces readiness immediately.

## Delivery semantics

- Offsets are committed **after** a batch is durably applied by the accelerator (the `CommitChange` contract): a graceful shutdown resumes exactly; a hard crash re-delivers at most the uncommitted tail (**at-least-once** for append tables; PK tables stay exact because replayed operations are idempotent by key).
- If the server reports the subscription offset is out of range (log segments truncated below the checkpoint — e.g. a tablet server lost unflushed data), the connector recovers instead of failing permanently: **PK tables** replay the changelog from `EARLIEST_OFFSET` and converge; **log tables** rejoin at the live tail and log a data-loss warning, since replaying appends would duplicate the table.
- Poll errors back off 1s between retries.

## Known limitations

- Bootstrap relies on changelog retention (`table.log.ttl`) — see above.
- The partition set is fixed at stream start; partitions created later are not picked up until the dataset restarts.
- No authentication parameters yet (matches the current fluss-rs client surface).
- No SQL federation: data is delivered via streaming only; direct `scan()` on the federated table returns empty and all queries are served from the accelerated table.
- Depends on a git-pinned `fluss-rs` (Arrow 58 branch) until an Arrow ≥58 release ships on crates.io.

## Testing

An end-to-end suite lives in [`e2e/fluss`](../../e2e/fluss/README.md): a podman Fluss cluster, a deterministic producer, and seven scenarios covering bootstrap replay, realtime append, live CDC, graceful and crash resume, tablet-server faults, and chaos (coordinator pause under load). Unit tests cover CDC batch construction, readiness accounting, and checkpoint serialization (`cargo test -p connector-fluss`).

## Troubleshooting

- **"The Fluss connector requires an accelerated dataset with refresh mode 'append' ... or 'changes'"** — add the acceleration block shown above.
- **"This Fluss table has a primary key; use refresh_mode: changes."** (and vice versa) — the refresh mode must match the table type.
- **"Invalid table path '...'. Expected format: 'database.table'"** — the `from` path needs exactly one dot: `fluss:db.table`.
- **"Fluss dataset is not file-accelerated. Connector state is ephemeral..."** — use a file-mode accelerator to persist checkpoints.
- **"Fluss log segments were truncated past the checkpoint"** — the source lost log data (retention or server fault); see Delivery semantics for how each mode recovers.
