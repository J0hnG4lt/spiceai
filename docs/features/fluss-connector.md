# Fluss Data Connector

[Apache Fluss](https://fluss.apache.org/) is a streaming storage system built for real-time analytics, designed as a unified streaming and lakehouse storage layer. The Fluss connector enables Spice to consume data from Fluss log tables and primary-key (KV) tables via continuous streaming.

## Features

- **Log tables** (append mode): Continuous ingestion of append-only log records using `RecordBatchLogScanner`
- **Primary-key tables** (changes mode): CDC streaming with insert, update, and delete operations using `LogScanner`
- **Partitioned tables**: Automatic partition discovery and per-partition bucket subscription
- **Bootstrap readiness**: Dataset transitions to ready only after catching up to high watermarks
- **Metrics**: Observable counters for records consumed, bytes consumed, and poll errors

## Requirements

- A running Apache Fluss cluster (coordinator + tablet servers)
- `protoc` (Protocol Buffers compiler) installed for building the connector
- Datasets **must** be accelerated with refresh mode `append` (log tables) or `changes` (PK tables)

## Configuration

### Log Table (Append Mode)

```yaml
datasets:
  - from: fluss:my_database.my_log_table
    name: my_log_table
    params:
      fluss_bootstrap_servers: "localhost:9123"
    acceleration:
      enabled: true
      refresh_mode: append
```

### Primary-Key Table (Changes/CDC Mode)

```yaml
datasets:
  - from: fluss:my_database.my_pk_table
    name: my_pk_table
    params:
      fluss_bootstrap_servers: "localhost:9123"
    acceleration:
      enabled: true
      refresh_mode: changes
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `fluss_bootstrap_servers` | string | Yes | The Fluss coordinator server address (`host:port`) |

## Table Path Format

The `from` field uses the format `fluss:<database>.<table>`:

```
fluss:my_database.my_table
      ^^^^^^^^^^ ^^^^^^^^^
      database    table name
```

## Refresh Modes

| Mode | Table Type | Description |
|------|-----------|-------------|
| `append` | Log tables (no primary key) | Records are wrapped as "create" operations and appended to the accelerated table |
| `changes` | PK tables (has primary key) | CDC operations: inserts ("c"), updates ("u"), and deletes ("d") are applied to the accelerated table |

## Metrics

The Fluss connector exposes the following observable metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `records_consumed_total` | Counter (u64) | Total number of records consumed from Fluss |
| `bytes_consumed_total` | Counter (u64) | Total bytes consumed from Fluss |
| `poll_errors_total` | Counter (u64) | Total poll errors encountered |

## Bootstrap Behavior

When a Fluss dataset starts, the connector fetches high watermarks from the Fluss coordinator to determine how much data exists. The dataset is marked as "Initializing" until it catches up:

- **Log tables**: Tracks consumed offsets per bucket. The dataset becomes ready when all buckets reach their high watermarks.
- **PK tables**: Uses a heuristic — the dataset becomes ready after receiving data and then observing 3 seconds of consecutive empty polls (indicating the stream has caught up to the log tail).
- **Empty tables**: If all high watermarks are 0, the dataset starts in the ready state immediately.

## Partitioned Tables

The connector automatically detects partitioned tables and subscribes to all partitions. Partition discovery uses the Fluss admin API (`list_partition_infos`). Each partition's buckets are subscribed independently.

No additional configuration is needed for partitioned tables — the connector handles them transparently.

## Known Limitations

- **No checkpoint persistence**: The connector does not persist consumed offsets across restarts. On restart, it replays from offset 0. This will be addressed in a future release.
- **No direct SQL scan**: Fluss data is delivered via streaming only. The `scan()` method returns an empty result set — all data flows through the acceleration layer.
- **CDC type propagation**: The `LogScanner` in fluss-rs may not fully propagate all CDC change types in early versions. The implementation structure is ready for proper CDC when fluss-rs matures.

## Troubleshooting

### "The Fluss data connector requires an accelerated dataset"

Fluss datasets must have acceleration enabled:

```yaml
acceleration:
  enabled: true
  refresh_mode: append  # or 'changes'
```

### "The Fluss connector requires refresh mode 'append' or 'changes'"

Set `refresh_mode` to `append` for log tables or `changes` for primary-key tables.

### "Invalid table path '...'. Expected format: 'database.table'"

The `from` path must contain exactly one dot separating the database and table names:

```yaml
from: fluss:my_database.my_table  # correct
from: fluss:my_table              # incorrect — missing database
```
