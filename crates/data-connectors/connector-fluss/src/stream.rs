/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Streaming logic for Fluss tables.
//!
//! - **Log tables** (no primary key): Uses `RecordBatchLogScanner` for batch-level
//!   polling, wrapping records as "create" operations (append mode).
//! - **PK tables** (primary key): Uses `LogScanner` for per-record polling with
//!   `ChangeType` awareness, mapping the changelog to CDC operations (changes mode).
//!
//! Both paths checkpoint per-(partition, bucket) offsets through the committer
//! carried on each [`ChangeEnvelope`]: the committer runs only after the batch is
//! durably applied by the accelerator, giving at-least-once delivery on restart.
//!
//! **Bootstrap readiness**: high watermarks are captured at subscribe time; the
//! stream reports `is_dataset_ready=false` until every subscribed (partition,
//! bucket) has been consumed up to its watermark. When the flip to ready happens
//! on an idle poll (e.g. resuming a checkpoint already at the tail), a zero-row
//! ready-signal envelope is emitted so the dataset still transitions to Ready.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, ListArray, RecordBatch, StringArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, SchemaRef};
use async_stream::stream;
use data_components::cdc::{self, ChangeBatch, ChangesStream, CommitChange};
use fluss::PartitionId;
use fluss::client::{EARLIEST_OFFSET, FlussConnection};
use fluss::metadata::TablePath;
use fluss::record::ChangeType;
use fluss::rpc::message::OffsetSpec;

use super::provider::FlussMetrics;

/// Offset key type: (partition_id, bucket_id).
/// `None` partition for non-partitioned tables.
pub type OffsetKey = (Option<PartitionId>, i32);

/// Factory closure that creates a `CommitChange` from the current consumed offsets.
///
/// Called by the stream functions per envelope to create a committer that
/// persists the latest offsets. If `None`, a no-op committer is used.
pub type CommitterFactory =
    Arc<dyn Fn(HashMap<OffsetKey, i64>) -> Box<dyn CommitChange + Send + Sync> + Send + Sync>;

/// Default poll timeout for the Fluss log scanner.
const POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// Wrap a Fluss client error into the connector-agnostic CDC stream error.
fn fluss_err(e: fluss::error::Error) -> cdc::StreamError {
    cdc::StreamError::Connector {
        connector: "fluss",
        source: Box::new(e),
    }
}

/// Create a committer — either from the factory or a no-op fallback.
fn make_committer(
    factory: Option<&CommitterFactory>,
    offsets: &HashMap<OffsetKey, i64>,
) -> Box<dyn CommitChange + Send + Sync> {
    match factory {
        Some(f) => f(offsets.clone()),
        None => Box::new(cdc::NoOpCommitter),
    }
}

/// Whether every watermarked (partition, bucket) has been consumed to its high
/// watermark. `last consumed offset` is inclusive; the watermark is exclusive
/// (next-to-write, Kafka semantics).
fn caught_up(
    high_watermarks: &HashMap<OffsetKey, i64>,
    consumed_offsets: &HashMap<OffsetKey, i64>,
) -> bool {
    high_watermarks.iter().all(|(key, &watermark)| {
        consumed_offsets
            .get(key)
            .map_or(watermark <= 0, |&consumed| consumed >= watermark - 1)
    })
}

/// Shared subscribe-time setup: capture high watermarks for every (partition,
/// bucket), then compute the start offset per bucket — resuming one past the
/// checkpointed offset when available, otherwise from `EARLIEST_OFFSET`.
///
/// Returns `(high_watermarks, bucket_offsets, partition_bucket_offsets)`; exactly
/// one of the two offset maps is non-empty depending on whether the table is
/// partitioned.
async fn prepare_subscription(
    connection: &FlussConnection,
    table_path: &TablePath,
    num_buckets: i32,
    is_partitioned: bool,
    initial_offsets: Option<&HashMap<OffsetKey, i64>>,
) -> Result<
    (
        HashMap<OffsetKey, i64>,
        HashMap<i32, i64>,
        HashMap<(PartitionId, i32), i64>,
    ),
    cdc::StreamError,
> {
    let admin = connection.get_admin().map_err(fluss_err)?;
    let bucket_ids: Vec<i32> = (0..num_buckets).collect();

    let mut high_watermarks: HashMap<OffsetKey, i64> = HashMap::new();
    let mut bucket_offsets: HashMap<i32, i64> = HashMap::new();
    let mut partition_bucket_offsets: HashMap<(PartitionId, i32), i64> = HashMap::new();

    let start_offset = |key: &OffsetKey| {
        initial_offsets
            .and_then(|m| m.get(key).copied())
            .map_or(EARLIEST_OFFSET, |o| o + 1) // resume AFTER last committed offset
    };

    if is_partitioned {
        let partitions = admin
            .list_partition_infos(table_path)
            .await
            .map_err(fluss_err)?;

        for partition_info in &partitions {
            let pid = partition_info.get_partition_id();
            let partition_name = partition_info.get_partition_name();
            let offsets = admin
                .list_partition_offsets(table_path, &partition_name, &bucket_ids, OffsetSpec::Latest)
                .await
                .map_err(fluss_err)?;
            for (bucket, offset) in offsets {
                high_watermarks.insert((Some(pid), bucket), offset);
            }
            for bucket in 0..num_buckets {
                partition_bucket_offsets
                    .insert((pid, bucket), start_offset(&(Some(pid), bucket)));
            }
        }
    } else {
        let offsets = admin
            .list_offsets(table_path, &bucket_ids, OffsetSpec::Latest)
            .await
            .map_err(fluss_err)?;
        for (bucket, offset) in offsets {
            high_watermarks.insert((None, bucket), offset);
        }
        for bucket in 0..num_buckets {
            bucket_offsets.insert(bucket, start_offset(&(None, bucket)));
        }
    }

    Ok((high_watermarks, bucket_offsets, partition_bucket_offsets))
}

/// Seed the consumed-offsets map from the checkpoint so readiness accounting
/// starts from the resume position rather than zero. Without this, a stream that
/// resumes at the tail of an idle table would never observe records and never
/// report ready.
fn seed_consumed(initial_offsets: Option<&HashMap<OffsetKey, i64>>) -> HashMap<OffsetKey, i64> {
    initial_offsets.cloned().unwrap_or_default()
}

/// Create a `ChangesStream` that polls a Fluss log table for new records
/// (append mode — every record is a "create").
pub async fn stream_log_table(
    connection: &FlussConnection,
    table_path: &TablePath,
    metrics: Arc<FlussMetrics>,
    initial_offsets: Option<HashMap<OffsetKey, i64>>,
    committer_factory: Option<CommitterFactory>,
) -> Result<ChangesStream, cdc::StreamError> {
    let table = connection.get_table(table_path).await.map_err(fluss_err)?;
    let table_info = table.get_table_info();
    let schema: SchemaRef = fluss::record::to_arrow_schema(table_info.row_type())
        .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?;

    let num_buckets = table_info.get_num_buckets();
    let is_partitioned = table_info.is_partitioned();

    let (high_watermarks, bucket_offsets, partition_bucket_offsets) = prepare_subscription(
        connection,
        table_path,
        num_buckets,
        is_partitioned,
        initial_offsets.as_ref(),
    )
    .await?;

    let scanner = table
        .new_scan()
        .create_record_batch_log_scanner()
        .map_err(fluss_err)?;

    if is_partitioned {
        scanner
            .subscribe_partition_buckets(&partition_bucket_offsets)
            .await
            .map_err(fluss_err)?;
    } else {
        scanner
            .subscribe_buckets(&bucket_offsets)
            .await
            .map_err(fluss_err)?;
    }

    let mut consumed_offsets = seed_consumed(initial_offsets.as_ref());
    let starts_ready = caught_up(&high_watermarks, &consumed_offsets);

    let stream = stream! {
        let mut is_ready = starts_ready;

        loop {
            let batches = match scanner.poll(POLL_TIMEOUT).await {
                Ok(batches) => batches,
                Err(e) => {
                    metrics.inc_poll_errors();
                    yield Err(fluss_err(e));
                    continue;
                }
            };

            if batches.is_empty() {
                // An idle poll can still complete bootstrap when the resume
                // position was already at the tail.
                if !is_ready && caught_up(&high_watermarks, &consumed_offsets) {
                    is_ready = true;
                    match cdc::build_ready_signal_envelope(&schema) {
                        Ok(envelope) => yield Ok(envelope),
                        Err(e) => yield Err(e.into()),
                    }
                }
                continue;
            }

            for scan_batch in batches {
                let batch = scan_batch.batch();
                if batch.num_rows() == 0 {
                    continue;
                }

                let key = (
                    scan_batch.bucket().partition_id(),
                    scan_batch.bucket().bucket_id(),
                );
                consumed_offsets.insert(key, scan_batch.last_offset());

                if !is_ready {
                    is_ready = caught_up(&high_watermarks, &consumed_offsets);
                }

                metrics.add_records_consumed(batch.num_rows() as u64);
                metrics.add_bytes_consumed(batch.get_array_memory_size() as u64);

                let change_batch = match cdc::wrap_data_as_change_batch(&schema, batch) {
                    Ok(cb) => cb,
                    Err(e) => {
                        yield Err(e.into());
                        continue;
                    }
                };

                let committer = make_committer(committer_factory.as_ref(), &consumed_offsets);
                yield Ok(cdc::ChangeEnvelope::new(committer, change_batch, is_ready));
            }
        }
    };

    Ok(Box::pin(stream))
}

/// Map Fluss `ChangeType` to a Spice CDC operation code.
///
/// Returns `None` for `UpdateBefore`, which is skipped — the accelerator
/// upserts by primary key, so only `UpdateAfter` is needed.
fn change_type_to_op(ct: ChangeType) -> Option<&'static str> {
    match ct {
        ChangeType::AppendOnly | ChangeType::Insert => Some("c"),
        ChangeType::UpdateAfter => Some("u"),
        ChangeType::Delete => Some("d"),
        ChangeType::UpdateBefore => None,
    }
}

/// Build a `ChangeBatch` from per-record scan results with CDC operations.
///
/// Each record's `ChangeType` is mapped to a CDC op code, and primary key
/// column names are attached per row. `UpdateBefore` records are filtered out.
/// Returns `Ok(None)` when every record was filtered.
fn build_cdc_change_batch(
    records: &[fluss::record::ScanRecord],
    table_schema: &SchemaRef,
    primary_keys: &[String],
) -> Result<Option<ChangeBatch>, cdc::StreamError> {
    let valid_records: Vec<_> = records
        .iter()
        .filter_map(|r| change_type_to_op(*r.change_type()).map(|op| (op, r)))
        .collect();

    if valid_records.is_empty() {
        return Ok(None);
    }

    let num_rows = valid_records.len();
    let changes_schema = cdc::changes_schema(table_schema.as_ref());

    // 1) op column
    let ops: Vec<&str> = valid_records.iter().map(|(op, _)| *op).collect();
    let op_array: ArrayRef = Arc::new(StringArray::from(ops));

    // 2) primary_keys column — the same key names for every row
    let pk_array: ArrayRef = if primary_keys.is_empty() {
        let offsets = vec![0i32; num_rows + 1];
        let values = Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            OffsetBuffer::new(offsets.into()),
            values,
            None,
        ))
    } else {
        let mut offsets = Vec::with_capacity(num_rows + 1);
        let mut pk_values = Vec::new();
        for _ in 0..num_rows {
            offsets.push(i32::try_from(pk_values.len()).unwrap_or(i32::MAX));
            for key in primary_keys {
                pk_values.push(key.as_str());
            }
        }
        offsets.push(i32::try_from(pk_values.len()).unwrap_or(i32::MAX));
        let values = Arc::new(StringArray::from(pk_values)) as ArrayRef;
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            OffsetBuffer::new(offsets.into()),
            values,
            None,
        ))
    };

    // 3) data column — slice each record's single row out of its underlying
    //    batch and concatenate.
    let mut row_batches: Vec<RecordBatch> = Vec::with_capacity(num_rows);
    for (_, record) in &valid_records {
        let columnar_row = record.row();
        let Some(batch) = columnar_row.get_record_batch() else {
            return Err(cdc::StreamError::External(
                "Fluss scan record carries no Arrow batch (non-columnar row)".to_string(),
            ));
        };
        row_batches.push(batch.slice(columnar_row.get_row_id(), 1));
    }

    let combined_batch = arrow::compute::concat_batches(table_schema, &row_batches)
        .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?;

    let data_array: ArrayRef = Arc::new(StructArray::new(
        combined_batch.schema().fields().clone(),
        combined_batch.columns().to_vec(),
        None,
    ));

    let record_batch = RecordBatch::try_new(
        Arc::new(changes_schema),
        vec![op_array, pk_array, data_array],
    )
    .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?;

    ChangeBatch::try_new(record_batch)
        .map(Some)
        .map_err(cdc::StreamError::from)
}

/// Create a `ChangesStream` that subscribes to the changelog of a Fluss
/// primary-key (KV) table and emits CDC operations:
/// `Insert`/`AppendOnly` → "c", `UpdateAfter` → "u", `Delete` → "d",
/// `UpdateBefore` → skipped.
///
/// Bootstrap replays the changelog from `EARLIEST_OFFSET` (no checkpoint) and
/// converges to current state via PK upserts; this requires the table's
/// changelog retention (`table.log.ttl`) to cover its history. Resume uses the
/// per-bucket offsets persisted by the envelope committers.
pub async fn stream_cdc_table(
    connection: &FlussConnection,
    table_path: &TablePath,
    metrics: Arc<FlussMetrics>,
    initial_offsets: Option<HashMap<OffsetKey, i64>>,
    committer_factory: Option<CommitterFactory>,
) -> Result<ChangesStream, cdc::StreamError> {
    let table = connection.get_table(table_path).await.map_err(fluss_err)?;
    let table_info = table.get_table_info();
    let schema: SchemaRef = fluss::record::to_arrow_schema(table_info.row_type())
        .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?;

    let primary_keys: Vec<String> = table_info.primary_keys.clone();

    let num_buckets = table_info.get_num_buckets();
    let is_partitioned = table_info.is_partitioned();

    let (high_watermarks, bucket_offsets, partition_bucket_offsets) = prepare_subscription(
        connection,
        table_path,
        num_buckets,
        is_partitioned,
        initial_offsets.as_ref(),
    )
    .await?;

    let scanner = table.new_scan().create_log_scanner().map_err(fluss_err)?;

    if is_partitioned {
        scanner
            .subscribe_partition_buckets(&partition_bucket_offsets)
            .await
            .map_err(fluss_err)?;
    } else {
        scanner
            .subscribe_buckets(&bucket_offsets)
            .await
            .map_err(fluss_err)?;
    }

    let mut consumed_offsets = seed_consumed(initial_offsets.as_ref());
    let starts_ready = caught_up(&high_watermarks, &consumed_offsets);

    let stream = stream! {
        let mut is_ready = starts_ready;

        loop {
            let scan_records = match scanner.poll(POLL_TIMEOUT).await {
                Ok(records) => records,
                Err(e) => {
                    metrics.inc_poll_errors();
                    yield Err(fluss_err(e));
                    continue;
                }
            };

            if scan_records.is_empty() {
                if !is_ready && caught_up(&high_watermarks, &consumed_offsets) {
                    is_ready = true;
                    tracing::info!("Fluss CDC bootstrap complete (caught up to high watermarks)");
                    match cdc::build_ready_signal_envelope(&schema) {
                        Ok(envelope) => yield Ok(envelope),
                        Err(e) => yield Err(e.into()),
                    }
                }
                continue;
            }

            // Advance per-bucket offsets from the records themselves, then
            // collect all records across buckets into one change batch.
            let records_by_bucket = scan_records.into_records_by_buckets();
            let mut all_records = Vec::new();
            for (bucket, records) in records_by_bucket {
                if let Some(last) = records.last() {
                    consumed_offsets.insert(
                        (bucket.partition_id(), bucket.bucket_id()),
                        last.offset(),
                    );
                }
                all_records.extend(records);
            }

            if all_records.is_empty() {
                continue;
            }

            if !is_ready {
                is_ready = caught_up(&high_watermarks, &consumed_offsets);
            }

            metrics.add_records_consumed(all_records.len() as u64);

            match build_cdc_change_batch(&all_records, &schema, &primary_keys) {
                Ok(Some(change_batch)) => {
                    let committer = make_committer(committer_factory.as_ref(), &consumed_offsets);
                    yield Ok(cdc::ChangeEnvelope::new(committer, change_batch, is_ready));
                }
                Ok(None) => {
                    // All records were UpdateBefore — nothing to emit, but the
                    // offsets still advanced; they ride on the next envelope.
                }
                Err(e) => {
                    yield Err(e);
                }
            }
        }
    };

    Ok(Box::pin(stream))
}
