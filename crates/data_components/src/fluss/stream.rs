// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Streaming logic for Fluss tables.
//!
//! - **Log tables** (no primary key): Uses `RecordBatchLogScanner` for batch-level
//!   polling, wrapping records as "create" operations (append mode).
//! - **PK tables** (primary key): Uses `LogScanner` for per-record polling with
//!   `ChangeType` awareness, mapping to CDC operations (changes mode).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, ListArray, RecordBatch, StringArray, StructArray};
use arrow::datatypes::{DataType, Field};
use arrow_buffer::OffsetBuffer;
use async_stream::stream;
use fluss::PartitionId;
use fluss::client::FlussConnection;
use fluss::metadata::TablePath;
use fluss::record::ChangeType;
use fluss::rpc::message::OffsetSpec;

use super::FlussMetrics;
use crate::cdc::{self, ChangeBatch, ChangesStream, CommitChange, CommitError};

/// Offset key type: (partition_id, bucket_id).
/// `None` partition for non-partitioned tables.
pub type OffsetKey = (Option<PartitionId>, i32);

/// Factory closure that creates a `CommitChange` from the current consumed offsets.
///
/// Called by the stream functions after each batch to create a committer that
/// persists the latest offsets. If `None`, a no-op committer is used.
pub type CommitterFactory =
    Arc<dyn Fn(HashMap<OffsetKey, i64>) -> Box<dyn CommitChange + Send> + Send + Sync>;

/// Default poll timeout for the Fluss log scanner.
const POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// Number of consecutive empty polls required to consider the CDC stream
/// caught up during bootstrap (6 × 500ms = 3 seconds of silence).
const BOOTSTRAP_EMPTY_POLLS_THRESHOLD: u32 = 6;

/// A no-op committer used when no checkpoint persistence is configured.
struct FlussNoOpCommitter;

impl CommitChange for FlussNoOpCommitter {
    fn commit(&self) -> Result<(), CommitError> {
        Ok(())
    }
}

/// Create a committer — either from the factory or a no-op fallback.
fn make_committer(
    factory: &Option<CommitterFactory>,
    offsets: &HashMap<OffsetKey, i64>,
) -> Box<dyn CommitChange + Send> {
    match factory {
        Some(f) => f(offsets.clone()),
        None => Box::new(FlussNoOpCommitter),
    }
}

/// Create a `ChangesStream` that polls a Fluss log table for new records.
///
/// Supports both non-partitioned and partitioned tables. Non-partitioned tables
/// use `subscribe_buckets()`; partitioned tables discover partitions via the
/// admin API and use `subscribe_partition_buckets()`.
///
/// **Bootstrap readiness**: The stream signals `is_dataset_ready=false` until
/// all (partition, bucket) pairs have caught up to their high watermarks
/// (fetched at startup). Once caught up, the dataset transitions to ready.
pub async fn stream_log_table(
    connection: &FlussConnection,
    table_path: &TablePath,
    metrics: Arc<FlussMetrics>,
    initial_offsets: Option<HashMap<OffsetKey, i64>>,
    committer_factory: Option<CommitterFactory>,
) -> Result<ChangesStream, cdc::StreamError> {
    let table = connection
        .get_table(table_path)
        .await
        .map_err(cdc::StreamError::Fluss)?;

    let table_info = table.get_table_info();
    let schema = Arc::new(
        fluss::record::to_arrow_schema(table_info.row_type())
            .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?,
    );

    let num_buckets = table_info.num_buckets;
    let bucket_ids: Vec<i32> = (0..num_buckets).collect();
    let is_partitioned = table_info.is_partitioned();

    let admin = connection
        .get_admin()
        .await
        .map_err(cdc::StreamError::Fluss)?;

    // Unified watermark key: (partition_id_if_any, bucket_id) → high watermark.
    // Latest offset = next-to-write offset (Kafka semantics).
    let mut high_watermarks: HashMap<(Option<PartitionId>, i32), i64> = HashMap::new();

    let scanner = table
        .new_scan()
        .create_record_batch_log_scanner()
        .map_err(cdc::StreamError::Fluss)?;

    if is_partitioned {
        let partitions = admin
            .list_partition_infos(table_path)
            .await
            .map_err(cdc::StreamError::Fluss)?;

        // Fetch per-partition watermarks.
        for partition_info in &partitions {
            let pid = partition_info.get_partition_id();
            let partition_name = partition_info.get_partition_name();
            let offsets = admin
                .list_partition_offsets(table_path, &partition_name, &bucket_ids, OffsetSpec::Latest)
                .await
                .map_err(cdc::StreamError::Fluss)?;
            for (bucket, offset) in offsets {
                high_watermarks.insert((Some(pid), bucket), offset);
            }
        }

        // Subscribe to all partition buckets, resuming from checkpoint if available.
        let mut partition_offsets: HashMap<(PartitionId, i32), i64> = HashMap::new();
        for partition_info in &partitions {
            let pid = partition_info.get_partition_id();
            for bucket in 0..num_buckets {
                let saved = initial_offsets
                    .as_ref()
                    .and_then(|m| m.get(&(Some(pid), bucket)).copied())
                    .map(|o| o + 1) // resume AFTER last committed offset
                    .unwrap_or(0);
                partition_offsets.insert((pid, bucket), saved);
            }
        }
        scanner
            .subscribe_partition_buckets(&partition_offsets)
            .await
            .map_err(cdc::StreamError::Fluss)?;
    } else {
        let offsets = admin
            .list_offsets(table_path, &bucket_ids, OffsetSpec::Latest)
            .await
            .map_err(cdc::StreamError::Fluss)?;
        for (bucket, offset) in offsets {
            high_watermarks.insert((None, bucket), offset);
        }

        // Subscribe to all buckets, resuming from checkpoint if available.
        let mut bucket_offsets: HashMap<i32, i64> = HashMap::new();
        for bucket in 0..num_buckets {
            let saved = initial_offsets
                .as_ref()
                .and_then(|m| m.get(&(None, bucket)).copied())
                .map(|o| o + 1) // resume AFTER last committed offset
                .unwrap_or(0);
            bucket_offsets.insert(bucket, saved);
        }
        scanner
            .subscribe_buckets(&bucket_offsets)
            .await
            .map_err(cdc::StreamError::Fluss)?;
    }

    // If all watermarks are 0, the table is empty — start as ready.
    let starts_ready = high_watermarks.values().all(|&offset| offset <= 0);

    let stream = stream! {
        let mut is_ready = starts_ready;
        let mut consumed_offsets: HashMap<(Option<PartitionId>, i32), i64> = HashMap::new();

        loop {
            let batches = match scanner.poll(POLL_TIMEOUT).await {
                Ok(batches) => batches,
                Err(e) => {
                    metrics.inc_poll_errors();
                    yield Err(cdc::StreamError::Fluss(e));
                    continue;
                }
            };

            for scan_batch in batches {
                let batch = scan_batch.batch();
                if batch.num_rows() == 0 {
                    continue;
                }

                // Always track consumed offsets (for checkpointing and bootstrap).
                let key = (
                    scan_batch.bucket().partition_id(),
                    scan_batch.bucket().bucket_id(),
                );
                consumed_offsets.insert(key, scan_batch.last_offset());

                // Check bootstrap readiness.
                if !is_ready {
                    // last_offset() is inclusive, watermark is exclusive (next-to-write).
                    is_ready = high_watermarks.iter().all(|(wm_key, &watermark)| {
                        consumed_offsets
                            .get(wm_key)
                            .map_or(watermark <= 0, |&consumed| consumed >= watermark - 1)
                    });
                }

                metrics.add_records_consumed(batch.num_rows() as u64);
                metrics.add_bytes_consumed(
                    batch.get_array_memory_size() as u64,
                );

                let change_batch = match cdc::wrap_data_as_change_batch(&schema, batch) {
                    Ok(cb) => cb,
                    Err(e) => {
                        yield Err(cdc::StreamError::Arrow(e.to_string()));
                        continue;
                    }
                };

                let committer = make_committer(&committer_factory, &consumed_offsets);
                let envelope = cdc::ChangeEnvelope::new(
                    committer,
                    change_batch,
                    is_ready,
                );

                yield Ok(envelope);
            }
        }
    };

    Ok(Box::pin(stream))
}

/// Map Fluss `ChangeType` to Spice AI CDC operation code.
///
/// Returns `None` for `UpdateBefore` which should be skipped — Spice AI
/// uses "last writer wins" semantics, so only `UpdateAfter` is needed.
fn change_type_to_op(ct: ChangeType) -> Option<&'static str> {
    match ct {
        ChangeType::AppendOnly | ChangeType::Insert => Some("c"),
        ChangeType::UpdateAfter => Some("u"),
        ChangeType::Delete => Some("d"),
        ChangeType::UpdateBefore => None, // skip
    }
}

/// Build a `ChangeBatch` from per-record scan results with CDC operations.
///
/// Each record's `ChangeType` is mapped to a CDC op code, and primary key
/// column names are included. `UpdateBefore` records are filtered out.
fn build_cdc_change_batch(
    records: &[fluss::record::ScanRecord],
    table_schema: &arrow::datatypes::SchemaRef,
    primary_keys: &[String],
) -> Result<Option<ChangeBatch>, cdc::StreamError> {
    // Filter to only records with valid ops (skip UpdateBefore).
    let valid_records: Vec<_> = records
        .iter()
        .filter_map(|r| change_type_to_op(*r.change_type()).map(|op| (op, r)))
        .collect();

    if valid_records.is_empty() {
        return Ok(None);
    }

    let num_rows = valid_records.len();
    let changes_schema = cdc::changes_schema(table_schema.as_ref());

    // 1) Build op column
    let ops: Vec<String> = valid_records
        .iter()
        .map(|(op, _)| (*op).to_string())
        .collect();
    let op_array: ArrayRef = Arc::new(StringArray::from(ops));

    // 2) Build primary_keys column — same key names for every row
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
        for _i in 0..num_rows {
            offsets.push(pk_values.len() as i32);
            for key in primary_keys {
                pk_values.push(key.as_str());
            }
        }
        offsets.push(pk_values.len() as i32);
        let values = Arc::new(StringArray::from(pk_values)) as ArrayRef;
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            OffsetBuffer::new(offsets.into()),
            values,
            None,
        ))
    };

    // 3) Build data column — extract each row from its underlying RecordBatch.
    //    ColumnarRow wraps Arc<RecordBatch> with a row_id, so we slice single
    //    rows and concatenate them into one batch.
    let row_batches: Vec<RecordBatch> = valid_records
        .iter()
        .map(|(_, record)| {
            let columnar_row = record.row();
            let batch = columnar_row.get_record_batch();
            let row_id = columnar_row.get_row_id();
            batch.slice(row_id, 1)
        })
        .collect();

    let combined_batch = arrow::compute::concat_batches(table_schema, &row_batches)
        .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?;

    let data_array: ArrayRef = Arc::new(StructArray::new(
        combined_batch.schema().fields().clone(),
        combined_batch.columns().to_vec(),
        None,
    ));

    let columns = vec![op_array, pk_array, data_array];
    let record_batch = RecordBatch::try_new(Arc::new(changes_schema), columns)
        .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?;

    ChangeBatch::try_new(record_batch)
        .map(Some)
        .map_err(|e| cdc::StreamError::Arrow(e.to_string()))
}

/// Create a `ChangesStream` that polls a Fluss primary-key (KV) table
/// for CDC changes.
///
/// Supports both non-partitioned and partitioned tables. Uses `LogScanner`
/// for per-record `ChangeType` access, mapping Fluss change types to
/// Spice AI CDC operations:
/// - `Insert` / `AppendOnly` -> Create ("c")
/// - `UpdateAfter` -> Update ("u")
/// - `Delete` -> Delete ("d")
/// - `UpdateBefore` -> skipped
///
/// **Bootstrap readiness**: The `LogScanner` does not expose per-record offsets,
/// so readiness uses a heuristic: if the table has data (high watermarks > 0),
/// the stream signals `is_dataset_ready=false` until it observes
/// `BOOTSTRAP_EMPTY_POLLS_THRESHOLD` consecutive empty polls after receiving
/// at least one batch of records — indicating it has caught up to the log tail.
pub async fn stream_cdc_table(
    connection: &FlussConnection,
    table_path: &TablePath,
    metrics: Arc<FlussMetrics>,
    initial_offsets: Option<HashMap<OffsetKey, i64>>,
    _committer_factory: Option<CommitterFactory>,
) -> Result<ChangesStream, cdc::StreamError> {
    let table = connection
        .get_table(table_path)
        .await
        .map_err(cdc::StreamError::Fluss)?;

    let table_info = table.get_table_info();
    let schema = Arc::new(
        fluss::record::to_arrow_schema(table_info.row_type())
            .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?,
    );

    let primary_keys: Vec<String> = table_info
        .primary_keys
        .iter()
        .map(|pk| pk.to_string())
        .collect();

    let num_buckets = table_info.num_buckets;
    let bucket_ids: Vec<i32> = (0..num_buckets).collect();
    let is_partitioned = table_info.is_partitioned();

    let admin = connection
        .get_admin()
        .await
        .map_err(cdc::StreamError::Fluss)?;

    // Collect all watermarks to determine if table has existing data.
    let mut all_watermark_values: Vec<i64> = Vec::new();

    let scanner = table
        .new_scan()
        .create_log_scanner()
        .map_err(cdc::StreamError::Fluss)?;

    if is_partitioned {
        let partitions = admin
            .list_partition_infos(table_path)
            .await
            .map_err(cdc::StreamError::Fluss)?;

        for partition_info in &partitions {
            let partition_name = partition_info.get_partition_name();
            let offsets = admin
                .list_partition_offsets(table_path, &partition_name, &bucket_ids, OffsetSpec::Latest)
                .await
                .map_err(cdc::StreamError::Fluss)?;
            all_watermark_values.extend(offsets.values());
        }

        // Subscribe to all partition buckets, resuming from checkpoint if available.
        let mut partition_offsets: HashMap<(PartitionId, i32), i64> = HashMap::new();
        for partition_info in &partitions {
            let pid = partition_info.get_partition_id();
            for bucket in 0..num_buckets {
                let saved = initial_offsets
                    .as_ref()
                    .and_then(|m| m.get(&(Some(pid), bucket)).copied())
                    .map(|o| o + 1) // resume AFTER last committed offset
                    .unwrap_or(0);
                partition_offsets.insert((pid, bucket), saved);
            }
        }
        scanner
            .subscribe_partition_buckets(&partition_offsets)
            .await
            .map_err(cdc::StreamError::Fluss)?;
    } else {
        let offsets = admin
            .list_offsets(table_path, &bucket_ids, OffsetSpec::Latest)
            .await
            .map_err(cdc::StreamError::Fluss)?;
        all_watermark_values.extend(offsets.values());

        // Subscribe to all buckets, resuming from checkpoint if available.
        let mut bucket_offsets: HashMap<i32, i64> = HashMap::new();
        for bucket in 0..num_buckets {
            let saved = initial_offsets
                .as_ref()
                .and_then(|m| m.get(&(None, bucket)).copied())
                .map(|o| o + 1) // resume AFTER last committed offset
                .unwrap_or(0);
            bucket_offsets.insert(bucket, saved);
        }
        scanner
            .subscribe_buckets(&bucket_offsets)
            .await
            .map_err(cdc::StreamError::Fluss)?;
    }

    // If all watermarks are 0, the table is empty — start as ready.
    let starts_ready = all_watermark_values.iter().all(|&offset| offset <= 0);

    let stream = stream! {
        let mut is_ready = starts_ready;
        let mut has_received_data = false;
        let mut consecutive_empty_polls: u32 = 0;

        loop {
            let scan_records = match scanner.poll(POLL_TIMEOUT).await {
                Ok(records) => records,
                Err(e) => {
                    metrics.inc_poll_errors();
                    yield Err(cdc::StreamError::Fluss(e));
                    continue;
                }
            };

            if scan_records.is_empty() {
                // Track consecutive empty polls for bootstrap readiness.
                if !is_ready && has_received_data {
                    consecutive_empty_polls += 1;
                    if consecutive_empty_polls >= BOOTSTRAP_EMPTY_POLLS_THRESHOLD {
                        is_ready = true;
                        tracing::info!("Fluss CDC bootstrap complete (caught up after {BOOTSTRAP_EMPTY_POLLS_THRESHOLD} empty polls)");

                        // Yield an empty envelope to deliver the readiness signal
                        // downstream. Without this, the accelerated table never
                        // transitions to Ready when all data was consumed during
                        // bootstrap and no new records arrive.
                        let empty_schema = cdc::changes_schema(schema.as_ref());
                        let empty_record = RecordBatch::new_empty(Arc::new(empty_schema));
                        if let Ok(change_batch) = ChangeBatch::try_new(empty_record) {
                            let envelope = cdc::ChangeEnvelope::new(
                                Box::new(FlussNoOpCommitter),
                                change_batch,
                                true,
                            );
                            yield Ok(envelope);
                        }
                    }
                }
                continue;
            }

            consecutive_empty_polls = 0;

            // Collect all records across all buckets into one batch.
            let all_records: Vec<_> = scan_records
                .into_records_by_buckets()
                .into_values()
                .flatten()
                .collect();

            if all_records.is_empty() {
                continue;
            }

            has_received_data = true;
            metrics.add_records_consumed(all_records.len() as u64);

            match build_cdc_change_batch(&all_records, &schema, &primary_keys) {
                Ok(Some(change_batch)) => {
                    let envelope = cdc::ChangeEnvelope::new(
                        Box::new(FlussNoOpCommitter),
                        change_batch,
                        is_ready,
                    );
                    yield Ok(envelope);
                }
                Ok(None) => {
                    // All records were UpdateBefore — nothing to emit.
                }
                Err(e) => {
                    yield Err(e);
                }
            }
        }
    };

    Ok(Box::pin(stream))
}
