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

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use fluss::client::FlussConnection;
use fluss::metadata::{TableInfo, TablePath};
use fluss::record::to_arrow_schema;

/// A `TableProvider` backed by an Apache Fluss table.
///
/// Data is delivered via streaming (`append_stream` for log tables or
/// `changes_stream` for primary-key tables), not via SQL scan. The `scan()`
/// method returns an empty execution plan, matching the Kafka connector.
pub struct FlussTableProvider {
    schema: SchemaRef,
    connection: Arc<FlussConnection>,
    table_path: TablePath,
    table_info: TableInfo,
}

impl fmt::Debug for FlussTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlussTableProvider")
            .field("table_path", &self.table_path.to_string())
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl FlussTableProvider {
    /// Create a new `FlussTableProvider` by connecting to a Fluss table
    /// and discovering its schema.
    pub async fn new(
        connection: Arc<FlussConnection>,
        table_path: TablePath,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (table_info, arrow_schema) = {
            let table = connection.get_table(&table_path).await?;
            let info = table.get_table_info().clone();
            let schema = to_arrow_schema(info.row_type())?;
            (info, schema)
        };

        Ok(Self {
            schema: arrow_schema,
            connection,
            table_path,
            table_info,
        })
    }

    /// Returns the underlying Fluss `TableInfo` metadata.
    #[must_use]
    pub fn table_info(&self) -> &TableInfo {
        &self.table_info
    }

    /// Returns the Fluss connection.
    #[must_use]
    pub fn connection(&self) -> &Arc<FlussConnection> {
        &self.connection
    }

    /// Returns the Fluss table path (database.table).
    #[must_use]
    pub fn table_path(&self) -> &TablePath {
        &self.table_path
    }

    /// Returns whether this table has a primary key (KV table).
    #[must_use]
    pub fn has_primary_key(&self) -> bool {
        self.table_info.has_primary_key()
    }
}

/// Thread-safe metrics state for the Fluss connector.
///
/// Updated atomically during the streaming poll loop and read by
/// the `FlussMetricsProvider` via OpenTelemetry callbacks.
#[derive(Debug, Default)]
pub struct FlussMetrics {
    pub records_consumed: AtomicU64,
    pub bytes_consumed: AtomicU64,
    pub poll_errors: AtomicU64,
}

impl FlussMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_records_consumed(&self, count: u64) {
        self.records_consumed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_bytes_consumed(&self, bytes: u64) {
        self.bytes_consumed.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn inc_poll_errors(&self) {
        self.poll_errors.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl TableProvider for FlussTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Streaming connectors deliver data via ChangesStream, not via scan.
        Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            self.schema(),
        )))
    }
}
