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

//! Apache Fluss data connector for Spice.ai runtime.
//!
//! Supports two dataset modes:
//! - **Log tables** with `refresh_mode: append` — records stream in as inserts.
//! - **Primary-key tables** with `refresh_mode: changes` — the table's changelog
//!   streams in as CDC upserts/deletes, DynamoDB-Streams-style.
//!
//! Per-(partition, bucket) offsets are checkpointed in a sidecar table of the
//! dataset's own accelerator ([`runtime::dataaccelerator::spice_sys`]), so a
//! restart resumes the stream instead of re-reading from the beginning.
#![allow(clippy::missing_errors_doc)]

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::sync::atomic::Ordering;

use async_stream::stream;
use data_components::cdc::{self, ChangesStream, CommitChange, CommitError};
use datafusion::catalog::TableProvider;
use fluss::client::FlussConnection;
use fluss::config::Config as FlussConfig;
use fluss::metadata::TablePath;
use futures::StreamExt;
use runtime::{
    component::dataset::{Dataset, acceleration::RefreshMode},
    dataaccelerator::spice_sys,
    dataconnector::{
        ConnectorComponent, DataConnector, DataConnectorError, DataConnectorFactory,
        DataConnectorResult, InvalidConfigurationNoSourceSnafu, NewDataConnectorResult,
        UnableToGetReadProviderSnafu, parameters::ConnectorParams,
    },
    federated_table::FederatedTable,
    parameters::{ParameterSpec, Parameters},
};
use runtime_api_types::v1::ComponentType;
use runtime_checkpoint_api::BlobCheckpointStore;
use runtime_metrics::component::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tonic::async_trait;

pub mod provider;
pub mod stream;

use provider::{FlussMetrics, FlussTableProvider};
use stream::{CommitterFactory, OffsetKey};

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "fluss";

const FLUSS_DOCS: &str = "https://spiceai.org/docs/components/data-connectors/fluss";

/// Sidecar table (in the dataset's own accelerator) holding this connector's
/// serialized per-bucket offsets.
const FLUSS_CHECKPOINT_TABLE: &str = "spice_sys_fluss_log";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: 'fluss_bootstrap_servers'. Specify the Fluss coordinator address. For details, visit: {FLUSS_DOCS}"
    ))]
    MissingBootstrapServers,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// -- Checkpoint serialization --

#[derive(Serialize, Deserialize)]
struct FlussCheckpoint {
    offsets: Vec<FlussOffsetEntry>,
}

#[derive(Serialize, Deserialize)]
struct FlussOffsetEntry {
    partition_id: Option<i64>,
    bucket_id: i32,
    offset: i64,
}

fn serialize_offsets(offsets: &HashMap<OffsetKey, i64>) -> Result<String, serde_json::Error> {
    let entries: Vec<FlussOffsetEntry> = offsets
        .iter()
        .map(|(&(partition_id, bucket_id), &offset)| FlussOffsetEntry {
            partition_id,
            bucket_id,
            offset,
        })
        .collect();
    serde_json::to_string(&FlussCheckpoint { offsets: entries })
}

fn deserialize_offsets(data: &str) -> Option<HashMap<OffsetKey, i64>> {
    let checkpoint: FlussCheckpoint = serde_json::from_str(data).ok()?;
    Some(
        checkpoint
            .offsets
            .into_iter()
            .map(|e| ((e.partition_id, e.bucket_id), e.offset))
            .collect(),
    )
}

// -- Committer --

/// Persists the consumed per-bucket offsets to the accelerator sidecar once the
/// batch it rides on has been durably applied (the `CommitChange` ordering
/// contract), giving at-least-once delivery across restarts.
struct FlussStreamCommitter {
    store: Option<Arc<dyn BlobCheckpointStore>>,
    offsets: HashMap<OffsetKey, i64>,
    dataset: String,
}

#[async_trait]
impl CommitChange for FlussStreamCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        let checkpoint_json = serialize_offsets(&self.offsets).map_err(|e| {
            CommitError::UnableToCommitChange {
                source: Box::new(e),
            }
        })?;

        if let Some(store) = self.store.as_ref() {
            store.upsert(&checkpoint_json).await.map_err(|e| {
                CommitError::UnableToCommitChange {
                    source: Box::new(e),
                }
            })?;
        }
        cdc::log_committer_progress(
            CONNECTOR_NAME,
            &self.dataset,
            &format!("buckets={}", self.offsets.len()),
            None,
        );
        Ok(())
    }
}

/// Initialize the sidecar checkpoint store and load any persisted offsets.
///
/// Returns `(initial_offsets, committer_factory)`. A missing or unusable store
/// degrades gracefully: offsets are not persisted and the stream restarts from
/// the beginning on every runtime restart.
async fn initialize_checkpoint(
    dataset: &Dataset,
) -> (Option<HashMap<OffsetKey, i64>>, Option<CommitterFactory>) {
    let store: Option<Arc<dyn BlobCheckpointStore>> = if dataset.is_file_accelerated() {
        spice_sys::checkpoint_store(dataset, FLUSS_CHECKPOINT_TABLE).await
    } else {
        tracing::warn!(
            dataset = %dataset.name,
            "Fluss dataset is not file-accelerated. Connector state is ephemeral and the stream will restart on every runtime restart"
        );
        None
    };

    let Some(store) = store else {
        return (None, None);
    };

    let initial_offsets = match store.get().await {
        Ok(Some(checkpoint)) => match deserialize_offsets(&checkpoint.data) {
            Some(offsets) => {
                tracing::info!(
                    dataset = %dataset.name,
                    num_offsets = offsets.len(),
                    "Resuming Fluss stream from persisted offsets"
                );
                Some(offsets)
            }
            None => {
                tracing::warn!(
                    dataset = %dataset.name,
                    "Failed to deserialize the persisted Fluss checkpoint, starting from the beginning"
                );
                None
            }
        },
        Ok(None) => None,
        Err(err) => {
            tracing::error!(
                dataset = %dataset.name,
                error = %err,
                "Failed to read the Fluss checkpoint from the accelerator; starting from the beginning"
            );
            None
        }
    };

    let dataset_name = dataset.name.to_string();
    let factory: CommitterFactory = Arc::new(move |offsets| {
        Box::new(FlussStreamCommitter {
            store: Some(Arc::clone(&store)),
            offsets,
            dataset: dataset_name.clone(),
        })
    });

    (initial_offsets, Some(factory))
}

// -- Connector --

pub struct Fluss {
    params: Parameters,
    metrics: Arc<FlussMetrics>,
    /// Captured in `read_provider` for later use by `append_stream`, which
    /// receives no dataset argument.
    dataset: OnceLock<Dataset>,
}

impl std::fmt::Debug for Fluss {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fluss").field("params", &self.params).finish_non_exhaustive()
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct FlussFactory {}

impl FlussFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("bootstrap_servers")
        .required()
        .description("The Fluss coordinator server address (host:port).")
        .examples(&["localhost:9123"])
        .help_link(FLUSS_DOCS),
];

impl DataConnectorFactory for FlussFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let fluss = Fluss {
                params: params.parameters,
                metrics: Arc::new(FlussMetrics::new()),
                dataset: OnceLock::new(),
            };
            Ok(Arc::new(fluss) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        CONNECTOR_NAME
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Fluss {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let refresh_mode = dataset
            .acceleration
            .as_ref()
            .filter(|acceleration| acceleration.enabled)
            .and_then(|acceleration| acceleration.refresh_mode);

        ensure!(
            matches!(
                refresh_mode,
                Some(RefreshMode::Append | RefreshMode::Changes)
            ),
            InvalidConfigurationNoSourceSnafu {
                dataconnector: CONNECTOR_NAME,
                message: format!(
                    "The Fluss connector requires an accelerated dataset with refresh mode 'append' (log tables) or 'changes' (primary-key tables). For details, visit: {FLUSS_DOCS}"
                ),
                connector_component: ConnectorComponent::from(dataset),
            }
        );

        // Captured for append_stream, which receives no dataset argument.
        let _ = self.dataset.set(dataset.clone());

        let bootstrap_servers = self
            .params
            .get("bootstrap_servers")
            .expose()
            .ok()
            .context(MissingBootstrapServersSnafu)
            .boxed()
            .context(UnableToGetReadProviderSnafu {
                dataconnector: CONNECTOR_NAME,
                connector_component: ConnectorComponent::from(dataset),
            })?
            .to_string();

        let table_path_str = dataset.path();
        let (database, table) = table_path_str.split_once('.').ok_or_else(|| {
            DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: CONNECTOR_NAME.to_string(),
                message: format!(
                    "Invalid table path '{table_path_str}'. Expected format: 'database.table'."
                ),
                connector_component: ConnectorComponent::from(dataset),
            }
        })?;
        let table_path = TablePath::new(database, table);

        let mut config = FlussConfig::default();
        config.bootstrap_servers = bootstrap_servers;

        let connection = Arc::new(FlussConnection::new(config).await.boxed().context(
            UnableToGetReadProviderSnafu {
                dataconnector: CONNECTOR_NAME,
                connector_component: ConnectorComponent::from(dataset),
            },
        )?);

        let fluss_provider = FlussTableProvider::new(connection, table_path)
            .await
            .context(UnableToGetReadProviderSnafu {
                dataconnector: CONNECTOR_NAME,
                connector_component: ConnectorComponent::from(dataset),
            })?;

        // A PK table's changelog carries updates/deletes, which append mode
        // would misapply as inserts; a log table has no changelog semantics for
        // changes mode. Reject the mismatches up front.
        let mode_matches_table = match refresh_mode {
            Some(RefreshMode::Changes) => fluss_provider.has_primary_key(),
            _ => !fluss_provider.has_primary_key(),
        };
        ensure!(
            mode_matches_table,
            InvalidConfigurationNoSourceSnafu {
                dataconnector: CONNECTOR_NAME,
                message: if fluss_provider.has_primary_key() {
                    "This Fluss table has a primary key; use refresh_mode: changes."
                } else {
                    "This Fluss table is a log table (no primary key); use refresh_mode: append."
                },
                connector_component: ConnectorComponent::from(dataset),
            }
        );

        Ok(Arc::new(fluss_provider))
    }

    fn supports_append_stream(&self) -> bool {
        true
    }

    fn append_stream(&self, federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        let metrics = Arc::clone(&self.metrics);
        let dataset = self.dataset.get().cloned();
        Some(Box::pin(stream! {
            let table_provider = federated_table.table_provider().await;
            let Some(fluss_provider) = table_provider.downcast_ref::<FlussTableProvider>() else {
                tracing::error!("Failed to downcast TableProvider to FlussTableProvider");
                return;
            };

            let (initial_offsets, committer_factory) = match dataset {
                Some(ref ds) => initialize_checkpoint(ds).await,
                None => (None, None),
            };

            let mut changes_stream = match stream::stream_log_table(
                Arc::clone(fluss_provider.connection()),
                fluss_provider.table_path().clone(),
                metrics,
                initial_offsets,
                committer_factory,
            )
            .await
            {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!("Failed to create Fluss log stream: {e}");
                    yield Err(e);
                    return;
                }
            };

            while let Some(item) = changes_stream.next().await {
                yield item;
            }
        }))
    }

    fn supports_changes_stream(&self) -> bool {
        true
    }

    fn changes_stream(
        &self,
        federated_table: Arc<FederatedTable>,
        dataset: &Dataset,
    ) -> Option<ChangesStream> {
        let metrics = Arc::clone(&self.metrics);
        let dataset = dataset.clone();
        Some(Box::pin(stream! {
            let table_provider = federated_table.table_provider().await;
            let Some(fluss_provider) = table_provider.downcast_ref::<FlussTableProvider>() else {
                tracing::error!("Failed to downcast TableProvider to FlussTableProvider");
                return;
            };

            let (initial_offsets, committer_factory) = initialize_checkpoint(&dataset).await;

            let mut changes_stream = match stream::stream_cdc_table(
                Arc::clone(fluss_provider.connection()),
                fluss_provider.table_path().clone(),
                metrics,
                initial_offsets,
                committer_factory,
            )
            .await
            {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!("Failed to create Fluss CDC stream: {e}");
                    yield Err(e);
                    return;
                }
            };

            while let Some(item) = changes_stream.next().await {
                yield item;
            }
        }))
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        Some(Arc::new(FlussMetricsProvider::new(Arc::clone(
            &self.metrics,
        ))))
    }
}

#[derive(Debug, Clone)]
struct FlussMetricsProvider {
    metrics: Arc<FlussMetrics>,
}

impl FlussMetricsProvider {
    fn new(metrics: Arc<FlussMetrics>) -> Self {
        Self { metrics }
    }
}

const METRICS: &[MetricSpec] = &[
    MetricSpec {
        name: "records_consumed_total",
        description: Some("Total number of records consumed from Fluss"),
        unit: Some("records"),
        metric_type: MetricType::ObservableCounterU64,
        auto_register: false,
    },
    MetricSpec {
        name: "bytes_consumed_total",
        description: Some("Total bytes consumed from Fluss"),
        unit: Some("bytes"),
        metric_type: MetricType::ObservableCounterU64,
        auto_register: false,
    },
    MetricSpec {
        name: "poll_errors_total",
        description: Some("Total poll errors encountered"),
        unit: Some("errors"),
        metric_type: MetricType::ObservableCounterU64,
        auto_register: false,
    },
];

impl MetricsProvider for FlussMetricsProvider {
    fn component_type(&self) -> ComponentType {
        ComponentType::Dataset
    }

    fn component_name(&self) -> &'static str {
        CONNECTOR_NAME
    }

    fn available_metrics(&self) -> &'static [MetricSpec] {
        METRICS
    }

    fn callback_to_observe_metric(
        &self,
        metric: &MetricSpec,
        attributes: Vec<opentelemetry::KeyValue>,
    ) -> Option<ObserveMetricCallback> {
        match metric.name {
            "records_consumed_total" => {
                let metrics = Arc::clone(&self.metrics);
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(metrics.records_consumed.load(Ordering::Relaxed), &attributes);
                })))
            }
            "bytes_consumed_total" => {
                let metrics = Arc::clone(&self.metrics);
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(metrics.bytes_consumed.load(Ordering::Relaxed), &attributes);
                })))
            }
            "poll_errors_total" => {
                let metrics = Arc::clone(&self.metrics);
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(metrics.poll_errors.load(Ordering::Relaxed), &attributes);
                })))
            }
            _ => None,
        }
    }
}

/// Returns a new instance of the Fluss connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    FlussFactory::new_arc()
}

// Self-register into runtime's linkme `DATA_CONNECTOR_REGISTRATIONS` slice. Any binary/tool that
// should see this connector must force-link the crate (`use connector_fluss as _;`) -- a plain
// Cargo dependency won't link the slice static. See `register_data_connector!` docs.
runtime::register_data_connector!(
    register_fluss_connector,
    FLUSS_CONNECTOR_REGISTRATION,
    CONNECTOR_NAME,
    FlussFactory
);
