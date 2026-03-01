/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::collections::HashMap;
use std::sync::OnceLock;

use async_stream::stream;
use data_components::cdc::{ChangesStream, CommitChange, CommitError};
use data_components::fluss::stream::{CommitterFactory, OffsetKey};
use data_components::fluss::{FlussMetrics, FlussTableProvider};
use datafusion::datasource::TableProvider;
use fluss::client::FlussConnection;
use fluss::config::Config as FlussConfig;
use fluss::metadata::TablePath;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::sync::atomic::Ordering;
use std::{any::Any, future::Future, pin::Pin, sync::Arc};
use tokio::sync::Mutex;

use crate::{
    component::{
        ComponentType,
        dataset::{Dataset, acceleration::RefreshMode},
        metrics::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback},
    },
    dataaccelerator::spice_sys::{
        OpenOption,
        fluss::{FlussCheckpointMetadata, FlussSys},
    },
    dataconnector::{
        ConnectorComponent, DataConnector, DataConnectorFactory, parameters::ConnectorParams,
    },
    federated_table::FederatedTable,
    parameters::{ParameterSpec, Parameters},
    register_data_connector,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: 'bootstrap_servers'. Specify the Fluss coordinator address."
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

fn serialize_offsets(offsets: &HashMap<OffsetKey, i64>) -> String {
    let entries: Vec<FlussOffsetEntry> = offsets
        .iter()
        .map(|((pid, bucket), &offset)| FlussOffsetEntry {
            partition_id: *pid,
            bucket_id: *bucket,
            offset,
        })
        .collect();
    serde_json::to_string(&FlussCheckpoint { offsets: entries }).unwrap_or_default()
}

fn deserialize_offsets(data: &str) -> Option<HashMap<OffsetKey, i64>> {
    let checkpoint: FlussCheckpoint = serde_json::from_str(data).ok()?;
    let map = checkpoint
        .offsets
        .into_iter()
        .map(|e| ((e.partition_id, e.bucket_id), e.offset))
        .collect();
    Some(map)
}

// -- Committer --

struct FlussStreamCommitter {
    fluss_sys: Arc<FlussSys>,
    offsets: HashMap<OffsetKey, i64>,
}

impl CommitChange for FlussStreamCommitter {
    fn commit(&self) -> std::result::Result<(), CommitError> {
        let checkpoint_data = serialize_offsets(&self.offsets);
        let metadata = FlussCheckpointMetadata {
            checkpoint_data,
            updated_at: None,
        };

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.fluss_sys.upsert(&metadata).await.map_err(|e| {
                    CommitError::UnableToCommitChange {
                        source: Box::new(e),
                    }
                })
            })
        })
    }
}

/// Initialize the `FlussSys` checkpoint storage and load any existing offsets.
///
/// Returns `(initial_offsets, committer_factory)` if storage is available.
async fn initialize_checkpoint(
    dataset: &Dataset,
) -> Option<(Option<HashMap<OffsetKey, i64>>, CommitterFactory)> {
    let fluss_sys = match FlussSys::try_new(dataset, OpenOption::CreateIfNotExists).await {
        Ok(sys) => Arc::new(sys),
        Err(err) => {
            tracing::warn!(
                dataset = %dataset.name,
                error = ?err,
                "Failed to initialize Fluss checkpoint storage. Offsets will not be persisted."
            );
            return None;
        }
    };

    // Load existing checkpoint offsets.
    let initial_offsets = match fluss_sys.get().await {
        Some(metadata) => {
            match deserialize_offsets(&metadata.checkpoint_data) {
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
                        "Failed to deserialize Fluss checkpoint data, starting from beginning"
                    );
                    None
                }
            }
        }
        None => None,
    };

    // Create the committer factory closure.
    let factory: CommitterFactory = Arc::new(move |offsets| {
        Box::new(FlussStreamCommitter {
            fluss_sys: Arc::clone(&fluss_sys),
            offsets,
        })
    });

    Some((initial_offsets, factory))
}

// -- Connector --

pub struct Fluss {
    params: Parameters,
    metrics: Arc<FlussMetrics>,
    dataset: OnceLock<Dataset>,
}

impl std::fmt::Debug for Fluss {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fluss")
            .field("params", &self.params)
            .field("metrics", &self.metrics)
            .finish()
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
        .description("The Fluss coordinator server address (host:port)."),
];

impl DataConnectorFactory for FlussFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
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
        "fluss"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[tonic::async_trait]
impl DataConnector for Fluss {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        ensure!(
            dataset.is_accelerated(),
            super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "fluss",
                message: "The Fluss data connector requires an accelerated dataset.",
                connector_component: ConnectorComponent::from(dataset),
            }
        );

        let Some(ref acceleration) = dataset.acceleration else {
            unreachable!("Dataset acceleration already verified.");
        };

        let is_append = acceleration.refresh_mode == Some(RefreshMode::Append);
        let is_changes = acceleration.refresh_mode == Some(RefreshMode::Changes);

        ensure!(
            is_append || is_changes,
            super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "fluss",
                message: "The Fluss connector requires refresh mode 'append' (for log tables) or 'changes' (for primary-key tables).",
                connector_component: ConnectorComponent::from(dataset),
            }
        );

        // Store dataset for later use in append_stream/changes_stream.
        let _ = self.dataset.set(dataset.clone());

        let bootstrap_servers = self
            .params
            .get("bootstrap_servers")
            .expose()
            .ok()
            .context(MissingBootstrapServersSnafu)
            .map_err(|e| super::DataConnectorError::UnableToConnectInternal {
                dataconnector: "fluss".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            })?
            .to_string();

        let table_path_str = dataset.path();

        let mut config = FlussConfig::default();
        config.bootstrap_servers = bootstrap_servers;

        let connection = Arc::new(
            FlussConnection::new(config)
                .await
                .map_err(|e| super::DataConnectorError::UnableToConnectInternal {
                    dataconnector: "fluss".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                })?,
        );

        // Parse "database.table" path format
        let (database, table) = table_path_str.split_once('.').ok_or_else(|| {
            super::DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "fluss".to_string(),
                message: format!(
                    "Invalid table path '{table_path_str}'. Expected format: 'database.table'."
                ),
                connector_component: ConnectorComponent::from(dataset),
            }
        })?;
        let table_path = TablePath::new(database, table);

        let provider = FlussTableProvider::new(connection, table_path)
            .await
            .map_err(|e| super::DataConnectorError::UnableToConnectInternal {
                dataconnector: "fluss".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            })?;

        Ok(Arc::new(provider))
    }

    fn supports_append_stream(&self) -> bool {
        true
    }

    fn append_stream(&self, federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        let metrics = Arc::clone(&self.metrics);
        let dataset = self.dataset.get().cloned();
        Some(Box::pin(stream! {
            let table_provider = federated_table.table_provider().await;
            let Some(fluss_provider) = table_provider
                .as_any()
                .downcast_ref::<FlussTableProvider>()
            else {
                tracing::error!("Failed to downcast TableProvider to FlussTableProvider");
                return;
            };

            let connection = fluss_provider.connection();
            let table_path = fluss_provider.table_path();

            // Initialize checkpoint storage if dataset info is available.
            let (initial_offsets, committer_factory) = if let Some(ref ds) = dataset {
                match initialize_checkpoint(ds).await {
                    Some((offsets, factory)) => (offsets, Some(factory)),
                    None => (None, None),
                }
            } else {
                (None, None)
            };

            let mut changes_stream = match data_components::fluss::stream::stream_log_table(
                connection,
                table_path,
                Arc::clone(&metrics),
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

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        Some(Arc::new(FlussMetricsProvider::new(Arc::clone(
            &self.metrics,
        ))))
    }

    fn supports_changes_stream(&self) -> bool {
        true
    }

    fn changes_stream(
        &self,
        federated_table: Arc<FederatedTable>,
        dataset: &Dataset,
        _accelerated_table_provider: Arc<dyn TableProvider>,
        _accelerator_write_mutex: Arc<Mutex<()>>,
    ) -> Option<ChangesStream> {
        let metrics = Arc::clone(&self.metrics);
        let dataset = dataset.clone();
        Some(Box::pin(stream! {
            let table_provider = federated_table.table_provider().await;
            let Some(fluss_provider) = table_provider
                .as_any()
                .downcast_ref::<FlussTableProvider>()
            else {
                tracing::error!("Failed to downcast TableProvider to FlussTableProvider");
                return;
            };

            let connection = fluss_provider.connection();
            let table_path = fluss_provider.table_path();

            // Initialize checkpoint storage for CDC stream.
            let (initial_offsets, committer_factory) =
                match initialize_checkpoint(&dataset).await {
                    Some((offsets, factory)) => (offsets, Some(factory)),
                    None => (None, None),
                };

            let mut changes_stream = match data_components::fluss::stream::stream_cdc_table(
                connection,
                table_path,
                Arc::clone(&metrics),
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
}

register_data_connector!("fluss", FlussFactory);

#[derive(Debug, Clone)]
struct FlussMetricsProvider {
    metrics: Arc<FlussMetrics>,
}

impl FlussMetricsProvider {
    fn new(metrics: Arc<FlussMetrics>) -> Self {
        Self { metrics }
    }
}

const FLUSS_METRICS: &[MetricSpec] = &[
    MetricSpec {
        name: "records_consumed_total",
        description: Some("Total number of records consumed from Fluss"),
        unit: Some("records"),
        metric_type: MetricType::ObservableCounterU64,
    },
    MetricSpec {
        name: "bytes_consumed_total",
        description: Some("Total bytes consumed from Fluss"),
        unit: Some("bytes"),
        metric_type: MetricType::ObservableCounterU64,
    },
    MetricSpec {
        name: "poll_errors_total",
        description: Some("Total poll errors encountered"),
        unit: Some("errors"),
        metric_type: MetricType::ObservableCounterU64,
    },
];

impl MetricsProvider for FlussMetricsProvider {
    fn component_type(&self) -> ComponentType {
        ComponentType::Dataset
    }

    fn component_name(&self) -> &'static str {
        "fluss"
    }

    fn available_metrics(&self) -> &'static [MetricSpec] {
        FLUSS_METRICS
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
                    observer.observe(
                        metrics.records_consumed.load(Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "bytes_consumed_total" => {
                let metrics = Arc::clone(&self.metrics);
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics.bytes_consumed.load(Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "poll_errors_total" => {
                let metrics = Arc::clone(&self.metrics);
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics.poll_errors.load(Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            _ => None,
        }
    }
}
