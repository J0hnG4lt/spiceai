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

use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use futures::TryStreamExt;
use runtime::Runtime;

pub mod bootstrap;

use bootstrap::{
    create_log_table_with_concurrent_data, create_log_table_with_data,
    create_pk_table_with_data, create_pk_table_with_delete, create_pk_table_with_update,
    make_fluss_cdc_dataset, make_fluss_log_dataset, start_fluss_cluster, verify_fluss_ready,
};
use tokio::time::sleep;

use crate::configure_test_datafusion;
use crate::utils::runtime_ready_check;
use crate::{init_tracing, utils::test_request_context};

const FLUSS_COORDINATOR_PORT: u16 = 19123;
const FLUSS_ZK_PORT: u16 = 12181;
const FLUSS_TABLET_PORT: u16 = 19124;

/// Test Fluss log table (append mode) — data is streamed as create operations.
#[tokio::test]
async fn fluss_log_table_append_test() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let cluster = start_fluss_cluster(
                FLUSS_COORDINATOR_PORT,
                FLUSS_ZK_PORT,
                FLUSS_TABLET_PORT,
            )
            .await?;

            tracing::info!("Fluss cluster started");

            // Verify cluster readiness
            verify_fluss_ready(&cluster.bootstrap_servers()).await?;

            // Create log table and insert test data
            create_log_table_with_data(
                &cluster.bootstrap_servers(),
                "testdb",
                "orders",
            )
            .await?;

            // Create Spice dataset pointing to Fluss log table
            let ds = make_fluss_log_dataset(
                "testdb.orders",
                "fluss_orders",
                &cluster.bootstrap_servers(),
            );

            let app = AppBuilder::new("fluss_log_table_test")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(120)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Wait for streaming data to be processed
            sleep(Duration::from_secs(5)).await;

            // Query and verify schema
            run_and_snapshot_query(
                &rt,
                "describe fluss_orders",
                "fluss_log_table_schema",
            )
            .await?;

            // Query and verify data
            run_and_snapshot_query(
                &rt,
                "select * from fluss_orders order by order_id",
                "fluss_log_table_data",
            )
            .await?;

            rt.shutdown().await;
            drop(rt);

            cluster.remove().await.map_err(|e| {
                tracing::error!("cluster.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

/// Test Fluss PK table (changes/CDC mode) — data is streamed with CDC operations.
///
/// Uses a forked fluss-rs with ARROW-format ChangeTypeVector parsing,
/// enabling log scanning for PK tables with `table.log.format = ARROW`.
#[tokio::test]
async fn fluss_pk_table_cdc_test() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    // Use different ports to avoid conflicts with the log table test
    let coordinator_port = FLUSS_COORDINATOR_PORT + 10;
    let zk_port = FLUSS_ZK_PORT + 10;
    let tablet_port = FLUSS_TABLET_PORT + 10;

    test_request_context()
        .scope(async {
            let cluster = start_fluss_cluster(coordinator_port, zk_port, tablet_port).await?;

            tracing::info!("Fluss cluster started for CDC test");

            verify_fluss_ready(&cluster.bootstrap_servers()).await?;

            // Create PK table and insert test data
            create_pk_table_with_data(
                &cluster.bootstrap_servers(),
                "testdb",
                "users",
            )
            .await?;

            // Create Spice dataset pointing to Fluss PK table (CDC mode)
            let ds = make_fluss_cdc_dataset(
                "testdb.users",
                "fluss_users",
                &cluster.bootstrap_servers(),
            );

            let app = AppBuilder::new("fluss_pk_table_test")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(120)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Wait for streaming CDC data to be processed
            sleep(Duration::from_secs(8)).await;

            // Query and verify schema
            run_and_snapshot_query(
                &rt,
                "describe fluss_users",
                "fluss_pk_table_schema",
            )
            .await?;

            // Query and verify data
            run_and_snapshot_query(
                &rt,
                "select * from fluss_users order by user_id",
                "fluss_pk_table_data",
            )
            .await?;

            rt.shutdown().await;
            drop(rt);

            cluster.remove().await.map_err(|e| {
                tracing::error!("cluster.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

/// Test Fluss PK table CDC with an update operation.
///
/// Inserts 2 users, then updates user_id=1. The CDC stream should process
/// Insert + UpdateBefore/UpdateAfter events, resulting in the final state
/// where user_id=1 has the updated name and score.
#[tokio::test]
async fn fluss_pk_table_update_test() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let coordinator_port = FLUSS_COORDINATOR_PORT + 20;
    let zk_port = FLUSS_ZK_PORT + 20;
    let tablet_port = FLUSS_TABLET_PORT + 20;

    test_request_context()
        .scope(async {
            let cluster = start_fluss_cluster(coordinator_port, zk_port, tablet_port).await?;

            tracing::info!("Fluss cluster started for update test");

            verify_fluss_ready(&cluster.bootstrap_servers()).await?;

            create_pk_table_with_update(
                &cluster.bootstrap_servers(),
                "testdb",
                "users_update",
            )
            .await?;

            let ds = make_fluss_cdc_dataset(
                "testdb.users_update",
                "fluss_users_update",
                &cluster.bootstrap_servers(),
            );

            let app = AppBuilder::new("fluss_pk_update_test")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(120)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Wait for CDC events to be fully processed
            sleep(Duration::from_secs(10)).await;

            // Verify the update is reflected: user_id=1 should have updated name/score
            run_and_snapshot_query(
                &rt,
                "select * from fluss_users_update order by user_id",
                "fluss_pk_table_update_data",
            )
            .await?;

            rt.shutdown().await;
            drop(rt);

            cluster.remove().await.map_err(|e| {
                tracing::error!("cluster.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

/// Test Fluss PK table CDC with a delete operation.
///
/// Inserts 3 users, then deletes user_id=2. The CDC stream should process
/// Insert + Delete events, resulting in only user_id=1 and user_id=3 remaining.
#[tokio::test]
async fn fluss_pk_table_delete_test() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let coordinator_port = FLUSS_COORDINATOR_PORT + 30;
    let zk_port = FLUSS_ZK_PORT + 30;
    let tablet_port = FLUSS_TABLET_PORT + 30;

    test_request_context()
        .scope(async {
            let cluster = start_fluss_cluster(coordinator_port, zk_port, tablet_port).await?;

            tracing::info!("Fluss cluster started for delete test");

            verify_fluss_ready(&cluster.bootstrap_servers()).await?;

            create_pk_table_with_delete(
                &cluster.bootstrap_servers(),
                "testdb",
                "users_delete",
            )
            .await?;

            let ds = make_fluss_cdc_dataset(
                "testdb.users_delete",
                "fluss_users_delete",
                &cluster.bootstrap_servers(),
            );

            let app = AppBuilder::new("fluss_pk_delete_test")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(120)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Wait for CDC events to be fully processed
            sleep(Duration::from_secs(10)).await;

            // Verify the delete: only user_id=1 and user_id=3 should remain
            run_and_snapshot_query(
                &rt,
                "select * from fluss_users_delete order by user_id",
                "fluss_pk_table_delete_data",
            )
            .await?;

            rt.shutdown().await;
            drop(rt);

            cluster.remove().await.map_err(|e| {
                tracing::error!("cluster.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

/// Test Fluss log table with concurrent writers.
///
/// Spawns multiple tasks that concurrently write to the same log table,
/// verifying that all records are eventually available through SpiceAI.
#[tokio::test]
async fn fluss_log_table_concurrent_writes_test() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let coordinator_port = FLUSS_COORDINATOR_PORT + 40;
    let zk_port = FLUSS_ZK_PORT + 40;
    let tablet_port = FLUSS_TABLET_PORT + 40;

    test_request_context()
        .scope(async {
            let cluster = start_fluss_cluster(coordinator_port, zk_port, tablet_port).await?;

            tracing::info!("Fluss cluster started for concurrent writes test");

            verify_fluss_ready(&cluster.bootstrap_servers()).await?;

            let num_writers = 3;
            let rows_per_writer = 5;
            let total_expected = num_writers * rows_per_writer;

            create_log_table_with_concurrent_data(
                &cluster.bootstrap_servers(),
                "testdb",
                "concurrent_orders",
                num_writers,
                rows_per_writer,
            )
            .await?;

            let ds = make_fluss_log_dataset(
                "testdb.concurrent_orders",
                "fluss_concurrent_orders",
                &cluster.bootstrap_servers(),
            );

            let app = AppBuilder::new("fluss_concurrent_writes_test")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(120)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Wait for streaming data to be processed
            sleep(Duration::from_secs(10)).await;

            // Verify total row count
            let query_result = rt
                .datafusion()
                .query_builder("select count(*) as cnt from fluss_concurrent_orders")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            let data = query_result
                .data
                .try_collect::<Vec<_>>()
                .await?;

            let count = data[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("count should be Int64")
                .value(0);

            assert_eq!(
                count, total_expected as i64,
                "Expected {total_expected} rows from concurrent writes, got {count}"
            );

            tracing::info!(
                "Concurrent writes: verified {count} rows from {num_writers} writers"
            );

            rt.shutdown().await;
            drop(rt);

            cluster.remove().await.map_err(|e| {
                tracing::error!("cluster.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

async fn run_and_snapshot_query(
    rt: &Runtime,
    query: &str,
    test_name: &str,
) -> Result<(), anyhow::Error> {
    let query_result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let data = query_result.data.try_collect::<Vec<_>>().await?;

    let formatted = arrow::util::pretty::pretty_format_batches(&data)
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;
    insta::assert_snapshot!(test_name, formatted);
    Ok(())
}
