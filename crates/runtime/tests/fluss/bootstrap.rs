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
use std::time::Duration;

use fluss::client::FlussConnection;
use fluss::config::Config as FlussConfig;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::GenericRow;
use spicepod::acceleration::{Acceleration, RefreshMode};
use spicepod::component::dataset::Dataset;
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

const FLUSS_IMAGE: &str = "apache/fluss:0.8.0-incubating";
const ZOOKEEPER_IMAGE: &str = "zookeeper:3.9";

/// Running Fluss cluster (3 containers with host networking).
pub struct FlussCluster {
    pub zk: RunningContainer<'static>,
    pub coordinator: RunningContainer<'static>,
    pub tablet: RunningContainer<'static>,
    pub coordinator_port: u16,
}

impl FlussCluster {
    pub async fn remove(&self) -> Result<(), anyhow::Error> {
        // Remove containers in reverse order
        self.tablet.remove().await?;
        self.coordinator.remove().await?;
        self.zk.remove().await?;
        Ok(())
    }

    pub fn bootstrap_servers(&self) -> String {
        format!("localhost:{}", self.coordinator_port)
    }
}

/// Start a Fluss cluster with ZooKeeper, Coordinator, and TabletServer
/// using host networking.
///
/// Host networking avoids custom bridge networks (which require aardvark-dns
/// and may not work in rootless podman without systemd). All containers share
/// the host's network namespace and communicate via `localhost:<port>`.
#[instrument]
pub async fn start_fluss_cluster(
    coordinator_port: u16,
    zk_port: u16,
    tablet_port: u16,
) -> Result<FlussCluster, anyhow::Error> {
    let zk_name = format!("fluss-test-zk-{zk_port}");
    let zk_name: &'static str = Box::leak(zk_name.into_boxed_str());

    let coord_name = format!("fluss-test-coord-{coordinator_port}");
    let coord_name: &'static str = Box::leak(coord_name.into_boxed_str());

    let tablet_name = format!("fluss-test-tablet-{tablet_port}");
    let tablet_name: &'static str = Box::leak(tablet_name.into_boxed_str());

    // 1. Start ZooKeeper with host networking
    //    ZK binds to zk_port on the host. We must use ZOO_SERVERS (not
    //    ZOO_CLIENT_PORT) because the entrypoint hardcodes the client port
    //    in `server.1=...;2181` and ignores ZOO_CLIENT_PORT.
    //    Peer/election ports are derived from zk_port to avoid conflicts.
    let zk_peer_port = zk_port + 700; // e.g. 12181 → 12881
    let zk_election_port = zk_peer_port + 1000; // e.g. 12881 → 13881
    let zk = ContainerRunnerBuilder::new(zk_name)
        .image(ZOOKEEPER_IMAGE.to_string())
        .host_network()
        .add_env_var("ZOO_4LW_COMMANDS_WHITELIST", "ruok")
        .add_env_var(
            "ZOO_SERVERS",
            &format!("server.1=localhost:{zk_peer_port}:{zk_election_port};{zk_port}"),
        )
        .add_env_var("ZOO_ADMINSERVER_ENABLED", "false")
        // ZK image defines VOLUME [/data, /datalog, /logs]. Mount tmpfs to
        // prevent anonymous volume creation (podman API bug: _data dirs missing).
        .add_tmpfs("/data")
        .add_tmpfs("/datalog")
        .add_tmpfs("/logs")
        .build()?
        .run(Some(Duration::from_secs(60)))
        .await?;

    tracing::info!("ZooKeeper started on port {zk_port}");

    // 2. Start Fluss Coordinator with host networking
    //    Connects to ZK via localhost:{zk_port}
    //    Binds to coordinator_port on host
    let coordinator = ContainerRunnerBuilder::new(coord_name)
        .image(FLUSS_IMAGE.to_string())
        .host_network()
        .command(["coordinatorServer"])
        .add_env_var(
            "FLUSS_PROPERTIES",
            &format!(
                "zookeeper.address: localhost:{zk_port}\n\
                 coordinator.host: localhost\n\
                 coordinator.port: {coordinator_port}\n\
                 remote.data.dir: /tmp/fluss/remote-data"
            ),
        )
        .build()?
        .run(Some(Duration::from_secs(90)))
        .await?;

    tracing::info!("Fluss Coordinator started on port {coordinator_port}");

    // 3. Start Fluss TabletServer with host networking
    //    Connects to ZK via localhost:{zk_port}
    //    Binds to tablet_port on host
    let tablet = ContainerRunnerBuilder::new(tablet_name)
        .image(FLUSS_IMAGE.to_string())
        .host_network()
        .command(["tabletServer"])
        .add_env_var(
            "FLUSS_PROPERTIES",
            &format!(
                "zookeeper.address: localhost:{zk_port}\n\
                 tablet-server.host: localhost\n\
                 tablet-server.port: {tablet_port}\n\
                 tablet-server.id: 0\n\
                 data.dir: /tmp/fluss/data\n\
                 remote.data.dir: /tmp/fluss/remote-data"
            ),
        )
        .build()?
        .run(Some(Duration::from_secs(90)))
        .await?;

    tracing::info!("Fluss TabletServer started on port {tablet_port}");

    // Wait for the cluster to stabilize
    tokio::time::sleep(Duration::from_secs(10)).await;

    Ok(FlussCluster {
        zk,
        coordinator,
        tablet,
        coordinator_port,
    })
}

/// Verify the Fluss cluster is ready by connecting and creating a test database.
pub async fn verify_fluss_ready(bootstrap_servers: &str) -> Result<(), anyhow::Error> {
    const MAX_RETRIES: u32 = 30;
    const RETRY_DELAY: Duration = Duration::from_secs(2);

    for attempt in 1..=MAX_RETRIES {
        match try_connect_fluss(bootstrap_servers).await {
            Ok(()) => {
                tracing::info!("Fluss cluster ready (attempt {attempt})");
                return Ok(());
            }
            Err(e) => {
                tracing::debug!(
                    "Fluss not ready (attempt {}/{MAX_RETRIES}): {e}",
                    attempt,
                );
                if attempt < MAX_RETRIES {
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        }
    }

    Err(anyhow::anyhow!(
        "Failed to verify Fluss readiness after {MAX_RETRIES} attempts"
    ))
}

async fn try_connect_fluss(bootstrap_servers: &str) -> Result<(), anyhow::Error> {
    let mut config = FlussConfig::default();
    config.bootstrap_servers = bootstrap_servers.to_string();
    let connection = FlussConnection::new(config).await?;
    let admin = connection.get_admin().await?;
    admin
        .create_database("_fluss_test_probe", None, true)
        .await?;
    Ok(())
}

/// Create a Fluss log table (no primary key) and insert test data.
pub async fn create_log_table_with_data(
    bootstrap_servers: &str,
    database: &str,
    table_name: &str,
) -> Result<(), anyhow::Error> {
    let mut config = FlussConfig::default();
    config.bootstrap_servers = bootstrap_servers.to_string();
    let connection = FlussConnection::new(config).await?;
    let admin = connection.get_admin().await?;

    admin.create_database(database, None, true).await?;

    let schema = Schema::builder()
        .column("order_id", DataTypes::int())
        .column("product", DataTypes::string())
        .column("quantity", DataTypes::int())
        .column("price", DataTypes::float())
        .build()?;

    let descriptor = TableDescriptor::builder()
        .schema(schema)
        .distributed_by(Some(1), vec![]) // 1 bucket, no bucket keys for log tables
        .build()?;

    let table_path = TablePath::new(database, table_name);
    admin.create_table(&table_path, &descriptor, true).await?;

    let table = connection.get_table(&table_path).await?;
    let append = table.new_append()?;
    let writer = append.create_writer()?;

    let orders = vec![
        (1i32, "Widget A", 10i32, 9.99f32),
        (2, "Widget B", 5, 19.99),
        (3, "Gadget C", 3, 49.99),
        (4, "Widget A", 7, 9.99),
        (5, "Gadget D", 1, 99.99),
    ];

    for (id, product, qty, price) in &orders {
        let mut row = GenericRow::new(4);
        row.set_field(0, *id);
        row.set_field(1, product.to_string());
        row.set_field(2, *qty);
        row.set_field(3, *price);
        writer.append(&row)?;
    }
    writer.flush().await?;

    tracing::info!(
        "Inserted {} rows into {}.{}",
        orders.len(),
        database,
        table_name
    );

    Ok(())
}

/// Create a Fluss PK table (with primary key) and insert test data for CDC.
pub async fn create_pk_table_with_data(
    bootstrap_servers: &str,
    database: &str,
    table_name: &str,
) -> Result<(), anyhow::Error> {
    let mut config = FlussConfig::default();
    config.bootstrap_servers = bootstrap_servers.to_string();
    let connection = FlussConnection::new(config).await?;
    let admin = connection.get_admin().await?;

    admin.create_database(database, None, true).await?;

    let schema = Schema::builder()
        .column("user_id", DataTypes::int())
        .column("name", DataTypes::string())
        .column("email", DataTypes::string())
        .column("score", DataTypes::int())
        .primary_key(vec!["user_id"])
        .build()?;

    let descriptor = TableDescriptor::builder()
        .schema(schema)
        .distributed_by(Some(1), vec!["user_id".to_string()])
        // PK tables default to INDEXED log format; we need ARROW for our
        // forked fluss-rs client which only supports ARROW format scanning.
        .property("table.log.format", "ARROW")
        .build()?;

    let table_path = TablePath::new(database, table_name);
    admin.create_table(&table_path, &descriptor, true).await?;

    let table = connection.get_table(&table_path).await?;
    let upsert = table.new_upsert()?;
    let writer = upsert.create_writer()?;

    let users = vec![
        (1i32, "Alice", "alice@example.com", 100i32),
        (2, "Bob", "bob@example.com", 200),
        (3, "Charlie", "charlie@example.com", 150),
    ];

    for (id, name, email, score) in &users {
        let mut row = GenericRow::new(4);
        row.set_field(0, *id);
        row.set_field(1, name.to_string());
        row.set_field(2, email.to_string());
        row.set_field(3, *score);
        writer.upsert(&row)?;
    }
    writer.flush().await?;

    tracing::info!(
        "Inserted {} rows into {}.{}",
        users.len(),
        database,
        table_name
    );

    Ok(())
}

/// Create a Fluss PK table, insert initial data, then update a row via upsert.
///
/// This tests CDC update propagation: after the initial insert, the same primary
/// key (user_id=1) is upserted with a new name and score, producing
/// UpdateBefore/UpdateAfter CDC events.
pub async fn create_pk_table_with_update(
    bootstrap_servers: &str,
    database: &str,
    table_name: &str,
) -> Result<(), anyhow::Error> {
    let mut config = FlussConfig::default();
    config.bootstrap_servers = bootstrap_servers.to_string();
    let connection = FlussConnection::new(config).await?;
    let admin = connection.get_admin().await?;

    admin.create_database(database, None, true).await?;

    let schema = Schema::builder()
        .column("user_id", DataTypes::int())
        .column("name", DataTypes::string())
        .column("email", DataTypes::string())
        .column("score", DataTypes::int())
        .primary_key(vec!["user_id"])
        .build()?;

    let descriptor = TableDescriptor::builder()
        .schema(schema)
        .distributed_by(Some(1), vec!["user_id".to_string()])
        .property("table.log.format", "ARROW")
        .build()?;

    let table_path = TablePath::new(database, table_name);
    admin.create_table(&table_path, &descriptor, true).await?;

    let table = connection.get_table(&table_path).await?;
    let upsert = table.new_upsert()?;
    let writer = upsert.create_writer()?;

    // Initial inserts
    let users = vec![
        (1i32, "Alice", "alice@example.com", 100i32),
        (2, "Bob", "bob@example.com", 200),
    ];

    for (id, name, email, score) in &users {
        let mut row = GenericRow::new(4);
        row.set_field(0, *id);
        row.set_field(1, name.to_string());
        row.set_field(2, email.to_string());
        row.set_field(3, *score);
        writer.upsert(&row)?;
    }
    writer.flush().await?;

    // Wait for initial data to be committed
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Update: upsert user_id=1 with new name and score
    let mut row = GenericRow::new(4);
    row.set_field(0, 1i32);
    row.set_field(1, "Alice Updated".to_string());
    row.set_field(2, "alice@example.com".to_string());
    row.set_field(3, 999i32);
    writer.upsert(&row)?;
    writer.flush().await?;

    tracing::info!("Inserted 2 rows, then updated user_id=1 in {database}.{table_name}");

    Ok(())
}

/// Create a Fluss PK table, insert initial data, then delete a row.
///
/// This tests CDC delete propagation: after the initial insert, user_id=2
/// is deleted via `UpsertWriter::delete()`.
pub async fn create_pk_table_with_delete(
    bootstrap_servers: &str,
    database: &str,
    table_name: &str,
) -> Result<(), anyhow::Error> {
    let mut config = FlussConfig::default();
    config.bootstrap_servers = bootstrap_servers.to_string();
    let connection = FlussConnection::new(config).await?;
    let admin = connection.get_admin().await?;

    admin.create_database(database, None, true).await?;

    let schema = Schema::builder()
        .column("user_id", DataTypes::int())
        .column("name", DataTypes::string())
        .column("email", DataTypes::string())
        .column("score", DataTypes::int())
        .primary_key(vec!["user_id"])
        .build()?;

    let descriptor = TableDescriptor::builder()
        .schema(schema)
        .distributed_by(Some(1), vec!["user_id".to_string()])
        .property("table.log.format", "ARROW")
        .build()?;

    let table_path = TablePath::new(database, table_name);
    admin.create_table(&table_path, &descriptor, true).await?;

    let table = connection.get_table(&table_path).await?;
    let upsert = table.new_upsert()?;
    let writer = upsert.create_writer()?;

    // Initial inserts
    let users = vec![
        (1i32, "Alice", "alice@example.com", 100i32),
        (2, "Bob", "bob@example.com", 200),
        (3, "Charlie", "charlie@example.com", 150),
    ];

    for (id, name, email, score) in &users {
        let mut row = GenericRow::new(4);
        row.set_field(0, *id);
        row.set_field(1, name.to_string());
        row.set_field(2, email.to_string());
        row.set_field(3, *score);
        writer.upsert(&row)?;
    }
    writer.flush().await?;

    // Wait for initial data to be committed
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Delete user_id=2
    let mut delete_row = GenericRow::new(4);
    delete_row.set_field(0, 2i32);
    delete_row.set_field(1, "Bob".to_string());
    delete_row.set_field(2, "bob@example.com".to_string());
    delete_row.set_field(3, 200i32);
    writer.delete(&delete_row)?;
    writer.flush().await?;

    tracing::info!("Inserted 3 rows, then deleted user_id=2 in {database}.{table_name}");

    Ok(())
}

/// Create a Fluss log table and concurrently insert data from multiple writers.
///
/// Spawns `num_writers` tasks that each insert `rows_per_writer` rows,
/// verifying that the Fluss cluster handles concurrent appends correctly.
pub async fn create_log_table_with_concurrent_data(
    bootstrap_servers: &str,
    database: &str,
    table_name: &str,
    num_writers: usize,
    rows_per_writer: usize,
) -> Result<(), anyhow::Error> {
    let mut config = FlussConfig::default();
    config.bootstrap_servers = bootstrap_servers.to_string();
    let connection = FlussConnection::new(config).await?;
    let admin = connection.get_admin().await?;

    admin.create_database(database, None, true).await?;

    let schema = Schema::builder()
        .column("writer_id", DataTypes::int())
        .column("seq", DataTypes::int())
        .column("payload", DataTypes::string())
        .build()?;

    let descriptor = TableDescriptor::builder()
        .schema(schema)
        .distributed_by(Some(1), vec![])
        .build()?;

    let table_path = TablePath::new(database, table_name);
    admin.create_table(&table_path, &descriptor, true).await?;

    // Spawn concurrent writers
    let mut handles = Vec::new();
    for writer_id in 0..num_writers {
        let bs = bootstrap_servers.to_string();
        let db = database.to_string();
        let tbl = table_name.to_string();
        handles.push(tokio::spawn(async move {
            let mut cfg = FlussConfig::default();
            cfg.bootstrap_servers = bs;
            let conn = FlussConnection::new(cfg).await?;
            let tp = TablePath::new(&db, &tbl);
            let table = conn.get_table(&tp).await?;
            let append = table.new_append()?;
            let writer = append.create_writer()?;

            for seq in 0..rows_per_writer {
                let mut row = GenericRow::new(3);
                row.set_field(0, writer_id as i32);
                row.set_field(1, seq as i32);
                row.set_field(2, format!("w{writer_id}-s{seq}"));
                writer.append(&row)?;
            }
            writer.flush().await?;
            Ok::<_, anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    let total = num_writers * rows_per_writer;
    tracing::info!("Concurrent insert complete: {num_writers} writers × {rows_per_writer} rows = {total} total in {database}.{table_name}");

    Ok(())
}

/// Create a Spice AI dataset configured for Fluss log table (append mode).
pub fn make_fluss_log_dataset(
    path: &str,
    name: &str,
    bootstrap_servers: &str,
) -> Dataset {
    let params = HashMap::from([(
        "fluss_bootstrap_servers".to_string(),
        bootstrap_servers.to_string(),
    )]);

    let mut dataset = Dataset::new(format!("fluss:{path}"), name.to_string());
    dataset.params = Some(spicepod::param::Params::from_string_map(params));

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        refresh_mode: Some(RefreshMode::Append),
        ..Default::default()
    });

    dataset
}

/// Create a Spice AI dataset configured for Fluss PK table (changes mode).
pub fn make_fluss_cdc_dataset(
    path: &str,
    name: &str,
    bootstrap_servers: &str,
) -> Dataset {
    let params = HashMap::from([(
        "fluss_bootstrap_servers".to_string(),
        bootstrap_servers.to_string(),
    )]);

    let mut dataset = Dataset::new(format!("fluss:{path}"), name.to_string());
    dataset.params = Some(spicepod::param::Params::from_string_map(params));

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        refresh_mode: Some(RefreshMode::Changes),
        ..Default::default()
    });

    dataset
}
