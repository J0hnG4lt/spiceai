//! Deterministic Fluss data producer for the SpiceAI connector-fluss E2E suite.
//!
//! Every workload is deterministic so `run-e2e.sh` can assert exact counts and
//! values through the SpiceAI SQL API:
//!
//! - `orders` (log table): `order_id` = start_id..start_id+count,
//!   `customer` = "customer_<id % 10>", `amount` = id * 1.5
//! - `users` (PK table): insert `user_id` = start_id..start_id+inserts with
//!   `name` = "user_<id>", `score` = id * 10.0; updates rewrite the FIRST
//!   `updates` ids with `name` = "user_<id>_v2", `score` = id * 10.0 + 1000.0;
//!   deletes remove the LAST `deletes` ids of the inserted range.
//! - `mixed` runs a continuous append + upsert workload with retry/reconnect,
//!   for fault-tolerance and chaos scenarios. It never panics on transient
//!   cluster errors; it reconnects and keeps a monotonic cursor so produced
//!   ids stay unique across retries.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use fluss::client::FlussConnection;
use fluss::config::Config as FlussConfig;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::GenericRow;

const DATABASE: &str = "testdb";
const ORDERS_TABLE: &str = "orders";
const USERS_TABLE: &str = "users";

#[derive(Parser)]
#[command(name = "fluss-producer", about = "Deterministic Fluss data producer for E2E tests")]
struct Cli {
    /// Fluss coordinator address (host:port). Falls back to FLUSS_BOOTSTRAP_SERVERS.
    #[arg(long, global = true)]
    bootstrap_servers: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create the test database and tables (idempotent).
    Setup {
        /// Number of buckets for each table.
        #[arg(long, default_value_t = 3)]
        buckets: i32,
    },
    /// Append `count` orders starting at `start_id` to the log table.
    Append {
        #[arg(long, default_value_t = 100)]
        count: i64,
        #[arg(long, default_value_t = 0)]
        start_id: i64,
        /// Rows per second (0 = as fast as possible).
        #[arg(long, default_value_t = 0)]
        rate: u64,
    },
    /// Deterministic CDC workload on the PK table: inserts, then updates, then deletes.
    Cdc {
        #[arg(long, default_value_t = 50)]
        inserts: i32,
        #[arg(long, default_value_t = 0)]
        updates: i32,
        #[arg(long, default_value_t = 0)]
        deletes: i32,
        #[arg(long, default_value_t = 1)]
        start_id: i32,
    },
    /// Continuous mixed workload with retry/reconnect for chaos scenarios.
    Mixed {
        /// How long to keep producing.
        #[arg(long, default_value_t = 60)]
        duration_secs: u64,
        /// Order appends per second.
        #[arg(long, default_value_t = 20)]
        rate: u64,
        /// First order_id to use (ids increase monotonically from here).
        #[arg(long, default_value_t = 1_000_000)]
        start_id: i64,
    },
}

fn connection_config(bootstrap: &str) -> FlussConfig {
    let mut config = FlussConfig::default();
    config.bootstrap_servers = bootstrap.to_string();
    config
}

async fn connect(bootstrap: &str) -> Result<FlussConnection> {
    FlussConnection::new(connection_config(bootstrap))
        .await
        .with_context(|| format!("connecting to Fluss at {bootstrap}"))
}

fn order_row(id: i64) -> GenericRow<'static> {
    let mut row = GenericRow::new(3);
    row.set_field(0, id);
    row.set_field(1, format!("customer_{}", id % 10));
    row.set_field(2, id as f64 * 1.5);
    row
}

fn user_row(id: i32, updated: bool) -> GenericRow<'static> {
    let mut row = GenericRow::new(3);
    row.set_field(0, id);
    if updated {
        row.set_field(1, format!("user_{id}_v2"));
        row.set_field(2, f64::from(id) * 10.0 + 1000.0);
    } else {
        row.set_field(1, format!("user_{id}"));
        row.set_field(2, f64::from(id) * 10.0);
    }
    row
}

async fn setup(bootstrap: &str, buckets: i32) -> Result<()> {
    let conn = connect(bootstrap).await?;
    let admin = conn.get_admin()?;

    admin
        .create_database(DATABASE, None, true)
        .await
        .context("creating database")?;

    let orders = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("order_id", DataTypes::bigint())
                .column("customer", DataTypes::string())
                .column("amount", DataTypes::double())
                .build()?,
        )
        .distributed_by(Some(buckets), vec![])
        .build()?;
    admin
        .create_table(&TablePath::new(DATABASE, ORDERS_TABLE), &orders, true)
        .await
        .context("creating orders log table")?;

    let users = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("user_id", DataTypes::int())
                .column("name", DataTypes::string())
                .column("score", DataTypes::double())
                .primary_key(vec!["user_id"])
                .build()?,
        )
        .distributed_by(Some(buckets), vec!["user_id".to_string()])
        .build()?;
    admin
        .create_table(&TablePath::new(DATABASE, USERS_TABLE), &users, true)
        .await
        .context("creating users PK table")?;

    println!("setup: database '{DATABASE}' with tables '{ORDERS_TABLE}' (log) and '{USERS_TABLE}' (pk), {buckets} buckets");
    Ok(())
}

async fn append(bootstrap: &str, count: i64, start_id: i64, rate: u64) -> Result<()> {
    let conn = connect(bootstrap).await?;
    let table = conn
        .get_table(&TablePath::new(DATABASE, ORDERS_TABLE))
        .await?;
    let writer = table.new_append()?.create_writer()?;

    let tick = if rate > 0 {
        Some(Duration::from_micros(1_000_000 / rate))
    } else {
        None
    };

    for id in start_id..start_id + count {
        writer.append(&order_row(id))?;
        if let Some(tick) = tick {
            // Flush per row when rate-limited so consumers observe a steady stream.
            writer.flush().await?;
            tokio::time::sleep(tick).await;
        }
    }
    writer.flush().await?;
    println!("append: wrote order_id [{start_id}..{}) ({count} rows)", start_id + count);
    Ok(())
}

async fn cdc(bootstrap: &str, inserts: i32, updates: i32, deletes: i32, start_id: i32) -> Result<()> {
    anyhow::ensure!(updates <= inserts, "updates must be <= inserts");
    anyhow::ensure!(deletes <= inserts, "deletes must be <= inserts");

    let conn = connect(bootstrap).await?;
    let table = conn
        .get_table(&TablePath::new(DATABASE, USERS_TABLE))
        .await?;
    let writer = table.new_upsert()?.create_writer()?;

    let end_id = start_id + inserts;
    for id in start_id..end_id {
        writer.upsert(&user_row(id, false))?;
    }
    writer.flush().await?;

    for id in start_id..start_id + updates {
        writer.upsert(&user_row(id, true))?;
    }
    writer.flush().await?;

    // Delete the LAST `deletes` ids of the inserted range.
    for id in end_id - deletes..end_id {
        let mut key = GenericRow::new(3);
        key.set_field(0, id);
        writer.delete(&key)?;
    }
    writer.flush().await?;

    println!(
        "cdc: inserted [{start_id}..{end_id}), updated [{start_id}..{}), deleted [{}..{end_id})",
        start_id + updates,
        end_id - deletes,
    );
    Ok(())
}

/// Continuous workload that tolerates cluster faults: on any error it drops the
/// connection, backs off, reconnects, and resumes from its monotonic cursor.
async fn mixed(bootstrap: &str, duration_secs: u64, rate: u64, start_id: i64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let tick = Duration::from_micros(1_000_000 / rate.max(1));
    let mut next_order_id = start_id;
    let mut appended: u64 = 0;
    let mut upserted: u64 = 0;
    let mut retries: u64 = 0;

    'outer: while Instant::now() < deadline {
        let session: Result<()> = async {
            let conn = connect(bootstrap).await?;
            let orders = conn
                .get_table(&TablePath::new(DATABASE, ORDERS_TABLE))
                .await?;
            let users = conn
                .get_table(&TablePath::new(DATABASE, USERS_TABLE))
                .await?;
            let order_writer = orders.new_append()?.create_writer()?;
            let user_writer = users.new_upsert()?.create_writer()?;

            while Instant::now() < deadline {
                order_writer.append(&order_row(next_order_id))?;
                order_writer.flush().await?;
                next_order_id += 1;
                appended += 1;

                // Every 10th order, upsert a user in a rolling window so the
                // CDC stream stays active too. Values are deterministic in id.
                if next_order_id % 10 == 0 {
                    let uid = i32::try_from(next_order_id % 100).unwrap_or(0) + 10_000;
                    user_writer.upsert(&user_row(uid, false))?;
                    user_writer.flush().await?;
                    upserted += 1;
                }
                tokio::time::sleep(tick).await;
            }
            Ok(())
        }
        .await;

        match session {
            Ok(()) => break 'outer,
            Err(e) => {
                retries += 1;
                eprintln!("mixed: transient failure (retry {retries}): {e:#}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }

    println!(
        "mixed: appended={appended} upserted={upserted} retries={retries} next_order_id={next_order_id}"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let bootstrap = cli
        .bootstrap_servers
        .or_else(|| std::env::var("FLUSS_BOOTSTRAP_SERVERS").ok())
        .unwrap_or_else(|| "localhost:9123".to_string());

    match cli.command {
        Command::Setup { buckets } => setup(&bootstrap, buckets).await,
        Command::Append { count, start_id, rate } => append(&bootstrap, count, start_id, rate).await,
        Command::Cdc { inserts, updates, deletes, start_id } => {
            cdc(&bootstrap, inserts, updates, deletes, start_id).await
        }
        Command::Mixed { duration_secs, rate, start_id } => {
            mixed(&bootstrap, duration_secs, rate, start_id).await
        }
    }
}
