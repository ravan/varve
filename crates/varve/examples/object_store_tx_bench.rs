//! Concurrent transaction throughput through the object-store log and an
//! explicitly configured S3-compatible backend.
//!
//! Required environment: VARVE_S3_ENDPOINT, VARVE_S3_BUCKET,
//! AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY.

use std::sync::Arc;
use std::time::Instant;
use varve::{Config, Db};

const DEFAULT_TOTAL: u64 = 65_536;
const DEFAULT_WORKERS: u64 = 128;

fn required_env(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(name).map_err(|_| format!("{name} must be set").into())
}

fn positive_env(name: &str, default: u64) -> Result<u64, Box<dyn std::error::Error>> {
    let value = match std::env::var(name) {
        Ok(value) => value.parse::<u64>()?,
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    if value == 0 {
        return Err(format!("{name} must be greater than zero").into());
    }
    Ok(value)
}

fn config() -> Result<Config, Box<dyn std::error::Error>> {
    let endpoint = required_env("VARVE_S3_ENDPOINT")?;
    let bucket = required_env("VARVE_S3_BUCKET")?;
    let access_key_id = required_env("AWS_ACCESS_KEY_ID")?;
    let secret_access_key = required_env("AWS_SECRET_ACCESS_KEY")?;
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "garage".to_string());

    Ok(Config::from_toml_str(&format!(
        "[log]\nbackend = \"object-store\"\ngroup_commit_window_ms = 15\n\
         [storage]\nbackend = \"s3\"\nmax_block_rows = 1000000\n\
         [storage.s3]\nbucket = {bucket:?}\nendpoint = {endpoint:?}\n\
         region = {region:?}\naccess_key_id = {access_key_id:?}\n\
         secret_access_key = {secret_access_key:?}\npath_style = true\nallow_http = true\n\
         [cache]\ntiers = []\n"
    ))?)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let total = positive_env("VARVE_OBJECT_STORE_TX_TOTAL", DEFAULT_TOTAL)?;
    let workers = positive_env("VARVE_OBJECT_STORE_TX_WORKERS", DEFAULT_WORKERS)?;
    if total % workers != 0 {
        return Err("VARVE_OBJECT_STORE_TX_TOTAL must be divisible by workers".into());
    }

    let db = Arc::new(Db::open(config()?).await?);
    let started = Instant::now();
    let mut handles = Vec::with_capacity(workers as usize);
    for worker in 0..workers {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            for i in 0..total / workers {
                let id = worker * 1_000_000_000 + i;
                db.execute(&format!("INSERT (:ObjectBench {{_id: {id}, v: {i}}})"))
                    .await?;
            }
            Ok::<(), varve::EngineError>(())
        }));
    }
    for handle in handles {
        handle.await??;
    }
    let elapsed = started.elapsed();
    let committed = db.metrics().txs_committed;
    if committed != total {
        return Err(format!("expected {total} committed transactions, got {committed}").into());
    }
    println!(
        "object-store log: {committed} txs / {workers} workers in {elapsed:.2?} ({:.0} tx/s)",
        committed as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}
