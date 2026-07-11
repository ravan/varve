//! Multi-process `varved` cluster harness (roadmap slice 9, task 14).
//!
//! [`ProcessCluster::start`] spawns one Writer+Query+Compactor process and two
//! Query-only processes, all sharing ONE temporary local log/store directory.
//! Every process is a real `varved` binary (`env!("CARGO_BIN_EXE_varved")`).
//!
//! Readiness is never a fixed sleep: a per-child stdout thread parses the
//! `VARVED_LISTENING <addr>` contract line, and the harness then polls
//! `/healthz` until the node reports healthy. Child stderr is captured on a
//! dedicated thread and surfaced on any startup/test failure. `Drop` kills
//! every child in reverse creation order and waits, so no process survives a
//! test, and the shared temp dir is only removed once all children are gone.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::json;
use tempfile::TempDir;

/// Shared bearer token for every node's `[auth.static]` table.
const TOKEN: &str = "varve-cluster-test-token-9";
/// Follower poll interval: deliberately slow (200 ms) so a basis-bounded read
/// normally has to block until the follower tails the new record.
const TAIL_POLL_INTERVAL_MS: u64 = 200;
/// Generous per-node basis timeout so a 200 ms-poll follower always catches up.
const BASIS_TIMEOUT_MS: u64 = 5_000;
/// Bound on `VARVED_LISTENING` delivery and on the `/healthz` readiness poll.
const READY_TIMEOUT: Duration = Duration::from_secs(10);

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, BoxError>;

/// A writer receipt, deserialized from the `/v1/tx` `TxResponse`. Unknown
/// fields are ignored, so only the load-bearing basis/tx id are pinned here.
#[derive(Debug, Clone, Deserialize)]
pub struct TxReceipt {
    pub tx_id: u64,
    pub basis: u64,
}

/// One running `varved` child plus its captured streams.
struct Node {
    name: String,
    base_url: String,
    child: Child,
    stderr: Arc<Mutex<String>>,
    stdout_join: Option<JoinHandle<()>>,
    stderr_join: Option<JoinHandle<()>>,
}

impl Node {
    /// Kills the child, waits for it, and joins both stream-reader threads.
    /// Idempotent: after the first call the join handles are gone.
    fn shutdown(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(handle) = self.stdout_join.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.stderr_join.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub struct ProcessCluster {
    /// Creation order: `[writer, query1, query2]`.
    nodes: Vec<Node>,
    writer_advertised: String,
    client: reqwest::Client,
    token: String,
    _tempdir: TempDir,
}

impl Drop for ProcessCluster {
    fn drop(&mut self) {
        // Reverse creation order: query nodes first, writer last. `Node::drop`
        // does the kill+wait+join; popping drives it in reverse.
        while let Some(node) = self.nodes.pop() {
            drop(node);
        }
        // `_tempdir` drops after this method returns — i.e. after every child
        // is dead — so nothing is writing the shared dir when it is removed.
    }
}

impl ProcessCluster {
    /// Starts the three-process cluster against a fresh shared temp dir. The
    /// writer starts first (it creates the log/store and publishes
    /// `v1/writer.json`); the two query nodes follow. Any partially started
    /// node is killed if a later node fails to come up.
    pub async fn start() -> Result<ProcessCluster> {
        let tempdir = TempDir::new()?;
        let log_dir = tempdir.path().join("log");
        let store_dir = tempdir.path().join("store");

        // Reserve a loopback port for the writer, capture it, then release it
        // immediately so the child can bind it. The writer must advertise a
        // concrete address, so it cannot use port 0.
        let writer_port = reserve_loopback_port()?;
        let writer_advertised = format!("http://127.0.0.1:{writer_port}");

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        let mut nodes: Vec<Node> = Vec::new();

        let writer_config = node_config(
            &["writer", "query", "compactor"],
            &format!("127.0.0.1:{writer_port}"),
            Some(&writer_advertised),
            &log_dir,
            &store_dir,
        );
        match start_node(
            "writer",
            &tempdir,
            "node-writer.toml",
            &writer_config,
            &client,
        )
        .await
        {
            Ok(node) => nodes.push(node),
            Err(error) => {
                reverse_shutdown(&mut nodes);
                return Err(error);
            }
        }

        for index in 1..=2 {
            let name = format!("query{index}");
            let config = node_config(&["query"], "127.0.0.1:0", None, &log_dir, &store_dir);
            let file = format!("node-{name}.toml");
            match start_node(&name, &tempdir, &file, &config, &client).await {
                Ok(node) => nodes.push(node),
                Err(error) => {
                    reverse_shutdown(&mut nodes);
                    return Err(error);
                }
            }
        }

        Ok(ProcessCluster {
            nodes,
            writer_advertised,
            client,
            token: TOKEN.to_string(),
            _tempdir: tempdir,
        })
    }

    /// The writer's advertised base URL (also its `/v1/tx` target and the
    /// exact address a query node returns on a 421 redirect).
    pub fn writer_url(&self) -> &str {
        &self.writer_advertised
    }

    /// Base URLs of the two Query-only nodes, in creation order.
    pub fn query_urls(&self) -> Vec<&str> {
        self.nodes[1..]
            .iter()
            .map(|node| node.base_url.as_str())
            .collect()
    }

    /// The shared reqwest client (for tests that build requests directly, e.g.
    /// the Arrow-stream and 421 checks).
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// The shared bearer token.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// POSTs a mutation to `base`'s `/v1/tx` and returns the receipt. Errors
    /// (with the response body) if the status is not 200.
    pub async fn tx(&self, base: &str, gql: &str) -> Result<TxReceipt> {
        let response = self
            .client
            .post(format!("{base}/v1/tx"))
            .bearer_auth(&self.token)
            .json(&json!({ "gql": gql }))
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        if status != reqwest::StatusCode::OK {
            return Err(format!("tx to {base} returned {status}: {body}").into());
        }
        Ok(serde_json::from_str(&body)?)
    }

    /// POSTs a query to `base`'s `/v1/query` and returns the `rows` JSON array.
    /// When `basis` is `Some`, it is attached with a generous `basis_timeout_ms`
    /// so the read blocks until the follower reaches the basis.
    pub async fn query_json(
        &self,
        base: &str,
        gql: &str,
        basis: Option<u64>,
    ) -> Result<serde_json::Value> {
        let mut body = json!({ "gql": gql });
        if let Some(basis) = basis {
            body["basis"] = json!(basis);
            body["basis_timeout_ms"] = json!(BASIS_TIMEOUT_MS);
        }
        let response = self
            .client
            .post(format!("{base}/v1/query"))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let text = response.text().await?;
        if status != reqwest::StatusCode::OK {
            return Err(format!("query to {base} returned {status}: {text}").into());
        }
        let value: serde_json::Value = serde_json::from_str(&text)?;
        Ok(value
            .get("rows")
            .cloned()
            .ok_or("query response missing `rows`")?)
    }

    /// Number of rows a query returns at the given basis.
    pub async fn query_row_count(
        &self,
        base: &str,
        gql: &str,
        basis: Option<u64>,
    ) -> Result<usize> {
        let rows = self.query_json(base, gql, basis).await?;
        Ok(rows.as_array().map(Vec::len).unwrap_or(0))
    }
}

/// Reverse-order kill of any already-started nodes (partial-startup cleanup).
fn reverse_shutdown(nodes: &mut Vec<Node>) {
    while let Some(node) = nodes.pop() {
        drop(node);
    }
}

/// Binds `127.0.0.1:0`, captures the assigned port, and releases the socket so
/// a freshly spawned child can bind the same port.
fn reserve_loopback_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Renders a complete node TOML: shared `[log.local]`/`[storage.local]`,
/// authenticated `[server.http]`, static auth, prometheus metrics. Writers pass
/// `Some(advertised)`; query nodes pass `None` and may bind port 0.
fn node_config(
    roles: &[&str],
    listen: &str,
    advertised: Option<&str>,
    log_dir: &Path,
    store_dir: &Path,
) -> String {
    let roles = roles
        .iter()
        .map(|role| format!("\"{role}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let advertised_line = match advertised {
        Some(address) => format!("advertised_address = \"{address}\"\n"),
        None => String::new(),
    };
    format!(
        "[node]\n\
         roles = [{roles}]\n\
         tail_poll_interval_ms = {TAIL_POLL_INTERVAL_MS}\n\
         tail_batch_records = 1024\n\
         basis_timeout_ms = {BASIS_TIMEOUT_MS}\n\
         \n\
         [log]\n\
         backend = \"local\"\n\
         group_commit_window_ms = 0\n\
         [log.local]\n\
         dir = {log_dir:?}\n\
         \n\
         [storage]\n\
         backend = \"local\"\n\
         max_block_rows = 100000\n\
         flush_interval_ms = 300000\n\
         [storage.local]\n\
         dir = {store_dir:?}\n\
         \n\
         [server]\n\
         backend = \"http\"\n\
         [server.http]\n\
         listen = \"{listen}\"\n\
         {advertised_line}\
         max_body_bytes = \"8MiB\"\n\
         \n\
         [auth]\n\
         backend = \"static\"\n\
         [auth.static]\n\
         tokens = [{{ subject = \"test\", token = \"{TOKEN}\" }}]\n\
         \n\
         [metrics]\n\
         backend = \"prometheus\"\n",
        log_dir = log_dir.display().to_string(),
        store_dir = store_dir.display().to_string(),
    )
}

/// Spawns one `varved` child from a written config file, wires stdout/stderr
/// reader threads, and blocks (up to [`READY_TIMEOUT`]) for the
/// `VARVED_LISTENING <addr>` line. On failure the child is killed and its
/// captured stderr is surfaced.
fn spawn_node(name: &str, tempdir: &TempDir, file: &str, config: &str) -> Result<Node> {
    let config_path = tempdir.path().join(file);
    std::fs::write(&config_path, config)?;

    let mut child = Command::new(env!("CARGO_BIN_EXE_varved"))
        .arg("--config")
        .arg(&config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed to spawn {name}: {error}"))?;

    let stdout = child.stdout.take().ok_or("child stdout was not captured")?;
    let stderr = child.stderr.take().ok_or("child stderr was not captured")?;

    let (address_tx, address_rx) = mpsc::channel::<String>();
    let stdout_join = std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            let Ok(line) = line else { break };
            if let Some(address) = line.strip_prefix("VARVED_LISTENING ") {
                let _ = address_tx.send(address.trim().to_string());
            }
        }
    });

    let captured = Arc::new(Mutex::new(String::new()));
    let sink = Arc::clone(&captured);
    let stderr_join = std::thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines() {
            let Ok(line) = line else { break };
            if let Ok(mut buffer) = sink.lock() {
                buffer.push_str(&line);
                buffer.push('\n');
            }
        }
    });

    let mut node = Node {
        name: name.to_string(),
        base_url: String::new(),
        child,
        stderr: captured,
        stdout_join: Some(stdout_join),
        stderr_join: Some(stderr_join),
    };

    match address_rx.recv_timeout(READY_TIMEOUT) {
        Ok(address) => {
            node.base_url = format!("http://{address}");
            Ok(node)
        }
        Err(_) => {
            let captured = snapshot(&node.stderr);
            node.shutdown();
            Err(format!(
                "{name} did not print VARVED_LISTENING within {READY_TIMEOUT:?}; stderr:\n{captured}"
            )
            .into())
        }
    }
}

/// Spawns a node, waits for its `VARVED_LISTENING` line, then polls `/healthz`
/// until it reports healthy. Either failure kills the child and surfaces its
/// stderr before propagating.
async fn start_node(
    name: &str,
    tempdir: &TempDir,
    file: &str,
    config: &str,
    client: &reqwest::Client,
) -> Result<Node> {
    let mut node = spawn_node(name, tempdir, file, config)?;
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        if let Ok(response) = client
            .get(format!("{}/healthz", node.base_url))
            .send()
            .await
        {
            if response.status().is_success() {
                return Ok(node);
            }
        }
        if Instant::now() >= deadline {
            let captured = snapshot(&node.stderr);
            node.shutdown();
            return Err(format!(
                "{name} never reported healthy within {READY_TIMEOUT:?}; stderr:\n{captured}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn snapshot(buffer: &Arc<Mutex<String>>) -> String {
    buffer
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_else(|poisoned| poisoned.into_inner().clone())
}
