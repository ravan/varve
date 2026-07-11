//! HTTP fixture driver for the Garage scale-out Compose demo (roadmap slice 9,
//! task 15). It exercises the deployed `writer` + two `query` nodes exactly the
//! way an external client would: load the deterministic Slice 6 social graph
//! over `POST /v1/tx`, then read it back at the writer's final basis from EVERY
//! query node and assert the nodes AGREE — same rows, same Arrow decode. Any
//! disagreement, HTTP failure, or empty result exits nonzero, so the demo
//! script's `set -e` fails the run.
//!
//! Pure helpers (query text, row extraction, agreement) are unit-tested; the
//! networked `run` is only reachable against a live deployment.

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::time::Duration;

use arrow::ipc::reader::StreamReader;
use clap::Parser;
use reqwest::Client;
use serde_json::{json, Value};
use varve_testkit::fixture::social_graph;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The Slice 6 fixture shape driven end to end (matches the process scale-out
/// test's `social_graph(200, 1_000, 42)`).
const PEOPLE: usize = 200;
const FRIENDSHIPS: usize = 1_000;
const SEED: u64 = 42;
/// Fixture batch sizes: 100 nodes per INSERT, 100 edge INSERTs per program.
const NODE_BATCH: usize = 100;
const EDGE_BATCH: usize = 100;
/// Generous per-read basis timeout so a lagging follower always catches up to
/// the writer's final basis before it answers.
const BASIS_TIMEOUT_MS: u64 = 30_000;

const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

#[derive(Parser)]
#[command(name = "http_fixture", about = "Load + cross-verify the Compose demo")]
struct Args {
    /// Writer base URL (mutations target this node).
    #[arg(long)]
    writer: String,
    /// Query-node base URL. Repeat once per query node.
    #[arg(long = "query", required = true)]
    query: Vec<String>,
    /// Bearer token accepted by every node's `[auth.static]` table.
    #[arg(long)]
    token: String,
}

/// Fixed two-hop traversal: friends-of-friends.
fn two_hop_query() -> &'static str {
    "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN c.name AS name"
}

/// Variable-length `{1,3}` traversal: everyone reachable within 1..=3 hops.
fn variable_length_query() -> &'static str {
    "MATCH (a:Person)-[:KNOWS]->{1,3}(c:Person) RETURN c.name AS name"
}

/// Sorted `name` column of a `{ "rows": [ { "name": .. }, .. ] }` body. Sorting
/// makes the multiset comparable across nodes regardless of row order.
fn extract_names(body: &Value) -> Result<Vec<String>, BoxError> {
    let rows = body
        .get("rows")
        .and_then(Value::as_array)
        .ok_or("query response missing `rows` array")?;
    let mut names = Vec::with_capacity(rows.len());
    for row in rows {
        let name = row
            .get("name")
            .and_then(Value::as_str)
            .ok_or("row missing string `name` column")?;
        names.push(name.to_string());
    }
    names.sort();
    Ok(names)
}

/// Asserts every node returned the SAME non-empty result for `form`. Returns
/// the agreed row count.
fn agree(form: &str, per_node: &[(String, Vec<String>)]) -> Result<usize, BoxError> {
    let (first_url, first) = per_node.first().ok_or("no query nodes produced a result")?;
    if first.is_empty() {
        return Err(format!("{form}: {first_url} returned zero rows").into());
    }
    for (url, names) in &per_node[1..] {
        if names != first {
            return Err(format!(
                "{form}: {url} disagrees with {first_url} ({} vs {} rows)",
                names.len(),
                first.len()
            )
            .into());
        }
    }
    Ok(first.len())
}

/// POSTs one mutation and returns the receipt's `basis`.
async fn post_tx(client: &Client, writer: &str, token: &str, gql: &str) -> Result<u64, BoxError> {
    let response = client
        .post(format!("{writer}/v1/tx"))
        .bearer_auth(token)
        .json(&json!({ "gql": gql }))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(format!("tx to {writer} returned {status}: {body}").into());
    }
    let value: Value = serde_json::from_str(&body)?;
    value
        .get("basis")
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("tx response missing u64 `basis`: {body}").into())
}

/// POSTs a query at `basis` and returns the parsed JSON body.
async fn post_query_json(
    client: &Client,
    base: &str,
    token: &str,
    gql: &str,
    basis: u64,
) -> Result<Value, BoxError> {
    let response = client
        .post(format!("{base}/v1/query"))
        .bearer_auth(token)
        .json(&json!({
            "gql": gql,
            "basis": basis,
            "basis_timeout_ms": BASIS_TIMEOUT_MS,
        }))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(format!("query to {base} returned {status}: {body}").into());
    }
    Ok(serde_json::from_str(&body)?)
}

/// Requests the Arrow IPC stream for `gql` at `basis` and returns the total row
/// count across every decoded `RecordBatch`.
async fn post_query_arrow(
    client: &Client,
    base: &str,
    token: &str,
    gql: &str,
    basis: u64,
) -> Result<usize, BoxError> {
    let response = client
        .post(format!("{base}/v1/query"))
        .bearer_auth(token)
        .header(reqwest::header::ACCEPT, ARROW_STREAM_CONTENT_TYPE)
        .json(&json!({
            "gql": gql,
            "basis": basis,
            "basis_timeout_ms": BASIS_TIMEOUT_MS,
        }))
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await?;
        return Err(format!("arrow query to {base} returned {status}: {body}").into());
    }
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    if !content_type.starts_with(ARROW_STREAM_CONTENT_TYPE) {
        return Err(format!("arrow query to {base} returned content-type {content_type:?}").into());
    }
    let bytes = response.bytes().await?;
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let mut rows = 0usize;
    for batch in reader {
        rows += batch?.num_rows();
    }
    Ok(rows)
}

async fn run(args: Args) -> Result<(), BoxError> {
    let client = Client::builder().timeout(Duration::from_secs(60)).build()?;
    let graph = social_graph(PEOPLE, FRIENDSHIPS, SEED);

    // Load nodes then edges, one program per transaction, retaining the final
    // basis the writer commits.
    let mut basis = 0u64;
    for statement in graph.node_statements(NODE_BATCH) {
        basis = post_tx(&client, &args.writer, &args.token, &statement).await?;
    }
    for program in graph.edge_programs(EDGE_BATCH) {
        basis = post_tx(&client, &args.writer, &args.token, &program).await?;
    }
    println!("http_fixture: loaded fixture, final basis = {basis}");

    // Every query form must agree across every query node at the final basis.
    let forms: BTreeMap<&str, &str> = BTreeMap::from([
        ("two-hop", two_hop_query()),
        ("var-length-1..3", variable_length_query()),
    ]);
    for (label, gql) in &forms {
        let mut per_node = Vec::new();
        for base in &args.query {
            let body = post_query_json(&client, base, &args.token, gql, basis).await?;
            per_node.push((base.clone(), extract_names(&body)?));
        }
        let count = agree(label, &per_node)?;
        println!(
            "http_fixture: {label} agrees across {} query node(s) at {count} rows",
            args.query.len()
        );
    }

    // Arrow end to end: decode the two-hop stream from the first query node and
    // confirm it matches that node's JSON row count.
    let first = args
        .query
        .first()
        .ok_or("at least one --query URL is required")?;
    let json_body = post_query_json(&client, first, &args.token, two_hop_query(), basis).await?;
    let json_rows = extract_names(&json_body)?.len();
    let arrow_rows = post_query_arrow(&client, first, &args.token, two_hop_query(), basis).await?;
    if arrow_rows != json_rows {
        return Err(format!(
            "arrow decode mismatch on {first}: {arrow_rows} arrow rows vs {json_rows} json rows"
        )
        .into());
    }
    println!("http_fixture: Arrow stream from {first} decoded {arrow_rows} rows (matches JSON)");

    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    match run(args).await {
        Ok(()) => {
            println!("http_fixture: OK");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("http_fixture: FAILED: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_forms_parse_as_gql() {
        // Both driven forms must be valid GQL programs (compile-gate against the
        // parser, so a typo in the traversal text fails the unit test, not the
        // live demo).
        varve_gql::parse_program(two_hop_query()).expect("two-hop query parses");
        varve_gql::parse_program(variable_length_query()).expect("variable-length query parses");
    }

    #[test]
    fn extract_names_sorts_the_name_column() {
        let body = json!({ "rows": [ { "name": "p3" }, { "name": "p1" }, { "name": "p2" } ] });
        assert_eq!(extract_names(&body).unwrap(), vec!["p1", "p2", "p3"]);
    }

    #[test]
    fn extract_names_rejects_a_missing_rows_array() {
        assert!(extract_names(&json!({})).is_err());
    }

    #[test]
    fn extract_names_rejects_a_non_string_name() {
        assert!(extract_names(&json!({ "rows": [ { "name": 7 } ] })).is_err());
    }

    #[test]
    fn agree_accepts_identical_nonempty_results() {
        let per_node = vec![
            ("q1".to_string(), vec!["a".to_string(), "b".to_string()]),
            ("q2".to_string(), vec!["a".to_string(), "b".to_string()]),
        ];
        assert_eq!(agree("two-hop", &per_node).unwrap(), 2);
    }

    #[test]
    fn agree_rejects_divergent_results() {
        let per_node = vec![
            ("q1".to_string(), vec!["a".to_string()]),
            ("q2".to_string(), vec!["b".to_string()]),
        ];
        assert!(agree("two-hop", &per_node).is_err());
    }

    #[test]
    fn agree_rejects_empty_results() {
        let per_node = vec![("q1".to_string(), Vec::new())];
        assert!(agree("two-hop", &per_node).is_err());
    }
}
