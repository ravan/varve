//! Native ReBAC access control (label / edge-type granularity): policy lives
//! in the reserved bitemporal `__security` graph, principals resolve their
//! privileges by `MEMBER_OF*` traversal, and enforcement is deny-by-default
//! for both reads and writes once `[security] enabled = true`.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use varve::{Config, Db, EngineError, Value};

/// `[security]` enabled with `root` as the bootstrap admin, on memory
/// backends (zero group-commit window, like `Db::memory()`).
async fn secured_db() -> Db {
    let config = Config::from_toml_str(
        "[log]\ngroup_commit_window_ms = 0\n\
         [security]\nenabled = true\nadmins = [\"root\"]\n",
    )
    .unwrap();
    Db::open(config).await.unwrap()
}

fn params() -> BTreeMap<String, Value> {
    BTreeMap::new()
}

async fn run_as(db: &Db, user: &str, gql: &str) -> Result<varve::TxReceipt, EngineError> {
    db.execute_as(gql, &params(), user).await
}

async fn admin(db: &Db, gql: &str) {
    run_as(db, "root", gql)
        .await
        .unwrap_or_else(|e| panic!("admin statement {gql}: {e}"));
}

async fn query_as(db: &Db, user: &str, gql: &str) -> Result<Vec<varve::RecordBatch>, EngineError> {
    db.query(gql).as_principal(user).await
}

fn rows(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// The values of a Utf8 column across batches, sorted.
fn strings(batches: &[varve::RecordBatch], column: &str) -> Vec<String> {
    use arrow::array::{Array, StringArray};
    let mut out = Vec::new();
    for batch in batches {
        let Some(col) = batch.column_by_name(column) else {
            continue;
        };
        let col: &StringArray = col.as_any().downcast_ref().unwrap();
        for i in 0..col.len() {
            if !col.is_null(i) {
                out.push(col.value(i).to_string());
            }
        }
    }
    out.sort();
    out
}

fn assert_denied<T: std::fmt::Debug>(result: Result<T, EngineError>, what: &str) {
    match result {
        Err(EngineError::AccessDenied(_)) => {}
        other => panic!("{what} should be AccessDenied, got {other:?}"),
    }
}

/// Seed: two Person nodes, one Secret, one dual-labeled node, and edges of
/// two types (KNOWS grantable, MANAGES not), plus a KNOWS chain through a
/// Secret intermediate for endpoint-visibility checks.
async fn seed(db: &Db) {
    admin(
        db,
        "INSERT (a:Person {_id: 'ada', name: 'Ada'}), \
                (b:Person {_id: 'bob', name: 'Bob'}), \
                (s:Secret {_id: 's1', name: 'Cabal'}), \
                (d:Person:Secret {_id: 'dual', name: 'Dual'}), \
                (a)-[:KNOWS {_id: 'k1'}]->(b), \
                (a)-[:MANAGES {_id: 'm1'}]->(b), \
                (a)-[:KNOWS {_id: 'k2'}]->(s), \
                (s)-[:KNOWS {_id: 'k3'}]->(b)",
    )
    .await;
}

/// Grants `reader` READ on Person nodes + KNOWS edges on every graph and
/// makes `ada` a member.
async fn grant_reader(db: &Db) {
    admin(db, "CREATE ROLE reader").await;
    admin(db, "GRANT READ ON GRAPH * NODES Person TO ROLE reader").await;
    admin(db, "GRANT READ ON GRAPH * EDGES KNOWS TO ROLE reader").await;
    admin(db, "GRANT ROLE reader TO USER 'ada'").await;
}

#[tokio::test]
async fn disabled_mode_is_todays_behavior() {
    // No [security] section at all: a principal on either path changes nothing.
    let db = Db::memory();
    db.execute("INSERT (:Secret {_id: 's', name: 'S'})")
        .await
        .unwrap();
    run_as(&db, "nobody", "INSERT (:Secret {_id: 's2', name: 'S2'})")
        .await
        .unwrap();
    let batches = query_as(&db, "nobody", "MATCH (s:Secret) RETURN s.name")
        .await
        .unwrap();
    assert_eq!(rows(&batches), 2);
}

#[tokio::test]
async fn deny_by_default_reads_empty_and_writes_rejected() {
    let db = secured_db().await;
    seed(&db).await;

    // No grants at all for 'ada': reads see nothing, writes reject.
    let batches = query_as(&db, "ada", "MATCH (p:Person) RETURN p.name")
        .await
        .unwrap();
    assert_eq!(rows(&batches), 0, "ungranted read must be empty");

    assert_denied(
        run_as(&db, "ada", "INSERT (:Person {_id: 'x', name: 'X'})").await,
        "ungranted write",
    );

    // The embedded process owner (no principal) keeps full access.
    let batches = db.query("MATCH (p:Person) RETURN p.name").await.unwrap();
    assert_eq!(rows(&batches), 3);
    // And so does the bootstrap admin.
    let batches = query_as(&db, "root", "MATCH (p:Person) RETURN p.name")
        .await
        .unwrap();
    assert_eq!(rows(&batches), 3);
}

#[tokio::test]
async fn label_read_grant_filters_scans_conservatively() {
    let db = secured_db().await;
    seed(&db).await;
    grant_reader(&db).await;

    // Person granted: Ada and Bob visible. The dual-labeled Person:Secret
    // node is NOT (every label must be granted), nor is Secret.
    let batches = query_as(&db, "ada", "MATCH (p:Person) RETURN p.name")
        .await
        .unwrap();
    assert_eq!(strings(&batches, "p.name"), vec!["Ada", "Bob"]);

    let batches = query_as(&db, "ada", "MATCH (s:Secret) RETURN s.name")
        .await
        .unwrap();
    assert_eq!(rows(&batches), 0, "Secret label is not granted");
}

#[tokio::test]
async fn edge_read_requires_type_grant_and_visible_endpoints() {
    let db = secured_db().await;
    seed(&db).await;
    grant_reader(&db).await;

    // KNOWS between two visible Persons: traversable.
    let batches = query_as(
        &db,
        "ada",
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name",
    )
    .await
    .unwrap();
    assert_eq!(strings(&batches, "b.name"), vec!["Bob"]);

    // MANAGES is not granted: invisible even between visible endpoints.
    let batches = query_as(
        &db,
        "ada",
        "MATCH (a:Person)-[:MANAGES]->(b:Person) RETURN b.name",
    )
    .await
    .unwrap();
    assert_eq!(rows(&batches), 0);

    // Quantified traversal cannot route THROUGH the invisible Secret
    // intermediate: a->s->b via KNOWS{2} must not reach Bob.
    let batches = query_as(
        &db,
        "ada",
        "MATCH (a:Person {_id: 'ada'})-[:KNOWS]->{2}(b:Person) RETURN b.name",
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&batches),
        0,
        "path through non-granted intermediate must be blocked"
    );

    // Admin sees the same 2-hop path fine (control).
    let batches = query_as(
        &db,
        "root",
        "MATCH (a:Person {_id: 'ada'})-[:KNOWS]->{2}(b:Person) RETURN b.name",
    )
    .await
    .unwrap();
    assert_eq!(strings(&batches, "b.name"), vec!["Bob"]);
}

/// Chain seed for the anchored-fast-path tests: `p1->p2->p3` is an
/// all-visible 2-hop KNOWS chain, `p1->d->p4` routes through a dual-labeled
/// `Person:Secret` intermediate (visible to admins and node-wildcard
/// principals, invisible to a Person-only grant), and `p1-MANAGES->p2`
/// exercises the non-granted edge type.
async fn seed_chain(db: &Db) {
    admin(
        db,
        "INSERT (p1:Person {_id: 'p1', name: 'P1'}), \
                (p2:Person {_id: 'p2', name: 'P2'}), \
                (p3:Person {_id: 'p3', name: 'P3'}), \
                (p4:Person {_id: 'p4', name: 'P4'}), \
                (d:Person:Secret {_id: 'd', name: 'D'}), \
                (p1)-[:KNOWS {_id: 'e1'}]->(p2), \
                (p2)-[:KNOWS {_id: 'e2'}]->(p3), \
                (p1)-[:KNOWS {_id: 'e3'}]->(d), \
                (d)-[:KNOWS {_id: 'e4'}]->(p4), \
                (p1)-[:MANAGES {_id: 'e5'}]->(p2)",
    )
    .await;
}

/// The `{_id: 'p1'}` anchor takes task-12's anchored fast path, which since
/// the security-aware-pruning slice stays ON under active (non-wildcard)
/// enforcement. Every assertion cross-checks the anchored (pruned) form
/// against the name-anchored form, which takes the full filtered scan —
/// enforcement semantics must be identical on both paths.
#[tokio::test]
async fn anchored_fast_path_keeps_enforcement_fixed_hops() {
    let db = secured_db().await;
    seed_chain(&db).await;
    grant_reader(&db).await;

    let anchored = "MATCH (a:Person {_id: 'p1'})-[:KNOWS]->(x:Person)-[:KNOWS]->(c:Person) \
                    RETURN c.name";
    let full = "MATCH (a:Person)-[:KNOWS]->(x:Person)-[:KNOWS]->(c:Person) \
                WHERE a.name = 'P1' RETURN c.name";

    // Admin control: both chains qualify (`d` is a Person too).
    let batches = query_as(&db, "root", anchored).await.unwrap();
    assert_eq!(strings(&batches, "c.name"), vec!["P3", "P4"]);

    // Person-only grant: the dual-labeled intermediate is invisible, so only
    // the all-visible chain survives — identically on the pruned and full paths.
    let batches = query_as(&db, "ada", anchored).await.unwrap();
    assert_eq!(strings(&batches, "c.name"), vec!["P3"]);
    let batches = query_as(&db, "ada", full).await.unwrap();
    assert_eq!(strings(&batches, "c.name"), vec!["P3"]);

    // Non-granted edge type: empty on the fast path too (and no traversal
    // budget spent — the plan short-circuits before the BFS).
    let batches = query_as(
        &db,
        "ada",
        "MATCH (a:Person {_id: 'p1'})-[:MANAGES]->(b:Person) RETURN b.name",
    )
    .await
    .unwrap();
    assert_eq!(rows(&batches), 0);
}

/// Same cross-check for the quantified-hop fast path (`QuantifiedAdjacency`):
/// endpoint visibility is enforced on the pruned adjacency itself, computed
/// over the anchor-reachable set instead of the whole graph.
#[tokio::test]
async fn anchored_fast_path_keeps_enforcement_quantified_hops() {
    let db = secured_db().await;
    seed_chain(&db).await;
    grant_reader(&db).await;

    let anchored = "MATCH (a:Person {_id: 'p1'})-[:KNOWS]->{2}(c:Person) RETURN c.name";
    let full = "MATCH (a:Person)-[:KNOWS]->{2}(c:Person) WHERE a.name = 'P1' RETURN c.name";

    let batches = query_as(&db, "root", anchored).await.unwrap();
    assert_eq!(strings(&batches, "c.name"), vec!["P3", "P4"]);

    // The walk through the invisible intermediate is cut from the pruned
    // adjacency exactly as it is from the full one.
    let batches = query_as(&db, "ada", anchored).await.unwrap();
    assert_eq!(strings(&batches, "c.name"), vec!["P3"]);
    let batches = query_as(&db, "ada", full).await.unwrap();
    assert_eq!(strings(&batches, "c.name"), vec!["P3"]);

    // Non-granted edge type short-circuits to an empty adjacency.
    let batches = query_as(
        &db,
        "ada",
        "MATCH (a:Person {_id: 'p1'})-[:MANAGES]->{2}(b:Person) RETURN b.name",
    )
    .await
    .unwrap();
    assert_eq!(rows(&batches), 0);
}

/// Node-wildcard + name-scoped edges (`unrestricted()` false, so enforcement
/// is active and the fast path's edge filtering runs, but endpoint
/// visibility filtering must NOT cut anything).
#[tokio::test]
async fn anchored_fast_path_node_wildcard_edge_named() {
    let db = secured_db().await;
    seed_chain(&db).await;
    admin(&db, "CREATE ROLE walker").await;
    admin(&db, "GRANT READ ON GRAPH * NODES * TO ROLE walker").await;
    admin(&db, "GRANT READ ON GRAPH * EDGES KNOWS TO ROLE walker").await;
    admin(&db, "GRANT ROLE walker TO USER 'eve'").await;

    // All nodes visible: both chains qualify, fixed and quantified.
    let batches = query_as(
        &db,
        "eve",
        "MATCH (a:Person {_id: 'p1'})-[:KNOWS]->(x:Person)-[:KNOWS]->(c:Person) RETURN c.name",
    )
    .await
    .unwrap();
    assert_eq!(strings(&batches, "c.name"), vec!["P3", "P4"]);
    let batches = query_as(
        &db,
        "eve",
        "MATCH (a:Person {_id: 'p1'})-[:KNOWS]->{2}(c:Person) RETURN c.name",
    )
    .await
    .unwrap();
    assert_eq!(strings(&batches, "c.name"), vec!["P3", "P4"]);

    // MANAGES stays invisible.
    let batches = query_as(
        &db,
        "eve",
        "MATCH (a:Person {_id: 'p1'})-[:MANAGES]->(b:Person) RETURN b.name",
    )
    .await
    .unwrap();
    assert_eq!(rows(&batches), 0);
}

#[tokio::test]
async fn transitive_role_membership_resolves_and_revoke_severs() {
    let db = secured_db().await;
    seed(&db).await;
    admin(&db, "CREATE ROLE junior").await;
    admin(&db, "CREATE ROLE senior").await;
    admin(&db, "GRANT READ ON GRAPH * NODES Person TO ROLE senior").await;
    // user -> junior -> senior -> grant
    admin(&db, "GRANT ROLE senior TO ROLE junior").await;
    admin(&db, "GRANT ROLE junior TO USER 'ada'").await;

    let batches = query_as(&db, "ada", "MATCH (p:Person) RETURN p.name")
        .await
        .unwrap();
    assert_eq!(strings(&batches, "p.name"), vec!["Ada", "Bob"]);

    // Severing the middle of the chain removes the inherited grant.
    admin(&db, "REVOKE ROLE senior FROM ROLE junior").await;
    let batches = query_as(&db, "ada", "MATCH (p:Person) RETURN p.name")
        .await
        .unwrap();
    assert_eq!(rows(&batches), 0, "revoked chain must sever access");
}

#[tokio::test]
async fn write_enforcement_rejects_the_whole_transaction() {
    let db = secured_db().await;
    admin(&db, "CREATE ROLE writer").await;
    admin(&db, "GRANT ALL ON GRAPH * NODES Person TO ROLE writer").await;
    admin(&db, "GRANT ROLE writer TO USER 'ada'").await;

    // Granted label: fine.
    run_as(&db, "ada", "INSERT (:Person {_id: 'p1', name: 'P1'})")
        .await
        .unwrap();

    // One program, two statements, second touches a non-granted label:
    // the WHOLE tx must reject and nothing may commit.
    assert_denied(
        run_as(
            &db,
            "ada",
            "INSERT (:Person {_id: 'p2', name: 'P2'}); \
             INSERT (:Secret {_id: 's9', name: 'S9'})",
        )
        .await,
        "mixed-label tx",
    );
    let batches = db.query("MATCH (p:Person) RETURN p.name").await.unwrap();
    assert_eq!(
        strings(&batches, "p.name"),
        vec!["P1"],
        "denied tx must leave no partial effects"
    );
    let batches = db.query("MATCH (s:Secret) RETURN s.name").await.unwrap();
    assert_eq!(rows(&batches), 0);

    // Multi-label write: every label must be write-granted.
    assert_denied(
        run_as(&db, "ada", "INSERT (:Person:Secret {_id: 'd2', name: 'D'})").await,
        "multi-label insert with non-granted label",
    );

    // Edge writes need the edge-type grant.
    assert_denied(
        run_as(
            &db,
            "ada",
            "MATCH (a:Person {_id: 'p1'}) INSERT (a)-[:KNOWS {_id: 'ke'}]->(a)",
        )
        .await,
        "edge insert without edge grant",
    );
}

#[tokio::test]
async fn match_driven_dml_cannot_touch_unreadable_nodes() {
    let db = secured_db().await;
    seed(&db).await;
    admin(&db, "CREATE ROLE wiper").await;
    // Write everything, but read only Person: a principal can't delete what
    // it can't even find.
    admin(&db, "GRANT WRITE ON GRAPH * NODES * TO ROLE wiper").await;
    admin(&db, "GRANT WRITE ON GRAPH * EDGES * TO ROLE wiper").await;
    admin(&db, "GRANT READ ON GRAPH * NODES Person TO ROLE wiper").await;
    admin(&db, "GRANT ROLE wiper TO USER 'ada'").await;

    let receipt = run_as(&db, "ada", "MATCH (s:Secret) DELETE s")
        .await
        .unwrap();
    assert!(
        receipt.side_effects.is_empty(),
        "MATCH over unreadable label must bind nothing"
    );
    let batches = db.query("MATCH (s:Secret) RETURN s.name").await.unwrap();
    assert_eq!(rows(&batches), 2, "both Secret-labeled nodes must survive");
}

#[tokio::test]
async fn ddl_gate_requires_admin_and_security_graph_is_unreachable() {
    let db = secured_db().await;
    seed(&db).await;
    grant_reader(&db).await;

    assert_denied(
        run_as(&db, "ada", "CREATE ROLE sneaky").await,
        "non-admin CREATE ROLE",
    );
    assert_denied(
        run_as(&db, "ada", "GRANT READ ON GRAPH * NODES * TO ROLE reader").await,
        "non-admin GRANT",
    );
    assert_denied(query_as(&db, "ada", "SHOW GRANTS").await, "non-admin SHOW");

    // The policy graph itself is unreachable through user GQL on both paths.
    assert!(db
        .query("USE __security; MATCH (u:User) RETURN u.subject")
        .await
        .is_err());
    assert!(db
        .execute("USE __security; INSERT (:Role {name: 'evil'})")
        .await
        .is_err());
}

#[tokio::test]
async fn show_roles_and_grants_report_policy() {
    let db = secured_db().await;
    grant_reader(&db).await;
    admin(&db, "CREATE ROLE ops").await;
    admin(&db, "GRANT ADMIN TO ROLE ops").await;

    let batches = db.query("SHOW ROLES").await.unwrap();
    assert_eq!(strings(&batches, "role"), vec!["ops", "reader"]);

    let batches = db.query("SHOW GRANTS FOR USER 'ada'").await.unwrap();
    assert_eq!(strings(&batches, "role"), vec!["reader", "reader"]);
    assert_eq!(strings(&batches, "action"), vec!["read", "read"]);
    let names: BTreeSet<String> = strings(&batches, "name").into_iter().collect();
    assert_eq!(
        names,
        BTreeSet::from(["Person".to_string(), "KNOWS".to_string()])
    );

    let batches = db.query("SHOW GRANTS FOR ROLE ops").await.unwrap();
    assert_eq!(strings(&batches, "action"), vec!["admin"]);

    // A principal holding ADMIN through a role may run SHOW too.
    admin(&db, "GRANT ROLE ops TO USER 'opsy'").await;
    let batches = db.query("SHOW ROLES").as_principal("opsy").await.unwrap();
    assert_eq!(strings(&batches, "role"), vec!["ops", "reader"]);
}

#[tokio::test]
async fn role_lifecycle_errors_are_reported() {
    let db = secured_db().await;
    admin(&db, "CREATE ROLE r").await;
    assert!(
        run_as(&db, "root", "CREATE ROLE r").await.is_err(),
        "duplicate CREATE ROLE must error"
    );
    assert!(
        run_as(&db, "root", "GRANT ROLE nosuch TO USER 'ada'")
            .await
            .is_err(),
        "granting an unknown role must error"
    );
    assert!(
        run_as(&db, "root", "DROP ROLE nosuch").await.is_err(),
        "dropping an unknown role must error"
    );

    // DROP ROLE severs everything that flowed through it.
    admin(&db, "GRANT READ ON GRAPH * NODES Person TO ROLE r").await;
    admin(&db, "GRANT ROLE r TO USER 'ada'").await;
    admin(&db, "INSERT (:Person {_id: 'p', name: 'P'})").await;
    let batches = query_as(&db, "ada", "MATCH (p:Person) RETURN p.name")
        .await
        .unwrap();
    assert_eq!(rows(&batches), 1);
    admin(&db, "DROP ROLE r").await;
    let batches = query_as(&db, "ada", "MATCH (p:Person) RETURN p.name")
        .await
        .unwrap();
    assert_eq!(rows(&batches), 0, "dropped role must revoke access");
}

#[tokio::test]
async fn admin_role_bypasses_enforcement() {
    let db = secured_db().await;
    seed(&db).await;
    admin(&db, "CREATE ROLE ops").await;
    admin(&db, "GRANT ADMIN TO ROLE ops").await;
    admin(&db, "GRANT ROLE ops TO USER 'opsy'").await;

    let batches = query_as(&db, "opsy", "MATCH (s:Secret) RETURN s.name")
        .await
        .unwrap();
    assert_eq!(
        rows(&batches),
        2,
        "admin-role read sees both Secret-labeled nodes"
    );
    run_as(&db, "opsy", "INSERT (:Secret {_id: 's2', name: 'S2'})")
        .await
        .unwrap();
    run_as(&db, "opsy", "CREATE ROLE from_ops").await.unwrap();
}

#[tokio::test]
async fn graph_scoped_grants_do_not_leak_across_graphs() {
    let db = secured_db().await;
    admin(&db, "CREATE GRAPH tenant_a").await;
    admin(&db, "CREATE GRAPH tenant_b").await;
    admin(&db, "USE tenant_a; INSERT (:Doc {_id: 'a', name: 'A'})").await;
    admin(&db, "USE tenant_b; INSERT (:Doc {_id: 'b', name: 'B'})").await;
    admin(&db, "CREATE ROLE tenant_a_reader").await;
    admin(
        &db,
        "GRANT READ ON GRAPH tenant_a NODES Doc TO ROLE tenant_a_reader",
    )
    .await;
    admin(&db, "GRANT ROLE tenant_a_reader TO USER 'ada'").await;

    let batches = query_as(&db, "ada", "USE tenant_a; MATCH (d:Doc) RETURN d.name")
        .await
        .unwrap();
    assert_eq!(strings(&batches, "d.name"), vec!["A"]);
    let batches = query_as(&db, "ada", "USE tenant_b; MATCH (d:Doc) RETURN d.name")
        .await
        .unwrap();
    assert_eq!(rows(&batches), 0, "grant is scoped to tenant_a");
}

/// A grant issued on the writer becomes effective on a query node once the
/// follower has applied it (log tail), with no writer restart.
#[tokio::test]
async fn follower_propagates_grants_to_query_nodes() {
    let root = tempfile::TempDir::new().unwrap();
    let config = |roles: &str| {
        Config::from_toml_str(&format!(
            "[node]\nroles = [{roles}]\ntail_poll_interval_ms = 5\n\
             tail_batch_records = 64\nbasis_timeout_ms = 2000\n\
             [log]\nbackend = \"local\"\ngroup_commit_window_ms = 0\n\
             [log.local]\ndir = {:?}\n\
             [storage]\nbackend = \"local\"\n\
             [storage.local]\ndir = {:?}\n\
             [security]\nenabled = true\nadmins = [\"root\"]\n",
            root.path().join("log").display().to_string(),
            root.path().join("store").display().to_string(),
        ))
        .unwrap()
    };
    let writer = Db::open(config("\"writer\", \"query\", \"compactor\""))
        .await
        .unwrap();
    let query_node = Db::open(config("\"query\"")).await.unwrap();

    run_as(&writer, "root", "INSERT (:Person {_id: 'p', name: 'P'})")
        .await
        .unwrap();
    run_as(&writer, "root", "CREATE ROLE reader").await.unwrap();
    run_as(
        &writer,
        "root",
        "GRANT READ ON GRAPH * NODES Person TO ROLE reader",
    )
    .await
    .unwrap();
    let receipt = run_as(&writer, "root", "GRANT ROLE reader TO USER 'ada'")
        .await
        .unwrap();

    // Basis-wait pins the query node past the grant tx, then the read is
    // allowed there without any restart.
    let batches = query_node
        .query("MATCH (p:Person) RETURN p.name")
        .as_principal("ada")
        .basis(receipt)
        .await
        .unwrap();
    assert_eq!(strings(&batches, "p.name"), vec!["P"]);
}
