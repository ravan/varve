//! Native ReBAC-flavored access control (label / edge-type granularity).
//!
//! Policy is itself a graph: users, roles, and grants live as ordinary
//! bitemporal nodes/edges in the reserved `__security` graph (unreachable
//! through user GQL — `validate_user_graph_name` rejects `__` prefixes), so
//! grants are durable, replicated to query nodes through the normal log, and
//! auditable via system-time travel. A principal's effective privileges are
//! resolved by relationship traversal: `(:User {subject})` →
//! `[:MEMBER_OF]*` → `(:Role)` → `[:GRANTED]` → `(:Privilege)`.
//!
//! Shape:
//! - `(:User {subject})` — auto-created on first mention in a GRANT.
//! - `(:Role {name})` — `CREATE ROLE` / `DROP ROLE`.
//! - `(User|Role)-[:MEMBER_OF]->(Role)` — transitive role inheritance.
//! - `(Role)-[:GRANTED]->(:Privilege {action, graph, kind, name})` where
//!   `action ∈ {read, write, admin}`, `kind ∈ {nodes, edges}`, and
//!   `graph`/`name` may be `*`.
//!
//! Enforcement is deny-by-default when `[security] enabled = true` and a
//! principal is set; the embedded process owner (no principal), configured
//! bootstrap admins, and principals holding `ADMIN` bypass all checks.

use crate::db::EngineError;
use crate::state::{GraphsState, TableKind};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use varve_gql::ast::RoleTarget;
use varve_index::{decode_events, Event, Op};
use varve_storage::{keys, ObjectStore};
use varve_types::{Doc, Iid, Instant, TemporalBounds, TemporalDimension, Value};

/// The reserved policy graph. Present in every `GraphsState` from birth, like
/// `__meta`; never reachable through user statements.
pub(crate) const SECURITY_GRAPH: &str = "__security";

pub(crate) const USER_LABEL: &str = "User";
pub(crate) const ROLE_LABEL: &str = "Role";
pub(crate) const PRIVILEGE_LABEL: &str = "Privilege";
pub(crate) const MEMBER_OF_EDGE: &str = "MEMBER_OF";
pub(crate) const GRANTED_EDGE: &str = "GRANTED";

/// The `*` wildcard stored in privilege docs for graph/name scope.
pub(crate) const WILDCARD: &str = "*";

/// `[security]` config section (documented in the generated configuration
/// reference — `varve-testkit/src/config_reference.rs`).
#[derive(Clone, Debug, Default, serde::Deserialize)]
pub struct SecurityTuning {
    /// Master switch. `false` (default) is exactly the pre-security engine:
    /// zero enforcement, zero overhead.
    #[serde(default)]
    pub enabled: bool,
    /// Bootstrap subjects that bypass every check (and may run security DDL
    /// to grant everyone else).
    #[serde(default)]
    pub admins: Vec<String>,
}

/// One `action × kind` grant set for one graph scope.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Grants {
    pub wildcard: bool,
    pub names: BTreeSet<String>,
}

impl Grants {
    pub fn allows(&self, name: &str) -> bool {
        self.wildcard || self.names.contains(name)
    }

    fn merge(&mut self, other: &Grants) {
        self.wildcard |= other.wildcard;
        self.names.extend(other.names.iter().cloned());
    }
}

/// The four grant sets that govern one graph, already merged across the
/// `ON GRAPH *` and `ON GRAPH g` scopes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct GraphGrants {
    pub read_nodes: Grants,
    pub read_edges: Grants,
    pub write_nodes: Grants,
    pub write_edges: Grants,
}

impl GraphGrants {
    fn merge(&mut self, other: &GraphGrants) {
        self.read_nodes.merge(&other.read_nodes);
        self.read_edges.merge(&other.read_edges);
        self.write_nodes.merge(&other.write_nodes);
        self.write_edges.merge(&other.write_edges);
    }

    /// Fully-wildcard grants filter nothing: enforcement short-circuits to
    /// "unrestricted" so such principals keep the anchored fast path and pay
    /// only the cached context lookup.
    fn unrestricted(&self) -> bool {
        self.read_nodes.wildcard
            && self.read_edges.wildcard
            && self.write_nodes.wildcard
            && self.write_edges.wildcard
    }
}

/// A principal's fully resolved privileges (all graphs).
#[derive(Clone, Debug, Default)]
pub(crate) struct SecurityContext {
    pub admin: bool,
    all_graphs: GraphGrants,
    graphs: BTreeMap<String, GraphGrants>,
}

impl SecurityContext {
    pub fn graph_grants(&self, graph: &str) -> GraphGrants {
        let mut grants = self.all_graphs.clone();
        if let Some(scoped) = self.graphs.get(graph) {
            grants.merge(scoped);
        }
        grants
    }
}

/// Shared by `DbInner` and `WriterState` (one `Arc` per database handle):
/// config plus the per-subject context cache. Cache entries are keyed by the
/// `GraphsState::security_epoch` current at resolution time — the writer and
/// the follower bump that epoch on every `__security` effect, so invalidation
/// is exact and the steady-state cost is one hash lookup.
pub(crate) struct SecurityEnforcer {
    pub enabled: bool,
    admins: BTreeSet<String>,
    cache: RwLock<HashMap<String, (u64, Arc<SecurityContext>)>>,
}

impl SecurityEnforcer {
    pub fn new(tuning: SecurityTuning) -> Arc<SecurityEnforcer> {
        Arc::new(SecurityEnforcer {
            enabled: tuning.enabled,
            admins: tuning.admins.into_iter().collect(),
            cache: RwLock::new(HashMap::new()),
        })
    }

    /// The grants to ENFORCE for `user` on `graph`, or `None` when no
    /// filtering applies: security disabled, no principal (the embedded
    /// process owner — `user` empty), a configured bootstrap admin, or a
    /// principal holding `ADMIN`.
    pub async fn enforcement_for(
        &self,
        state: &Arc<RwLock<GraphsState>>,
        store: &Arc<dyn ObjectStore>,
        user: &str,
        graph: &str,
        now: Instant,
    ) -> Result<Option<GraphGrants>, EngineError> {
        if !self.enforces(user) {
            return Ok(None);
        }
        let ctx = self.resolve_cached(state, store, user, now).await?;
        if ctx.admin {
            return Ok(None);
        }
        let grants = ctx.graph_grants(graph);
        if grants.unrestricted() {
            return Ok(None);
        }
        Ok(Some(grants))
    }

    /// May `user` run security DDL, `SHOW` statements, and catalog DDL?
    pub async fn is_admin(
        &self,
        state: &Arc<RwLock<GraphsState>>,
        store: &Arc<dyn ObjectStore>,
        user: &str,
        now: Instant,
    ) -> Result<bool, EngineError> {
        if !self.enforces(user) {
            return Ok(true);
        }
        Ok(self.resolve_cached(state, store, user, now).await?.admin)
    }

    fn enforces(&self, user: &str) -> bool {
        self.enabled && !user.is_empty() && !self.admins.contains(user)
    }

    async fn resolve_cached(
        &self,
        state: &Arc<RwLock<GraphsState>>,
        store: &Arc<dyn ObjectStore>,
        user: &str,
        now: Instant,
    ) -> Result<Arc<SecurityContext>, EngineError> {
        let epoch = state
            .read()
            .map_err(|_| EngineError::Poisoned)?
            .security_epoch;
        {
            let cache = self.cache.read().map_err(|_| EngineError::Poisoned)?;
            if let Some((cached_epoch, ctx)) = cache.get(user) {
                if *cached_epoch == epoch {
                    return Ok(Arc::clone(ctx));
                }
            }
        }
        let policy = load_policy(state, store, now).await?;
        let ctx = Arc::new(policy.context_for_subject(user));
        self.cache
            .write()
            .map_err(|_| EngineError::Poisoned)?
            .insert(user.to_string(), (epoch, Arc::clone(&ctx)));
        Ok(ctx)
    }
}

// ---- deterministic policy-graph ids -----------------------------------------
//
// Every policy entity's `_id` is derived from its identity, so DDL is
// idempotent by construction (a re-grant resolves to the same iid) and
// revokes address exactly the edge they mean. Role names are GQL identifiers
// (no `:` or `>`), so the separators below cannot collide.

pub(crate) fn user_node_id(subject: &str) -> String {
    format!("user:{subject}")
}

pub(crate) fn role_node_id(name: &str) -> String {
    format!("role:{name}")
}

pub(crate) fn privilege_node_id(action: &str, graph: &str, kind: &str, name: &str) -> String {
    format!("priv:{action}:{kind}:{graph}:{name}")
}

pub(crate) fn member_edge_id(from_node_id: &str, role_node_id: &str) -> String {
    format!("member:{from_node_id}>{role_node_id}")
}

pub(crate) fn grant_edge_id(role_node_id: &str, privilege_node_id: &str) -> String {
    format!("grant:{role_node_id}>{privilege_node_id}")
}

pub(crate) fn user_node_doc(subject: &str) -> Doc {
    let mut doc = Doc::new();
    doc.insert("_id".into(), Value::Str(user_node_id(subject)));
    doc.insert("subject".into(), Value::Str(subject.to_string()));
    doc
}

pub(crate) fn role_node_doc(name: &str) -> Doc {
    let mut doc = Doc::new();
    doc.insert("_id".into(), Value::Str(role_node_id(name)));
    doc.insert("name".into(), Value::Str(name.to_string()));
    doc
}

pub(crate) fn privilege_node_doc(action: &str, graph: &str, kind: &str, name: &str) -> Doc {
    let mut doc = Doc::new();
    doc.insert(
        "_id".into(),
        Value::Str(privilege_node_id(action, graph, kind, name)),
    );
    doc.insert("action".into(), Value::Str(action.to_string()));
    doc.insert("graph".into(), Value::Str(graph.to_string()));
    doc.insert("kind".into(), Value::Str(kind.to_string()));
    doc.insert("name".into(), Value::Str(name.to_string()));
    doc
}

pub(crate) fn edge_only_doc(id: String) -> Doc {
    let mut doc = Doc::new();
    doc.insert("_id".into(), Value::Str(id));
    doc
}

pub(crate) fn security_iid(table: TableKind, id: &str) -> Result<Iid, EngineError> {
    Ok(Iid::derive(
        SECURITY_GRAPH,
        table.name(),
        &Value::Str(id.to_string()).id_bytes()?,
    ))
}

// ---- policy snapshot ---------------------------------------------------------

/// One visible `__security` entity at the resolution bounds.
struct PolicyEntity {
    iid: Iid,
    labels: Vec<String>,
    doc: Doc,
    src: Option<Iid>,
    dst: Option<Iid>,
}

/// The whole policy graph, resolved at one instant. Small by construction
/// (users × roles × grants), so loading it wholesale is cheap and the
/// traversal below is plain in-memory BFS.
pub(crate) struct Policy {
    users: BTreeMap<String, Iid>,
    roles: BTreeMap<Iid, String>,
    privileges: BTreeMap<Iid, PrivilegeDoc>,
    member_of: BTreeMap<Iid, Vec<Iid>>,
    granted: BTreeMap<Iid, Vec<Iid>>,
}

struct PrivilegeDoc {
    action: String,
    graph: String,
    kind: String,
    name: String,
}

pub(crate) async fn load_policy(
    state: &Arc<RwLock<GraphsState>>,
    store: &Arc<dyn ObjectStore>,
    now: Instant,
) -> Result<Policy, EngineError> {
    let bounds = TemporalBounds {
        valid: TemporalDimension::at(now),
        system: TemporalDimension::at(now),
    };
    let nodes = policy_entities(state, store, TableKind::Nodes, &bounds).await?;
    let edges = policy_entities(state, store, TableKind::Edges, &bounds).await?;

    let mut policy = Policy {
        users: BTreeMap::new(),
        roles: BTreeMap::new(),
        privileges: BTreeMap::new(),
        member_of: BTreeMap::new(),
        granted: BTreeMap::new(),
    };
    for entity in nodes {
        if entity.labels.iter().any(|l| l == USER_LABEL) {
            if let Some(Value::Str(subject)) = entity.doc.get("subject") {
                policy.users.insert(subject.clone(), entity.iid);
            }
        } else if entity.labels.iter().any(|l| l == ROLE_LABEL) {
            if let Some(Value::Str(name)) = entity.doc.get("name") {
                policy.roles.insert(entity.iid, name.clone());
            }
        } else if entity.labels.iter().any(|l| l == PRIVILEGE_LABEL) {
            let field = |key: &str| match entity.doc.get(key) {
                Some(Value::Str(s)) => s.clone(),
                _ => String::new(),
            };
            policy.privileges.insert(
                entity.iid,
                PrivilegeDoc {
                    action: field("action"),
                    graph: field("graph"),
                    kind: field("kind"),
                    name: field("name"),
                },
            );
        }
    }
    for entity in edges {
        let (Some(src), Some(dst)) = (entity.src, entity.dst) else {
            continue;
        };
        if entity.labels.iter().any(|l| l == MEMBER_OF_EDGE) {
            policy.member_of.entry(src).or_default().push(dst);
        } else if entity.labels.iter().any(|l| l == GRANTED_EDGE) {
            policy.granted.entry(src).or_default().push(dst);
        }
    }
    Ok(policy)
}

impl Policy {
    pub fn role_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.roles.values().cloned().collect();
        names.sort();
        names
    }

    /// Transitive `MEMBER_OF*` closure starting from the given policy nodes.
    fn transitive_roles(&self, start: impl IntoIterator<Item = Iid>) -> BTreeSet<Iid> {
        let mut queue: Vec<Iid> = start.into_iter().collect();
        let mut roles = BTreeSet::new();
        while let Some(node) = queue.pop() {
            if let Some(next) = self.member_of.get(&node) {
                for role in next {
                    if self.roles.contains_key(role) && roles.insert(*role) {
                        queue.push(*role);
                    }
                }
            }
        }
        roles
    }

    fn context_for_subject(&self, subject: &str) -> SecurityContext {
        let mut ctx = SecurityContext::default();
        let Some(user) = self.users.get(subject) else {
            return ctx; // unknown principal: deny-by-default (empty context)
        };
        for role in self.transitive_roles([*user]) {
            for privilege in self.granted.get(&role).into_iter().flatten() {
                if let Some(doc) = self.privileges.get(privilege) {
                    apply_privilege(&mut ctx, doc);
                }
            }
        }
        ctx
    }

    /// `SHOW GRANTS [FOR USER 's' | FOR ROLE r]` rows: `(role, action, graph,
    /// kind, name)` sorted. `None` lists every role's direct grants; a target
    /// lists the transitive closure reachable from it.
    pub fn grant_rows(&self, target: Option<&RoleTarget>) -> Vec<GrantRow> {
        let roles: Vec<Iid> = match target {
            None => self.roles.keys().copied().collect(),
            Some(RoleTarget::User(subject)) => match self.users.get(subject) {
                Some(user) => self.transitive_roles([*user]).into_iter().collect(),
                None => Vec::new(),
            },
            Some(RoleTarget::Role(name)) => {
                match self.roles.iter().find(|(_, n)| n.as_str() == name) {
                    Some((iid, _)) => {
                        let mut roles = self.transitive_roles([*iid]);
                        roles.insert(*iid);
                        roles.into_iter().collect()
                    }
                    None => Vec::new(),
                }
            }
        };
        let mut rows = Vec::new();
        for role in roles {
            let Some(role_name) = self.roles.get(&role) else {
                continue;
            };
            for privilege in self.granted.get(&role).into_iter().flatten() {
                if let Some(doc) = self.privileges.get(privilege) {
                    rows.push(GrantRow {
                        role: role_name.clone(),
                        action: doc.action.clone(),
                        graph: doc.graph.clone(),
                        kind: doc.kind.clone(),
                        name: doc.name.clone(),
                    });
                }
            }
        }
        rows.sort();
        rows
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct GrantRow {
    pub role: String,
    pub action: String,
    pub graph: String,
    pub kind: String,
    pub name: String,
}

fn apply_privilege(ctx: &mut SecurityContext, doc: &PrivilegeDoc) {
    if doc.action == "admin" {
        ctx.admin = true;
        return;
    }
    let scope = if doc.graph == WILDCARD {
        &mut ctx.all_graphs
    } else {
        ctx.graphs.entry(doc.graph.clone()).or_default()
    };
    let grants = match (doc.action.as_str(), doc.kind.as_str()) {
        ("read", "nodes") => &mut scope.read_nodes,
        ("read", "edges") => &mut scope.read_edges,
        ("write", "nodes") => &mut scope.write_nodes,
        ("write", "edges") => &mut scope.write_edges,
        _ => return, // unknown action/kind: ignore (forward compatibility)
    };
    if doc.name == WILDCARD {
        grants.wildcard = true;
    } else {
        grants.names.insert(doc.name.clone());
    }
}

/// All visible `__security` entities of one table at `bounds` — the same
/// live ∪ persisted merge as `merged_snapshot`, but yielding decoded
/// entities instead of an Arrow batch (the traversal wants docs, not rows).
async fn policy_entities(
    state: &Arc<RwLock<GraphsState>>,
    store: &Arc<dyn ObjectStore>,
    kind: TableKind,
    bounds: &TemporalBounds,
) -> Result<Vec<PolicyEntity>, EngineError> {
    let (live_events, tries) = {
        let shared = state.read().map_err(|_| EngineError::Poisoned)?;
        let graph = shared
            .graph(SECURITY_GRAPH)
            .ok_or_else(|| EngineError::UnknownGraph(SECURITY_GRAPH.to_string()))?;
        let core = graph.core(kind);
        let live_events: Vec<(Iid, Vec<Event>)> = core
            .live
            .entities()
            .map(|(iid, events)| (*iid, events.to_vec()))
            .collect();
        (live_events, core.tries.clone())
    };
    let mut blocks: Vec<Vec<Event>> = Vec::new();
    for trie in &tries {
        let data_key = keys::data_key(SECURITY_GRAPH, kind.name(), &trie.entry.trie_key);
        let mut block_events = Vec::new();
        for page in trie.pages.iter().filter(|page| page.selected(bounds, None)) {
            let bytes = store
                .get_range(&data_key, page.offset..page.offset + page.len)
                .await?;
            block_events.extend(decode_events(&bytes)?);
        }
        blocks.push(block_events);
    }
    let merged = varve_index::merge_sources(blocks, live_events);
    let mut out = Vec::new();
    for (iid, events) in &merged {
        for version in varve_index::resolve(events, bounds) {
            let Op::Put { labels, doc } = &version.event.op else {
                continue;
            };
            out.push(PolicyEntity {
                iid: *iid,
                labels: labels.clone(),
                doc: doc.clone(),
                src: version.event.src,
                dst: version.event.dst,
            });
            break; // point bounds: one visible version per entity
        }
    }
    Ok(out)
}
