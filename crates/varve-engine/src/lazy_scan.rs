//! Page-at-a-time table scan for read queries. The eager
//! [`crate::scan::merged_snapshot`] copies every event, groups the whole
//! table per entity and builds every column before DataFusion sees a row, so
//! `LIMIT 5` costs the same as the full result. This scan hands DataFusion a
//! `TableProvider` instead: the schema comes from the per-block property
//! catalog, pages are fetched as the k-way merge over blocks and tails
//! reaches them, entities are resolved a chunk at a time, and only the
//! projected columns are built. Dropping the stream (a satisfied `LIMIT`)
//! stops the work.

use crate::db::EngineError;
use crate::page_cache::PageCache;
use crate::scan::{label_candidates, IidSel};
use crate::state::{GraphsState, ScanStats, TableKind};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::Statistics;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr as DfExpr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use varve_index::{
    decode_events, snapshot_projected, Event, LabelFilter, OwnedLabelFilter, PageMeta, PropSchema,
    SnapshotEntity,
};
use varve_storage::{keys, ObjectStore};
use varve_types::{Iid, TemporalBounds};

/// Entities resolved per output batch.
const CHUNK_ENTITIES: usize = 2048;
/// Pages in flight ahead of each block's cursor.
const PREFETCH_PAGES: usize = 4;

pub(crate) enum LazyPlan {
    /// The selector admits no entity: the element matches nothing.
    Empty,
    /// The table has no fixed schema (a type conflict in its history), so the
    /// caller keeps the eager scan and its visible-rows-only error.
    Eager,
    Lazy(Arc<ScanPlan>),
}

struct TrieSource {
    data_key: Arc<str>,
    /// Pages the selector keeps, in file order.
    pages: Vec<PageMeta>,
}

/// One table scan at fixed bounds, snapshotted under a single read lock.
pub(crate) struct ScanPlan {
    store: Arc<dyn ObjectStore>,
    page_cache: Arc<PageCache>,
    stats: Arc<ScanStats>,
    kind: TableKind,
    /// Persisted blocks, oldest first.
    tries: Vec<TrieSource>,
    /// Unflushed rows in block file order (iid asc, system_from desc):
    /// the sealed tail (if any) then the live tail.
    tails: Vec<Arc<Vec<Event>>>,
    sel: IidSel,
    label: OwnedLabelFilter,
    bounds: TemporalBounds,
    /// Unmangled output schema: the fixed columns then the catalog's
    /// properties in name order (the eager snapshot's order).
    fields: Vec<Field>,
    rows_estimate: usize,
}

/// Fills the property catalog of any block that predates it, once per
/// process: decodes the block's pages (through the page cache) and records
/// the result in the in-memory inventory. Blocks flushed or compacted by
/// this version carry the catalog as a sidecar and never take this path.
pub(crate) async fn ensure_block_catalogs(
    state: &Arc<RwLock<GraphsState>>,
    store: &Arc<dyn ObjectStore>,
    graph: &str,
    kind: TableKind,
) -> Result<(), EngineError> {
    let (missing, page_cache, stats) = {
        let s = state.read().map_err(|_| EngineError::Poisoned)?;
        let Some(table) = s.graph(graph) else {
            return Ok(());
        };
        let missing: Vec<(String, Arc<Vec<PageMeta>>)> = table
            .core(kind)
            .tries
            .iter()
            .filter(|trie| trie.props.is_none())
            .map(|trie| (trie.entry.trie_key.clone(), Arc::clone(&trie.pages)))
            .collect();
        (
            missing,
            Arc::clone(&s.page_cache),
            Arc::clone(&s.scan_stats),
        )
    };
    if missing.is_empty() {
        return Ok(());
    }
    let mut derived = Vec::with_capacity(missing.len());
    for (trie_key, pages) in missing {
        let data_key = keys::data_key(graph, kind.name(), &trie_key);
        let mut props = PropSchema::new();
        for page in pages.iter() {
            let events =
                fetch_page(store, &page_cache, &stats, &data_key, page, &IidSel::All).await?;
            for event in events.iter() {
                props.observe_event(event);
            }
        }
        derived.push((trie_key, Arc::new(props)));
    }
    let mut s = state.write().map_err(|_| EngineError::Poisoned)?;
    if let Some(table) = s.graph_mut(graph) {
        for trie in table.core_mut(kind).tries.iter_mut() {
            if trie.props.is_some() {
                continue;
            }
            if let Some((_, props)) = derived.iter().find(|(key, _)| *key == trie.entry.trie_key) {
                trie.props = Some(Arc::clone(props));
            }
        }
    }
    Ok(())
}

/// Snapshots the sources of a whole-table (or label-narrowed) scan under one
/// read lock. Same narrowing as the eager scan: a labelled scan prunes to the
/// entities the label indexes name.
pub(crate) fn plan_lazy_scan(
    state: &Arc<RwLock<GraphsState>>,
    store: &Arc<dyn ObjectStore>,
    graph: &str,
    kind: TableKind,
    label: &LabelFilter<'_>,
    bounds: &TemporalBounds,
    sel: &IidSel,
) -> Result<LazyPlan, EngineError> {
    let s = state.read().map_err(|_| EngineError::Poisoned)?;
    let table = s
        .graph(graph)
        .ok_or_else(|| EngineError::UnknownGraph(graph.to_string()))?;
    let core = table.core(kind);

    let mut props = PropSchema::new();
    for trie in &core.tries {
        match &trie.props {
            Some(block_props) => props.merge(block_props),
            None => return Ok(LazyPlan::Eager),
        }
    }
    if let Some(sealed) = &core.sealed {
        props.merge(sealed.prop_schema());
    }
    props.merge(core.live.prop_schema());
    let Some(prop_fields) = props.fields() else {
        return Ok(LazyPlan::Eager);
    };

    let narrowed = match (sel, label.needed_labels()) {
        (IidSel::All, Some(needed)) => label_candidates(
            &core.tries,
            &core.live,
            core.sealed.as_deref(),
            None,
            &needed,
        )
        .map(|set| IidSel::Set(Arc::new(set))),
        _ => None,
    };
    let sel = narrowed.as_ref().unwrap_or(sel).clone();
    if let IidSel::Set(set) = &sel {
        if set.is_empty() {
            return Ok(LazyPlan::Empty);
        }
    }

    let tail_of = |live: &varve_index::LiveTable| -> Arc<Vec<Event>> {
        let mut rows = Vec::new();
        for (iid, events) in live.entities() {
            if sel.admits(iid) {
                rows.extend(events.iter().rev().cloned());
            }
        }
        Arc::new(rows)
    };
    let mut tails = Vec::new();
    if let Some(sealed) = &core.sealed {
        tails.push(tail_of(sealed));
    }
    tails.push(tail_of(&core.live));

    let mut tries = Vec::with_capacity(core.tries.len());
    for trie in &core.tries {
        if sel.filtered_out(trie.keys.as_deref()) {
            s.scan_stats.record_skipped_block();
            continue;
        }
        let pages: Vec<PageMeta> = trie
            .pages
            .iter()
            .filter(|page| sel.selects_page(page, bounds))
            .cloned()
            .collect();
        if pages.is_empty() {
            continue;
        }
        tries.push(TrieSource {
            data_key: Arc::from(keys::data_key(graph, kind.name(), &trie.entry.trie_key)),
            pages,
        });
    }
    let rows_estimate = tries
        .iter()
        .flat_map(|trie| trie.pages.iter())
        .map(|page| page.rows as usize)
        .sum::<usize>()
        + tails.iter().map(|tail| tail.len()).sum::<usize>();
    if rows_estimate == 0 {
        return Ok(LazyPlan::Empty);
    }

    Ok(LazyPlan::Lazy(Arc::new(ScanPlan {
        store: Arc::clone(store),
        page_cache: Arc::clone(&s.page_cache),
        stats: Arc::clone(&s.scan_stats),
        kind,
        tries,
        tails,
        sel,
        label: label.to_owned_filter(),
        bounds: *bounds,
        fields: scan_fields(kind, prop_fields),
        rows_estimate,
    })))
}

fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

fn scan_fields(kind: TableKind, props: Vec<Field>) -> Vec<Field> {
    let mut fields = vec![Field::new("_iid", DataType::FixedSizeBinary(16), false)];
    for name in ["_system_from", "_system_to", "_valid_from", "_valid_to"] {
        fields.push(Field::new(name, timestamp_type(), false));
    }
    if kind == TableKind::Edges {
        fields.push(Field::new("_src_iid", DataType::FixedSizeBinary(16), false));
        fields.push(Field::new("_dst_iid", DataType::FixedSizeBinary(16), false));
    }
    fields.push(Field::new(
        "_labels",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        false,
    ));
    fields.extend(props);
    fields
}

/// One page through the decoded-page cache, as a shared vector the cursor
/// borrows rows from. A whole-page decode is cached; a narrowed keyed decode
/// (sparse set) materializes only admitted rows and is not.
async fn fetch_page(
    store: &Arc<dyn ObjectStore>,
    cache: &PageCache,
    stats: &ScanStats,
    key: &str,
    page: &PageMeta,
    sel: &IidSel,
) -> Result<Arc<Vec<Event>>, EngineError> {
    if let Some(cached) = cache.get(key, page.offset) {
        stats.record_cached_page();
        return Ok(cached);
    }
    let bytes = store
        .get_range(key, page.offset..page.offset + page.len)
        .await?;
    if sel.is_narrow() && !sel.decode_whole(page) {
        let admits = |iid: Iid| sel.admits(&iid);
        let decoded =
            varve_index::decode_events_keyed(&bytes, varve_index::SortOrder::ByIid, &admits)?;
        stats.record_page(decoded.len());
        return Ok(Arc::new(decoded));
    }
    let decoded = Arc::new(decode_events(&bytes)?);
    stats.record_page(decoded.len());
    cache.insert(key, page.offset, Arc::clone(&decoded));
    Ok(decoded)
}

type PageFuture = Pin<Box<dyn Future<Output = Result<Arc<Vec<Event>>, EngineError>> + Send>>;

enum Pages {
    /// A tail is a single in-memory page.
    Tail(Option<Arc<Vec<Event>>>),
    Trie {
        source: usize,
        next: usize,
        prefetch: FuturesOrdered<PageFuture>,
    },
}

/// A source's read position: the current page and the next admitted row.
struct Cursor {
    pages: Pages,
    current: Option<Arc<Vec<Event>>>,
    pos: usize,
    exhausted: bool,
}

/// One entity's rows within one page, in file order (newest first).
struct Run {
    page: Arc<Vec<Event>>,
    start: usize,
    end: usize,
}

struct Entity {
    iid: Iid,
    /// Newest source first, file order within a source.
    runs: Vec<Run>,
}

impl Cursor {
    fn tail(page: Arc<Vec<Event>>) -> Cursor {
        Cursor {
            pages: Pages::Tail(Some(page)),
            current: None,
            pos: 0,
            exhausted: false,
        }
    }

    fn trie(source: usize) -> Cursor {
        Cursor {
            pages: Pages::Trie {
                source,
                next: 0,
                prefetch: FuturesOrdered::new(),
            },
            current: None,
            pos: 0,
            exhausted: false,
        }
    }

    /// Positions the cursor on its next admitted row, loading pages as
    /// needed; `exhausted` once nothing is left.
    async fn settle(&mut self, plan: &Arc<ScanPlan>) -> Result<(), EngineError> {
        loop {
            if self.exhausted {
                return Ok(());
            }
            if let Some(page) = &self.current {
                while self.pos < page.len() && !plan.sel.admits(&page[self.pos].iid) {
                    self.pos += 1;
                }
                if self.pos < page.len() {
                    return Ok(());
                }
            }
            self.current = self.next_page(plan).await?;
            self.pos = 0;
            if self.current.is_none() {
                self.exhausted = true;
            }
        }
    }

    async fn next_page(
        &mut self,
        plan: &Arc<ScanPlan>,
    ) -> Result<Option<Arc<Vec<Event>>>, EngineError> {
        match &mut self.pages {
            Pages::Tail(page) => Ok(page.take()),
            Pages::Trie {
                source,
                next,
                prefetch,
            } => {
                let trie = &plan.tries[*source];
                while prefetch.len() < PREFETCH_PAGES && *next < trie.pages.len() {
                    let plan = Arc::clone(plan);
                    let source = *source;
                    let index = *next;
                    prefetch.push_back(Box::pin(async move {
                        let trie = &plan.tries[source];
                        fetch_page(
                            &plan.store,
                            &plan.page_cache,
                            &plan.stats,
                            &trie.data_key,
                            &trie.pages[index],
                            &plan.sel,
                        )
                        .await
                    }));
                    *next += 1;
                }
                match prefetch.next().await {
                    Some(page) => Ok(Some(page?)),
                    None => Ok(None),
                }
            }
        }
    }

    fn peek(&self) -> Option<Iid> {
        if self.exhausted {
            return None;
        }
        self.current.as_ref().map(|page| page[self.pos].iid)
    }

    /// The rows of `iid` from the current position to the end of its run on
    /// this page. The run may continue on the next page: the caller settles
    /// and peeks again.
    fn take_run(&mut self, iid: Iid) -> Option<Run> {
        let page = self.current.as_ref()?;
        let start = self.pos;
        let mut end = start;
        while end < page.len() && page[end].iid == iid {
            end += 1;
        }
        self.pos = end;
        Some(Run {
            page: Arc::clone(page),
            start,
            end,
        })
    }
}

struct ScanStream {
    plan: Arc<ScanPlan>,
    /// Projected, unmangled.
    fields: Vec<Field>,
    schema: SchemaRef,
    /// Oldest source first: blocks, then the sealed tail, then the live tail.
    cursors: Vec<Cursor>,
    remaining: Option<usize>,
    chunk: usize,
    done: bool,
}

impl ScanStream {
    fn new(
        plan: Arc<ScanPlan>,
        fields: Vec<Field>,
        schema: SchemaRef,
        limit: Option<usize>,
    ) -> ScanStream {
        let mut cursors: Vec<Cursor> = (0..plan.tries.len()).map(Cursor::trie).collect();
        cursors.extend(plan.tails.iter().map(|tail| Cursor::tail(Arc::clone(tail))));
        ScanStream {
            chunk: limit.map_or(CHUNK_ENTITIES, |l| l.clamp(256, CHUNK_ENTITIES)),
            plan,
            fields,
            schema,
            cursors,
            remaining: limit,
            done: false,
        }
    }

    /// The next `chunk` entities in ascending iid order, each with its runs
    /// newest-source-first.
    async fn next_entities(&mut self) -> Result<Vec<Entity>, EngineError> {
        let mut entities = Vec::with_capacity(self.chunk);
        while entities.len() < self.chunk {
            for cursor in &mut self.cursors {
                cursor.settle(&self.plan).await?;
            }
            let Some(iid) = self.cursors.iter().filter_map(Cursor::peek).min() else {
                self.done = true;
                break;
            };
            let mut runs = Vec::new();
            for cursor in self.cursors.iter_mut().rev() {
                while cursor.peek() == Some(iid) {
                    if let Some(run) = cursor.take_run(iid) {
                        runs.push(run);
                    }
                    cursor.settle(&self.plan).await?;
                }
            }
            entities.push(Entity { iid, runs });
        }
        Ok(entities)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, EngineError> {
        loop {
            if self.done || self.remaining == Some(0) {
                return Ok(None);
            }
            let entities = self.next_entities().await?;
            if entities.is_empty() {
                continue;
            }
            let borrowed: Vec<SnapshotEntity<'_>> = entities
                .iter()
                .map(|entity| SnapshotEntity {
                    iid: entity.iid,
                    events: entity
                        .runs
                        .iter()
                        .flat_map(|run| run.page[run.start..run.end].iter())
                        .collect(),
                })
                .collect();
            let (rows, columns) =
                snapshot_projected(&borrowed, &self.plan.label, &self.plan.bounds, &self.fields)?;
            if rows == 0 {
                continue;
            }
            let take = self.remaining.map_or(rows, |left| left.min(rows));
            if let Some(left) = &mut self.remaining {
                *left -= take;
            }
            let columns = if take < rows {
                columns.iter().map(|column| column.slice(0, take)).collect()
            } else {
                columns
            };
            let batch = RecordBatch::try_new_with_options(
                Arc::clone(&self.schema),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(take)),
            )
            .map_err(varve_index::IndexError::Arrow)?;
            return Ok(Some(batch));
        }
    }
}

/// The DataFusion table over one element's scan; its schema is already
/// mangled with the element variable.
pub(crate) struct LazyScanTable {
    plan: Arc<ScanPlan>,
    schema: SchemaRef,
}

impl LazyScanTable {
    pub(crate) fn new(plan: Arc<ScanPlan>, var: &str) -> LazyScanTable {
        let fields: Vec<Field> = plan
            .fields
            .iter()
            .map(|field| {
                Field::new(
                    varve_plan::mangled(var, field.name()),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect();
        LazyScanTable {
            plan,
            schema: Arc::new(Schema::new(fields)),
        }
    }

    pub(crate) fn rows_estimate(&self) -> usize {
        self.plan.rows_estimate
    }
}

impl std::fmt::Debug for LazyScanTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyScanTable")
            .field("table", &self.plan.kind.name())
            .field("rows_estimate", &self.plan.rows_estimate)
            .finish()
    }
}

#[async_trait]
impl TableProvider for LazyScanTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let projection: Vec<usize> = match projection {
            Some(projection) => projection.clone(),
            None => (0..self.schema.fields().len()).collect(),
        };
        Ok(Arc::new(LazyScanExec::new(
            Arc::clone(&self.plan),
            &self.schema,
            projection,
            limit,
        )))
    }

    fn statistics(&self) -> Option<Statistics> {
        Some(
            Statistics::new_unknown(&self.schema)
                .with_num_rows(Precision::Inexact(self.plan.rows_estimate)),
        )
    }
}

struct LazyScanExec {
    plan: Arc<ScanPlan>,
    /// Projected, unmangled.
    fields: Vec<Field>,
    schema: SchemaRef,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl LazyScanExec {
    fn new(
        plan: Arc<ScanPlan>,
        mangled: &SchemaRef,
        projection: Vec<usize>,
        limit: Option<usize>,
    ) -> LazyScanExec {
        let fields: Vec<Field> = projection.iter().map(|i| plan.fields[*i].clone()).collect();
        let schema = Arc::new(Schema::new(
            projection
                .iter()
                .map(|i| mangled.field(*i).clone())
                .collect::<Vec<_>>(),
        ));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        LazyScanExec {
            plan,
            fields,
            schema,
            limit,
            properties,
        }
    }
}

impl std::fmt::Debug for LazyScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyScanExec")
            .field("table", &self.plan.kind.name())
            .field("limit", &self.limit)
            .finish()
    }
}

impl DisplayAs for LazyScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "VarveScanExec: table={}, blocks={}, columns={}, limit={:?}",
            self.plan.kind.name(),
            self.plan.tries.len(),
            self.fields.len(),
            self.limit
        )
    }
}

impl ExecutionPlan for LazyScanExec {
    fn name(&self) -> &str {
        "VarveScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let stream = ScanStream::new(
            Arc::clone(&self.plan),
            self.fields.clone(),
            Arc::clone(&self.schema),
            self.limit,
        );
        let batches = futures::stream::try_unfold(stream, |mut stream| async move {
            match stream.next_batch().await {
                Ok(Some(batch)) => Ok(Some((batch, stream))),
                Ok(None) => Ok(None),
                Err(err) => Err(DataFusionError::External(Box::new(err))),
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            batches,
        )))
    }
}
