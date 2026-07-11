use crate::db::EngineError;
use crate::state::{
    GraphsState, TableKind, TableState, DEFAULT_GRAPH, EDGES_TABLE, META_GRAPH, NODES_TABLE,
};
use std::collections::{BTreeMap, BTreeSet};
use varve_index::{decode_events, Event, IndexError, Op};
use varve_log::LogRecord;
use varve_types::{Iid, Instant, Value};

pub(crate) struct DecodedLogRecord {
    pub tx_id: u64,
    pub system_time: Instant,
    pub effects: Vec<DecodedTableEffects>,
}

pub(crate) struct DecodedTableEffects {
    pub graph: String,
    pub table: TableKind,
    pub events: Vec<Event>,
}

pub(crate) fn decode_log_record(record: &LogRecord) -> Result<DecodedLogRecord, EngineError> {
    let mut effects = Vec::with_capacity(record.effects.len());
    for effect in &record.effects {
        let table = match effect.table.as_str() {
            NODES_TABLE => TableKind::Nodes,
            EDGES_TABLE => TableKind::Edges,
            other => return Err(EngineError::UnknownTable(other.to_string())),
        };
        effects.push(DecodedTableEffects {
            graph: if effect.graph.is_empty() {
                DEFAULT_GRAPH.to_string()
            } else {
                effect.graph.clone()
            },
            table,
            events: decode_events(&effect.arrow_ipc)?,
        });
    }
    Ok(DecodedLogRecord {
        tx_id: record.tx_id,
        system_time: Instant::from_micros(record.system_time_us),
        effects,
    })
}

pub(crate) fn apply_decoded_log_record(
    state: &mut GraphsState,
    record: DecodedLogRecord,
) -> Result<(), EngineError> {
    validate_decoded_log_record(state, &record)?;
    for effect in record.effects {
        for event in effect.events {
            if effect.graph == META_GRAPH && effect.table == TableKind::Nodes {
                apply_catalog_event(state, &event);
            }
            let table_state = state
                .graph_mut(&effect.graph)
                .ok_or_else(|| EngineError::UnknownGraph(effect.graph.clone()))?;
            table_state.core_mut(effect.table).live.append(event)?;
        }
    }
    Ok(())
}

fn validate_decoded_log_record(
    state: &GraphsState,
    record: &DecodedLogRecord,
) -> Result<(), EngineError> {
    let mut graphs = state.graphs.keys().cloned().collect::<BTreeSet<_>>();
    let mut catalog_graphs = state.catalog_graphs.clone();
    let mut last_system_from = BTreeMap::new();
    for (graph, tables) in &state.graphs {
        for table in [TableKind::Nodes, TableKind::Edges] {
            if let Some(last) = tables.core(table).live.last_system_from() {
                last_system_from.insert((graph.clone(), table.name()), last);
            }
        }
    }

    for effect in &record.effects {
        if !graphs.contains(&effect.graph) {
            return Err(EngineError::UnknownGraph(effect.graph.clone()));
        }
        for event in &effect.events {
            if effect.graph == META_GRAPH && effect.table == TableKind::Nodes {
                project_catalog_event(
                    &mut graphs,
                    &mut catalog_graphs,
                    &mut last_system_from,
                    event,
                );
            }
            let key = (effect.graph.clone(), effect.table.name());
            if let Some(last) = last_system_from.get(&key) {
                if event.system_from < *last {
                    return Err(IndexError::OutOfOrderEvent {
                        last: *last,
                        got: event.system_from,
                    }
                    .into());
                }
            }
            last_system_from.insert(key, event.system_from);
        }
    }
    Ok(())
}

fn project_catalog_event(
    graphs: &mut BTreeSet<String>,
    catalog_graphs: &mut BTreeMap<Iid, String>,
    last_system_from: &mut BTreeMap<(String, &'static str), Instant>,
    event: &Event,
) {
    match catalog_mutation(event) {
        CatalogMutation::Put { iid, name } => {
            catalog_graphs.insert(iid, name.to_string());
            graphs.insert(name.to_string());
        }
        CatalogMutation::Remove { iid } => {
            let Some(name) = catalog_graphs.remove(&iid) else {
                return;
            };
            graphs.remove(&name);
            last_system_from.remove(&(name.clone(), NODES_TABLE));
            last_system_from.remove(&(name, EDGES_TABLE));
        }
        CatalogMutation::None => {}
    }
}

pub(crate) fn apply_catalog_event(state: &mut GraphsState, event: &Event) {
    match catalog_mutation(event) {
        CatalogMutation::Put { iid, name } => {
            state.catalog_graphs.insert(iid, name.to_string());
            state
                .graphs
                .entry(name.to_string())
                .or_insert_with(TableState::new);
        }
        CatalogMutation::Remove { iid } => {
            let Some(name) = state.catalog_graphs.remove(&iid) else {
                return;
            };
            state.graphs.remove(&name);
        }
        CatalogMutation::None => {}
    }
}

enum CatalogMutation<'a> {
    Put { iid: Iid, name: &'a str },
    Remove { iid: Iid },
    None,
}

fn catalog_mutation(event: &Event) -> CatalogMutation<'_> {
    match &event.op {
        Op::Put { labels, doc } if labels.iter().any(|label| label == "Graph") => {
            let Some(Value::Str(name)) = doc.get("_id") else {
                return CatalogMutation::None;
            };
            if name == DEFAULT_GRAPH || name == META_GRAPH {
                return CatalogMutation::None;
            }
            CatalogMutation::Put {
                iid: event.iid,
                name,
            }
        }
        Op::Delete | Op::Erase => CatalogMutation::Remove { iid: event.iid },
        _ => CatalogMutation::None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_decoded_log_record, decode_log_record, DecodedLogRecord, DecodedTableEffects,
    };
    use crate::state::{
        GraphsState, TableKind, DEFAULT_GRAPH, EDGES_TABLE, META_GRAPH, NODES_TABLE,
    };
    use crate::EngineError;
    use varve_index::{encode_events, Event, Op};
    use varve_log::{LogRecord, TableEffects};
    use varve_types::{Doc, Iid, Instant, Value};

    fn put_event(id: u8) -> Event {
        put_event_for(NODES_TABLE, id, 1)
    }

    fn put_event_for(table: &str, id: u8, system_time_us: i64) -> Event {
        Event {
            iid: Iid::derive(DEFAULT_GRAPH, table, &[id]),
            system_from: Instant::from_micros(system_time_us),
            valid_from: Instant::from_micros(system_time_us),
            valid_to: Instant::END_OF_TIME,
            src: None,
            dst: None,
            op: Op::Put {
                labels: vec!["P".into()],
                doc: Doc::new(),
            },
        }
    }

    fn catalog_event(name: &str, op: Op) -> Event {
        Event {
            iid: Iid::derive(META_GRAPH, NODES_TABLE, name.as_bytes()),
            system_from: Instant::from_micros(1),
            valid_from: Instant::from_micros(1),
            valid_to: Instant::END_OF_TIME,
            src: None,
            dst: None,
            op,
        }
    }

    fn decoded_catalog_put(name: &str) -> DecodedLogRecord {
        let mut doc = Doc::new();
        doc.insert("_id".into(), Value::Str(name.into()));
        decoded_catalog_event(catalog_event(
            name,
            Op::Put {
                labels: vec!["Graph".into()],
                doc,
            },
        ))
    }

    fn decoded_catalog_delete(name: &str) -> DecodedLogRecord {
        decoded_catalog_event(catalog_event(name, Op::Delete))
    }

    fn decoded_catalog_erase(name: &str) -> DecodedLogRecord {
        decoded_catalog_event(catalog_event(name, Op::Erase))
    }

    fn decoded_catalog_event(event: Event) -> DecodedLogRecord {
        DecodedLogRecord {
            tx_id: 1,
            system_time: Instant::from_micros(1),
            effects: vec![DecodedTableEffects {
                graph: META_GRAPH.into(),
                table: TableKind::Nodes,
                events: vec![event],
            }],
        }
    }

    #[test]
    fn malformed_later_effect_cannot_partially_apply_a_record() {
        let good = TableEffects {
            graph: DEFAULT_GRAPH.into(),
            table: NODES_TABLE.into(),
            arrow_ipc: encode_events(&[put_event(1)]).unwrap(),
        };
        let bad = TableEffects {
            graph: DEFAULT_GRAPH.into(),
            table: EDGES_TABLE.into(),
            arrow_ipc: vec![0xff, 0xff],
        };
        let record = LogRecord {
            tx_id: 1,
            system_time_us: 1,
            user: String::new(),
            effects: vec![good, bad],
        };
        let mut state = GraphsState::new();

        assert!(decode_log_record(&record)
            .and_then(|decoded| apply_decoded_log_record(&mut state, decoded))
            .is_err());
        assert_eq!(state.live_rows(), 0);
    }

    #[test]
    fn later_unknown_graph_cannot_partially_apply_a_record() {
        let mut state = GraphsState::new();
        let record = DecodedLogRecord {
            tx_id: 1,
            system_time: Instant::from_micros(1),
            effects: vec![
                DecodedTableEffects {
                    graph: DEFAULT_GRAPH.into(),
                    table: TableKind::Nodes,
                    events: vec![put_event(1)],
                },
                DecodedTableEffects {
                    graph: "missing".into(),
                    table: TableKind::Nodes,
                    events: vec![put_event(2)],
                },
            ],
        };

        assert!(matches!(
            apply_decoded_log_record(&mut state, record),
            Err(EngineError::UnknownGraph(graph)) if graph == "missing"
        ));
        assert_eq!(state.live_rows(), 0);
    }

    #[test]
    fn empty_effect_for_unknown_graph_is_an_explicit_error() {
        let mut state = GraphsState::new();
        let record = DecodedLogRecord {
            tx_id: 1,
            system_time: Instant::from_micros(1),
            effects: vec![DecodedTableEffects {
                graph: "missing".into(),
                table: TableKind::Nodes,
                events: Vec::new(),
            }],
        };

        assert!(matches!(
            apply_decoded_log_record(&mut state, record),
            Err(EngineError::UnknownGraph(graph)) if graph == "missing"
        ));
        assert_eq!(state.live_rows(), 0);
    }

    #[test]
    fn later_out_of_order_effect_cannot_partially_apply_a_record() {
        let mut state = GraphsState::new();
        let record = DecodedLogRecord {
            tx_id: 1,
            system_time: Instant::from_micros(2),
            effects: vec![
                DecodedTableEffects {
                    graph: DEFAULT_GRAPH.into(),
                    table: TableKind::Nodes,
                    events: vec![put_event_for(NODES_TABLE, 1, 2)],
                },
                DecodedTableEffects {
                    graph: DEFAULT_GRAPH.into(),
                    table: TableKind::Nodes,
                    events: vec![put_event_for(NODES_TABLE, 2, 1)],
                },
            ],
        };

        assert!(matches!(
            apply_decoded_log_record(&mut state, record),
            Err(EngineError::Index(
                varve_index::IndexError::OutOfOrderEvent { .. }
            ))
        ));
        assert_eq!(state.live_rows(), 0);
    }

    #[test]
    fn catalog_put_and_delete_change_the_graph_map() {
        let mut state = GraphsState::new();
        apply_decoded_log_record(&mut state, decoded_catalog_put("tenant_a")).unwrap();
        assert!(state.graph("tenant_a").is_some());
        apply_decoded_log_record(&mut state, decoded_catalog_delete("tenant_a")).unwrap();
        assert!(state.graph("tenant_a").is_none());
    }

    #[test]
    fn catalog_erase_cleans_up_the_iid_map_and_reserved_names_stay_reserved() {
        let mut state = GraphsState::new();
        apply_decoded_log_record(&mut state, decoded_catalog_put("tenant_a")).unwrap();
        assert_eq!(state.catalog_graphs.len(), 1);

        apply_decoded_log_record(&mut state, decoded_catalog_erase("tenant_a")).unwrap();
        assert!(state.graph("tenant_a").is_none());
        assert!(state.catalog_graphs.is_empty());

        apply_decoded_log_record(&mut state, decoded_catalog_put(DEFAULT_GRAPH)).unwrap();
        apply_decoded_log_record(&mut state, decoded_catalog_put(META_GRAPH)).unwrap();
        assert!(state.graph(DEFAULT_GRAPH).is_some());
        assert!(state.graph(META_GRAPH).is_some());
        assert!(state.catalog_graphs.is_empty());
    }

    #[test]
    fn empty_graph_falls_back_to_default_and_routes_nodes_and_edges() {
        let record = LogRecord {
            tx_id: 1,
            system_time_us: 1,
            user: String::new(),
            effects: vec![
                TableEffects {
                    graph: String::new(),
                    table: NODES_TABLE.into(),
                    arrow_ipc: encode_events(&[put_event_for(NODES_TABLE, 1, 1)]).unwrap(),
                },
                TableEffects {
                    graph: String::new(),
                    table: EDGES_TABLE.into(),
                    arrow_ipc: encode_events(&[put_event_for(EDGES_TABLE, 2, 1)]).unwrap(),
                },
            ],
        };
        let mut state = GraphsState::new();

        let decoded = decode_log_record(&record).unwrap();
        apply_decoded_log_record(&mut state, decoded).unwrap();

        let default = state.graph(DEFAULT_GRAPH).unwrap();
        assert_eq!(default.nodes.live.event_count(), 1);
        assert_eq!(default.edges.live.event_count(), 1);
    }
}
