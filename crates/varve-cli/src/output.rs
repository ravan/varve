//! Deterministic rendering for query results, transaction receipts, and
//! node status: the same functions back both the shell and (in later
//! tasks) any non-interactive `varve` subcommands.

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use varve_server::api::{
    CompactionResponse, GcResponse, SideEffectsResponse, StatusResponse, TxResponse, VerifyResponse,
};

use crate::client::CliError;

/// Renders query results as an Arrow pretty-printed ASCII table. An empty
/// result set (no batches, or batches with no rows) prints `(0 rows)`
/// instead of an empty table.
pub(crate) fn format_batches(batches: &[RecordBatch]) -> Result<String, CliError> {
    if batches.iter().all(|batch| batch.num_rows() == 0) {
        return Ok("(0 rows)".to_string());
    }
    Ok(pretty_format_batches(batches)?.to_string())
}

/// Renders a transaction receipt as `tx <id> @ <RFC3339-micros>`, followed
/// by one line per nonzero side-effect field in the brief's fixed order:
/// nodes created/deleted, relationships created/deleted, properties
/// set/removed, labels added/removed.
pub(crate) fn format_receipt(response: &TxResponse) -> String {
    let mut lines = vec![format!("tx {} @ {}", response.tx_id, response.system_time)];
    lines.extend(side_effect_lines(&response.side_effects));
    lines.join("\n")
}

fn side_effect_lines(effects: &SideEffectsResponse) -> Vec<String> {
    let fields: [(&str, usize); 8] = [
        ("nodes created", effects.nodes_created),
        ("nodes deleted", effects.nodes_deleted),
        ("relationships created", effects.relationships_created),
        ("relationships deleted", effects.relationships_deleted),
        ("properties set", effects.properties_set),
        ("properties removed", effects.properties_removed),
        ("labels added", effects.labels_added),
        ("labels removed", effects.labels_removed),
    ];
    fields
        .into_iter()
        .filter(|(_, count)| *count > 0)
        .map(|(label, count)| format!("  {label}: {count}"))
        .collect()
}

/// Renders a `StatusResponse` for `:status` (and, in later tasks, `varve
/// status`).
pub(crate) fn format_status(status: &StatusResponse) -> String {
    let mut lines = vec![
        format!("roles: {}", status.roles.join(", ")),
        format!("applied_tx_id: {}", status.applied_tx_id),
        format!("applied_log_position: {}", status.applied_log_position),
    ];
    match status.manifest_block_id {
        Some(block_id) => lines.push(format!("manifest_block_id: {block_id}")),
        None => lines.push("manifest_block_id: none".to_string()),
    }
    lines.push(format!("manifest_watermark: {}", status.manifest_watermark));
    if let Some(reason) = &status.follower_error {
        lines.push(format!("follower_error: {reason}"));
    }
    let mut probe_line = format!("probe: {}", status.probe.verdict);
    if let Some(reason) = &status.probe.reason {
        probe_line.push_str(&format!(" ({reason})"));
    }
    lines.push(probe_line);
    lines.join("\n")
}

/// Renders a `CompactionResponse` for `varve admin compact` in a fixed
/// field order.
pub(crate) fn format_compaction(response: &CompactionResponse) -> String {
    [
        format!("jobs: {}", response.jobs),
        format!("input_tries: {}", response.input_tries),
        format!("output_tries: {}", response.output_tries),
        format!("input_rows: {}", response.input_rows),
        format!("output_rows: {}", response.output_rows),
    ]
    .join("\n")
}

/// Renders a `GcResponse` for `varve admin gc` in a fixed field order.
pub(crate) fn format_gc(response: &GcResponse) -> String {
    [
        format!("planned_objects: {}", response.planned_objects),
        format!("deleted_objects: {}", response.deleted_objects),
    ]
    .join("\n")
}

/// Renders a `VerifyResponse` for `varve admin verify` in a fixed field
/// order; `manifest_block_id` prints `none` rather than an empty field when
/// no block has been sealed yet, mirroring `format_status`.
pub(crate) fn format_verify(response: &VerifyResponse) -> String {
    let manifest_block_id = match response.manifest_block_id {
        Some(block_id) => block_id.to_string(),
        None => "none".to_string(),
    };
    [
        format!("manifest_block_id: {manifest_block_id}"),
        format!("tries_checked: {}", response.tries_checked),
        format!("pages_checked: {}", response.pages_checked),
        format!("events_checked: {}", response.events_checked),
        format!("log_records_checked: {}", response.log_records_checked),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn format_batches_with_no_batches_prints_zero_rows_marker() {
        let rendered = format_batches(&[])
            .unwrap_or_else(|error| panic!("format_batches must succeed: {error}"));
        assert_eq!(rendered, "(0 rows)");
    }

    #[test]
    fn format_batches_with_a_zero_row_batch_prints_zero_rows_marker() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let empty_batch = RecordBatch::new_empty(schema);

        let rendered = format_batches(&[empty_batch])
            .unwrap_or_else(|error| panic!("format_batches must succeed: {error}"));

        assert_eq!(rendered, "(0 rows)");
    }

    #[test]
    fn format_receipt_orders_nonzero_fields_and_omits_zero_fields() {
        // Deliberately populates fields out of declaration order (a later
        // field nonzero, an earlier one zero) so the assertion actually
        // pins the fixed rendering order rather than just struct order.
        let response = TxResponse {
            tx_id: 42,
            system_time: "2024-01-01T00:00:00.000000Z".to_string(),
            system_time_us: 0,
            side_effects: SideEffectsResponse {
                nodes_created: 2,
                nodes_deleted: 0,
                relationships_created: 3,
                relationships_deleted: 0,
                properties_set: 0,
                properties_removed: 5,
                labels_added: 0,
                labels_removed: 7,
            },
            basis: 42,
        };

        let rendered = format_receipt(&response);

        let expected = [
            "tx 42 @ 2024-01-01T00:00:00.000000Z".to_string(),
            "  nodes created: 2".to_string(),
            "  relationships created: 3".to_string(),
            "  properties removed: 5".to_string(),
            "  labels removed: 7".to_string(),
        ]
        .join("\n");
        assert_eq!(rendered, expected);
    }
}
