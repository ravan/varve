use crate::coord::fence::load_fences;
use crate::replay::decode_log_record;
use crate::EngineError;
use varve_index::{decode_events, decode_meta, IndexError};
use varve_log::Log;
use varve_storage::{latest_manifest, ObjectStore};
use varve_types::LogPosition;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VerifyReport {
    pub manifest_block_id: Option<u64>,
    pub tries_checked: usize,
    pub pages_checked: usize,
    pub events_checked: usize,
    pub log_records_checked: usize,
}

fn corruption(message: impl Into<String>) -> EngineError {
    EngineError::Index(IndexError::Codec(message.into()))
}

pub(crate) async fn verify_database(
    store: &dyn ObjectStore,
    log: &dyn Log,
) -> Result<VerifyReport, EngineError> {
    let manifest = latest_manifest(store).await?;
    let mut report = VerifyReport {
        manifest_block_id: manifest.as_ref().map(|value| value.block_id),
        ..VerifyReport::default()
    };

    let mut cursor = LogPosition::ZERO;
    if let Some(manifest) = manifest {
        cursor = LogPosition::from_u64(manifest.watermark);
        for table in &manifest.tables {
            let scope = table.scope_ref();
            for entry in &table.tries {
                let data_key = scope.data_key(&entry.trie_key);
                let meta_key = scope.meta_key(&entry.trie_key);
                let data = store.get(&data_key).await?;
                let meta = store.get(&meta_key).await?;
                if entry.data_len != data.len() as u64 {
                    return Err(corruption(format!(
                        "{data_key}: manifest data_len {} does not match object length {}",
                        entry.data_len,
                        data.len()
                    )));
                }

                let pages = decode_meta(&meta)?;
                let mut previous_end = 0_u64;
                let mut trie_rows = 0_u64;
                for page in pages {
                    if page.offset < previous_end {
                        return Err(corruption(format!(
                            "{meta_key}: page offset {} overlaps or precedes prior end {previous_end}",
                            page.offset
                        )));
                    }
                    let end = page.offset.checked_add(page.len).ok_or_else(|| {
                        corruption(format!("{meta_key}: page byte range overflows"))
                    })?;
                    if end > data.len() as u64 {
                        return Err(corruption(format!(
                            "{data_key}: page range {}..{end} exceeds object length {}",
                            page.offset,
                            data.len()
                        )));
                    }
                    let start = usize::try_from(page.offset)
                        .map_err(|_| corruption(format!("{data_key}: page offset is too large")))?;
                    let end = usize::try_from(end)
                        .map_err(|_| corruption(format!("{data_key}: page end is too large")))?;
                    let events = decode_events(&data[start..end])?;
                    if events.len() as u64 != page.rows {
                        return Err(corruption(format!(
                            "{data_key}: decoded {} rows for page declaring {}",
                            events.len(),
                            page.rows
                        )));
                    }
                    trie_rows = trie_rows.checked_add(page.rows).ok_or_else(|| {
                        corruption(format!("{meta_key}: trie row count overflows"))
                    })?;
                    report.pages_checked += 1;
                    report.events_checked += events.len();
                    previous_end = end as u64;
                }
                if trie_rows != entry.row_count {
                    return Err(corruption(format!(
                        "{meta_key}: decoded trie row count {trie_rows} does not match manifest {}",
                        entry.row_count
                    )));
                }
                report.tries_checked += 1;
            }
        }
    }

    // Epoch fences (spec §12), loaded once: verify is a one-shot batch walk,
    // unlike the follower's continuous poll, so a single snapshot of the
    // fence set is enough to walk every epoch boundary in this pass.
    let fences = load_fences(store).await?;

    loop {
        let to = cursor.advance(1024)?;
        let records = log.read_range(cursor, to).await?;
        if records.is_empty() {
            if let Some(next) = fences.jump(cursor)? {
                cursor = next;
                continue;
            }
            break;
        }
        for (position, record) in records {
            if position != cursor {
                return Err(EngineError::LogGap {
                    expected: cursor,
                    actual: position,
                });
            }
            if !fences.is_live(position) {
                // Dead record: a fence reassigned this tx id, so it is
                // checked-but-skipped — walked past without decoding or
                // counting it as verified.
                cursor = cursor.next()?;
                continue;
            }
            decode_log_record(&record)?;
            report.log_records_checked += 1;
            cursor = cursor.next()?;
        }
    }

    Ok(report)
}
