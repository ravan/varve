//! Disk cache tier (spec §9): one self-describing file per `(path,
//! byte-range)`-keyed entry — the file header is the encoded cache key, the
//! body is the value. The index rebuilds by walking `dir` on `open`, so a
//! restart survives with no separate index file. Recency is an in-memory
//! LRU tick at runtime; it is persisted by touching the file's mtime on a
//! hit, so eviction order approximately survives a restart too. Handing out
//! page-sized reads as ref-counted borrows (avoiding the copy on `get`) is a
//! post-v1 refinement. Follows `MemoryCache`'s discipline: a poisoned lock
//! degrades to cache-miss behavior, never an error.

use crate::cache::{CacheKey, CacheTier};
use crate::store::StorageError;
use bytes::Bytes;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};
use xxhash_rust::xxh3::xxh3_128;

const MAGIC: &[u8; 4] = b"VCA1";
const SUFFIX: &str = "vcache";

/// Header: magic · path-len (u32 LE) · path bytes · range tag (u8: 0 = whole
/// object, 1 = range) · [start u64 LE · end u64 LE].
fn encode_key(key: &CacheKey) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + 4 + key.path.len() + 17);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(key.path.len() as u32).to_le_bytes());
    out.extend_from_slice(key.path.as_bytes());
    match key.range {
        None => out.push(0),
        Some((start, end)) => {
            out.push(1);
            out.extend_from_slice(&start.to_le_bytes());
            out.extend_from_slice(&end.to_le_bytes());
        }
    }
    out
}

/// Splits a stored file into `(key, value)`; `None` on anything malformed.
fn decode_entry(bytes: &[u8]) -> Option<(CacheKey, Bytes)> {
    if bytes.len() < 9 || &bytes[..4] != MAGIC {
        return None;
    }
    let path_len = u32::from_le_bytes(bytes[4..8].try_into().ok()?) as usize;
    let mut off = 8;
    let path = String::from_utf8(bytes.get(off..off + path_len)?.to_vec()).ok()?;
    off += path_len;
    let tag = *bytes.get(off)?;
    off += 1;
    let range = match tag {
        0 => None,
        1 => {
            let start = u64::from_le_bytes(bytes.get(off..off + 8)?.try_into().ok()?);
            let end = u64::from_le_bytes(bytes.get(off + 8..off + 16)?.try_into().ok()?);
            off += 16;
            Some((start, end))
        }
        _ => return None,
    };
    Some((
        CacheKey { path, range },
        Bytes::copy_from_slice(bytes.get(off..)?),
    ))
}

/// File name for `key`: hex `xxh3_128` of the encoded key. 128-bit
/// (2⁻¹²⁸) collision odds are negligible, and a hash collision still
/// self-heals to a miss on `get` (the decoded key is compared against the
/// requested one) rather than serving the wrong value.
fn file_name(key: &CacheKey) -> String {
    format!("{:032x}.{SUFFIX}", xxh3_128(&encode_key(key)))
}

struct DiskEntry {
    file: PathBuf,
    bytes: u64,
    last_used: u64,
}

#[derive(Default)]
struct DiskInner {
    entries: HashMap<CacheKey, DiskEntry>,
    bytes: u64,
    tick: u64,
}

/// Evicts least-recently-used entries until `inner.bytes <= max_bytes`.
/// Best-effort: an I/O error removing a file is ignored — the entry is
/// dropped from the index regardless, and the orphaned file will be
/// re-adopted or swept on the next `open`.
fn evict_over_budget(inner: &mut DiskInner, max_bytes: u64) {
    while inner.bytes > max_bytes {
        let Some(oldest) = inner
            .entries
            .iter()
            .min_by_key(|(_, e)| e.last_used)
            .map(|(k, _)| k.clone())
        else {
            break;
        };
        if let Some(e) = inner.entries.remove(&oldest) {
            inner.bytes -= e.bytes;
            let _ = fs::remove_file(&e.file);
        }
    }
}

pub struct DiskCache {
    dir: PathBuf,
    max_bytes: u64,
    inner: Mutex<DiskInner>,
}

impl DiskCache {
    /// Opens (creating `dir` if needed) and rebuilds the index from the
    /// directory's contents: entries are re-ranked by file mtime so LRU
    /// order approximately survives a restart. Malformed `.vcache` files and
    /// crashed `.tmpN` leftovers are removed; anything else is left alone.
    /// If the walked total exceeds `max_bytes`, the oldest entries are
    /// evicted right away.
    pub fn open(dir: &Path, max_bytes: u64) -> Result<DiskCache, StorageError> {
        fs::create_dir_all(dir)?;
        let mut found: Vec<(std::time::SystemTime, CacheKey, PathBuf, u64)> = Vec::new();
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if ext.starts_with("tmp") {
                let _ = fs::remove_file(&path); // crashed mid-insert
                continue;
            }
            if ext != SUFFIX {
                continue; // foreign file: not ours, leave it alone
            }
            match fs::read(&path)
                .ok()
                .and_then(|b| decode_entry(&b).map(|(key, _)| (key, b.len() as u64)))
            {
                Some((key, len)) => {
                    let meta = fs::metadata(&path)?;
                    let mtime = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
                    found.push((mtime, key, path, len));
                }
                None => {
                    let _ = fs::remove_file(&path); // malformed
                }
            }
        }
        found.sort_by(|a, b| a.0.cmp(&b.0));
        let mut inner = DiskInner::default();
        for (_, key, path, len) in found {
            inner.tick += 1;
            let tick = inner.tick;
            inner.bytes += len;
            inner.entries.insert(
                key,
                DiskEntry {
                    file: path,
                    bytes: len,
                    last_used: tick,
                },
            );
        }
        evict_over_budget(&mut inner, max_bytes);
        Ok(DiskCache {
            dir: dir.to_path_buf(),
            max_bytes,
            inner: Mutex::new(inner),
        })
    }
}

impl CacheTier for DiskCache {
    fn get(&self, key: &CacheKey) -> Option<Bytes> {
        let Ok(mut inner) = self.inner.lock() else {
            return None;
        };
        inner.tick += 1;
        let tick = inner.tick;
        let file = {
            let entry = inner.entries.get_mut(key)?;
            entry.last_used = tick;
            entry.file.clone()
        };
        match fs::read(&file).ok().and_then(|b| decode_entry(&b)) {
            Some((stored, value)) if stored == *key => {
                // Touch mtime (best-effort) so LRU order survives restart.
                let _ = fs::File::options()
                    .append(true)
                    .open(&file)
                    .and_then(|f| f.set_modified(std::time::SystemTime::now()));
                Some(value)
            }
            _ => {
                // Vanished, corrupt, or hash-collision mismatch:
                // self-heal to miss; read-through wrapper refills.
                if let Some(e) = inner.entries.remove(key) {
                    inner.bytes -= e.bytes;
                    let _ = fs::remove_file(&e.file);
                }
                None
            }
        }
    }

    fn insert(&self, key: CacheKey, value: Bytes) {
        let mut body = encode_key(&key);
        body.extend_from_slice(&value);
        let total = body.len() as u64;
        if total > self.max_bytes {
            return;
        }
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.tick += 1;
        let tick = inner.tick;
        let file = self.dir.join(file_name(&key));
        // Write-temp-then-rename: a crash mid-write leaves only a `.tmpN`
        // file — swept on the next `open` — never a half-readable entry.
        let tmp = file.with_extension(format!("tmp{tick}"));
        let wrote = fs::write(&tmp, &body).and_then(|()| fs::rename(&tmp, &file));
        if wrote.is_err() {
            let _ = fs::remove_file(&tmp);
            return;
        }
        if let Some(old) = inner.entries.insert(
            key,
            DiskEntry {
                file,
                bytes: total,
                last_used: tick,
            },
        ) {
            inner.bytes -= old.bytes;
        }
        inner.bytes += total;
        evict_over_budget(&mut inner, self.max_bytes);
    }

    fn invalidate_path(&self, path: &str) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        let stale: Vec<CacheKey> = inner
            .entries
            .keys()
            .filter(|k| k.path == path)
            .cloned()
            .collect();
        for key in stale {
            if let Some(e) = inner.entries.remove(&key) {
                inner.bytes -= e.bytes;
                let _ = fs::remove_file(&e.file);
            }
        }
    }
}

#[derive(serde::Deserialize)]
struct DiskCacheConfig {
    dir: String,
    #[serde(default = "default_disk_max_bytes")]
    max_bytes: u64,
}

fn default_disk_max_bytes() -> u64 {
    50 * 1024 * 1024 * 1024
}

/// Registry factory: listed as `"disk"` in `[cache] tiers`; `[cache.disk]`
/// requires `dir` (`max_bytes` default 50 GiB).
pub struct DiskCacheFactory;

impl ComponentFactory<dyn CacheTier> for DiskCacheFactory {
    fn name(&self) -> &'static str {
        "disk"
    }

    fn build(
        &self,
        cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn CacheTier>, RegistryError> {
        let section = cfg.child("disk").ok_or_else(|| RegistryError::Build {
            kind: "cache",
            name: "disk".into(),
            source: "missing [cache.disk] section (requires `dir`)"
                .to_string()
                .into(),
        })?;
        let config: DiskCacheConfig = section.get()?;
        match DiskCache::open(Path::new(&config.dir), config.max_bytes) {
            Ok(cache) => Ok(Arc::new(cache)),
            Err(e) => Err(RegistryError::Build {
                kind: "cache",
                name: "disk".into(),
                source: Box::new(e),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{CacheKey, CacheTier};
    use bytes::Bytes;
    use std::time::Duration;

    fn key(path: &str, range: Option<(u64, u64)>) -> CacheKey {
        CacheKey {
            path: path.into(),
            range,
        }
    }

    #[test]
    fn round_trips_values_by_key_and_range() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::open(dir.path(), 1024).unwrap();
        cache.insert(key("v1/a", None), Bytes::from_static(b"whole"));
        cache.insert(key("v1/a", Some((0, 2))), Bytes::from_static(b"wh"));
        assert_eq!(
            cache.get(&key("v1/a", None)),
            Some(Bytes::from_static(b"whole"))
        );
        assert_eq!(
            cache.get(&key("v1/a", Some((0, 2)))),
            Some(Bytes::from_static(b"wh")),
            "ranges are distinct entries"
        );
        assert_eq!(cache.get(&key("v1/b", None)), None);
    }

    #[test]
    fn survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let cache = DiskCache::open(dir.path(), 1024).unwrap();
            cache.insert(key("v1/a", None), Bytes::from_static(b"aaaa"));
            cache.insert(key("v1/b", Some((3, 9))), Bytes::from_static(b"bbbb"));
        }
        let cache = DiskCache::open(dir.path(), 1024).unwrap();
        assert_eq!(
            cache.get(&key("v1/a", None)),
            Some(Bytes::from_static(b"aaaa"))
        );
        assert_eq!(
            cache.get(&key("v1/b", Some((3, 9)))),
            Some(Bytes::from_static(b"bbbb"))
        );
    }

    #[test]
    fn enforces_the_byte_budget_lru() {
        let dir = tempfile::tempdir().unwrap();
        // Entry size = header + value. Header for a 1-char path with no
        // range = 4 (magic) + 4 (len) + 1 (path) + 1 (tag) = 10; values are
        // 100 bytes ⇒ 110 per entry. Budget fits two.
        let cache = DiskCache::open(dir.path(), 250).unwrap();
        let value = Bytes::from(vec![7u8; 100]);
        cache.insert(key("a", None), value.clone());
        cache.insert(key("b", None), value.clone());
        assert!(cache.get(&key("a", None)).is_some()); // touch a → b is LRU
        cache.insert(key("c", None), value.clone());
        assert!(cache.get(&key("a", None)).is_some());
        assert!(cache.get(&key("b", None)).is_none(), "b was evicted");
        assert!(cache.get(&key("c", None)).is_some());
        // Files on disk match the index: exactly 2 .vcache entries remain.
        let entries = std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "vcache")
            })
            .count();
        assert_eq!(entries, 2);
    }

    #[test]
    fn restart_preserves_lru_order_via_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let value = Bytes::from(vec![7u8; 100]);
        {
            let cache = DiskCache::open(dir.path(), 1024).unwrap();
            cache.insert(key("a", None), value.clone());
            std::thread::sleep(Duration::from_millis(50)); // mtime resolution
            cache.insert(key("b", None), value.clone());
            std::thread::sleep(Duration::from_millis(50));
            cache.get(&key("a", None)); // touch: a is now newer than b
        }
        let cache = DiskCache::open(dir.path(), 250).unwrap();
        cache.insert(key("c", None), value.clone()); // forces one eviction
        assert!(cache.get(&key("a", None)).is_some(), "a was touched last");
        assert!(
            cache.get(&key("b", None)).is_none(),
            "b was oldest by mtime"
        );
    }

    #[test]
    fn oversized_values_are_never_cached() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::open(dir.path(), 64).unwrap();
        cache.insert(key("big", None), Bytes::from(vec![0u8; 128]));
        assert_eq!(cache.get(&key("big", None)), None);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn invalidate_path_removes_all_ranges_and_files() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::open(dir.path(), 1024).unwrap();
        cache.insert(key("v1/a", None), Bytes::from_static(b"1"));
        cache.insert(key("v1/a", Some((0, 1))), Bytes::from_static(b"2"));
        cache.insert(key("v1/b", None), Bytes::from_static(b"3"));
        cache.invalidate_path("v1/a");
        assert_eq!(cache.get(&key("v1/a", None)), None);
        assert_eq!(cache.get(&key("v1/a", Some((0, 1)))), None);
        assert!(cache.get(&key("v1/b", None)).is_some());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn malformed_and_temp_files_are_swept_at_open_foreign_files_kept() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("junk.vcache"), b"not a header").unwrap();
        std::fs::write(dir.path().join("leftover.tmp3"), b"crashed write").unwrap();
        std::fs::write(dir.path().join("notes.txt"), b"foreign").unwrap();
        let cache = DiskCache::open(dir.path(), 1024).unwrap();
        assert_eq!(cache.get(&key("junk", None)), None);
        let remaining: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(remaining, vec!["notes.txt".to_string()]);
    }

    #[test]
    fn corrupt_entry_self_heals_to_a_miss() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::open(dir.path(), 1024).unwrap();
        cache.insert(key("v1/a", None), Bytes::from_static(b"good"));
        // Corrupt the entry file behind the cache's back.
        let file = std::fs::read_dir(dir.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::write(&file, b"garbage").unwrap();
        assert_eq!(cache.get(&key("v1/a", None)), None);
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "the broken file was removed"
        );
        assert_eq!(cache.get(&key("v1/a", None)), None, "stays a clean miss");
    }
}
