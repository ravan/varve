//! Object-key layout (spec §9). Everything lives under the format-version
//! prefix `v1/`. Trie keys adopt XTDB's encoding exactly (Trie.kt +
//! StringUtil.kt): lexicographic listing order == logical order.

/// Lex-hex: one hex digit encoding (hex-body length − 1), then the hex body.
/// `0 → "00"`, `0x34 → "134"`. Sorts lexicographically in numeric order over
/// the whole u64 range (body length 1..=16 ⇒ prefix digit '0'..='f').
pub fn lex_hex(n: u64) -> String {
    let body = format!("{n:x}");
    format!("{:x}{body}", body.len() - 1)
}

pub fn parse_lex_hex(s: &str) -> Option<u64> {
    if s.chars().any(|c| c.is_ascii_uppercase()) {
        return None;
    }
    let mut chars = s.chars();
    let len_digit = chars.next()?.to_digit(16)? as usize;
    let body = chars.as_str();
    if body.len() != len_digit + 1 {
        return None;
    }
    u64::from_str_radix(body, 16).ok()
}

pub use varve_types::{Bucketer, MAX_TRIE_LEVELS, PAGE_LIMIT, TRIE_BRANCH_FACTOR, TRIE_LEVEL_BITS};

pub const LOG_LIMIT: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableScope {
    pub graph: String,
    pub table: String,
    pub family: String,
}

impl TableScope {
    pub fn new(
        graph: impl Into<String>,
        table: impl Into<String>,
        family: impl Into<String>,
    ) -> TableScope {
        TableScope {
            graph: graph.into(),
            table: table.into(),
            family: family.into(),
        }
    }

    pub fn data_key(&self, trie_key: &str) -> String {
        if self.family.is_empty() {
            data_key(&self.graph, &self.table, trie_key)
        } else {
            adj_data_key(&self.graph, &self.table, &self.family, trie_key)
        }
    }

    pub fn meta_key(&self, trie_key: &str) -> String {
        if self.family.is_empty() {
            meta_key(&self.graph, &self.table, trie_key)
        } else {
            adj_meta_key(&self.graph, &self.table, &self.family, trie_key)
        }
    }

    pub fn scoped_trie_key(&self, trie_key: impl Into<String>) -> ScopedTrieKey {
        ScopedTrieKey::new(self.clone(), trie_key)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScopedTrieKey {
    pub scope: TableScope,
    pub trie_key: String,
}

impl ScopedTrieKey {
    pub fn new(scope: TableScope, trie_key: impl Into<String>) -> ScopedTrieKey {
        ScopedTrieKey {
            scope,
            trie_key: trie_key.into(),
        }
    }

    pub fn data_key(&self) -> String {
        self.scope.data_key(&self.trie_key)
    }

    pub fn meta_key(&self) -> String {
        self.scope.meta_key(&self.trie_key)
    }

    pub fn parse_trie_key(&self) -> Result<TrieKey, crate::StorageError> {
        TrieKey::parse(&self.trie_key)
    }

    pub fn trie_shard(&self) -> Result<TrieShard, crate::StorageError> {
        Ok(TrieShard::from_trie_key(
            self.scope.clone(),
            &self.parse_trie_key()?,
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TrieKeyShard {
    pub level: u64,
    pub recency: Recency,
    pub part: Vec<u8>,
}

impl TrieKeyShard {
    pub fn to_trie_key(&self, block: u64) -> TrieKey {
        TrieKey {
            level: self.level,
            recency: self.recency.clone(),
            part: self.part.clone(),
            block,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TrieShard {
    pub scope: TableScope,
    pub key_shard: TrieKeyShard,
}

impl TrieShard {
    pub fn new(scope: TableScope, key_shard: TrieKeyShard) -> TrieShard {
        TrieShard { scope, key_shard }
    }

    pub fn from_trie_key(scope: TableScope, trie_key: &TrieKey) -> TrieShard {
        TrieShard::new(scope, trie_key.shard())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Recency {
    Current,
    Week { yyyymmdd: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TrieKey {
    pub level: u64,
    pub recency: Recency,
    pub part: Vec<u8>,
    pub block: u64,
}

impl TrieKey {
    pub fn l0(block: u64) -> TrieKey {
        TrieKey {
            level: 0,
            recency: Recency::Current,
            part: Vec::new(),
            block,
        }
    }

    pub fn child(&self, bucket: u8, block: u64) -> TrieKey {
        assert!(
            bucket < TRIE_BRANCH_FACTOR,
            "trie bucket {bucket} outside branch factor {TRIE_BRANCH_FACTOR}"
        );
        let mut part = self.part.clone();
        part.push(bucket);
        TrieKey {
            level: self.level + 1,
            recency: self.recency.clone(),
            part,
            block,
        }
    }

    pub fn shard(&self) -> TrieKeyShard {
        TrieKeyShard {
            level: self.level,
            recency: self.recency.clone(),
            part: self.part.clone(),
        }
    }

    pub fn to_key_string(&self) -> String {
        let mut key = format!("l{}-r{}", lex_hex(self.level), self.recency.as_key_part());
        if !self.part.is_empty() {
            key.push_str("-p");
            for bucket in &self.part {
                key.push(char::from(b'0' + *bucket));
            }
        }
        key.push_str("-b");
        key.push_str(&lex_hex(self.block));
        key
    }

    pub fn parse(s: &str) -> Result<TrieKey, crate::StorageError> {
        let mut segments = s.split('-');
        let level = segments
            .next()
            .and_then(|seg| seg.strip_prefix('l'))
            .and_then(parse_lex_hex)
            .ok_or_else(|| invalid_trie_key(s))?;
        if level > MAX_TRIE_LEVELS as u64 {
            return Err(invalid_trie_key(s));
        }
        let recency = segments
            .next()
            .and_then(|seg| seg.strip_prefix('r'))
            .ok_or_else(|| invalid_trie_key(s))
            .and_then(|seg| Recency::parse(seg).ok_or_else(|| invalid_trie_key(s)))?;
        let next = segments.next().ok_or_else(|| invalid_trie_key(s))?;
        let (part, block_segment) = if let Some(part_text) = next.strip_prefix('p') {
            if part_text.is_empty() || part_text.len() > MAX_TRIE_LEVELS {
                return Err(invalid_trie_key(s));
            }
            let mut part = Vec::with_capacity(part_text.len());
            for ch in part_text.chars() {
                let Some(bucket) = ch.to_digit(TRIE_BRANCH_FACTOR as u32) else {
                    return Err(invalid_trie_key(s));
                };
                part.push(bucket as u8);
            }
            (part, segments.next().ok_or_else(|| invalid_trie_key(s))?)
        } else {
            (Vec::new(), next)
        };
        if segments.next().is_some() {
            return Err(invalid_trie_key(s));
        }
        let block = block_segment
            .strip_prefix('b')
            .and_then(parse_lex_hex)
            .ok_or_else(|| invalid_trie_key(s))?;
        Ok(TrieKey {
            level,
            recency,
            part,
            block,
        })
    }
}

impl Recency {
    fn as_key_part(&self) -> String {
        match self {
            Recency::Current => "c".to_string(),
            Recency::Week { yyyymmdd } => format!("{yyyymmdd:08}"),
        }
    }

    fn parse(s: &str) -> Option<Recency> {
        if s == "c" {
            return Some(Recency::Current);
        }
        if s.len() != 8 || !s.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let yyyymmdd = s.parse::<u32>().ok()?;
        let year = yyyymmdd / 10_000;
        let month = (yyyymmdd / 100) % 100;
        let day = yyyymmdd % 100;
        if valid_ymd(year, month, day) {
            Some(Recency::Week { yyyymmdd })
        } else {
            None
        }
    }
}

fn invalid_trie_key(key: &str) -> crate::StorageError {
    crate::StorageError::InvalidKey(key.to_string())
}

fn valid_ymd(year: u32, month: u32, day: u32) -> bool {
    if year == 0 || !(1..=12).contains(&month) {
        return false;
    }
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => return false,
    };
    (1..=max_day).contains(&day)
}

fn is_leap_year(year: u32) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

/// L0 trie key (XTDB `Trie.l0Key`): level 0, recency `c` (current), no part
/// segment. Levels > 0, recency dates, and IID partitions arrive in slice 8.
pub fn l0_trie_key(block_id: u64) -> String {
    TrieKey::l0(block_id).to_key_string()
}

pub fn data_key(graph: &str, table: &str, trie_key: &str) -> String {
    format!("v1/graphs/{graph}/tables/{table}/data/{trie_key}.arrow")
}

pub fn meta_key(graph: &str, table: &str, trie_key: &str) -> String {
    format!("v1/graphs/{graph}/tables/{table}/meta/{trie_key}.arrow")
}

/// Adjacency-family names (slice 6): the src-sorted out-adjacency and the
/// dst-sorted in-adjacency of an edges table. A `""` family denotes the
/// primary (iid-sorted) table. Family objects live under a per-family
/// subdirectory so they never collide with the primary data/meta keys.
pub const ADJ_OUT: &str = "adj-out";
pub const ADJ_IN: &str = "adj-in";

/// Data key for an adjacency family (slice 6). Mirrors [`data_key`] but with
/// the `{family}` subdirectory: `v1/graphs/{graph}/tables/{table}/{family}/
/// data/{trie_key}.arrow`.
pub fn adj_data_key(graph: &str, table: &str, family: &str, trie_key: &str) -> String {
    format!("v1/graphs/{graph}/tables/{table}/{family}/data/{trie_key}.arrow")
}

/// Meta key for an adjacency family (slice 6). Mirrors [`meta_key`] but with
/// the `{family}` subdirectory: `v1/graphs/{graph}/tables/{table}/{family}/
/// meta/{trie_key}.arrow`.
pub fn adj_meta_key(graph: &str, table: &str, family: &str, trie_key: &str) -> String {
    format!("v1/graphs/{graph}/tables/{table}/{family}/meta/{trie_key}.arrow")
}

pub fn data_key_for_family(graph: &str, table: &str, family: &str, trie_key: &str) -> String {
    TableScope::new(graph, table, family).data_key(trie_key)
}

pub fn meta_key_for_family(graph: &str, table: &str, family: &str, trie_key: &str) -> String {
    TableScope::new(graph, table, family).meta_key(trie_key)
}

pub const MANIFEST_PREFIX: &str = "v1/blocks";

pub fn manifest_key(block_id: u64) -> String {
    format!("{MANIFEST_PREFIX}/{}.manifest", lex_hex(block_id))
}

/// Parses the block id out of a manifest key; `None` for anything else
/// (foreign keys under the prefix are ignored, never an error).
pub fn manifest_block_id(key: &str) -> Option<u64> {
    parse_lex_hex(
        key.strip_prefix(MANIFEST_PREFIX)?
            .strip_prefix('/')?
            .strip_suffix(".manifest")?,
    )
}

/// Log-object keys (spec §9): `v1/log/<epoch>/<offset-lexhex>.vlog`, one
/// object per group-commit batch, named by the batch's FIRST position. The
/// epoch directory is fixed-width hex (u16 ⇒ 4 digits) and the offset is
/// lex-hex, so lexicographic listing order == position order.
pub const LOG_PREFIX: &str = "v1/log";

pub fn log_key(first: varve_types::LogPosition) -> String {
    format!(
        "{LOG_PREFIX}/{:04x}/{}.vlog",
        first.epoch(),
        lex_hex(first.offset())
    )
}

/// Parses a log-object key back to its first position; `None` for anything
/// else (foreign keys under the prefix are ignored, never an error — same
/// policy as `manifest_block_id`).
pub fn parse_log_key(key: &str) -> Option<varve_types::LogPosition> {
    let rest = key.strip_prefix(LOG_PREFIX)?.strip_prefix('/')?;
    let (epoch_hex, offset_part) = rest.split_once('/')?;
    if epoch_hex.len() != 4
        || epoch_hex
            .chars()
            .any(|c| !c.is_ascii_hexdigit() || c.is_ascii_uppercase())
    {
        return None;
    }
    let epoch = u16::from_str_radix(epoch_hex, 16).ok()?;
    let offset = parse_lex_hex(offset_part.strip_suffix(".vlog")?)?;
    varve_types::LogPosition::new(epoch, offset).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lex_hex_known_answers() {
        // XTDB StringUtil.asLexHex: one hex digit of (body length - 1), then
        // the hex body. trie-cat.allium: "b134 is block 0x34".
        assert_eq!(lex_hex(0), "00");
        assert_eq!(lex_hex(1), "01");
        assert_eq!(lex_hex(0xf), "0f");
        assert_eq!(lex_hex(0x10), "110");
        assert_eq!(lex_hex(0x34), "134");
        assert_eq!(lex_hex(0xff), "1ff");
        assert_eq!(lex_hex(0x100), "2100");
        assert_eq!(lex_hex(u64::MAX), format!("f{:x}", u64::MAX));
    }

    #[test]
    fn lex_hex_round_trips_and_rejects_garbage() {
        for n in [0u64, 1, 15, 16, 0x34, 255, 256, 1 << 47, u64::MAX] {
            assert_eq!(parse_lex_hex(&lex_hex(n)), Some(n), "{n}");
        }
        assert_eq!(parse_lex_hex(""), None);
        assert_eq!(parse_lex_hex("1"), None); // body missing
        assert_eq!(parse_lex_hex("134x"), None); // body length mismatch
        assert_eq!(parse_lex_hex("zz"), None);
        assert_eq!(parse_lex_hex("A10000000000"), None); // uppercase length-prefix digit
        assert_eq!(parse_lex_hex("1FF"), None); // uppercase body
    }

    #[test]
    fn lexicographic_order_is_numeric_order() {
        let ns = [
            0u64,
            1,
            9,
            0xf,
            0x10,
            0x99,
            0xff,
            0x100,
            0xabc,
            1 << 20,
            1 << 47,
        ];
        let mut by_string: Vec<u64> = ns.to_vec();
        by_string.sort_by_key(|n| lex_hex(*n));
        let mut by_value = ns.to_vec();
        by_value.sort_unstable();
        assert_eq!(by_string, by_value);
    }

    #[test]
    fn l0_trie_key_matches_the_xtdb_reference() {
        // trie-cat.allium canonical example: "l00-rc-b00  level 0, current, block 0".
        assert_eq!(l0_trie_key(0), "l00-rc-b00");
        assert_eq!(l0_trie_key(0x34), "l00-rc-b134");
    }

    #[test]
    fn object_keys_follow_the_spec_layout() {
        // Spec §9 key layout, verbatim.
        assert_eq!(
            data_key("default", "nodes", "l00-rc-b00"),
            "v1/graphs/default/tables/nodes/data/l00-rc-b00.arrow"
        );
        assert_eq!(
            data_key_for_family("default", "nodes", "", "l00-rc-b00"),
            "v1/graphs/default/tables/nodes/data/l00-rc-b00.arrow"
        );
        assert_eq!(
            meta_key("default", "nodes", "l00-rc-b00"),
            "v1/graphs/default/tables/nodes/meta/l00-rc-b00.arrow"
        );
        assert_eq!(
            meta_key_for_family("default", "nodes", "", "l00-rc-b00"),
            "v1/graphs/default/tables/nodes/meta/l00-rc-b00.arrow"
        );
        assert_eq!(manifest_key(0), "v1/blocks/00.manifest");
        assert_eq!(manifest_key(0x34), "v1/blocks/134.manifest");
    }

    #[test]
    fn adjacency_family_keys() {
        assert_eq!(
            adj_data_key("default", "edges", ADJ_OUT, "l00-rc-b00"),
            "v1/graphs/default/tables/edges/adj-out/data/l00-rc-b00.arrow"
        );
        assert_eq!(
            data_key_for_family("default", "edges", ADJ_OUT, "l00-rc-b00"),
            "v1/graphs/default/tables/edges/adj-out/data/l00-rc-b00.arrow"
        );
        assert_eq!(
            adj_meta_key("default", "edges", ADJ_IN, "l00-rc-b00"),
            "v1/graphs/default/tables/edges/adj-in/meta/l00-rc-b00.arrow"
        );
        assert_eq!(
            meta_key_for_family("default", "edges", ADJ_IN, "l00-rc-b00"),
            "v1/graphs/default/tables/edges/adj-in/meta/l00-rc-b00.arrow"
        );
    }

    #[test]
    fn manifest_block_id_parses_only_manifest_keys() {
        assert_eq!(manifest_block_id("v1/blocks/00.manifest"), Some(0));
        assert_eq!(manifest_block_id(&manifest_key(0x34)), Some(0x34));
        assert_eq!(manifest_block_id("v1/blocks/00.tmp"), None);
        assert_eq!(manifest_block_id("v1/other/00.manifest"), None);
        assert_eq!(manifest_block_id("v1/blocks/zz.manifest"), None);
    }

    #[test]
    fn log_keys_follow_the_spec_layout() {
        use varve_types::LogPosition;
        // Spec §9: v1/log/<epoch>/<offset-lexhex>.vlog; epoch dir is
        // fixed-width u16 hex so listing sorts numerically.
        let p = |e, o| LogPosition::new(e, o).unwrap();
        assert_eq!(log_key(p(0, 0)), "v1/log/0000/00.vlog");
        assert_eq!(log_key(p(0, 0x34)), "v1/log/0000/134.vlog");
        assert_eq!(log_key(p(3, 2)), "v1/log/0003/02.vlog");
    }

    #[test]
    fn log_keys_round_trip_and_reject_foreign_keys() {
        use varve_types::LogPosition;
        for (e, o) in [
            (0u16, 0u64),
            (0, 1),
            (0, 0xff),
            (3, 0x34),
            (u16::MAX, 1 << 40),
        ] {
            let pos = LogPosition::new(e, o).unwrap();
            assert_eq!(parse_log_key(&log_key(pos)), Some(pos), "{e}/{o}");
        }
        assert_eq!(parse_log_key("v1/log/0000/00.manifest"), None); // wrong ext
        assert_eq!(parse_log_key("v1/log/00/00.vlog"), None); // short epoch
        assert_eq!(parse_log_key("v1/log/000A/00.vlog"), None); // uppercase
        assert_eq!(parse_log_key("v1/log/0000/1FF.vlog"), None); // uppercase body
        assert_eq!(parse_log_key("v1/blocks/00.vlog"), None); // wrong prefix
        assert_eq!(parse_log_key("v1/log/0000.vlog"), None); // missing segment
    }

    #[test]
    fn log_key_listing_order_is_position_order() {
        use varve_types::LogPosition;
        let positions = [
            LogPosition::new(0, 0).unwrap(),
            LogPosition::new(0, 9).unwrap(),
            LogPosition::new(0, 0x10).unwrap(),
            LogPosition::new(0, 0x100).unwrap(),
            LogPosition::new(1, 0).unwrap(),
            LogPosition::new(0x10, 5).unwrap(),
        ];
        let mut by_key: Vec<_> = positions.to_vec();
        by_key.sort_by_key(|p| log_key(*p));
        let mut by_pos = positions.to_vec();
        by_pos.sort();
        assert_eq!(by_key, by_pos);
    }
}
