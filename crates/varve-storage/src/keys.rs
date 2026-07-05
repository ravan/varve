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
    let mut chars = s.chars();
    let len_digit = chars.next()?.to_digit(16)? as usize;
    let body = chars.as_str();
    if body.len() != len_digit + 1 || body.chars().any(|c| c.is_ascii_uppercase()) {
        return None;
    }
    u64::from_str_radix(body, 16).ok()
}

/// L0 trie key (XTDB `Trie.l0Key`): level 0, recency `c` (current), no part
/// segment. Levels > 0, recency dates, and IID partitions arrive in slice 8.
pub fn l0_trie_key(block_id: u64) -> String {
    format!("l{}-rc-b{}", lex_hex(0), lex_hex(block_id))
}

pub fn data_key(graph: &str, table: &str, trie_key: &str) -> String {
    format!("v1/graphs/{graph}/tables/{table}/data/{trie_key}.arrow")
}

pub fn meta_key(graph: &str, table: &str, trie_key: &str) -> String {
    format!("v1/graphs/{graph}/tables/{table}/meta/{trie_key}.arrow")
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
    }

    #[test]
    fn lexicographic_order_is_numeric_order() {
        let ns = [0u64, 1, 9, 0xf, 0x10, 0x99, 0xff, 0x100, 0xabc, 1 << 20];
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
            meta_key("default", "nodes", "l00-rc-b00"),
            "v1/graphs/default/tables/nodes/meta/l00-rc-b00.arrow"
        );
        assert_eq!(manifest_key(0), "v1/blocks/00.manifest");
        assert_eq!(manifest_key(0x34), "v1/blocks/134.manifest");
    }

    #[test]
    fn manifest_block_id_parses_only_manifest_keys() {
        assert_eq!(manifest_block_id("v1/blocks/00.manifest"), Some(0));
        assert_eq!(manifest_block_id(&manifest_key(0x34)), Some(0x34));
        assert_eq!(manifest_block_id("v1/blocks/00.tmp"), None);
        assert_eq!(manifest_block_id("v1/other/00.manifest"), None);
        assert_eq!(manifest_block_id("v1/blocks/zz.manifest"), None);
    }
}
