use varve_storage::keys::{
    l0_trie_key, Bucketer, Recency, TableScope, TrieKey, TrieKeyShard, TrieShard, ADJ_IN,
    LOG_LIMIT, MAX_TRIE_LEVELS, PAGE_LIMIT, TRIE_BRANCH_FACTOR, TRIE_LEVEL_BITS,
};

#[test]
fn trie_key_round_trips_l0_l1_l2() {
    let l0 = TrieKey::l0(0x34);
    assert_eq!(l0.to_key_string(), "l00-rc-b134");
    assert_eq!(TrieKey::parse("l00-rc-b134").unwrap(), l0);
    assert_eq!(l0_trie_key(0x34), l0.to_key_string());

    let l1h = TrieKey {
        level: 1,
        recency: Recency::Week { yyyymmdd: 20200106 },
        part: Vec::new(),
        block: 9,
    };
    assert_eq!(l1h.to_key_string(), "l01-r20200106-b09");
    assert_eq!(TrieKey::parse("l01-r20200106-b09").unwrap(), l1h);
    assert_eq!(
        l1h.child(3, 10),
        TrieKey {
            level: 2,
            recency: Recency::Week { yyyymmdd: 20200106 },
            part: vec![3],
            block: 10,
        }
    );

    let l2c = TrieKey {
        level: 2,
        recency: Recency::Current,
        part: vec![1, 3],
        block: 0,
    };
    assert_eq!(l2c.to_key_string(), "l02-rc-p13-b00");
    assert_eq!(TrieKey::parse("l02-rc-p13-b00").unwrap(), l2c);
}

#[test]
fn trie_key_rejects_invalid_parts_and_recency() {
    assert!(TrieKey::parse("l02-rc-p14-b00").is_err());
    assert!(TrieKey::parse("l02-rc-px-b00").is_err());
    assert!(TrieKey::parse("l02-r202001-b00").is_err());
    assert!(TrieKey::parse("l02-r20201306-b00").is_err());
    assert!(TrieKey::parse("l02-p13-rc-b00").is_err());
    assert!(TrieKey::parse("l02-rC-p13-b00").is_err());
    assert!(TrieKey::parse("l02-rc-p13-bFF").is_err());
    assert!(TrieKey::parse(&format!("l02-rc-p{}-b00", "0".repeat(MAX_TRIE_LEVELS + 1))).is_err());
    assert!(TrieKey::parse("l141-rc-b00").is_err());
}

#[test]
fn trie_key_shard_excludes_block() {
    let key = TrieKey {
        level: 2,
        recency: Recency::Week { yyyymmdd: 20200106 },
        part: vec![1, 3],
        block: 9,
    };

    assert_eq!(
        key.shard(),
        TrieKeyShard {
            level: 2,
            recency: Recency::Week { yyyymmdd: 20200106 },
            part: vec![1, 3],
        }
    );
    assert_eq!(
        key.shard(),
        TrieKey {
            block: 10,
            ..key.clone()
        }
        .shard()
    );
}

#[test]
fn scoped_trie_key_and_shard_helpers_round_trip() {
    let scope = TableScope::new("default", "edges", ADJ_IN);
    let key = TrieKey {
        level: 2,
        recency: Recency::Week { yyyymmdd: 20200106 },
        part: vec![1, 3],
        block: 9,
    };
    let scoped = scope.scoped_trie_key(key.to_key_string());

    assert_eq!(scoped.scope, scope);
    assert_eq!(scoped.parse_trie_key().unwrap(), key);
    assert_eq!(
        scoped.trie_shard().unwrap(),
        TrieShard::new(
            scoped.scope.clone(),
            TrieKeyShard {
                level: 2,
                recency: Recency::Week { yyyymmdd: 20200106 },
                part: vec![1, 3],
            },
        )
    );
    assert_eq!(key.shard().to_trie_key(10), TrieKey { block: 10, ..key });
}

#[test]
fn table_scope_routes_primary_and_family_object_keys() {
    let primary = TableScope::new("default", "nodes", "");
    assert_eq!(
        primary.data_key("l00-rc-b00"),
        "v1/graphs/default/tables/nodes/data/l00-rc-b00.arrow"
    );
    assert_eq!(
        primary.meta_key("l00-rc-b00"),
        "v1/graphs/default/tables/nodes/meta/l00-rc-b00.arrow"
    );

    let family = TableScope::new("default", "edges", ADJ_IN);
    assert_eq!(
        family.data_key("l00-rc-b00"),
        "v1/graphs/default/tables/edges/adj-in/data/l00-rc-b00.arrow"
    );
    assert_eq!(
        family.meta_key("l00-rc-b00"),
        "v1/graphs/default/tables/edges/adj-in/meta/l00-rc-b00.arrow"
    );
}

#[test]
fn bucketer_matches_xtdb_known_bit_patterns() {
    assert_eq!(TRIE_LEVEL_BITS, 2);
    assert_eq!(TRIE_BRANCH_FACTOR, 4);
    assert_eq!(PAGE_LIMIT, 1024);
    assert_eq!(LOG_LIMIT, 64);

    let iid = varve_types::Iid::from_bytes([
        0b0001_1011,
        0b1110_0100,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    ]);
    assert_eq!(Bucketer::bucket(&iid, 0), Some(0));
    assert_eq!(Bucketer::bucket(&iid, 1), Some(1));
    assert_eq!(Bucketer::bucket(&iid, 2), Some(2));
    assert_eq!(Bucketer::bucket(&iid, 3), Some(3));
    assert_eq!(Bucketer::bucket(&iid, 4), Some(3));
    assert_eq!(Bucketer::bucket(&iid, 5), Some(2));
    assert_eq!(Bucketer::path(&iid, 6), Some(vec![0, 1, 2, 3, 3, 2]));
    assert!(Bucketer::contains(&[0, 1, 2, 3], &iid));
    assert!(!Bucketer::contains(&[0, 1, 2, 2], &iid));
}
