use varve_storage::keys::{
    l0_trie_key, Bucketer, Recency, TrieKey, LOG_LIMIT, TRIE_BRANCH_FACTOR, TRIE_LEVEL_BITS,
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
}

#[test]
fn bucketer_matches_xtdb_known_bit_patterns() {
    assert_eq!(TRIE_LEVEL_BITS, 2);
    assert_eq!(TRIE_BRANCH_FACTOR, 4);
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
    assert_eq!(Bucketer::bucket(&iid, 0), 0);
    assert_eq!(Bucketer::bucket(&iid, 1), 1);
    assert_eq!(Bucketer::bucket(&iid, 2), 2);
    assert_eq!(Bucketer::bucket(&iid, 3), 3);
    assert_eq!(Bucketer::bucket(&iid, 4), 3);
    assert_eq!(Bucketer::bucket(&iid, 5), 2);
    assert_eq!(Bucketer::path(&iid, 6), vec![0, 1, 2, 3, 3, 2]);
    assert!(Bucketer::contains(&[0, 1, 2, 3], &iid));
    assert!(!Bucketer::contains(&[0, 1, 2, 2], &iid));
}
