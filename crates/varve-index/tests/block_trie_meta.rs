use varve_index::{decode_meta, encode_block, Event, LiveTable, Op, PageMeta};
use varve_types::{Doc, Iid, Instant, TemporalBounds, TemporalDimension};

const EOT: Instant = Instant::END_OF_TIME;

fn iid(first: u8) -> Iid {
    Iid::from_bytes([first, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
}

fn us(n: i64) -> Instant {
    Instant::from_micros(n)
}

fn bounds() -> TemporalBounds {
    TemporalBounds {
        valid: TemporalDimension::at(us(10)),
        system: TemporalDimension::at(us(10)),
    }
}

fn put(first: u8, system_from: i64) -> Event {
    Event {
        iid: iid(first),
        system_from: us(system_from),
        valid_from: us(system_from),
        valid_to: EOT,
        src: None,
        dst: None,
        op: Op::Put {
            labels: vec!["P".into()],
            doc: Doc::new(),
        },
    }
}

#[test]
fn meta_wire_round_trips_path_column() {
    let mut live = LiveTable::new();
    live.append(put(0x00, 1)).unwrap();
    live.append(put(0x40, 2)).unwrap();

    let block = encode_block(&live, 1).unwrap();
    assert_eq!(block.pages.len(), 2);
    assert_eq!(
        block
            .pages
            .iter()
            .map(|page| page.path.clone())
            .collect::<Vec<_>>(),
        vec![Vec::<u8>::new(), Vec::<u8>::new()]
    );
    assert_eq!(decode_meta(&block.meta).unwrap(), block.pages);
}

#[test]
fn iid_point_outside_page_path_is_pruned_before_range_stats() {
    let page = PageMeta {
        path: vec![1],
        offset: 0,
        len: 0,
        rows: 1,
        min_iid: iid(0x00),
        max_iid: iid(0xff),
        min_system_from: us(1),
        max_system_from: us(1),
        min_valid_from: us(1),
        max_valid_from: us(1),
        min_valid_to: EOT,
        max_valid_to: EOT,
        has_erase: false,
    };

    assert!(page.selected(&bounds(), Some(&iid(0x40))));
    assert!(!page.selected(&bounds(), Some(&iid(0x00))));
}
