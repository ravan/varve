#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, ListBuilder, StringArray,
    StringBuilder, StructArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use varve_testkit::tck::values::{compare_results, parse_value, TckValue};

#[test]
fn parses_path_literals() {
    assert_eq!(
        parse_value("<()>").unwrap(),
        TckValue::Path(vec![TckValue::Node {
            labels: Vec::new(),
            props: BTreeMap::new(),
        }])
    );

    assert_eq!(
        parse_value("<(:A {name: 'A'})-[:KNOWS]->(:B)>").unwrap(),
        TckValue::Path(vec![
            TckValue::Node {
                labels: vec!["A".to_string()],
                props: BTreeMap::from([("name".to_string(), TckValue::Str("A".to_string()))]),
            },
            TckValue::Rel {
                typ: "KNOWS".to_string(),
                props: BTreeMap::new(),
            },
            TckValue::Node {
                labels: vec!["B".to_string()],
                props: BTreeMap::new(),
            },
        ])
    );

    assert_eq!(
        parse_value("<(:A)<-[:R]-(:B)>").unwrap(),
        TckValue::Path(vec![
            TckValue::Node {
                labels: vec!["A".to_string()],
                props: BTreeMap::new(),
            },
            TckValue::Rel {
                typ: "R".to_string(),
                props: BTreeMap::new(),
            },
            TckValue::Node {
                labels: vec!["B".to_string()],
                props: BTreeMap::new(),
            },
        ])
    );
}

#[test]
fn compares_path_result_columns_by_iid_list_shape() {
    let header = vec!["p".to_string()];
    let expected = vec![vec![TckValue::Path(vec![
        TckValue::Node {
            labels: vec!["A".to_string()],
            props: BTreeMap::new(),
        },
        TckValue::Rel {
            typ: "KNOWS".to_string(),
            props: BTreeMap::new(),
        },
        TckValue::Node {
            labels: vec!["B".to_string()],
            props: BTreeMap::new(),
        },
    ])]];

    let mut path = ListBuilder::new(FixedSizeBinaryBuilder::new(16));
    path.values().append_value([1_u8; 16]).unwrap();
    path.values().append_value([2_u8; 16]).unwrap();
    path.values().append_value([3_u8; 16]).unwrap();
    path.append(true);

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "p",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::FixedSizeBinary(16),
                true,
            ))),
            false,
        )])),
        vec![Arc::new(path.finish())],
    )
    .unwrap();

    compare_results(&header, &expected, &[batch], true).unwrap();
}

#[test]
fn parses_primitives_lists_maps() {
    assert_eq!(parse_value("null").unwrap(), TckValue::Null);
    assert_eq!(parse_value("true").unwrap(), TckValue::Bool(true));
    assert_eq!(parse_value("-42").unwrap(), TckValue::Int(-42));
    assert_eq!(parse_value("1.5").unwrap(), TckValue::Float(1.5));
    assert_eq!(
        parse_value("['a', null, 3]").unwrap(),
        TckValue::List(vec![
            TckValue::Str("a".to_string()),
            TckValue::Null,
            TckValue::Int(3)
        ])
    );

    let mut map = BTreeMap::new();
    map.insert("a".to_string(), TckValue::Int(1));
    map.insert("b".to_string(), TckValue::Str("two".to_string()));
    assert_eq!(parse_value("{b: 'two', a: 1}").unwrap(), TckValue::Map(map));
}

#[test]
fn parses_node_and_rel_literals() {
    let mut node_props = BTreeMap::new();
    node_props.insert("name".to_string(), TckValue::Str("Ada".to_string()));
    node_props.insert("age".to_string(), TckValue::Int(42));
    assert_eq!(
        parse_value("(:Person:Engineer {name: 'Ada', age: 42})").unwrap(),
        TckValue::Node {
            labels: vec!["Person".to_string(), "Engineer".to_string()],
            props: node_props,
        }
    );

    let mut rel_props = BTreeMap::new();
    rel_props.insert("since".to_string(), TckValue::Int(2024));
    assert_eq!(
        parse_value("[:KNOWS {since: 2024}]").unwrap(),
        TckValue::Rel {
            typ: "KNOWS".to_string(),
            props: rel_props,
        }
    );
}

#[test]
fn unordered_multiset_compare() {
    let header = vec!["x".to_string()];
    let expected = vec![
        vec![TckValue::Int(2)],
        vec![TckValue::Int(1)],
        vec![TckValue::Int(2)],
    ];
    let actual = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![2, 1, 2]))],
    )
    .unwrap();
    compare_results(&header, &expected, std::slice::from_ref(&actual), false).unwrap();

    let wrong_duplicate = vec![
        vec![TckValue::Int(1)],
        vec![TckValue::Int(1)],
        vec![TckValue::Int(2)],
    ];
    let err = compare_results(&header, &wrong_duplicate, &[actual], false).unwrap_err();
    assert!(err.contains("unordered rows differ"));
}

#[test]
fn reconstructs_node_and_rel_column_groups() {
    let header = vec!["n".to_string(), "r".to_string()];
    let mut node_props = BTreeMap::new();
    node_props.insert("age".to_string(), TckValue::Int(42));
    node_props.insert("name".to_string(), TckValue::Str("Ada".to_string()));
    let mut rel_props = BTreeMap::new();
    rel_props.insert("since".to_string(), TckValue::Int(2024));
    let expected = vec![vec![
        TckValue::Node {
            labels: vec!["Person".to_string()],
            props: node_props,
        },
        TckValue::Rel {
            typ: "KNOWS".to_string(),
            props: rel_props,
        },
    ]];

    let mut iid = FixedSizeBinaryBuilder::new(16);
    iid.append_value([0_u8; 16]).unwrap();
    let mut src_iid = FixedSizeBinaryBuilder::new(16);
    src_iid.append_value([1_u8; 16]).unwrap();
    let mut dst_iid = FixedSizeBinaryBuilder::new(16);
    dst_iid.append_value([2_u8; 16]).unwrap();

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n._iid", DataType::FixedSizeBinary(16), false),
            Field::new(
                "n._labels",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            Field::new("n.age", DataType::Int64, true),
            Field::new("n.name", DataType::Utf8, true),
            Field::new("r._src_iid", DataType::FixedSizeBinary(16), false),
            Field::new("r._dst_iid", DataType::FixedSizeBinary(16), false),
            Field::new(
                "r._labels",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            Field::new("r.since", DataType::Int64, true),
        ])),
        vec![
            Arc::new(iid.finish()),
            list_utf8(&[&["Person"]]),
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(arrow::array::StringArray::from(vec!["Ada"])),
            Arc::new(src_iid.finish()),
            Arc::new(dst_iid.finish()),
            list_utf8(&[&["KNOWS"]]),
            Arc::new(Int64Array::from(vec![2024])),
        ],
    )
    .unwrap();

    compare_results(&header, &expected, &[batch], true).unwrap();
}

#[test]
fn compares_list_and_map_result_columns() {
    let header = vec!["xs".to_string(), "m".to_string()];
    let mut expected_map = BTreeMap::new();
    expected_map.insert("a".to_string(), TckValue::Int(1));
    expected_map.insert("b".to_string(), TckValue::Str("two".to_string()));
    expected_map.insert("c".to_string(), TckValue::Null);
    let expected = vec![vec![
        TckValue::List(vec![TckValue::Int(1), TckValue::Null, TckValue::Int(3)]),
        TckValue::Map(expected_map),
    ]];

    let mut list_builder = ListBuilder::new(Int64Builder::new());
    list_builder.values().append_value(1);
    list_builder.values().append_null();
    list_builder.values().append_value(3);
    list_builder.append(true);

    let map_like = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("two")])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("c", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        ),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "xs",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
            Field::new("m", DataType::Struct(map_like.fields().clone()), true),
        ])),
        vec![Arc::new(list_builder.finish()), Arc::new(map_like)],
    )
    .unwrap();

    compare_results(&header, &expected, &[batch], true).unwrap();
}

#[test]
fn element_reconstruction_omits_null_properties() {
    let header = vec!["n".to_string()];
    let expected = vec![
        vec![TckValue::Node {
            labels: vec!["A".to_string()],
            props: BTreeMap::new(),
        }],
        vec![TckValue::Node {
            labels: vec!["A".to_string()],
            props: BTreeMap::from([("name".to_string(), TckValue::Str("Ada".to_string()))]),
        }],
    ];

    let mut iid = FixedSizeBinaryBuilder::new(16);
    iid.append_value([0_u8; 16]).unwrap();
    iid.append_value([1_u8; 16]).unwrap();

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n._iid", DataType::FixedSizeBinary(16), false),
            Field::new(
                "n._labels",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            Field::new("n.name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(iid.finish()),
            list_utf8(&[&["A"], &["A"]]),
            Arc::new(StringArray::from(vec![None, Some("Ada")])),
        ],
    )
    .unwrap();

    compare_results(&header, &expected, &[batch], true).unwrap();
}

#[test]
fn element_reconstruction_omits_generated_id_property() {
    let header = vec!["n".to_string()];
    let expected = vec![vec![TckValue::Node {
        labels: vec!["A".to_string()],
        props: BTreeMap::from([("name".to_string(), TckValue::Str("Ada".to_string()))]),
    }]];

    let mut iid = FixedSizeBinaryBuilder::new(16);
    iid.append_value([0_u8; 16]).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n._iid", DataType::FixedSizeBinary(16), false),
            Field::new(
                "n._labels",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            Field::new("n._id", DataType::Utf8, true),
            Field::new("n.name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(iid.finish()),
            list_utf8(&[&["A"]]),
            Arc::new(StringArray::from(vec!["varve:gen:1:0"])),
            Arc::new(StringArray::from(vec!["Ada"])),
        ],
    )
    .unwrap();

    compare_results(&header, &expected, &[batch], true).unwrap();
}

#[test]
fn compare_results_rejects_unexpected_columns() {
    let header = vec!["x".to_string()];
    let expected = vec![vec![TckValue::Int(1)]];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![2])),
        ],
    )
    .unwrap();

    let err = compare_results(&header, &expected, &[batch], true).unwrap_err();
    assert!(err.contains("unexpected actual column `y`"));

    let element_header = vec!["n".to_string()];
    let element_expected = vec![vec![TckValue::Node {
        labels: vec!["A".to_string()],
        props: BTreeMap::from([("name".to_string(), TckValue::Str("Ada".to_string()))]),
    }]];
    let mut iid = FixedSizeBinaryBuilder::new(16);
    iid.append_value([0_u8; 16]).unwrap();
    let element_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n._iid", DataType::FixedSizeBinary(16), false),
            Field::new(
                "n._labels",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            Field::new("n.name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(iid.finish()),
            list_utf8(&[&["A"]]),
            Arc::new(StringArray::from(vec!["Ada"])),
        ],
    )
    .unwrap();

    compare_results(&element_header, &element_expected, &[element_batch], true).unwrap();
}

#[test]
fn float_compare_is_exact_string_form() {
    let header = vec!["x".to_string()];
    let expected = vec![vec![TckValue::Float(1.0)]];
    let actual = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)])),
        vec![Arc::new(arrow::array::Float64Array::from(vec![1.0]))],
    )
    .unwrap();
    compare_results(&header, &expected, &[actual], true).unwrap();

    let int_actual = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let err = compare_results(&header, &expected, &[int_actual], true).unwrap_err();
    assert!(err.contains("ordered row 0 differs"));
}

fn list_utf8(rows: &[&[&str]]) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in rows {
        for value in *row {
            builder.values().append_value(*value);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}
