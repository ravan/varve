use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use varve::Db;
use varve_types::Value;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Db::memory();

    db.execute("CREATE GRAPH tour").await?;
    let seed = db
        .execute(
            "USE tour; \
             INSERT (:Person {_id: 1, name: 'Ada', age: 36, city: 'London', legacy: 'yes'}), \
                    (:Person {_id: 2, name: 'Bob', age: 41, city: 'Paris'}), \
                    (:Person {_id: 3, name: 'Cy', age: 36, city: 'London'}); \
             MATCH (a:Person {_id: 1}), (b:Person {_id: 2}) INSERT (a)-[:KNOWS]->(b); \
             MATCH (a:Person {_id: 1}), (c:Person {_id: 3}) INSERT (a)-[:KNOWS]->(c)",
        )
        .await?;

    db.execute("USE tour; MATCH (p:Person {_id: 1}) SET p.role = 'founder', p:Speaker")
        .await?;
    db.execute("USE tour; MATCH (p:Person {_id: 1}) REMOVE p.legacy")
        .await?;

    show(
        "aggregation",
        &db.query(
            "USE tour; MATCH (p:Person) \
             RETURN p.city AS city, count(*) AS people \
             ORDER BY city ASC",
        )
        .await?,
    )?;

    show(
        "optional match (missing mentors preserved)",
        &db.query(
            "USE tour; MATCH (p:Person) \
             OPTIONAL MATCH (p)-[:MENTORS]->(mentor:Person) \
             RETURN p.name AS person, mentor.name AS mentor \
             ORDER BY person ASC",
        )
        .await?,
    )?;

    show(
        "exists",
        &db.query(
            "USE tour; MATCH (p:Person) \
             WHERE EXISTS { (p)-[:KNOWS]->(friend:Person) } \
             RETURN p.name AS connector \
             ORDER BY connector ASC",
        )
        .await?,
    )?;

    let params = BTreeMap::from([("city".to_string(), Value::Str("London".to_string()))]);
    show(
        "params",
        &db.query(
            "USE tour; MATCH (p:Person) \
             WHERE p.city = $city \
             RETURN p.name AS name \
             ORDER BY name ASC",
        )
        .params(params.clone())
        .await?,
    )?;

    show(
        "order by limit",
        &db.query(
            "USE tour; MATCH (p:Person) \
             RETURN p.name AS name, p.age AS age \
             ORDER BY age DESC, name ASC \
             LIMIT 2",
        )
        .await?,
    )?;

    show_sorted_string_pairs(
        "union (source/name, sorted)",
        &db.query(
            "USE tour; MATCH (p:Person {_id: 2}) RETURN 'city' AS source, p.name AS name \
             UNION \
             MATCH (p:Speaker) RETURN 'label' AS source, p.name AS name",
        )
        .await?,
        "source",
        "name",
    )?;

    let historic_ada = format!(
        "USE tour; FOR SYSTEM_TIME AS OF TIMESTAMP '{}' \
         MATCH (p:Person {{_id: 1}}) RETURN p.name AS name",
        seed.system_time
    );

    show("history before erase", &db.query(&historic_ada).await?)?;
    db.execute("USE tour; MATCH (p:Person {_id: 1}) DETACH ERASE p")
        .await?;
    let after_erase = db.query(&historic_ada).await?;
    println!(
        "history after erase visible rows: {}",
        row_count(&after_erase)
    );
    show("history after erase", &after_erase)?;

    Ok(())
}

fn show(title: &str, batches: &[varve::RecordBatch]) -> Result<(), Box<dyn std::error::Error>> {
    println!("{title}:");
    println!(
        "{}",
        datafusion::arrow::util::pretty::pretty_format_batches(batches)?
    );
    Ok(())
}

fn row_count(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(varve::RecordBatch::num_rows).sum()
}

fn show_sorted_string_pairs(
    title: &str,
    batches: &[varve::RecordBatch],
    left_name: &str,
    right_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rows = Vec::new();
    for batch in batches {
        let left = string_column(batch, left_name)?;
        let right = string_column(batch, right_name)?;
        for row in 0..batch.num_rows() {
            rows.push((string_value(left, row), string_value(right, row)));
        }
    }
    rows.sort();

    let left_values: Vec<String> = rows.iter().map(|row| row.0.clone()).collect();
    let right_values: Vec<String> = rows.iter().map(|row| row.1.clone()).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new(left_name, DataType::Utf8, false),
        Field::new(right_name, DataType::Utf8, false),
    ]));
    let sorted = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(left_values)) as ArrayRef,
            Arc::new(StringArray::from(right_values)) as ArrayRef,
        ],
    )?;

    show(title, &[sorted])
}

fn string_column<'a>(
    batch: &'a varve::RecordBatch,
    name: &str,
) -> Result<&'a StringArray, Box<dyn std::error::Error>> {
    let column = batch.column_by_name(name).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("missing column {name}"),
        )
    })?;
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("column {name} is not Utf8"),
            )
            .into()
        })
}

fn string_value(column: &StringArray, row: usize) -> String {
    if column.is_null(row) {
        String::new()
    } else {
        column.value(row).to_string()
    }
}
