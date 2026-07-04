use varve::Db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})").await?;
    let batches = db.query("MATCH (p:Person) RETURN p.name AS name").await?;
    println!(
        "{}",
        datafusion::arrow::util::pretty::pretty_format_batches(&batches)?
    );
    Ok(())
}
