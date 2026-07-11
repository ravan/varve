use varve::Db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Db::memory();
    let before = db.execute("INSERT (:Product {_id: 1, price: 10})").await?;

    println!("price right after insert:");
    show(
        &db.query("MATCH (p:Product) RETURN p.price AS price")
            .await?,
    )?;

    // Retroactive correction: the price was actually 12 starting back in
    // January 2026 — we just found out about it now.
    db.execute("INSERT (:Product {_id: 1, price: 12}) VALID FROM DATE '2026-01-01'")
        .await?;

    println!("\nprice now (after the retroactive correction):");
    show(
        &db.query("MATCH (p:Product) RETURN p.price AS price")
            .await?,
    )?;

    println!(
        "\nwhat we believed at tx {} ({}), before the correction was recorded:",
        before.tx_id, before.system_time
    );
    show(
        &db.query(format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:Product) RETURN p.price AS price",
            before.system_time
        ))
        .await?,
    )?;
    Ok(())
}

fn show(batches: &[varve::RecordBatch]) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{}",
        datafusion::arrow::util::pretty::pretty_format_batches(batches)?
    );
    Ok(())
}
