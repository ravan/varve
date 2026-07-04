use std::sync::Arc;
use varve_engine::Db;

// This exact workload raced the slice-2 engine's clock/lock pair into
// OutOfOrderEvent; the writer loop must serialize it.
#[tokio::test]
async fn concurrent_executes_are_serialized_and_all_committed() {
    let db = Arc::new(Db::memory());
    let mut handles = Vec::new();
    for i in 0..50 {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            db.execute(&format!("INSERT (:C {{_id: {i}, n: {i}}})"))
                .await
        }));
    }
    let mut receipts = Vec::new();
    for handle in handles {
        receipts.push(handle.await.unwrap().unwrap());
    }
    receipts.sort_by_key(|r| r.tx_id);
    for pair in receipts.windows(2) {
        assert!(pair[1].tx_id > pair[0].tx_id);
        assert!(pair[1].system_time > pair[0].system_time);
    }
    assert_eq!(receipts.first().unwrap().tx_id, 1);
    assert_eq!(receipts.last().unwrap().tx_id, 50); // 50 unique ids, no gaps

    let batches = db.query("MATCH (c:C) RETURN c.n").await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 50);
}
