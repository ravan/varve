//! Gated S3-backend matrix smoke test (Task 8). Each test starts a real
//! backend container, builds a store THROUGH the `storage_registry()` s3
//! factory (never by hand), and round-trips one object. Every test skips
//! silently unless its backend is named in `VARVE_S3_BACKENDS` — this file
//! must stay green (all skips) with no docker daemon present; Task 9 grows
//! the assertions into the full matrix contract.
#![allow(clippy::unwrap_used)]

use varve_testkit::backends;

/// Starts `name`, builds a store through the registry, round-trips one
/// object. Skips silently unless `VARVE_S3_BACKENDS` names `name`.
async fn smoke(name: &'static str) {
    if !backends::enabled(name) {
        eprintln!("skipping {name} (set VARVE_S3_BACKENDS={name} to run)");
        return;
    }
    let backend = backends::start(name).await;
    let store = backend.params.store();
    store
        .put("v1/smoke", bytes::Bytes::from_static(b"ok"))
        .await
        .unwrap();
    assert_eq!(
        store.get("v1/smoke").await.unwrap(),
        bytes::Bytes::from_static(b"ok")
    );
}

#[tokio::test]
async fn garage_matrix() {
    smoke("garage").await;
}

#[tokio::test]
async fn seaweedfs_matrix() {
    smoke("seaweedfs").await;
}

#[tokio::test]
async fn minio_matrix() {
    smoke("minio").await;
}

#[tokio::test]
async fn ceph_matrix() {
    smoke("ceph").await;
}
