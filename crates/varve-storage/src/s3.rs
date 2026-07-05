//! `storage/s3` S3-API §6/§9, AWS, Garage, Ceph RGW, SeaweedFS, MinIO.
//! `store.rs` `ObjectStore` trait covers put/get/get_range/list ONLY.
//! Conditional-PUT is deferred (slice-10 cas-failover) `probe.rs`.

use crate::store::{ObjectStore, StorageError};
use object_store::aws::AmazonS3Builder;
use serde::Deserialize;
use std::sync::Arc;
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};

/// `[storage.s3]` configuration parsed from TOML.
/// `AmazonS3Builder::from_env()` loads ambient `AWS_*` environment variables.
/// AWS credentials fall back to the lazy provider chain when not specified.
#[derive(Deserialize)]
struct S3Config {
    bucket: String,
    /// e.g. `http://127.0.0.1:3900` (Garage). Omitted = AWS endpoint resolution.
    endpoint: Option<String>,
    /// Garage requires match `s3_region` (conventionally "garage");
    /// omitted = env or builder default `us-east-1`.
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    /// Path-style addressing (`endpoint/bucket/key`). Garage & MinIO need
    /// it, so it DEFAULT; `false` selects virtual-hosted style
    /// (`bucket.endpoint/key`, AWS default).
    #[serde(default = "default_path_style")]
    path_style: bool,
    /// Permit plain-HTTP endpoints. Default: derived from endpoint
    /// scheme (`http://…` ⇒ true), so local containers just work while TLS
    /// stays mandatory for https/AWS.
    allow_http: Option<bool>,
}

fn default_path_style() -> bool {
    true
}

fn build_s3(config: &S3Config) -> Result<Arc<dyn ObjectStore>, StorageError> {
    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(&config.bucket)
        .with_virtual_hosted_style_request(!config.path_style);

    if let Some(endpoint) = &config.endpoint {
        let allow_http = config
            .allow_http
            .unwrap_or_else(|| endpoint.starts_with("http://"));
        builder = builder.with_endpoint(endpoint).with_allow_http(allow_http);
    }

    if let Some(region) = &config.region {
        builder = builder.with_region(region);
    }

    if let Some(key) = &config.access_key_id {
        builder = builder.with_access_key_id(key);
    }

    if let Some(secret) = &config.secret_access_key {
        builder = builder.with_secret_access_key(secret);
    }

    let s3 = builder.build().map_err(StorageError::Backend)?;
    Ok(Arc::new(s3))
}

/// `S3StoreFactory` builds an S3 store from `[storage.s3]` configuration.
pub struct S3StoreFactory;

impl ComponentFactory<dyn ObjectStore> for S3StoreFactory {
    fn name(&self) -> &'static str {
        "s3"
    }

    fn build(
        &self,
        cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn ObjectStore>, RegistryError> {
        let section = cfg.child("s3").ok_or_else(|| RegistryError::Build {
            kind: "storage",
            name: "s3".into(),
            source: "missing [storage.s3]".into(),
        })?;

        let s3_config: S3Config = section.get()?;

        build_s3(&s3_config).map_err(|e| RegistryError::Build {
            kind: "storage",
            name: "s3".into(),
            source: Box::new(e),
        })
    }
}
