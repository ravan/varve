//! Test-only harness that drives real S3-API backends (Garage, SeaweedFS,
//! MinIO, Ceph) through the docker CLI (`std::process::Command`) — never the
//! `testcontainers` crate. Garage's `layout assign`/`apply` dance and
//! MinIO's `mc` sidecar both need multi-step `docker exec` choreography that
//! a generic container-lifecycle crate does not model well (design decision
//! 7). Everything about ONE backend — image pin, init dance, credentials —
//! lives in this single file, so a drifted image tag or CLI wording is a
//! one-file fix.
//!
//! Stores are always built THROUGH the real `storage_registry()` s3 factory
//! (never hand-assembled), so these tests exercise the exact path production
//! config takes. Containers are only started when `VARVE_S3_BACKENDS` opts a
//! backend in (see [`enabled`]); otherwise every test using this module
//! returns immediately, so `cargo test --workspace` stays hermetic and fast
//! with no docker daemon present.
#![allow(clippy::unwrap_used, clippy::expect_used)]
// Sanctioned exception (Global Constraints): this module's entire job is to
// drive an external docker daemon through a brittle, multi-step init dance
// that only ever runs under an explicit `VARVE_S3_BACKENDS` opt-in. If a
// container fails to start, or its CLI output stops parsing the way we
// expect, the right behavior is to abort loudly with a panic that pinpoints
// the failing step — not to thread a `Result` through call sites that exist
// purely to set up a test fixture.

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use varve_config::{BuildContext, Config};
use varve_storage::{storage_registry, ObjectStore};

/// Image pins. Bump ONLY here, and record the bump in STATUS.md.
pub const GARAGE_IMAGE: &str = "dxflrs/garage:v1.0.1";
pub const SEAWEEDFS_IMAGE: &str = "chrislusf/seaweedfs:3.80";
pub const MINIO_IMAGE: &str = "minio/minio:RELEASE.2025-04-22T22-12-26Z";
pub const MC_IMAGE: &str = "minio/mc:RELEASE.2025-04-16T18-13-26Z";
pub const CEPH_IMAGE: &str = "quay.io/ceph/demo:latest-quincy";

pub const ACCESS_KEY: &str = "varve";
pub const SECRET_KEY: &str = "varvesecret123"; // >= 8 chars (MinIO minimum)
pub const DB_BUCKET: &str = "varve";
pub const CONTRACT_BUCKET: &str = "varve-contract";

/// Connection parameters for one running backend. `bucket` is switchable so
/// tests that need raw-DB and contract phases side by side can use ISOLATED
/// buckets on the same running backend.
#[derive(Clone, Debug)]
pub struct S3Params {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl S3Params {
    pub fn with_bucket(&self, bucket: &str) -> S3Params {
        S3Params {
            bucket: bucket.to_string(),
            ..self.clone()
        }
    }

    /// `[storage]` TOML for this backend — tests configure through the REAL
    /// factory path, never a hand-assembled store.
    pub fn storage_toml(&self) -> String {
        self.storage_toml_with("")
    }

    /// Same as [`Self::storage_toml`], but with `extra` lines inserted
    /// INSIDE the `[storage]` table (e.g. `"max_block_rows = 2\n"`), before
    /// the nested `[storage.s3]` table — so `extra`'s keys stay in
    /// `[storage]`'s scope, not `[storage.s3]`'s.
    pub fn storage_toml_with(&self, extra: &str) -> String {
        format!(
            "[storage]\nbackend = \"s3\"\n{extra}[storage.s3]\n\
             endpoint = \"{}\"\nbucket = \"{}\"\nregion = \"{}\"\n\
             access_key_id = \"{}\"\nsecret_access_key = \"{}\"\n",
            self.endpoint, self.bucket, self.region, self.access_key_id, self.secret_access_key
        )
    }

    /// Builds a live store through the REAL `storage_registry()` s3
    /// factory — never hand-assembled.
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        let config = Config::from_toml_str(&self.storage_toml()).expect("valid storage toml");
        let section = config.section("storage").expect("[storage] section");
        storage_registry()
            .build("s3", &section, &BuildContext::empty())
            .expect("s3 store builds")
    }
}

/// Runs `docker` with `args`, returning trimmed stdout on success.
fn docker(args: &[&str]) -> Result<String, String> {
    let out = Command::new("docker")
        .args(args)
        .output()
        .map_err(|e| format!("failed to spawn docker: {e}"))?;
    let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if out.status.success() {
        Ok(stdout)
    } else {
        Err(format!(
            "docker {args:?} failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        ))
    }
}

/// A running container, force-removed on Drop.
struct Container {
    id: String,
    /// Mounted config files must outlive the container.
    _files: Option<tempfile::TempDir>,
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.id]).output();
    }
}

fn run_detached(args: &[&str], files: Option<tempfile::TempDir>) -> Container {
    let mut full = vec!["run", "-d"];
    full.extend_from_slice(args);
    let id = docker(&full).expect("container starts");
    Container { id, _files: files }
}

/// The host port docker mapped to `container_port` (we always publish
/// `127.0.0.1:0:<port>` and let the OS pick a free one).
fn host_port(container: &Container, container_port: u16) -> u16 {
    let spec = format!("{container_port}/tcp");
    let out = docker(&["port", &container.id, &spec]).expect("docker port");
    let line = out.lines().next().expect("a port mapping line");
    line.rsplit(':')
        .next()
        .expect("host port")
        .trim()
        .parse()
        .expect("numeric host port")
}

fn exec(container: &Container, cmd: &[&str]) -> Result<String, String> {
    let mut args = vec!["exec", container.id.as_str()];
    args.extend_from_slice(cmd);
    docker(&args)
}

/// Retries `f` (500 ms apart, up to 2 minutes) until it yields a value —
/// container init is eventually consistent, not instantaneous.
async fn poll<T>(mut f: impl FnMut() -> Option<T>) -> T {
    for _ in 0..240 {
        if let Some(v) = f() {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("timed out waiting for container init");
}

/// Waits until `params` can LIST its bucket — container-reported "healthy"
/// does not guarantee the S3 API is actually serving requests yet.
async fn wait_ready(params: &S3Params) {
    let store = params.store();
    for _ in 0..240 {
        if store.list("").await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("backend at {} never became ready", params.endpoint);
}

/// True if `name` is opted into `VARVE_S3_BACKENDS` (a comma-separated
/// list, or the literal `all`). Unset/absent means every backend is
/// disabled, so container tests skip silently by default.
pub fn enabled(name: &str) -> bool {
    match std::env::var("VARVE_S3_BACKENDS") {
        Ok(v) => {
            let v = v.to_lowercase();
            v.trim() == "all" || v.split(',').any(|b| b.trim() == name)
        }
        Err(_) => false,
    }
}

pub struct Backend {
    pub name: &'static str,
    /// Defaults to `DB_BUCKET`; use `params.with_bucket(CONTRACT_BUCKET)`
    /// for the isolated contract bucket.
    pub params: S3Params,
    _container: Container,
}

pub async fn start(name: &str) -> Backend {
    match name {
        "garage" => start_garage().await,
        "seaweedfs" => start_seaweedfs().await,
        "minio" => start_minio().await,
        "ceph" => start_ceph().await,
        other => panic!("unknown backend '{other}'"),
    }
}

// ---------------------------------------------------------------- garage --

/// One-node Garage config (v1.x quick start). `rpc_secret` must be 64 hex
/// chars; the value here is fixed test-rig material, not a real secret.
const GARAGE_TOML: &str = r#"
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "1799bccfd7411eddcf9ebd316bc1f5287ad12a68094e1c6ac6abde7e6feae1ec"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"
"#;

/// Extracts the value after `label` on the first line carrying it.
fn field(out: &str, label: &str) -> String {
    out.lines()
        .find_map(|l| l.trim().strip_prefix(label))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("'{label}' not found in output:\n{out}"))
}

async fn start_garage() -> Backend {
    let files = tempfile::tempdir().expect("tempdir");
    let cfg = files.path().join("garage.toml");
    std::fs::write(&cfg, GARAGE_TOML).expect("write garage.toml");
    let mount = format!("{}:/etc/garage.toml", cfg.display());
    let container = run_detached(
        &["-p", "127.0.0.1:0:3900", "-v", &mount, GARAGE_IMAGE],
        Some(files),
    );
    let port = host_port(&container, 3900);

    // Quick-start layout dance (Garage v1.x CLI): find the sole node's ID,
    // assign it a zone/capacity, apply the layout, then create buckets and
    // an access key.
    let node_id = poll(|| {
        let status = exec(&container, &["/garage", "status"]).ok()?;
        status.lines().find_map(|l| {
            let first = l.split_whitespace().next()?;
            let id = first.trim_end_matches('…');
            (id.len() >= 16 && id.chars().all(|c| c.is_ascii_hexdigit())).then(|| id.to_string())
        })
    })
    .await;

    exec(
        &container,
        &[
            "/garage", "layout", "assign", "-z", "dc1", "-c", "1G", &node_id,
        ],
    )
    .expect("garage layout assign");
    exec(
        &container,
        &["/garage", "layout", "apply", "--version", "1"],
    )
    .expect("garage layout apply");
    for bucket in [DB_BUCKET, CONTRACT_BUCKET] {
        exec(&container, &["/garage", "bucket", "create", bucket]).expect("garage bucket create");
    }
    let key_out =
        exec(&container, &["/garage", "key", "create", "varve-ci"]).expect("garage key create");
    let access = field(&key_out, "Key ID:");
    let secret = field(&key_out, "Secret key:");
    for bucket in [DB_BUCKET, CONTRACT_BUCKET] {
        exec(
            &container,
            &[
                "/garage", "bucket", "allow", "--read", "--write", "--owner", bucket, "--key",
                "varve-ci",
            ],
        )
        .expect("garage bucket allow");
    }

    let params = S3Params {
        endpoint: format!("http://127.0.0.1:{port}"),
        bucket: DB_BUCKET.to_string(),
        region: "garage".to_string(),
        access_key_id: access,
        secret_access_key: secret,
    };
    wait_ready(&params).await;
    Backend {
        name: "garage",
        params,
        _container: container,
    }
}

// ------------------------------------------------------------- seaweedfs --

const SEAWEEDFS_S3_JSON: &str = r#"{
  "identities": [
    {
      "name": "varve",
      "credentials": [{ "accessKey": "varve", "secretKey": "varvesecret123" }],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}"#;

async fn start_seaweedfs() -> Backend {
    let files = tempfile::tempdir().expect("tempdir");
    let cfg = files.path().join("s3.json");
    std::fs::write(&cfg, SEAWEEDFS_S3_JSON).expect("write s3.json");
    let mount = format!("{}:/etc/seaweedfs/s3.json", cfg.display());
    let container = run_detached(
        &[
            "-p",
            "127.0.0.1:0:8333",
            "-v",
            &mount,
            SEAWEEDFS_IMAGE,
            "server",
            "-s3",
            "-s3.config=/etc/seaweedfs/s3.json",
        ],
        Some(files),
    );
    let port = host_port(&container, 8333);

    for bucket in [DB_BUCKET, CONTRACT_BUCKET] {
        let cmd = format!("s3.bucket.create -name {bucket}");
        poll(|| exec(&container, &["weed", "shell", "-c", &cmd]).ok()).await;
    }

    let params = S3Params {
        endpoint: format!("http://127.0.0.1:{port}"),
        bucket: DB_BUCKET.to_string(),
        region: "us-east-1".to_string(),
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    wait_ready(&params).await;
    Backend {
        name: "seaweedfs",
        params,
        _container: container,
    }
}

// ----------------------------------------------------------------- minio --

async fn start_minio() -> Backend {
    let user_env = format!("MINIO_ROOT_USER={ACCESS_KEY}");
    let pass_env = format!("MINIO_ROOT_PASSWORD={SECRET_KEY}");
    let container = run_detached(
        &[
            "-p",
            "127.0.0.1:0:9000",
            "-e",
            &user_env,
            "-e",
            &pass_env,
            MINIO_IMAGE,
            "server",
            "/data",
        ],
        None,
    );
    let port = host_port(&container, 9000);
    // Buckets via a one-shot `mc` container sharing minio's network
    // namespace (so 127.0.0.1:9000 resolves minio, not the host).
    let net = format!("container:{}", container.id);
    let script = format!(
        "mc alias set m http://127.0.0.1:9000 {ACCESS_KEY} {SECRET_KEY} \
         && mc mb m/{DB_BUCKET} && mc mb m/{CONTRACT_BUCKET}"
    );
    poll(|| {
        Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                &net,
                "--entrypoint",
                "sh",
                MC_IMAGE,
                "-c",
                &script,
            ])
            .output()
            .ok()
            .filter(|o| o.status.success())
    })
    .await;

    let params = S3Params {
        endpoint: format!("http://127.0.0.1:{port}"),
        bucket: DB_BUCKET.to_string(),
        region: "us-east-1".to_string(),
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    wait_ready(&params).await;
    Backend {
        name: "minio",
        params,
        _container: container,
    }
}

// ------------------------------------------------------------------ ceph --

/// Ceph demo (weekly CI only): heavyweight, host networking, RGW on :8080.
/// The demo entrypoint auto-creates a `CEPH_DEMO_*` user and bucket; the
/// contract bucket is added with the image's bundled `s3cmd`.
async fn start_ceph() -> Backend {
    let demo_bucket = format!("CEPH_DEMO_BUCKET={DB_BUCKET}");
    let demo_access = format!("CEPH_DEMO_ACCESS_KEY={ACCESS_KEY}");
    let demo_secret = format!("CEPH_DEMO_SECRET_KEY={SECRET_KEY}");
    let container = run_detached(
        &[
            "--net",
            "host",
            "-e",
            "MON_IP=127.0.0.1",
            "-e",
            "CEPH_PUBLIC_NETWORK=127.0.0.0/8",
            "-e",
            "CEPH_DEMO_UID=varve",
            "-e",
            &demo_bucket,
            "-e",
            &demo_access,
            "-e",
            &demo_secret,
            CEPH_IMAGE,
        ],
        None,
    );

    let params = S3Params {
        endpoint: "http://127.0.0.1:8080".to_string(),
        bucket: DB_BUCKET.to_string(),
        region: "us-east-1".to_string(),
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    wait_ready(&params).await;

    // The demo entrypoint only creates the DB bucket; add the contract
    // bucket via the bundled s3cmd, pointed at the loopback RGW.
    let mb = format!("s3://{CONTRACT_BUCKET}");
    poll(|| {
        exec(
            &container,
            &[
                "s3cmd",
                "--host=127.0.0.1:8080",
                "--host-bucket=",
                "mb",
                &mb,
            ],
        )
        .ok()
    })
    .await;

    Backend {
        name: "ceph",
        params,
        _container: container,
    }
}
