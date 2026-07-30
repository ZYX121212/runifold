//! Dedicated real `MinIO` verification. The CI job supplies a lock-enabled bucket.

#![cfg(feature = "archive-s3")]

use std::{
    fs,
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    num::NonZeroU32,
    path::PathBuf,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use reqwest::Url;
use runifold::{
    CheckpointId,
    archive_s3::{
        S3ArchiveEncryption, S3ArchivePresignRequest, S3ArchivePresigner, S3ObjectLock,
        S3ObjectLockMode, S3SigV4Credentials, S3SigV4Presigner, S3SigV4PresignerConfig,
        S3TombstoneArchive, S3TombstoneArchiveConfig,
    },
};
use runifold_workflow::{
    WorkflowTaskStatus, WorkflowTaskTombstone, WorkflowTaskTombstoneArchive,
    WorkflowTaskTombstoneArchiveBatch, WorkflowTaskTombstoneArchiveBatchId,
    WorkflowTaskTombstoneCursor, WorkflowTenantId,
};
use serde::Serialize;

const MINIO_SERVER_IMAGE: &str = "minio/minio:RELEASE.2025-04-22T22-12-26Z";
const MINIO_SERVER_DIGEST: &str =
    "sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e";
const MINIO_CLIENT_IMAGE: &str = "minio/mc:RELEASE.2025-04-16T18-13-26Z";
const MINIO_CLIENT_DIGEST: &str =
    "sha256:aead63c77f9db9107f1696fb08ecb0faeda23729cde94b0f663edf4fe09728e3";

#[derive(Debug, Serialize)]
struct MinioReliabilityEvidence {
    schema_version: u32,
    suite: &'static str,
    result: &'static str,
    revision: Option<String>,
    server_image: &'static str,
    server_digest: &'static str,
    client_image: &'static str,
    client_digest: &'static str,
    stress_iterations: u32,
    concurrent_requests: u64,
    response_loss_recoveries: u32,
    elapsed_ms: u64,
}

#[tokio::test]
#[ignore = "requires the mandatory MinIO CI service and lock-enabled bucket"]
async fn lock_checksum_concurrency_and_reconstruction_survive_real_minio() {
    let started = Instant::now();
    let endpoint = required("RUNIFOLD_MINIO_ENDPOINT");
    let access_key = required("RUNIFOLD_MINIO_ACCESS_KEY");
    let secret_key = required("RUNIFOLD_MINIO_SECRET_KEY");
    let bucket = required("RUNIFOLD_MINIO_BUCKET");
    let endpoint = Url::parse(&endpoint).expect("MinIO endpoint must be a valid URL");
    let original_batch = batch("minio-live", "original");

    let archive_client = archive(&endpoint, &bucket, &access_key, &secret_key);
    let (left, right) = tokio::join!(
        archive_client.archive(original_batch.clone()),
        archive_client.archive(original_batch.clone())
    );
    let left = left.expect("one concurrent archive must create or reconcile");
    let right = right.expect("the competing archive must reconcile");
    assert_eq!(left, right);

    let reconstructed = archive(&endpoint, &bucket, &access_key, &secret_key);
    assert_eq!(
        reconstructed
            .archive(original_batch)
            .await
            .expect("a reconstructed archive must reconcile the committed object"),
        left
    );

    let mismatch = reconstructed
        .archive(batch("minio-live", "tampered"))
        .await
        .expect_err("the same batch identity with different bytes must fail closed");
    assert!(mismatch.to_string().contains("matching checksum"));

    let signer = signer(&endpoint, &access_key, &secret_key);
    let head_authority = signer
        .presign(S3ArchivePresignRequest {
            bucket: bucket.clone(),
            key: "live/minio-live.json".into(),
            required_put_headers: reqwest::header::HeaderMap::new(),
        })
        .await
        .expect("HEAD authority must be signed");
    let head = reqwest::Client::new()
        .head(head_authority.head_url)
        .headers(head_authority.head_headers)
        .send()
        .await
        .expect("MinIO HEAD must be reachable");
    assert!(head.status().is_success());
    assert_eq!(
        head.headers()
            .get("x-amz-object-lock-mode")
            .and_then(|value| value.to_str().ok()),
        Some("COMPLIANCE")
    );
    assert!(
        head.headers()
            .get("x-amz-object-lock-retain-until-date")
            .is_some()
    );
    assert!(head.headers().get("x-amz-meta-runifold-sha256").is_some());
    assert!(head.headers().get("x-amz-version-id").is_some());

    let iterations = stress_iterations();
    let concurrent_requests =
        concurrent_stress(&endpoint, &bucket, &access_key, &secret_key, iterations).await;
    let target = endpoint
        .socket_addrs(|| None)
        .expect("MinIO endpoint must resolve")
        .into_iter()
        .next()
        .expect("MinIO endpoint must resolve to one address");
    let response_loss_recoveries = response_loss_stress(
        &endpoint,
        target,
        &bucket,
        &access_key,
        &secret_key,
        iterations.min(8),
    )
    .await;
    write_evidence(&MinioReliabilityEvidence {
        schema_version: 1,
        suite: "runifold.s3-worm-reliability",
        result: "passed",
        revision: evidence_revision(),
        server_image: MINIO_SERVER_IMAGE,
        server_digest: MINIO_SERVER_DIGEST,
        client_image: MINIO_CLIENT_IMAGE,
        client_digest: MINIO_CLIENT_DIGEST,
        stress_iterations: iterations,
        concurrent_requests,
        response_loss_recoveries,
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    });
}

async fn concurrent_stress(
    endpoint: &Url,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    iterations: u32,
) -> u64 {
    for iteration in 0..iterations {
        let archive_client = archive(endpoint, bucket, access_key, secret_key);
        let batch = batch(&format!("minio-stress-{iteration}"), "concurrent-stress");
        let results = tokio::join!(
            archive_client.archive(batch.clone()),
            archive_client.archive(batch.clone()),
            archive_client.archive(batch.clone()),
            archive_client.archive(batch),
        );
        let receipts = [
            results.0.expect("stress writer one must converge"),
            results.1.expect("stress writer two must converge"),
            results.2.expect("stress writer three must converge"),
            results.3.expect("stress writer four must converge"),
        ];
        assert!(
            receipts.windows(2).all(|pair| pair[0] == pair[1]),
            "all same-batch stress writers must return one receipt"
        );
    }
    u64::from(iterations) * 4
}

async fn response_loss_stress(
    endpoint: &Url,
    target: SocketAddr,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    iterations: u32,
) -> u32 {
    for iteration in 0..iterations {
        let fault_batch = batch(
            &format!("minio-ambiguous-{iteration}"),
            "response-lost-after-commit",
        );
        let (proxy_endpoint, proxy) = response_loss_proxy(target);
        let recovered = archive(&proxy_endpoint, bucket, access_key, secret_key)
            .archive(fault_batch.clone())
            .await
            .expect("a committed PUT with a lost response must reconcile through HEAD");
        proxy.join().expect("fault proxy must finish cleanly");
        let replayed = archive(endpoint, bucket, access_key, secret_key)
            .archive(fault_batch)
            .await
            .expect("direct replay after the injected disconnect must remain idempotent");
        assert_eq!(recovered, replayed);
    }
    iterations
}

fn archive(
    endpoint: &Url,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
) -> S3TombstoneArchive<S3SigV4Presigner> {
    let config = S3TombstoneArchiveConfig::new(bucket, "live", S3ArchiveEncryption::Aes256)
        .expect("MinIO archive policy must be valid")
        .with_object_lock(S3ObjectLock {
            mode: S3ObjectLockMode::Compliance,
            retention_days: NonZeroU32::new(1).expect("one day is positive"),
        });
    S3TombstoneArchive::new(config, Arc::new(signer(endpoint, access_key, secret_key)))
}

fn signer(endpoint: &Url, access_key: &str, secret_key: &str) -> S3SigV4Presigner {
    S3SigV4Presigner::new(
        S3SigV4PresignerConfig::new(endpoint.clone(), "us-east-1", 300, true)
            .expect("MinIO signer policy must be valid"),
        S3SigV4Credentials::new(access_key, secret_key, None)
            .expect("MinIO credentials must be valid"),
    )
}

fn batch(batch_id: &str, workflow: &str) -> WorkflowTaskTombstoneArchiveBatch {
    let tenant_id = WorkflowTenantId::parse("minio-live").expect("tenant fixture is valid");
    WorkflowTaskTombstoneArchiveBatch {
        batch_id: WorkflowTaskTombstoneArchiveBatchId::parse(batch_id)
            .expect("batch fixture is valid"),
        tenant_id: tenant_id.clone(),
        tombstones: vec![WorkflowTaskTombstone {
            cursor: WorkflowTaskTombstoneCursor::new(1),
            checkpoint_id: CheckpointId::new(),
            tenant_id,
            workflow: workflow.into(),
            workflow_version: 1,
            final_status: WorkflowTaskStatus::Completed,
            created_at_ms: 1,
            terminal_at_ms: 2,
            deleted_at_ms: 3,
        }],
    }
}

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!("{name} is required; the dedicated MinIO test must never silently skip")
    })
}

fn stress_iterations() -> u32 {
    std::env::var("RUNIFOLD_MINIO_STRESS_ITERATIONS").map_or(16, |value| {
        value
            .parse::<u32>()
            .ok()
            .filter(|value| (1..=1_000).contains(value))
            .expect("RUNIFOLD_MINIO_STRESS_ITERATIONS must be in 1..=1000")
    })
}

fn evidence_revision() -> Option<String> {
    std::env::var("RUNIFOLD_EVIDENCE_REVISION")
        .ok()
        .filter(|value| {
            (7..=64).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

fn write_evidence(evidence: &MinioReliabilityEvidence) {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("runifold package must be nested under the workspace")
        .to_owned();
    let path = std::env::var("RUNIFOLD_EVIDENCE_PATH").map_or_else(
        |_| workspace_root.join("target/reliability-evidence/minio-worm.json"),
        |value| {
            let path = PathBuf::from(value);
            if path.is_absolute() {
                path
            } else {
                workspace_root.join(path)
            }
        },
    );
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).expect("evidence directory must be writable");
    }
    let bytes = serde_json::to_vec_pretty(evidence).expect("evidence must serialize");
    fs::write(path, bytes).expect("evidence report must be writable");
}

fn response_loss_proxy(target: SocketAddr) -> (Url, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("fault proxy must bind");
    let endpoint = Url::parse(&format!(
        "http://{}",
        listener.local_addr().expect("proxy address must exist")
    ))
    .expect("proxy endpoint must be valid");
    let worker = thread::spawn(move || {
        let (client, request) = read_request(&listener);
        assert!(request.starts_with(b"PUT "));
        let response = forward(&request, target);
        assert_success(&response);
        client
            .shutdown(Shutdown::Both)
            .expect("the injected response-loss connection must close");

        let (mut client, request) = read_request(&listener);
        assert!(request.starts_with(b"HEAD "));
        let response = forward(&request, target);
        assert_success(&response);
        client
            .write_all(&response)
            .expect("reconciliation response must reach the client");
    });
    (endpoint, worker)
}

fn read_request(listener: &TcpListener) -> (TcpStream, Vec<u8>) {
    let (mut stream, _) = listener.accept().expect("fault proxy must accept");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("fault proxy timeout must be valid");
    let mut request = Vec::new();
    let mut buffer = [0_u8; 4_096];
    loop {
        let count = stream.read(&mut buffer).expect("request read must succeed");
        assert!(count > 0, "client closed before completing its request");
        request.extend_from_slice(&buffer[..count]);
        let Some(header_end) = find_header_end(&request) else {
            continue;
        };
        let content_length = content_length(&request[..header_end]);
        if request.len() >= header_end + 4 + content_length {
            return (stream, request);
        }
    }
}

fn forward(request: &[u8], target: SocketAddr) -> Vec<u8> {
    let mut upstream = TcpStream::connect_timeout(&target, Duration::from_secs(5))
        .expect("fault proxy must connect to MinIO");
    upstream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("MinIO response timeout must be valid");
    upstream
        .write_all(request)
        .expect("fault proxy must forward the complete request");
    let mut response = Vec::new();
    let mut buffer = [0_u8; 4_096];
    while find_header_end(&response).is_none() {
        let count = upstream
            .read(&mut buffer)
            .expect("MinIO response headers must arrive");
        assert!(count > 0, "MinIO closed before returning response headers");
        response.extend_from_slice(&buffer[..count]);
    }
    response
}

fn find_header_end(message: &[u8]) -> Option<usize> {
    message.windows(4).position(|window| window == b"\r\n\r\n")
}

fn assert_success(response: &[u8]) {
    let status = String::from_utf8_lossy(response)
        .lines()
        .next()
        .unwrap_or("<missing status>")
        .to_owned();
    assert!(
        status.starts_with("HTTP/1.1 200"),
        "MinIO returned {status}"
    );
}

fn content_length(headers: &[u8]) -> usize {
    String::from_utf8_lossy(headers)
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or_default()
}
