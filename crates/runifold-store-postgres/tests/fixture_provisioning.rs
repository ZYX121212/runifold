//! Real pre-start port-conflict recovery without replaying database tests.
mod support;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use support::{MAX_START_ATTEMPTS, PostgresTestContext};
use testcontainers::{
    TestcontainersError,
    bollard::errors::Error as DockerError,
    core::{
        client::docker_client_instance,
        error::{ClientError, WaitContainerError},
    },
};

async fn assert_removed(instance: uuid::Uuid, attempt: usize) {
    let docker = docker_client_instance()
        .await
        .expect("configured fixture Docker client");
    let error = docker
        .inspect_container(&support::provisioning_name(instance, attempt), None)
        .await
        .expect_err("a rejected start must not leave a Created container");
    assert!(matches!(
        error,
        DockerError::DockerResponseServerError {
            status_code: 404,
            ..
        }
    ));
}

#[test]
fn only_explicit_pre_start_port_conflicts_are_retryable() {
    for message in [
        "ports are not available: listen tcp: bind: address already in use",
        "Bind failed: port is already allocated",
    ] {
        let error = TestcontainersError::Client(ClientError::StartContainer(
            DockerError::DockerResponseServerError {
                status_code: 500,
                message: message.into(),
            },
        ));
        assert!(support::is_start_port_conflict(&error));
    }
    for error in [
        TestcontainersError::Client(ClientError::StartContainer(
            DockerError::DockerResponseServerError {
                status_code: 500,
                message: "daemon disconnected; outcome unknown".into(),
            },
        )),
        TestcontainersError::Client(ClientError::CreateContainer(
            DockerError::DockerResponseServerError {
                status_code: 500,
                message: "port is already allocated".into(),
            },
        )),
        TestcontainersError::Client(ClientError::StartContainer(
            DockerError::DockerResponseServerError {
                status_code: 403,
                message: "port is already allocated".into(),
            },
        )),
        TestcontainersError::WaitContainer(WaitContainerError::StartupTimeout),
    ] {
        assert!(!support::is_start_port_conflict(&error));
    }
}

#[tokio::test]
async fn occupied_port_is_reselected_and_rejected_container_is_removed() {
    // Occupy the port in Docker's network namespace as well as on the host.
    let occupied = PostgresTestContext::isolated().await;
    let config: tokio_postgres::Config = occupied.connection_url().parse().expect("fixture URL");
    let port = config.get_ports()[0];
    let instance = uuid::Uuid::now_v7();
    let mut attempts = 0;
    let database = PostgresTestContext::isolated_with_port_picker(instance, || {
        attempts += 1;
        if attempts == 1 {
            port
        } else {
            support::select_host_port()
        }
    })
    .await;
    assert_eq!(attempts, 2);
    assert_removed(instance, 0).await;
    let (client, connection) =
        tokio_postgres::connect(database.connection_url(), tokio_postgres::NoTls)
            .await
            .expect("replacement database is reachable");
    let task = tokio::spawn(connection);
    let row = client
        .query_one("SELECT 42::INT", &[])
        .await
        .expect("actual database assertion");
    assert_eq!(row.get::<_, i32>(0), 42);
    task.abort();
}

#[tokio::test]
async fn repeated_bind_conflicts_fail_at_the_bound_and_remove_every_attempt() {
    let occupied = PostgresTestContext::isolated().await;
    let config: tokio_postgres::Config = occupied.connection_url().parse().expect("fixture URL");
    let port = config.get_ports()[0];
    let instance = uuid::Uuid::now_v7();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let failure = tokio::spawn(async move {
        PostgresTestContext::isolated_with_port_picker(instance, || {
            observed.fetch_add(1, Ordering::SeqCst);
            port
        })
        .await
    })
    .await;
    assert!(matches!(failure, Err(error) if error.is_panic()));
    assert_eq!(calls.load(Ordering::SeqCst), MAX_START_ATTEMPTS);
    for attempt in 0..MAX_START_ATTEMPTS {
        assert_removed(instance, attempt).await;
    }
}
