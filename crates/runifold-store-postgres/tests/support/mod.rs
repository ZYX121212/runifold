//! Shared disposable `PostgreSQL`/pgvector integration-test infrastructure.
#![allow(dead_code)]

use std::{env, net::TcpListener, time::Duration};

use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, TestcontainersError,
    bollard::{errors::Error as DockerError, query_parameters::RemoveContainerOptionsBuilder},
    core::{IntoContainerPort, WaitFor, client::docker_client_instance, error::ClientError},
    runners::AsyncRunner,
};
use tokio::time::{Instant, sleep, timeout};
use tokio_postgres::NoTls;

const POSTGRES_PORT: u16 = 5432;
const POSTGRES_PASSWORD: &str = "runifold";
const POSTGRES_DATABASE: &str = "runifold";
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum provisioning attempts, only after explicit pre-start bind rejection.
pub const MAX_START_ATTEMPTS: usize = 4;

/// Live database endpoint backed by an explicit URL or a disposable container.
pub struct PostgresTestContext {
    connection_url: String,
    container: Option<ContainerAsync<GenericImage>>,
}

impl PostgresTestContext {
    /// Uses `environment_variable` when configured, otherwise starts pgvector.
    ///
    /// # Panics
    ///
    /// Panics when the configured URL is invalid text or Docker cannot start a
    /// disposable database.
    pub async fn start(environment_variable: &str) -> Self {
        match env::var(environment_variable) {
            Ok(connection_url) => {
                assert!(
                    !connection_url.trim().is_empty(),
                    "{environment_variable} must not be empty"
                );
                Self {
                    connection_url,
                    container: None,
                }
            }
            Err(env::VarError::NotPresent) => Self::isolated().await,
            Err(env::VarError::NotUnicode(_)) => {
                panic!("{environment_variable} must contain valid Unicode")
            }
        }
    }

    /// Always starts a disposable pgvector container for fault injection.
    ///
    /// # Panics
    ///
    /// Panics when Docker cannot start or expose the disposable database.
    pub async fn isolated() -> Self {
        Self::isolated_with_port_picker(uuid::Uuid::now_v7(), select_host_port).await
    }

    /// Allows fault-injection tests to occupy a chosen port before provisioning.
    ///
    /// # Panics
    ///
    /// Panics on unknown startup failures, failed owned-container cleanup,
    /// exhausted explicit bind conflicts, or an unavailable database.
    pub async fn isolated_with_port_picker(
        instance: uuid::Uuid,
        mut select_port: impl FnMut() -> u16,
    ) -> Self {
        let docker = docker_client_instance()
            .await
            .expect("the configured test Docker client must be available");
        for attempt in 0..MAX_START_ATTEMPTS {
            let host_port = select_port();
            let name = provisioning_name(instance, attempt);
            let started = GenericImage::new("pgvector/pgvector", "pg16")
                .with_exposed_port(POSTGRES_PORT.tcp())
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ))
                .with_mapped_port(host_port, POSTGRES_PORT.tcp())
                .with_container_name(name.clone())
                .with_env_var("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
                .with_env_var("POSTGRES_DB", POSTGRES_DATABASE)
                .start()
                .await;
            match started {
                Ok(container) => {
                    let connection_url = container_connection_url(&container, host_port).await;
                    wait_until_ready(&connection_url).await;
                    return Self {
                        connection_url,
                        container: Some(container),
                    };
                }
                Err(error) => {
                    // The runner creates the container before Docker starts it;
                    // a rejected start otherwise leaves an unowned Created object.
                    let removed = timeout(
                        Duration::from_secs(5),
                        docker.remove_container(
                            &name,
                            Some(
                                RemoveContainerOptionsBuilder::new()
                                    .force(true)
                                    .v(true)
                                    .build(),
                            ),
                        ),
                    )
                    .await
                    .expect("owned-container cleanup must finish within five seconds");
                    assert!(
                        removed.is_ok()
                            || matches!(
                                removed,
                                Err(DockerError::DockerResponseServerError {
                                    status_code: 404,
                                    ..
                                })
                            ),
                        "failed database provisioning must clean up its own container: {removed:?}"
                    );
                    // Only an explicit pre-start bind rejection can be repeated.
                    // Readiness, transport, SQL and test assertions are never retried.
                    assert!(
                        is_start_port_conflict(&error) && attempt + 1 < MAX_START_ATTEMPTS,
                        "Docker database provisioning failed on attempt {}: {error}",
                        attempt + 1
                    );
                }
            }
        }
        unreachable!("the final rejected provisioning attempt panics above")
    }

    /// Returns the live connection URL.
    pub fn connection_url(&self) -> &str {
        &self.connection_url
    }

    /// Stops the owned database while retaining its writable layer.
    ///
    /// # Panics
    ///
    /// Panics for an externally supplied database or when Docker cannot stop
    /// the owned container.
    pub async fn stop(&self) {
        self.owned_container()
            .stop()
            .await
            .expect("fault injection must stop the test database");
    }

    /// Restarts the owned database and waits until it accepts queries.
    ///
    /// # Panics
    ///
    /// Panics for an externally supplied database, when Docker cannot restart
    /// the container, or when `PostgreSQL` does not become ready.
    pub async fn restart(&self) -> String {
        self.owned_container()
            .start()
            .await
            .expect("fault recovery must restart the test database");
        // An explicit host binding survives restart; establish readiness by a
        // real SQL query instead of relying on transient Docker inspect fields.
        let connection_url = self.connection_url.clone();
        wait_until_ready(&connection_url).await;
        connection_url
    }

    fn owned_container(&self) -> &ContainerAsync<GenericImage> {
        self.container
            .as_ref()
            .expect("fault injection requires a disposable test container")
    }
}

/// Names one container owned by this unique fixture invocation.
pub fn provisioning_name(instance: uuid::Uuid, attempt: usize) -> String {
    format!("runifold-postgres-{instance}-{attempt}")
}

/// Recognizes only Docker's explicit pre-start port-binding rejection.
pub fn is_start_port_conflict(error: &TestcontainersError) -> bool {
    matches!(error,
        TestcontainersError::Client(ClientError::StartContainer(
            DockerError::DockerResponseServerError { status_code: 500, message }
        )) if (message.contains("ports are not available") && message.contains("bind: address already in use"))
            || message.contains("port is already allocated"))
}

/// Selects an available port outside default outbound ephemeral ranges.
///
/// # Panics
///
/// Panics if the bounded candidate range has no available port after 128 probes.
pub fn select_host_port() -> u16 {
    // Avoid the default Linux/macOS outbound ephemeral ranges: releasing a
    // kernel-selected port lets an unrelated connection claim it before Docker.
    for _ in 0..128 {
        let offset = u16::try_from(uuid::Uuid::now_v7().as_u128() % 20_000)
            .expect("the port offset is bounded below u16::MAX");
        let port = 10_000 + offset;
        if let Ok(reservation) = TcpListener::bind(("0.0.0.0", port)) {
            drop(reservation);
            return port;
        }
    }
    panic!("no disposable database port was available in the bounded fixture range")
}

async fn wait_until_ready(connection_url: &str) {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        let last_error = match timeout(
            Duration::from_secs(2),
            tokio_postgres::connect(connection_url, NoTls),
        )
        .await
        {
            Ok(Ok((client, connection))) => {
                let connection_task = tokio::spawn(connection);
                let query = timeout(Duration::from_secs(2), client.simple_query("SELECT 1")).await;
                connection_task.abort();
                match query {
                    Ok(Ok(_)) => return,
                    Ok(Err(error)) => error.to_string(),
                    Err(_) => "readiness query timed out".to_owned(),
                }
            }
            Ok(Err(error)) => format!("{error:?}"),
            Err(_) => "connection attempt timed out".to_owned(),
        };
        assert!(
            Instant::now() < deadline,
            "disposable PostgreSQL did not become ready within {READY_TIMEOUT:?}: {last_error}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

async fn container_connection_url(container: &ContainerAsync<GenericImage>, port: u16) -> String {
    let host = container
        .get_host()
        .await
        .expect("test database host must be discoverable")
        .to_string();
    let host = if host == "localhost" {
        "127.0.0.1"
    } else {
        host.as_str()
    };
    format!("postgres://postgres:{POSTGRES_PASSWORD}@{host}:{port}/{POSTGRES_DATABASE}")
}
