//! Shared disposable `PostgreSQL`/pgvector integration-test infrastructure.
#![allow(dead_code)]

use std::{env, time::Duration};

use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::time::{Instant, sleep, timeout};
use tokio_postgres::NoTls;

const POSTGRES_PORT: u16 = 5432;
const POSTGRES_PASSWORD: &str = "runifold";
const POSTGRES_DATABASE: &str = "runifold";
const READY_TIMEOUT: Duration = Duration::from_secs(30);

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
        let container = GenericImage::new("pgvector/pgvector", "pg16")
            .with_exposed_port(POSTGRES_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
            .with_env_var("POSTGRES_DB", POSTGRES_DATABASE)
            .start()
            .await
            .expect("Docker must start the disposable pgvector test database");
        let connection_url = container_connection_url(&container).await;
        wait_until_ready(&connection_url).await;
        Self {
            connection_url,
            container: Some(container),
        }
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
        let connection_url = container_connection_url(self.owned_container()).await;
        wait_until_ready(&connection_url).await;
        connection_url
    }

    fn owned_container(&self) -> &ContainerAsync<GenericImage> {
        self.container
            .as_ref()
            .expect("fault injection requires a disposable test container")
    }
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
                let query = client.simple_query("SELECT 1").await;
                connection_task.abort();
                match query {
                    Ok(_) => return,
                    Err(error) => error.to_string(),
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

async fn container_connection_url(container: &ContainerAsync<GenericImage>) -> String {
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
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT.tcp())
        .await
        .expect("test database port must be mapped");
    format!("postgres://postgres:{POSTGRES_PASSWORD}@{host}:{port}/{POSTGRES_DATABASE}")
}
