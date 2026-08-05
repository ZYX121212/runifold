//! Dedicated blocking `PostgreSQL` worker for synchronous runtime store traits.

use std::{
    sync::mpsc::{self, Sender},
    thread,
};

type Job = Box<dyn FnOnce(&mut postgres::Client) + Send + 'static>;

/// Serializes synchronous checkpoint/effect operations on a dedicated thread.
#[derive(Clone)]
pub(crate) struct PostgresBlockingClient {
    jobs: Sender<Job>,
}

impl PostgresBlockingClient {
    pub(crate) fn connect(connection: &str) -> Result<Self, String> {
        let mut client = postgres::Client::connect(connection, postgres::NoTls)
            .map_err(|error| error.to_string())?;
        let (jobs, receiver) = mpsc::channel::<Job>();
        thread::Builder::new()
            .name("runifold-postgres-store".into())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    job(&mut client);
                }
            })
            .map_err(|error| error.to_string())?;
        Ok(Self { jobs })
    }

    pub(crate) fn execute<T, F>(&self, operation: F) -> Result<T, BlockingClientError>
    where
        T: Send + 'static,
        F: FnOnce(&mut postgres::Client) -> T + Send + 'static,
    {
        let (result, receiver) = mpsc::sync_channel(1);
        self.jobs
            .send(Box::new(move |client| {
                let _ = result.send(operation(client));
            }))
            .map_err(|_| BlockingClientError)?;
        receiver.recv().map_err(|_| BlockingClientError)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BlockingClientError;
