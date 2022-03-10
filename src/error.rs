use thiserror::Error;

#[derive(Error, Debug)]
#[error("worker terminated")]
pub struct WorkerTerminated;
