use thiserror::Error;

#[derive(Error, Debug)]
#[error("worker crashed")]
pub struct WorkerCrashed;
