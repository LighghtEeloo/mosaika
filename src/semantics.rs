use std::path::PathBuf;
use thiserror::Error;

pub struct Transaction {
    pub overwrites: Vec<PathBuf>,
    pub arrows: Vec<Arrow>,
    pub transform: Vec<String>,
}

#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("source path {0} does not exist")]
    MissingSource(PathBuf),
    #[error("glob pattern is invalid: {0}")]
    GlobPattern(#[from] glob::PatternError),
    #[error("glob is invalid: {0}")]
    Glob(#[from] glob::GlobError),
    #[error("strip prefix is invalid: {0}")]
    StripPrefix(#[from] std::path::StripPrefixError),
    #[error("unknown transform {0}")]
    UnknownTransform(String),
}

/// A file-wise transaction arrow, describing a single pair of
/// the source and destination files.
pub struct Arrow {
    pub src: PathBuf,
    pub dst: PathBuf,
}
