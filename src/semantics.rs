use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug)]
pub struct Transform {
    pub open: Delimiter,
    pub close: Delimiter,
    pub action: Action,
}

#[derive(Debug)]
pub enum Delimiter {
    /// e.g. '/*end*/'
    String(String),
    /// e.g. '/\*todo:(([^*]|\*[^/])*)\*/'
    Regex(regex::Regex),
}

#[derive(Debug)]
pub struct Action {
    /// e.g. 'todo!("$1")'
    pub replace: Vec<Replacer>,
}

#[derive(Debug)]
pub enum Replacer {
    Plain(String),
    Insertor(usize),
}

#[derive(Debug)]
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
#[derive(Debug)]
pub struct Arrow {
    pub src: PathBuf,
    pub dst: PathBuf,
}
