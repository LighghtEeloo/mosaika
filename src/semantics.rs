use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Replace,
    LogBlock,
    LogAnchor,
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            | Mode::Replace => write!(f, "replace"),
            | Mode::LogBlock => write!(f, "log.block"),
            | Mode::LogAnchor => write!(f, "log.anchor"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Transform {
    pub mode: Mode,
    pub matcher: Matcher,
    pub replace: Option<Vec<Replacer>>,
}

#[derive(Debug, Clone)]
pub enum Matcher {
    Pair { open: Delimiter, close: Delimiter },
    Single { anchor: Delimiter },
}

#[derive(Debug, Clone)]
pub enum Delimiter {
    /// e.g. `/*end*/`
    String(String),
    /// e.g. `/\*todo:(([^*]|\*[^/])*)\*/`
    Regex(regex::Regex),
}

#[derive(Debug, Clone)]
pub enum Replacer {
    Plain(String),
    Insertor(usize),
}

#[derive(Debug)]
pub struct Transaction {
    pub overwrites: Vec<PathBuf>,
    pub arrows: Vec<Arrow>,
    pub log: Option<PathBuf>,
    pub transform: Vec<String>,
}

#[derive(Error, Debug)]
pub enum TransformError {
    #[error(
        "transform {name} in mode {mode} expects {expected} delimiters, got {actual}"
    )]
    InvalidDelimiterCount {
        name: String,
        mode: Mode,
        expected: usize,
        actual: usize,
    },
    #[error("transform {name} contains an invalid regex `{regex}`")]
    InvalidRegex {
        name: String,
        regex: String,
        #[source]
        source: regex::Error,
    },
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

/// A file-wise transaction arrow, describing a single source file and optional
/// destination file.
#[derive(Debug)]
pub struct Arrow {
    pub src: PathBuf,
    pub dst: Option<PathBuf>,
}
