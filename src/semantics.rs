//! Normalized runtime model for the current `mosaika` executable.
//!
//! These types sit between TOML parsing and execution. They replace
//! syntax-oriented representations such as raw regex strings with compiled and
//! validated runtime values.

use std::path::PathBuf;
use thiserror::Error;

/// Execution mode derived from a transform action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Replace a matched region.
    Replace,
    /// Log a two-delimiter region.
    LogBlock,
    /// Log a single delimiter occurrence.
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

/// Runtime transform with compiled delimiters and parsed replacement parts.
#[derive(Debug, Clone)]
pub struct Transform {
    /// Action mode applied to each match.
    pub mode: Mode,
    /// Delimiter matcher normalized by action shape.
    pub matcher: Matcher,
    /// Parsed replacement template for `Mode::Replace`.
    pub replace: Option<Vec<Replacer>>,
}

/// Runtime matcher form chosen from the delimiter count.
#[derive(Debug, Clone)]
pub enum Matcher {
    /// Paired delimiters used by `replace` and `log.block`.
    Pair { open: Delimiter, close: Delimiter },
    /// Single delimiter used by `log.anchor`.
    Single { anchor: Delimiter },
}

/// Compiled runtime delimiter.
#[derive(Debug, Clone)]
pub enum Delimiter {
    /// e.g. `/*end*/`
    String(String),
    /// e.g. `/\*todo:(([^*]|\*[^/])*)\*/`
    Regex(regex::Regex),
}

/// Parsed replacement-template fragment.
#[derive(Debug, Clone)]
pub enum Replacer {
    /// Verbatim text copied into the replacement output.
    Plain(String),
    /// Capture-group insertion by zero-based index.
    Insertor(usize),
}

/// Planned transaction after source expansion and overwrite discovery.
#[derive(Debug)]
pub struct Transaction {
    /// Existing outputs that require overwrite confirmation.
    pub overwrites: Vec<PathBuf>,
    /// Concrete source-to-destination file mappings.
    pub arrows: Vec<Arrow>,
    /// Optional transaction-scoped log file.
    pub log: Option<PathBuf>,
    /// Transform names applied to each concrete source file.
    pub transform: Vec<String>,
}

/// Transform lowering errors.
#[derive(Error, Debug)]
pub enum TransformError {
    /// The action mode and delimiter count do not agree.
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

/// Transaction-planning errors.
#[derive(Error, Debug)]
pub enum TransactionError {
    /// Source path is missing on disk.
    #[error("source path {0} does not exist")]
    MissingSource(PathBuf),
    /// Glob pattern did not parse.
    #[error("glob pattern is invalid: {0}")]
    GlobPattern(#[from] glob::PatternError),
    /// Glob expansion failed.
    #[error("glob is invalid: {0}")]
    Glob(#[from] glob::GlobError),
    /// Expanded source file escaped the declared source root.
    #[error("strip prefix is invalid: {0}")]
    StripPrefix(#[from] std::path::StripPrefixError),
    /// Transaction references an unknown transform name.
    #[error("unknown transform {0}")]
    UnknownTransform(String),
}

/// One concrete source file and optional destination file.
#[derive(Debug)]
pub struct Arrow {
    /// Source file path.
    pub src: PathBuf,
    /// Destination file path.
    pub dst: Option<PathBuf>,
}
