//! Surface syntax for `mosaika` schemes.
//!
//! These types preserve the user-written scheme shape across TOML and JSON
//! inputs. They do not resolve filesystem paths or compile matchers. That work
//! happens during semantic lowering.

#[cfg(feature = "json-schema")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Display,
    path::{Path, PathBuf},
};
use thiserror::Error;

/// Parsed projection scheme as it appears in the surface syntax.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
pub struct Projection {
    /// Declared transforms.
    #[serde(rename = "transform")]
    pub transforms: Vec<Transform>,
    /// Declared transactions.
    #[serde(rename = "transaction")]
    pub transactions: Vec<Transaction>,
    /// Declared post commands.
    #[serde(rename = "post")]
    pub posts: Vec<PostCommand>,
}

impl Projection {
    /// Returns an empty scheme with no transforms, transactions, or post
    /// commands.
    pub fn empty() -> Self {
        Self {
            transforms: Vec::new(),
            transactions: Vec::new(),
            posts: Vec::new(),
        }
    }

    /// Parses a TOML scheme from an in-memory source string.
    pub fn from_toml_str(
        source_name: impl Into<String>, source: &str,
    ) -> Result<Self, LoadError> {
        let source_name = source_name.into();
        toml::from_str(source)
            .map_err(|source| LoadError::ParseToml { source_name, source })
    }

    /// Parses a JSON scheme from an in-memory source string.
    pub fn from_json_str(
        source_name: impl Into<String>, source: &str,
    ) -> Result<Self, LoadError> {
        let source_name = source_name.into();
        serde_json::from_str(source)
            .map_err(|source| LoadError::ParseJson { source_name, source })
    }

    /// Reads and parses a scheme file from disk.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, LoadError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path).map_err(|source| {
            LoadError::Read { path: path.to_path_buf(), source }
        })?;
        Self::from_toml_str(path.display().to_string(), &contents)
    }
}

impl Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "transforms:")?;
        for transform in &self.transforms {
            writeln!(f, "  {transform}")?;
        }
        writeln!(f, "transactions:")?;
        for transaction in &self.transactions {
            writeln!(f, "  {transaction}")?;
        }
        writeln!(f, "posts:")?;
        for post in &self.posts {
            writeln!(f, "  {post}")?;
        }
        Ok(())
    }
}

/// Errors raised while loading one scheme source.
#[derive(Debug, Error)]
pub enum LoadError {
    /// The scheme file could not be read.
    #[error("failed to read scheme file {path}")]
    Read {
        /// Path to the scheme file.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// One TOML scheme source failed to parse.
    #[error("failed to parse TOML scheme source {source_name}")]
    ParseToml {
        /// Human-readable scheme source label.
        source_name: String,
        /// Underlying TOML parse error.
        #[source]
        source: toml::de::Error,
    },
    /// One JSON scheme source failed to parse.
    #[error("failed to parse JSON scheme source {source_name}")]
    ParseJson {
        /// Human-readable scheme source label.
        source_name: String,
        /// Underlying JSON parse error.
        #[source]
        source: serde_json::Error,
    },
}

/// One named transform in surface syntax.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Transform {
    /// User-facing transform identifier.
    pub name: String,
    /// Delimiters in source order.
    pub delimiters: Vec<Delimiter>,
    /// Action applied when the delimiter sequence matches.
    pub action: Action,
}

impl Display for Transform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "`{}` {} [{}]",
            self.name,
            self.action,
            self.delimiters
                .iter()
                .map(Delimiter::to_string)
                .collect::<Vec<_>>()
                .join(", "),
        )
    }
}

/// One delimiter as written in the scheme file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
pub enum Delimiter {
    /// A literal delimiter matched by exact text.
    String(String),
    /// A regular-expression delimiter matched by the Rust regex engine.
    Regex(RegexDelimiter),
}

impl Display for Delimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            | Delimiter::String(value) => write!(f, "\"{value}\""),
            | Delimiter::Regex(regex) => write!(f, "re\"{}\"", regex.regex),
        }
    }
}

/// Regular-expression delimiter payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RegexDelimiter {
    /// Regular-expression source text.
    pub regex: String,
}

/// Transform action in surface syntax.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(untagged)]
pub enum Action {
    /// Replaces a matched region with a template string.
    Replace {
        /// Replacement template.
        replace: String,
    },
    /// Logs a matched region or anchor.
    Log {
        /// Presence marker for the log action.
        ///
        /// Note: The design uses delimiter count to distinguish region and
        /// anchor logging. The boolean keeps the TOML shape concise:
        /// `action = { log = true }`.
        log: bool,
    },
}

impl Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            | Action::Replace { replace } => {
                write!(f, "replace -> \"{replace}\"")
            }
            | Action::Log { .. } => write!(f, "log"),
        }
    }
}

/// One transaction as it appears in the scheme file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Transaction {
    /// Path-wise source and output description.
    #[serde(flatten)]
    pub arrow: Arrow,
    /// Transform names applied to every selected work item.
    pub transform: Vec<String>,
}

impl Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} :: [{}]", self.arrow, self.transform.join(", "))
    }
}

/// Path-wise transaction inputs and outputs before filesystem resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Arrow {
    /// Source file or source directory.
    pub src: PathBuf,
    /// Optional destination file or destination directory.
    pub dst: Option<PathBuf>,
    /// Optional transaction log sink.
    pub log: Option<LogDestination>,
    /// Optional glob patterns for directory transactions.
    pub pattern: Option<Vec<String>>,
}

impl Display for Arrow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.src.display())?;
        if let Some(dst) = &self.dst {
            write!(f, " -> {}", dst.display())?;
        }
        if let Some(log) = &self.log {
            write!(f, " [log: {log}]")?;
        }
        if let Some(patterns) = &self.pattern {
            write!(f, " @ {}", patterns.join(", "))?;
        }
        Ok(())
    }
}

/// Transaction log sink in surface syntax.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(untagged)]
pub enum LogDestination {
    /// Write log records to a file.
    File(PathBuf),
    /// Write log records to a named pipe target.
    Pipe(LogPipe),
}

impl Display for LogDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            | LogDestination::File(path) => write!(f, "{}", path.display()),
            | LogDestination::Pipe(pipe) => write!(f, "{pipe}"),
        }
    }
}

/// Named log pipe target.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct LogPipe {
    /// Selected pipe target.
    pub pipe: PipeName,
}

impl Display for LogPipe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.pipe)
    }
}

/// Supported log pipe targets.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
pub enum PipeName {
    /// Standard output stream.
    #[serde(rename = "stdout")]
    Stdout,
}

impl Display for PipeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            | PipeName::Stdout => write!(f, "stdout"),
        }
    }
}

/// Scheme-level post command.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PostCommand {
    /// Working directory for the command.
    pub dir: PathBuf,
    /// Command string executed by the shell.
    pub cmd: String,
}

impl Display for PostCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} :: `{}`", self.dir.display(), self.cmd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() {
        let config = Projection::from_file("examples/proj/mosaika.toml").unwrap();
        assert!(!config.transforms.is_empty());
    }

    #[test]
    fn parses_stdout_log_sink() {
        let config = toml::from_str::<Projection>(
            r#"
            [[transform]]
            name = "anchors"
            delimiters = ["/*anchor*/"]
            action = { log = true }

            [[transaction]]
            src = "src"
            log = { pipe = "stdout" }
            pattern = ["**/*"]
            transform = ["anchors"]

            [[post]]
            dir = "."
            cmd = "true"
            "#,
        )
        .unwrap();

        match &config.transactions[0].arrow.log {
            | Some(LogDestination::Pipe(LogPipe {
                pipe: PipeName::Stdout,
            })) => {}
            | other => panic!("unexpected log sink: {other:?}"),
        }
    }

    #[test]
    fn parses_inline_json_scheme() {
        let config = Projection::from_json_str(
            "<json>",
            r#"{
                "transform": [],
                "transaction": [],
                "post": []
            }"#,
        )
        .unwrap();

        assert!(config.transforms.is_empty());
        assert!(config.transactions.is_empty());
        assert!(config.posts.is_empty());
    }

    #[test]
    fn constructs_empty_scheme() {
        let config = Projection::empty();

        assert!(config.transforms.is_empty());
        assert!(config.transactions.is_empty());
        assert!(config.posts.is_empty());
    }
}
