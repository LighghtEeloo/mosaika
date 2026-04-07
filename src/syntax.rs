//! TOML-facing syntax for `mosaika` scheme files.
//!
//! These types preserve the surface structure of the configuration file. They
//! intentionally stay close to the serialized representation so that parsing and
//! syntax-level validation happen before semantic lowering.

#[cfg(feature = "json-schema")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, path::PathBuf};

/// Parsed projection scheme as it appears in the TOML file.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
pub struct Proj {
    /// Declared transforms keyed by `Transform::name` after semantic lowering.
    #[serde(rename = "transform")]
    pub transforms: Vec<Transform>,
    /// Declared transactions in scheme order.
    #[serde(rename = "transaction")]
    pub transactions: Vec<Transaction>,
    /// Post commands that run after transaction execution in the current
    /// implementation.
    #[serde(rename = "post")]
    pub commands: Vec<Command>,
}

impl Proj {
    /// Reads and parses a projection scheme from disk.
    pub fn from_file<P: AsRef<std::path::Path>>(
        path: P,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        let config = toml::from_str(&contents)?;
        Ok(config)
    }
}

impl Display for Proj {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "transforms:")?;
        for t in &self.transforms {
            writeln!(f, "  {}", t)?;
        }
        writeln!(f, "transactions:")?;
        for t in &self.transactions {
            writeln!(f, "  {}", t)?;
        }
        writeln!(f, "posts:")?;
        for c in &self.commands {
            writeln!(f, "  {}", c)?;
        }
        Ok(())
    }
}

/// One named transform in the surface syntax.
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
            "`{}` [{}] delimited by [{}]",
            self.name,
            self.action.mode(),
            self.delimiters
                .iter()
                .map(|d| d.to_string())
                .collect::<Vec<String>>()
                .join(", "),
        )?;
        write!(f, " -> {}", self.action)?;
        Ok(())
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
            | Delimiter::String(s) => write!(f, "\"{}\"", s),
            | Delimiter::Regex(r) => write!(f, "re\"{}\"", r),
        }
    }
}

/// Regular-expression delimiter payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RegexDelimiter {
    /// Regular expression source text.
    pub regex: String,
}

impl Display for RegexDelimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.regex)
    }
}

/// Action syntax attached to a transform.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(untagged)]
pub enum Action {
    /// Replaces a matched region with a rendered template.
    Replace {
        /// Replacement template in the current placeholder syntax.
        replace: String,
    },
    /// Records a matched region or anchor to the transaction log sink.
    Log {
        /// Logging mode in the current surface syntax.
        log: LogMode,
    },
}

impl Action {
    /// Returns the stable display name of the action mode.
    pub fn mode(&self) -> &'static str {
        match self {
            | Action::Replace { .. } => "replace",
            | Action::Log { log: LogMode::Block } => "log.block",
            | Action::Log { log: LogMode::Anchor } => "log.anchor",
        }
    }
}

impl Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            | Action::Replace { replace } => {
                write!(f, "replace: \"{}\"", replace)
            }
            | Action::Log { log } => write!(f, "log: \"{}\"", log),
        }
    }
}

/// Logging modes supported by the current surface syntax.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
pub enum LogMode {
    /// Logs a region bounded by two delimiters.
    #[serde(rename = "block")]
    Block,
    /// Logs a single delimiter occurrence.
    #[serde(rename = "anchor")]
    Anchor,
}

impl Display for LogMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            | LogMode::Block => write!(f, "block"),
            | LogMode::Anchor => write!(f, "anchor"),
        }
    }
}

/// One transaction as it appears in the scheme file.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Transaction {
    /// Path-wise source and output description.
    #[serde(flatten)]
    pub arrow: Arrow,
    /// Transform names applied in transaction order.
    pub transform: Vec<String>,
}

impl Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} :: [{}]", self.arrow, self.transform.join(", "))
    }
}

/// Path-wise transaction inputs and outputs before file expansion.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Arrow {
    /// Source file or source directory.
    pub src: PathBuf,
    /// Optional destination file or destination directory.
    pub dst: Option<PathBuf>,
    /// Optional transaction-scoped log file.
    ///
    /// Note: The current syntax models `log` only as a file path. The design
    /// document describes a future stdout log sink as part of the target
    /// pipeline.
    pub log: Option<PathBuf>,
    /// Optional glob patterns used when expanding directory transactions.
    pub pattern: Option<Vec<String>>,
}

impl Display for Arrow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.src.display())?;
        if let Some(dst) = &self.dst {
            write!(f, " -> {}", dst.display())?;
        }
        if let Some(log) = &self.log {
            write!(f, " [log: {}]", log.display())?;
        }
        if let Some(pattern) = &self.pattern {
            write!(f, " @ {}", pattern.join(", "))?;
        }
        Ok(())
    }
}

/// Post-execution command in the current implementation.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub enum Command {
    /// Shell command executed by the host system.
    #[serde(rename = "system")]
    System(SystemCommand),
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            | Command::System(c) => write!(f, "{}", c),
        }
    }
}

/// One shell command executed after all transactions complete.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SystemCommand {
    /// Working directory for the command.
    pub dir: PathBuf,
    /// Shell command string.
    pub cmd: String,
}

impl Display for SystemCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} :: `{}`", self.dir.display(), self.cmd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() {
        let config = Proj::from_file("examples/proj/mosaika.toml").unwrap();
        println!("{:#?}", config);
    }
}
