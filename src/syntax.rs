#[cfg(feature = "json-schema")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, path::PathBuf};

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
pub struct Proj {
    #[serde(rename = "transform")]
    pub transforms: Vec<Transform>,
    #[serde(rename = "transaction")]
    pub transactions: Vec<Transaction>,
    #[serde(rename = "post")]
    pub commands: Vec<Command>,
}

impl Proj {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Transform {
    pub name: String,
    pub delimiters: Vec<Delimiter>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
pub enum Delimiter {
    String(String),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RegexDelimiter {
    pub regex: String,
}

impl Display for RegexDelimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.regex)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(untagged)]
pub enum Action {
    Replace {
        replace: String,
    },
    Log {
        log: LogMode,
    },
}

impl Action {
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
pub enum LogMode {
    #[serde(rename = "block")]
    Block,
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

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Transaction {
    #[serde(flatten)]
    pub arrow: Arrow,
    pub transform: Vec<String>,
}

impl Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} :: [{}]", self.arrow, self.transform.join(", "))
    }
}

/// A path-wise transaction arrow, describing the source path and the optional
/// output artifacts that a transaction can materialize.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Arrow {
    pub src: PathBuf,
    pub dst: Option<PathBuf>,
    pub log: Option<PathBuf>,
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

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub enum Command {
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

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SystemCommand {
    pub dir: PathBuf,
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
