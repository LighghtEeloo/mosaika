use serde::{Deserialize, Serialize};
use std::{fmt::Display, path::PathBuf};

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
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
            "`{}` delimited by [{}] -> {}",
            self.name,
            self.delimiters
                .iter()
                .map(|d| d.to_string())
                .collect::<Vec<String>>()
                .join(", "),
            self.action,
        )?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegexDelimiter {
    pub regex: String,
}

impl Display for RegexDelimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.regex)?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Action {
    pub replace: String,
}

impl Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "replace: \"{}\"", self.replace)?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transaction {
    #[serde(flatten)]
    pub arrow: Arrow,
    pub transform: Vec<String>,
}

impl Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} :: [{}]", self.arrow, self.transform.join(", "))?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Arrow {
    pub src: PathBuf,
    pub dst: PathBuf,
    pub pattern: Option<Vec<String>>,
}

impl Display for Arrow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> {}{}",
            self.src.display(),
            self.dst.display(),
            self.pattern.as_ref().map_or_else(
                || String::new(),
                |p| format!(" @ {}", p.join(", ")),
            )
        )?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
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
#[serde(deny_unknown_fields)]
pub struct SystemCommand {
    pub dir: PathBuf,
    pub cmd: String,
}

impl Display for SystemCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} :: `{}`", self.dir.display(), self.cmd)?;
        Ok(())
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
