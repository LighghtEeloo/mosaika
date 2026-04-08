//! Normalized runtime model for `mosaika`.
//!
//! These types are the boundary between TOML parsing and execution. They
//! resolve relative paths against the scheme directory, compile regular
//! expressions, compile glob patterns, and parse replacement templates.

use crate::syntax as syn;
use glob::Pattern;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use thiserror::Error;

/// Semantically validated scheme ready for planning.
#[derive(Debug)]
pub struct Scheme {
    /// Transforms keyed by name.
    pub transforms: BTreeMap<String, Transform>,
    /// Transactions in scheme order.
    pub transactions: Vec<Transaction>,
    /// Post commands in scheme order.
    pub posts: Vec<PostCommand>,
}

impl Scheme {
    /// Lowers TOML-facing syntax into the runtime scheme model.
    pub fn from_syntax(
        proj: syn::Projection, scheme_dir: &Path,
    ) -> Result<Self, SchemeError> {
        let mut transforms = BTreeMap::new();
        for transform in proj.transforms {
            if transforms.contains_key(&transform.name) {
                return Err(SchemeError::DuplicateTransformName {
                    name: transform.name,
                });
            }
            let name = transform.name.clone();
            let lowered = Transform::from_syntax(transform)?;
            transforms.insert(name, lowered);
        }

        let transactions = proj
            .transactions
            .into_iter()
            .enumerate()
            .map(|(index, transaction)| {
                Transaction::from_syntax(index + 1, transaction, scheme_dir)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let posts = proj
            .posts
            .into_iter()
            .map(|post| PostCommand::from_syntax(post, scheme_dir))
            .collect();

        Ok(Self { transforms, transactions, posts })
    }
}

/// One runtime transform.
#[derive(Debug)]
pub struct Transform {
    /// Ordered delimiter sequence.
    pub delimiters: Vec<Delimiter>,
    /// Action applied to every matched chain.
    pub action: Action,
}

impl Transform {
    fn from_syntax(transform: syn::Transform) -> Result<Self, SchemeError> {
        if transform.delimiters.is_empty() {
            return Err(SchemeError::EmptyDelimiterSequence {
                name: transform.name,
            });
        }

        let delimiters = transform
            .delimiters
            .into_iter()
            .enumerate()
            .map(|(delimiter_index, delimiter)| {
                Delimiter::from_syntax(
                    &transform.name,
                    delimiter_index,
                    delimiter,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let action = match transform.action {
            | syn::Action::Replace { replace } => Action::Replace {
                template: Template::parse(&transform.name, &replace)?,
            },
            | syn::Action::Log { log } => {
                if !log {
                    return Err(SchemeError::DisabledLogAction {
                        name: transform.name,
                    });
                }
                Action::Log
            }
        };

        Ok(Self { delimiters, action })
    }
}

/// Action applied to a matched chain.
#[derive(Debug)]
pub enum Action {
    /// Replace the matched region with a rendered template.
    Replace {
        /// Parsed replacement template.
        template: Template,
    },
    /// Emit a log record for the matched region.
    Log,
}

/// One compiled delimiter matcher.
#[derive(Debug)]
pub enum Delimiter {
    /// Literal string matcher.
    String(String),
    /// Regular-expression matcher.
    Regex {
        /// Original regex source text.
        source: String,
        /// Compiled regex.
        regex: regex::Regex,
    },
}

impl Delimiter {
    fn from_syntax(
        transform_name: &str, delimiter_index: usize, delimiter: syn::Delimiter,
    ) -> Result<Self, SchemeError> {
        match delimiter {
            | syn::Delimiter::String(value) => {
                if value.is_empty() {
                    return Err(SchemeError::EmptyDelimiter {
                        name: transform_name.to_string(),
                        delimiter_index,
                    });
                }
                Ok(Self::String(value))
            }
            | syn::Delimiter::Regex(regex) => {
                let compiled =
                    regex::Regex::new(&regex.regex).map_err(|source| {
                        SchemeError::InvalidRegex {
                            name: transform_name.to_string(),
                            regex: regex.regex.clone(),
                            source,
                        }
                    })?;
                if compiled.is_match("") {
                    return Err(SchemeError::EmptyRegexMatch {
                        name: transform_name.to_string(),
                        delimiter_index,
                        regex: regex.regex,
                    });
                }
                Ok(Self::Regex { source: regex.regex, regex: compiled })
            }
        }
    }
}

/// One transaction after path resolution and pattern compilation.
#[derive(Debug)]
pub struct Transaction {
    /// 1-based transaction index in scheme order.
    pub index: usize,
    /// Source path resolved against the scheme directory.
    pub src: PathBuf,
    /// Destination path resolved against the scheme directory.
    pub dst: Option<PathBuf>,
    /// Log sink resolved against the scheme directory.
    pub log: Option<LogDestination>,
    /// Compiled directory patterns.
    pub patterns: Option<Vec<Pattern>>,
    /// Transform names applied to this transaction.
    pub transform_names: Vec<String>,
}

impl Transaction {
    fn from_syntax(
        index: usize, transaction: syn::Transaction, scheme_dir: &Path,
    ) -> Result<Self, SchemeError> {
        let syn::Transaction { arrow, transform } = transaction;
        let syn::Arrow { src, dst, log, pattern } = arrow;

        let patterns = pattern
            .map(|patterns| {
                patterns
                    .into_iter()
                    .map(|pattern| {
                        Pattern::new(&pattern).map_err(|source| {
                            SchemeError::InvalidPattern {
                                transaction: index,
                                pattern,
                                source,
                            }
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;

        Ok(Self {
            index,
            src: resolve_path(scheme_dir, src),
            dst: dst.map(|path| resolve_path(scheme_dir, path)),
            log: log.map(|log| LogDestination::from_syntax(log, scheme_dir)),
            patterns,
            transform_names: transform,
        })
    }
}

/// Transaction log sink.
#[derive(Debug, Clone)]
pub enum LogDestination {
    /// Write log records to a file.
    File(PathBuf),
    /// Write log records to standard output.
    Stdout,
}

impl LogDestination {
    fn from_syntax(log: syn::LogDestination, scheme_dir: &Path) -> Self {
        match log {
            | syn::LogDestination::File(path) => {
                Self::File(resolve_path(scheme_dir, path))
            }
            | syn::LogDestination::Pipe(syn::LogPipe {
                pipe: syn::PipeName::Stdout,
            }) => Self::Stdout,
        }
    }
}

/// Scheme-level post command.
#[derive(Debug, Clone)]
pub struct PostCommand {
    /// Working directory resolved against the scheme directory.
    pub dir: PathBuf,
    /// Shell command string.
    pub cmd: String,
}

impl PostCommand {
    fn from_syntax(post: syn::PostCommand, scheme_dir: &Path) -> Self {
        Self { dir: resolve_path(scheme_dir, post.dir), cmd: post.cmd }
    }
}

/// Parsed replacement template.
#[derive(Debug)]
pub struct Template {
    parts: Vec<TemplatePart>,
}

impl Template {
    fn parse(transform_name: &str, source: &str) -> Result<Self, SchemeError> {
        enum State {
            Plain,
            OpenBrace,
            Capture,
            CloseBrace,
        }

        let mut parts = Vec::new();
        let mut state = State::Plain;
        let mut buffer = String::new();

        for ch in source.chars() {
            match state {
                | State::Plain => match ch {
                    | '{' => {
                        if !buffer.is_empty() {
                            parts.push(TemplatePart::Plain(std::mem::take(
                                &mut buffer,
                            )));
                        }
                        state = State::OpenBrace;
                    }
                    | '}' => {
                        if !buffer.is_empty() {
                            parts.push(TemplatePart::Plain(std::mem::take(
                                &mut buffer,
                            )));
                        }
                        state = State::CloseBrace;
                    }
                    | _ => buffer.push(ch),
                },
                | State::OpenBrace => match ch {
                    | '{' => {
                        parts.push(TemplatePart::Plain("{".to_string()));
                        state = State::Plain;
                    }
                    | '0'..='9' => {
                        buffer.push(ch);
                        state = State::Capture;
                    }
                    | _ => {
                        return Err(SchemeError::InvalidReplacementTemplate {
                            name: transform_name.to_string(),
                            template: source.to_string(),
                            problem: format!(
                                "expected `{{` or a capture index after `{{`, got `{ch}`"
                            ),
                        });
                    }
                },
                | State::Capture => match ch {
                    | '0'..='9' => buffer.push(ch),
                    | '}' => {
                        let index = buffer.parse::<usize>().map_err(|_| {
                            SchemeError::InvalidReplacementTemplate {
                                name: transform_name.to_string(),
                                template: source.to_string(),
                                problem: "invalid capture index".to_string(),
                            }
                        })?;
                        buffer.clear();
                        parts.push(TemplatePart::Capture(index));
                        state = State::Plain;
                    }
                    | _ => {
                        return Err(SchemeError::InvalidReplacementTemplate {
                            name: transform_name.to_string(),
                            template: source.to_string(),
                            problem: format!(
                                "expected a digit or `}}` in a capture, got `{ch}`"
                            ),
                        });
                    }
                },
                | State::CloseBrace => match ch {
                    | '}' => {
                        parts.push(TemplatePart::Plain("}".to_string()));
                        state = State::Plain;
                    }
                    | _ => {
                        return Err(SchemeError::InvalidReplacementTemplate {
                            name: transform_name.to_string(),
                            template: source.to_string(),
                            problem: format!(
                                "expected `}}` after `}}`, got `{ch}`"
                            ),
                        });
                    }
                },
            }
        }

        match state {
            | State::Plain => {
                if !buffer.is_empty() {
                    parts.push(TemplatePart::Plain(buffer));
                }
                Ok(Self { parts })
            }
            | State::OpenBrace => {
                Err(SchemeError::InvalidReplacementTemplate {
                    name: transform_name.to_string(),
                    template: source.to_string(),
                    problem: "unterminated `{`".to_string(),
                })
            }
            | State::Capture => Err(SchemeError::InvalidReplacementTemplate {
                name: transform_name.to_string(),
                template: source.to_string(),
                problem: "unterminated capture".to_string(),
            }),
            | State::CloseBrace => {
                Err(SchemeError::InvalidReplacementTemplate {
                    name: transform_name.to_string(),
                    template: source.to_string(),
                    problem: "unterminated `}` escape".to_string(),
                })
            }
        }
    }

    /// Renders the template with the provided flattened capture list.
    pub fn render(
        &self, captures: &[String],
    ) -> Result<String, TemplateRenderError> {
        let mut rendered = String::new();
        for part in &self.parts {
            match part {
                | TemplatePart::Plain(text) => rendered.push_str(text),
                | TemplatePart::Capture(index) => {
                    let capture = captures.get(*index).ok_or(
                        TemplateRenderError::MissingCapture {
                            capture_index: *index,
                            capture_count: captures.len(),
                        },
                    )?;
                    rendered.push_str(capture);
                }
            }
        }
        Ok(rendered)
    }
}

/// One replacement-template fragment.
#[derive(Debug)]
enum TemplatePart {
    Plain(String),
    Capture(usize),
}

/// Errors raised while rendering a replacement template.
#[derive(Debug, Error)]
pub enum TemplateRenderError {
    /// The template references a capture index that is not available.
    #[error(
        "capture {capture_index} is not available; only {capture_count} captures were produced"
    )]
    MissingCapture {
        /// Referenced capture index.
        capture_index: usize,
        /// Number of available captures.
        capture_count: usize,
    },
}

/// Errors raised while lowering syntax into the runtime model.
#[derive(Debug, Error)]
pub enum SchemeError {
    /// Two transforms share the same name.
    #[error("transform `{name}` is declared more than once")]
    DuplicateTransformName {
        /// Duplicate transform name.
        name: String,
    },
    /// One transform omits all delimiters.
    #[error("transform `{name}` must declare at least one delimiter")]
    EmptyDelimiterSequence {
        /// Transform name.
        name: String,
    },
    /// One literal delimiter is empty.
    #[error(
        "transform `{name}` delimiter {delimiter_index} must not match empty text"
    )]
    EmptyDelimiter {
        /// Transform name.
        name: String,
        /// Zero-based delimiter index.
        delimiter_index: usize,
    },
    /// One regex delimiter fails to compile.
    #[error("transform `{name}` contains an invalid regex `{regex}`")]
    InvalidRegex {
        /// Transform name.
        name: String,
        /// Regex source text.
        regex: String,
        /// Regex compilation error.
        #[source]
        source: regex::Error,
    },
    /// One regex delimiter can match empty text.
    #[error(
        "transform `{name}` delimiter {delimiter_index} regex `{regex}` must not match empty text"
    )]
    EmptyRegexMatch {
        /// Transform name.
        name: String,
        /// Zero-based delimiter index.
        delimiter_index: usize,
        /// Regex source text.
        regex: String,
    },
    /// One replacement template is malformed.
    #[error(
        "transform `{name}` has an invalid replacement template `{template}`: {problem}"
    )]
    InvalidReplacementTemplate {
        /// Transform name.
        name: String,
        /// Template source text.
        template: String,
        /// Human-readable problem description.
        problem: String,
    },
    /// One transform disables the log action explicitly.
    #[error("transform `{name}` must use `action = {{ log = true }}`")]
    DisabledLogAction {
        /// Transform name.
        name: String,
    },
    /// One transaction contains an invalid glob pattern.
    #[error(
        "transaction {transaction} contains an invalid pattern `{pattern}`"
    )]
    InvalidPattern {
        /// 1-based transaction index.
        transaction: usize,
        /// Pattern source text.
        pattern: String,
        /// Pattern compilation error.
        #[source]
        source: glob::PatternError,
    },
}

fn resolve_path(base: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() { path } else { base.join(path) }
}
