//! Normalized runtime model for `mosaika`.
//!
//! These types are the boundary between TOML parsing and execution. They
//! resolve relative paths against the scheme directory, compile regular
//! expressions, compile glob patterns, and parse replacement templates.

use crate::syntax as syn;
use glob::{MatchOptions, Pattern};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use thiserror::Error;

/// Semantically validated scheme ready for planning.
pub struct Scheme {
    transforms: Vec<Transform>,
    transform_ids: BTreeMap<String, TransformId>,
    transactions: Vec<Transaction>,
    posts: Vec<PostCommand>,
}

impl std::fmt::Debug for Scheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheme")
            .field("transforms", &self.transforms)
            .field("transactions", &self.transactions)
            .field("posts", &self.posts)
            .finish()
    }
}

impl Scheme {
    /// Lowers surface syntax into the runtime scheme model.
    pub fn from_syntax(proj: syn::Projection, scheme_dir: &Path) -> Result<Self, SchemeError> {
        let mut transforms = Vec::with_capacity(proj.transforms.len());
        let mut transform_ids = BTreeMap::new();

        for transform in proj.transforms {
            if transform_ids.contains_key(&transform.name) {
                return Err(SchemeError::DuplicateTransformName { name: transform.name });
            }

            let id = TransformId(transforms.len());
            transform_ids.insert(transform.name.clone(), id);
            transforms.push(Transform::from_syntax(id, transform)?);
        }

        let transactions = proj
            .transactions
            .into_iter()
            .enumerate()
            .map(|(index, transaction)| {
                Transaction::from_syntax(index + 1, transaction, scheme_dir, &transform_ids)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let posts =
            proj.posts.into_iter().map(|post| PostCommand::from_syntax(post, scheme_dir)).collect();
        let scheme = Self { transforms, transform_ids, transactions, posts };
        for transaction in &scheme.transactions {
            transaction.validate(&scheme)?;
        }
        Ok(scheme)
    }

    /// Returns every declared transform in declaration order.
    pub fn transforms(&self) -> &[Transform] {
        &self.transforms
    }

    /// Returns every declared transaction in declaration order.
    pub fn transactions(&self) -> &[Transaction] {
        &self.transactions
    }

    /// Returns every declared post command in declaration order.
    pub fn posts(&self) -> &[PostCommand] {
        &self.posts
    }

    /// Returns one transform by its resolved identifier.
    pub fn transform(&self, id: TransformId) -> &Transform {
        self.transforms.get(id.0).unwrap_or_else(|| panic!("invalid transform id {}", id.0))
    }

    /// Returns one transform id by its declared name.
    pub fn transform_id(&self, name: &str) -> Option<TransformId> {
        self.transform_ids.get(name).copied()
    }
}

/// Stable transform identifier within one validated scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransformId(usize);

impl TransformId {
    /// Constructs one raw transform id.
    ///
    /// Note: Library callers usually obtain transform ids from
    /// [`Scheme::transform_id`]. This constructor exists so tests and synthetic
    /// schemes can build standalone transforms without a full scheme.
    pub fn new(index: usize) -> Self {
        Self(index)
    }

    /// Returns the zero-based transform slot within the owning scheme.
    pub fn index(self) -> usize {
        self.0
    }
}

/// One runtime transform.
#[derive(Debug)]
pub struct Transform {
    /// Stable transform identifier within the owning scheme.
    pub id: TransformId,
    /// User-facing transform name for diagnostics.
    pub name: String,
    /// Shared matcher applied before any effect-specific work.
    pub matcher: Matcher,
    /// Effects applied to every matched chain.
    pub effects: Vec<Effect>,
}

impl Transform {
    fn from_syntax(id: TransformId, transform: syn::Transform) -> Result<Self, SchemeError> {
        let matcher =
            Matcher::from_syntax(&transform.name, transform.matching, transform.delimiters)?;
        let effects = transform
            .effects
            .into_iter()
            .map(|effect| Effect::from_syntax(&transform.name, effect))
            .collect::<Result<Vec<_>, _>>()?;

        if effects.is_empty() {
            return Err(SchemeError::EmptyEffectList { name: transform.name });
        }

        Ok(Self { id, name: transform.name, matcher, effects })
    }
}

/// Delimiter matcher reused by every effect on the transform.
#[derive(Debug)]
pub enum Matcher {
    /// Ordered delimiter sequence matching.
    Sequence(Vec<Delimiter>),
    /// Stack-based nested matching with delimiter index `0` as open and
    /// delimiter index `1` as close.
    Balanced {
        /// Opening delimiter.
        open: Delimiter,
        /// Closing delimiter.
        close: Delimiter,
    },
}

impl Matcher {
    fn from_syntax(
        transform_name: &str, matching: syn::Matching, delimiters: Vec<syn::Delimiter>,
    ) -> Result<Self, SchemeError> {
        match matching {
            | syn::Matching::Sequence => {
                if delimiters.is_empty() {
                    return Err(SchemeError::EmptyDelimiterSequence {
                        name: transform_name.to_string(),
                    });
                }

                let delimiters = Self::compile_delimiters(transform_name, delimiters)?;
                Ok(Self::Sequence(delimiters))
            }
            | syn::Matching::Balanced => {
                let delimiter_count = delimiters.len();
                if delimiter_count != 2 {
                    return Err(SchemeError::BalancedDelimiterCount {
                        name: transform_name.to_string(),
                        delimiter_count,
                    });
                }

                let mut delimiters = Self::compile_delimiters(transform_name, delimiters)?;
                let close = delimiters.pop().expect("balanced matcher has a closing delimiter");
                let open = delimiters.pop().expect("balanced matcher has an opening delimiter");
                Ok(Self::Balanced { open, close })
            }
        }
    }

    fn compile_delimiters(
        transform_name: &str, delimiters: Vec<syn::Delimiter>,
    ) -> Result<Vec<Delimiter>, SchemeError> {
        delimiters
            .into_iter()
            .enumerate()
            .map(|(delimiter_index, delimiter)| {
                Delimiter::from_syntax(transform_name, delimiter_index, delimiter)
            })
            .collect()
    }

    /// Returns delimiter recognizers in matcher index order.
    pub fn delimiters(&self) -> Vec<&Delimiter> {
        match self {
            | Self::Sequence(delimiters) => delimiters.iter().collect(),
            | Self::Balanced { open, close } => vec![open, close],
        }
    }

    /// Returns a stable name for diagnostics and trace events.
    pub fn kind_name(&self) -> &'static str {
        match self {
            | Self::Sequence(_) => "sequence",
            | Self::Balanced { .. } => "balanced",
        }
    }
}

/// One effect applied to a matched chain.
#[derive(Debug)]
pub enum Effect {
    /// Replace the matched region with a rendered template.
    Replace {
        /// Parsed replacement template.
        template: Template,
    },
    /// Emit a log record for the matched region.
    Log,
}

impl Effect {
    fn from_syntax(transform_name: &str, effect: syn::Effect) -> Result<Self, SchemeError> {
        match effect {
            | syn::Effect::Replace { replace } => {
                Ok(Self::Replace { template: Template::parse(transform_name, &replace)? })
            }
            | syn::Effect::Log { log } => {
                if !log {
                    return Err(SchemeError::DisabledLogEffect {
                        name: transform_name.to_string(),
                    });
                }
                Ok(Self::Log)
            }
        }
    }
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
                let compiled = regex::Regex::new(&regex.regex).map_err(|source| {
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

/// One transaction after path resolution, selector compilation, and transform
/// binding.
#[derive(Debug)]
pub struct Transaction {
    /// 1-based transaction index in scheme order.
    pub index: usize,
    /// Resolved source shape.
    pub source: TransactionSource,
    /// Resolved output targets.
    pub outputs: TransactionOutputs,
    /// Resolved transform ids applied to every selected work item.
    pub transform_ids: Vec<TransformId>,
}

impl Transaction {
    fn from_syntax(
        index: usize, transaction: syn::Transaction, scheme_dir: &Path,
        transform_ids: &BTreeMap<String, TransformId>,
    ) -> Result<Self, SchemeError> {
        let syn::Transaction { arrow, transform } = transaction;
        let syn::Arrow { src, dst, log, pattern } = arrow;

        let transform_ids = transform
            .into_iter()
            .map(|name| {
                transform_ids
                    .get(&name)
                    .copied()
                    .ok_or(SchemeError::UnknownTransform { transaction: index, name })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let source = match pattern {
            | Some(patterns) => TransactionSource::Directory {
                root: resolve_path(scheme_dir, src),
                selection: FileSelection::from_patterns(index, patterns)?,
            },
            | None => TransactionSource::File { path: resolve_path(scheme_dir, src) },
        };
        let outputs = TransactionOutputs::from_syntax(dst, log, scheme_dir, &source);

        Ok(Self { index, source, outputs, transform_ids })
    }

    fn validate(&self, scheme: &Scheme) -> Result<(), SchemeError> {
        if !self.outputs.has_materialized_target() {
            return Err(SchemeError::TransactionHasNoOutputs { transaction: self.index });
        }

        if self.outputs.log.is_some() {
            return Ok(());
        }

        let Some(transform) =
            self.transform_ids.iter().map(|id| scheme.transform(*id)).find(|transform| {
                transform.effects.iter().any(|effect| matches!(effect, Effect::Log))
            })
        else {
            return Ok(());
        };

        Err(SchemeError::TransactionLogSinkRequired {
            transaction: self.index,
            transform: transform.name.clone(),
        })
    }
}

/// Resolved source shape for one transaction.
#[derive(Debug)]
pub enum TransactionSource {
    /// One source file.
    File {
        /// Source file resolved against the scheme directory.
        path: PathBuf,
    },
    /// One source directory expanded by a compiled selector.
    Directory {
        /// Source directory resolved against the scheme directory.
        root: PathBuf,
        /// Compiled file selector applied to paths relative to `root`.
        selection: FileSelection,
    },
}

/// Compiled file selector for one directory transaction.
#[derive(Debug, Clone)]
pub struct FileSelection {
    patterns: Vec<FilePattern>,
}

impl FileSelection {
    fn from_patterns(index: usize, patterns: Vec<String>) -> Result<Self, SchemeError> {
        if patterns.is_empty() {
            return Err(SchemeError::EmptyPatternList { transaction: index });
        }

        let patterns = patterns
            .into_iter()
            .map(|source| {
                let pattern_source = source.clone();
                FilePattern::from_source(source).map_err(|source| SchemeError::InvalidPattern {
                    transaction: index,
                    pattern: pattern_source,
                    source,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { patterns })
    }

    /// Returns whether the selector includes the given path relative to the
    /// transaction source root.
    pub fn matches(&self, relative_path: &Path) -> bool {
        self.patterns.iter().any(|pattern| pattern.matches(relative_path))
    }
}

/// One compiled glob pattern within a [`FileSelection`].
#[derive(Debug, Clone)]
struct FilePattern {
    pattern: Pattern,
}

impl FilePattern {
    fn from_source(source: String) -> Result<Self, glob::PatternError> {
        let pattern = Pattern::new(&source)?;
        Ok(Self { pattern })
    }

    fn matches(&self, relative_path: &Path) -> bool {
        self.pattern.matches_path_with(relative_path, file_match_options())
    }
}

/// Resolved output targets for one transaction.
#[derive(Debug, Clone)]
pub struct TransactionOutputs {
    /// Optional destination tree.
    pub destination: Option<DestinationRoot>,
    /// Optional log sink.
    pub log: Option<LogDestination>,
}

impl TransactionOutputs {
    fn from_syntax(
        dst: Option<PathBuf>, log: Option<syn::LogDestination>, scheme_dir: &Path,
        source: &TransactionSource,
    ) -> Self {
        let destination = dst.map(|path| match source {
            | TransactionSource::File { .. } => {
                DestinationRoot::File(resolve_path(scheme_dir, path))
            }
            | TransactionSource::Directory { .. } => {
                DestinationRoot::Directory(resolve_path(scheme_dir, path))
            }
        });

        Self { destination, log: log.map(|log| LogDestination::from_syntax(log, scheme_dir)) }
    }

    /// Returns whether the transaction materializes any filesystem or stdout
    /// output.
    pub fn has_materialized_target(&self) -> bool {
        self.destination.is_some() || self.log.is_some()
    }
}

/// Destination root resolved against the scheme directory.
#[derive(Debug, Clone)]
pub enum DestinationRoot {
    /// One destination file.
    File(PathBuf),
    /// One destination directory that preserves relative file paths.
    Directory(PathBuf),
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
            | syn::LogDestination::File(path) => Self::File(resolve_path(scheme_dir, path)),
            | syn::LogDestination::Pipe(syn::LogPipe { pipe: syn::PipeName::Stdout }) => {
                Self::Stdout
            }
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
                            parts.push(TemplatePart::Plain(std::mem::take(&mut buffer)));
                        }
                        state = State::OpenBrace;
                    }
                    | '}' => {
                        if !buffer.is_empty() {
                            parts.push(TemplatePart::Plain(std::mem::take(&mut buffer)));
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
                            problem: format!("expected a digit or `}}` in a capture, got `{ch}`"),
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
                            problem: format!("expected `}}` after `}}`, got `{ch}`"),
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
            | State::OpenBrace => Err(SchemeError::InvalidReplacementTemplate {
                name: transform_name.to_string(),
                template: source.to_string(),
                problem: "unterminated `{`".to_string(),
            }),
            | State::Capture => Err(SchemeError::InvalidReplacementTemplate {
                name: transform_name.to_string(),
                template: source.to_string(),
                problem: "unterminated capture".to_string(),
            }),
            | State::CloseBrace => Err(SchemeError::InvalidReplacementTemplate {
                name: transform_name.to_string(),
                template: source.to_string(),
                problem: "unterminated `}` escape".to_string(),
            }),
        }
    }

    /// Renders the template with the provided flattened capture list.
    pub fn render(&self, captures: &[String]) -> Result<String, TemplateRenderError> {
        let mut rendered = String::new();
        for part in &self.parts {
            match part {
                | TemplatePart::Plain(text) => rendered.push_str(text),
                | TemplatePart::Capture(index) => {
                    let capture =
                        captures.get(*index).ok_or(TemplateRenderError::MissingCapture {
                            capture_index: *index,
                            capture_count: captures.len(),
                        })?;
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
    /// One balanced matcher does not have exactly two delimiters.
    #[error(
        "transform `{name}` with balanced matching must declare exactly two delimiters, got {delimiter_count}"
    )]
    BalancedDelimiterCount {
        /// Transform name.
        name: String,
        /// Number of declared delimiters.
        delimiter_count: usize,
    },
    /// One transform omits all effects.
    #[error("transform `{name}` must declare at least one effect")]
    EmptyEffectList {
        /// Transform name.
        name: String,
    },
    /// One literal delimiter is empty.
    #[error("transform `{name}` delimiter {delimiter_index} must not match empty text")]
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
    #[error("transform `{name}` has an invalid replacement template `{template}`: {problem}")]
    InvalidReplacementTemplate {
        /// Transform name.
        name: String,
        /// Template source text.
        template: String,
        /// Human-readable problem description.
        problem: String,
    },
    /// One transform disables the log effect explicitly.
    #[error("transform `{name}` must use `effects = [{{ log = true }}]` for log effects")]
    DisabledLogEffect {
        /// Transform name.
        name: String,
    },
    /// One transaction references a transform name that is not declared.
    #[error("transaction {transaction} references unknown transform `{name}`")]
    UnknownTransform {
        /// 1-based transaction index.
        transaction: usize,
        /// Unknown transform name.
        name: String,
    },
    /// One declared directory transaction omits all patterns.
    #[error("transaction {transaction} must declare at least one pattern")]
    EmptyPatternList {
        /// 1-based transaction index.
        transaction: usize,
    },
    /// One transaction declares neither a destination nor a log sink.
    #[error("transaction {transaction} must declare at least one output target")]
    TransactionHasNoOutputs {
        /// 1-based transaction index.
        transaction: usize,
    },
    /// One transaction uses a log effect without declaring a log sink.
    #[error(
        "transaction {transaction} must declare a log sink because transform `{transform}` logs matches"
    )]
    TransactionLogSinkRequired {
        /// 1-based transaction index.
        transaction: usize,
        /// First transform in declaration order that requires the log sink.
        transform: String,
    },
    /// One transaction contains an invalid glob pattern.
    #[error("transaction {transaction} contains an invalid pattern `{pattern}`")]
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

fn file_match_options() -> MatchOptions {
    MatchOptions {
        case_sensitive: true,
        require_literal_separator: false,
        require_literal_leading_dot: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_transactions_without_outputs() {
        let projection = syn::Projection::from_toml_str(
            "<test>",
            r#"
            [[transform]]
            name = "anchor"
            delimiters = ["hello"]
            effects = [{ replace = "hello" }]

            [[transaction]]
            src = "src/lib.rs"
            transform = ["anchor"]
            "#,
        )
        .unwrap();

        let err = Scheme::from_syntax(projection, Path::new("/tmp")).unwrap_err();
        match err {
            | SchemeError::TransactionHasNoOutputs { transaction } => assert_eq!(transaction, 1),
            | other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_log_effects_without_log_sink() {
        let projection = syn::Projection::from_toml_str(
            "<test>",
            r#"
            [[transform]]
            name = "anchor"
            delimiters = ["hello"]
            effects = [{ log = true }]

            [[transaction]]
            src = "src/lib.rs"
            dst = "dst/lib.rs"
            transform = ["anchor"]
            "#,
        )
        .unwrap();

        let err = Scheme::from_syntax(projection, Path::new("/tmp")).unwrap_err();
        match err {
            | SchemeError::TransactionLogSinkRequired { transaction, transform } => {
                assert_eq!(transaction, 1);
                assert_eq!(transform, "anchor");
            }
            | other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn balanced_matchers_require_exactly_two_delimiters() {
        let projection = syn::Projection::from_toml_str(
            "<test>",
            r#"
            [[transform]]
            name = "balanced"
            matching = "balanced"
            delimiters = ["open"]
            effects = [{ log = true }]
            "#,
        )
        .unwrap();

        let err = Scheme::from_syntax(projection, Path::new("/tmp")).unwrap_err();
        match err {
            | SchemeError::BalancedDelimiterCount { name, delimiter_count } => {
                assert_eq!(name, "balanced");
                assert_eq!(delimiter_count, 1);
            }
            | other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn directory_selector_matches_relative_paths() {
        let projection = syn::Projection::from_toml_str(
            "<test>",
            r#"
            [[transform]]
            name = "rewrite"
            delimiters = ["hello"]
            effects = [{ replace = "hello" }]

            [[transaction]]
            src = "src"
            dst = "dst"
            pattern = ["**/*.rs"]
            transform = ["rewrite"]
            "#,
        )
        .unwrap();

        let scheme = Scheme::from_syntax(projection, Path::new("/tmp")).unwrap();
        let transaction = &scheme.transactions()[0];
        let selection = match &transaction.source {
            | TransactionSource::Directory { selection, .. } => selection,
            | other => panic!("unexpected transaction source: {other:?}"),
        };

        assert!(selection.matches(Path::new("nested/lib.rs")));
        assert!(!selection.matches(Path::new("notes.txt")));
    }
}
