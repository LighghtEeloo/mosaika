//! Library execution engine for `mosaika`.
//!
//! The engine owns one semantically validated scheme, exposes an explicit
//! planning step for overwrite inspection, and executes the analysis-first
//! pipeline after the caller chooses an overwrite policy.

use crate::semantics as sem;
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    io::Write,
    path::{Path, PathBuf},
    process::ExitStatus,
};
use thiserror::Error;
use tracing::{trace, warn};

/// Executes the projection pipeline for one semantically validated scheme.
///
/// The engine owns a fully lowered [`sem::Scheme`]. Loading surface syntax from
/// TOML or JSON belongs to [`crate::syntax`] and [`crate::semantics`].
#[derive(Debug)]
pub struct Engine {
    scheme_source: String,
    scheme: sem::Scheme,
}

impl Engine {
    /// Constructs an engine for one validated scheme.
    pub fn new(scheme_source: impl Into<String>, scheme: sem::Scheme) -> Self {
        Self { scheme_source: scheme_source.into(), scheme }
    }

    /// Returns the human-readable scheme source label used in diagnostics.
    pub fn scheme_source(&self) -> &str {
        &self.scheme_source
    }

    /// Returns the validated scheme owned by the engine.
    pub fn scheme(&self) -> &sem::Scheme {
        &self.scheme
    }

    /// Resolves transactions into a plan that can be inspected before writes.
    pub fn plan(self) -> Result<RunPlan, EngineError> {
        trace!(
            scheme_source = %self.scheme_source,
            transaction_count = self.scheme.transactions().len(),
            "planning engine run"
        );
        let operation_plan = plan_scheme(&self.scheme).map_err(|source| EngineError::Planning {
            scheme_source: self.scheme_source.clone(),
            source: Box::new(source),
        })?;
        trace!(
            scheme_source = %self.scheme_source,
            overwrite_count = operation_plan.approved_overwrites.len(),
            "finished planning engine run"
        );
        Ok(RunPlan { scheme_source: self.scheme_source, scheme: self.scheme, operation_plan })
    }

    /// Plans and executes the scheme using the process standard output stream.
    pub fn run(self, overwrite_mode: OverwriteMode) -> Result<RunReport, EngineError> {
        let mut stdout = std::io::stdout();
        self.run_with_stdout(overwrite_mode, &mut stdout)
    }

    /// Plans and executes the scheme while routing stdout log sinks to the
    /// provided writer.
    pub fn run_with_stdout<W: Write>(
        self, overwrite_mode: OverwriteMode, stdout: &mut W,
    ) -> Result<RunReport, EngineError> {
        self.plan()?.execute_with_stdout(overwrite_mode, stdout)
    }
}

/// Overwrite behavior chosen by the caller for pre-existing claimed outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverwriteMode {
    /// Reject execution when any claimed output already exists.
    RejectExisting,
    /// Delete claimed pre-existing outputs before materialization.
    ///
    /// Note: The current engine policy deletes files directly. The design keeps
    /// trash-based deletion as a future choice.
    DeleteExisting,
}

/// Stage-2 plan for one engine run.
///
/// A plan owns the validated scheme and the resolved file operations. Callers
/// may inspect [`RunPlan::overwrite_paths`] before executing the remaining
/// stages.
#[derive(Debug)]
pub struct RunPlan {
    scheme_source: String,
    scheme: sem::Scheme,
    operation_plan: OperationPlan,
}

impl RunPlan {
    /// Returns the human-readable scheme source label used in diagnostics.
    pub fn scheme_source(&self) -> &str {
        &self.scheme_source
    }

    /// Returns the claimed output files that already exist on disk.
    pub fn overwrite_paths(&self) -> &BTreeSet<PathBuf> {
        &self.operation_plan.approved_overwrites
    }

    /// Executes the remaining pipeline stages using the process standard
    /// output stream.
    pub fn execute(self, overwrite_mode: OverwriteMode) -> Result<RunReport, EngineError> {
        let mut stdout = std::io::stdout();
        self.execute_with_stdout(overwrite_mode, &mut stdout)
    }

    /// Executes the remaining pipeline stages while routing stdout log sinks to
    /// the provided writer.
    pub fn execute_with_stdout<W: Write>(
        self, overwrite_mode: OverwriteMode, stdout: &mut W,
    ) -> Result<RunReport, EngineError> {
        let Self { scheme_source, scheme, operation_plan } = self;

        if overwrite_mode == OverwriteMode::RejectExisting
            && !operation_plan.approved_overwrites.is_empty()
        {
            return Err(EngineError::OverwriteRequired {
                scheme_source,
                paths: operation_plan.approved_overwrites,
            });
        }

        trace!(
            scheme_source = %scheme_source,
            overwrite_mode = ?overwrite_mode,
            "executing engine run plan"
        );
        let operation_plan = analyze_operation_plan(&scheme, operation_plan).map_err(|source| {
            EngineError::Analysis { scheme_source: scheme_source.clone(), source }
        })?;
        let report = materialize_run(&operation_plan, stdout).map_err(|source| {
            EngineError::Materialization {
                scheme_source: scheme_source.clone(),
                source: Box::new(source),
            }
        })?;
        run_post_commands(scheme.posts()).map_err(|source| EngineError::Post {
            scheme_source: scheme_source.clone(),
            source: Box::new(source),
        })?;
        trace!(scheme_source = %scheme_source, "finished engine run plan");
        Ok(report)
    }
}

/// Materialization summary for one completed engine run.
#[derive(Debug, Default)]
pub struct RunReport {
    overwritten_paths: BTreeSet<PathBuf>,
    file_outputs: Vec<PathBuf>,
    log_outputs: Vec<LogOutputTarget>,
}

impl RunReport {
    /// Returns the pre-existing outputs that were deleted before writing.
    pub fn overwritten_paths(&self) -> &BTreeSet<PathBuf> {
        &self.overwritten_paths
    }

    /// Returns the destination files written by replace transactions.
    pub fn file_outputs(&self) -> &[PathBuf] {
        &self.file_outputs
    }

    /// Returns the log sinks materialized by the run.
    pub fn log_outputs(&self) -> &[LogOutputTarget] {
        &self.log_outputs
    }
}

/// One materialized log sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogOutputTarget {
    /// A log file written on disk.
    File(PathBuf),
    /// Bytes written to the caller-provided stdout writer.
    Stdout,
}

fn plan_scheme(scheme: &sem::Scheme) -> Result<OperationPlan, PlanningError> {
    trace!(transaction_count = scheme.transactions().len(), "planning transactions");

    let mut transactions = Vec::new();
    let mut output_claims = OutputClaims::default();

    for transaction in scheme.transactions() {
        if transaction_has_no_outputs(transaction) {
            warn!(
                transaction = transaction.index,
                src = %transaction_source_path(transaction).display(),
                "transaction has neither dst nor log; it will run as analysis-only"
            );
        }

        let operations = match &transaction.kind {
            | sem::TransactionKind::File { src, dst } => {
                plan_file_transaction(transaction.index, src, dst.as_ref(), &mut output_claims)?
            }
            | sem::TransactionKind::Directory { src_root, dst_root, patterns } => {
                plan_directory_transaction(
                    transaction.index,
                    src_root,
                    dst_root.as_ref(),
                    patterns,
                    &mut output_claims,
                )?
            }
        };

        if let Some(log) = &transaction.log {
            output_claims.claim_log_sink(transaction.index, log)?;
        }

        transactions.push(TransactionPlan {
            index: transaction.index,
            log_sink: transaction.log.clone(),
            transform_ids: transaction.transform_ids.clone(),
            operations,
        });
    }

    trace!(
        transaction_count = transactions.len(),
        claimed_output_count = output_claims.claimed_outputs.len(),
        overwrite_count = output_claims.approved_overwrites.len(),
        "finished planning transactions"
    );

    Ok(OperationPlan {
        transactions,
        approved_overwrites: output_claims.approved_overwrites,
        claimed_outputs: output_claims.claimed_outputs,
    })
}

fn plan_file_transaction(
    transaction: usize, src: &Path, dst: Option<&PathBuf>, output_claims: &mut OutputClaims,
) -> Result<Vec<FileOperation>, PlanningError> {
    if !src.exists() {
        return Err(PlanningError::MissingSource { transaction, path: src.to_path_buf() });
    }
    if !src.is_file() {
        return Err(PlanningError::SourceKindMismatch {
            transaction,
            path: src.to_path_buf(),
            expected: PathKind::File,
        });
    }

    let dst = if let Some(dst) = dst {
        output_claims.claim_file_target(transaction, src, dst, PathKind::File)?;
        Some(dst.clone())
    } else {
        None
    };

    Ok(vec![FileOperation::planned(src.to_path_buf(), dst)])
}

fn plan_directory_transaction(
    transaction: usize, src_root: &Path, dst_root: Option<&PathBuf>, patterns: &[glob::Pattern],
    output_claims: &mut OutputClaims,
) -> Result<Vec<FileOperation>, PlanningError> {
    if !src_root.exists() {
        return Err(PlanningError::MissingSource { transaction, path: src_root.to_path_buf() });
    }
    if !src_root.is_dir() {
        return Err(PlanningError::SourceKindMismatch {
            transaction,
            path: src_root.to_path_buf(),
            expected: PathKind::Directory,
        });
    }

    if let Some(dst_root) = dst_root
        && dst_root.exists()
        && !dst_root.is_dir()
    {
        return Err(PlanningError::DestinationKindMismatch {
            transaction,
            src: src_root.to_path_buf(),
            dst: dst_root.clone(),
            expected: PathKind::Directory,
        });
    }

    let mut operations = Vec::new();
    for src in walk_files(transaction, src_root)? {
        let relative = src.strip_prefix(src_root).map_err(|source| PlanningError::StripPrefix {
            transaction,
            root: src_root.to_path_buf(),
            path: src.clone(),
            source,
        })?;
        if !patterns.iter().any(|pattern| pattern.matches_path(relative)) {
            continue;
        }

        let dst = if let Some(dst_root) = dst_root {
            let dst = dst_root.join(relative);
            output_claims.claim_file_target(transaction, &src, &dst, PathKind::File)?;
            Some(dst)
        } else {
            None
        };

        operations.push(FileOperation::planned(src, dst));
    }

    Ok(operations)
}

fn analyze_operation_plan(
    scheme: &sem::Scheme, mut operation_plan: OperationPlan,
) -> AnalysisResult<OperationPlan> {
    trace!(transaction_count = operation_plan.transactions.len(), "analyzing file operations");

    for transaction in &mut operation_plan.transactions {
        let active_transforms =
            transaction.transform_ids.iter().map(|id| scheme.transform(*id)).collect::<Vec<_>>();

        if transaction.log_sink.is_none()
            && active_transforms.iter().any(|transform| {
                transform.effects.iter().any(|effect| matches!(effect, sem::Effect::Log))
            })
        {
            warn!(
                transaction = transaction.index,
                "transaction has log effects but no log sink; findings will be discarded"
            );
        }

        for operation in &mut transaction.operations {
            let content = std::fs::read_to_string(&operation.src).map_err(|source| {
                Box::new(AnalysisError::ReadSource {
                    transaction: transaction.index,
                    path: operation.src.clone(),
                    source,
                })
            })?;
            let analyzer = FileAnalyzer::new(transaction.index, &operation.src, &content);
            let analysis = analyzer.analyze(&active_transforms)?;

            if operation.dst.is_some() {
                operation.rendered_content = Some(analysis.rendered_content);
            }

            if transaction.log_sink.is_some() {
                operation.log_records = analysis.log_records;
            }
        }
    }

    trace!(transaction_count = operation_plan.transactions.len(), "finished analysis");
    Ok(operation_plan)
}

fn materialize_run<W: Write>(
    operation_plan: &OperationPlan, stdout: &mut W,
) -> Result<RunReport, MaterializationError> {
    trace!(transaction_count = operation_plan.transactions.len(), "materializing outputs");

    let mut report = RunReport {
        overwritten_paths: operation_plan.approved_overwrites.clone(),
        ..RunReport::default()
    };

    for path in &operation_plan.approved_overwrites {
        if path.exists() {
            std::fs::remove_file(path).map_err(|source| MaterializationError::DeleteOutput {
                path: path.clone(),
                source,
            })?;
        }
    }

    for path in &operation_plan.claimed_outputs {
        if path.exists() {
            return Err(MaterializationError::OccupiedOutput { path: path.clone() });
        }
    }

    for transaction in &operation_plan.transactions {
        for operation in &transaction.operations {
            if let Some(dst) = &operation.dst {
                let rendered_content = operation.rendered_content.as_ref().expect(
                    "analysis populates rendered content before materializing destination files",
                );
                write_output_file(dst, rendered_content)?;
                report.file_outputs.push(dst.clone());
            }
        }

        if let Some(log_sink) = &transaction.log_sink {
            let log_content = render_log_records(
                transaction.operations.iter().flat_map(|operation| operation.log_records.iter()),
            );

            match log_sink {
                | sem::LogDestination::File(path) => {
                    write_output_file(path, &log_content)?;
                    report.log_outputs.push(LogOutputTarget::File(path.clone()));
                }
                | sem::LogDestination::Stdout => {
                    stdout
                        .write_all(log_content.as_bytes())
                        .map_err(|source| MaterializationError::WriteStdout { source })?;
                    stdout
                        .flush()
                        .map_err(|source| MaterializationError::WriteStdout { source })?;
                    report.log_outputs.push(LogOutputTarget::Stdout);
                }
            }
        }
    }

    trace!("finished materialization");
    Ok(report)
}

/// Runs scheme-level post commands in declaration order.
///
/// Note: The engine starts each shell in [`sem::PostCommand::dir`]. Tools
/// invoked by that shell may still apply their own directory discovery after
/// startup. For example, `cargo fmt` searches parent directories for
/// `Cargo.toml`, so it can target an ancestor even when the shell started in
/// the declared working directory.
fn run_post_commands(posts: &[sem::PostCommand]) -> Result<(), PostError> {
    trace!(post_count = posts.len(), "running post commands");
    for post in posts {
        trace!(dir = %post.dir.display(), cmd = %post.cmd, "running post command");
        let status = std::process::Command::new("sh")
            .arg("-lc")
            .arg(&post.cmd)
            .current_dir(&post.dir)
            .status()
            .map_err(|source| PostError::Spawn {
                dir: post.dir.clone(),
                cmd: post.cmd.clone(),
                source,
            })?;
        if !status.success() {
            return Err(PostError::Failed { dir: post.dir.clone(), cmd: post.cmd.clone(), status });
        }
        trace!(dir = %post.dir.display(), cmd = %post.cmd, "finished post command");
    }
    trace!("finished post commands");
    Ok(())
}

#[derive(Debug)]
struct OperationPlan {
    transactions: Vec<TransactionPlan>,
    approved_overwrites: BTreeSet<PathBuf>,
    claimed_outputs: BTreeSet<PathBuf>,
}

/// One transaction inside a planned run.
///
/// Note: Planning resolves source paths and output claims, then analysis fills
/// the per-file rendered content and log records in place.
#[derive(Debug)]
struct TransactionPlan {
    index: usize,
    log_sink: Option<sem::LogDestination>,
    transform_ids: Vec<sem::TransformId>,
    operations: Vec<FileOperation>,
}

/// One concrete file operation selected by a transaction.
#[derive(Debug)]
struct FileOperation {
    src: PathBuf,
    dst: Option<PathBuf>,
    rendered_content: Option<String>,
    log_records: Vec<LogRecord>,
}

impl FileOperation {
    fn planned(src: PathBuf, dst: Option<PathBuf>) -> Self {
        Self { src, dst, rendered_content: None, log_records: Vec::new() }
    }
}

/// One filesystem path kind expected by the planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathKind {
    /// A regular file path.
    File,
    /// A directory path.
    Directory,
}

impl PathKind {
    fn matches(self, path: &Path) -> bool {
        match self {
            | Self::File => path.is_file(),
            | Self::Directory => path.is_dir(),
        }
    }
}

impl std::fmt::Display for PathKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            | Self::File => write!(f, "file"),
            | Self::Directory => write!(f, "directory"),
        }
    }
}

#[derive(Debug, Default)]
struct OutputClaims {
    claims: BTreeMap<PathBuf, usize>,
    claimed_outputs: BTreeSet<PathBuf>,
    approved_overwrites: BTreeSet<PathBuf>,
}

impl OutputClaims {
    fn claim_file_target(
        &mut self, transaction: usize, src: &Path, target: &Path, expected: PathKind,
    ) -> Result<(), PlanningError> {
        if target.exists() && !expected.matches(target) {
            return Err(PlanningError::DestinationKindMismatch {
                transaction,
                src: src.to_path_buf(),
                dst: target.to_path_buf(),
                expected,
            });
        }

        self.claim_output_path(transaction, target)?;
        if target.exists() {
            self.approved_overwrites.insert(target.to_path_buf());
        }
        Ok(())
    }

    fn claim_log_sink(
        &mut self, transaction: usize, sink: &sem::LogDestination,
    ) -> Result<(), PlanningError> {
        let sem::LogDestination::File(path) = sink else {
            return Ok(());
        };

        if path.exists() && !path.is_file() {
            return Err(PlanningError::LogTargetMustBeFile { transaction, path: path.clone() });
        }

        self.claim_output_path(transaction, path)?;
        if path.exists() {
            self.approved_overwrites.insert(path.clone());
        }
        Ok(())
    }

    fn claim_output_path(&mut self, transaction: usize, path: &Path) -> Result<(), PlanningError> {
        if let Some(first_transaction) = self.claims.get(path) {
            return Err(PlanningError::ClaimConflict {
                path: path.to_path_buf(),
                first_transaction: *first_transaction,
                second_transaction: transaction,
            });
        }
        self.claims.insert(path.to_path_buf(), transaction);
        self.claimed_outputs.insert(path.to_path_buf());
        Ok(())
    }
}

type AnalysisResult<T> = Result<T, Box<AnalysisError>>;

#[derive(Debug)]
struct FileAnalyzer<'a> {
    /// 1-based transaction index that owns this work item.
    transaction: usize,
    path: &'a Path,
    content: &'a str,
    locator: Locator<'a>,
}

impl<'a> FileAnalyzer<'a> {
    fn new(transaction: usize, path: &'a Path, content: &'a str) -> Self {
        Self { transaction, path, content, locator: Locator::new(content) }
    }

    fn analyze(&self, transforms: &[&sem::Transform]) -> AnalysisResult<FileAnalysis> {
        let token_lists = self.collect_tokens(transforms)?;
        let mut replacements = Vec::new();
        let mut log_records = Vec::new();

        for transform in transforms {
            let candidates = self.build_candidate_chains(
                &transform.name,
                token_lists
                    .get(&transform.id)
                    .expect("tokens collected for every active transform"),
            )?;

            for effect in &transform.effects {
                match effect {
                    | sem::Effect::Replace { template } => {
                        for candidate in &candidates {
                            let replacement =
                                template.render(&candidate.captures()).map_err(|source| {
                                    Box::new(AnalysisError::TemplateRender {
                                        transaction: self.transaction,
                                        path: self.path.to_path_buf(),
                                        transform: transform.name.clone(),
                                        source,
                                    })
                                })?;
                            replacements.push(Replacement {
                                transform: transform.name.clone(),
                                start: candidate.start(),
                                end: candidate.end(),
                                text: replacement,
                            });
                        }
                    }
                    | sem::Effect::Log => {
                        for candidate in &candidates {
                            log_records.push(LogRecord::from_candidate(
                                &transform.name,
                                self.path,
                                &self.locator,
                                self.content,
                                candidate,
                            ));
                        }
                    }
                }
            }
        }

        self.validate_replace_overlaps(&replacements)?;
        Ok(FileAnalysis {
            rendered_content: apply_replacements(self.content, &replacements),
            log_records,
        })
    }

    fn collect_tokens(
        &self, transforms: &[&sem::Transform],
    ) -> AnalysisResult<BTreeMap<sem::TransformId, Vec<Vec<TokenOccurrence>>>> {
        let mut shared_streams = BTreeMap::<DelimiterKey, DelimiterTokenStream>::new();
        let mut token_lists = BTreeMap::new();
        let mut next_id = 0usize;

        for transform in transforms {
            for delimiter in &transform.matcher.delimiters {
                let cache_key = DelimiterKey::from(delimiter);
                match shared_streams.entry(cache_key) {
                    | Entry::Occupied(_) => {}
                    | Entry::Vacant(entry) => {
                        entry.insert(scan_delimiter_tokens(self.content, delimiter, &mut next_id));
                    }
                }
            }
        }

        self.validate_token_overlaps(&shared_streams)?;

        for transform in transforms {
            let delimiters = transform
                .matcher
                .delimiters
                .iter()
                .enumerate()
                .map(|(delimiter_index, delimiter)| {
                    let cache_key = DelimiterKey::from(delimiter);
                    let stream = shared_streams
                        .get(&cache_key)
                        .expect("shared token stream exists for every active delimiter");
                    bind_tokens(&stream.tokens, delimiter_index)
                })
                .collect();
            token_lists.insert(transform.id, delimiters);
        }

        Ok(token_lists)
    }

    fn validate_token_overlaps(
        &self, shared_streams: &BTreeMap<DelimiterKey, DelimiterTokenStream>,
    ) -> AnalysisResult<()> {
        let mut all_tokens = shared_streams
            .values()
            .flat_map(|stream| {
                stream.tokens.iter().map(move |token| ScannedToken {
                    description: stream.description.clone(),
                    start: token.start,
                    end: token.end,
                })
            })
            .collect::<Vec<_>>();

        all_tokens.sort_by_key(|token| (token.start, token.end));
        for window in all_tokens.windows(2) {
            let [left, right] = window else { continue };
            if right.start < left.end {
                return Err(Box::new(AnalysisError::TokenOverlap {
                    transaction: self.transaction,
                    path: self.path.to_path_buf(),
                    left_delimiter: left.description.clone(),
                    left_span: self.locator.span(left.start, left.end),
                    right_delimiter: right.description.clone(),
                    right_span: self.locator.span(right.start, right.end),
                }));
            }
        }

        Ok(())
    }

    fn build_candidate_chains(
        &self, transform_name: &str, token_lists: &[Vec<TokenOccurrence>],
    ) -> AnalysisResult<Vec<MatchCandidate>> {
        // Stage 1 (`sem::Scheme::from_syntax`) rejects transforms with an empty
        // delimiter sequence, so `token_lists` is guaranteed non-empty here.
        let mut candidates = Vec::new();
        for start_token in &token_lists[0] {
            let mut tokens = vec![start_token.clone()];
            let mut previous_end = start_token.end;
            let mut complete = true;

            for delimiter_tokens in token_lists.iter().skip(1) {
                let Some(next) = delimiter_tokens.iter().find(|token| token.start >= previous_end)
                else {
                    complete = false;
                    break;
                };
                previous_end = next.end;
                tokens.push(next.clone());
            }

            if complete {
                candidates.push(MatchCandidate { tokens });
            }
        }

        for index in 0..candidates.len() {
            for other_index in index + 1..candidates.len() {
                let left = &candidates[index];
                let right = &candidates[other_index];
                let shares_token = left.tokens.iter().any(|left_token| {
                    right.tokens.iter().any(|right_token| left_token.id == right_token.id)
                });
                let overlaps = ranges_overlap(left.start(), left.end(), right.start(), right.end());
                if shares_token || overlaps {
                    return Err(Box::new(AnalysisError::AmbiguousTransform {
                        transaction: self.transaction,
                        path: self.path.to_path_buf(),
                        transform: transform_name.to_string(),
                        left_span: self.locator.span(left.start(), left.end()),
                        right_span: self.locator.span(right.start(), right.end()),
                    }));
                }
            }
        }

        Ok(candidates)
    }

    fn validate_replace_overlaps(&self, replacements: &[Replacement]) -> AnalysisResult<()> {
        let mut replacements = replacements.iter().collect::<Vec<_>>();
        replacements.sort_by_key(|replacement| (replacement.start, replacement.end));

        for window in replacements.windows(2) {
            let [left, right] = window else { continue };
            if ranges_overlap(left.start, left.end, right.start, right.end) {
                return Err(Box::new(AnalysisError::ReplaceOverlap {
                    transaction: self.transaction,
                    path: self.path.to_path_buf(),
                    left_transform: left.transform.clone(),
                    left_span: self.locator.span(left.start, left.end),
                    right_transform: right.transform.clone(),
                    right_span: self.locator.span(right.start, right.end),
                }));
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
struct FileAnalysis {
    rendered_content: String,
    log_records: Vec<LogRecord>,
}

/// Concrete delimiter token scanned from the source file.
///
/// Note: This token is shared by every transform position that references the
/// same delimiter recognizer.
#[derive(Debug, Clone)]
struct SharedToken {
    id: usize,
    start: usize,
    end: usize,
    matched: String,
    captures: Vec<String>,
}

/// Token stream produced by one distinct delimiter recognizer.
///
/// Note: The analyzer shares these streams across transforms so one matcher can
/// feed replacement and logging effects without duplicate scans.
#[derive(Debug, Clone)]
struct DelimiterTokenStream {
    description: String,
    tokens: Vec<SharedToken>,
}

/// Cache key for one distinct delimiter recognizer.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum DelimiterKey {
    String(String),
    Regex(String),
}

impl From<&sem::Delimiter> for DelimiterKey {
    fn from(value: &sem::Delimiter) -> Self {
        match value {
            | sem::Delimiter::String(text) => Self::String(text.clone()),
            | sem::Delimiter::Regex { source, .. } => Self::Regex(source.clone()),
        }
    }
}

#[derive(Debug, Clone)]
struct ScannedToken {
    description: String,
    start: usize,
    end: usize,
}

#[derive(Debug, Clone)]
struct TokenOccurrence {
    id: usize,
    delimiter_index: usize,
    start: usize,
    end: usize,
    matched: String,
    captures: Vec<String>,
}

#[derive(Debug)]
struct MatchCandidate {
    tokens: Vec<TokenOccurrence>,
}

impl MatchCandidate {
    fn start(&self) -> usize {
        self.tokens.first().expect("candidate chains always contain at least one token").start
    }

    fn end(&self) -> usize {
        self.tokens.last().expect("candidate chains always contain at least one token").end
    }

    fn captures(&self) -> Vec<String> {
        self.tokens.iter().flat_map(|token| token.captures.iter().cloned()).collect()
    }
}

#[derive(Debug)]
struct Replacement {
    transform: String,
    start: usize,
    end: usize,
    text: String,
}

#[derive(Debug)]
struct Locator<'a> {
    content: &'a str,
    line_breaks: Vec<usize>,
}

impl<'a> Locator<'a> {
    fn new(content: &'a str) -> Self {
        let line_breaks = content.match_indices('\n').map(|(index, _)| index).collect();
        Self { content, line_breaks }
    }

    fn position(&self, byte_index: usize) -> (usize, usize) {
        let line_index = self.line_breaks.partition_point(|index| *index < byte_index);
        let line_start = if line_index == 0 { 0 } else { self.line_breaks[line_index - 1] + 1 };
        let column = self.content[line_start..byte_index].chars().count() + 1;
        (line_index + 1, column)
    }

    fn span(&self, start: usize, end: usize) -> SourceSpan {
        let (start_line, start_column) = self.position(start);
        let (end_line, end_column) = self.position(end);
        SourceSpan {
            start_byte: start,
            end_byte: end,
            start_line,
            start_column,
            end_line,
            end_column,
        }
    }
}

/// One byte and line-column span in a source file.
#[derive(Debug, Clone, Serialize)]
pub struct SourceSpan {
    start_byte: usize,
    end_byte: usize,
    start_line: usize,
    start_column: usize,
    end_line: usize,
    end_column: usize,
}

impl std::fmt::Display for SourceSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}-{}:{}", self.start_line, self.start_column, self.end_line, self.end_column)
    }
}

#[derive(Debug, Serialize)]
struct LogRecord {
    transform: String,
    file: String,
    region: SourceSpan,
    delimiters: Vec<LogDelimiterRecord>,
    body: String,
}

impl LogRecord {
    fn from_candidate(
        transform: &str, path: &Path, locator: &Locator<'_>, content: &str,
        candidate: &MatchCandidate,
    ) -> Self {
        let region = locator.span(candidate.start(), candidate.end());
        let delimiters = candidate
            .tokens
            .iter()
            .map(|token| LogDelimiterRecord {
                delimiter_index: token.delimiter_index,
                span: locator.span(token.start, token.end),
                matched: token.matched.clone(),
                captures: token.captures.clone(),
            })
            .collect();

        Self {
            transform: transform.to_string(),
            file: path.display().to_string(),
            region,
            delimiters,
            body: content[candidate.start()..candidate.end()].to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
struct LogDelimiterRecord {
    delimiter_index: usize,
    span: SourceSpan,
    matched: String,
    captures: Vec<String>,
}

/// Errors raised by the library execution engine.
#[derive(Debug, Error)]
pub enum EngineError {
    /// Stage 2 failed while resolving transactions and output claims.
    #[error("scheme {scheme_source}: {source}")]
    Planning {
        /// Human-readable scheme source label.
        scheme_source: String,
        /// Underlying planning failure.
        #[source]
        source: Box<PlanningError>,
    },
    /// The caller must approve or delete pre-existing outputs before executing.
    #[error(
        "scheme {scheme_source}: overwrite approval is required for {} path(s)",
        paths.len()
    )]
    OverwriteRequired {
        /// Human-readable scheme source label.
        scheme_source: String,
        /// Claimed output files that already exist.
        paths: BTreeSet<PathBuf>,
    },
    /// Stage 3 failed while analyzing source files.
    #[error("scheme {scheme_source}: {source}")]
    Analysis {
        /// Human-readable scheme source label.
        scheme_source: String,
        /// Underlying analysis failure.
        #[source]
        source: Box<AnalysisError>,
    },
    /// Stage 4 failed while deleting or writing outputs.
    #[error("scheme {scheme_source}: {source}")]
    Materialization {
        /// Human-readable scheme source label.
        scheme_source: String,
        /// Underlying materialization failure.
        #[source]
        source: Box<MaterializationError>,
    },
    /// Stage 5 failed while running post commands.
    #[error("scheme {scheme_source}: {source}")]
    Post {
        /// Human-readable scheme source label.
        scheme_source: String,
        /// Underlying post-command failure.
        #[source]
        source: Box<PostError>,
    },
}

/// Errors raised while resolving transactions into concrete file operations.
#[derive(Debug, Error)]
pub enum PlanningError {
    /// One source path does not exist.
    #[error("transaction {transaction} source path {path} does not exist")]
    MissingSource { transaction: usize, path: PathBuf },
    /// One source path does not match the transaction's declared shape.
    #[error("transaction {transaction} requires source path {path} to be a {expected}")]
    SourceKindMismatch { transaction: usize, path: PathBuf, expected: PathKind },
    /// One destination path has the wrong filesystem kind for its source.
    #[error(
        "transaction {transaction} requires destination path {dst} to be a {expected} because source {src} determines that kind"
    )]
    DestinationKindMismatch { transaction: usize, src: PathBuf, dst: PathBuf, expected: PathKind },
    /// One file-backed log sink points at a non-file path.
    #[error("transaction {transaction} requires log path {path} to be a file")]
    LogTargetMustBeFile { transaction: usize, path: PathBuf },
    /// Two transactions claim the same output path.
    #[error(
        "output path {path} is claimed by both transaction {first_transaction} and transaction {second_transaction}"
    )]
    ClaimConflict { path: PathBuf, first_transaction: usize, second_transaction: usize },
    /// Reading a directory failed while expanding a declared directory
    /// transaction.
    #[error("transaction {transaction} failed to read directory {path}")]
    ReadDirectory {
        transaction: usize,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Reading one directory entry failed while expanding a declared directory
    /// transaction.
    #[error("transaction {transaction} failed to read a directory entry under {path}")]
    ReadDirectoryEntry {
        transaction: usize,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The planner failed to compute a file-operation path relative to the
    /// transaction root.
    #[error("failed to compute the relative path of {path} under transaction root {root}")]
    StripPrefix {
        transaction: usize,
        root: PathBuf,
        path: PathBuf,
        #[source]
        source: std::path::StripPrefixError,
    },
}

/// Errors raised while analyzing source files without writing outputs.
#[derive(Debug, Error)]
pub enum AnalysisError {
    /// Reading one source file failed before tokenization.
    #[error("transaction {transaction} failed to read source file {path}")]
    ReadSource {
        transaction: usize,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Two distinct delimiter recognizers produced overlapping tokens.
    #[error(
        "transaction {transaction} has overlapping delimiter tokens in {path} between {left_delimiter} at {left_span} and {right_delimiter} at {right_span}"
    )]
    TokenOverlap {
        transaction: usize,
        path: PathBuf,
        left_delimiter: String,
        left_span: SourceSpan,
        right_delimiter: String,
        right_span: SourceSpan,
    },
    /// One transform produced two completed candidate chains that conflict.
    #[error(
        "transaction {transaction} transform `{transform}` is ambiguous in {path}: candidate regions {left_span} and {right_span} conflict"
    )]
    AmbiguousTransform {
        transaction: usize,
        path: PathBuf,
        transform: String,
        left_span: SourceSpan,
        right_span: SourceSpan,
    },
    /// Two replace transforms target overlapping source regions.
    #[error(
        "transaction {transaction} has overlapping replace regions in {path} between transform `{left_transform}` at {left_span} and transform `{right_transform}` at {right_span}"
    )]
    ReplaceOverlap {
        transaction: usize,
        path: PathBuf,
        left_transform: String,
        left_span: SourceSpan,
        right_transform: String,
        right_span: SourceSpan,
    },
    /// Rendering one replacement template failed because captures were missing.
    #[error("transaction {transaction} failed to render transform `{transform}` in {path}")]
    TemplateRender {
        transaction: usize,
        path: PathBuf,
        transform: String,
        #[source]
        source: sem::TemplateRenderError,
    },
}

/// Errors raised while deleting or writing outputs in stage 4.
#[derive(Debug, Error)]
pub enum MaterializationError {
    /// Deleting one approved pre-existing output failed.
    #[error("failed to delete output file {path}")]
    DeleteOutput {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// One claimed output path was occupied during the stage-4 re-check.
    #[error("claimed output path {path} is occupied before materialization")]
    OccupiedOutput { path: PathBuf },
    /// Creating a parent directory for an output file failed.
    #[error("failed to create parent directory {path}")]
    CreateParent {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Writing one destination or log file failed.
    #[error("failed to write output file {path}")]
    WriteOutput {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Writing to the caller-provided stdout sink failed.
    #[error("failed to write stdout log output")]
    WriteStdout {
        #[source]
        source: std::io::Error,
    },
}

/// Errors raised while running scheme-level post commands.
#[derive(Debug, Error)]
pub enum PostError {
    /// Starting one post command failed.
    #[error("failed to start post command `{cmd}` in {dir}")]
    Spawn {
        dir: PathBuf,
        cmd: String,
        #[source]
        source: std::io::Error,
    },
    /// One post command exited unsuccessfully.
    #[error("post command `{cmd}` in {dir} exited with status {status}")]
    Failed { dir: PathBuf, cmd: String, status: ExitStatus },
}

fn transaction_has_no_outputs(transaction: &sem::Transaction) -> bool {
    let has_dst = match &transaction.kind {
        | sem::TransactionKind::File { dst, .. } => dst.is_some(),
        | sem::TransactionKind::Directory { dst_root, .. } => dst_root.is_some(),
    };
    !has_dst && transaction.log.is_none()
}

fn transaction_source_path(transaction: &sem::Transaction) -> &Path {
    match &transaction.kind {
        | sem::TransactionKind::File { src, .. } => src.as_path(),
        | sem::TransactionKind::Directory { src_root, .. } => src_root.as_path(),
    }
}

fn walk_files(transaction: usize, root: &Path) -> Result<Vec<PathBuf>, PlanningError> {
    let mut files = Vec::new();
    collect_files(transaction, root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_files(
    transaction: usize, root: &Path, files: &mut Vec<PathBuf>,
) -> Result<(), PlanningError> {
    let mut entries = std::fs::read_dir(root)
        .map_err(|source| PlanningError::ReadDirectory {
            transaction,
            path: root.to_path_buf(),
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| PlanningError::ReadDirectoryEntry {
            transaction,
            path: root.to_path_buf(),
            source,
        })?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_files(transaction, &path, files)?;
        } else if path.is_file() {
            files.push(path);
        }
    }

    Ok(())
}

fn scan_delimiter_tokens(
    content: &str, delimiter: &sem::Delimiter, next_id: &mut usize,
) -> DelimiterTokenStream {
    let description = delimiter_description(delimiter);
    let tokens = match delimiter {
        | sem::Delimiter::String(value) => content
            .match_indices(value)
            .map(|(start, matched)| {
                let id = *next_id;
                *next_id += 1;
                SharedToken {
                    id,
                    start,
                    end: start + matched.len(),
                    matched: matched.to_string(),
                    captures: Vec::new(),
                }
            })
            .collect(),
        | sem::Delimiter::Regex { regex, .. } => regex
            .captures_iter(content)
            .map(|captures| {
                let matched =
                    captures.get(0).expect("regex capture iteration always yields a full match");
                let id = *next_id;
                *next_id += 1;
                SharedToken {
                    id,
                    start: matched.start(),
                    end: matched.end(),
                    matched: matched.as_str().to_string(),
                    captures: captures
                        .iter()
                        .skip(1)
                        .map(|capture| {
                            capture.map_or_else(String::new, |capture| capture.as_str().to_string())
                        })
                        .collect(),
                }
            })
            .collect(),
    };

    DelimiterTokenStream { description, tokens }
}

fn delimiter_description(delimiter: &sem::Delimiter) -> String {
    match delimiter {
        | sem::Delimiter::String(value) => format!("literal `{value}`"),
        | sem::Delimiter::Regex { source, .. } => format!("regex `{source}`"),
    }
}

fn bind_tokens(tokens: &[SharedToken], delimiter_index: usize) -> Vec<TokenOccurrence> {
    tokens
        .iter()
        .map(|token| TokenOccurrence {
            id: token.id,
            delimiter_index,
            start: token.start,
            end: token.end,
            matched: token.matched.clone(),
            captures: token.captures.clone(),
        })
        .collect()
}

fn apply_replacements(content: &str, replacements: &[Replacement]) -> String {
    let mut rewritten = content.to_string();
    let mut replacements = replacements.iter().collect::<Vec<_>>();
    replacements.sort_by_key(|replacement| replacement.start);
    replacements.reverse();

    for replacement in replacements {
        rewritten.replace_range(replacement.start..replacement.end, &replacement.text);
    }

    rewritten
}

fn render_log_records<'a>(records: impl Iterator<Item = &'a LogRecord>) -> String {
    let mut content = String::new();
    for record in records {
        content.push_str(
            &serde_json::to_string(record)
                .expect("serializing log records to a string cannot fail"),
        );
        content.push('\n');
    }
    content
}

fn ranges_overlap(
    left_start: usize, left_end: usize, right_start: usize, right_end: usize,
) -> bool {
    left_start < right_end && right_start < left_end
}

fn write_output_file(path: &Path, content: &str) -> Result<(), MaterializationError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| MaterializationError::CreateParent {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    std::fs::write(path, content)
        .map_err(|source| MaterializationError::WriteOutput { path: path.to_path_buf(), source })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::syntax as syn;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn sequence_matching_rejects_ambiguous_candidates() {
        let transform = sem::Transform {
            id: sem::TransformId::new(0),
            name: "ambiguous".to_string(),
            matcher: sem::Matcher {
                delimiters: vec![
                    sem::Delimiter::String("A".to_string()),
                    sem::Delimiter::String("B".to_string()),
                ],
            },
            effects: vec![sem::Effect::Log],
        };
        let analyzer = FileAnalyzer::new(7, Path::new("sample.txt"), "A A B");
        let err = analyzer.analyze(&[&transform]).expect_err("expected ambiguity rejection");

        match *err {
            | AnalysisError::AmbiguousTransform { transaction, .. } => {
                assert_eq!(transaction, 7);
            }
            | other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn sequence_matching_accepts_repeated_delimiter_positions() {
        let transform = sem::Transform {
            id: sem::TransformId::new(0),
            name: "repeat".to_string(),
            matcher: sem::Matcher {
                delimiters: vec![
                    sem::Delimiter::String("A".to_string()),
                    sem::Delimiter::String("A".to_string()),
                    sem::Delimiter::String("B".to_string()),
                ],
            },
            effects: vec![sem::Effect::Log],
        };
        let analyzer = FileAnalyzer::new(1, Path::new("sample.txt"), "A A B");
        let analysis = analyzer.analyze(&[&transform]).unwrap();

        assert_eq!(analysis.log_records.len(), 1);
    }

    #[test]
    fn transforms_may_replace_and_log_from_one_matcher() {
        let scheme = scheme_from_toml(
            r#"
            [[transform]]
            name = "rewrite"
            delimiters = ["A", "B"]
            effects = [{ replace = "x" }, { log = true }]

            [[transaction]]
            src = "src"
            dst = "dst"
            log = { pipe = "stdout" }
            pattern = ["**/*"]
            transform = ["rewrite"]
            "#,
        );
        let analyzer = FileAnalyzer::new(1, Path::new("sample.txt"), "A body B");
        let transform = scheme.transform(scheme.transform_id("rewrite").unwrap());

        let analysis = analyzer.analyze(&[transform]).unwrap();

        assert_eq!(analysis.rendered_content, "x");
        assert_eq!(analysis.log_records.len(), 1);
        assert_eq!(analysis.log_records[0].body, "A body B");
    }

    #[test]
    fn replace_regions_must_be_disjoint_across_transforms() {
        let scheme = scheme_from_toml(
            r#"
            [[transform]]
            name = "outer"
            delimiters = ["A", "D"]
            effects = [{ replace = "x" }]

            [[transform]]
            name = "inner"
            delimiters = ["B", "C"]
            effects = [{ replace = "y" }]

            [[transaction]]
            src = "src"
            dst = "dst"
            pattern = ["**/*"]
            transform = ["outer", "inner"]
            "#,
        );
        let analyzer = FileAnalyzer::new(1, Path::new("sample.txt"), "A B C D");
        let transforms = vec![
            scheme.transform(scheme.transform_id("outer").unwrap()),
            scheme.transform(scheme.transform_id("inner").unwrap()),
        ];

        let err = analyzer.analyze(&transforms).expect_err("expected overlap");
        match *err {
            | AnalysisError::ReplaceOverlap { .. } => {}
            | other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn declared_directory_transactions_require_directory_sources() {
        let temp = TestDir::new();
        std::fs::write(temp.path.join("input.txt"), "hello").unwrap();

        let scheme = sem::Scheme::from_syntax(
            toml::from_str::<syn::Projection>(&format!(
                r#"
                [[transform]]
                name = "noop"
                delimiters = ["hello"]
                effects = [{{ log = true }}]

                [[transaction]]
                src = "{}"
                pattern = ["**/*"]
                transform = ["noop"]
                "#,
                temp.path.join("input.txt").display()
            ))
            .unwrap(),
            Path::new("/"),
        )
        .unwrap();

        let err = test_engine("test", scheme).plan().expect_err("expected planning failure");
        match err {
            | EngineError::Planning { source, .. } => match *source {
                | PlanningError::SourceKindMismatch { expected, .. } => {
                    assert_eq!(expected, PathKind::Directory);
                }
                | other => panic!("unexpected planning error: {other:?}"),
            },
            | other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn directory_transactions_expand_patterns_and_preserve_paths() {
        let temp = TestDir::new();
        let src_root = temp.path.join("src");
        std::fs::create_dir_all(src_root.join("nested")).unwrap();
        std::fs::write(src_root.join("nested").join("lib.rs"), "body").unwrap();
        std::fs::write(src_root.join("skip.txt"), "body").unwrap();

        let scheme = sem::Scheme::from_syntax(
            toml::from_str::<syn::Projection>(&format!(
                r#"
                [[transform]]
                name = "noop"
                delimiters = ["body"]
                effects = [{{ log = true }}]

                [[transaction]]
                src = "{}"
                dst = "{}"
                pattern = ["**/*.rs"]
                transform = ["noop"]
                "#,
                src_root.display(),
                temp.path.join("dst").display(),
            ))
            .unwrap(),
            Path::new("/"),
        )
        .unwrap();

        let planned = test_engine("test", scheme).plan().unwrap();

        assert_eq!(planned.operation_plan.transactions[0].operations.len(), 1);
        assert_eq!(
            planned.operation_plan.transactions[0].operations[0].dst.as_ref().unwrap(),
            &temp.path.join("dst").join("nested").join("lib.rs")
        );
    }

    #[test]
    fn run_plan_reports_overwrite_requirements() {
        let temp = TestDir::new();
        let src_path = temp.path.join("input.txt");
        let dst_path = temp.path.join("output.txt");
        std::fs::write(&src_path, "hello").unwrap();
        std::fs::write(&dst_path, "old").unwrap();

        let scheme = sem::Scheme::from_syntax(
            toml::from_str::<crate::syntax::Projection>(&format!(
                r#"
                [[transform]]
                name = "rewrite"
                delimiters = ["hello"]
                effects = [{{ replace = "updated" }}]

                [[transaction]]
                src = "{}"
                dst = "{}"
                transform = ["rewrite"]
                "#,
                src_path.display(),
                dst_path.display(),
            ))
            .unwrap(),
            Path::new("/"),
        )
        .unwrap();

        let err = test_engine("overwrite-test", scheme)
            .plan()
            .unwrap()
            .execute(OverwriteMode::RejectExisting)
            .expect_err("expected overwrite rejection");

        match err {
            | EngineError::OverwriteRequired { paths, .. } => {
                assert!(paths.contains(&dst_path));
            }
            | other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn post_commands_run_in_their_declared_directory() {
        let temp = TestDir::new();
        let post_dir = temp.path.join("post-dir");
        std::fs::create_dir_all(&post_dir).unwrap();

        let scheme = sem::Scheme::from_syntax(
            toml::from_str::<crate::syntax::Projection>(
                r#"
                transform = []
                transaction = []

                [[post]]
                dir = "post-dir"
                cmd = "pwd > pwd.txt"
                "#,
            )
            .unwrap(),
            temp.path.as_path(),
        )
        .unwrap();

        test_engine("post-dir-test", scheme)
            .plan()
            .unwrap()
            .execute(OverwriteMode::RejectExisting)
            .unwrap();

        let observed =
            PathBuf::from(std::fs::read_to_string(post_dir.join("pwd.txt")).unwrap().trim_end());
        assert_eq!(observed.canonicalize().unwrap(), post_dir.canonicalize().unwrap());
    }

    fn scheme_from_toml(source: &str) -> sem::Scheme {
        let proj = toml::from_str::<syn::Projection>(source).unwrap();
        sem::Scheme::from_syntax(proj, Path::new("/tmp")).unwrap()
    }

    fn test_engine(source_name: &str, scheme: sem::Scheme) -> Engine {
        Engine::new(source_name, scheme)
    }

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let path = std::env::temp_dir().join(format!("mosaika-test-{nonce}"));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
