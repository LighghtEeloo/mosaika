//! Command line entry point for the `mosaika` projection pipeline.
//!
//! The analyzer scans each distinct delimiter recognizer once per source file
//! and reuses that concrete token stream anywhere the same delimiter appears.
//! This keeps replace and log transforms composable while preserving the
//! design's ambiguity checks.

use clap::Parser;
use mosaika::{semantics as sem, syntax as syn};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    io::{self, Write},
    path::{Path, PathBuf},
    process::ExitStatus,
};
use thiserror::Error;
use tracing::{trace, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
struct Cli {
    /// Path to the scheme file.
    #[arg()]
    proj: PathBuf,
    /// Delete approved outputs without prompting.
    #[arg(long)]
    force: bool,
}

#[derive(Debug)]
struct Pipeline {
    scheme_path: PathBuf,
    scheme_dir: PathBuf,
    force: bool,
}

impl Pipeline {
    fn from_cli(cli: Cli) -> Result<Self, RunError> {
        let scheme_path = if cli.proj.is_absolute() {
            cli.proj
        } else {
            std::env::current_dir()
                .map_err(RunError::CurrentDirectory)?
                .join(cli.proj)
        };
        let scheme_dir = scheme_path
            .parent()
            .ok_or_else(|| RunError::SchemeHasNoParent {
                path: scheme_path.clone(),
            })?
            .to_path_buf();
        Ok(Self { scheme_path, scheme_dir, force: cli.force })
    }

    fn run(&self) -> Result<(), RunError> {
        trace!(
            scheme_path = %self.scheme_path.display(),
            force = self.force,
            "starting mosaika pipeline"
        );

        let scheme = self.load_scheme()?;
        let planned =
            self.plan(&scheme).map_err(|source| RunError::Planning {
                scheme_path: self.scheme_path.clone(),
                source: Box::new(source),
            })?;
        if !self.confirm_overwrites(&planned.approved_overwrites)? {
            println!("Overwrite rejected, exiting.");
            return Ok(());
        }
        let prepared = self.analyze(&scheme, planned).map_err(|source| {
            RunError::Analysis { scheme_path: self.scheme_path.clone(), source }
        })?;
        self.materialize(&prepared).map_err(|source| {
            RunError::Materialization {
                scheme_path: self.scheme_path.clone(),
                source: Box::new(source),
            }
        })?;
        self.run_posts(&scheme.posts).map_err(|source| RunError::Post {
            scheme_path: self.scheme_path.clone(),
            source: Box::new(source),
        })?;

        trace!(scheme_path = %self.scheme_path.display(), "finished mosaika pipeline");
        Ok(())
    }

    fn load_scheme(&self) -> Result<sem::Scheme, RunError> {
        trace!(scheme_path = %self.scheme_path.display(), "loading scheme");
        let proj =
            syn::Proj::from_file(&self.scheme_path).map_err(|source| {
                RunError::LoadScheme {
                    scheme_path: self.scheme_path.clone(),
                    source: Box::new(source),
                }
            })?;
        let scheme = sem::Scheme::from_syntax(proj, &self.scheme_dir).map_err(
            |source| RunError::Scheme {
                scheme_path: self.scheme_path.clone(),
                source: Box::new(source),
            },
        )?;
        trace!(
            transform_count = scheme.transforms.len(),
            transaction_count = scheme.transactions.len(),
            post_count = scheme.posts.len(),
            "loaded scheme"
        );
        Ok(scheme)
    }

    fn plan(&self, scheme: &sem::Scheme) -> Result<PlannedRun, PlanningError> {
        trace!(
            transaction_count = scheme.transactions.len(),
            "planning transactions"
        );

        let mut transactions = Vec::new();
        let mut output_claims = OutputClaims::default();

        for transaction in &scheme.transactions {
            for transform_name in &transaction.transform_names {
                if !scheme.transforms.contains_key(transform_name) {
                    return Err(PlanningError::UnknownTransform {
                        transaction: transaction.index,
                        name: transform_name.clone(),
                    });
                }
            }

            if transaction.dst.is_none() && transaction.log.is_none() {
                warn!(
                    transaction = transaction.index,
                    src = %transaction.src.display(),
                    "transaction has neither dst nor log; it will run as analysis-only"
                );
            }

            let work_items = match SourceKind::detect(
                transaction.index,
                &transaction.src,
            )? {
                | SourceKind::File => {
                    self.plan_file_transaction(transaction, &mut output_claims)?
                }
                | SourceKind::Directory => self.plan_directory_transaction(
                    transaction,
                    &mut output_claims,
                )?,
            };

            if let Some(log) = &transaction.log {
                output_claims.claim_log_sink(transaction.index, log)?;
            }

            transactions.push(PlannedTransaction {
                index: transaction.index,
                log: transaction.log.clone(),
                transform_names: transaction.transform_names.clone(),
                work_items,
            });
        }

        trace!(
            transaction_count = transactions.len(),
            claimed_output_count = output_claims.claimed_outputs.len(),
            overwrite_count = output_claims.approved_overwrites.len(),
            "finished planning transactions"
        );

        Ok(PlannedRun {
            transactions,
            approved_overwrites: output_claims.approved_overwrites,
            claimed_outputs: output_claims.claimed_outputs,
        })
    }

    fn plan_file_transaction(
        &self, transaction: &sem::Transaction, output_claims: &mut OutputClaims,
    ) -> Result<Vec<WorkItem>, PlanningError> {
        if transaction.patterns.is_some() {
            return Err(PlanningError::PatternOnFileTransaction {
                transaction: transaction.index,
                src: transaction.src.clone(),
            });
        }

        let dst = if let Some(dst) = &transaction.dst {
            output_claims.claim_file_target(
                transaction.index,
                &transaction.src,
                dst,
                PathExpectation::File,
            )?;
            Some(dst.clone())
        } else {
            None
        };

        Ok(vec![WorkItem { src: transaction.src.clone(), dst }])
    }

    fn plan_directory_transaction(
        &self, transaction: &sem::Transaction, output_claims: &mut OutputClaims,
    ) -> Result<Vec<WorkItem>, PlanningError> {
        let patterns = transaction
            .patterns
            .as_ref()
            .filter(|patterns| !patterns.is_empty())
            .ok_or_else(|| PlanningError::MissingPattern {
                transaction: transaction.index,
                src: transaction.src.clone(),
            })?;

        if let Some(dst) = &transaction.dst
            && dst.exists()
            && !dst.is_dir()
        {
            return Err(PlanningError::DestinationKindMismatch {
                transaction: transaction.index,
                src: transaction.src.clone(),
                dst: dst.clone(),
                expected: PathExpectation::Directory,
            });
        }

        let mut selected = BTreeSet::new();
        for file in walk_files(transaction.index, &transaction.src)? {
            let relative =
                file.strip_prefix(&transaction.src).map_err(|source| {
                    PlanningError::StripPrefix {
                        transaction: transaction.index,
                        root: transaction.src.clone(),
                        path: file.clone(),
                        source,
                    }
                })?;
            if patterns.iter().any(|pattern| pattern.matches_path(relative)) {
                selected.insert(file);
            }
        }

        let mut work_items = Vec::new();
        for src in selected {
            let dst = if let Some(dst_root) = &transaction.dst {
                let relative =
                    src.strip_prefix(&transaction.src).map_err(|source| {
                        PlanningError::StripPrefix {
                            transaction: transaction.index,
                            root: transaction.src.clone(),
                            path: src.clone(),
                            source,
                        }
                    })?;
                let dst = dst_root.join(relative);
                output_claims.claim_file_target(
                    transaction.index,
                    &src,
                    &dst,
                    PathExpectation::File,
                )?;
                Some(dst)
            } else {
                None
            };

            work_items.push(WorkItem { src, dst });
        }

        Ok(work_items)
    }

    fn confirm_overwrites(
        &self, approved_overwrites: &BTreeSet<PathBuf>,
    ) -> Result<bool, RunError> {
        if approved_overwrites.is_empty() || self.force {
            return Ok(true);
        }

        println!(
            "The following output files already exist and will be deleted:"
        );
        for path in approved_overwrites {
            println!("  {}", path.display());
        }
        print!("Continue? [y/N] ");
        io::stdout().flush().map_err(RunError::Prompt)?;
        let mut input = String::new();
        io::stdin().read_line(&mut input).map_err(RunError::Prompt)?;
        Ok(input.trim().eq_ignore_ascii_case("y"))
    }

    fn analyze(
        &self, scheme: &sem::Scheme, planned: PlannedRun,
    ) -> AnalysisResult<PreparedRun> {
        trace!(
            transaction_count = planned.transactions.len(),
            "analyzing work items"
        );

        let mut transactions = Vec::new();
        for transaction in planned.transactions {
            let active_transforms = transaction
                .transform_names
                .iter()
                .map(|name| {
                    let transform = scheme.transforms.get(name).expect(
                        "planning validated transform names before analysis",
                    );
                    (name.as_str(), transform)
                })
                .collect::<Vec<_>>();

            if transaction.log.is_none()
                && active_transforms.iter().any(|(_, transform)| {
                    matches!(transform.action, sem::Action::Log)
                })
            {
                warn!(
                    transaction = transaction.index,
                    "transaction has log transforms but no log sink; findings will be discarded"
                );
            }

            let mut file_outputs = Vec::new();
            let mut log_records = Vec::new();
            for work_item in &transaction.work_items {
                let content = std::fs::read_to_string(&work_item.src).map_err(
                    |source| {
                        Box::new(AnalysisError::ReadSource {
                            transaction: transaction.index,
                            path: work_item.src.clone(),
                            source,
                        })
                    },
                )?;
                let analyzer = FileAnalyzer::new(
                    transaction.index,
                    &work_item.src,
                    &content,
                );
                let analysis = analyzer.analyze(&active_transforms)?;

                if let Some(dst) = &work_item.dst {
                    file_outputs.push(FileOutput {
                        path: dst.clone(),
                        content: analysis.rendered_content,
                    });
                }

                if transaction.log.is_some() {
                    log_records.extend(analysis.log_records);
                }
            }

            let log_output = transaction.log.as_ref().map(|sink| {
                PreparedLogOutput::from_records(sink.clone(), &log_records)
            });

            transactions.push(PreparedTransaction { file_outputs, log_output });
        }

        trace!(transaction_count = transactions.len(), "finished analysis");

        Ok(PreparedRun {
            transactions,
            approved_overwrites: planned.approved_overwrites,
            claimed_outputs: planned.claimed_outputs,
        })
    }

    fn materialize(
        &self, prepared: &PreparedRun,
    ) -> Result<(), MaterializationError> {
        trace!(
            transaction_count = prepared.transactions.len(),
            "materializing outputs"
        );

        for path in &prepared.approved_overwrites {
            if path.exists() {
                std::fs::remove_file(path).map_err(|source| {
                    MaterializationError::DeleteOutput {
                        path: path.clone(),
                        source,
                    }
                })?;
            }
        }

        for path in &prepared.claimed_outputs {
            if path.exists() {
                return Err(MaterializationError::OccupiedOutput {
                    path: path.clone(),
                });
            }
        }

        for transaction in &prepared.transactions {
            for output in &transaction.file_outputs {
                write_output_file(&output.path, &output.content)?;
            }
            if let Some(log_output) = &transaction.log_output {
                match log_output {
                    | PreparedLogOutput::File { path, content } => {
                        write_output_file(path, content)?;
                    }
                    | PreparedLogOutput::Stdout { content } => {
                        if !content.is_empty() {
                            print!("{content}");
                            io::stdout().flush().map_err(|source| {
                                MaterializationError::WriteStdout { source }
                            })?;
                        }
                    }
                }
            }
        }

        trace!("finished materialization");
        Ok(())
    }

    fn run_posts(&self, posts: &[sem::PostCommand]) -> Result<(), PostError> {
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
                return Err(PostError::Failed {
                    dir: post.dir.clone(),
                    cmd: post.cmd.clone(),
                    status,
                });
            }
            trace!(dir = %post.dir.display(), cmd = %post.cmd, "finished post command");
        }
        trace!("finished post commands");
        Ok(())
    }
}

#[derive(Debug)]
struct PlannedRun {
    transactions: Vec<PlannedTransaction>,
    approved_overwrites: BTreeSet<PathBuf>,
    claimed_outputs: BTreeSet<PathBuf>,
}

#[derive(Debug)]
struct PlannedTransaction {
    index: usize,
    log: Option<sem::LogDestination>,
    transform_names: Vec<String>,
    work_items: Vec<WorkItem>,
}

#[derive(Debug)]
struct WorkItem {
    src: PathBuf,
    dst: Option<PathBuf>,
}

#[derive(Debug)]
struct PreparedRun {
    transactions: Vec<PreparedTransaction>,
    approved_overwrites: BTreeSet<PathBuf>,
    claimed_outputs: BTreeSet<PathBuf>,
}

#[derive(Debug)]
struct PreparedTransaction {
    file_outputs: Vec<FileOutput>,
    log_output: Option<PreparedLogOutput>,
}

#[derive(Debug)]
struct FileOutput {
    path: PathBuf,
    content: String,
}

#[derive(Debug)]
enum PreparedLogOutput {
    File { path: PathBuf, content: String },
    Stdout { content: String },
}

impl PreparedLogOutput {
    fn from_records(sink: sem::LogDestination, records: &[LogRecord]) -> Self {
        let mut content = String::new();
        for record in records {
            content.push_str(
                &serde_json::to_string(record)
                    .expect("serializing log records to a string cannot fail"),
            );
            content.push('\n');
        }

        match sink {
            | sem::LogDestination::File(path) => Self::File { path, content },
            | sem::LogDestination::Stdout => Self::Stdout { content },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceKind {
    File,
    Directory,
}

impl SourceKind {
    fn detect(transaction: usize, path: &Path) -> Result<Self, PlanningError> {
        if !path.exists() {
            return Err(PlanningError::MissingSource {
                transaction,
                path: path.to_path_buf(),
            });
        }
        if path.is_file() {
            return Ok(Self::File);
        }
        if path.is_dir() {
            return Ok(Self::Directory);
        }
        Err(PlanningError::UnsupportedSourceKind {
            transaction,
            path: path.to_path_buf(),
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum PathExpectation {
    File,
    Directory,
}

impl PathExpectation {
    fn matches(self, path: &Path) -> bool {
        match self {
            | Self::File => path.is_file(),
            | Self::Directory => path.is_dir(),
        }
    }
}

impl std::fmt::Display for PathExpectation {
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
        &mut self, transaction: usize, src: &Path, target: &Path,
        expected: PathExpectation,
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
            return Err(PlanningError::LogTargetMustBeFile {
                transaction,
                path: path.clone(),
            });
        }

        self.claim_output_path(transaction, path)?;
        if path.exists() {
            self.approved_overwrites.insert(path.clone());
        }
        Ok(())
    }

    fn claim_output_path(
        &mut self, transaction: usize, path: &Path,
    ) -> Result<(), PlanningError> {
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

    fn analyze(
        &self, transforms: &[(&str, &sem::Transform)],
    ) -> AnalysisResult<FileAnalysis> {
        let token_lists = self.collect_tokens(transforms)?;
        let mut replacements = Vec::new();
        let mut log_records = Vec::new();

        for (transform_name, transform) in transforms {
            let candidates = self.build_candidate_chains(
                transform_name,
                token_lists
                    .get(*transform_name)
                    .expect("tokens collected for every active transform"),
            )?;
            match &transform.action {
                | sem::Action::Replace { template } => {
                    for candidate in candidates {
                        let replacement = template
                            .render(&candidate.captures())
                            .map_err(|source| {
                                Box::new(AnalysisError::TemplateRender {
                                    transaction: self.transaction,
                                    path: self.path.to_path_buf(),
                                    transform: (*transform_name).to_string(),
                                    source,
                                })
                            })?;
                        replacements.push(Replacement {
                            transform: (*transform_name).to_string(),
                            start: candidate.start(),
                            end: candidate.end(),
                            text: replacement,
                        });
                    }
                }
                | sem::Action::Log => {
                    for candidate in candidates {
                        log_records.push(LogRecord::from_candidate(
                            transform_name,
                            self.path,
                            &self.locator,
                            self.content,
                            &candidate,
                        ));
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
        &self, transforms: &[(&str, &sem::Transform)],
    ) -> AnalysisResult<BTreeMap<String, Vec<Vec<TokenOccurrence>>>> {
        let mut shared_streams =
            BTreeMap::<String, DelimiterTokenStream>::new();
        let mut token_lists = BTreeMap::new();
        let mut next_id = 0usize;

        for (_, transform) in transforms {
            for delimiter in &transform.delimiters {
                let cache_key = delimiter_key(delimiter);
                match shared_streams.entry(cache_key) {
                    | Entry::Occupied(_) => {}
                    | Entry::Vacant(entry) => {
                        entry.insert(scan_delimiter_tokens(
                            self.content,
                            delimiter,
                            &mut next_id,
                        ));
                    }
                }
            }
        }

        self.validate_token_overlaps(&shared_streams)?;

        for (transform_name, transform) in transforms {
            let delimiters = transform
                .delimiters
                .iter()
                .enumerate()
                .map(|(delimiter_index, delimiter)| {
                    let cache_key = delimiter_key(delimiter);
                    let stream = shared_streams.get(&cache_key).expect(
                        "shared token stream exists for every active delimiter",
                    );
                    bind_tokens(&stream.tokens, delimiter_index)
                })
                .collect();
            token_lists.insert((*transform_name).to_string(), delimiters);
        }

        Ok(token_lists)
    }

    fn validate_token_overlaps(
        &self, shared_streams: &BTreeMap<String, DelimiterTokenStream>,
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
        if token_lists.is_empty() {
            return Ok(Vec::new());
        }

        let mut candidates = Vec::new();
        for start_token in &token_lists[0] {
            let mut tokens = vec![start_token.clone()];
            let mut previous_end = start_token.end;
            let mut complete = true;

            for delimiter_tokens in token_lists.iter().skip(1) {
                let Some(next) = delimiter_tokens
                    .iter()
                    .find(|token| token.start >= previous_end)
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
                    right
                        .tokens
                        .iter()
                        .any(|right_token| left_token.id == right_token.id)
                });
                let overlaps = ranges_overlap(
                    left.start(),
                    left.end(),
                    right.start(),
                    right.end(),
                );
                if shares_token || overlaps {
                    return Err(Box::new(AnalysisError::AmbiguousTransform {
                        transaction: self.transaction,
                        path: self.path.to_path_buf(),
                        transform: transform_name.to_string(),
                        left_span: self.locator.span(left.start(), left.end()),
                        right_span: self
                            .locator
                            .span(right.start(), right.end()),
                    }));
                }
            }
        }

        Ok(candidates)
    }

    fn validate_replace_overlaps(
        &self, replacements: &[Replacement],
    ) -> AnalysisResult<()> {
        let mut replacements = replacements.iter().collect::<Vec<_>>();
        replacements
            .sort_by_key(|replacement| (replacement.start, replacement.end));

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
/// Note: The analyzer shares these streams across transforms so one delimiter
/// sequence can be reused for both replacement and logging.
#[derive(Debug, Clone)]
struct DelimiterTokenStream {
    description: String,
    tokens: Vec<SharedToken>,
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
        self.tokens
            .first()
            .expect("candidate chains always contain at least one token")
            .start
    }

    fn end(&self) -> usize {
        self.tokens
            .last()
            .expect("candidate chains always contain at least one token")
            .end
    }

    fn captures(&self) -> Vec<String> {
        self.tokens
            .iter()
            .flat_map(|token| token.captures.iter().cloned())
            .collect()
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
        let line_breaks =
            content.match_indices('\n').map(|(index, _)| index).collect();
        Self { content, line_breaks }
    }

    fn position(&self, byte_index: usize) -> (usize, usize) {
        let line_index =
            self.line_breaks.partition_point(|index| *index < byte_index);
        let line_start = if line_index == 0 {
            0
        } else {
            self.line_breaks[line_index - 1] + 1
        };
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

#[derive(Debug, Clone, Serialize)]
struct SourceSpan {
    start_byte: usize,
    end_byte: usize,
    start_line: usize,
    start_column: usize,
    end_line: usize,
    end_column: usize,
}

impl std::fmt::Display for SourceSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}-{}:{}",
            self.start_line, self.start_column, self.end_line, self.end_column
        )
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

#[derive(Debug, Error)]
enum RunError {
    #[error("scheme {scheme_path}: {source}")]
    LoadScheme {
        scheme_path: PathBuf,
        #[source]
        source: Box<syn::LoadError>,
    },
    #[error("scheme {scheme_path}: {source}")]
    Scheme {
        scheme_path: PathBuf,
        #[source]
        source: Box<sem::SchemeError>,
    },
    #[error("scheme {scheme_path}: {source}")]
    Planning {
        scheme_path: PathBuf,
        #[source]
        source: Box<PlanningError>,
    },
    #[error("scheme {scheme_path}: {source}")]
    Analysis {
        scheme_path: PathBuf,
        #[source]
        source: Box<AnalysisError>,
    },
    #[error("scheme {scheme_path}: {source}")]
    Materialization {
        scheme_path: PathBuf,
        #[source]
        source: Box<MaterializationError>,
    },
    #[error("scheme {scheme_path}: {source}")]
    Post {
        scheme_path: PathBuf,
        #[source]
        source: Box<PostError>,
    },
    #[error("failed to determine the current directory")]
    CurrentDirectory(#[source] std::io::Error),
    #[error("scheme path {path} is not contained in a directory")]
    SchemeHasNoParent { path: PathBuf },
    #[error("failed while reading overwrite confirmation")]
    Prompt(#[source] std::io::Error),
}

#[derive(Debug, Error)]
enum PlanningError {
    #[error("transaction {transaction} source path {path} does not exist")]
    MissingSource { transaction: usize, path: PathBuf },
    #[error(
        "transaction {transaction} source path {path} must be a file or directory"
    )]
    UnsupportedSourceKind { transaction: usize, path: PathBuf },
    #[error("transaction {transaction} references unknown transform `{name}`")]
    UnknownTransform { transaction: usize, name: String },
    #[error(
        "transaction {transaction} rooted at {src} cannot define `pattern` because `src` is a file"
    )]
    PatternOnFileTransaction { transaction: usize, src: PathBuf },
    #[error(
        "transaction {transaction} rooted at {src} must define `pattern` because `src` is a directory"
    )]
    MissingPattern { transaction: usize, src: PathBuf },
    #[error(
        "transaction {transaction} requires destination path {dst} to be a {expected} because source {src} determines that kind"
    )]
    DestinationKindMismatch {
        transaction: usize,
        src: PathBuf,
        dst: PathBuf,
        expected: PathExpectation,
    },
    #[error("transaction {transaction} requires log path {path} to be a file")]
    LogTargetMustBeFile { transaction: usize, path: PathBuf },
    #[error(
        "output path {path} is claimed by both transaction {first_transaction} and transaction {second_transaction}"
    )]
    ClaimConflict {
        path: PathBuf,
        first_transaction: usize,
        second_transaction: usize,
    },
    #[error("transaction {transaction} failed to read directory {path}")]
    ReadDirectory {
        transaction: usize,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "transaction {transaction} failed to read a directory entry under {path}"
    )]
    ReadDirectoryEntry {
        transaction: usize,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "failed to compute the relative path of {path} under transaction root {root}"
    )]
    StripPrefix {
        transaction: usize,
        root: PathBuf,
        path: PathBuf,
        #[source]
        source: std::path::StripPrefixError,
    },
}

#[derive(Debug, Error)]
enum AnalysisError {
    #[error("transaction {transaction} failed to read source file {path}")]
    ReadSource {
        transaction: usize,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
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
    #[error(
        "transaction {transaction} failed to render transform `{transform}` in {path}"
    )]
    TemplateRender {
        transaction: usize,
        path: PathBuf,
        transform: String,
        #[source]
        source: sem::TemplateRenderError,
    },
}

#[derive(Debug, Error)]
enum MaterializationError {
    #[error("failed to delete output file {path}")]
    DeleteOutput {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("claimed output path {path} is occupied before materialization")]
    OccupiedOutput { path: PathBuf },
    #[error("failed to create parent directory {path}")]
    CreateParent {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write output file {path}")]
    WriteOutput {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write stdout log output")]
    WriteStdout {
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Error)]
enum PostError {
    #[error("failed to start post command `{cmd}` in {dir}")]
    Spawn {
        dir: PathBuf,
        cmd: String,
        #[source]
        source: std::io::Error,
    },
    #[error("post command `{cmd}` in {dir} exited with status {status}")]
    Failed { dir: PathBuf, cmd: String, status: ExitStatus },
}

fn main() {
    init_tracing();
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), RunError> {
    let cli = Cli::parse();
    let pipeline = Pipeline::from_cli(cli)?;
    pipeline.run()
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

fn walk_files(
    transaction: usize, root: &Path,
) -> Result<Vec<PathBuf>, PlanningError> {
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
                let matched = captures.get(0).expect(
                    "regex capture iteration always yields a full match",
                );
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
                            capture.map_or_else(String::new, |capture| {
                                capture.as_str().to_string()
                            })
                        })
                        .collect(),
                }
            })
            .collect(),
    };

    DelimiterTokenStream { description, tokens }
}

fn delimiter_key(delimiter: &sem::Delimiter) -> String {
    match delimiter {
        | sem::Delimiter::String(value) => format!("string:{value}"),
        | sem::Delimiter::Regex { source, .. } => format!("regex:{source}"),
    }
}

fn delimiter_description(delimiter: &sem::Delimiter) -> String {
    match delimiter {
        | sem::Delimiter::String(value) => format!("literal `{value}`"),
        | sem::Delimiter::Regex { source, .. } => format!("regex `{source}`"),
    }
}

fn bind_tokens(
    tokens: &[SharedToken], delimiter_index: usize,
) -> Vec<TokenOccurrence> {
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
        rewritten.replace_range(
            replacement.start..replacement.end,
            &replacement.text,
        );
    }

    rewritten
}

fn ranges_overlap(
    left_start: usize, left_end: usize, right_start: usize, right_end: usize,
) -> bool {
    left_start < right_end && right_start < left_end
}

fn write_output_file(
    path: &Path, content: &str,
) -> Result<(), MaterializationError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| {
            MaterializationError::CreateParent {
                path: parent.to_path_buf(),
                source,
            }
        })?;
    }
    std::fs::write(path, content).map_err(|source| {
        MaterializationError::WriteOutput { path: path.to_path_buf(), source }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn sequence_matching_rejects_ambiguous_candidates() {
        let transform = sem::Transform {
            delimiters: vec![
                sem::Delimiter::String("A".to_string()),
                sem::Delimiter::String("B".to_string()),
            ],
            action: sem::Action::Log,
        };
        let analyzer = FileAnalyzer::new(7, Path::new("sample.txt"), "A A B");
        let err = analyzer
            .analyze(&[("ambiguous", &transform)])
            .expect_err("expected ambiguity rejection");

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
            delimiters: vec![
                sem::Delimiter::String("A".to_string()),
                sem::Delimiter::String("A".to_string()),
                sem::Delimiter::String("B".to_string()),
            ],
            action: sem::Action::Log,
        };
        let analyzer = FileAnalyzer::new(1, Path::new("sample.txt"), "A A B");
        let analysis = analyzer.analyze(&[("repeat", &transform)]).unwrap();

        assert_eq!(analysis.log_records.len(), 1);
    }

    #[test]
    fn replace_and_log_transforms_may_share_delimiters() {
        let scheme = scheme_from_toml(
            r#"
            [[transform]]
            name = "rewrite"
            delimiters = ["A", "B"]
            action = { replace = "x" }

            [[transform]]
            name = "audit"
            delimiters = ["A", "B"]
            action = { log = true }

            [[transaction]]
            src = "src"
            dst = "dst"
            log = { pipe = "stdout" }
            pattern = ["**/*"]
            transform = ["rewrite", "audit"]

            [[post]]
            dir = "."
            cmd = "true"
            "#,
        );
        let analyzer =
            FileAnalyzer::new(1, Path::new("sample.txt"), "A body B");
        let transforms = vec![
            ("rewrite", scheme.transforms.get("rewrite").unwrap()),
            ("audit", scheme.transforms.get("audit").unwrap()),
        ];

        let analysis = analyzer.analyze(&transforms).unwrap();

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
            action = { replace = "x" }

            [[transform]]
            name = "inner"
            delimiters = ["B", "C"]
            action = { replace = "y" }

            [[transaction]]
            src = "src"
            dst = "dst"
            pattern = ["**/*"]
            transform = ["outer", "inner"]

            [[post]]
            dir = "."
            cmd = "true"
            "#,
        );
        let analyzer = FileAnalyzer::new(1, Path::new("sample.txt"), "A B C D");
        let transforms = vec![
            ("outer", scheme.transforms.get("outer").unwrap()),
            ("inner", scheme.transforms.get("inner").unwrap()),
        ];

        let err = analyzer.analyze(&transforms).expect_err("expected overlap");
        match *err {
            | AnalysisError::ReplaceOverlap { .. } => {}
            | other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn planning_rejects_pattern_on_file_transactions() {
        let temp = TestDir::new();
        std::fs::write(temp.path.join("input.txt"), "hello").unwrap();

        let scheme = sem::Scheme::from_syntax(
            toml::from_str::<syn::Proj>(&format!(
                r#"
                [[transform]]
                name = "noop"
                delimiters = ["hello"]
                action = {{ log = true }}

                [[transaction]]
                src = "{}"
                pattern = ["**/*"]
                transform = ["noop"]

                [[post]]
                dir = "."
                cmd = "true"
                "#,
                temp.path.join("input.txt").display()
            ))
            .unwrap(),
            Path::new("/"),
        )
        .unwrap();

        let pipeline = Pipeline {
            scheme_path: temp.path.join("mosaika.toml"),
            scheme_dir: temp.path.clone(),
            force: true,
        };

        let err =
            pipeline.plan(&scheme).expect_err("expected planning failure");
        match err {
            | PlanningError::PatternOnFileTransaction { .. } => {}
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
            toml::from_str::<syn::Proj>(&format!(
                r#"
                [[transform]]
                name = "noop"
                delimiters = ["body"]
                action = {{ log = true }}

                [[transaction]]
                src = "{}"
                dst = "{}"
                pattern = ["**/*.rs"]
                transform = ["noop"]

                [[post]]
                dir = "."
                cmd = "true"
                "#,
                src_root.display(),
                temp.path.join("dst").display(),
            ))
            .unwrap(),
            Path::new("/"),
        )
        .unwrap();

        let pipeline = Pipeline {
            scheme_path: temp.path.join("mosaika.toml"),
            scheme_dir: temp.path.clone(),
            force: true,
        };
        let planned = pipeline.plan(&scheme).unwrap();

        assert_eq!(planned.transactions[0].work_items.len(), 1);
        assert_eq!(
            planned.transactions[0].work_items[0].dst.as_ref().unwrap(),
            &temp.path.join("dst").join("nested").join("lib.rs")
        );
    }

    fn scheme_from_toml(source: &str) -> sem::Scheme {
        let proj = toml::from_str::<syn::Proj>(source).unwrap();
        sem::Scheme::from_syntax(proj, Path::new("/tmp")).unwrap()
    }

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path =
                std::env::temp_dir().join(format!("mosaika-test-{nonce}"));
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
