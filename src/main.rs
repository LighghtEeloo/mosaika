//! A command line tool to perform a series of transformations on a project.
//!
//! The transformations are defined in a TOML file and applied to files in the
//! order selected by each transaction.

use clap::Parser;
use mosaika::{semantics as sem, syntax as syn};
use rustc_hash::FxHashMap;
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashSet},
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

#[derive(Debug, Parser)]
struct Cli {
    /// The path to the TOML configuration file.
    #[arg()]
    proj: PathBuf,
}

#[derive(Debug, Clone)]
struct Occurrence {
    start: usize,
    end: usize,
    matched: String,
    captures: Vec<String>,
}

#[derive(Debug)]
struct PairMatch {
    open: Occurrence,
    close: Occurrence,
    depth_after_close: usize,
}

#[derive(Debug)]
struct Replacement {
    start: usize,
    end: usize,
    text: String,
}

#[derive(Debug)]
struct Token {
    occurrence: Occurrence,
    open: bool,
    close: bool,
}

#[derive(Debug, Serialize)]
struct LogRecord {
    mode: String,
    transform: String,
    file: String,
    start_line: usize,
    start_column: usize,
    end_line: Option<usize>,
    end_column: Option<usize>,
    matched: Option<String>,
    captures: Vec<String>,
    body: Option<String>,
}

#[derive(Debug)]
struct Locator<'a> {
    content: &'a str,
    line_breaks: Vec<usize>,
}

impl<'a> Locator<'a> {
    fn new(content: &'a str) -> Self {
        let line_breaks =
            content.match_indices('\n').map(|(idx, _)| idx).collect();
        Self { content, line_breaks }
    }

    fn position(&self, byte_index: usize) -> (usize, usize) {
        let line_idx = self.line_breaks.partition_point(|idx| *idx < byte_index);
        let line_start = if line_idx == 0 {
            0
        } else {
            self.line_breaks[line_idx - 1] + 1
        };
        let column =
            self.content[line_start..byte_index].chars().count() + 1;
        (line_idx + 1, column)
    }
}

fn main() {
    env_logger::init();
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let proj =
        syn::Proj::from_file(&cli.proj).expect("failed to read project file");
    let proj_dir =
        cli.proj.parent().expect("project file is not in a directory");
    std::env::set_current_dir(proj_dir)
        .expect("failed to set current directory");

    let syn::Proj { transforms, transactions, commands } = proj;
    let transforms = lower_transforms(transforms)?;
    let transactions = plan_transactions(transactions, &transforms)?;

    if !confirm_overwrites(&transactions)? {
        println!("Overwrite rejected, exiting.");
        return Ok(());
    }

    for transaction in &transactions {
        execute_transaction(transaction, &transforms)?;
    }

    for cmd in commands {
        run_post_command(cmd)?;
    }

    Ok(())
}

fn lower_transforms(
    transforms: Vec<syn::Transform>,
) -> Result<FxHashMap<String, sem::Transform>, sem::TransformError> {
    transforms
        .into_iter()
        .map(|transform| {
            let name = transform.name.clone();
            let mode = match transform.action {
                syn::Action::Replace { .. } => sem::Mode::Replace,
                syn::Action::Log { log: syn::LogMode::Block } => {
                    sem::Mode::LogBlock
                }
                syn::Action::Log { log: syn::LogMode::Anchor } => {
                    sem::Mode::LogAnchor
                }
            };
            let expected = match mode {
                sem::Mode::Replace | sem::Mode::LogBlock => 2,
                sem::Mode::LogAnchor => 1,
            };
            if transform.delimiters.len() != expected {
                return Err(sem::TransformError::InvalidDelimiterCount {
                    name,
                    mode,
                    expected,
                    actual: transform.delimiters.len(),
                });
            }

            let delimiters = transform
                .delimiters
                .into_iter()
                .map(|delimiter| compile_delimiter(&transform.name, delimiter))
                .collect::<Result<Vec<_>, _>>()?;

            let matcher = match (mode, delimiters.as_slice()) {
                (sem::Mode::Replace | sem::Mode::LogBlock, [open, close]) => sem::Matcher::Pair {
                    open: open.clone(),
                    close: close.clone(),
                },
                (sem::Mode::LogAnchor, [anchor]) => sem::Matcher::Single {
                    anchor: anchor.clone(),
                },
                _ => unreachable!(),
            };

            let replace = match transform.action {
                syn::Action::Replace { replace } => Some(parse_replace(replace)),
                syn::Action::Log { .. } => None,
            };

            Ok((transform.name, sem::Transform { mode, matcher, replace }))
        })
        .collect()
}

fn compile_delimiter(
    name: &str,
    delimiter: syn::Delimiter,
) -> Result<sem::Delimiter, sem::TransformError> {
    match delimiter {
        | syn::Delimiter::String(s) => Ok(sem::Delimiter::String(s)),
        | syn::Delimiter::Regex(regex) => regex::Regex::new(&regex.regex)
            .map(sem::Delimiter::Regex)
            .map_err(|source| sem::TransformError::InvalidRegex {
                name: name.to_string(),
                regex: regex.regex,
                source,
            }),
    }
}

fn parse_replace(replace: String) -> Vec<sem::Replacer> {
    let mut parsed = Vec::new();
    let mut buffer = String::new();

    enum State {
        Normal,
        Open,
        Insertor,
        Close,
    }

    use State::*;
    let mut state = Normal;
    for c in replace.chars() {
        match state {
            | Normal => match c {
                | '{' => {
                    if !buffer.is_empty() {
                        parsed.push(sem::Replacer::Plain(std::mem::take(
                            &mut buffer,
                        )));
                    }
                    state = Open;
                }
                | '}' => {
                    if !buffer.is_empty() {
                        parsed.push(sem::Replacer::Plain(std::mem::take(
                            &mut buffer,
                        )));
                    }
                    state = Close;
                }
                | _ => buffer.push(c),
            },
            | Open => match c {
                | '0'..='9' => {
                    buffer.push(c);
                    state = Insertor;
                }
                | '{' => {
                    parsed.push(sem::Replacer::Plain("{".to_string()));
                    debug_assert!(buffer.is_empty());
                    state = Normal;
                }
                | _ => panic!("expected digit, got {c} in open state"),
            },
            | Insertor => match c {
                | '0'..='9' => buffer.push(c),
                | '}' => {
                    parsed.push(sem::Replacer::Insertor(
                        buffer.parse().expect("invalid insertor"),
                    ));
                    buffer.clear();
                    state = Normal;
                }
                | _ => panic!("expected digit, got {c} in insertor state"),
            },
            | Close => match c {
                | '}' => {
                    parsed.push(sem::Replacer::Plain("}".to_string()));
                    debug_assert!(buffer.is_empty());
                    state = Normal;
                }
                | _ => panic!("expected `}}`, got {c} in close state"),
            },
        }
    }

    if !buffer.is_empty() {
        parsed.push(sem::Replacer::Plain(buffer));
    }

    parsed
}

fn plan_transactions(
    transactions: Vec<syn::Transaction>,
    transforms: &FxHashMap<String, sem::Transform>,
) -> Result<Vec<sem::Transaction>, sem::TransactionError> {
    let mut planned = Vec::new();

    for syn::Transaction { arrow, transform } in transactions {
        let syn::Arrow { src, dst, log, pattern } = arrow;
        if dst.is_none() && log.is_none() {
            log::warn!(
                "Skipping transaction on {} because it has neither dst nor log",
                src.display()
            );
            continue;
        }
        if !src.exists() {
            return Err(sem::TransactionError::MissingSource(src));
        }
        for name in &transform {
            if !transforms.contains_key(name) {
                return Err(sem::TransactionError::UnknownTransform(
                    name.clone(),
                ));
            }
        }

        let mut arrows = Vec::new();
        let mut overwrites = HashSet::new();
        if let Some(dst_path) = &dst {
            if dst_path.exists() {
                overwrites.insert(dst_path.clone());
            }
        }
        if let Some(log_path) = &log {
            if log_path.exists() {
                overwrites.insert(log_path.clone());
            }
        }

        match pattern {
            | None => arrows.push(sem::Arrow { src, dst }),
            | Some(patterns) => {
                for pattern in patterns {
                    let query = src.join(pattern).to_string_lossy().to_string();
                    for entry in glob::glob(&query)? {
                        let src_path = entry?;
                        if !src_path.is_file() {
                            continue;
                        }
                        let diff = src_path.strip_prefix(&src)?;
                        let dst_path =
                            dst.as_ref().map(|base| base.join(diff));
                        if let Some(path) = &dst_path {
                            if path.exists() {
                                overwrites.insert(path.clone());
                            }
                        }
                        arrows.push(sem::Arrow { src: src_path, dst: dst_path });
                    }
                }
            }
        }

        planned.push(sem::Transaction {
            overwrites: overwrites.into_iter().collect(),
            arrows,
            log,
            transform,
        });
    }

    Ok(planned)
}

fn confirm_overwrites(
    transactions: &[sem::Transaction],
) -> Result<bool, Box<dyn std::error::Error>> {
    let mut overwrites = HashSet::new();
    for transaction in transactions {
        for path in &transaction.overwrites {
            overwrites.insert(path.clone());
        }
    }
    if overwrites.is_empty() {
        return Ok(true);
    }

    println!("The following output paths exist and will be overwritten:");
    let mut overwrites = overwrites.into_iter().collect::<Vec<_>>();
    overwrites.sort();
    for overwrite in overwrites {
        println!("  {}", overwrite.display());
    }
    print!("Force overwrite? [y/N] ");
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input.trim().eq_ignore_ascii_case("y"))
}

fn execute_transaction(
    transaction: &sem::Transaction,
    transforms: &FxHashMap<String, sem::Transform>,
) -> Result<(), Box<dyn std::error::Error>> {
    let has_log = transaction.transform.iter().any(|name| {
        matches!(
            transforms[name].mode,
            sem::Mode::LogBlock | sem::Mode::LogAnchor
        )
    });
    if transaction.log.is_none() && has_log {
        log::warn!(
            "Transaction with sources rooted at {} has log transforms but no log file; findings will be discarded",
            transaction
                .arrows
                .first()
                .map(|arrow| arrow.src.display().to_string())
                .unwrap_or_else(|| "<empty>".to_string())
        );
    }

    let mut writer = match &transaction.log {
        | Some(path) => Some(open_log_writer(path)?),
        | None => None,
    };

    for arrow in &transaction.arrows {
        let mut content = std::fs::read_to_string(&arrow.src)?;
        for name in &transaction.transform {
            let transform = &transforms[name];
            match &transform.matcher {
                | sem::Matcher::Pair { open, close } => {
                    let matches =
                        scan_pair_matches(&content, open, close, &arrow.src, name)?;
                    match transform.mode {
                        | sem::Mode::Replace => {
                            let mut replacements = Vec::new();
                            for pair_match in matches
                                .into_iter()
                                .filter(|pair| pair.depth_after_close == 0)
                            {
                                let replace = transform
                                    .replace
                                    .as_ref()
                                    .expect("replace mode must have replacers");
                                replacements.push(Replacement {
                                    start: pair_match.open.start,
                                    end: pair_match.close.end,
                                    text: render_replace(
                                        replace,
                                        &pair_match.open.captures,
                                        name,
                                    )?,
                                });
                            }
                            content = apply_replacements(&content, replacements);
                        }
                        | sem::Mode::LogBlock => {
                            for pair_match in matches {
                                write_log_record(
                                    writer.as_mut(),
                                    LogRecord::from_block(
                                        name,
                                        &arrow.src,
                                        &content,
                                        pair_match,
                                    ),
                                )?;
                            }
                        }
                        | sem::Mode::LogAnchor => unreachable!(),
                    }
                }
                | sem::Matcher::Single { anchor } => {
                    for occurrence in scan_anchor_matches(&content, anchor) {
                        write_log_record(
                            writer.as_mut(),
                            LogRecord::from_anchor(
                                name,
                                &arrow.src,
                                &content,
                                occurrence,
                            ),
                        )?;
                    }
                }
            }
        }

        if let Some(dst) = &arrow.dst {
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(dst, content)?;
        }
    }

    if let Some(writer) = writer.as_mut() {
        writer.flush()?;
    }
    Ok(())
}

fn open_log_writer(path: &Path) -> Result<BufWriter<File>, Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(BufWriter::new(File::create(path)?))
}

fn write_log_record(
    writer: Option<&mut BufWriter<File>>,
    record: LogRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(writer) = writer {
        serde_json::to_writer(&mut *writer, &record)?;
        writer.write_all(b"\n")?;
    }
    Ok(())
}

fn scan_pair_matches(
    content: &str,
    open: &sem::Delimiter,
    close: &sem::Delimiter,
    path: &Path,
    transform_name: &str,
) -> Result<Vec<PairMatch>, Box<dyn std::error::Error>> {
    let locator = Locator::new(content);
    let mut tokens = BTreeMap::new();
    let mut ranges = Vec::<(usize, usize, &'static str)>::new();

    for occurrence in find_occurrences(content, open) {
        add_token(
            &mut tokens,
            &mut ranges,
            occurrence,
            true,
            false,
            &locator,
            path,
            transform_name,
        )?;
    }
    for occurrence in find_occurrences(content, close) {
        add_token(
            &mut tokens,
            &mut ranges,
            occurrence,
            false,
            true,
            &locator,
            path,
            transform_name,
        )?;
    }

    let mut stack = Vec::new();
    let mut pairs = Vec::new();
    for token in tokens.into_values() {
        if token.close && !stack.is_empty() {
            let open = stack.pop().unwrap();
            pairs.push(PairMatch {
                open,
                close: token.occurrence,
                depth_after_close: stack.len(),
            });
            continue;
        }
        if token.open {
            stack.push(token.occurrence);
            continue;
        }
        if token.close {
            let (line, column) = locator.position(token.occurrence.start);
            log::debug!(
                "ignoring unmatched closing delimiter for transform {transform_name} at {}:{line}:{column}",
                path.display()
            );
        }
    }

    if let Some(unclosed) = stack.last() {
        let (line, column) = locator.position(unclosed.start);
        return Err(invalid_input(format!(
            "unmatched opening delimiter for transform {transform_name} at {}:{line}:{column}",
            path.display()
        ))
        .into());
    }

    Ok(pairs)
}

fn add_token(
    tokens: &mut BTreeMap<usize, Token>,
    ranges: &mut Vec<(usize, usize, &'static str)>,
    occurrence: Occurrence,
    open: bool,
    close: bool,
    locator: &Locator<'_>,
    path: &Path,
    transform_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for (start, end, kind) in ranges.iter().copied() {
        let separated = occurrence.end <= start || end <= occurrence.start;
        let identical =
            occurrence.start == start && occurrence.end == end;
        if !separated && !identical {
            let (line, column) = locator.position(occurrence.start);
            let (end_line, end_column) = locator.position(occurrence.end);
            return Err(invalid_input(format!(
                "collision between {kind} delimiter and transform {transform_name} at {}:{line}:{column}-{end_line}:{end_column}",
                path.display()
            ))
            .into());
        }
    }

    ranges.push((
        occurrence.start,
        occurrence.end,
        if open { "opening" } else { "closing" },
    ));
    tokens
        .entry(occurrence.start)
        .and_modify(|token| {
            token.open |= open;
            token.close |= close;
        })
        .or_insert(Token { occurrence, open, close });
    Ok(())
}

fn scan_anchor_matches(content: &str, anchor: &sem::Delimiter) -> Vec<Occurrence> {
    find_occurrences(content, anchor)
}

fn find_occurrences(content: &str, delimiter: &sem::Delimiter) -> Vec<Occurrence> {
    match delimiter {
        | sem::Delimiter::String(string) => content
            .match_indices(string)
            .map(|(start, matched)| Occurrence {
                start,
                end: start + matched.len(),
                matched: matched.to_string(),
                captures: Vec::new(),
            })
            .collect(),
        | sem::Delimiter::Regex(regex) => regex
            .captures_iter(content)
            .filter_map(|captures| {
                captures.get(0).map(|matched| Occurrence {
                    start: matched.start(),
                    end: matched.end(),
                    matched: matched.as_str().to_string(),
                    captures: captures
                        .iter()
                        .skip(1)
                        .map(|capture| {
                            capture.map_or(String::new(), |m| m.as_str().to_string())
                        })
                        .collect(),
                })
            })
            .collect(),
    }
}

fn render_replace(
    replace: &[sem::Replacer],
    captures: &[String],
    transform_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut rendered = String::new();
    for replacer in replace {
        match replacer {
            | sem::Replacer::Plain(text) => rendered.push_str(text),
            | sem::Replacer::Insertor(index) => {
                let capture = captures.get(*index).ok_or_else(|| {
                    invalid_input(format!(
                        "transform {transform_name} references missing capture {{{index}}}"
                    ))
                })?;
                rendered.push_str(capture);
            }
        }
    }
    Ok(rendered)
}

fn apply_replacements(content: &str, replacements: Vec<Replacement>) -> String {
    let mut content = content.to_string();
    let mut replacements = replacements;
    replacements.sort_by(|lhs, rhs| rhs.start.cmp(&lhs.start));
    for replacement in replacements {
        content.replace_range(replacement.start..replacement.end, &replacement.text);
    }
    content
}

fn run_post_command(cmd: syn::Command) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        | syn::Command::System(syn::SystemCommand { dir, cmd }) => {
            let dir = dir.canonicalize()?;
            let output = std::process::Command::new("sh")
                .arg("-c")
                .arg(&cmd)
                .current_dir(&dir)
                .output()?;
            if output.status.success() {
                return Ok(());
            }
            Err(invalid_input(format!(
                "command failed: {cmd}\ndirectory: {}\noutput: {}\nerror: {}",
                dir.display(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ))
            .into())
        }
    }
}

fn invalid_input(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into())
}

impl LogRecord {
    fn from_block(
        transform: &str,
        path: &Path,
        content: &str,
        pair_match: PairMatch,
    ) -> Self {
        let locator = Locator::new(content);
        let (start_line, start_column) = locator.position(pair_match.open.start);
        let (end_line, end_column) = locator.position(pair_match.close.end);
        Self {
            mode: sem::Mode::LogBlock.to_string(),
            transform: transform.to_string(),
            file: path.display().to_string(),
            start_line,
            start_column,
            end_line: Some(end_line),
            end_column: Some(end_column),
            matched: Some(content[pair_match.open.start..pair_match.close.end].to_string()),
            captures: pair_match.open.captures,
            body: Some(content[pair_match.open.end..pair_match.close.start].to_string()),
        }
    }

    fn from_anchor(
        transform: &str,
        path: &Path,
        content: &str,
        occurrence: Occurrence,
    ) -> Self {
        let locator = Locator::new(content);
        let (start_line, start_column) = locator.position(occurrence.start);
        Self {
            mode: sem::Mode::LogAnchor.to_string(),
            transform: transform.to_string(),
            file: path.display().to_string(),
            start_line,
            start_column,
            end_line: None,
            end_column: None,
            matched: Some(occurrence.matched),
            captures: occurrence.captures,
            body: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_template_uses_capture_groups() {
        let replace = parse_replace("todo({0}, {{ok}})".to_string());
        let rendered =
            render_replace(&replace, &[String::from("\"hello\"")], "todo")
                .unwrap();
        assert_eq!(rendered, "todo(\"hello\", {ok})");
    }

    #[test]
    fn replace_mode_rewrites_top_level_pairs() {
        let open = sem::Delimiter::String("/*blank*/".to_string());
        let close = sem::Delimiter::String("/*end*/".to_string());
        let content = "a\n/*blank*/\ninner\n/*blank*/\nmore\n/*end*/\n/*end*/\nz\n";
        let pairs =
            scan_pair_matches(content, &open, &close, Path::new("x.rs"), "blank")
                .unwrap();
        let replacements = pairs
            .into_iter()
            .filter(|pair| pair.depth_after_close == 0)
            .map(|pair| Replacement {
                start: pair.open.start,
                end: pair.close.end,
                text: String::new(),
            })
            .collect();
        let output = apply_replacements(content, replacements);
        assert_eq!(output, "a\n\nz\n");
    }

    #[test]
    fn anchor_scan_finds_each_occurrence() {
        let anchor = sem::Delimiter::String("/*anchor*/".to_string());
        let occurrences = scan_anchor_matches("x /*anchor*/ y /*anchor*/", &anchor);
        assert_eq!(occurrences.len(), 2);
    }
}
