//! A command line tool to remove reference solution from a (Rust) file.
//!
//! The reference solutions are guarded by `/*<action>*/` and `/*end*/` comments.
//! The <action>s allowed are:
//! - `blank` : remove the reference solution.
//! - `todo` : replace the reference solution with a `todo!()` macro.

use clap::Parser;
use itertools::Itertools;
use std::{io::Write, path::PathBuf};

/// A command line tool to remove reference solution from a (Rust) file.
#[derive(Debug, Parser)]
struct Cli {
    /// The path to the rust source file to process.
    #[arg()]
    src: PathBuf,
    /// The path of the output file.
    #[arg(short, long)]
    output: PathBuf,
}

fn main() {
    env_logger::init();

    let cli = Cli::parse();
    let src = cli.src;
    let out = cli.output;
    log::info!(
        "Processing {} and output to {}",
        src.display(),
        out.display()
    );

    // detecting output path
    if out.exists() {
        // ask user to confirm overwrite
        print!(
            "Output path {} already exists, force overwrite? [y/N] ",
            out.canonicalize().unwrap().display()
        );
        std::io::stdout().flush().unwrap();
        let mut input = String::new();
        std::io::stdin().read_line(&mut input).unwrap();
        if input.trim().to_lowercase() != "y" {
            println!("skipping");
            return;
        }
    }
    // ensure parent of output path exists
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }

    // read the content, perform the action, and write the content back to the file.
    let mut content = match std::fs::read_to_string(src.as_path()) {
        Ok(content) => content,
        Err(e) => {
            panic!("Error reading file {}: {}", src.display(), e);
        }
    };

    // collect all line break positions
    let line_break_positions: Vec<usize> = content
        .chars()
        .enumerate()
        .filter(|(_, c)| *c == '\n')
        .map(|(i, _)| i)
        .collect();

    let find_line_index = |pos: usize| {
        line_break_positions
            .binary_search(&pos)
            .map_or_else(|e| e, |i| i) + 1
    };

    use regex::Regex;
    use std::collections::BTreeMap;
    // find all delimiters
    let re = Regex::new(r#"/\*[a-z]+(:[a-zA-Z:\w\s'"]*)?\*/"#).unwrap();
    let ordered = BTreeMap::from_iter(re.find_iter(&content).map(|m| (m.start(), m)));
    log::info!("length: {}", ordered.len());
    let delimiters = ordered
        .values()
        .batching(|iter| match (iter.next(), iter.next()) {
            (Some(open), Some(closed)) => Some((open, closed)),
            (Some(open), None) => {
                panic!(
                    "found open {} ({}-{} @ line {}) but no corresponding closed delimiter",
                    open.as_str(),
                    open.start(),
                    open.end(),
                    find_line_index(open.start())
                )
            }
            (None, Some(_)) => unreachable!(),
            (None, None) => None,
        });

    #[derive(Debug)]
    enum Action {
        Blank,
        Todo { message: Option<String> },
    }

    let actions = Vec::from_iter(delimiters.map(|(open, closed)| {
        log::info!(
            "found open {} ({}-{} @ line {})",
            open.as_str(),
            open.start(),
            open.end(),
            find_line_index(open.start())
        );
        let action = match open
            .as_str()
            .trim_start_matches("/*")
            .trim_end_matches("*/")
        {
            "blank" => Action::Blank,
            "todo" => Action::Todo { message: None },
            todo_str if todo_str.starts_with("todo:") => Action::Todo {
                message: Some(todo_str[5..].trim().to_string()),
            },
            act_str => panic!(
                "unknown action {} ({}-{} @ line {})",
                act_str,
                open.start(),
                open.end(),
                find_line_index(open.start())
            ),
        };
        log::info!(
            "found closed {} ({}-{} @ line {})",
            closed.as_str(),
            closed.start(),
            closed.end(),
            find_line_index(closed.start())
        );
        if closed.as_str() != "/*end*/" {
            panic!(
                "expected end delimiter, found {} ({}-{} @ line {})",
                closed.as_str(),
                closed.start(),
                closed.end(),
                find_line_index(closed.start())
            );
        }
        (action, (open.start(), closed.end()))
    }));

    log::info!("actions: {:?}", actions);
    for (action, (start, end)) in actions.into_iter().rev() {
        log::trace!(
            "action: {:?}\n```rust\n{}\n```",
            action,
            &content[start..end]
        );
        match action {
            Action::Blank => {
                content.replace_range(start..end, "");
            }
            Action::Todo { message } => {
                let fill = format!(
                    r#"todo!({})"#,
                    message.map_or(String::new(), |c| format!("\"{}\"", c))
                );
                content.replace_range(start..end, fill.as_str());
            }
        }
    }

    //
    std::fs::write(out, content).unwrap();
}
