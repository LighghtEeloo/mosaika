//! A command line tool to perform a series of transformations on a project.
//!
//! The transformations are defined in a TOML file, and applied to the files
//! in the order they are defined.

use clap::Parser;
use itertools::Itertools;
use mosaika::{semantics as sem, syntax as syn};
use rustc_hash::FxHashMap;
use std::{io::Write, path::PathBuf};

#[derive(Debug, Parser)]
struct Cli {
    /// The path to the TOML configuration file.
    #[arg()]
    proj: PathBuf,
}

fn main() {
    env_logger::init();

    let cli = Cli::parse();
    let proj =
        syn::Proj::from_file(&cli.proj).expect("failed to read project file");
    let proj_dir =
        cli.proj.parent().expect("project file is not in a directory");
    std::env::set_current_dir(proj_dir)
        .expect("failed to set current directory");
    log::info!("processing..\n{}", proj);

    let transforms = FxHashMap::from_iter(proj.transforms.into_iter().map(
        |syn::Transform { name, delimiters, action }| {
            // convert delimiters
            if delimiters.len() != 2 {
                panic!(
                    "In transform {name}, expected 2 delimiters, got {}",
                    delimiters.len()
                );
            }
            let mut delimiter_iter = delimiters.into_iter();
            let open = match delimiter_iter.next().unwrap() {
                | syn::Delimiter::String(s) => sem::Delimiter::String(s),
                | syn::Delimiter::Regex(r) => sem::Delimiter::Regex(
                    regex::Regex::new(r.regex.as_str()).expect("invalid regex"),
                ),
            };
            let close = match delimiter_iter.next().unwrap() {
                | syn::Delimiter::String(s) => sem::Delimiter::String(s),
                | syn::Delimiter::Regex(r) => sem::Delimiter::Regex(
                    regex::Regex::new(r.regex.as_str()).expect("invalid regex"),
                ),
            };
            // convert action
            let replace = {
                let mut replace = Vec::new();
                let mut buffer = String::new();
                enum State {
                    Normal,
                    Open,
                    Insertor,
                    Close,
                }
                use State::*;
                let mut state = Normal;
                for c in action.replace.chars() {
                    match state {
                        | Normal => match c {
                            | '{' => {
                                replace.push(sem::Replacer::Plain(buffer));
                                buffer = String::new();
                                state = Open;
                            }
                            | '}' => {
                                replace.push(sem::Replacer::Plain(buffer));
                                buffer = String::new();
                                state = Close;
                            }
                            | c => buffer.push(c),
                        },
                        | Open => match c {
                            | '0'..='9' => {
                                buffer.push(c);
                                state = Insertor;
                            }
                            | '{' => {
                                replace.push(sem::Replacer::Plain(
                                    "{".to_string(),
                                ));
                                assert!(buffer.is_empty());
                                state = Normal;
                            }
                            | c => {
                                panic!("expected digit, got {c} in open state")
                            }
                        },
                        | Insertor => match c {
                            | '0'..='9' => {
                                buffer.push(c);
                            }
                            | '}' => {
                                replace.push(sem::Replacer::Insertor(
                                    buffer.parse().expect("invalid insertor"),
                                ));
                                buffer.clear();
                                state = Normal;
                            }
                            | c => {
                                panic!(
                                    "expected digit, got {c} in insertor state"
                                )
                            }
                        },
                        | Close => match c {
                            | '}' => {
                                replace.push(sem::Replacer::Plain(
                                    "}".to_string(),
                                ));
                                assert!(buffer.is_empty());
                                state = Normal;
                            }
                            | c => {
                                panic!("expected `}}`, got {c} in close state")
                            }
                        },
                    }
                }
                if !buffer.is_empty() {
                    replace.push(sem::Replacer::Plain(buffer));
                }
                replace
            };
            let action = sem::Action { replace };
            (name, sem::Transform { open, close, action })
        },
    ));
    log::info!("transforms: {:?}", transforms);

    let transactions = proj
        .transactions
        .into_iter()
        .map(|syn::Transaction { arrow, transform }| {
            let syn::Arrow { src, dst, pattern } = arrow;
            let true = src.exists() else {
                return Err(sem::TransactionError::MissingSource(
                    src.to_owned(),
                ));
            };
            let mut overwrites = Vec::new();
            let mut arrows = Vec::new();
            let mut checked_arrow = |arrow: sem::Arrow| {
                if arrow.dst.exists() {
                    overwrites.push(dst.to_owned());
                }
                arrows.push(arrow);
            };
            match pattern {
                | None => {
                    checked_arrow(sem::Arrow {
                        src: src.to_owned(),
                        dst: dst.to_owned(),
                    });
                }
                | Some(patterns) => {
                    for pattern in patterns {
                        // let mut query = src
                        //     .as_os_str()
                        //     .to_str()
                        //     .expect("src is not a valid UTF-8 string")
                        //     .to_string();
                        // query += pattern.as_str();
                        let query = src
                            .join(pattern)
                            .as_os_str()
                            .to_str()
                            .expect("src is not a valid UTF-8 string")
                            .to_string();
                        for src_path in glob::glob(query.as_str())? {
                            let src_path = src_path?;
                            let diff = src_path.strip_prefix(&src)?;
                            let dst_path = dst.join(diff);
                            checked_arrow(sem::Arrow {
                                src: src_path.to_owned(),
                                dst: dst_path.to_owned(),
                            })
                        }
                    }
                }
            }
            for name in transform.iter() {
                if !transforms.contains_key(name) {
                    return Err(sem::TransactionError::UnknownTransform(
                        name.clone(),
                    ));
                }
            }
            Ok(sem::Transaction { overwrites, arrows, transform })
        })
        .collect::<Result<Vec<sem::Transaction>, sem::TransactionError>>()
        .expect("failed to collect transactions");

    log::info!("transactions: {:?}", transactions);

    // ask user to confirm overwrite
    println!("The following output paths exist and will be overwritten:");
    for transaction in transactions.iter() {
        for overwrite in transaction.overwrites.iter() {
            println!("  {}", overwrite.canonicalize().unwrap().display());
        }
    }
    print!("Force overwrite? [y/N] ",);
    std::io::stdout().flush().unwrap();
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
    if input.trim().to_lowercase() != "y" {
        println!("Overwrite rejected, exiting.");
        return;
    }

    // perform the transactions
    for sem::Transaction { overwrites: _, arrows, transform } in transactions {
        for sem::Arrow { src, dst } in arrows {
            // ensure parent of output path exists
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }

            // read the content, perform the action, and write the content back to the file.
            let mut content = match std::fs::read_to_string(src.as_path()) {
                | Ok(content) => content,
                | Err(e) => {
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

            let find_line_and_column_readable = |pos: usize| {
                let line = line_break_positions
                    .binary_search(&pos)
                    .map_or_else(|e| e, |i| i);
                let column = if line == 0 {
                    pos
                } else {
                    pos - line_break_positions[line - 1]
                };
                (line + 1, column)
            };

            // prepare semantic transforms
            let transforms = Vec::from_iter(
                transform.iter().map(|name| (name, &transforms[name])),
            );
            struct Delimited {
                /// the index of the transform in the transform list
                pub transform: usize,
                pub side: Side,
                pub len: usize,
                pub captured: Vec<String>,
            }
            #[derive(Clone, Copy)]
            enum Side {
                Open,
                Close,
            }
            impl std::fmt::Display for Side {
                fn fmt(
                    &self, f: &mut std::fmt::Formatter<'_>,
                ) -> std::fmt::Result {
                    match self {
                        | Side::Open => write!(f, "open"),
                        | Side::Close => write!(f, "close"),
                    }
                }
            }

            use std::collections::BTreeMap;
            let mut hits = BTreeMap::new();
            // for each delimiter in the transform
            for (transform, (_, sem::Transform { open, close, action: _ })) in
                transforms.iter().enumerate()
            {
                let mut find_delim =
                    |delim: &sem::Delimiter, side: Side| match delim {
                        | sem::Delimiter::String(delim) => hits.extend(
                            content.match_indices(delim).map(|(start, m)| {
                                let delimited = Delimited {
                                    transform,
                                    side,
                                    len: m.len(),
                                    captured: Vec::new(),
                                };
                                (start, delimited)
                            }),
                        ),
                        | sem::Delimiter::Regex(regex) => hits.extend(
                            regex.captures_iter(&content).map(|caps| {
                                let m = caps.get_match();
                                let start = m.start();
                                let captured = Vec::from_iter(
                                    caps.iter()
                                        .filter_map(|m| m)
                                        .map(|m| m.as_str().to_string()),
                                );
                                let delimited = Delimited {
                                    transform,
                                    side,
                                    len: m.len(),
                                    captured,
                                };
                                (start, delimited)
                            }),
                        ),
                    };
                find_delim(open, Side::Open);
                find_delim(close, Side::Close);
            }
            for (start, delimited) in hits.into_iter() {
                let (line, column) = find_line_and_column_readable(start);
                log::info!(
                    "found delimited {} in transform {} ({}-{} @ {}:{}:{})",
                    delimited.side,
                    transforms[delimited.transform].0,
                    start,
                    start + delimited.len,
                    src.canonicalize().unwrap().display(),
                    line,
                    column,
                );
                if !delimited.captured.is_empty() {
                    log::info!("captured: {:?}", delimited.captured);
                }
            }

            // // find all delimiters
            // let re = Regex::new(r#"/\*[a-z]+(:[a-zA-Z:\w\s'"]*)?\*/"#).unwrap();
            // let ordered =
            //     BTreeMap::from_iter(re.find_iter(&content).map(|m| (m.start(), m)));
            // log::info!("length: {}", ordered.len());
            // let delimiters = ordered
            //     .values()
            //     .batching(|iter| match (iter.next(), iter.next()) {
            //         (Some(open), Some(closed)) => Some((open, closed)),
            //         (Some(open), None) => {
            //             panic!(
            //                 "found open {} ({}-{} @ line {}) but no corresponding closed delimiter",
            //                 open.as_str(),
            //                 open.start(),
            //                 open.end(),
            //                 find_line_index(open.start())
            //             )
            //         }
            //         (None, Some(_)) => unreachable!(),
            //         (None, None) => None,
            //     });

            // #[derive(Debug)]
            // enum Action {
            //     Blank,
            //     Todo { message: Option<String> },
            // }

            // let actions = Vec::from_iter(delimiters.map(|(open, closed)| {
            //     log::info!(
            //         "found open {} ({}-{} @ line {})",
            //         open.as_str(),
            //         open.start(),
            //         open.end(),
            //         find_line_index(open.start())
            //     );
            //     let action =
            //         match open.as_str().trim_start_matches("/*").trim_end_matches("*/")
            //         {
            //             | "blank" => Action::Blank,
            //             | "todo" => Action::Todo { message: None },
            //             | todo_str if todo_str.starts_with("todo:") => Action::Todo {
            //                 message: Some(todo_str[5..].trim().to_string()),
            //             },
            //             | act_str => panic!(
            //                 "unknown action {} ({}-{} @ line {})",
            //                 act_str,
            //                 open.start(),
            //                 open.end(),
            //                 find_line_index(open.start())
            //             ),
            //         };
            //     log::info!(
            //         "found closed {} ({}-{} @ line {})",
            //         closed.as_str(),
            //         closed.start(),
            //         closed.end(),
            //         find_line_index(closed.start())
            //     );
            //     if closed.as_str() != "/*end*/" {
            //         panic!(
            //             "expected end delimiter, found {} ({}-{} @ line {})",
            //             closed.as_str(),
            //             closed.start(),
            //             closed.end(),
            //             find_line_index(closed.start())
            //         );
            //     }
            //     (action, (open.start(), closed.end()))
            // }));

            // log::info!("actions: {:?}", actions);
            // for (action, (start, end)) in actions.into_iter().rev() {
            //     log::trace!(
            //         "action: {:?}\n```rust\n{}\n```",
            //         action,
            //         &content[start..end]
            //     );
            //     match action {
            //         | Action::Blank => {
            //             content.replace_range(start..end, "");
            //         }
            //         | Action::Todo { message } => {
            //             let fill = format!(
            //                 r#"todo!({})"#,
            //                 message.map_or(String::new(), |c| format!("\"{}\"", c))
            //             );
            //             content.replace_range(start..end, fill.as_str());
            //         }
            //     }
            // }

            // //
            // std::fs::write(out, content).unwrap();
        }
    }

    for cmd in proj.commands {
        match cmd {
            | syn::Command::System(syn::SystemCommand { dir, cmd }) => {
                // run the command in the directory
                let dir = dir.canonicalize().unwrap();
                log::info!("running command: {cmd} in {}", dir.display());
                let output = std::process::Command::new("sh")
                    .arg("-c")
                    .arg(cmd.as_str())
                    .current_dir(&dir)
                    .output()
                    .expect("failed to run command");
                if !output.status.success() {
                    eprintln!("command failed: {cmd}");
                    eprintln!("directory: {}", dir.display());
                    eprintln!(
                        "output: {}",
                        String::from_utf8_lossy(&output.stdout)
                    );
                    eprintln!(
                        "error: {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                    std::process::exit(1);
                }
            }
        }
    }
}
