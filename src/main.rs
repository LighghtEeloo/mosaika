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
    let proj = syn::Proj::from_file(cli.proj).unwrap();
    log::info!("processing..\n{}", proj);

    let transforms = FxHashMap::from_iter(
        proj.transforms.into_iter().map(|t| (t.name.clone(), t)),
    );

    let transactions = proj
        .transactions
        .into_iter()
        .map(|syn::Transaction { arrow, transform }| {
            let syn::Arrow { src, dst, pattern } = arrow;
            let true = !src.exists() else {
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
                        let mut query = src
                            .as_os_str()
                            .to_str()
                            .expect("src is not a valid UTF-8 string")
                            .to_string();
                        query += pattern.as_str();
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

    // // detecting output path
    // if out.exists() {
    //     // ask user to confirm overwrite
    //     print!(
    //         "Output path {} already exists, force overwrite? [y/N] ",
    //         out.canonicalize().unwrap().display()
    //     );
    //     std::io::stdout().flush().unwrap();
    //     let mut input = String::new();
    //     std::io::stdin().read_line(&mut input).unwrap();
    //     if input.trim().to_lowercase() != "y" {
    //         println!("skipping");
    //         return;
    //     }
    // }
    // // ensure parent of output path exists
    // if let Some(parent) = out.parent() {
    //     std::fs::create_dir_all(parent).unwrap();
    // }

    // // read the content, perform the action, and write the content back to the file.
    // let mut content = match std::fs::read_to_string(src.as_path()) {
    //     | Ok(content) => content,
    //     | Err(e) => {
    //         panic!("Error reading file {}: {}", src.display(), e);
    //     }
    // };

    // // collect all line break positions
    // let line_break_positions: Vec<usize> = content
    //     .chars()
    //     .enumerate()
    //     .filter(|(_, c)| *c == '\n')
    //     .map(|(i, _)| i)
    //     .collect();

    // let find_line_index = |pos: usize| {
    //     line_break_positions.binary_search(&pos).map_or_else(|e| e, |i| i) + 1
    // };

    // use regex::Regex;
    // use std::collections::BTreeMap;
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
