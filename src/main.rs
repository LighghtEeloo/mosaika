//! Command line wrapper for the `mosaika` library engine.

use clap::{ArgGroup, Parser};
use mosaika::{
    engine::{Engine, EngineError, OverwriteMode},
    semantics as sem, syntax as syn,
};
use std::{
    collections::BTreeSet,
    io::{self, Write},
    path::{Path, PathBuf},
};
use thiserror::Error;
use tracing::trace;
use tracing_subscriber::EnvFilter;

const DEFAULT_SCHEME_PATH: &str = "mosaika.toml";
const INLINE_JSON_SCHEME_SOURCE: &str = "<scheme-json>";
const EMPTY_SCHEME_SOURCE: &str = "<scheme-empty>";

#[derive(Debug, Parser)]
#[command(group(
    ArgGroup::new("scheme_source")
        .args(["scheme", "scheme_json", "scheme_empty"])
        .multiple(false)
))]
struct Cli {
    /// Path to the TOML scheme file.
    #[arg(long, value_name = "PATH")]
    scheme: Option<PathBuf>,
    /// Inline JSON for the scheme surface syntax.
    #[arg(long, value_name = "JSON")]
    scheme_json: Option<String>,
    /// Start from an empty scheme.
    #[arg(long)]
    scheme_empty: bool,
    /// Delete approved outputs without prompting.
    #[arg(long)]
    force: bool,
}

/// One CLI-selected scheme source together with its path-resolution base.
#[derive(Debug)]
struct SchemeInput {
    /// Human-readable label used in diagnostics.
    source_name: String,
    /// Base directory for resolving relative paths inside the scheme.
    base_dir: PathBuf,
    /// Concrete source kind selected by the CLI.
    source: SchemeSource,
}

/// Concrete scheme source selected by the CLI.
#[derive(Debug)]
enum SchemeSource {
    /// Load TOML from a file path.
    TomlFile { path: PathBuf },
    /// Parse inline JSON provided on the CLI.
    Json { source: String },
    /// Start from an empty scheme.
    Empty,
}

impl SchemeInput {
    /// Resolves CLI arguments into one concrete scheme source.
    fn from_cli(cli: &Cli) -> Result<Self, CliError> {
        let current_dir = std::env::current_dir().map_err(CliError::CurrentDirectory)?;

        if let Some(path) = &cli.scheme {
            return Self::from_toml_path(resolve_cli_path(&current_dir, path));
        }

        if let Some(source) = &cli.scheme_json {
            return Ok(Self {
                source_name: INLINE_JSON_SCHEME_SOURCE.to_string(),
                base_dir: current_dir,
                source: SchemeSource::Json { source: source.clone() },
            });
        }

        if cli.scheme_empty {
            return Ok(Self {
                source_name: EMPTY_SCHEME_SOURCE.to_string(),
                base_dir: current_dir,
                source: SchemeSource::Empty,
            });
        }

        Self::from_toml_path(current_dir.join(DEFAULT_SCHEME_PATH))
    }

    /// Builds a file-backed scheme input.
    fn from_toml_path(path: PathBuf) -> Result<Self, CliError> {
        let base_dir = path
            .parent()
            .ok_or_else(|| CliError::SchemeHasNoParent { path: path.clone() })?
            .to_path_buf();
        Ok(Self {
            source_name: path.display().to_string(),
            base_dir,
            source: SchemeSource::TomlFile { path },
        })
    }

    /// Returns the human-readable scheme source label used in diagnostics.
    fn source_name(&self) -> &str {
        &self.source_name
    }

    /// Returns the base directory used for relative path resolution.
    fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Loads the surface scheme from the selected source.
    fn load_proj(&self) -> Result<syn::Projection, syn::LoadError> {
        match &self.source {
            | SchemeSource::TomlFile { path } => syn::Projection::from_file(path),
            | SchemeSource::Json { source } => {
                syn::Projection::from_json_str(self.source_name(), source)
            }
            | SchemeSource::Empty => Ok(syn::Projection::empty()),
        }
    }
}

/// Errors raised by the CLI wrapper.
#[derive(Debug, Error)]
enum CliError {
    #[error("failed to determine the current directory")]
    CurrentDirectory(#[source] std::io::Error),
    #[error("scheme path {path} is not contained in a directory")]
    SchemeHasNoParent { path: PathBuf },
    #[error("scheme {scheme_source}: {source}")]
    LoadScheme {
        scheme_source: String,
        #[source]
        source: Box<syn::LoadError>,
    },
    #[error("scheme {scheme_source}: {source}")]
    Scheme {
        scheme_source: String,
        #[source]
        source: Box<sem::SchemeError>,
    },
    #[error("failed while reading overwrite confirmation")]
    Prompt(#[source] std::io::Error),
    #[error(transparent)]
    Engine(#[from] EngineError),
}

fn main() {
    init_tracing();
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), CliError> {
    let cli = Cli::parse();
    let scheme_input = SchemeInput::from_cli(&cli)?;
    let scheme = load_scheme(&scheme_input)?;
    let plan = Engine::new(scheme_input.source_name().to_string(), scheme).plan()?;

    if !confirm_overwrites(cli.force, plan.overwrite_paths())? {
        println!("Overwrite rejected, exiting.");
        return Ok(());
    }

    let overwrite_mode = if plan.overwrite_paths().is_empty() {
        OverwriteMode::RejectExisting
    } else {
        OverwriteMode::DeleteExisting
    };
    let mut stdout = io::stdout();
    let _report = plan.execute_with_stdout(overwrite_mode, &mut stdout)?;
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).with_target(false).try_init();
}

fn load_scheme(scheme_input: &SchemeInput) -> Result<sem::Scheme, CliError> {
    trace!(scheme_source = %scheme_input.source_name(), "loading scheme");
    let proj = scheme_input.load_proj().map_err(|source| CliError::LoadScheme {
        scheme_source: scheme_input.source_name().to_string(),
        source: Box::new(source),
    })?;
    let scheme = sem::Scheme::from_syntax(proj, scheme_input.base_dir()).map_err(|source| {
        CliError::Scheme {
            scheme_source: scheme_input.source_name().to_string(),
            source: Box::new(source),
        }
    })?;
    trace!(
        scheme_source = %scheme_input.source_name(),
        transform_count = scheme.transforms.len(),
        transaction_count = scheme.transactions.len(),
        post_count = scheme.posts.len(),
        "loaded scheme"
    );
    Ok(scheme)
}

fn confirm_overwrites(
    force: bool, approved_overwrites: &BTreeSet<PathBuf>,
) -> Result<bool, CliError> {
    if approved_overwrites.is_empty() || force {
        return Ok(true);
    }

    println!("The following output files already exist and will be deleted:");
    for path in approved_overwrites {
        println!("  {}", path.display());
    }
    print!("Continue? [y/N] ");
    io::stdout().flush().map_err(CliError::Prompt)?;
    let mut input = String::new();
    io::stdin().read_line(&mut input).map_err(CliError::Prompt)?;
    Ok(input.trim().eq_ignore_ascii_case("y"))
}

fn resolve_cli_path(current_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() { path.to_path_buf() } else { current_dir.join(path) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_rejects_multiple_scheme_sources() {
        let result = Cli::try_parse_from(["mosaika", "--scheme", "a.toml", "--scheme-empty"]);

        assert!(result.is_err());
    }

    #[test]
    fn cli_defaults_to_mosaika_toml() {
        let cli = Cli::try_parse_from(["mosaika"]).unwrap();
        let input = SchemeInput::from_cli(&cli).unwrap();
        let current_dir = std::env::current_dir().unwrap();
        let expected_path = current_dir.join(DEFAULT_SCHEME_PATH);

        assert_eq!(input.source_name(), expected_path.display().to_string());
        assert_eq!(input.base_dir(), current_dir.as_path());
        match &input.source {
            | SchemeSource::TomlFile { path } => {
                assert_eq!(path, &expected_path)
            }
            | other => panic!("unexpected scheme source: {other:?}"),
        }
    }

    #[test]
    fn cli_loads_inline_json_scheme() {
        let cli = Cli::try_parse_from([
            "mosaika",
            "--scheme-json",
            r#"{"transform":[],"transaction":[],"post":[]}"#,
        ])
        .unwrap();
        let input = SchemeInput::from_cli(&cli).unwrap();
        let proj = input.load_proj().unwrap();

        assert_eq!(input.source_name(), INLINE_JSON_SCHEME_SOURCE);
        assert!(proj.transforms.is_empty());
        assert!(proj.transactions.is_empty());
        assert!(proj.posts.is_empty());
    }

    #[test]
    fn cli_loads_empty_scheme() {
        let cli = Cli::try_parse_from(["mosaika", "--scheme-empty"]).unwrap();
        let input = SchemeInput::from_cli(&cli).unwrap();
        let proj = input.load_proj().unwrap();

        assert_eq!(input.source_name(), EMPTY_SCHEME_SOURCE);
        assert!(proj.transforms.is_empty());
        assert!(proj.transactions.is_empty());
        assert!(proj.posts.is_empty());
    }
}
