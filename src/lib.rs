//! Public library surface for the `mosaika` configuration model.
//!
//! `syntax` contains the TOML-facing data structures.
//! `semantics` contains the normalized runtime data structures that the current
//! executable lowers into before planning and execution.

pub mod syntax;
pub mod semantics;
