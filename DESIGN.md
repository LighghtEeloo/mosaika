# Mosaika Design

## Motivation

`mosaika` exists to make one source tree yield multiple deliberate outputs
without forking the codebase or maintaining ad hoc scripts.

The core problem is simple:

- a project often contains regions that are useful in one context and unwanted
  in another
- those regions are usually small, explicit, and local
- teams still end up solving the problem with brittle copy steps, custom grep
  pipelines, or manual edits

`mosaika` is meant to provide a single declarative tool for this job. A project
author marks meaningful regions in source files, defines a small set of
transforms, and then asks `mosaika` to produce another tree or inspect the
marked regions systematically.

The larger goal is not just "text replacement". It is controlled projection:

- derive a production tree from a development tree
- remove or rewrite marked code blocks
- discover annotated regions without editing files
- leave behind a clear log of what was found and where

## Goals

The design should optimize for:

- explicitness: transforms are declared, named, and visible in config
- locality: markers live next to the code they affect
- reproducibility: the same project file should produce the same outputs
- inspectability: discovery modes should produce logs that are easy to audit
- composability: the same transaction model should support both rewrite and
  find-style workflows

Non-goals:

- full parsing of programming languages
- implicit or heuristic code transformation
- hidden project state outside the config and the source tree

## Product Overview

`mosaika` is a config-driven CLI that reads a TOML project file and performs a
series of transactions over a source tree.

Each transaction:

- chooses a source path
- may choose a destination path
- may choose a structured log file
- selects one or more named transforms
- applies each transform in a declared mode

At a high level, `mosaika` has two families of behavior:

- `replace`: rewrite output files under `dst`
- `find.*`: inspect marked regions and write findings to the transaction log

This keeps the tool centered on one model: a transaction walks files selected by
`src`, `dst`, and optional patterns, then applies named transforms with a clear
runtime effect.

## Design Principles

### Markers over heuristics

The tool should prefer explicit delimiters in source files over inference. If a
region matters, it should be marked.

### One config, multiple projections

The same project should be able to produce multiple outputs, such as:

- developer-facing source
- production-facing source
- audit logs of marked regions

### Transaction-first execution

The transaction is the unit of work. It defines:

- where inputs come from
- where outputs go
- where logs go
- which transforms are active

### Small DSL, predictable behavior

Transforms should stay simple enough to understand by inspection. The design
should favor a narrow, deterministic replacement model over a powerful but
opaque templating language.

## Configuration Model

The project file contains three top-level arrays:

```toml
[[transform]]
[[transaction]]
[[post]]
```

### Transform

A transform is a named rule that describes:

- how to recognize a marked location or block
- what to do when it is encountered

Conceptually, a transform has:

- `name`
- `mode`
- `delimiters`
- optional action data required by that mode

The current implementation already supports a `replace`-style transform with:

- two delimiters
- a `replace` template

This design extends that model with additional explicit modes.

### Transaction

A transaction maps a source path to one or more outputs and applies an ordered
list of named transforms.

Reader-facing shape:

```toml
[[transaction]]
src = "src"
dst = "../prod/src"
log = "../prod/mosaika.log"
pattern = ["**/*"]
transform = ["blank", "todo", "anchors"]
```

Fields:

- `src`: source file or directory
- `dst`: optional destination file or directory
- `log`: optional file where discovery modes write findings for this
  transaction
- `pattern`: optional glob patterns used when `src` is a directory
- `transform`: ordered list of transform names

At least one of `dst` or `log` must be present.

- `dst` only: pure rewrite transaction
- `log` only: pure discovery transaction
- `dst` and `log`: mixed transaction that both rewrites and records findings

If neither `dst` nor `log` is present, the tool should warn and treat the
transaction as invalid because it produces no artifact.

The `log` field belongs to the transaction rather than to a transform because
logging is a property of a particular run over a particular part of the tree.
The same transform should be reusable across transactions that log to different
files.

### Post

Post commands are shell commands that run after all transactions finish. They
are intended for formatting or follow-up build steps.

## Transform Modes

The design centers on three initial modes.

### `replace`

Purpose:

- rewrite a delimited region in the destination output

Shape:

- takes exactly two delimiters
- takes a replacement template

Semantics:

- find an opening delimiter
- find the corresponding closing delimiter
- replace the entire inclusive region with rendered output
- write the modified content to `dst`

This is the current mode the codebase already models.

Example:

```toml
[[transform]]
name = "todo"
mode = "replace"
delimiters = [{ regex = '/\*todo:(([^*]|\*[^/])*)\*/' }, "/*end*/"]
action = { replace = 'todo!("{0}")' }
```

### `find.block`

Purpose:

- inspect a delimited block without changing output files

Shape:

- takes exactly two delimiters
- does not require a replacement template

Semantics:

- find an opening delimiter
- find the corresponding closing delimiter
- capture the text between delimiters
- write a log entry that includes:
  - transform name
  - source file path
  - start and end position
  - the delimited text payload

This mode is for code review, auditing, content extraction, and migration work.
It treats marked regions as structured findings instead of rewrite targets.

### `find.anchor`

Purpose:

- locate a single marked position without requiring a closing delimiter

Shape:

- takes exactly one delimiter

Semantics:

- find each occurrence of the delimiter
- write a log entry that includes:
  - transform name
  - source file path
  - position of the anchor
  - optionally the matched text if the delimiter is regex-based

This mode is useful for TODO markers, insertion points, migration anchors, or
other cases where a single annotated location matters more than a full block.

## Delimiters

Delimiters may be either:

- literal strings
- regexes

Rules by mode:

- `replace` requires exactly two delimiters
- `find.block` requires exactly two delimiters
- `find.anchor` requires exactly one delimiter

Regex delimiters may expose capture groups. For `replace`, captures can feed the
replacement template. For `find.*`, captures may be included in log output.

## Replacement Template DSL

The current replacement DSL is intentionally small:

- plain text is emitted verbatim
- `{0}`, `{1}`, ... insert capture groups by index
- `{{` emits `{`
- `}}` emits `}`

This remains appropriate for `replace`. The design does not need a larger
templating language yet.

## Transaction Execution Model

Each transaction executes in five conceptual stages.

### 1. Planning

The engine resolves:

- `src`
- optional `dst`
- optional `log`
- optional glob patterns
- the ordered transform list

Directory transactions expand into concrete file-to-file arrows while preserving
relative paths from `src` under `dst`.

### 2. Validation

Before work begins, the engine validates:

- referenced transforms exist
- each transform's mode-specific delimiter count is valid
- regexes compile
- source paths exist
- at least one of `dst` or `log` is present
- destination and log parents can be created if needed

### 3. Scan

For each concrete source file, the engine scans for delimiters used by the
active transforms.

It should collect:

- byte positions
- line and column information for logs and errors
- matched delimiter text
- regex captures where applicable

### 4. Execute by mode

For each transform occurrence:

- `replace` produces rewrite spans for the output file
- `find.block` produces log records
- `find.anchor` produces log records

The important design choice is that discovery and rewriting can happen in the
same transaction. A transaction may both modify outputs and emit findings,
provided the transform list asks for both.

### 5. Materialize outputs

After scanning:

- rewrite spans are applied to destination file content
- log records are appended or written to the transaction log file
- post commands run after all transactions complete

## Logging Model

The `log` file is a first-class transaction output. It is primarily used by
`find.block` and `find.anchor`, but the design should leave room for `replace`
to emit optional trace entries later if that becomes useful.

The log should be machine-readable enough to parse later and human-readable
enough to inspect directly. A line-oriented JSON format or a clearly delimited
text format would both work; the implementation can choose one, but the record
shape should include:

- transaction identity
- transform name
- mode
- source file path
- start line and column
- end line and column for block modes
- matched delimiter text where useful
- captured text or block body where relevant

Illustrative `find.block` record:

```text
mode=find.block transform=todo file=src/main.rs start=10:5 end=14:12
body=println!("debug only");
```

Illustrative `find.anchor` record:

```text
mode=find.anchor transform=anchor file=src/lib.rs at=42:9 match=/*anchor*/
```

## Path Semantics

Relative paths in the project file should resolve relative to the project file's
directory, not the caller's shell.

This keeps the configuration portable and matches how users naturally read the
TOML file.

## Error Model

The tool should fail fast on invalid configuration and ambiguous scans.

Important errors include:

- unknown transform names
- invalid glob patterns
- missing source paths
- invalid regexes
- transaction missing both `dst` and `log`
- wrong delimiter count for a mode
- unmatched open or close delimiters in two-delimiter modes
- overlapping or colliding delimiter matches
- inability to write destination or log files

Error messages should include file paths and human-readable positions whenever
possible.

## Suggested Internal Architecture

The codebase already hints at a healthy separation between parsing and runtime
execution. The long-term structure should be:

- `syntax`: TOML-facing config types
- `semantics`: normalized runtime types
- `planner`: transaction expansion and validation
- `engine`: scanning, pairing, replacement, and find-mode emission
- `runner`: filesystem writes and post-command execution

This keeps `main.rs` small and makes each stage easier to test.

## Current Implementation Status

The current repository already contains the beginnings of this design:

- TOML parsing is implemented
- JSON Schema generation exists
- semantic lowering exists for the current replacement model
- transaction expansion exists
- overwrite confirmation exists
- delimiter scanning and collision detection are partially implemented
- post commands are implemented

The main missing piece is the final executor:

- delimiter pairing is incomplete
- replacement rendering is not fully applied to files
- destination writeback is not complete
- find modes and transaction logging do not exist yet

So the design described here is partly current state and partly the intended
next shape of the tool.

## Example Direction

The intended workflow looks like this:

1. Mark source code with explicit block or anchor delimiters.
2. Declare reusable transforms in `mosaika.toml`.
3. Define transactions that choose both output paths and log paths.
4. Run `mosaika` to derive output trees and produce inspection logs.
5. Optionally run formatting or build commands as post steps.

This gives the project one place to define both transformation policy and
inspection policy.

## Summary

`mosaika` should be understood as a declarative projection tool for source
trees. Its job is not merely to replace text, but to turn explicit markers into
repeatable outcomes: rewritten outputs when the mode is `replace`, and auditable
findings when the mode is `find.block` or `find.anchor`. The addition of a
transaction-level `log` output completes that model by making discovery a
first-class product of the run, not just a side effect of stdout.
