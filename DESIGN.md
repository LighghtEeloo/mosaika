# Mosaika Design

## Scope

`mosaika` projects a source tree into derived artifacts by analyzing explicit
delimiter sequences in plain text files.

The projection is defined by a scheme source. The scheme declares transforms and
transactions. A transform describes one ordered sequence of delimiters and one
action. A transaction binds transforms to source files and optional output
locations.

The pipeline has five stages:

1. Parse and validate the scheme.
2. Resolve each transaction into concrete file work items and output claims.
3. Analyze every source file, derive replacement and log regions, and reject
   invalid schedules.
4. Materialize all outputs after the whole analysis succeeds.
5. Run post commands after materialization succeeds.

The pipeline is analysis-first. Source files are read before any destination or
log file is written.

The implementation has two program surfaces:

- a library engine, which plans and executes one validated scheme
- a CLI, which selects a scheme source, presents overwrite prompts, and invokes
  the library engine

## Terms

A scheme is the full declarative input to one `mosaika` run.

A transaction is one mapping from a source path to derived artifacts. The source
path is either one file or one directory. The derived artifacts are an optional
destination tree and an optional log sink.

A work item is one concrete source file selected by a transaction.

A transform is a named rule with a non-empty delimiter sequence and an action.

A delimiter is one token recognizer. A delimiter is either a literal string or a
regular expression.

A delimiter token is one concrete occurrence of a delimiter in a source file. A
token has a byte range, line and column information, matched text, and optional
capture groups.

A chain is one ordered list of delimiter tokens that satisfies a transform.

A region is the inclusive byte interval from the start of the first token in a
chain to the end of the last token in the same chain.

## Scheme

The scheme is the only configuration input.

The CLI selects the scheme source with at most one of:

- `--scheme <PATH>`, which reads TOML from a file
- `--scheme-json <JSON>`, which parses the surface scheme from inline JSON
- `--scheme-empty`, which starts from an empty scheme

If none of these options is provided, the CLI behaves as
`--scheme ./mosaika.toml`.

The scheme base directory is the directory used to resolve relative paths in
transactions and post commands.

For `--scheme <PATH>` and the default `./mosaika.toml`, the base directory is
the directory that contains that file.

For `--scheme-json <JSON>` and `--scheme-empty`, the base directory is the
current working directory.

The scheme contains three collections:

- transforms
- transactions
- post commands

The surface parser and semantic lowering belong to the library. A caller may
construct a scheme from TOML, JSON, or direct data structures and then invoke
the engine without using the CLI.

Scheme validation in stage 1 is syntactic. It checks that the file parses, that
the shapes of transforms and transactions are valid, that transform names are
unique, that replacement templates parse, that regular expressions compile,
that glob patterns compile, and that post commands provide `dir` and `cmd`
fields. It does not consult the filesystem.

## Transform Model

Each transform has:

- a unique name
- a non-empty ordered delimiter sequence
- an action

The action is one of:

- `replace`, which renders a template into the matched region
- `log`, which records the matched region in the transaction log sink

The design does not distinguish `log.block` and `log.anchor`. A one-delimiter
log transform is an anchor log. A multi-delimiter log transform is a region log.

The action does not constrain delimiter count. A transform may use one
delimiter, two delimiters, or a longer sequence.

Regular-expression delimiters may define capture groups. Replacement templates
may reference those captures. Captures are flattened in delimiter order and then
in capture order within each delimiter.

## Transaction Model

Each transaction has:

- `src`, which names one file or one directory
- optional `dst`, which names one file or one directory that mirrors `src`
- optional `log`, which is either a file path or `{ pipe = "stdout" }`
- optional `pattern`, which selects files under a directory transaction
- `transform`, which is an ordered list of transform names

Transaction order is a reporting order. It does not change matching semantics.
All matches are computed from the original source text of each work item.

If `src` is a file, the transaction is a file transaction.

If `src` is a directory, the transaction is a directory transaction.

If `dst` is present, it must have the same kind as `src`. A file source requires
a file destination. A directory source requires a directory destination.

`pattern` is rejected for file transactions.

`pattern` is required for directory transactions. Each pattern expands to a set
of source files under `src`. Each selected file yields one work item. The
relative path from `src` to the selected file is preserved under `dst`.

If a transaction provides neither `dst` nor `log`, the planner emits a warning
and the transaction becomes analysis-only.

## Post Commands

A post command is a scheme-level shell command that runs after all transactions
have been materialized.

Each post command has:

- `dir`, which names the working directory
- `cmd`, which is the command string

Post commands are not transactions. They do not participate in source-file
analysis, output claiming, or conflict checking.

## Stage 1: Scheme Validation

Stage 1 parses the scheme source and validates it without consulting the
filesystem.

This stage validates:

- the top-level scheme structure
- transform-name uniqueness
- delimiter syntax
- regular-expression compilation
- glob-pattern compilation
- replacement-template syntax
- transaction field shapes
- post-command field shapes

Stage 1 rejects malformed schemes before any source path or output path is
resolved.

## Stage 2: Transaction Resolution

Stage 2 consults the filesystem. It resolves transactions into work items and
output claims.

For a file transaction:

- `src` must exist and must be a file
- `dst`, when present, must name a file path
- `pattern` must be absent

For a directory transaction:

- `src` must exist and must be a directory
- `dst`, when present, must name a directory path
- `pattern` expands to source files only

For every selected work item:

- the source file must exist
- the destination file, when present, must not exist unless overwrite has been
  approved

For the transaction log sink:

- `{ pipe = "stdout" }` is always valid
- a file path must name a file target
- the file must not exist unless overwrite has been approved

Overwrite approval is requested once for the full set of claimed output files.
`--force` suppresses the prompt and approves all claims. Approved existing files
are scheduled for deletion or trashing before stage 4.

The planner also enforces claim uniqueness. No two transactions may claim the
same output file. A path may not be claimed once as a destination file and once
as a log file. This avoids write-order dependence.

The library engine exposes stage 2 as an explicit planning step. The plan
reports the full set of pre-existing claimed output files that would need
overwrite approval.

The CLI is responsible for interactive approval. Other callers choose the
overwrite policy directly when executing the plan.

## Stage 3: File Analysis

Stage 3 analyzes work items on source text only. It does not write outputs.

For each work item, the engine reads the source file and selects the active
transforms named by the transaction.

### Tokenization

The engine groups active delimiter positions by delimiter recognizer and
tokenizes the source file once per distinct recognizer.

Repeated identical delimiters share one concrete token stream. This applies both
within one transform and across different transforms.

Each token records:

- delimiter recognizer
- byte start and end
- line and column start and end
- matched text
- captures

All token ranges produced by distinct delimiter recognizers in one source file
must be pairwise disjoint. Reusing one recognizer in multiple transform
positions does not duplicate tokens. If two distinct tokens overlap in bytes,
the work item is rejected. Equality counts as overlap.

A delimiter that can match the empty string is invalid. Empty tokens break the
ordering relation and make sequence matching unstable.

### Sequence Matching

Let a transform have delimiter sequence `d[0] .. d[n-1]`.

For each delimiter `d[i]`, the engine already has the list of tokens recognized
by that delimiter, ordered by byte start.

A chain is valid when:

- it contains exactly `n` tokens
- token `i` was produced by delimiter `d[i]`
- token `i` ends at or before token `i + 1` starts

The engine constructs candidate chains from start tokens:

1. Enumerate every token of `d[0]` in byte order.
2. For one chosen start token of `d[0]`, choose the earliest token of `d[1]`
   whose start is at or after the end of the previous token.
3. Repeat step 2 for `d[2]` through `d[n-1]`.
4. If all steps succeed, emit one candidate chain for that start token.
5. If any step fails, that start token emits no chain.

This construction gives at most one candidate chain per start token.

Repeated identical delimiters share one concrete token stream. This allows a
sequence such as `[A, A, B]` to match three concrete tokens `A A B` without
treating the first two delimiter positions as overlapping lexical scans. The
same sharing rule applies when two transforms use the same delimiter sequence.

The engine then validates the full candidate set for one transform in one file:

- if two candidate chains share any delimiter token, reject
- if two candidate chains overlap in bytes, reject
- otherwise, the candidate chains are the transform matches for that file

This rule is deterministic and ambiguity-rejecting. It does not enumerate all
ordered subsequences and then choose one by tie-breaking.

Unmatched start tokens are ignored. They do not cause rejection by themselves.
Rejection happens only when completed candidate chains are ambiguous.

### Action Semantics

For both actions, the region of a chain is the byte interval from the start of
the first token to the end of the last token.

For `replace`:

- the region is replaced by the rendered template
- the template may reference flattened capture groups
- all replacement regions in the same source file must be pairwise disjoint
  across all replace transforms in the transaction

For `log`:

- the region is recorded in the transaction log sink
- the record includes the source path, the region bounds, the delimiter token
  bounds, the transform name, the matched delimiter texts, and the full region
  body
- regions must be pairwise disjoint within one transform

Log regions from different transforms may overlap. Replace regions and log
regions may overlap. Only replacement has a cross-transform exclusivity rule.

## Stage 4: Materialization

Stage 4 begins only after every transaction and work item has passed stage 3.

Materialization has three steps:

1. Delete or trash every approved pre-existing destination file and log file.
2. Re-check every claimed output path. If any claimed file exists at this point,
   reject the run.
3. Write all destination files and log outputs.

Directory parents may be created during stage 4. File claims apply to files, not
to parent directories.

The run is not transactional across the whole filesystem. It is, however,
analysis-complete before the first write. A failure in stage 3 leaves outputs
untouched.

The library engine executes stage 3 through stage 5 from one approved plan. The
plan-to-execute transition is explicit so callers can inspect overwrite claims
before any deletion or write occurs.

## Stage 5: Post Commands

Stage 5 runs post commands in scheme order.

Each command runs with its declared working directory, resolved relative to the
scheme base directory.

Post commands begin only after stage 4 succeeds. If materialization fails, no
post command runs.

## Sequence-Matching Examples

With delimiter sequence `[A, B, C]` and token order `A B C A B C`, the file has
two matches.

With delimiter sequence `[A, B]` and token order `A B B`, the file has one
match. The second `B` is unused.

With delimiter sequence `[A, B]` and token order `A A B`, the file is rejected.
The first `A` yields candidate chain `(A1, B1)`. The second `A` yields candidate
chain `(A2, B1)`. The candidate chains share token `B1`.

With delimiter sequence `[open, close]` and token order
`open open close close`, the file is rejected. This syntax is ambiguous under
sequence matching. Nested bracketing is a different matcher class.

With delimiter sequence `[A, A, B]` and token order `A A B`, the file has one
match. The first `A` satisfies the first position and the second `A` satisfies
the second position.

With one replace transform `[A, B]` and one log transform `[A, B]`, token order
`A B` yields one replacement and one log record. The two transforms share the
same delimiter stream.

## Sequence-Matching Consequences

The rule has three direct consequences.

First, sequence matching is not stack matching. A transform describes ordered
tokens in the source text. It does not describe nested structure.

Second, repeated leading delimiters may create ambiguity instead of silently
binding to the earliest completion. Ambiguous completed chains are rejected.

Third, a longer transform may enclose the region of a shorter replace
transform. This is rejected by replacement-region overlap checking even when the
delimiter tokens themselves are disjoint.

## Failure Model

The run rejects on any of the following conditions:

- scheme parse failure
- duplicate transform names
- invalid regular expression
- invalid replacement template
- missing source path
- mismatched file-versus-directory transaction shape
- `pattern` used on a file transaction
- missing `pattern` on a directory transaction
- output claim collision between transactions
- unapproved pre-existing destination or log file
- overlapping delimiter tokens in one source file
- empty-string delimiter match
- overlapping replacement regions in one source file
- overlapping log regions within one transform
- claimed output path occupied during stage 4

The error report names the scheme source, the transaction, the source file when
relevant, and the byte or line-column locations that triggered the rejection.

## Generate Schema for Mosaika Scheme

```bash
cargo run --features=json-schema --bin=schema > mosaika.schema.json 
```

and then in `.taplo.toml`:

```toml
[[rule]]
include = ["**/mosaika.toml"]
schema.path = "file://mosaika.schema.json"
schema.enabled = true
```

## Open Design Points

The sequence rule is fixed by this document, but three surface choices remain.

The replacement-template syntax should expose captures from multiple delimiters
without ambiguity. Flattened numeric indexing is sufficient. Named references by
delimiter and capture index would be clearer.

The log encoding should preserve region text and location data without losing
streamability. A line-oriented structured format is sufficient. The exact record
syntax is still open.

The overwrite action may delete files permanently or move them to trash. The
planner needs one explicit policy and one explicit flag for bypassing the prompt.
