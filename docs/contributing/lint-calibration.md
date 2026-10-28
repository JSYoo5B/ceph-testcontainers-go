# Lint calibration report

`internal/tools/lintcalibration` runs the metric linters named in issue #226
(`cyclop`, `dupl`, `funlen`, `gocognit`) through golangci-lint with a
report-only configuration that keeps every diagnostic, and prints them grouped by
file and line. It is the raw-collection step that comes before a concise
base-versus-head delta; its `pair` subcommand (below) builds that delta from two
raw collections. It is a reference measurement for triage: it defines no
threshold, gate or ranking, and nothing in CI runs it.

The repository's own lint configuration (`.golangci.yml`) and the Code checks
workflow are unchanged. The calibration configuration is embedded in the tool
(`internal/tools/lintcalibration/golangci.yml`) and is never read by a regular
`golangci-lint run`.

## Why a separate configuration

Code CI pins golangci-lint 2.14.0. In that release the default issue filters
drop diagnostics before they are printed:

- `uniq-by-line` (on by default) keeps one issue per file and line and keys by
  neither linter nor message, so a second metric reported at the same location
  is dropped.
- `max-issues-per-linter` (50) and `max-same-issues` (3) cap the output.

A combined comparison of several metrics made through those defaults can hide
one of them. The embedded configuration sets `uniq-by-line: false` and both caps
to `0`, enables only the four metric linters and sets their thresholds to the
linters' own defaults (these are collection thresholds, not a project policy).
`run.relative-path-mode: gomod` makes paths relative to the module root even
though the file is written to a temporary directory.

## Command

```sh
go run ./internal/tools/lintcalibration report                    # JSON, ./...
go run ./internal/tools/lintcalibration report -format tsv ./ceph/...
go run ./internal/tools/lintcalibration report -dir <module root> -golangci-lint <path to binary>
```

The tool needs a `golangci-lint` v2 executable. It writes nothing into the
module; the JSON that golangci-lint produces goes to a temporary file that is
removed. Keep the output uncommitted.

## Reading the report

The header records the schema, the golangci-lint version that ran, the version
pinned by Code CI and whether they are equal (`matches_ci_pin`), a SHA-256 of the
embedded configuration, the package patterns, the enabled linters, the
diagnostic count per linter and the number of overlapping locations. Scope
lines state what was and was not looked at.

Each row of the TSV is one diagnostic: `file`, `line`, `column`, `linter`,
`overlap`, `test_file` and the message. `overlap` is true when more than one
linter reported at that file and line. The JSON form groups the diagnostics by
location. Rows are sorted by file, line, linter, column and message, so the output does
not depend on the order in which golangci-lint reported them.

The tool refuses output it cannot trust, instead of printing an empty report: a
missing `Report.Linters`, an omitted top-level `Issues` member (`null` and `[]`
are accepted as an empty result), an enabled set that differs from the four metric
linters (the configuration was not applied), an issue without linter, text or a
file, line and column of the right type, an issue from another linter, a
non-zero exit of golangci-lint, or JSON it cannot decode.

## Pairing two collections

```sh
lc="go run ./internal/tools/lintcalibration"
$lc report -dir <base checkout> > base.json
$lc report -dir <head checkout> > head.json
$lc pair -base base.json -head head.json -base-ref <base commit> -head-ref <head commit> \
    -base-dir <base checkout> -head-dir <head checkout> [-format tsv]
```

`pair` reads two raw collections (the JSON form of `report`) and writes a focused
delta. The raw files stay the evidence: the delta references each by SHA-256,
the identifier given with `-base-ref` / `-head-ref` and its diagnostic and
location counts, and never embeds or replaces them. Keep all of it
uncommitted. `-base-dir` / `-head-dir` are the module roots the matching
collections ran in; they are read, never written, and their paths are not
recorded in the output.

**Provenance is checked first.** The two collections must carry the same schema,
golangci-lint version, CI-pinned version, configuration digest, patterns,
enabled linters and scope lines (which include the test-file scope). Any
difference is refused, and the error names each mismatching field
(`golangci_lint_version`, `ci_pinned_version`, `config.sha256`, `patterns`,
`enabled_linters`, `scope`, `schema`). A raw file is also refused when a member
the collector always writes is missing or wrong-typed, when its counts, overlap
flags or test flags disagree with its locations, or when it has unknown members
(`null` and `[]` locations are accepted as an empty collection).

**An absent diagnostic is not a measurement.** The collector reports only above
its linter thresholds, so a function that has no diagnostic on one side may be
below the threshold, unchanged, moved, renamed, excluded by a build constraint
or `//nolint`, or gone. File and line groups do not identify a function. The
delta therefore never prints a value, a zero, a signed difference or the word
improvement for a missing diagnostic. Each entry has one state:

| State | Meaning |
| --- | --- |
| `reported_both` | The same function and metric is reported on both sides. Both messages (which carry the values) and both locations are kept. |
| `new_above_threshold` | Reported at head only, and the same function is declared at base without a diagnostic of that metric. `base_declaration` says where. |
| `not_reported_above_threshold` | Reported at base only, and the same function is declared at head without a diagnostic of that metric. `head_declaration` says where. This is not a value of zero and not an improvement. |
| `moved_or_unmatched` | Identity cannot be established, so nothing is concluded. `reason` says why. |

Identity is the package (module-relative directory plus package clause), the
receiver type name (pointer, value and type-parameter spelling are not
distinguished) and the function name. It is read with `go/parser` from the
snapshot source at the reported line, and the diagnostic message must name that
function. A metric is the linter plus the message with its numbers masked, so
funlen's line count and statement count are separate rows and a changed value
does not change the row. The reasons are `no_snapshot_source` (a snapshot
directory was not given), `no_declaration_at_position`,
`message_does_not_name_declaration` (the source does not match the collection),
`snapshot_file_unparsable`, `linter_has_no_function_identity` (every `dupl`
diagnostic: it reports a code block, not a declaration), `ambiguous_identity`
(two diagnostics of one metric for one identity, for example build-tagged
variants), `declaration_absent_in_head` / `declaration_absent_in_base` (the
function was moved to another package, renamed, removed or newly added, which
the data cannot tell apart) and `declaration_ambiguous_in_head` /
`declaration_ambiguous_in_base`.

`new_above_threshold` and `not_reported_above_threshold` carry `caveats` when the
declaration that is absent from a collection has a `//nolint` comment
(`nolint_in_declaration`) or sits in a file with a build constraint
(`file_has_build_constraint`), because neither is evaluated: for those the
absence may be suppression or exclusion rather than a lower value. Generated
files are not indexed, as golangci-lint skips them.

Entries are ordered by package, receiver, function, linter, metric, state and
then locations, so the output does not depend on the order of the diagnostics in
either input. The TSV form starts with `# key: value` lines for the schema, both
evidence references, the shared provenance, the number of entries per state and
the scope. There is no ranking, threshold, gate, summary verdict or saving claim.

## What it does not do

- `report` alone gives no base-versus-head delta; `pair` gives the states above
  and no ranking, no threshold beyond the collector's own and no gate. Nothing
  in CI runs either.
- Test files are included and flagged `test_file`; generated files are skipped
  by golangci-lint's own default. The report does not separate scenario code
  from implementation beyond that flag.
- A diagnostic is a signal to read the code. It is not evidence of a poor design
  or of behavior, and a moved function is not an improvement.
- It makes no claim about analysis cost, CI time or agent usage.
- `pair` cannot say that a function became simpler, only that the collector no
  longer reported it. It does not follow a move across packages or a rename, and
  it does not read a git history: the caller supplies the two collections, the
  identifiers and the two source trees. A snapshot directory that is not the
  tree the matching collection ran in produces `no_declaration_at_position` or
  `message_does_not_name_declaration`, not a pairing.
- It does not choose or calibrate thresholds, does not select a metric tool, and
  is not yet a concise PR report or discovery path.

## Tests

`go test ./internal/tools/lintcalibration` is Docker-free. The parsing,
grouping, version and refusal tests use a shell script as a fake golangci-lint
and run everywhere with `/bin/sh`. The controls against the real line filter
need a golangci-lint binary (`GOLANGCI_LINT`, else `PATH`) and are **skipped
without one**; a skip is not a pass. With a binary they check, on a synthetic
function that exceeds cyclop, funlen and gocognit together, that:

- the default filter keeps exactly one diagnostic at the shared line for each of
  three metric pairs, and which one depends on the pairing;
- `uniq-by-line: false` keeps both;
- the default caps drop diagnostics beyond three identical texts, and the
  calibration configuration keeps all 55 of a synthetic set;
- the report keeps all three at the shared line, keeps a single-metric function
  and a test-file function as separate non-overlapping locations, and is
  identical on a second run.

The `pair` tests (`pair_test.go`) use fake raw collections made by the collector
through a fake binary and small synthetic modules, so they need no Docker and no
real golangci-lint, except `TestPairOnRealBinary`, which is skipped without one
(a skip is not a pass). They cover a refusal for each provenance field and for
unusable raw files, the absent-versus-not-zero control (the same input without
snapshot source stays unmatched), a function moved to another package, renamed
or given another receiver, receiver normalization, ambiguous and unreliable
identity, caveats, identical output for both input orders, and the provenance
and evidence header of both output formats.

Each of the four configuration settings has a mutation that makes at least one
of these controls fail.
