# Lint calibration report

`internal/tools/lintcalibration` runs the metric linters named in issue #226
(`cyclop`, `dupl`, `funlen`, `gocognit`) through golangci-lint with a
report-only configuration that keeps every diagnostic, and prints them grouped by
file and line. It is the raw-collection step that comes before a concise
base-versus-head delta. It is a reference measurement for triage: it defines no
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
location. Rows are sorted by file, line, linter and message, so the output does
not depend on the order in which golangci-lint reported them.

The tool refuses output it cannot trust, instead of printing an empty report: a
missing `Report.Linters`, an enabled set that differs from the four metric
linters (the configuration was not applied), an issue without linter, text or a
file, line and column of the right type, an issue from another linter, a
non-zero exit of golangci-lint, or JSON it cannot decode.

## What it does not do

- No base-versus-head delta, no introduced or resolved classification, no
  ranking, no threshold and no gate. Those are later units.
- Test files are included and flagged `test_file`; generated files are skipped
  by golangci-lint's own default. The report does not separate scenario code
  from implementation beyond that flag.
- A diagnostic is a signal to read the code. It is not evidence of a poor design
  or of behavior, and a moved function is not an improvement.
- It makes no claim about analysis cost, CI time or agent usage.

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

Each of the four configuration settings has a mutation that makes at least one
of these controls fail.
