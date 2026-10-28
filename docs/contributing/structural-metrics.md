# Structural metrics

`internal/tools/structmetrics` counts, for named struct types, how many methods
and fields each one declares, and reports how those counts differ between two
snapshots of the source. Its purpose is to give reviewers and AI agents a fixed
number and a source location instead of recounting a large type by reading it
(issue #226).

It is a **reference measurement for triage**. It defines no threshold, target
or acceptance limit, and nothing in CI runs it or fails on its output. A count
cannot say whether a responsibility boundary is good; read the code for that.

The tool parses source with the Go standard library only. It does not build,
type-check, import or run the code it reads, needs no Docker and uses no
network. Reading a git ref uses `git` only to resolve the ref and read file
contents; the working tree is not touched.

## Commands

Measure one snapshot (the working tree by default, or a git ref):

```sh
go run ./internal/tools/structmetrics measure -packages ceph,override
go run ./internal/tools/structmetrics measure -packages ceph,override -ref origin/main
```

Compare a base with a head (head defaults to the working tree):

```sh
go run ./internal/tools/structmetrics delta -packages ceph,override -base-ref origin/main
go run ./internal/tools/structmetrics delta -packages ceph,override -base-ref <sha> -head-ref <sha>
```

`-base-dir` and `-head-dir` read a module root directory instead of a ref.
`-packages` is required: a comma-separated list of module-relative package
directories (`ceph`, `rgw/multisite`), not recursive. `-format json` (default)
or `-format tsv` selects the table form. Output goes to standard output; commit
nothing. A pull request description may quote the changed rows.

## What is measured

Each row is an **owner**: a top-level named struct type in a listed package
directory, keyed `<directory>.<Type>` (the same form the API catalogue uses).

| Column | Meaning |
| --- | --- |
| `exported_methods` | Methods declared on the owner whose name is exported. A private owner can have them; they are then not public package API. |
| `private_methods` | Methods declared on the owner whose name is not exported. |
| `fields` | Direct members of the struct. An embedded member counts once and `a, b int` counts two. Nested struct types are not expanded. |
| `declarations` | How many struct declarations of that name were read. Above 1 means files for several build profiles were merged and the counts are a sum. |
| `locations` | `file:line` of each declaration, relative to the module root. |

Pointer, value, generic (single and multiple type parameter) and parenthesized
receivers all reduce to the type name. This is the same normalization as
`internal/apiinventory/catalogue`, with the parenthesized form added.

The `measure` output also carries one total row per package. Its scope block is
printed in every report.

## Reading a delta

`delta` lists only owners whose measured values changed, each with `status`
(`changed`, `introduced` or `removed`), the base side, the head side, signed
deltas and the locations on both sides. Both sides of a responsibility
boundary stay in the report. For example, moving methods from a large type to a
private hooks type gives two rows: the large type goes down and the hooks type
goes up. Reporting only the first would read as an improvement that is really a
relocation, so the report also lists the package total for any package whose
total changed.

Rules that keep a move from reading as an improvement:

- A renamed or relocated type is one `removed` row plus one `introduced` row.
  The tool does not guess that they are the same type.
- An owner with equal counts is omitted, even if it moved to another file or
  line, so a pure file split yields no row.
- A listed package without production Go files on one side is listed under
  `unmeasured_packages` and the summary says so. It is not treated as zero
  owners. A package missing on both sides is an error, so a mistyped name cannot
  read as "no change".
- If the head snapshot has per-change API move records under
  `docs/design/api-naming/moves/`, a row whose owner name appears in a record
  (`from`, `to`, `removed` or `added`) carries that record's change identifier
  in `move_records`. `-moves-dir ""` disables this. An annotation names the
  record only; it does not show that behavior is equivalent.

With no changed owner the summary is `no measured structural changes`.

Example, the merged change that moved the OSD flag override handle from `ceph`
to `override` (base `ecb052c9`, head `6709439b`, `-packages ceph,override`):

| Owner | Exported | Private | Fields | Status |
| --- | ---: | ---: | ---: | --- |
| `ceph.Container` | 43 to 42 | 189 to 189 | 35 to 35 | changed |
| `ceph.overrideHooks` | 22 to 24 | 0 to 0 | 0 to 0 | changed |
| `ceph.OSDFlagOverrideHandle` | 1 to - | 1 to - | 3 to - | removed |
| `ceph.osdFlagOverrideState` | 0 to - | 0 to - | 4 to - | removed |
| `override.OSDFlagHandle` | - to 1 | - to 2 | - to 3 | introduced |
| `override.osdFlagState` | - to 0 | - to 0 | - to 5 | introduced |

These are the numbers from the paired-change sample in issue #226. One result
on one pair of commits is not a statement about how the tool behaves elsewhere.

## What it does not measure

- Promoted methods and promoted fields of embedded members.
- Top-level functions, function length, cognitive or cyclomatic complexity and
  duplication. Use the linters named in issue #226 for those.
- Types in build profiles separately: every file is read and merged regardless
  of build constraints. Test files, generated files, files whose name starts
  with `.` or `_`, subdirectories, aliases, non-struct named types and struct
  types declared inside functions are excluded.
- Methods whose receiver is not a counted owner (for example a method on a
  named slice type).
- Whether a boundary is good, whether state and its synchronization moved
  together, or whether behavior is equivalent. Counts do not show lock
  ordering, ownership, lifecycle safety or native correctness.
- Anything about CI cost or agent usage.

Because it is a count, it can be gamed: moving methods into package functions or
another facade lowers a number without improving a boundary. Do not treat a
lower number as the goal.

## Tests

`go test ./internal/tools/structmetrics` runs Docker-free fixtures built from
synthetic Go sources written to a temporary directory, one fixture group per
rule above, each with a negative control (for example a test file, a generated
file, an alias and a hidden file that must not be counted, and an unchanged
owner that must not be reported). The git path is covered with a temporary
repository.
