// Command structmetrics reports a fixed structural measurement of named struct
// types: declared methods (split by exported-name and private) and direct
// fields per type, for chosen package directories of one source snapshot, and
// the per-type difference between a base and a head snapshot. It gives
// reviewers and agents a fixed number and a source location instead of
// recounting a large type by reading it (issue #226).
//
// It is a reference measurement for triage, not an acceptance threshold.
// Nothing in CI runs it or fails on its output, and a count cannot say whether
// a responsibility boundary is good. It parses source lexically with the
// standard library, never type-checks, builds, imports or executes the code it
// inspects, needs no Docker and uses no network. Reading a git ref uses git
// only to resolve the ref and read file contents; the working tree is not
// touched. The output is deterministic: it holds no timestamps, host details or
// local paths.
//
// # Usage
//
//	go run ./internal/tools/structmetrics measure -packages ceph,override [-ref REF | -dir DIR]
//	go run ./internal/tools/structmetrics delta -packages ceph,override -base-ref REF [-head-ref REF | -head-dir DIR]
//
// Flags:
//
//   - -packages: required; comma-separated module-relative package
//     directories such as ceph or rgw/multisite, not recursive.
//   - -format: json (default) or tsv.
//   - -repo: git repository used to resolve refs (default ".").
//   - -ref, -dir: measure reads the git ref, or else the module root in -dir
//     (default ".").
//   - -base-ref, -base-dir: the delta base, as a git ref or a module root.
//   - -head-ref, -head-dir: the delta head; the working tree "." by default.
//
// Output goes to standard output; commit nothing. A pull request description
// may quote the changed rows.
//
// # What is measured
//
// Each row is an owner: a top-level named struct type in a listed package
// directory, keyed <directory>.<Type> (the same form the API catalogue uses).
//
//   - exported_methods: methods declared on the owner whose name is exported.
//     A private owner can have them; they are then not public package API.
//   - private_methods: methods declared on the owner whose name is not
//     exported.
//   - fields: direct members of the struct. An embedded member counts once and
//     "a, b int" counts two. Nested struct types are not expanded.
//   - declarations: how many struct declarations of that name were read.
//     Above 1 means files for several build profiles were merged and the
//     counts are a sum.
//   - locations: file:line of each declaration, relative to the module root.
//
// Pointer, value, generic (single and multiple type parameter) and
// parenthesized receivers all reduce to the type name, the same normalization
// as internal/apiinventory/catalogue with the parenthesized form added.
//
// # Output
//
// The JSON measure report holds schema (ceph-structure-metrics/v1), scope,
// source (kind, ref, commit), unmeasured_packages, packages (one total per
// package: files, owners and the three counts) and owners. The JSON delta
// report holds schema (ceph-structure-metrics-delta/v1), scope, base, head,
// unmeasured_packages (package and side), summary, changed_packages and
// changed_owners. The scope block is printed in every report.
//
// A TSV report opens with "# key: value" lines and then a tab-separated table;
// skip the lines that start with "#" to parse the table. The header carries
// schema, the kind, ref and commit of each side (source.* for measure, base.*
// and head.* for delta), every scope.measure, scope.exclusion and
// scope.caveat line, and one unmeasured_package line per package without
// production files (with the side for delta). Measure adds one
// "# package: <dir> files=N owners=N exported_methods=N private_methods=N
// fields=N" line per package; delta adds the summary line. Table columns:
//
//   - measure: owner, exported_methods, private_methods, fields,
//     declarations, locations.
//   - delta: status, owner, then base_ and head_ exported, private, fields,
//     declarations and locations, then delta_exported, delta_private,
//     delta_fields, delta_declarations. Package totals are rows whose first
//     column is "package"; their declarations columns hold the owner count
//     and their locations are "-".
//
// # Reading a delta
//
// Delta lists only owners whose measured values changed, each with status
// (changed, introduced or removed), both sides, signed deltas and the
// locations on both sides. Both sides of a responsibility boundary stay in the
// report: moving methods from a large type to a private hooks type gives two
// rows, one going down and one going up. The package total is also listed for
// any package whose total changed, so a relocation does not read as an
// improvement. Further rules:
//
//   - A renamed or relocated type is one removed row plus one introduced row;
//     the tool does not guess that they are the same type.
//   - An owner with equal counts is omitted even if it moved to another file
//     or line, so a pure file split yields no row.
//   - A listed package without production Go files on one side is listed
//     under unmeasured_packages and the summary says so; it is not treated as
//     zero owners. A package missing on both sides is an error, so a mistyped
//     name cannot read as no change.
//   - With no changed owner the summary is "no measured structural changes".
//
// # What it does not measure
//
//   - Promoted methods and promoted fields of embedded members.
//   - Top-level functions, function length, cognitive or cyclomatic
//     complexity and duplication. The lintcalibration command
//     (internal/tools/lintcalibration) collects those linter diagnostics.
//   - Build profiles separately: every file is read and merged regardless of
//     build constraints. Test files, generated files, files whose name starts
//     with . or _, subdirectories, aliases, non-struct named types and struct
//     types declared inside functions are excluded.
//   - Methods whose receiver is not a counted owner, such as a method on a
//     named slice type.
//   - Whether a boundary is good, whether state and its synchronization moved
//     together, or whether behavior is equivalent. Counts do not show lock
//     ordering, ownership, lifecycle safety or native correctness.
//   - CI cost or agent usage.
//
// A count can be gamed: moving methods into package functions or another
// facade lowers a number without improving a boundary. Do not treat a lower
// number as the goal.
package main
