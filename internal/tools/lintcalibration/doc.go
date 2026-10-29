// Command lintcalibration runs the metric linters of issue #226 (cyclop, dupl,
// funlen, gocognit) through golangci-lint with a report-only configuration that
// keeps every diagnostic, and prints them grouped by source location. Its pair
// subcommand joins two such raw collections into a base-versus-head delta. It
// is a reference measurement for triage: it defines no threshold, gate or
// ranking, and nothing in CI runs it.
//
// # Usage
//
//	lintcalibration report [-dir MODULE_ROOT] [-golangci-lint PATH] [-format json|tsv] [PACKAGE_PATTERN...]
//	lintcalibration pair -base RAW.json -head RAW.json -base-ref ID -head-ref ID [-base-dir SNAPSHOT] [-head-dir SNAPSHOT] [-format json|tsv]
//
// Flags of report:
//
//   - -dir: module root to analyse (default ".").
//   - -golangci-lint: golangci-lint v2 executable (default "golangci-lint").
//   - -format: json (default) or tsv.
//   - Package patterns default to ./... .
//
// Flags of pair:
//
//   - -base, -head: raw collections (the JSON form of report).
//   - -base-ref, -head-ref: identifiers of the snapshots, for example commits.
//   - -base-dir, -head-dir: module roots the matching collections ran in. They
//     are read, never written, and their paths are not recorded.
//   - -format: json (default) or tsv.
//
// A typical pairing:
//
//	lc="go run ./internal/tools/lintcalibration"
//	$lc report -dir BASE_CHECKOUT > base.json
//	$lc report -dir HEAD_CHECKOUT > head.json
//	$lc pair -base base.json -head head.json -base-ref BASE_COMMIT -head-ref HEAD_COMMIT \
//	    -base-dir BASE_CHECKOUT -head-dir HEAD_CHECKOUT
//
// The tool writes nothing into the module; golangci-lint's JSON goes to a
// temporary file that is removed. Keep all output uncommitted.
//
// # Why a separate configuration
//
// In the golangci-lint release pinned by Code CI, the uniq-by-line filter keeps
// one issue per file and line regardless of linter, and the
// max-issues-per-linter (50) and max-same-issues (3) caps drop more. A combined
// comparison made through those defaults can hide a second metric reported at
// the same location. The embedded configuration (golangci.yml) sets
// uniq-by-line to false and both caps to 0, enables only the four metric
// linters at their own default thresholds (collection thresholds, not a project
// policy) and uses run.relative-path-mode gomod so paths stay module-relative.
// It is never read by a regular golangci-lint run, and the repository's own
// .golangci.yml is unchanged.
//
// # Report output
//
// The header records the schema, the golangci-lint version that ran, the
// version pinned by Code CI and whether they match (matches_ci_pin), a SHA-256
// of the embedded configuration, the package patterns, the enabled linters,
// the diagnostic count per linter, the number of overlapping locations and
// scope lines stating what was and was not looked at.
//
// Each TSV row is one diagnostic: file, line, column, linter, overlap,
// test_file and the message. overlap is true when more than one linter
// reported at that file and line. The JSON form groups diagnostics by
// location. Rows are sorted by file, line, linter, column and message, so the
// output does not depend on golangci-lint's order.
//
// The report refuses output it cannot trust instead of printing an empty
// report: a missing Report.Linters, an omitted top-level Issues member (null
// and [] are accepted as empty), an enabled set other than the four metric
// linters, an issue without linter, text, or a well-typed file, line and
// column, an issue from another linter, a non-zero golangci-lint exit, or
// undecodable JSON.
//
// # Pairing
//
// pair writes a focused delta and keeps the raw files as the evidence: it
// references each by SHA-256, its identifier and its diagnostic and location
// counts, and never embeds them.
//
// Provenance is checked first. Both collections must carry the same schema,
// golangci-lint version, CI-pinned version, configuration digest, patterns,
// enabled linters and scope lines; the error names each mismatching field. A
// raw file is also refused when a member the collector always writes is
// missing or wrong-typed, when its counts, overlap or test flags disagree with
// its locations, when a per-linter count is null or not a number, or when it
// has unknown members.
//
// An absent diagnostic is not a measurement: the function may be below the
// threshold, unchanged, moved, renamed, excluded by a build constraint or
// //nolint, or gone. The delta therefore never prints a value, zero, signed
// difference or improvement for a missing diagnostic. Each entry has one
// state:
//
//   - reported_both: the same function and metric is reported on both sides;
//     both messages and locations are kept.
//   - new_above_threshold: reported at head only, and the function is declared
//     at base without that diagnostic (base_declaration says where).
//   - not_reported_above_threshold: reported at base only, and the function is
//     declared at head without that diagnostic (head_declaration says where).
//     This is not a value of zero and not an improvement.
//   - moved_or_unmatched: identity cannot be established, so nothing is
//     concluded; reason says why.
//
// Identity is the package (module-relative directory plus package clause), the
// receiver type name (pointer, value and type-parameter spelling are not
// distinguished) and the function name, read with go/parser from the snapshot
// source at the reported line; the diagnostic message must name that function.
// The declaration must be unique per package, receiver and function across the
// whole snapshot, including build-tagged files. A metric is the linter plus the
// message with its numbers masked, so funlen's line and statement counts are
// separate rows and a changed value does not change the row.
//
// The reasons for moved_or_unmatched are:
//
//   - no_snapshot_source: a snapshot directory was not given.
//   - no_declaration_at_position, message_does_not_name_declaration: the
//     source does not match the collection.
//   - snapshot_file_unparsable.
//   - linter_has_no_function_identity: every dupl diagnostic, since dupl
//     reports a code block, not a declaration.
//   - ambiguous_identity: more than one declaration of the identity in the
//     originating snapshot, or two diagnostics of one metric for it.
//   - declaration_absent_in_head, declaration_absent_in_base: moved to another
//     package, renamed, removed or newly added, which the data cannot tell
//     apart.
//   - declaration_ambiguous_in_head, declaration_ambiguous_in_base.
//
// new_above_threshold and not_reported_above_threshold carry caveats when the
// declaration absent from a collection has a //nolint comment
// (nolint_in_declaration) or sits in a file with a build constraint
// (file_has_build_constraint), since neither is evaluated. Generated files are
// not indexed, as golangci-lint skips them.
//
// Entries are ordered by package, receiver, function, linter, metric, state
// and locations, independent of input order. The TSV form starts with
// "# key: value" lines for the schema, both evidence references, the shared
// provenance, the entry count per state and the scope.
//
// # What it does not measure
//
//   - No ranking, gate, summary verdict or threshold beyond the collector's
//     own, and it does not choose or calibrate thresholds.
//   - Test files are included and flagged test_file; generated files are
//     skipped. Scenario code is not otherwise separated from implementation.
//   - A diagnostic is a signal to read the code, not evidence of poor design
//     or behavior, and a moved function is not an improvement.
//   - No claim about analysis cost, CI time or agent usage.
//   - pair cannot say a function became simpler, only that the collector no
//     longer reported it. It does not follow moves across packages or renames,
//     and it reads no git history: the caller supplies both collections,
//     identifiers and source trees.
//
// # Tests
//
// The tests are Docker-free. Parsing, grouping, version, refusal and pair
// tests use a shell script as a fake golangci-lint. Controls against the real
// line filter, and TestPairOnRealBinary, need a golangci-lint binary
// (GOLANGCI_LINT, else PATH) and are skipped without one; a skip is not a
// pass. Each of the configuration settings has a mutation that makes at least
// one control fail.
package main
