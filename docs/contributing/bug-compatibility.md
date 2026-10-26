# Ceph behavioral bug compatibility policy

This records the policy in [#24](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/24).
The CI planner implements explicit selection and its validation, and the
tagged runtime workflow carries one selection input through every job (see
[Bug compatibility selection](ci.md#bug-compatibility-selection)). No caller
sets that input, and no paired implementation or native compatibility result
exists yet.
The [supported-tag catalogue](../guides/bug-compatibility-tags.md) currently has
no registered tags. Existing runtime behavior is unchanged.

## Classify the difference first

| Difference | Approach |
| --- | --- |
| Feature availability | Prefer the relevant target's actual capabilities when they can be inspected. A missing feature remains unavailable. |
| Release syntax or representation | Adapt command forms/output as appropriate while preserving the supported operation. Do not turn every version branch into a bug tag. |
| Behavioral bug or fix | Use an explicitly selected bug-ID compatibility path with evidence for the defect and the ordinary/alternate behavior. |

This policy covers behavioral compatibility; security fixes are a separate
consideration. The [release/image guide](../guides/releases-and-images.md)
continues to describe existing behavior and image requirements.

A reported release is not proof of a behavioral bug or fix. A custom image can
contain a backport while reporting the same release as an affected official
image. An image-tag suffix or an assumed patch-version ordering is not a
standard declaration of its fixes. Published images are validated choices,
not an allowlist excluding custom images that meet the image requirements.
A feature probe may establish availability without distinguishing buggy and
fixed behavior. Safe behavioral probes may supply evidence, but universal
automatic detection is not a prerequisite for explicit selection.

## Selection contract for future implementations

- Use `ceph_bug_<id>` as the naming template, with a documented bug identifier.
  The template is not a supported tag or a command to copy.
- With no tag for a behavior, compile its ordinary implementation. A selected
  tag compiles only that bug's compatibility implementation. Use separately
  constrained source files with the same callable contract where appropriate.
- Keep independent bugs independently selectable and composable. One bug with
  the same compatibility behavior across releases keeps one identity; list
  affected releases in the catalogue instead of creating release-wide bundles.
- Do not override explicit build selection from a runtime release string or
  image name. Fixed custom builds use the ordinary path for the fixed bug,
  while selecting other bugs that still apply.
- The caller selects paths to match the actual image. An omitted required tag
  or an incompatible selection may fail. Do not convert that failure to a skip
  or silently try the opposite path until something passes.

| Actual target behavior | Selection for this bug | Intended path |
| --- | --- | --- |
| Unaffected official or custom image | None | Ordinary |
| Affected image | Its registered bug tag | Compatibility |
| Custom image fixes A but retains independent B | B only | Ordinary for A, compatibility for B |

These rows define intended selection, not validated image combinations.
A successful tagged run alone does not prove that its workaround was necessary.

## Preserve the operation and its evidence

Both paths must preserve the requested operation and final success criteria.
An alternate sequence or result interpretation must not suppress failures,
fabricate evidence, weaken byte/identity/cleanup assertions, or declare an
unavailable feature supported. Changing the requested semantics needs an
explicit design decision. Retain uncertain outcomes; do not replay a mutation
to obtain a passing observation.

Tests must exercise the same contract in ordinary and compatibility builds.
Splitting files must not make the affected tests disappear under one selection.
Integration-suite selectors such as `all` and opt-in `native_regression` select
test coverage; they must not implicitly enable every bug workaround.

## Scope and unresolved design

Build selection applies to the package implementation in the test binary,
not one container, cluster or call. A binary cannot select two mutually
exclusive implementations of the same behavior for different targets. A
multicluster scenario that needs both simultaneously remains a design problem;
separate executions do not prove that scenario, and this policy creates no
runtime override API.

The following must be settled for an implementation, rather than inferred from
this policy or filled with placeholder IDs:

- Authoritative tracker/identifier spelling, namespace collisions and bugs
  without assigned upstream IDs. Retirement and binding history are recorded in
  [Identity binding and retirement](#identity-binding-and-retirement), which
  leaves reactivation unresolved.
- Explicit conflict declarations and diagnostics for incompatible selections;
  do not rely on duplicate-symbol errors or undocumented precedence.
- Selection verification beyond the planner. Go can accept unused or
  misspelled custom tags, so a successful build alone does not prove a path
  was selected. The planner refuses unregistered selections and requires each
  registered tag's sources to constrain on it; it does not prove that a
  compiled source behaves differently.
- Which callers may set a selection, and for which image cohorts.
  `tagged-runtime.yml` takes it as one input, history rows record it and job
  summaries show it beside image identities, but no caller sets it yet.
  Every additional supported tag adds configurations to maintain.

## Identity binding and retirement

A published `ceph_bug_<id>` spelling is bound to one defect for as long as the
repository is maintained. The catalogue must keep that binding from being
rewritten by a later edit, so reviewers and any future check need to know where
it lives and which edits change it.

### Where the binding and its history live

- The canonical binding is the `tag` and `tracker` pair of that tag's entry in
  [`bug-compatibility-tags.json`](../guides/bug-compatibility-tags.json). The
  v1 entry permits exactly `tag`, `tracker`, `status` and `sources`, and this
  policy adds no identity or alias field to it. The tag spelling is the key; the
  tracker is the one canonical link of the defect it names.
- A retired tag keeps its entry as a tombstone, with `status` `retired` and the
  same `tag` and `tracker`. Its spelling is never deleted or reassigned.
- Alias history is not stored in the entry. A reviewed alias (for example a
  moved or redirected tracker URL for the same defect) is recorded in that
  tag's identity record on the
  [supported-tag catalogue page](../guides/bug-compatibility-tags.md), and the
  edit history of both files on the maintained branch is the immutable ledger.
  The prior link is added to the identity record in the same change that
  replaces the entry's `tracker`.
- Published release tags are never edited to record any of this. A correction
  lands on the maintained branch and ships in a new version.

### Transitions between two revisions

A candidate ledger is compared with a named maintained-base revision, never
with its own earlier snapshot. The decisions below are the policy; the
synthetic controls behind them are recorded in
[#24](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/24).

| Transition of an existing tag | Decision |
| --- | --- |
| Supported to retired, same defect | Permit. Keep `tag` and `tracker`, and remove the tag from every guarded test and source file. |
| Retired entry deleted | Refuse. The historical binding must remain. |
| Retired to supported under another defect's tracker | Refuse. This reassigns the spelling. |
| Retired to retired with another defect's tracker | Refuse. A refused selection does not by itself preserve the identity. |
| Same defect, reviewed URL alias | Permit as a link edit. The defect, and therefore the tag, is unchanged. |

### Link edit, retirement and reactivation

- A link edit changes only `tracker` (and the identity record's alias list)
  while `tag`, `status` and the defect stay the same. The reviewer must show
  that the new link names the same defect in the same tracker.
- Retirement changes only `status` from `supported` to `retired`, together with
  removing its source references. It does not touch `tag` or `tracker`.
- Reactivation, a retired tag becoming `supported` again for the same defect,
  is not decided. Until it is, treat it as refused. Resolving it needs its own
  review of whether a fixed-then-regressed or re-qualified defect reuses the
  tombstone or takes a new spelling, and what evidence that requires.

### What exists today and what does not

| Check | State |
| --- | --- |
| Single-snapshot validator (`bug_registry`): exact v1 fields, one `https` tracker string, no repeated spelling | Implemented. It accepts two distinct tags with the same tracker URL, so it does not prove identity. |
| Registered-source check (`bug_registry_sources`) and the retired-source guard, which refuses a retired tag still guarding a test | Implemented. A valid retirement must also remove those source references. |
| Retired-source scan (`bug_retired_sources`): refuses a retired tag in the `//go:build` or `// +build` header of any maintained Go file, including production, inactive platform and test-helper files, and names each path | Implemented. `testdata`, `vendor` and dot or underscore directories are outside its scope, and tag words in comments or string literals are not constraints. It runs in every planner command. |
| Selection refusal of unregistered, retired, repeated and conflicting tags | Implemented. |
| Revision-to-revision comparison against a named maintained base | Proposed only. No such check exists and no workflow runs one. |
| Reporting the exact refused transition from the table above | Proposed only. |
| Deriving a defect key (tracker namespace and project plus issue ID) from `tracker` so a link edit can be told apart from reassignment mechanically | Not decided. It may need a reviewed schema change, which this document does not make. |

The production catalogue is empty, so none of these transitions has occurred.
A later enforcement change needs its own review and authorization.

## Implementation and review checklist

1. Audit existing bug-specific version assumptions separately from capability
   checks and release-format adaptations. Record evidence and affected operations
   before migrating a branch. The [audit](../design/bug-compatibility-audit.md)
   of the current source found no branch that needs a bug tag.
2. Register a concrete tag in the catalogue with its authoritative bug link,
   ordinary/alternate contract, confirmed image boundaries and selection advice.
   Do not invent an upstream issue number or register this naming template.
3. Verify no-tag and individual-tag selection, independent combinations, clear
   conflict refusal and retained tests. Record compilation separately from
   native compatibility evidence.
4. Retain native evidence for an affected image using the matching path and a
   fixed/unaffected image using the ordinary path. Exercise mismatched selections
   where reproducible and a partial-patch combination where applicable. Keep
   unverified combinations and same-binary mixed-target limits explicit.
5. Identify the selected paths alongside source SHA, actual release/build,
   immutable image IDs, platform and attempt/cleanup results. Preserve failed
   attempts; do not infer coverage from a later retry or an unrelated cohort.
6. Review the catalogue, testing instructions, release implications and
   maintenance burden together. Published release tags remain immutable.

Use the existing [testing workflow](testing.md) and
[release process](releasing.md). Future implementation and native validation
remain work under #24; this document does not close those acceptance items.
