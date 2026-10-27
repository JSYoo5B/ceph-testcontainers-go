# Release-note fragments

A change that deserves a release note adds one file under `docs/changes/`
instead of editing the shared `Unreleased` section of
[CHANGELOG.md](../../CHANGELOG.md). Each pull request owns its own file, so two
pull requests do not conflict on release notes. This is steps 1 and 2 of the plan for
issue #216.

This page describes the fragment format, the validator and the release
assembler. A feature pull request only adds its fragment: it does not run the
assembler and commits no aggregate. See [Release assembly](#release-assembly)
and [Transition](#transition).

## When to add one

Add a fragment for a change that a consumer of the module would want to know
about. These are the same four classes the changelog is for:

- a breaking API or import change (with a migration link);
- user-visible behavior, including fixes;
- a compatibility or requirement change;
- a meaningful change to the supported scope.

Internal refactors, synthetic fixtures and research progress need no release
note. Apart from the API catalogue case below, no check can tell whether a
change needs a fragment, so a missing fragment is not an error. A reviewer may
ask for one. To record that a change was considered and needs no note, add a
fragment of kind `none` with a reason.

One case is checked: a pull request that changes a package's API catalogue
under `docs/design/api-naming/` must add or change a fragment (kind `none`
with a reason is enough). `go run ./internal/tools/apinaming moves -base
origin/main` enforces this together with the move-record accounting described
in [Testing](testing.md#package-layout-and-the-export-inventory), and CI runs it on pull requests only.

## Format

The file is `docs/changes/<change-id>.md`: Markdown with a front matter block.

```
---
change: 216-release-note-fragments
kind: api-break | feature | behavior | compatibility | scope | none
migration: docs/guides/upgrading-example.md#anchor
supersedes: [215-older-change]
reason: why no release note is needed
---
User-facing text. State what a consumer must change or can now do.
```

| Field | Required | Rule |
| --- | --- | --- |
| `change` | yes | Equal to the file name without `.md`. |
| `kind` | yes | One of `api-break`, `feature`, `behavior`, `compatibility`, `scope`, `none`. |
| `migration` | for `api-break` | `docs/guides/<file>.md`, optionally followed by `#anchor`. The file must exist. Allowed with any kind. |
| `supersedes` | no | A list such as `[a-change, b-change]`. Each item must be a fragment that exists in the tree; no self reference, duplicate or cycle. |
| `reason` | for `none` | One line. Not allowed with any other kind. |

The text after the front matter is required for every kind except `none`.

Change identifier. The author chooses it, as `<issue>-<slug>` or
`<topic>-<slug>`: lowercase `a-z`, `0-9` and `-`, starting with a letter or
digit, at most 64 characters. An issue number alone is not unique, because
several pull requests can address one issue, and the pull request number does
not exist before the pull request is opened. Pick a name that describes the
change. Two pull requests that choose the same name produce an add/add
conflict in Git, which is the intended signal.

Front matter is parsed strictly, and not as YAML. Each line is `key: value`
with one space after the colon and a value on the same line. Unknown or
repeated keys, comments, CRLF line endings, a file that is not UTF-8 and a file
over 64 KiB are rejected. The pull request number is not stored; the release
step recovers it from history.

## Validation

`python3 .github/scripts/release_fragments.py` checks the whole directory and
prints every problem. It rejects:

- a file that is not a regular `<change-id>.md` file, which covers a symbolic
  link, a nested path, a non-Markdown file and a malformed or upper-case name;
- a missing or malformed front matter block, an unknown kind, a missing
  reason for `none`, and a missing or unresolvable migration link for `api-break`;
- a `change` that differs from the file name, and duplicate identifiers;
- a `supersedes` entry that is missing, repeated, self-referencing or cyclic.

A missing `docs/changes/` directory is valid. Git cannot hold an empty
directory, so a tree with no unreleased fragment has none. The unit tests are
in `.github/scripts/test_release_fragments.py` and
`.github/scripts/test_release_assemble.py` and need no Docker.

The Code checks job `Vet, compile and CI tooling` runs the validator on every
head. See [Documentation-only changes](ci.md#documentation-only-changes) for
how a head that changes only fragments is classified.

## Release assembly

The release preparer, not a feature pull request, runs the assembler:

```
python3 .github/scripts/release_assemble.py assemble --version v0.4.0 --date 2026-11-01 --output draft.md
```

It validates `docs/changes`, runs the released-range check below and prints one
`## v0.4.0 (2026-11-01)` section, to standard output or to a new file (it never
overwrites one). It does not edit `CHANGELOG.md`, delete a fragment or read the
clock, so the same tree gives the same bytes. The draft contains:

1. the legacy `Unreleased` block, verbatim, when it has text;
2. one subsection per kind in the fixed order `api-break` (Breaking changes),
   `feature`, `behavior`, `compatibility`, `scope`, with entries sorted by
   change identifier. An `api-break` entry ends with its migration link;
3. no text for a `kind: none` fragment or for a fragment that another fragment
   `supersedes` (directly or through a chain);
4. a marker per consumed fragment. A kept entry ends with
   `<!-- change: <id> sha256:<16 hex digits> -->` and an omitted fragment is
   listed as `<!-- omitted: <id> sha256:<16 hex digits> -->`. The digest is the
   start of the SHA-256 of the fragment file.

The assembler is a generator, not an approver. The preparer edits the draft
into a compact user-focused section, keeps every marker, replaces the legacy
`Unreleased` block with it, deletes the consumed fragments in the same release
pull request and reviews the diff. Fragment text may not hold a line that starts
with `#` or contains `<!--`, so it cannot open a section or forge a marker; the
validator enforces this on the feature pull request.

### Published range

`python3 .github/scripts/release_assemble.py check` treats every
`## vX.Y.Z (YYYY-MM-DD)` section of `CHANGELOG.md` as published. It refuses:

- a fragment in `docs/changes` whose identifier a released section already
  records, whether it is identical (not deleted after release), edited after
  release, or a re-used name for a new change; this includes the identifier of
  a `none` or superseded fragment;
- an identifier recorded twice, a malformed marker and an unrecognized
  second-level heading;
- a released section that differs from the same section in a release tag
  `vX.Y.Z` of the clone, or from a `--base REV` revision you pass. A changed,
  removed or deleted entry and a missing section all fail.

The release being prepared has no tag yet. `--candidate vX.Y.Z` names it: the
section must exist and be newer than every other section and tag, and the tag
must not exist. Only that section is exempt from the tag comparison, and the
report says `candidate: vX.Y.Z not yet tag-verified; marker format and
uniqueness checked; original fragment bytes and digests NOT verified`. The
consumed fragment files are deleted before the check, so it cannot compare the
section text or the markers with them: review the assembled draft's diff
yourself. Earlier sections are still compared with their
tags and any other untagged section still fails under `--require-tags`. The
order is: `assemble`, edit, replace the legacy block and delete the consumed
fragments, run `check --require-tags --candidate vX.Y.Z --base <previous release
commit>`, create the tag, then run `check --require-tags` without `--candidate`.

Without git, without release tags or without the base revision the comparison
cannot run. The report then says `tag comparison: SKIPPED`, or lists the
versions it did not verify, and exits successfully; that is a skip, not a pass.
`--require-tags` fails instead. CI runs the check on a shallow checkout, so
there it only checks the recorded identifiers. Run it with the tags fetched,
and with `--base` set to the previous release commit, before you publish.

Limits. A released section is protected against an earlier tag or base. The
section that a release pull request adds is protected only after it merges. If
the preparer merges two entries and drops a marker, the check cannot know the
identifier existed. `v0.2.0` and `v0.3.0` were corrected on `main` after their
tags (links to repository milestone pages were replaced); the assembler records
the SHA-256 of those two current sections as the one accepted correction and
reports them separately from sections identical to their tag. Any further edit
fails.

## Transition

The fragment format is additive. The `Unreleased` section of `CHANGELOG.md`
stays editable, and a pull request that is already open may keep editing it.
Prefer a fragment for new work. The assembler places the legacy `Unreleased`
block first and the fragments after it, and does not rewrite the legacy text.
The later rule that forbids new `Unreleased` entries is a separate change that
waits until the pull requests open at that time have merged or been rebased.
