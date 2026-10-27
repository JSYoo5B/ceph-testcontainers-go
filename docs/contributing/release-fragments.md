# Release-note fragments

A change that deserves a release note adds one file under `docs/changes/`
instead of editing the shared `Unreleased` section of
[CHANGELOG.md](../../CHANGELOG.md). Each pull request owns its own file, so two
pull requests do not conflict on release notes. This is step 1 of the plan for
issue #216.

This page describes the fragment format and the validator. Assembling
fragments into the changelog at release time is a separate step that does not
exist yet; see [Transition](#transition).

## When to add one

Add a fragment for a change that a consumer of the module would want to know
about. These are the same four classes the changelog is for:

- a breaking API or import change (with a migration link);
- user-visible behavior, including fixes;
- a compatibility or requirement change;
- a meaningful change to the supported scope.

Internal refactors, synthetic fixtures and research progress need no release
note. No check can tell yet whether a change needs a fragment, so a missing
fragment is not an error. A reviewer may ask for one. To record that a change
was considered and needs no note, add a fragment of kind `none` with a reason.

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
in `.github/scripts/test_release_fragments.py` and need no Docker.

The Code checks job `Vet, compile and CI tooling` runs the validator on every
head. See [Documentation-only changes](ci.md#documentation-only-changes) for
how a head that changes only fragments is classified.

## Transition

The fragment format is additive. The `Unreleased` section of `CHANGELOG.md`
stays editable, and a pull request that is already open may keep editing it.
Prefer a fragment for new work. The release assembler, which places the legacy
`Unreleased` block first and the fragments after it, and the later rule that
forbids new `Unreleased` entries, are separate changes.
