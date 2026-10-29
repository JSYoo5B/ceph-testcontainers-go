# Package-split rewrite shards

The retained native producer sources under `.github/scripts/fixtures/` were
recorded before the public API moved between packages. The scenario tests that
compare a retained producer with today's source first rewrite the retained text
to the current names. The rewrites live in
`.github/scripts/fixtures/package-split/`, one shard file per package move, so
two moves never edit the same file.

## Add a shard for a move

Create `.github/scripts/fixtures/package-split/<move-id>.json` in the same pull
request as the move. Do not edit another move's shard unless the rules below
tell you to.

```json
{
 "description": "What moved and why these rewrites are exact.",
 "identifiers": {"oldpkg.OldName": "newpkg.NewName"},
 "calls": [["regular expression", "replacement"]],
 "note": "optional prose"
}
```

| Field | Required | Rule |
| --- | --- | --- |
| `description` | yes | A non-empty string naming the move. |
| `identifiers` | one of `identifiers` and `calls` | An object mapping a qualified name as written in the retained producer (`package.Name`) to its current qualified name. |
| `calls` | one of `identifiers` and `calls` | A list of `[pattern, replacement]` pairs, a Python regular expression and its replacement template, for a former method call that is not a qualified name. |
| `note` | no | A non-empty string. |

Any other field is refused, and so is an empty shard.

The move identifier is the file name without `.json`: lowercase `a-z`, `0-9`
and `-`, starting with a letter or digit, at most 64 characters. Every entry in
the directory must be exactly `<move-id>.json` and a regular file. A README, a
backup, a link, a subdirectory or a name with a trailing newline is refused and
never silently skipped, because a skipped shard would drop its rewrites.

## What is applied

1. All identifiers of all shards, longest key first and equal lengths in key
   order. A key matches only as a whole qualified name, not inside a longer name
   or after a selector.
2. All calls, after the identifiers: the shards in file-name order and each
   shard's calls in their own order.
3. `gofmt` over the whole file, in the test that reads it. Every byte that no
   shard names stays significant.

Only the file-name order of the calls is a convention. The loader accepts a set
of shards only when that order does not matter.

## What the loader refuses

| Problem | Why |
| --- | --- |
| The same identifier key in two shards, or the same call pattern | Which target or replacement wins would depend on a choice nobody reviewed. |
| A chain: a target that contains another key (including its own shard's) | Sequential rewriting would compose the two moves in an order-dependent way. |
| A call whose replacement text is matched by another shard's call | The result would depend on which call runs first. |
| Calls of different shards that rewrite the same text of a retained producer differently depending on order | The loader applies the shards' calls in every order (every ordered pair above six shards with calls) to every retained producer under `fixtures/*.source.go.txt` and requires one result. |
| Unknown or missing fields, a malformed pair, an invalid regular expression or an invalid replacement template, duplicate JSON keys, `NaN`, an empty shard, a misnamed entry | Strict parsing; every problem is reported at once. |

Order independence for calls that rewrite overlapping text without feeding each
other is checked on the retained producer sources only. Text those sources do
not contain is not covered.

## A later move touches an earlier move's name

When a name that an earlier shard already rewrites moves again, the retained
producer still spells the original name. Keep the original key and change its
target in the earlier shard, in the pull request that moves the name. That edit
is the one reviewed migration; the loader refuses the alternative of adding a
second shard whose key is the earlier target, because that is a chain.

## Run the check

```sh
python3 .github/scripts/package_split.py
python3 -m unittest discover -s .github/scripts -p 'test_package_split.py' -v
```

The first command loads and validates the layout and exits non-zero with every
problem on failure. The unit tests run in the existing Python test step of the
Code checks, so no workflow or CI configuration refers to this directory.
