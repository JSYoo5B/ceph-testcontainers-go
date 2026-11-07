# Ceph bug compatibility

A `ceph_bug_<id>` build tag selects a compatibility path for one behavioral
Ceph defect. The CI planner validates a selection, and the tagged runtime
workflow carries one selection input through every job (see
[Bug compatibility selection](../contributing/ci.md#bug-compatibility-selection)).
No caller sets that input. This page lists the registered tags. The policy
for adding, maintaining and retiring one is in
[Bug compatibility tags](../contributing/bug-compatibility.md); see also the
[release/image guide](releases-and-images.md).

## Registered tags

No tag is registered yet, so there is no supported tag to enable and no
ordinary/compatibility build pair exists.

The machine-readable record is
[`bug-compatibility-tags.json`](bug-compatibility-tags.json). The CI planner
reads it to validate a selection, including the `bug_compatibility` input of
the tagged runtime workflow, whose history rows and job summaries record the
selection (see
[Bug compatibility selection](../contributing/ci.md#bug-compatibility-selection)).
Each entry has exactly these fields:

| Field | Content |
| --- | --- |
| `tag` | The exact `ceph_bug_<id>` tag: lowercase letters and digits in `_`-separated words. |
| `tracker` | The authoritative `https` tracker link for the bug. |
| `status` | `supported`, or `retired` for a tag that can no longer be selected and must not guard a test. |
| `sources` | The repository Go files of its ordinary and compatibility paths. Each must have one `//go:build` line that names the tag. Required for a supported tag. |

`conflicts` lists pairs of registered tags that cannot be selected together.
A unit test keeps this section and the record in agreement: every registered tag
is listed here, and nothing else is.

The naming template is `ceph_bug_<id>`; it is not a literal tag. Existing integration tags such as `all` and
`native_regression` are test selectors, not entries in this catalogue.
