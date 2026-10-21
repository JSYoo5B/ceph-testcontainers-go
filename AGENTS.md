# Agent instructions

## Release compatibility

Keep published release tags immutable. Make corrections on `main` and publish
a new version when needed. Use maintained source paths, test names and release
tags when explaining behavior.

For changes to Ceph behavioral bug compatibility, follow the
[compatibility policy](docs/contributing/bug-compatibility.md).

## Privacy review

Before publishing source, documentation, fixtures, commit messages, release notes,
pull requests, issues or GitHub discussions, search for private
execution-environment details. Review matches in context, including hidden CI
files and generated reports. These rules apply to internal code as well as
pull request and issue descriptions, reviews and comments.

- Omit actual personal hostnames, private domains and addresses, account-specific
  filesystem paths, remote-access helpers, checkout locations and private log names.
- When describing paths, use only file paths within this repository or related
  repositories. Do not expose unrelated filesystem locations or unnecessary
  arbitrary paths.
- Software versions, architecture and hardware specifications may remain when they
  explain reproducibility, compatibility or performance. Remove their association
  with an identifiable private host or access path.
- Preserve functional platform strings, synthetic test inputs, public CI runner
  information, general product requirements and public author attribution or image
  namespaces. Do not globally replace operating-system or architecture strings.
- When logs or other diagnostic information must be shown as examples, mask
  information unrelated to the point being demonstrated. Retain only the details
  needed to explain or reproduce that point.

## Validation

Use [Testing](docs/contributing/testing.md) and the Makefile for commands.
`make check` runs unit tests, race tests, vet and tagged compilation; it does not
prove Docker or native Ceph interoperability. Run substantial checks and Docker
integration on an authorized Linux runner. Use a desktop engine only for an
explicitly required desktop-specific behavior check.

For documentation-only changes, check the diff, privacy candidates and affected
links. For fixture changes, run the affected fixture tests. Match every CI claim
to its exact commit and attempt; skipped, canceled and earlier-commit jobs do not
count as passes of the current commit. Keep implementation, compilation, native
execution and downstream-consumer validation separate.

Image production belongs to `ceph-testcontainers-images`. Verify registry pulls
and Actions publication permissions separately. Module downloads do not prove
image publication or native test success.
