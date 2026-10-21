# Supported Ceph bug compatibility tags

No bug-ID compatibility tags are registered at this documentation checkpoint.
There is no supported tag to enable from this page. Existing release-specific
behavior has not been exhaustively classified or migrated, and no ordinary/
compatibility build pair or selection-checking tool is introduced here.

The planned naming template is `ceph_bug_<id>`. It is not a literal tag or an
implemented example. Existing integration tags such as `all` and
`native_regression` are test selectors, not entries in this catalogue.

## Required record for a future supported tag

| Field | Required content |
| --- | --- |
| Identity | Exact tag, bug ID, authoritative tracker link and affected operation. Unassigned-ID, namespace and retirement decisions must be explicit. |
| Defect evidence | Observed symptom, trigger and supporting evidence; distinguish confirmed observations from hypotheses. |
| Image boundaries | Confirmed affected/fixed versions and actual builds, immutable image IDs and platform; list unverified boundaries separately. A release string alone is insufficient. |
| Behavior | Ordinary versus tag-enabled operation, preserved final success criteria, and why the alternate path may fail on fixed/unaffected images. |
| Selection | When to enable or omit it, including custom/partially patched images, valid build/test invocations and independently selected combinations. |
| Conflicts and scope | Incompatible tags and their diagnostics, plus limitations when one binary must operate on differently patched targets. |
| Verification | No-tag/tag/composition checks and evidence that the intended path was selected; compilation and native results reported separately. |
| Execution provenance | Active compatibility selection, source/build, actual image identities, attempt and cleanup outcome. |
| Maintenance | Current support, unresolved limitations and any reviewed deprecation/retirement decision. |

Add entries only with their implementation and scoped validation. Do not list
an invented upstream ID or an unverified workaround as supported. A successful
build with an unused custom tag is not selection evidence.

See the [contributor policy](../contributing/bug-compatibility.md),
[release/image guide](releases-and-images.md) and
[#24](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/24) for the
implementation roadmap. This catalogue does not establish a workaround or
validate any new image configuration.
