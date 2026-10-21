# Messenger declaration and native-case evidence join

This is the first executable increment of #3. The reference inventory has 357
Ceph declarations; this join maps two message declarations (`MSG_COMMAND` and
`MSG_COMMAND_REPLY`) to `targeted-command/osd-pg-mds` and the existing
`TestTargetedCommandRecipe`. The other 355 declarations remain explicitly
unmapped. A mapping, available recipe or passing parser is not native execution,
wire coverage or consumer implementation.

## Source schema

[testdata/messenger_evidence.json](../../internal/integration/testdata/messenger_evidence.json)
is the normative `ceph-messenger-evidence/v1` binding document. Each unique
binding has a list of `(kind, symbol)` declarations, a requirement ID, an exact
case selector, parent/case receipt markers, evidence class and execution context
(roles, plane, authentication, placement and path). Multiple bindings may refer
to one declaration or requirement; numeric aliases are never merged across
namespaces. Unknown declarations, unrelated requirement symbols, missing cases,
stale markers, duplicate links/IDs and a changed Ceph reference are errors.

The first evidence class is `native-state`. The validator deliberately refuses
to elevate it to a wire claim. Future wire/opcode joins require their own actual
protocol observations and validators; fixture capability cannot supply them.

## Runtime receipt

`TestTargetedCommandRecipe` keeps its existing assertions and parent
`TARGETED_COMMAND_RECEIPT`. It additionally captures a `ceph-messenger-case/v1`
object with binding/case, unique run ID, cluster generation (FSID), exact parent
SHA-256, source snapshot, actual native Ceph/Go build and immutable image IDs.
Registration happens before resource creation. Its `MSGR_CASE_RECEIPT` is emitted
only by the last test cleanup callback after the existing owned cleanup; a test
or cleanup failure suppresses it. The consumer also requires exactly one test
pass and package pass, so a marker before a later failure is insufficient.

The source snapshot hashes the compiled embedded reference, requirement and
binding JSON bytes plus an aggregate of the integration Go source snapshot.
The aggregate is SHA-256 of sorted rows `relative/path`, NUL, file SHA-256 and
newline. The embedded Go files include the case and its assertion/cleanup
helpers, along with other integration files; inclusion is not a claim that
all build-tag alternatives executed. These digests bind this evidence to the
expected fixture source, independently of a changed checkout after compilation.
Associate the report with the actual CI head/attempt or retained local build
record as well; JSON validation is not cryptographic CI attestation.

The initial cohort is the manifest-pinned Ceph 20.2.4 build. Declare expected
release, platform and the sorted immutable image ID set **before** execution:

```json
{
  "release": "20.2.4",
  "platform": "linux/amd64",
  "images": ["sha256:0000000000000000000000000000000000000000000000000000000000000000"]
}
```

The zero digest above is a schema example, not an image to use. Resolve the
actual selected images through the engine and retain those IDs in the cohort
file. The receipt records actual inspected image IDs and the full native
version; mismatched IDs, release, platform or pinned native build are refused.
No Squid or additional platform support is established by this first increment.

## Validation and reports

```sh
# Source mapping only; all cases remain unexecuted.
python3 .github/scripts/messenger_evidence.py --output mapping.json

# Run the existing case on an authorized Linux runner with selected images.
go test -tags=all -count=1 -timeout=25m -json \
  -run '^TestTargetedCommandRecipe$' ./internal/integration > native-events.jsonl

python3 .github/scripts/messenger_evidence.py \
  --events native-events.jsonl --expected-cohort cohort.json \
  --require-execution --output evidence.json

python3 -m unittest discover -s .github/scripts -p test_messenger_evidence.py
```

An existing complete verbose native-binary log may be converted with
`go tool test2json -p github.com/JSYoo5B/ceph-testcontainers-go/internal/integration`;
retain the original exit status and log with that conversion. Do not rerun an
uncertain mutation merely to obtain a successful receipt.

The validator checks JSON duplicate keys, source identity, unique execution,
complete markers and SHA-256 binding, matching FSID/OSD UUIDs, PG/acting and MDS
generation, native/auth refusal outcomes, cleanup and terminal pass events.
Bound cases have an explicit pending/running/finished lifecycle. Their output
and other events must follow the unique run and precede terminal pass/skip;
pre-run receipt bytes cannot be joined to a later run/pass. Split parent and
case-output chunks are reassembled only within that running interval. A skipped
case never supplies a successful receipt. Missing, repeated, stale, truncated or
mixed evidence fails; streams over 64 MiB are refused. Unknown case receipts
are not silently counted as another case. Without execution, reports keep
`unexecuted_bindings`; unmapped declarations always stay visible. Reports
explicitly set wire and consumer implementation claims to false.

The synthetic validator tests are offline controls, not stored native receipts.
Other reference families, release/image cohorts, unsupported-case descriptions
and automatic history/report integration remain follow-up work under #3. No new
workflow or workflow per opcode is introduced.
