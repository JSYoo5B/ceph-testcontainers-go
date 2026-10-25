# Messenger declaration and native-case evidence join

This is the first executable increment of #3. The reference inventory has 357
Ceph declarations; this join maps two message declarations (`MSG_COMMAND` and
`MSG_COMMAND_REPLY`) to `targeted-command/osd-pg-mds` and the existing
`TestTargetedCommandRecipe`. The other 355 declarations remain explicitly
unmapped. A mapping, available recipe or passing parser is not native execution,
wire coverage or consumer implementation.

## Source schema

[testdata/messenger_evidence.json](../../internal/integration/testdata/messenger_evidence.json)
is the normative `ceph-messenger-evidence/v2` binding document. Each unique
binding has a list of `(kind, symbol)` declarations, a requirement ID, an exact
case selector, repository-relative `source`, a code-owned assertion `contract`,
parent/case receipt markers, evidence class and execution context
(roles, plane, authentication, placement and path). Multiple bindings may refer
to one declaration or requirement; numeric aliases are never merged across
namespaces. Unknown declarations, unrelated requirement symbols, missing cases,
stale markers, duplicate links/IDs and a changed Ceph reference are errors.

This increment permits **one binding per exact top-level Go test**, even when
separate binding IDs would use different declarations. Subtest/regular-expression
selectors and multiple binding records per test require a future producer/join
contract; the current producer retains one pending record. A single binding may
name several declarations only when its reviewed assertion contract explicitly
supports every edge. Sharing a parent test PASS is insufficient.

`source` must name a canonical, nonsymlink `internal/integration/*_test.go`
file. The standard-library Go AST reader in `internal/tools/messengercases`
indexes declarations without importing or executing the inspected source.
Registration requires one unambiguous `func Test...(t *testing.T)` definition
in that file and a literal parent-marker log call in its body. Comments, unused
string literals, wrong signatures and duplicate definitions cannot supply that
registration. It conservatively rejects duplicate names even across build-tag
alternatives; source indexing does not prove reachability or execution.

The closed Python dispatch currently has only `targeted-command/v1`. It retains
the original OSD/PG/MDS generation, placement and refusal validator and permits
only the `MSG_COMMAND`/`MSG_COMMAND_REPLY` declaration edges. JSON cannot import
validators or invent a contract by naming another parent marker. New declaration
families must add a reviewed validator with their own assertions before any
native-state receipt can count. Validated reports retain the selected source and
contract alongside the case.

The binding catalogue moves from v1 to v2 with explicit source/contract fields;
old catalogues are refused rather than silently assigned the first recipe's
semantics. The runtime `ceph-messenger-case/v1` envelope and the existing test's
assertions stay unchanged. Its source digests already bind the exact catalogue
bytes, so historical receipts do not become current-source executions merely by
adding these fields.

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
file. Include every image used by the recipe: the cluster/control and daemon
containers, the already exercised client toolkit, and the separately owned
limited client. With split roles this is the essential, client and cephfs image
ID set; an aggregate image appears only once. The recipe passes its actual
executor containers to the emitter, which only inspects them and never starts
an unused client to populate metadata. Inspection failure or a missing/invalid
immutable ID prevents a successful receipt. Do not derive the expected set from
the emitted receipt or remove the client ID to make a mismatch pass.

The receipt records actual inspected image IDs and the full native
version; mismatched IDs, release, platform or pinned native build are refused.
No Squid or additional platform support is established by this first increment.

## Validation and reports

Source registration now requires Go 1.26 or later as well as Python, matching
the repository toolchain. The AST helper is this validator's own trusted source;
`--root` selects data to inspect, not Go code to run.

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
