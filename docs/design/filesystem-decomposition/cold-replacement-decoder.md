# Cold-start and replacement native-map decoders

Component record for the remaining decoder boundaries of Step 3 in
[filesystem-decomposition.md](../filesystem-decomposition.md) (issue #313). It
follows [stopped-mds-decoder.md](stopped-mds-decoder.md) and the
one-file-per-component direction of #216. Inspected at `6396281` (main after
#316); path citations name the files of this change.

0.x: no exported name, signature, error text or catalogue row changed.

## What it is

Two strict decoders of one `fs dump --format json` document are now functions of
their arguments, each in its own file.

| Decoder | File | Compared with | Result |
| --- | --- | --- | --- |
| `decodeColdMDSMap` | `cephfs/cold_mds_decode.go` | `coldMDSMapExpectation`: configured name, reserved first-MDS name, native identity, attached data pool IDs | `error` |
| `decodeMDSReplacementMap` | `cephfs/mds_replacement_decode.go` | `replacementMDSMapExpectation`: stopped daemon name, configured name, reserved replacement name, whether the replacement daemon exists, native identity, native pool identity read for this decode | `replacementMDSMapState`: removal scope and `ready` |

Neither takes a lock, reads a `Filesystem`, roster or container, starts a command
or sees a context. Each file declares only its function and value types and
imports `errors`, `slices`, `internal/naming` (and `strconv` for the replacement
one).

### Cold start

`validateColdMDSMap` was already free of native reads: `verifyColdMDS` did the
queries and handed it the bytes. What changed is that it took the mutable
`*cephFSColdMDS` capability and dereferenced `cap.identity` and `cap.dataPools`
inside the body. It now receives the two values it uses, copied by
`verifyColdMDS`, whose first statement already refuses a nil capability or
identity. The function was renamed `decodeColdMDSMap` for symmetry with the other
two. There was no read/decode split to make here.

### Replacement

`readMDSReplacementScope` (`cephfs/mds_replacement.go`) used to run two native
queries and decode in one 107-line method. It now keeps the queries and their
error texts and passes the bytes and the expectation to the decoder:

1. `readNativePools` (error "read original MDS replacement pools"),
2. `fs dump --format json` (error "read original MDS replacement map"),
3. `decodeMDSReplacementMap`; a refusal is returned as is,
4. on success the scope and `ready` are returned together with `ctx.Err()`,
   exactly as the old last line did.

Its three callers (`addMDSReplacement` twice, `observeMDSReplacement`) and the
locks they hold did not change. No mutex was added; package `cephfs` still
declares the three mutexes of `TestMDSRosterAddsNoMutex`.

## What did not change

- Every guard moved without edit, in the same order, with the same refusal
  texts. The native identity is dereferenced at the same points, so a missing
  identity behaves as before.
- Both decoders still enter through `coldMDSJSON` (4 MiB limit, duplicate keys and
  trailing data rejected, no raw decode error returned) and still use the shared
  `cephFSMDSRowStateAccepted` list. `up:oneshot-replay` is still refused; that is
  preserved, not decided (proposal P4 of the parent record).
- The per-decoder rules that follow the state list (boot, standby and
  standby-replay positions, the six recovery states of the exact new member) were
  not merged.

## Before and after

Measured with `structmetrics delta -packages cephfs -base-ref origin/main` and
line spans from the source. Relocation is not removed complexity.

| Item | Before | After |
| --- | ---: | ---: |
| `Filesystem` exported / private methods / fields | unchanged | unchanged |
| `readMDSReplacementScope` lines | 107 | 17 |
| New `decodeMDSReplacementMap` lines | | 99 |
| `validateColdMDSMap` / `decodeColdMDSMap` lines | 73 | 73 |
| Expectation and state value types (fields) | | 3 (4 + 6 + 2) |
| Package `cephfs` owners / private methods / fields | 69 / 95 / 360 | 72 / 95 / 372 |

The replacement body grew by nine lines of wiring in total (107 to 17 + 99 + the
expectation types), and the cold decoder did not shrink. No guard was removed,
merged or reordered, so the control-flow complexity of both decoders is moved,
not reduced. No complexity tool exists in the repository, so no complexity score
is claimed. What the split buys is the boundary: both maps can be tested and
reviewed without a cluster, a container or a lock, and the replacement reader is
reduced to two queries and one call.

## Guards (Docker-free, in `cephfs/`)

- `TestMDSReplacementDecodeKeepsEveryGuardAndItsClassification`
  (`mds_replacement_decode_test.go`): 96 cases that pin the exact refusal text of
  each guard group of the replacement decoder (malformed documents, global rows,
  filesystem entries, sibling rows, up map, policy of the owned filesystem, the
  no-worker and exact-new-member states, global standbys), including
  `up:oneshot-replay`, `down:*`, `???` and the empty string.
  `TestMDSReplacementDecodeAcceptsTheExactStates` pins the 12 accepted states and
  the `ready` flag (only `up:active` is ready).
  `TestMDSReplacementDecodeReturnsTheScopeWithAContextError` and
  `TestMDSReplacementDecodeNativeReadFailuresKeepTheirContext` pin the last line
  of the reader and the two query error texts.
  They go through `readMDSReplacementScope`, and the same file passes against
  `6396281` before the split (first commit of this change), so they characterize
  the old behavior. The native pool identity comes from the same document, so the
  two pool-identity guards are not reachable that way;
  `TestMDSReplacementDecoderRefusesInvalidPoolIdentity` calls the decoder
  directly and exists only after the split.
- `TestColdMDSDecodeKeepsEveryGuardAndItsClassification`
  (`cold_mds_decode_test.go`): 67 cases pinning the cold decoder's refusal text per
  guard group, plus the settled baseline and an independent sibling with its
  standby. It also passes against `6396281` before the rename and the value
  expectation.
- `TestColdAndReplacementDecodersArePureFunctionsOfTheirArguments`: pinned
  declarations, imports and forbidden identifiers of both decoder files.
- `TestColdAndReplacementReadsHaveOneDecode`: the caller table
  (`decodeColdMDSMap` only from `verifyColdMDS`, `decodeMDSReplacementMap` only
  from `readMDSReplacementScope`, whose callers are pinned), the reader's order
  (pool read, map read, decode) and its final return with the context error.
- `TestMDSRowStateListHasOneCopy` and the per-decoder allowlist test were updated
  to the new decoder names and files; they still pin one copy of the list and
  the three callers.

Negative controls (each applied to the source, run, and reverted): dropping the
`up:boot` refusal, the laggy check, the foreign-standby check, the joinable flag
or the no-worker requirement of the replacement decoder fails the replacement
characterization; dropping the joinable flag or the no-rank requirement of the
cold decoder fails the cold characterization; dropping `ctx.Err()` from the
reader fails the reader tests; reading the map before the pools fails the
characterization; adding `up:oneshot-replay` to the list fails the list tests; a
forbidden identifier in a decoder file and a second caller of a decoder each
fail their guard.

## Not covered

- No native evidence. No Docker or native Ceph ran. The native scenarios that
  reach these decoders (cold start, `TestLastMDSReplacementTopology`,
  `TestDualStackReplacement`) run on CI for the PR head only if the planner
  selects them, and a result would apply to that head and attempt only.
- The state-list refusal of `up:oneshot-replay` is unchanged and undecided.
- Step 4 (ownership proofs), the remaining admission sites, public handles, the
  embedded `Container`, F1, F5 and F6 are untouched.
