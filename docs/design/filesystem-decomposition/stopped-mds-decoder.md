# Stopped-MDS native-state decoder

Component record for the decoder boundary of Step 3 in
[filesystem-decomposition.md](../filesystem-decomposition.md) (issue #313). It
follows the one-file-per-component direction of #216. Inspected at `fb2e760`
(main after #311); path citations name the files of this change.

0.x: no exported name, signature, error text or catalogue row changed.

## What it is

`decodeStoppedMDSMap` (`cephfs/stopped_mds_decode.go`) strictly decodes one
`fs dump --format json` document and compares it with a
`stoppedMDSMapExpectation`: the stopped daemon's name, the configured
filesystem name and policy (active and standby counts, standby-replay), the
native identity captured at creation, and the native pool identity read for this
decode. It returns a `stoppedMDSMapState`: the removal scope and the names of the
registered members.

It is a function of its arguments. It takes no lock, reads no `Filesystem`,
roster or container, starts no command and never sees a context. The file
declares only that function and its two value types, and imports `errors`,
`slices`, `strconv`, `strings` and `internal/naming`.

What stays on `Filesystem` (`cephfs/stopped_mds.go`):

| Function | Role | Lock |
| --- | --- | --- |
| `readStoppedMDSScope` | reads the native pools and the dump (two commands) | owner held |
| `decodeStoppedMDSScope` | builds the expectation from `fs.config` and the identity record, calls the decoder, then the verification, and returns the verified scope | owner held |
| `verifyStoppedMDSSurvivors` | checks every other owned daemon against the decoded registered set and inspects its container | owner held |

"Owner held" means the caller holds the cluster mutex, taken after `setupMu`
(`removeStoppedMDS`, `cephfs/stopped_mds.go:99,103`). The decoder adds no lock and
no mutex; package `cephfs` still declares the three mutexes of
`TestMDSRosterAddsNoMutex`.

## What did not change

- Every guard of the former `decodeStoppedMDSScope` moved without edit, in the
  same order, with the same refusal texts. The survivor verification stayed
  last: the final scope is returned only after Docker inspection and
  `ctx.Err()`, as before (R6, R7 of the parent record).
- `coldMDSJSON` is still the entry for the strict tier (4 MiB limit, duplicate
  keys and trailing data rejected, no raw decode error returned).
- The native identity is passed as a pointer and dereferenced at the same points
  as before, so a missing identity behaves as it did.
- Accepted-state list: the three copies of the twelve daemon states became one
  function, `cephFSMDSRowStateAccepted` (`cephfs/mds_native_state.go`), used by
  `validateColdMDSMap`, `decodeStoppedMDSMap` and `readMDSReplacementScope`.
  They are the `up:` names of `ceph_mds_state_name`
  (`src/common/ceph_strings.cc`, pinned commit `7f793731f1b39eb4f465e960113d2363c311b964`)
  without `up:oneshot-replay`. That refusal is preserved, not decided (proposal
  P4 of the parent record). The per-decoder rules that follow the list (which
  accepted states are refused for an assigned row, a standby row or a
  replacement) were not merged.

## Before and after

Measured with `structmetrics delta -packages cephfs -base-ref fb2e760` and line
spans from the source. Relocation is not removed complexity.

| Item | Before | After |
| --- | ---: | ---: |
| `Filesystem` exported / private methods / fields | 40 / 72 / 14 | 40 / 73 / 14 |
| `decodeStoppedMDSScope` lines | 150 | 15 |
| New `decodeStoppedMDSMap` lines | | 124 |
| New `verifyStoppedMDSSurvivors` lines | | 30 |
| Copies of the twelve-state list | 3 | 1 |
| `stoppedMDSMapExpectation` / `stoppedMDSMapState` fields | | 7 / 2 |
| Package `cephfs` private methods / fields | 93 / 348 | 94 / 357 |

`Filesystem` gains one private method (the survivor verification needs the
roster and the owner check) and no field. The decoder body is 124 lines of the
same guards; no guard was removed, merged or reordered, so the control-flow
complexity of the decoder is moved, not reduced. No complexity tool exists in
the repository, so no complexity score is claimed. What the split buys is the
boundary: the map comparison can be tested and reviewed without a cluster, a
container or a lock, and the Docker step is a separate function.

## Guards (Docker-free, in `cephfs/`)

- `TestStoppedMDSDecodeKeepsEveryGuardAndItsClassification`
  (`stopped_mds_decode_test.go`): 49 cases that pin the exact refusal text of each
  guard group, including `up:oneshot-replay`, `down:*`, `???` and the empty
  string in info rows and standby rows, malformed JSON, policy drift, laggy
  members, foreign and extra survivors and a survivor that is not running.
  `TestStoppedMDSDecodeAcceptsTheSettledBaseline` pins the accepted scope. All
  of them also pass against `fb2e760` before the split (with only the shared
  list function added), so they characterize the old behavior.
  The guard "no complete active survivor set" is not isolated by any fixture:
  the earlier policy and membership guards already refuse every map that could
  reach it. It stays as defense in depth.
- `TestMDSRowStateAllowlistIsExactlyTheTwelveNames` and
  `TestMDSRowStateAllowlistReachesEveryStrictDecoder`
  (`mds_native_state_test.go`): the list is exactly the twelve names, and each
  of the three decoders accepts the assignable ones and refuses the rest with its
  own classification.
- `TestStoppedMDSDecoderIsAPureFunctionOfItsArguments`: pinned declarations,
  imports and forbidden identifiers of the decoder file.
- `TestStoppedMDSScopeReadHasOneDecodeThenOneSurvivorVerification`: the caller
  table (`decodeStoppedMDSMap` and `verifyStoppedMDSSurvivors` only from
  `decodeStoppedMDSScope`, which only `readStoppedMDSScope` calls), decode before
  verification with the verified scope returned, and the places that inspect a
  container.
- `TestMDSRowStateListHasOneCopy`: no second copy of the state names, and the
  three callers of the shared function.
- `TestMDSRosterReadersHoldTheirLock` now pins `verifyStoppedMDSSurvivors` as an
  owner-held reader of the roster, like the functions around it.

Negative controls (each applied to a copy of the source, run, and reverted):
dropping the standby/boot refusal of the decoder, the registered-survivor check,
the laggy refusal or the joinable flag fails the characterization test; adding
`up:oneshot-replay` to the list or dropping `up:stopping` fails the list test;
accepting every state in any one decoder fails the per-decoder test; a second
list copy, a forbidden identifier in the decoder file and an unreturned verified
scope each fail their guard.

## Not covered

- No native evidence. No Docker or native Ceph ran. The native scenarios for
  this scope (`TestStoppedMDSRetirementTopology`, `TestLastMDSReplacementTopology`,
  `TestDualStackReplacement`) run on CI for the PR head only if the planner
  selects them, and a result would apply to that head and attempt only.
- The cold-start and replacement decoders were not split. `readMDSReplacementScope`
  reads native state itself, so it needs a read/decode split first.
- Step 2 (admission), Step 4 (ownership proofs), public handles, the embedded
  `Container`, F1, F5 and F6 are untouched.
