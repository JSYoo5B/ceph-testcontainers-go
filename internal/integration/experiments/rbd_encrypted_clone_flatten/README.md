# RBD encrypted clones: inherited, separately formatted and flattened

Owner: RBD. Kind: cross-build characterization of native librbd behaviour.
Test: `TestRBDEncryptedCloneFlatten`, batch `topology/rbd_encrypted_clone_flatten`.
The fixture's own contract for encrypted backups (an encrypted image travels
as its raw LUKS header and ciphertext) stays in `TestRBDClientFeatures`
(`encryption-backup-source`, `encryption-backup-restore`).

## Use case and native contract

A SUT that clones encrypted RBD images through its own librbd relies on:

- An unformatted clone of a LUKS2 parent opens with the parent secret and its
  writes stay copy-on-write.
- A clone formatted with its own secret opens only with `encryption_load2`
  and every formatted ancestor's secret; the child secret alone is refused
  with `EPERM`.
- Flatten works only while the whole chain is loaded. Afterwards the parent
  secret is refused, the child secret opens the exact bytes, the parent keeps
  its own bytes, and the clone survives the parent's removal.
- The fixture's `rbd.ExportBackup`/`RestoreBackup` of the flattened clone
  restores raw LUKS: no parent, the parent secret refused, the child secret
  opens the exact bytes, and the raw image hides the plaintext.

The Python binding's `encryption_load2` can fail with `EPERM` when the
passphrases are `str` (a freed temporary reused by an equal-length later
secret); the case passes `bytes`, so such an `EPERM` is not evidence about
encrypted clones.

No upstream defect is involved; the case records the native semantics found
when the backup recipe was qualified, and it passes on Squid 19.2.5,
Tentacle 20.2.4 and Umbrella 21.2.0.

## Requirements

- One cluster with one OSD, a replicated pool `tc-rbd-client-features` with
  namespaces `consumer` and `foreign`, and a principal limited to `consumer`
  (`harness.NewRBDClientFeatures`). Image roles `essential` and `client`;
  `CEPH_TEST_RBD_CLIENT_IMAGE` replaces the client image.
- The default network mode only; the behaviour is in librbd.
- The test bounds itself to 15 minutes; the batch timeout is 30 minutes.

## Tier and triggers

Cheap and deterministic: one run takes about 75 seconds on 20.2.4.

- Pull request: only when a file of this directory changes, or with the
  `ci:full` label (a required `topology` batch).
- Comment: `/ci run rbd_encrypted_clone_flatten [on squid|umbrella]`.
- Periodic: `rotate` can draw it on the supported releases.

## Running it

```sh
CEPH_TEST_IMAGE=quay.io/ceph/ceph:v19.2.5 \
CGO_ENABLED=0 go test -tags all -count=1 -v -timeout=30m \
  -run '^TestRBDEncryptedCloneFlatten$' \
  ./internal/integration/experiments/rbd_encrypted_clone_flatten
```

or exactly as CI runs the batch:

```sh
python3 .github/scripts/tag_scenarios.py run --category topology \
  --batch rbd_encrypted_clone_flatten \
  --package ./internal/integration/experiments/rbd_encrypted_clone_flatten \
  --directory "artifacts/rbd-encrypted-clone-flatten-$(date -u +%Y%m%dT%H%M%SZ)"
```

The runner refuses an existing `--directory`, so the timestamp gives each
run, such as each release of a comparison, its own.

## Observations and controls

Each phase prints one JSON proof bound to the cluster FSID and pool ID. Key
refusals must be `rbd.PermissionError` with `EPERM`; any other error fails the
phase. The foreign and default namespace sentinels and the pool identity are
checked at the end, so nothing outside the client namespace changed.

## Reading the result

- **Pass:** every contract above held.
- **Observed difference:** an assertion in a phase fails; the phase's stderr
  tail names the step (for example `child-secret-only-before-flatten`).
- **Not reproduced:** not applicable; there is no known bad build.
- **Unavailable prerequisite:** the client image lacks the Python bindings,
  or the pool or principal cannot be created.
- **Condition not established:** the parent cannot be formatted or protected,
  so no clone was taken.

## Cleanup and limits

Each phase removes its images; the harness terminates the cluster. LUKS1
clones, journaling, more than two formatted generations and arm64 were not
run.
