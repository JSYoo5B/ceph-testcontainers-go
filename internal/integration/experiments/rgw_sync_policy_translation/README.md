# RGW multisite sync-policy priority and user-mode authorization

Owner: RGW multisite. Kind: upstream bug reproducer (`native_regression`).
Tests: `TestMultiClusterRGWSyncTranslationFiltering` and
`TestHostNetworkMultiClusterRGWSyncTranslationFiltering`, optional batch
`native_rgw_translation`. The required test of the same name in
`internal/integration/rgw` runs the supported cases (`tag_owner_class`,
`tenant_system_user_isolation`) on the realm setup of
`harness.NewRGWSyncTranslationFixture`; this case builds the same realm
itself (`newTranslationFixture`) so that every container runs its `image`.

## Use case and native contract

Both cases use the sync-policy model of the Ceph multisite documentation
([Multisite Sync Policy](https://docs.ceph.com/en/latest/radosgw/multisite-sync-policy/)).

- `priority_tags_owner_class`: two bucket-scoped pipes select the same
  prefix and overlapping object tags with different `priority` and
  destination storage classes. Objects matching both pipes must replicate
  through the higher-priority pipe (`STANDARD_IA`), with owner translation to
  the destination bucket's owner, while unmatched tags, untagged objects and
  other prefixes stay absent. After the high-priority pipe is removed, a new
  write must use the lower-priority pipe's class (`STANDARD`) and earlier
  replicas must keep their bytes.
- `ordinary_user_denial_grant`: a user-mode pipe runs as an ordinary user
  that may write the destination but cannot read the source object. The
  object must not replicate while the source denies the read, even after the
  bucket checkpoint advances. After the source bucket policy grants
  `s3:GetObject` and `s3:GetObjectAcl`, later writes must replicate exactly.

On Tentacle 20.2.4 neither holds reliably: pipe priority between overlapping
pipes and source-side denial in user mode have no reliable effect (see the
`rgw/multisite` README). No upstream issue or fix is recorded for either.
The case exists to verify a patched RGW image.

## Requirements

- Two clusters, one OSD each, an RGW realm with zones `source` and
  `destination` (`multisite.Run`), a tiered placement with `STANDARD` and
  `STANDARD_IA` pools in both zones. Both clusters and both gateways run the
  package's `image`, one aggregate image.
- Bridge and host network variants. Host networking needs a Linux engine or
  Docker Desktop with host networking enabled.
- The test bounds itself to 30 minutes per variant; the batch timeout is 60
  minutes.

## Tier and triggers

Known-defect reproducer with a bridge and a host-network variant, optional
batch `native_rgw_translation` with a 60-minute budget; no automatic run
includes it. Its default image, Tentacle 20.2.4, reproduces both defects.
Its given and when steps are the realm, the placement, the policy import, the
bucket checkpoints and the positive controls. Each subtest prints one
[result line](../../../../docs/contributing/testing.md#experiment-result-lines),
`case=rgw_sync_policy_translation/<bridge|host>/<subtest>`, four in all. It
is an `Experiments` case without a fixed image, since no release fixes
either defect, so a dispatch has only the default run. `native_rgw_translation`
is the default choice of `Native regressions`; that workflow's `rgw_image`
input selects role images and does not change the case's `image`. The
comment is `/ci run native_rgw_translation`. The triggers, the result checks and the image injection are the shared ones
of [Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case).

## Running it

On the default image, which must print `result=REPRODUCED` in every line:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=60m \
  -run '^(TestMultiClusterRGWSyncTranslationFiltering|TestHostNetworkMultiClusterRGWSyncTranslationFiltering)$' \
  ./internal/integration/experiments/rgw_sync_policy_translation
```

On a patched build; a fixed image must print `result=NOT_REPRODUCED` for the defect it fixes. No release
fixes either defect yet.

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=60m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_sync_policy_translation_test.image=<image>" \
  -run '^TestMultiClusterRGWSyncTranslationFiltering$' \
  ./internal/integration/experiments/rgw_sync_policy_translation
```

Host networking needs a Linux engine or Docker Desktop with host networking
enabled. The test logs the image and the
clusters' Ceph versions.

## Observations and controls

- Each case waits for its own bucket policy to be imported on the destination
  before any write (`WaitRGWTranslationPolicyReady`).
- Replication evidence is exact S3 bytes on the destination, the listed
  storage class and the exact payload in the native data pool of that class.
- Exclusion evidence is `NoSuchKey` held over a window and again after the
  pipe's native bucket checkpoint is caught up, so a paused worker cannot pass.
- Positive controls: the `blue` replica, which both pipes select, the denied
  direct read and the replica after the grant. Owner translation is checked
  by a forbidden read as the source owner on the destination.

## Reading the result

- **REPRODUCED, `priority_tags_owner_class`:** the overlapping pipes do not
  resolve by priority. The `red` object, which only the higher-priority pipe
  selects, does not replicate within four minutes (20.2.4), a replica has
  another class than the winning pipe's, an object that no pipe selects
  replicates, or after the higher-priority pipe is removed a new write does
  not replicate as `STANDARD` or an earlier replica changes.
- **REPRODUCED, `ordinary_user_denial_grant`:** the object that the source
  denies the pipe's user replicates before the grant (20.2.4).
- **NOT_REPRODUCED:** the subtest's contract held. On a patched gateway this
  is the evidence the patch works for that case; compare the gateway build
  before trusting it.
- **Test failure (no result):** the realm, zones, placement or policy import
  do not complete, a bucket checkpoint is not caught up, or a positive
  control fails. The failure names the step; a setup or timing outcome is
  not a reproduction.

Native results (Linux amd64 host, Docker, aggregate image, one run):

Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…` (the default image when recorded):

| Variant | `priority_tags_owner_class` | `ordinary_user_denial_grant` | Time |
| --- | --- | --- | --- |
| bridge | `REPRODUCED`: `red` never arrives; `blue` replicates as `STANDARD`, the lower-priority pipe's class | `REPRODUCED`: the denied object replicates before the grant | 650 s |
| host | `REPRODUCED`, the same symptoms | `REPRODUCED`, the same symptom | 647 s |

No fixed image exists to run.

## Cleanup and limits

Each case removes its own sync group (also after a failure, from a child
cleanup); the multisite fixture and both clusters are terminated by their
cleanups. Realm and zone configuration are not reverted inside the clusters,
which are discarded. Account-owned buckets, IAM principals and versioned
buckets are out of scope.
