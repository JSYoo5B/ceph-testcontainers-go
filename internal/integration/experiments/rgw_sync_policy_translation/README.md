# RGW multisite sync-policy priority and user-mode authorization

Owner: RGW multisite. Kind: upstream bug reproducer (`native_regression`).
Tests: `TestMultiClusterRGWSyncTranslationFiltering` and
`TestHostNetworkMultiClusterRGWSyncTranslationFiltering`, optional batch
`native_rgw_translation`. The required test of the same name in
`internal/integration/rgw` runs the supported cases (`tag_owner_class`,
`tenant_system_user_isolation`) on the same realm setup,
`harness.NewRGWSyncTranslationFixture`.

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
The batch is expected to fail on the published images; it exists to verify a
patched RGW image.

## Requirements

- Two clusters, one OSD each, an RGW realm with zones `source` and
  `destination` (`multisite.Run`), a tiered placement with `STANDARD` and
  `STANDARD_IA` pools in both zones. Image roles `essential`, `client` and
  `rgw`; `CEPH_TEST_RGW_IMAGE` selects the gateway image.
- Bridge and host network variants. Host networking needs a Linux engine or
  Docker Desktop with host networking enabled.
- The test bounds itself to 30 minutes per variant; the batch timeout is 60
  minutes.

## Tier and triggers

Deliberately failing reproducer on the published images, with a bridge and a
host-network variant (NATIVE_RGW_README). It never runs on a pull request, on
`main` or in periodic samples, and never gates anything.

- Manual: `gh workflow run "Native regressions" -f batch=native_rgw_translation
  -f rgw_image=<patched image>` (the default batch of that workflow).
- Comment: `/ci run native_rgw_translation`.

## Running it

```sh
CEPH_TEST_RGW_IMAGE=<patched rgw image@sha256:...> \
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=75m \
  -run '^TestMultiClusterRGWSyncTranslationFiltering$' \
  ./internal/integration/experiments/rgw_sync_policy_translation
```

An empty `rgw_image` uses the published role image; the workflow never
builds one. Exactly as CI runs the batch:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch native_rgw_translation \
  --package ./internal/integration/experiments/rgw_sync_policy_translation \
  --directory artifacts/native-rgw-translation
```

Record the RGW image digest and the gateways' `ceph --version` with the
result.

## Observations and controls

- Each case waits for its own bucket policy to be imported on the destination
  before any write (`WaitRGWTranslationPolicyReady`).
- Replication evidence is exact S3 bytes on the destination, the listed
  storage class and the exact payload in the native data pool of that class.
- Exclusion evidence is `NoSuchKey` held over a window and again after the
  pipe's native bucket checkpoint is caught up, so a paused worker cannot pass.
- Positive controls: the higher-priority replicas, the denied direct read and
  the replicas after the grant. Owner translation is checked by a forbidden
  read as the source owner on the destination.

## Reading the result

- **Pass:** both cases held. On a patched gateway this is the evidence the
  patch works for these cases.
- **Observed bug:** an object replicates through the lower-priority pipe or
  class, or a denied object replicates before the grant (20.2.4).
- **Not reproduced:** a pass on the published 20.2.4 image. Compare the
  gateway build before trusting it.
- **Unavailable prerequisite:** the realm, zones, placement or policy import
  do not complete. The failure names the setup step.
- **Condition not established:** the policy import or the bucket checkpoint
  does not complete within its window. That is a setup or timing outcome, not
  a reproduction.

## Cleanup and limits

Each case removes its own sync group (also after a failure, from a child
cleanup); the multisite fixture and both clusters are terminated by their
cleanups. Realm and zone configuration are not reverted inside the clusters,
which are discarded. Account-owned buckets, IAM principals and versioned
buckets are out of scope.
