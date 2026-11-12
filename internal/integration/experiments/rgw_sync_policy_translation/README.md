# RGW multisite sync-policy priority and user-mode authorization

Owner: RGW multisite. Kind: upstream bug reproducer (`native_regression`).
Tests: `TestMultiClusterRGWSyncTranslationFiltering` and
`TestHostNetworkMultiClusterRGWSyncTranslationFiltering`, optional batch
`native_rgw_translation`. The required test of the same name in
`internal/integration/rgw` runs the supported cases (`tag_owner_class`,
`tenant_system_user_isolation`) on the same realm setup,
`harness.NewRGWSyncTranslationFixture`. The judges of both cases have
Docker-free tests in `sync_translation_fixture_test.go` (code batch `pure`).

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
Both cases are expected to reproduce on the published images; the batch
exists to verify a patched RGW image.

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

Deliberately failing reproducer on the published images (without an
expectation), with a bridge and a
host-network variant (about 8.5 minutes for the bridge variant on 20.2.4). No
automatic run includes it (the default checks of a pull request, pushes to
`main`, periodic samples), and it never gates anything. A maintainer can run
it on demand, also on a pull request's head with the comment below.

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
  --directory "artifacts/native-rgw-translation-$(date -u +%Y%m%dT%H%M%SZ)"
```

To check one case against an affected or a patched gateway, declare its
expected verdict; the run then passes only on that verdict, and the other
units only print their own:

```sh
CEPH_TEST_EXPECTED_VERDICT=rgw_sync_policy_translation/priority_tags_owner_class=REPRODUCED \
python3 .github/scripts/tag_scenarios.py optional --batch native_rgw_translation \
  --package ./internal/integration/experiments/rgw_sync_policy_translation \
  --directory "artifacts/native-rgw-translation-$(date -u +%Y%m%dT%H%M%SZ)"
```

The runner refuses an existing `--directory`, so the timestamp gives each
run, such as each release of a comparison, its own.

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

## Verdict

Each case is its own unit in each network variant, so one batch run prints
one `REPRODUCTION_VERDICT` line per unit and a row expects exactly one of
them. The bridge units are named after the subtests; the host-network units
(`TestHostNetworkMultiClusterRGWSyncTranslationFiltering`) add a `host_`
prefix: `rgw_sync_policy_translation/host_priority_tags_owner_class` and
`rgw_sync_policy_translation/host_ordinary_user_denial_grant`. The
observation also names the variant (`bridge network:` or `host network:`).
The verdicts below are the same for both variants.

- `rgw_sync_policy_translation/priority_tags_owner_class`, observation
  `priority`. **REPRODUCED:** the overlapping pipes do not resolve by
  priority: the `red` object, which only the higher-priority pipe selects,
  never arrives within four minutes (20.2.4), or a replica is listed in the
  lower-priority pipe's class. **NOT_REPRODUCED:** `blue` and `red`
  replicate with their bytes and are listed in `STANDARD_IA` after the
  pipe's bucket checkpoint. The exclusion, owner translation and fallback
  checks run after that verdict and fail the test on their own.
- `rgw_sync_policy_translation/ordinary_user_denial_grant`, observation
  `denial`. **REPRODUCED:** the destination serves the object written while
  the source denied the read, before the grant (20.2.4). **NOT_REPRODUCED:**
  it stays `NoSuchKey` for 35 seconds and again after the pipe's bucket
  checkpoint. The replication after the grant runs after that verdict and
  fails the test on its own.

**INCONCLUSIVE** names the category. Control failed: the `blue` object, which
both pipes select, does not replicate. Another fault: the destination
listing omits an arrived replica, or answers the denied object other than
`NoSuchKey` or `200`. A realm, zone, placement, policy import or bucket
checkpoint that does not complete ends the test before a verdict, with the
failing setup step in the log; that is a setup or timing outcome, not a
reproduction.

Without `CEPH_TEST_EXPECTED_VERDICT` a reproduction fails its subtest,
absence passes it and an inconclusive run fails it. With
`CEPH_TEST_EXPECTED_VERDICT=rgw_sync_policy_translation/[host_]<case>=<REPRODUCED|NOT_REPRODUCED>`
only that unit's matching verdict passes; the other units' symptoms do not
fail the run.

## Cleanup and limits

Each case removes its own sync group (also after a failure, from a child
cleanup); the multisite fixture and both clusters are terminated by their
cleanups. Realm and zone configuration are not reverted inside the clusters,
which are discarded. Account-owned buckets, IAM principals and versioned
buckets are out of scope.
