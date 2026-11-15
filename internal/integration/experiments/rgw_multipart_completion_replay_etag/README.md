# Replay the same signed multipart completion

An S3 client may retry a completed multipart upload. After the first success the client needs the same object identity in the replay's XML result. This case distinguishes the HTTP ETag header from the ETag element of CompleteMultipartUploadResult. See [Ceph Tracker 75999](https://tracker.ceph.com/issues/75999) and the [replay correction](https://github.com/ceph/ceph/commit/4ab2e9722c76ff17e827f4257db3488f02a10ac0).

## Trigger and controls

The fixture starts a single-OSD cluster and RGW with HTTPS using the selected aggregate for the essential, client and RGW roles. It uploads one known part, verifies its MD5 ETag and signs one completion request. After the first HTTP 200 with the expected XML result the case clones the same request, preserving the URL, signature, headers and body. It requires a second HTTP 200, a completion-result XML document, byte-for-byte GET readback and the expected multipart ETag on GET.

The case prints one [result line](../../../../docs/contributing/testing.md#experiment-result-lines) and names each observed symptom. REPRODUCED means the replay XML ETag was empty (including a quoted empty value), or a valid first HTTP ETag was absent or empty on replay, or both. A missing, invalid or ambiguous first header leaves the HTTP comparison unqualified and the result depends on XML alone. NOT_REPRODUCED means the XML value matched and every qualified header comparison preserved the expected ETag. An unexpected nonempty XML value or an unexpected header after a valid first header fails the test. The receipt logs the raw headers and XML for both responses. This case does not exercise an interrupted transfer or a lost TCP response.

## Tier and triggers

Known-defect reproducer, optional batch `rgw_multipart_completion_replay_etag`
with a 20-minute budget; no automatic run includes it. It is an `Experiments`
case, and its fixed image there is the 20.2.4 aggregate
(`ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all`): 19.2.5
REPRODUCED and 20.2.4 NOT_REPRODUCED. Its Docker-free header and XML verdict
tests (`TestReplayETagVerdict`, batch `code/pure`, under 1 s) run in Code
checks on every change. The triggers, the result checks and the image
injection are the shared ones of
[Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case).

## Running the case

The default uses the published 19.2.5 aggregate; the second command
compares it with the 20.2.4 aggregate:

```sh
pkg=github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_multipart_completion_replay_etag
go test -tags=all,native_regression -count=1 -v -timeout=20m \
  -run '^TestMultipartCompletionReplayETag$' "$pkg"
go test -tags=all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X ${pkg}_test.image=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all" \
  -run '^TestMultipartCompletionReplayETag$' "$pkg"
```

The case purges its owned bucket and tears down the disposable cluster. Its steps share a 12-minute deadline that starts before cluster startup. Startup (`harness.NewSingleImageCluster`) runs on the test context, so the deadline does not cut it short, but the time it takes is spent from the 12 minutes and the steps after it get what remains. In cleanup the purge of the bucket has its own 30-second deadline and the cluster's termination its own two-minute one, so cleanup is not bounded by the deadline and can take the test past 12 minutes. The outer bound is the batch's 20-minute `go test` timeout. The Squid 19.2.5 replay lost the XML ETag, while Tentacle 20.2.4 preserved it. Both releases omitted both completion HTTP ETag headers. The tagged correction is present in 20.2.3. The fixture passes through these native representations.
