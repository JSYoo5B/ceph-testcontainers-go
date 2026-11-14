# Replay the same signed multipart completion

An S3 client may retry a completed multipart upload. After the first success the client needs the same object identity in the replay's XML result. This case distinguishes the HTTP ETag header from the ETag element of CompleteMultipartUploadResult. See [Ceph Tracker 75999](https://tracker.ceph.com/issues/75999) and the [replay correction](https://github.com/ceph/ceph/commit/4ab2e9722c76ff17e827f4257db3488f02a10ac0).

## Trigger and controls

The fixture starts a single-OSD cluster and RGW with HTTPS using the selected aggregate for the essential, client and RGW roles. It uploads one known part, verifies its MD5 ETag and signs one completion request. After the first HTTP 200 with the expected XML result the case clones the same request, preserving the URL, signature, headers and body. It requires a second HTTP 200, a completion-result XML document, byte-for-byte GET readback and the expected multipart ETag on GET.

The case emits one verdict and names each observed symptom. REPRODUCED means the replay XML ETag was empty (including a quoted empty value), or a valid first HTTP ETag was absent or empty on replay, or both. A missing, invalid or ambiguous first header leaves the HTTP comparison unqualified and the result depends on XML alone. NOT_REPRODUCED means the XML value matched and every qualified header comparison preserved the expected ETag. An unexpected nonempty XML value or an unexpected header after a valid first header fails the test. The receipt logs the raw headers and XML for both responses. This case does not exercise an interrupted transfer or a lost TCP response.

## Running the case

The optional native-regression batch and Experiments choice are both `rgw_multipart_completion_replay_etag`. The default uses the published 19.2.5 aggregate. Compare it with the 20.2.4 aggregate through the workflow entry point:

```sh
python3 .github/scripts/run_experiment.py --case rgw_multipart_completion_replay_etag \
  --image ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all
```

For direct execution:

```sh
pkg=github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_multipart_completion_replay_etag
go test -tags=all,native_regression -count=1 -v -timeout=20m "$pkg"
go test -tags=all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X ${pkg}_test.image=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all" "$pkg"
```

The case bounds operations to 12 minutes, purges its owned bucket and tears down the disposable cluster. The Squid 19.2.5 replay lost the XML ETag, while Tentacle 20.2.4 preserved it. Both releases omitted both completion HTTP ETag headers. The tagged correction is present in 20.2.3. The fixture passes through these native representations.
