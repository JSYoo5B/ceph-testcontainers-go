# Reading encrypted multipart objects after lifecycle transition

A lifecycle transition to a separate data pool must preserve an SSE-C multipart object's plaintext and the range mapping across its original part boundary. The case checks independent controls on the selected release and encrypted transitions on two correction-bearing Tentacle releases. See [Ceph Tracker 23264](https://tracker.ceph.com/issues/23264) and the [Tentacle correction](https://github.com/ceph/ceph/commit/ec11f6284d0810708ff65d5c5b8dc29294c0b672).

## Native observations

Each subtest starts a one-OSD cluster, configures a COLD storage class backed by its own pool and reloads the gateway. The test trusts the fixture's certificate authority and sends SSE-C requests over HTTPS. It uploads two multipart objects with identical layouts and distinguishable part payloads. The selected-release row transitions a plaintext object and keeps its SSE-C sibling in default storage. The fixed-release rows encrypt both objects with the same SSE-C key. The first part has 5,245,951 bytes and the second has 81,000. Initiation must confirm the requested encryption mode and completion must return a successful S3 result document.

Before the transition both objects must report default storage and their original encryption mode. The background lifecycle worker is disabled. After two debug days (four seconds), a manual lifecycle pass selects only the first object's prefix. HEAD must report COLD on that object and default storage on the untouched control. Before and after the pass, full GET must return 200 and all 5,326,951 original bytes. A GET spanning 32 bytes from the first part and 129 from the second must return 206, the exact Content-Range and all 161 expected bytes. A failed upload, unavailable placement, failed transition or failed control fails the test.

On the published Tentacle 20.2.1 aggregate, HEAD confirmed COLD for the plaintext object and STANDARD with AES256 for the encrypted control. Both objects returned the expected full and range bytes before and after processing. The pre-correction encrypted transition itself remains unmeasured on that release.

## Running

Optional batch `rgw_encrypted_multipart_transition` runs the independent controls with the caller's normal aggregate or split-role selection. It also checks the encrypted transition on `quay.io/ceph/ceph:v20.2.2` and `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all`. The latter two rows use their specified aggregate for every daemon and client. An upstream source boundary alone does not establish an affected release. The encrypted object's reads in the selected-release control cover default storage only.

```sh
CEPH_TEST_IMAGE=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.1-all \
  go test -count=1 -tags=all,native_regression -timeout=20m -v \
  ./internal/integration/experiments/rgw_encrypted_multipart_transition
```

Each subtest bounds its operations to six minutes, purges its own bucket and tears down its cluster and disposable data pool before the next row starts.
