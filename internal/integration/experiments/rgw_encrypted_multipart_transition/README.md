# Reading encrypted multipart objects after lifecycle transition

A lifecycle transition to a separate data pool must preserve an SSE-C multipart object's plaintext and the range mapping across its original part boundary. The case checks normal reads on two correction-bearing Tentacle releases. See [Ceph Tracker 23264](https://tracker.ceph.com/issues/23264) and the [Tentacle correction](https://github.com/ceph/ceph/commit/ec11f6284d0810708ff65d5c5b8dc29294c0b672).

## Native observations

Each subtest starts a one-OSD cluster, configures a COLD storage class backed by its own pool and reloads the gateway. The test trusts the fixture's certificate authority and sends SSE-C requests over HTTPS. It uploads two objects with identical keys for encryption and two distinguishable part payloads per object. The first part has 5,245,951 bytes and the second has 81,000. Initiation must confirm AES256 and completion must return a successful S3 result document.

Before the transition both objects must report default storage and SSE-C. The background lifecycle worker is disabled. After two debug days (four seconds), a manual lifecycle pass selects only the first object's prefix. HEAD must report COLD on that object and default storage on the untouched control. Before and after the pass, full GET must return 200 and all 5,326,951 original bytes. A GET spanning 32 bytes from the first part and 129 from the second must return 206, the exact Content-Range and all 161 expected bytes. A failed upload, unavailable placement, failed transition or failed control fails the test.

## Running

Optional batch `rgw_encrypted_multipart_transition` runs the same comparison separately on `quay.io/ceph/ceph:v20.2.2` and `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all`. Both clusters and all their clients and gateways use the specified aggregate for that row. Selecting a different profile release leaves those two inputs fixed. This characterization establishes data integrity for those two releases and does not assign an affected-release boundary.

```sh
go test -count=1 -tags=all,native_regression -timeout=20m -v \
  ./internal/integration/experiments/rgw_encrypted_multipart_transition
```

Each subtest bounds its operations to six minutes, purges its own bucket and tears down its cluster and disposable data pool before the next row starts.
