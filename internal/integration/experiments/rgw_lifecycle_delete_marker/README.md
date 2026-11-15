# Removing expired delete markers

An S3 lifecycle rule can remove the current delete marker when it is the only remaining version of a key. A current marker above a noncurrent data version must remain. For the historical missing-head handling in the storage abstraction layer, see [Ceph Tracker 70853](https://tracker.ceph.com/issues/70853) and the [Tentacle correction](https://github.com/ceph/ceph/commit/5c82cbe78be7fb0c1bac07feac585aec10f201c7).

## Native observations

The test assigns a versioned bucket two prefix-filtered lifecycle rules. One covers both marker keys. The other expires only a witness key's noncurrent data. Before processing, a complete version listing must identify each current marker, the preserved underlying version, and both witness versions by their original S3 version IDs and latest flags.

The gateway's background lifecycle worker is disabled. One debug day is two seconds. After four seconds, the test drives a bounded `radosgw-admin lc process` pass for this bucket. A complete second listing must show exactly the ineligible marker, its underlying data version and the current witness version. Losing the old witness proves that lifecycle processing actually changed the bucket. Missing initial state, an unchanged witness or a failed native command fails the test.

Both Tentacle 20.2.2 and 20.2.4 removed the eligible marker in this RADOS-backed setup. This result establishes healthy behavior for the measured layout. Other storage backends and historical marker encodings are outside the case.

## Tier and triggers

Healthy-behaviour characterization in the required `topology` batch
`rgw_lifecycle_delete_marker` with a 15-minute budget. A pull request runs it
only when a file of this directory changes, or with `ci:full`; Periodic
`rotate` can draw it and `targeted` runs it on a chosen patch; on a pull
request's head, `/ci run rgw_lifecycle_delete_marker [on <release>]`. It
prints no result line. See [Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case).

## Running

The batch starts one cluster with one OSD, an admin client and an RGW. It accepts the usual aggregate image or essential, client and RGW role images. Each request has a 30-second deadline. The case purges the bucket before the fixture removes the cluster. Its steps share a six-minute deadline that starts before cluster startup. Startup (`harness.NewServiceCluster`) runs on the test context, so the deadline does not cut it short, but the time it takes is spent from the six minutes and the steps after it get what remains. In cleanup the purge of the bucket has its own 30-second deadline and the cluster's termination its own two-minute one, so cleanup is not bounded by the deadline and can take the test past six minutes. The outer bound is the batch's 15-minute `go test` timeout.

```sh
go test -count=1 -tags=all -timeout=15m -v \
  -run '^TestRGWLifecycleDeleteMarker$' \
  ./internal/integration/experiments/rgw_lifecycle_delete_marker
```
