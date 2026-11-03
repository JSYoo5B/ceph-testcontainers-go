# Ceph RGW

Import `github.com/JSYoo5B/ceph-testcontainers-go/rgw` to give an application
that uses S3 (or Swift) a disposable Ceph cluster with one or more gateways and
the server-side state its integration test needs: an S3 endpoint the test can
reach (optionally native HTTPS), users with explicit Admin Ops capabilities,
tenants, quotas and suspension, rate limits, and storage counters. Accounts
and their root users are in [`rgw/account`](account/README.md), placement targets with
storage classes in [`rgw/placement`](placement/README.md), and the persistent
queues of bucket notification topics in
[`rgw/notification`](notification/README.md); a test that needs one gateway
and an S3 user reads this package alone. Object operations belong
to the S3 client under test; the package adds no S3 SDK. See
[API purpose and purpose tags](../docs/guides/api-purpose.md).

```
go get github.com/JSYoo5B/ceph-testcontainers-go/rgw
```

## Order of use

1. **Start a cluster with a gateway**: `rgw.Run(ctx, ceph.DefaultImage,
   opts...)`; `rgw.WithGateways(rgw.Config{...})` selects other gateways (also
   on `ceph.Run`, `rbd.Run` and `cephfs.Run`), and `rgw.Start` adds one to a
   running cluster. When the test needs server-side RGW options (lifecycle
   clock, usage log, STS key, KMS backend), start with `ceph.Run`, apply them
   with `override.SetConfig`, then call `rgw.Start`: gateways read them at
   start.
2. **Prepare the identities** the scenario needs on a `*rgw.Gateway` (from
   `Gateways` or `Start`): `CreateUser` (with tenant, Admin Ops caps, bucket
   limit), `User.SetQuota` / `SetBucketQuota`, `User.Suspend`, and
   `User.CreateS3Key` / `S3Key.Revoke` for credential rotation. For an account,
   create it with `account.Create(ctx, gateway, cfg)` and its root user with
   `CreateUser` and `UserConfig.Account`; for a placement target, use
   `placement.Create(ctx, gateway, cfg)`, then `Placement.Apply` and
   `Placement.SetUserDefault`.
3. **Hand the endpoint to the SUT**: `S3Endpoint` (or `S3SecureEndpoint`) for a
   client on the test host, `DaemonEndpoint` for a client in the cluster
   network, with `User.CreationKey().Credentials()` or the credentials of a
   key from `CreateS3Key` (or the gateway's own `AccessKey` / `SecretKey`)
   and `Gateway.Region`, path-style addressing.
4. **Throttle the SUT** when the scenario tests its reaction to 503
   SlowDown: `User.OverrideRateLimit` applies a `rgw.RateLimit` to the user
   (`rgw.RateLimitScope{}`) or to one bucket it owns (`RateLimitScope{Bucket:
   ...}`, such as a bucket the SUT created), and the returned handle's
   `Restore` puts the prior policy back.
5. **Observe and verify**: `User.Info`, `User.Usage`, `notification.Queue`
   for a persistent notification topic, and `Gateway.Admin` for anything
   without a typed API.
6. **Clean up**: terminate the cluster; gateways, users and data go with it.
   `Gateway.Remove` and `User.Remove` are for tests of those steps.

The smallest S3 fixture keeps one daemon of each kind and single-copy pools:

```golang
cluster, err := rgw.Run(ctx, ceph.DefaultImage,
	ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(1).WithManagerCount(1).WithOSDCount(1).WithPoolDefaults(1, 1)),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
gateways, err := rgw.Gateways(ctx, cluster.Container)
if err != nil {
	t.Fatal(err)
}
gateway := gateways[0]
endpoint, err := gateway.S3Endpoint(ctx)
if err != nil {
	t.Fatal(err)
}
// Use endpoint, gateway.AccessKey, gateway.SecretKey and gateway.Region with a
// path-style S3 client. Keep the keys out of test logs.
```

It runs four containers (MON with the Ceph CLIs, MGR, OSD, gateway). With one
OSD and one copy nothing recovers from an OSD failure; tests of OSD faults or
recovery need the default two OSDs or more.

## Concepts and limits

- Everything in [`ceph`](../ceph/README.md) applies to the cluster. Multisite
  (one realm across clusters, zones, zonegroups, sync policy) is in
  [`rgw/multisite`](multisite/README.md).
- Images: the `Run` image is the essential image; `ceph.ImageOptions.WithRGWImage`
  selects the gateway image, which must contain `radosgw` and `radosgw-admin`
  (`Gateway.Admin` runs inside the gateway). Keep every role on the same Ceph
  release and architecture. `ceph.DefaultImage` is Tentacle 20.2.4; Squid
  19.2.5 is supported (use 19.2.5, not 19.2.6, for multisite). See
  [Releases and images](../docs/guides/releases-and-images.md).
- Identities belong to the cluster and scope that created them, not to one
  gateway: user handles, and the account and placement handles of the feature
  packages, run through any running gateway of their scope, and a gateway can
  be removed without removing them. Copies of a handle share its state.
- An account's root user is an ordinary `*rgw.User` created with
  `UserConfig.Account`; the gateway must belong to the account's cluster and
  scope, and every later operation on the user checks the account's native
  lifetime again.
- Credential rotation keeps the user and its data: `User.CreateS3Key` adds a
  key pair, and `S3Key.Revoke` removes one irreversibly (nothing restores
  it). Each `*rgw.S3Key` names one key pair for good, so creating a key never
  changes what an earlier handle or copied credentials mean, and a revoked
  handle still returns its credentials for refusal checks. The fixture
  touches only the keys it created: keys added with `Gateway.Admin` stay as
  they are. The last active fixture-created key of a user cannot be revoked,
  because it identifies the owned user; create its replacement first. A
  failed `CreateS3Key` is never retried by the fixture: a returned key whose
  outcome is unknown is only revocable, and its `Revoke` settles and removes
  it.
- Creating or revoking a key changes what RGW accepts, not what the SUT
  uses: it does not show that the SUT reloaded its configuration or dropped
  a cached credential. Hand the new key to the SUT the way it reads its
  configuration, and assert with requests the SUT makes afterwards. RGW
  refuses a revoked key once its metadata caches see the removal, so poll
  with new requests.
- Endpoints change on restart: in bridge mode a restarted gateway can get a
  new mapped host port. Resolve `S3Endpoint` again after any restart, including
  the ones done by `Placement.Apply`, `Placement.Reload` and multisite changes.
- Quota enforcement, suspension, placement defaults, rate limits and usage
  counters go through caches and asynchronous accounting: poll with new
  requests instead of asserting right after a change. Usage log persistence is
  asynchronous too.
- A rate limit is a per-minute budget that every gateway process counts on its
  own: two gateways serving the same bucket admit up to twice the budget, so
  it is not a cluster-wide request budget. A bucket scope throttles every
  request to that bucket, whoever signs it; a user scope throttles every
  request the user signs. The handle only references a bucket: it never
  deletes it, and a bucket the SUT deletes and creates again is a different
  bucket, whose restoration the handle refuses.
- HTTPS is native Beast TLS without a proxy; clients must trust the CA.
  `NetworkConfig.WithMessengerMode` encrypts Ceph messenger traffic only.
- Printing a handle never writes keys; `Gateway.Admin` output can contain
  keys, and a failed call never includes native output. Every key the
  fixture created or tried to create, revoked ones included, is masked in
  diagnostics reports.
- Gateways run 4 worker threads: functional tests, not load tests. Docker exec
  cancellation does not stop a native worker already running (such as
  `bucket sync run`).
- Out of scope: production Vault setups and other KMS backends, Swift large
  objects, temporary URLs and Keystone catalogues.

## Reference

- API: [pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/rgw)
  or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/rgw`.
- [Native commands](../docs/guides/native-commands.md#rgw-administration)
  (`radosgw-admin`, Admin Ops, RGW options),
  [Host network](../docs/guides/host-network.md),
  [Contexts and cleanup](../docs/guides/contexts-and-cleanup.md),
  [Errors](../docs/guides/errors.md).
