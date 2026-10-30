# Ceph RGW

Import `github.com/JSYoo5B/ceph-testcontainers-go/rgw` to give an application
that uses S3 (or Swift) a disposable Ceph cluster with one or more gateways and
the server-side state its integration test needs: an S3 endpoint the test can
reach (optionally native HTTPS), users with explicit Admin Ops capabilities,
tenants and accounts, quotas and suspension, storage counters, and placement
targets with storage classes. Object operations belong to the S3 client under
test; the package adds no S3 SDK. See
[API purpose and classification](../docs/guides/api-purpose.md).

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
2. **Prepare the identities and placement** the scenario needs on a
   `*rgw.Gateway` (from `Gateways` or `Start`): `CreateUser` (with tenant,
   Admin Ops caps, bucket limit), `CreateAccount` and
   `Account.CreateRootUser`, `User.SetQuota` / `SetBucketQuota`,
   `User.Suspend`, `CreatePlacement` then `ApplyPlacement`, `User.SetPlacement`.
3. **Hand the endpoint to the SUT**: `S3Endpoint` (or `S3SecureEndpoint`) for a
   client on the test host, `DaemonEndpoint` for a client in the cluster
   network, with `User.Credentials` (or the gateway's own `AccessKey` /
   `SecretKey`) and `Gateway.Region`, path-style addressing.
4. **Observe and verify**: `User.Info`, `User.Usage`, `Account.Info`,
   `Placement.Status`, and `Gateway.Admin` for anything without a typed API.
5. **Clean up**: terminate the cluster; gateways, users and data go with it.
   `Gateway.Remove`, `User.Remove` and `Account.Remove` are for tests of those
   steps.

The smallest S3 fixture keeps one daemon of each kind and single-copy pools:

```golang
cluster, err := rgw.Run(ctx, ceph.DefaultImage,
	ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(1).WithManagerCount(1).WithOSDCount(1).WithPoolDefaults(1, 1)),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
gateways, err := cluster.Gateways(ctx)
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
  gateway: user, account and placement handles run through any running gateway
  of their scope, and a gateway can be removed without removing them.
- Endpoints change on restart: in bridge mode a restarted gateway can get a
  new mapped host port. Resolve `S3Endpoint` again after any restart, including
  the ones done by `ApplyPlacement`, `ReloadPlacement` and multisite changes.
- Quota enforcement, suspension, placement defaults and usage counters go
  through caches and asynchronous accounting: poll with new requests instead of
  asserting right after a change. Usage log persistence is asynchronous too.
- HTTPS is native Beast TLS without a proxy; clients must trust the CA.
  `NetworkConfig.WithMessengerMode` encrypts Ceph messenger traffic only.
- Printing a handle never writes keys; `Gateway.Admin` output can contain
  keys, and a failed call never includes native output.
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
