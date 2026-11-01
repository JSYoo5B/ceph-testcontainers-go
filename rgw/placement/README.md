# RGW placement

Import `github.com/JSYoo5B/ceph-testcontainers-go/rgw/placement` to give an
application that chooses S3 storage classes or bucket placement the state its
integration test needs on an existing gateway: a fresh placement target with
its index, multipart and per-class data pools, its activation in a standalone
gateway or a realm period, and a user's default placement and placement tags.
Gateways, users and pools stay in the [rgw](../README.md) and
[ceph](../../ceph/README.md) packages; the package starts no container. See
[API purpose and purpose tags](../../docs/guides/api-purpose.md).

## Order of use

1. **Start a gateway** with `rgw.Run`, `rgw.Start` or `rgw.WithGateways`, and
   create the pools the target names with `cluster.CreatePool`.
2. **Create the target** with `placement.Create(ctx, gateway, cfg)`. It stores
   the zonegroup target and the zone mapping and returns an owned handle; it
   does not activate them.
3. **Activate it**: `Placement.Apply(ctx, gateway)` restarts a standalone
   gateway or publishes a realm period whose only change is this target. In
   another cluster of the realm, import the period, then call
   `Placement.Reload(ctx, gateway)` there. Resolve `S3Endpoint` again after
   either, because a restarted gateway can get a new mapped port.
4. **Hand it to the SUT**: pass `Placement.LocationConstraint` to S3
   `CreateBucket`, or make the target a user's default with
   `Placement.SetUserDefault(ctx, user, cfg)`, which also replaces the user's
   placement tags.
5. **Observe**: `Placement.Status` reads the native target and mapping;
   `rgw.User.Info` reads the user's default placement and tags.
6. **Clean up**: terminate the cluster. Placement targets are never removed or
   rolled back by this package.

```golang
cluster, err := rgw.Run(ctx, ceph.DefaultImage)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
for _, pool := range []string{"tiered-index", "tiered-extra", "tiered-standard", "tiered-ia"} {
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rgw"}); err != nil {
		t.Fatal(err)
	}
}
gateways, err := rgw.Gateways(ctx, cluster.Container)
if err != nil {
	t.Fatal(err)
}
gateway := gateways[0]
target, err := placement.Create(ctx, gateway, placement.Config{
	Name: "tiered", IndexPool: "tiered-index", DataExtraPool: "tiered-extra",
	StorageClasses: []placement.StorageClass{{Name: "STANDARD", DataPool: "tiered-standard"}, {Name: "STANDARD_IA", DataPool: "tiered-ia"}},
})
if err != nil {
	t.Fatal(err)
}
if err := target.Apply(ctx, gateway); err != nil {
	t.Fatal(err)
}
endpoint, err := gateway.S3Endpoint(ctx) // after the restart
// Create buckets with target.LocationConstraint and objects with STANDARD_IA.
```

## Concepts and limits

- A handle records the exact native scope (realm, zonegroup and zone IDs),
  the pool IDs and the zonegroup and zone documents it created. Every
  operation refuses a replaced pool, a changed zone or zonegroup, or a gateway
  of another cluster; editing the handle's exported fields does not redirect
  an operation. A partial creation returns a handle for inspection.
- Creation refuses an existing name and never changes the zonegroup default
  or any existing bucket's placement. Index and multipart pools must be
  replicated; data pools may be erasure coded.
- In a realm, `Apply` needs the current metadata master and refuses a staging
  period that holds unrelated changes; `Reload` never publishes, pulls or
  rewrites federation topology.
- `SetUserDefault` changes user metadata only. Tags nil keeps the user's tags;
  a nonempty list replaces them, and replacing matching tags with nonmatching
  ones revokes access to a restricted target. Placement tags authorize new
  bucket placement, not object access, and native caches can delay them.
- Do not race the operations with raw `radosgw-admin zonegroup`, `zone` or
  `period` edits through `Gateway.Admin`.

## Reference

The full contract of every type and function is in go doc:
[pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/rgw/placement)
or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/rgw/placement`.
See also [Native commands](../../docs/guides/native-commands.md#rgw-administration)
for what has no typed API, [rgw/multisite](../multisite/README.md) for realm
periods across clusters,
[Contexts and cleanup](../../docs/guides/contexts-and-cleanup.md) and
[Errors](../../docs/guides/errors.md).
