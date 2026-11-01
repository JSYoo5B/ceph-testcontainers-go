# RGW accounts

Import `github.com/JSYoo5B/ceph-testcontainers-go/rgw/account` to give an
application that works with RGW accounts the state its integration test needs
on an existing gateway: a fresh account with its tenant and limits, its
account-wide and per-bucket quotas, and root users whose buckets the account
owns. Gateways, ordinary users and their credentials stay in the
[rgw package](../README.md); the package starts no container. See
[API purpose and purpose tags](../../docs/guides/api-purpose.md).

## Order of use

1. **Start a gateway** with `rgw.Run`, `rgw.Start` or `rgw.WithGateways`, and
   take its handle from `rgw.Gateways(ctx, cluster.Container)`.
2. **Create the account** with `account.Create(ctx, gateway, cfg)`. It returns
   an owned handle; `SetQuota` and `SetBucketQuota` on the handle set the
   account-wide and per-bucket limits.
3. **Create its root users** with `gateway.CreateUser(ctx,
   rgw.UserConfig{..., Account: acct})`. The gateway must belong to the
   account's cluster and scope. A root user is an ordinary `*rgw.User`: hand
   `User.Credentials` to the SUT, which runs IAM operations itself.
4. **Observe**: `Account.Info` reads the native policy; `User.Info` and
   `User.Usage` of a root user report the account association and the
   account aggregate.
5. **Clean up**: terminate the cluster; it removes the account with its users
   and data. Use `Account.Remove` and `User.Remove` only to test those steps.

```golang
cluster, err := rgw.Run(ctx, ceph.DefaultImage)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
gateways, err := rgw.Gateways(ctx, cluster.Container)
if err != nil {
	t.Fatal(err)
}
gateway := gateways[0]
acct, err := account.Create(ctx, gateway, account.Config{Name: "tenant-a", Tenant: "tenant_a"})
if err != nil {
	t.Fatal(err)
}
if err := acct.SetQuota(ctx, rgw.Quota{Enabled: true, MaxSizeBytes: 64 << 20, MaxObjects: -1}); err != nil {
	t.Fatal(err)
}
root, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "root", DisplayName: "Root", Account: acct})
if err != nil {
	t.Fatal(err)
}
accessKey, secretKey, err := root.Credentials()
// Give the SUT the endpoint, gateway.Region and these keys.
```

## Concepts and limits

- An account handle records the native metadata version tag of the account it
  created, so a deleted and recreated account with the same ID is refused.
  Copies of a handle share its state. Creation refuses an existing ID and
  never adopts it.
- A root user's operations check the account's lifetime and tenant again
  every time. An empty `UserConfig.Tenant` inherits the account's tenant; a
  different tenant is refused, and the display name must be a valid IAM user
  name.
- Quotas of account-owned storage are set on the account, not on its users:
  `User.SetQuota` refuses a root user. Account quota sizes are rounded to KiB
  natively, so byte limits must be -1, zero or a multiple of 1024. Enforcement
  goes through caches and asynchronous accounting: poll with new requests.
- `Account.Remove` never purges data: native Ceph refuses an account that still
  has users, buckets, roles, groups, OIDC providers or topics.
- Do not race the operations with raw `radosgw-admin account` edits of the
  same account through `Gateway.Admin`.

## Reference

The full contract of every type and function is in go doc:
[pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/rgw/account)
or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/rgw/account`.
See also [Native commands](../../docs/guides/native-commands.md#rgw-administration)
for what has no typed API,
[Contexts and cleanup](../../docs/guides/contexts-and-cleanup.md) and
[Errors](../../docs/guides/errors.md).
