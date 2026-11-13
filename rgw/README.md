# Ceph RGW

Import `github.com/JSYoo5B/ceph-testcontainers-go/rgw` to give an application
that uses S3 (or Swift) a disposable Ceph cluster with one or more gateways and
the server-side state its integration test needs: an S3 endpoint the test can
reach (optionally native HTTPS), users with explicit Admin Ops capabilities,
tenants, quotas and suspension, STS roles with their policies, rate limits,
and storage counters. Accounts
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
   `User.CreateS3Key` / `S3Key.Revoke` for credential rotation, and
   `CreateRole` for a role that the SUT assumes with STS. For an account,
   create it with `account.Create(ctx, gateway, cfg)` and its root user with
   `CreateUser` and `UserConfig.Account`; for a placement target, use
   `placement.Create(ctx, gateway, cfg)`, then `Placement.Apply` and
   `Placement.SetUserDefault`.
3. **Hand the endpoint to the SUT**: `S3Endpoint` (or `S3SecureEndpoint`) for a
   client on the test host, `DaemonEndpoint` for a client in the cluster
   network, with `User.CreationKey().Credentials()` or the credentials of a
   key from `CreateS3Key` (or the gateway's own `AccessKey` / `SecretKey`)
   and `Gateway.Region`, path-style addressing. For STS, hand off
   `Role.ARN()` as well: the SUT calls AssumeRole on the same endpoint.
4. **Throttle the SUT** when the scenario tests its reaction to 503
   SlowDown: `User.OverrideRateLimit` applies a `rgw.RateLimit` to the user
   (`rgw.RateLimitScope{}`) or to one bucket it owns (`RateLimitScope{Bucket:
   ...}`, such as a bucket the SUT created), and the returned handle's
   `Restore` puts the prior policy back. The user scope needs Ceph 20: on
   Squid it returns `ceph.ErrUnsupported`, because a Squid gateway never
   applies a user's policy to requests its S3 keys sign. A user that stored
   no policy is restored to the stored native default, every budget zero and
   disabled, because `radosgw-admin` cannot delete a policy; on Umbrella,
   where `ratelimit get` reports such a user as `ENOENT`, it then reports the
   default (see
   [Known release-specific issues](../docs/guides/releases-and-images.md#known-release-specific-issues)).
5. **Replace the certificate** when the scenario tests how the SUT meets a
   renewed certificate or a changed CA: `Gateway.ReplaceTLS` installs a new
   `rgw.TLSConfig` and restarts the gateway; resolve `S3SecureEndpoint` again
   and give the SUT the new CA the way it reads its configuration.
6. **Observe and verify**: `User.Info`, `User.Usage`, `notification.Queue`
   for a persistent notification topic, and `Gateway.Admin` for anything
   without a typed API. [`rgw/expect`](expect/README.md) evaluates one
   captured `User.Usage` observation against a stated owner and totals.
7. **Clean up**: terminate the cluster; gateways, users and data go with it.
   `Gateway.Remove`, `User.Remove` and `Role.Remove` are for tests of those
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
- Images: gateways use the cluster's RGW role. That is the `Run` image, such
  as the aggregate `ceph.DefaultImage`, unless `ceph.ImageOptions.WithImageSet`
  selects an `ImageSet` whose `RGW` role differs, and
  `ceph.ImageOptions.WithRGWImage` replaces either. The gateway image must
  contain `radosgw` and `radosgw-admin` (`Gateway.Admin` runs inside the
  gateway). Keep every role on the same Ceph
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
- A role is created with its trust policy and inline permission policies,
  in the empty tenant and outside any account, and the fixture hands off its
  ARN only; AssumeRole, temporary credentials and their use belong to the
  SUT, and the package adds no STS client. STS needs `rgw_sts_key` (a
  session-token key, such as the output of `ceph-authtool --gen-print-key`)
  and `rgw_s3_auth_use_sts=true`, set with `override.SetConfig` before
  `rgw.Start`; without them the role exists but AssumeRole fails. An existing
  role of the same name is refused, never adopted, and `Role.Remove` deletes
  only the fixture's policies and role: it refuses a role that was replaced,
  whose trust policy, tags or metadata changed, or that holds a policy the
  fixture did not put, and leaves it as it is. A failed `CreateRole` is never
  retried: a returned role whose outcome is unknown is only removable, and
  a role whose policy RGW refused is returned with the error so that its
  `Remove` cleans it up. Policy and trust edits reach RGW through metadata
  caches, so poll with new requests.
- Endpoints change on restart: in bridge mode a restarted gateway can get a
  new mapped host port. Resolve `S3Endpoint` again after any restart, including
  the ones done by `Gateway.ReplaceTLS`, `Placement.Apply`, `Placement.Reload`
  and multisite changes. A test whose SUT must keep one URL while gateways
  stop or move uses the [stable endpoint recipe](#stable-endpoint-across-gateways).
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
- Beast loads its certificate only when the gateway starts; replacing the
  file is not reloaded. `Gateway.ReplaceTLS` therefore restarts the gateway:
  it refuses connections meanwhile, connections established before are
  closed (not moved to the new certificate), and a request in flight can fail
  with an unknown outcome. Only new connections see the new certificate, and
  every later start of the container, including a caller's own Stop and
  Start, is ready only when it serves that certificate. A failed restart
  leaves the new certificate installed; nothing rolls it back.
- Printing a handle never writes keys; `Gateway.Admin` output can contain
  keys, and a failed call never includes native output. Every key the
  fixture created or tried to create, revoked ones included, is masked in
  diagnostics reports.
- Gateways run 4 worker threads: functional tests, not load tests. Docker exec
  cancellation does not stop a native worker already running (such as
  `bucket sync run`).
- Out of scope: managing a KMS (see
  [Server-side encryption with an external KMS](#server-side-encryption-with-an-external-kms)
  for wiring a test-owned Vault), other KMS backends, Swift large objects,
  temporary URLs and Keystone catalogues.

## Stable endpoint across gateways

The package has no load balancer. A test that needs the SUT to keep one URL
while the gateway behind it stops or changes composes one from what the
package already hands off: start two gateways in the same zone with the same
`TLSConfig`, read each one's `S3SecureEndpoint`, and run a test-owned TCP
forwarder that listens on one fixed address and connects every new client
connection to the gateway the test currently selects. The forwarder copies
bytes only, so TLS runs end to end and the SUT verifies the gateway's own
certificate. The test switches the selection, stops the first gateway with
`Stop`, and asserts with new requests of the SUT.

```golang
a, _ := rgw.Start(ctx, cluster, rgw.Config{Name: "a", TLS: material})
b, _ := rgw.Start(ctx, cluster, rgw.Config{Name: "b", TLS: material})
// forwarder is the test's own TCP forwarder to the host:port of
// S3SecureEndpoint of a and of b.
// The SUT is configured with https://<forwarder address> and the CA.
forwarder.Select(1) // new connections go to b
_ = a.Stop(ctx, nil)
// The SUT's next requests reach b through the same URL.
```

`TestRGWStableEndpoint` in
[`internal/integration/rgw`](../internal/integration/rgw/rgw_stable_endpoint_integration_helpers_test.go)
runs this recipe with an AWS SDK process as the SUT and holds a complete
forwarder. What it shows is limited: the forwarder chooses a backend when a
connection opens and never checks health, retries or replays a request, or
moves an established connection, so a request in flight when a gateway stops
has an unknown outcome and the SUT's own retry decides what happens next. In
bridge mode, read the endpoints again after any gateway restart, because the
forwarder addresses mapped host ports. Both gateways serve the same cluster
storage: this is gateway failover, not replicated storage.

## Server-side encryption with an external KMS

The package has no KMS type. The test owns the KMS (a Vault container, its
keys, policies and tokens) and wires RGW to it with what the package already
has: `override.SetConfig` for the RGW options and
`testcontainers.WithFiles` on `rgw.Start` for the token and CA files.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage)
// Vault runs on the cluster bridge with the alias "vault", which is also in
// its certificate. kmsToken may read one KV-v2 key; sseS3Token manages keys of
// one Transit mount.
for _, s := range []ceph.ConfigSetting{
	{Section: "client.admin", Name: "rgw_crypt_s3_kms_backend", Value: "vault"},
	{Section: "client.admin", Name: "rgw_crypt_vault_auth", Value: "token"},
	{Section: "client.admin", Name: "rgw_crypt_vault_token_file", Value: "/tc/kms-token"},
	{Section: "client.admin", Name: "rgw_crypt_vault_addr", Value: "https://vault:8200"},
	{Section: "client.admin", Name: "rgw_crypt_vault_prefix", Value: "/v1/kv/data"},
	{Section: "client.admin", Name: "rgw_crypt_vault_secret_engine", Value: "kv"},
	{Section: "client.admin", Name: "rgw_crypt_vault_verify_ssl", Value: "true"},
	{Section: "client.admin", Name: "rgw_crypt_vault_ssl_cacert", Value: "/tc/vault-ca.pem"},
	// SSE-S3 has its own rgw_crypt_sse_s3_* options of the same names,
	// with the transit engine and its own token file.
} {
	change, err := override.SetConfig(ctx, cluster, s)
	// defer change.Restore(...)
}
gateway, err := rgw.Start(ctx, cluster, rgw.Config{Name: "s3", TLS: material},
	testcontainers.WithFiles(
		testcontainers.ContainerFile{Reader: bytes.NewReader(kmsToken), ContainerFilePath: "/tc/kms-token", FileMode: 0o600},
		testcontainers.ContainerFile{Reader: bytes.NewReader(vaultCA), ContainerFilePath: "/tc/vault-ca.pem", FileMode: 0o600},
	))
```

`TestRGWExternalKMS` in
[`internal/integration/rgw`](../internal/integration/rgw/rgw_external_kms_integration_helpers_test.go)
runs the recipe for both modes. What the recipe relies on:

- There are two TLS connections. `TLSConfig` secures the SUT's connection to
  RGW; `rgw_crypt_vault_verify_ssl` and `rgw_crypt_vault_ssl_cacert` (and
  their `sse_s3` twins) secure RGW's connection to Vault. With
  `rgw_crypt_require_ssl` left at `true`, RGW refuses SSE requests over plain
  HTTP.
- Every gateway runs as `client.admin`, so the options apply to every gateway
  of the cluster and are read when a gateway starts: set them before
  `rgw.Start`. Gateways differ only in the files at those paths. A gateway
  given an unrelated CA answers an encrypted GET with 500, while its sibling
  with the right CA reads the exact bytes and the unrelated one still serves
  listings and unencrypted objects.
- To change a gateway's CA, replace the file and restart the gateway (`Stop`,
  `Start`, then resolve its endpoint again). A running gateway need not read
  the CA file again: on Umbrella it keeps answering 500 after the file is
  replaced, while Tentacle reads the new file on the next request.
- RGW reads the token file on every Vault request, so a test replaces a token
  by writing the new one next to the file and renaming it over the same path
  (keep mode `0600`), without restarting the gateway.
- RGW keeps its connection to Vault open, so a cut KMS surfaces only when
  libcurl's low-speed limit aborts a request: lower `rgw_curl_low_speed_time`
  (5 minutes by default) for a test that cuts the KMS. Reattaching Vault's
  endpoint can publish its port on another host port; resolve `PortEndpoint`
  again before the next management request.
- With SSE-S3, a bucket DELETE is no proof that RGW removed the bucket's
  Transit key; check the key in Vault and delete an orphan as the KMS
  operator.

What RGW answers when the token is revoked or the KMS is cut, and how key
deletion differs between releases, is recorded by the experiment case
[`rgw_kms_failure_modes`](../internal/integration/experiments/rgw_kms_failure_modes/README.md).

## Reference

- API: [pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/rgw)
  or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/rgw`.
- [Native commands](../docs/guides/native-commands.md#rgw-administration)
  (`radosgw-admin`, Admin Ops, RGW options),
  [Host network](../docs/guides/host-network.md),
  [Contexts and cleanup](../docs/guides/contexts-and-cleanup.md),
  [Errors](../docs/guides/errors.md).
