# Ceph RGW

## Introduction

The Testcontainers module for Ceph RGW.

Ceph RGW (the RADOS Gateway) is Ceph's object storage service. It serves S3- and
Swift-compatible HTTP APIs on top of a Ceph cluster. This module starts a
disposable Ceph cluster (MON, MGR and OSD containers) with one or more gateways
and prepares the server-side state that S3 client tests need:

- gateways with an S3 endpoint that the test process can reach, optionally with
  a native HTTPS listener;
- S3 users with explicit Admin Ops capabilities, tenants, accounts and account
  root users, user, bucket and account quotas, suspension and storage counters;
- placement targets and S3 storage classes backed by replicated or
  erasure-coded pools.

Multisite orchestration (one realm across independent clusters, zones and
zonegroups, period transfer, sync status and sync policies) lives in the
separate `rgw/multisite` package, documented in [rgw-multisite.md](rgw-multisite.md).

The module configures Ceph through the CLIs inside its containers
(`radosgw-admin` and `ceph`). It does not wrap S3 object operations and adds no
S3 SDK or go-ceph dependency: tests use their own S3 client against the endpoint
and credentials that the module returns. Anything without a typed method is
available through `Gateway.Admin` (see [Running native commands](#running-native-commands)).

The cluster underneath (pools, central configuration, health, daemons,
networking) is documented in [ceph.md](ceph.md). The same cluster can also host
RBD ([rbd.md](rbd.md)) and CephFS ([cephfs.md](cephfs.md)).

## Adding this module to your project dependencies

Please run the following command to add the Ceph RGW module to your Go dependencies:

```
go get github.com/JSYoo5B/ceph-testcontainers-go/rgw
```

## Usage example

```golang
package app_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

func TestUploadAndDownload(t *testing.T) {
	ctx := t.Context()

	cluster, err := rgw.Run(ctx, ceph.DefaultImage)
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}

	// Run started one gateway named "default" with a generated S3 user.
	gateways, err := cluster.Gateways(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gateway := gateways[0]
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}

	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       gateway.Region,
		Credentials: credentials.NewStaticCredentialsProvider(
			gateway.AccessKey, gateway.SecretKey, ""),
		UsePathStyle: true,
	})

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("reports"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("2026/summary.txt"),
		Body:   bytes.NewReader([]byte("hello")),
	}); err != nil {
		t.Fatal(err)
	}

	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("2026/summary.txt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil || string(body) != "hello" {
		t.Fatalf("read %q: %v", body, err)
	}
}
```

### The smallest S3 fixture

For tests that only need an S3 endpoint, keep one daemon of each kind and
single-copy pools:

```golang
cluster, err := rgw.Run(ctx, ceph.DefaultImage,
	ceph.WithMonitorCount(1),
	ceph.WithManagerCount(1),
	ceph.WithOSDCount(1),
	ceph.WithPoolDefaults(1, 1),
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
// Use gateway.AccessKey and gateway.SecretKey with a path-style S3 client,
// as in the example above. Keep the keys out of test logs.
```

This runs four containers: the MON, which also runs the Ceph CLIs, the MGR, the
OSD and the gateway. Every pool, including the ones the gateway creates, has
size 1 and min_size 1. With cached images on Docker Desktop (arm64), `Run`
returned in 17 seconds with Ceph 20.2.4 and in 23 seconds with 19.2.5.

- Each daemon keeps its own container, so `Stop`, `Start` and `fault.Pause`
  on a handle still act on one daemon only.
- With one OSD and one copy, nothing can recover from an OSD failure. Tests of
  OSD faults, recovery or separate traffic planes need the default two OSDs or
  more.
- For role images, add `ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(...))` and `rgw.WithImage`, as described
  under [Image](#image).

`TestRGWMinimalS3Recipe` in `internal/integration` runs this recipe with a
signed bucket create, a binary PUT and GET, deletes, and a 404 after the
delete.

## Module Reference

### Run function

The Ceph RGW module exposes one entrypoint function to create the Ceph RGW container, and this function receives three parameters:

```golang
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error)
```

- `context.Context`, the Go context.
- `string`, the Docker image to use.
- `testcontainers.ContainerCustomizer`, a variadic argument for passing options.

`Run` starts a disposable Ceph cluster and, unless `WithGateways` is given, one
gateway named `default` together with a generated ordinary S3 test user whose
keys are in `Gateway.AccessKey` and `Gateway.SecretKey`. It returns when the
gateway answers HTTP requests.

The returned `Container` embeds `*ceph.Container`, so every cluster method from
[ceph.md](ceph.md) is available on it, including `Terminate`, which removes the
gateways together with the rest of the cluster. Gateways are owned by the
cluster: you never terminate a gateway separately.

`Run` accepts every `ceph.Run` option and the options of the other service
packages, so one cluster can also carry RBD pools and CephFS filesystems.
Bootstrap options that leave the cluster without OSDs or managers for the
gateway, such as `ceph.WithNoInitialOSDs` and `ceph.WithNoInitialManagers`, are
rejected; build those clusters with `ceph.Run` and add gateways later with
[`Start`](#start).

A non-nil `Container` returned together with an error owns whatever was created
and must still be terminated. `testcontainers.CleanupContainer(t, cluster)`
before the error check handles this.

The package-level functions that act on a cluster (`Start`,
`Gateways`) and the cluster fields of the
[multisite configuration](rgw-multisite.md) take a `*ceph.Container`. Pass `cluster.Container` for a cluster
returned by `rgw.Run` (or by `rbd.Run` or `cephfs.Run`), or pass the value
returned by `ceph.Run` directly.

Server-side RGW options are read when a gateway starts. When a test needs such
options (lifecycle clock, usage log, STS key, KMS backend), create the cluster
with `ceph.Run`, apply the options with `override.SetConfig`, and then call
`rgw.Start`. Most recipes under [Examples](#examples) follow this pattern.

#### Image

Use the second argument in the `Run` function to set a valid Docker image. For
example: `rgw.Run(ctx, ceph.DefaultImage)`.

- The default image is `ceph.DefaultImage`,
  `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
  (Ceph Tentacle 20.2.4).
- The image passed to `Run` is the essential image. It runs the MON, MGR and OSD
  daemons and the Ceph CLI. Every other role uses the same image unless a role
  image is selected: `rgw.WithImage` for gateways, `ceph.ImageOptions.WithImageSet` for the
  client, cephfs, rbd-aux and rgw roles.
- A gateway image must contain `radosgw` and `radosgw-admin`, because
  `Gateway.Admin` runs inside the gateway container. Multisite setup clients and
  the `SourceAdmin`/`DestinationAdmin`/`ZoneAdmin` commands use the control
  image of each cluster.
- Ceph Squid works with `quay.io/ceph/ceph:v19.2.5`. Use 19.2.5 rather than
  19.2.6 for multisite (see [Ceph Squid](#ceph-squid-192x)).
- Keep all role images on the same Ceph release and CPU architecture.
- The module reads no environment variables. Image selection is always
  explicit.

See [guides/releases-and-images.md](guides/releases-and-images.md) for role
images and release selection.

### Container Options

When starting the Ceph RGW container, you can pass options in a variadic way to configure it.

#### WithGateways

```golang
func WithGateways(configs ...Config) ceph.Option
```

Starts the named gateways during `Run` (or `ceph.Run`, `rbd.Run`,
`cephfs.Run`). Without arguments it starts one default gateway. Repeated
options replace the list. Each gateway is configured by a [`Config`](#config):
by default every gateway gets its own generated S3 test user; set
`SkipUserCreation` when the test creates its own users.

All gateways of one cluster serve the same zone, so they share users, buckets
and objects. Multisite scope is configured separately with
`multisite.Run` and `multisite.RunTopology` from the
[`rgw/multisite`](rgw-multisite.md) package.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithOSDCount(3),
	rgw.WithGateways(
		rgw.Config{Name: "gateway-a"},
		rgw.Config{Name: "gateway-b", SkipUserCreation: true},
	),
)
```

#### WithImage

```golang
func WithImage(image string) ceph.Option
```

Selects the image for every gateway of the cluster, both the ones started by
`Run` and the ones added later with `Start`. Without it, gateways use the image
passed to `Run`. Use the same Ceph release as the essential image.

```golang
cluster, err := rgw.Run(ctx, "registry.example.com/ceph:20.2.4-all",
	rgw.WithImage("registry.example.com/ceph:20.2.4-rgw"),
)
```

The cluster options of the `ceph` package also apply. These are the ones that
matter most for RGW tests:

| Option | Effect on gateways |
| --- | --- |
| `ceph.WithOSDCount(n)` | One OSD is enough for most S3 tests and starts faster. Erasure-coded storage classes need at least K+M OSDs. |
| `ceph.WithHostNetwork()` | Gateways bind ports on the Docker host. Ports are reserved automatically, so several clusters can share a host. See [guides/host-network.md](guides/host-network.md). |
| `ceph.WithHostAddress(addr)` | The address gateways bind and advertise in host mode. `S3Endpoint` returns it directly. |
| `ceph.WithConfigFile(path)` | Native `ceph.conf` entries for every daemon. RGW runs as `client.admin`, so RGW options belong in `[client]` or `[client.admin]`. |
| `ceph.WithStartupTimeout(d)` | Bounds gateway startup and each `Gateway.Admin` call. |
| `ceph.WithMessengerMode(...)` | Encrypts Ceph messenger traffic only. It does not enable HTTPS on gateways; use `Config.TLS`. |

The generic options of the `testcontainers` package (for example
`testcontainers.WithEnv`, `testcontainers.WithLogger`,
`testcontainers.CustomizeRequestOption`) also apply. Passed to `Run`, they
customise only the MON/control container. To customise a gateway container,
pass them to [`Start`](#start), or to `multisite.Run`, `multisite.RunTopology`,
`Fixture.AddZone` or `Fixture.AddZonegroup` from
[`rgw/multisite`](rgw-multisite.md), where they are applied last. Do not replace
the gateway's entrypoint, command, network or listener settings.

### Container Methods

The `Container` returned by `Run` has two shortcuts for its gateways:

```golang
func (c *Container) Gateways(ctx context.Context) ([]*Gateway, error)
func (c *Container) S3Endpoint(ctx context.Context) (string, error)
```

`Gateways` returns the cluster's gateways sorted by name, like
`rgw.Gateways(ctx, c.Container)`. `S3Endpoint` returns the S3 endpoint of the
cluster's only gateway, such as the `default` gateway of a `Run` without
`WithGateways`; with several gateways, or none, it fails, and you choose a
gateway from `Gateways` instead.

The `Container` also embeds `*ceph.Container`. Its cluster methods
(`override.SetConfig`, `CreatePool`, `Ceph`, `ControlContainer`, `WithClient`,
`ceph.FactsOf` and so on) are documented in
[ceph.md](ceph.md).

The RGW-specific methods live on each gateway. A gateway is a `*rgw.Gateway`,
obtained from `Gateways`, `Start`, or the `Source`, `Destination` and
`Zones()` of a multisite fixture. `Gateway` embeds `testcontainers.Container`,
so `Stop`, `Start`, `Logs`, `Exec` and `Inspect` act on the gateway container.
It also has these fields:

| Field | Meaning |
| --- | --- |
| `GatewayName` | `Config.Name`, or `default` when empty. |
| `AccessKey`, `SecretKey` | Keys of the generated ordinary S3 test user. Empty with `SkipUserCreation`. Multisite gateways share one test user. |
| `Region` | Region to use for SigV4 signing. `Config.Region`, which defaults to `us-east-1`; for multisite gateways, the zonegroup name. The fixture does not configure RGW with this value. |

Formatting a `Gateway` with `%v` or `%#v` prints only its name; credentials and
TLS keys are never included.

### Container Methods: Endpoints

#### S3Endpoint

```golang
func (g *Gateway) S3Endpoint(ctx context.Context) (string, error)
```

Returns the HTTP endpoint for S3 clients running in the test process, in the
form `http://host:port`. In bridge mode it is the Docker-mapped port of the
gateway's port 7480. In host mode it is the daemon port selected at startup on
the advertised address; the default `127.0.0.1` is resolved through the Docker
host, and an address given with `ceph.WithHostAddress` is returned as is.

Configure clients with path-style addressing (`UsePathStyle: true` in the AWS
SDK for Go) and sign with `Gateway.Region`.

In bridge mode the mapped port can change whenever the gateway container
restarts: after `ApplyPlacement`, `ReloadPlacement`, `ApplySyncGroup`,
`AddZone`, `AddZonegroup`, `RemoveZone`, or your own `Stop`/`Start`. Call
`S3Endpoint` again afterwards.

#### S3SecureEndpoint

```golang
func (g *Gateway) S3SecureEndpoint(ctx context.Context) (string, error)
```

Returns the `https://host:port` endpoint of the native TLS listener started by
`Config.TLS`. It returns an error when the gateway has no TLS configuration. It
does not disable certificate verification or change any client TLS setting: the
client must trust the CA and the certificate must name the returned host. See
[Native HTTPS endpoint](#native-https-endpoint).

#### DaemonEndpoint

```golang
func (g *Gateway) DaemonEndpoint(ctx context.Context) (string, error)
```

Returns the HTTP address used by peers inside the gateway's network. In bridge
mode it is the gateway's IP in the cluster's Docker network
(`FactsOf(cluster).NetworkName`) with port 7480; in host mode it is the advertised
address and port. Use it from containers attached to the
cluster (for example a `cluster.WithClient()` container), not from the test
process, which uses `S3Endpoint`.

On a `ceph.AddressDualStack` cluster it returns the gateway's IPv4 address.
The gateway listens on `0.0.0.0:7480` and `[::]:7480`, so clients in the
cluster's network reach it over its IPv6 address on port 7480 as well; see
[Dual-stack clusters](ceph.md#dual-stack-clusters).

### Container Methods: Native commands

#### Admin

```golang
func (g *Gateway) Admin(ctx context.Context, args ...string) ([]byte, error)
```

Runs `radosgw-admin` inside this gateway container with the gateway's realm,
zonegroup, zone, Ceph configuration and admin keyring, and returns standard
output. It is the escape hatch for anything without a typed method.

- `--format json` is always added, so output is JSON.
- Arguments are passed individually and never parsed by a shell. Arguments
  must not contain control characters, so pass JSON documents (policies,
  trust documents) as single-line strings.
- The call is bounded by `ctx` and by the cluster's startup timeout, and no
  `radosgw-admin` outlives it. A small `sh` supervisor in the gateway
  container (it needs no Python, which the RGW role image does not have)
  runs `radosgw-admin` and stops it with SIGTERM, then SIGKILL after 2 s:
  - 15 s before the bound (at half the bound for a short one), which returns
    `ceph.ErrNativeFailure` with "radosgw-admin still ran after Ns and was
    stopped";
  - when `ctx` ends first, for example when the caller cancels. Admin then
    stops the command under its own budget of about 10 s, which is not
    limited by the expired `ctx`, and returns `ctx`'s error only after the
    command is gone. A call that ends before Docker started the command
    makes sure that it never starts;
  - when the cluster's `Terminate` starts. The call then returns an error
    that says the cluster is terminated.
- When Admin cannot confirm that stop, for example because the gateway is
  paused or the engine is unreachable, it returns a
  `*ceph.NativeCommandUnstoppedError`. Its `Stop(ctx)` retries the stop, and
  `Terminate` retries it too. Stopping `radosgw-admin` does not undo a
  request that it already sent to the cluster: the outcome of a stopped or
  canceled mutation is unknown, and Admin never repeats it.
- Arguments that would change the scope or connection are rejected:
  `--rgw-realm`, `--rgw-zonegroup`, `--rgw-zone` and their ID and name
  variants, `--conf`/`-c`, `--keyring`/`-k`, `--name`/`-n`, `--id`,
  `--mon-host`/`-m`, `--cluster` and similar flags.
- Successful output can contain secret keys (for example `user info`). Do not
  log it.
- On failure the error does not include the native output, because that output
  can contain keys. It keeps a `*ceph.NativeFailure` with the exit code and
  errno but no reason, so `errors.Is(err, fs.ErrNotExist)` works. To see
  native error text, run the same command with `gateway.Exec` (see
  [Running native commands](#running-native-commands)). Refused arguments are
  `ceph.ErrInvalidRequest`.
- Calls are serialized with other operations on the same cluster and bounded by
  the cluster startup timeout.
- The gateway must still be running and owned by the cluster. After `Remove`,
  use another gateway in the same zone.
- Do not change users that the typed helpers own (create, modify, remove) with
  raw commands while those helpers run.

```golang
stats, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", "reports")
```

### Container Methods: Users

#### CreateUser

```golang
func (g *Gateway) CreateUser(ctx context.Context, config UserConfig) (*User, error)
```

Creates a fresh ordinary S3 identity in the gateway's zone and returns a handle
that holds its generated keys (see [`UserConfig`](#userconfig) and
[`User`](#user)). `AdminCaps` grants Admin Ops REST capabilities; the global
`admin` flag and the multisite `system` flag are never set.

```golang
user, err := gateway.CreateUser(ctx, rgw.UserConfig{
	ID:        "app",
	AdminCaps: "users=read;usage=read",
})
if err != nil {
	t.Fatal(err)
}
accessKey, secretKey, err := user.Credentials()
```

Contract:

- An existing ID is rejected; its keys and policy are not changed.
- A non-nil handle returned with an error records an attempted or partial
  creation. If the generated keys were confirmed (for example, only the
  capability step failed), `UserInfo` and `RemoveUser` still work with it.
- If the creation response was lost or malformed, ownership cannot be
  confirmed. Inspect the user with `Admin` or discard the cluster; do not retry
  the same ID blindly.
- Users remain in Ceph when the gateway that created them is removed. Another
  gateway in the same zone can manage them.

#### UserInfo

```golang
func (g *Gateway) UserInfo(ctx context.Context, user *User) (UserInfo, error)
```

Reads the user's current policy: quotas, Admin Ops capabilities, suspension,
flags, default placement and storage class, and placement tags (see
[`UserInfo`](#userinfo-type)). Keys are not included. Before returning, it
verifies that the key created by the fixture, the original user type and the
original account association are still present; for an account root it also
checks that the account is the one created by the fixture.

#### SuspendUser

```golang
func (g *Gateway) SuspendUser(ctx context.Context, user *User, suspended bool) error
```

Disables (`true`) or re-enables (`false`) S3 access for the user and reads back
the native state. Gateway metadata caches can delay the HTTP effect, so poll
with new requests when you test suspension or re-enablement.

#### RemoveUser

```golang
func (g *Gateway) RemoveUser(ctx context.Context, user *User) error
```

Removes a user created by the fixture without purging buckets or objects.
Ceph refuses to remove an ordinary user that still owns buckets; delete them
with the S3 client first. Buckets of an account belong to the account and can
outlive the removal of a root user.

Removal is idempotent across copies of the handle. After an uncertain failure,
an absent user counts as removed; a user that is still present is retried after
its key is verified again. Keys copied earlier with `Credentials` stay valid in
your variables but no longer authenticate.

### Container Methods: Tenants and accounts

Tenants and accounts are native RGW namespaces. A tenant user is created with
`UserConfig.Tenant`; its canonical ID is `tenant$id`, and buckets with the same
name can exist in different tenants. An account groups users, roles and
buckets under one owner with account-wide limits.

#### CreateAccount

```golang
func (g *Gateway) CreateAccount(ctx context.Context, config AccountConfig) (*Account, error)
```

Creates a fresh account in the gateway's zone (see
[`AccountConfig`](#accountconfig)). An existing account ID is rejected and never
adopted. A partial handle can be returned with an error after a native failure;
the account is owned only when its creation response and native metadata
version were confirmed. Inspect uncertain results with `Admin` or discard the
cluster. No existing users are migrated into the account.

#### CreateAccountRootUser

```golang
func (g *Gateway) CreateAccountRootUser(ctx context.Context, account *Account, config UserConfig) (*User, error)
```

Creates a fresh root user inside an account created by the fixture. An empty
`Tenant` inherits the account tenant; a different tenant is rejected.
`DisplayName` must be IAM-compatible: 1 to 64 characters from letters, digits
and `_+=,.@-` (the default `Testcontainers` qualifies). The root user has full
permissions inside its account; the global `admin` and `system` flags stay off.
IAM operations inside the account (users, roles, policies) are performed by the
test's own IAM client with the root user's keys.

```golang
account, err := gateway.CreateAccount(ctx, rgw.AccountConfig{Name: "team-a"})
if err != nil {
	t.Fatal(err)
}
root, err := gateway.CreateAccountRootUser(ctx, account, rgw.UserConfig{ID: "team-a-root"})
```

#### AccountInfo

```golang
func (g *Gateway) AccountInfo(ctx context.Context, account *Account) (AccountInfo, error)
```

Reads the account's native limits and quotas (see
[`AccountInfo`](#accountinfo-type)).

#### RemoveAccount

```golang
func (g *Gateway) RemoveAccount(ctx context.Context, account *Account) error
```

Removes an empty account created by the fixture, without purging data. Ceph
refuses an account that still has users (including root users), buckets,
roles, groups, OIDC providers or topics; remove those through their clients
first. Removal is idempotent across copies of the handle. Do not edit the
account with raw commands while these helpers run.

### Container Methods: Quotas

Quotas use the [`Quota`](#quota) type: sizes in bytes, `-1` disables a limit,
`0` is a real zero limit, and `Enabled` controls enforcement. Enforcement
depends on native statistics and caches, so a write right after a quota change,
or right after the quota is reached, may still be accepted. Poll with new
writes when you test rejection.

#### SetUserQuota

```golang
func (g *Gateway) SetUserQuota(ctx context.Context, user *User, quota Quota) error
```

Replaces the aggregate quota of an ordinary user (all of its buckets together),
including the enabled state. Users that belong to an account are rejected; use
`SetAccountQuota` for them. If a later native command fails, an earlier part of
the change may already be stored: `UserInfo` reads the actual state and the
same request can safely be retried.

```golang
err := gateway.SetUserQuota(ctx, user, rgw.Quota{
	Enabled: true, MaxSizeBytes: 64 << 20, MaxObjects: 1000,
})
```

#### SetBucketQuota

```golang
func (g *Gateway) SetBucketQuota(ctx context.Context, user *User, quota Quota) error
```

Sets the per-bucket quota of an ordinary user: the limits apply to each bucket
the user owns, individually. To limit one named bucket only, use `Admin` (see
[Bucket quota and resharding](#bucket-quota-and-resharding)).

#### SetAccountQuota

```golang
func (g *Gateway) SetAccountQuota(ctx context.Context, account *Account, quota Quota) error
```

Replaces the aggregate quota of an account: one limit across all of its users,
roles and buckets. Ceph stores account quota sizes in KiB, so `MaxSizeBytes`
must be `-1`, `0` or a positive multiple of 1024. The readback confirms the
stored policy, not cache enforcement.

#### SetAccountBucketQuota

```golang
func (g *Gateway) SetAccountBucketQuota(ctx context.Context, account *Account, quota Quota) error
```

Sets the individual bucket quota for every bucket owned by the account,
regardless of which account identity created the bucket.

### Container Methods: Storage usage

#### UserUsage

```golang
func (g *Gateway) UserUsage(ctx context.Context, user *User) (UserUsage, error)
```

Reads the current native storage counters of the user's effective owner (see
[`UserUsage`](#userusage-type)). For an ordinary or tenant user, `Scope` is
`user` and `OwnerID` is the user ID. For an account root user, `Scope` is
`account`, `OwnerID` is the account ID, and the counters cover the whole
account (all users, roles and buckets), not only the objects written by that
root user.

```golang
usage, err := gateway.UserUsage(ctx, user)
if err != nil {
	t.Fatal(err)
}
if usage.Scope != "user" || usage.NumObjects != 4 {
	t.Fatalf("unexpected usage %+v", usage)
}
```

Native statistics update asynchronously. `UserUsage` does not run
`--sync-stats`, reset counters or flush caches, so poll until the expected
values appear. If a test needs an explicit synchronisation step, run
`gateway.Admin(ctx, "user", "stats", "--uid", user.ID(), "--sync-stats")` as a
separate setup action. The counters are storage accounting only: they do not
prove quota enforcement or object content, and they are not dated request or
bandwidth usage (see [Usage logs and Admin Ops](#usage-logs-and-admin-ops)).

### Container Methods: Placement and storage classes

A placement target selects the index pool, the multipart metadata pool and one
data pool per S3 storage class. These helpers add a new, non-default target;
they never change the zonegroup default, an existing target, any user's default
or any existing bucket. Creation stores configuration; activation restarts the
gateway (standalone) or publishes a period (multisite). See
[Placement targets and storage classes](#placement-targets-and-storage-classes).

#### CreatePlacement

```golang
func (g *Gateway) CreatePlacement(ctx context.Context, config PlacementConfig) (*Placement, error)
```

Creates a named target and all of its storage classes in the gateway's actual
zone and zonegroup (see [`PlacementConfig`](#placementconfig)). All pools must
already exist; their native IDs are rechecked before each change. A target name
that already exists is rejected. Changes are applied one by one; after a
partial failure the stored configuration is left for inspection and nothing is
rolled back. In a realm, every participating zone needs its own compatible
mapping (create the target through each zone's gateway) before the master
publishes it.

#### ApplyPlacement

```golang
func (g *Gateway) ApplyPlacement(ctx context.Context, placement *Placement) error
```

Activates a confirmed new target.

- Standalone gateway: restarts only this gateway. Other gateways in the same
  zone need their own `ApplyPlacement`. Call `S3Endpoint` again afterwards.
  The restart is refused if the native defaults that the gateway would load at
  startup now point to a different zone.
- Realm (multisite): must be called on the gateway of the current metadata
  master. It commits a new period only when the staged change consists of this
  target alone; unrelated current, pending or staged changes are refused before
  `period update`. Then import the period into the other clusters
  (`PullDestinationPeriod` or `PullSourcePeriod`) and call `ReloadPlacement` on
  their gateways.

It never changes defaults, deletes resources or rolls back a partial failure.
After an uncertain restart or commit, check the native state before retrying.

#### PlacementStatus

```golang
func (g *Gateway) PlacementStatus(ctx context.Context, placement *Placement) (PlacementState, error)
```

Reads the exact native policy of a created or partial target (see
[`PlacementState`](#placementstate)). Incomplete or changed mappings return an
error; use `Admin` (`zonegroup get`, `zone get`) for detailed inspection. It
does not claim that a running gateway has loaded the configuration.

#### ReloadPlacement

```golang
func (g *Gateway) ReloadPlacement(ctx context.Context, placement *Placement) error
```

Restarts this gateway so that it loads a confirmed target, typically on a
secondary zone after the master's period was imported. In realm mode the
locally stored committed period must contain the exact target and zone, with
unchanged pool IDs and class mappings. It never publishes, pulls or rewrites
the realm. Call `S3Endpoint` again afterwards.

#### SetUserPlacement

```golang
func (g *Gateway) SetUserPlacement(ctx context.Context, user *User, placement *Placement, config UserPlacementConfig) error
```

Makes a confirmed target the user's default for new buckets and selects the
default storage class for new objects that carry no storage class header.
Optionally replaces the user's placement tags (see
[`UserPlacementConfig`](#userplacementconfig)).

```golang
err := gateway.SetUserPlacement(ctx, user, placement, rgw.UserPlacementConfig{
	StorageClass: "STANDARD_IA",
	Tags:         []string{"app-tier"},
})
```

- The target must be activated (`ApplyPlacement`/`ReloadPlacement`) before S3
  uses it. This method only changes user metadata.
- Existing buckets keep their placement; placement is immutable after bucket
  creation.
- It uses the gateway's actual zone IDs, checks that the placement's pool IDs
  and class mappings are unchanged, and verifies that unrelated user policy and
  keys did not change.
- Metadata caches can delay the effect on new S3 requests.
- A failed readback may follow a stored change. `UserInfo` shows the actual
  state, and the same request can be retried.

### Start, Remove and Gateways

These functions manage gateways on an existing cluster. They take the
`*ceph.Container` of the cluster (`cluster.Container` for a cluster from
`rgw.Run`).

#### Start

```golang
func Start(ctx context.Context, c *ceph.Container, config Config, opts ...testcontainers.ContainerCustomizer) (*Gateway, error)
```

Starts a named gateway on a running cluster created by `ceph.Run` or any
service package's `Run`. A zero `Config` starts the gateway named `default`
and creates its S3 test user. Gateway names must be unique within the cluster
and use letters, digits, dots, underscores or hyphens.

`opts` customise the gateway container and are applied last. Use them, for
example, to copy a token file into the gateway with `testcontainers.WithFiles`.
Keep the required command, network and listener settings.

A non-nil gateway returned with an error is still owned by the cluster and
removed when the cluster terminates.

```golang
gateway, err := rgw.Start(ctx, cluster, rgw.Config{Name: "s3", SkipUserCreation: true})
```

In host mode, the port is reserved on the Docker host and released just before
the daemon binds it. If another process takes the port in between, startup is
retried with a new port, up to five attempts.

#### Gateway.Remove

```golang
func (g *Gateway) Remove(ctx context.Context) error
```

Removes this gateway container. Zone configuration, users, buckets and objects stay available through the other
gateways or a replacement. The last gateway can be removed. Multisite period
endpoints are not updated automatically. A failed removal stays tracked and is
retried when the cluster terminates. A descriptor that was already removed, or
that is not the one the cluster owns for its name, returns an error instead of
acting on a later gateway of the same name.

#### Gateways

```golang
func Gateways(ctx context.Context, c *ceph.Container) ([]*Gateway, error)
```

Returns the cluster's gateways sorted by name, including gateways whose startup
failed part way. `ctx` bounds the wait for a concurrent topology change, and a
nil cluster returns an error. The returned slice is independent; the cluster
owns the gateways and their containers.

#### Printing handles: String and GoString

`Gateway`, `User`, `Account`, `Placement` and `TLSConfig`
implement `String` and `GoString` so that `fmt` verbs, including `%v`, `%+v`
and `%#v`, print only an identifying label such as `RGW gateway s3` or
`RGW user app`. Printing a handle never writes its S3 keys, TLS private key or
other credentials to test logs. Read credentials explicitly with
`User.Credentials` or the `Gateway` fields when a client needs them.

#### Config

| Field | Meaning |
| --- | --- |
| `Name` | Gateway name. Defaults to `default`. Different names allow several gateways in one zone. |
| `Realm`, `Zonegroup`, `Zone` | Optional native multisite scope for the gateway. They must already exist. Multisite fixtures set them for you; leave them empty for standalone gateways. |
| `Region` | Value reported in `Gateway.Region` for client signing. Defaults to `us-east-1`. |
| `SkipUserCreation` | Do not create the generated test user. Useful when the test creates its own users or shares existing credentials. |
| `TLS` | `*TLSConfig`; enables a native HTTPS listener next to HTTP. |

#### TLSConfig

| Field | Meaning |
| --- | --- |
| `CertificatePEM` | PEM certificate chain, leaf first. |
| `PrivateKeyPEM` | Matching unencrypted PEM private key. |

The bytes are copied; the certificate and key are written to
`/tc/rgw-tls.pem` with mode 0600 inside the gateway container only. Beast, RGW's
HTTP frontend, terminates TLS itself; no proxy is involved. In bridge mode the
container's ports 7480 (HTTP) and 7481 (HTTPS) are mapped to random host ports;
in host mode two free host ports are reserved. Startup verifies that the
gateway process owns the HTTPS socket and that the listener presents the
configured certificate. Formatting a `TLSConfig` never prints key bytes.

#### UserConfig

| Field | Meaning |
| --- | --- |
| `ID` | Local user ID (letters, digits, dots, underscores, hyphens), without a tenant prefix. Empty generates a unique ID. |
| `DisplayName` | Defaults to `Testcontainers`. |
| `Email` | Optional. |
| `Tenant` | Native tenant namespace (letters, digits, underscores; must not look like an account ID). Empty uses the default namespace. |
| `AdminCaps` | Admin Ops REST capabilities, for example `"users=read;usage=read"`. Permissions are `read`, `write` or `*`. |
| `MaxBuckets` | `*int`. `nil` keeps Ceph's default; `0` allows unlimited buckets; `-1` forbids creating buckets. |

#### User

A handle to an identity created by `CreateUser` or `CreateAccountRootUser`.
Copies share state. Formatting a `User` never prints secrets.

```golang
func (u *User) ID() string
func (u *User) Credentials() (accessKey, secretKey string, err error)
```

`ID` returns the canonical native ID: `tenant$id` for a tenant user, the plain
ID otherwise. `Credentials` returns copies of the generated S3 keys; it returns
an error when the keys of a partial creation were never confirmed.

#### UserInfo type

`UserInfo` is the snapshot returned by `Gateway.UserInfo`.

| Field | Meaning |
| --- | --- |
| `ID`, `DisplayName`, `Email` | Identity. `ID` is canonical (`tenant$id`). |
| `Tenant`, `LocalID`, `Namespace` | Tenant and local ID parts. |
| `Type` | Native user type: `rgw` (or empty) for ordinary users, `root` for account root users. |
| `AccountID` | Owning account, empty for ordinary users. |
| `Suspended`, `Admin`, `System` | Native flags. The fixture never sets `Admin` or `System`. |
| `MaxBuckets` | Bucket limit. |
| `AdminCaps` | `[]AdminCapability`. |
| `UserQuota`, `BucketQuota` | Aggregate quota and the per-bucket quota applied to each owned bucket. |
| `DefaultPlacement`, `DefaultStorageClass` | User defaults for new buckets and objects. `STANDARD` set on a named placement reads back as an empty string. |
| `PlacementTags` | Tags that allow the user to create buckets in tagged placement targets. They do not grant object access. |

#### AdminCapability

| Field | JSON | Meaning |
| --- | --- | --- |
| `Type` | `type` | Capability type, such as `users`, `usage`, `ratelimit`, `buckets`. |
| `Permission` | `perm` | `read`, `write`, or `*` when both were granted. `*` does not mean global admin. |

#### Quota

| Field | Meaning |
| --- | --- |
| `Enabled` | Whether the limits are enforced. |
| `MaxSizeBytes` | Size limit in bytes. `-1` disables it; `0` is a real zero limit. |
| `MaxObjects` | Object count limit. `-1` disables it; `0` is a real zero limit. |

#### AccountConfig

| Field | Meaning |
| --- | --- |
| `ID` | `RGW` followed by 17 digits. Empty generates a unique ID. Account IDs are globally unique. |
| `Name` | Defaults to a unique fixture name. Must not contain `$` or `:`. |
| `Tenant` | Optional tenant that isolates the account's bucket names. |
| `Email` | Optional; globally unique. |
| `MaxBuckets` | `*int`. `nil` keeps the native default; `-1`, `0` or positive. |

#### Account

A handle to an account created by `CreateAccount`. `ID()` returns the account
ID. The handle records the account's native metadata version, so a deleted and
recreated account with the same ID is not mistaken for the original. Copies
share lifecycle state. Removing a gateway does not remove the account.

#### AccountInfo type

| Field | Meaning |
| --- | --- |
| `ID`, `Name`, `Tenant`, `Email` | Identity. |
| `MaxUsers`, `MaxRoles`, `MaxGroups`, `MaxBuckets`, `MaxKeys` | Native account limits. |
| `AccountQuota` | Aggregate quota across all users, buckets and roles of the account. |
| `BucketQuota` | Individual quota applied to each account bucket. |

#### UserUsage type

| Field | Meaning |
| --- | --- |
| `UserID`, `Tenant` | The user that was queried. |
| `Scope` | `user` or `account`. |
| `OwnerID` | User ID or account ID whose counters are reported. |
| `SizeBytes` | Native `size`. |
| `SizeActualBytes` | Native `size_actual`: size rounded up to allocation units. Not physical disk usage, replicas or billing. |
| `NumObjects` | Native `num_objects`. |
| `LastStatsSync`, `LastStatsUpdate` | Native timestamp strings, unparsed. A never-synced value can be `0.000000`. |

#### PlacementConfig

| Field | Meaning |
| --- | --- |
| `Name` | New target name. |
| `IndexPool`, `DataExtraPool` | Existing replicated pools (they need omap support). `DataExtraPool` holds incomplete multipart metadata. |
| `StorageClasses` | `[]StorageClassConfig`. `STANDARD` is required. |
| `InlineData` | `*bool`. `nil` keeps Ceph's default (`true`, small object data stored with the head); `false` stores all data in the class data pool. |
| `Tags` | Restricts creating buckets in this target to users with at least one matching placement tag. Empty allows every user. Tags do not grant access to existing buckets. |

#### StorageClassConfig

| Field | Meaning |
| --- | --- |
| `Name` | S3 storage class, for example `STANDARD` or `STANDARD_IA`. The class name must also be accepted by the test's S3 SDK. |
| `DataPool` | Existing data pool, replicated or erasure-coded. RGW writes whole objects, so EC pools do not need `AllowOverwrites`. |

#### Placement

| Field | Meaning |
| --- | --- |
| `Name` | Target name. |
| `RealmID`, `ZonegroupID`, `ZoneID` | Native scope where the target was created. |
| `LocationConstraint` | Value to pass as `LocationConstraint` in S3 `CreateBucket` to place a bucket in this target (zonegroup API name and target name combined). |

A failed `CreatePlacement` may return a partial handle for inspection. Only a
confirmed creation can be activated.

#### PlacementState

`PlacementState` embeds `PlacementConfig` (as read from Ceph) and adds:

| Field | Meaning |
| --- | --- |
| `RealmID`, `ZonegroupID`, `ZoneID`, `LocationConstraint` | Native scope. |
| `DefaultPlacement` | The zonegroup default target (for example `default-placement`). These helpers never change it. |
| `Confirmed` | Creation and readback succeeded. It does not mean that a gateway loaded the target or that a realm published it. |

#### UserPlacementConfig

| Field | Meaning |
| --- | --- |
| `StorageClass` | Default class for new objects without a class header. Defaults to `STANDARD`. |
| `Tags` | `nil` keeps the user's current placement tags; a non-empty list replaces them exactly. An empty non-nil list is rejected because `radosgw-admin` cannot clear tags. To revoke access to a restricted target, replace matching tags with non-matching ones. |

### Shared cluster tenants

A tenant of a [shared cluster](ceph.md#shared-cluster) leases an RGW tenant of
its own. Start the gateway with `WithGateways` in `ceph.WithSharedRunOptions`.

```golang
func Lease(ctx context.Context, t *ceph.Tenant) (*TenantUser, error)

func (u *TenantUser) Tenant() string
func (u *TenantUser) UserID() string
func (u *TenantUser) Credentials() (accessKey, secretKey string)
func (u *TenantUser) Region() string
func (u *TenantUser) S3Endpoint(ctx context.Context) (string, error)
func (u *TenantUser) DaemonEndpoint(ctx context.Context) (string, error)
```

`Lease` creates the S3 user `ID$user` in the RGW tenant `ID` (the tenant's ID)
on the gateway named `default`, or else the first gateway by name. Use
`S3Endpoint` from the host and `DaemonEndpoint` from a container on the
cluster's network, with path-style addressing and `Credentials`.

Bucket names are scoped by the RGW tenant. Checked natively, two tenants can
both create a bucket of the same name, `ListBuckets` lists only the tenant's
own buckets, and a request for another tenant's bucket (`/OTHER:bucket`)
returns 403 AccessDenied.

The tenant never gets the `*rgw.Gateway`, whose container holds the admin
keyring. The shared cluster creates the pools of the gateway's zone before
its baseline, because RGW would otherwise create the bucket pools on the
first bucket. The tenant's release removes every user of the RGW tenant with
their buckets and objects, then any bucket of the tenant that remains, and
fails the test and poisons the cluster if a user or bucket remains.

## Examples

The snippets run inside a test function with `ctx := t.Context()`, and some
omit error checks for brevity. They share a few helpers:

```golang
import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

// startCluster starts a cluster without gateways, so that RGW options can be
// applied before a gateway starts.
func startCluster(t *testing.T, opts ...testcontainers.ContainerCustomizer) *ceph.Container {
	t.Helper()
	cluster, err := ceph.Run(t.Context(), ceph.DefaultImage, opts...)
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	return cluster
}

// applyConfig stores central configuration and restores it at the end of the
// test. Restoring is optional when the cluster is discarded anyway.
func applyConfig(t *testing.T, cluster *ceph.Container, settings ...ceph.ConfigSetting) {
	t.Helper()
	for _, setting := range settings {
		change, err := override.SetConfig(t.Context(), cluster, setting)
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := change.Restore(ctx); err != nil {
				t.Error(err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func newS3Client(endpoint, region, accessKey, secretKey string) *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		UsePathStyle: true,
	})
}

// eventually polls check until it returns true or the timeout expires.
func eventually(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before the deadline")
		}
		time.Sleep(time.Second)
	}
}
```

RGW runs as `client.admin`, so options go into the `client.admin` section (or
`client`). Apply them before `rgw.Start`. An alternative for options that never
change during a test is `ceph.WithConfigFile` with a `[client.admin]` section.

### Test an S3 client with the AWS SDK for Go

The default gateway user is enough for a single principal. Create one user per
principal when the test checks permissions between users:

```golang
cluster := startCluster(t, ceph.WithOSDCount(1))
gateway, err := rgw.Start(ctx, cluster, rgw.Config{SkipUserCreation: true})
if err != nil {
	t.Fatal(err)
}
endpoint, err := gateway.S3Endpoint(ctx)
if err != nil {
	t.Fatal(err)
}

clients := map[string]*s3.Client{}
for _, id := range []string{"owner", "reader"} {
	user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	accessKey, secretKey, err := user.Credentials()
	if err != nil {
		t.Fatal(err)
	}
	clients[id] = newS3Client(endpoint, gateway.Region, accessKey, secretKey)
}
// clients["owner"] creates a bucket and a bucket policy;
// clients["reader"] checks access denied, then granted.
```

Notes:

- Always use path-style addressing. Virtual-hosted-style bucket names would
  need DNS for every bucket.
- Versioning, multipart uploads, ACLs, bucket policies and object lock need no
  server preparation. Object lock must be enabled when the bucket is
  created (`ObjectLockEnabledForBucket: aws.Bool(true)`).
- Tenant users address buckets of their own tenant by plain name. Path-style
  requests reach a bucket in another tenant as `tenant:bucket`.
- Recent AWS SDK releases add CRC checksums to uploads by default. If a gateway
  rejects such requests, set `RequestChecksumCalculation:
  aws.RequestChecksumCalculationWhenRequired` and `ResponseChecksumValidation:
  aws.ResponseChecksumValidationWhenRequired` in `s3.Options`.

### Run several gateways in one zone

Gateways of one cluster serve the same data. This tests client failover between
endpoints, or credentials shared by several gateways:

```golang
cluster, err := rgw.Run(ctx, ceph.DefaultImage,
	rgw.WithGateways(
		rgw.Config{Name: "a"},
		rgw.Config{Name: "b", SkipUserCreation: true},
	),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
gateways, err := cluster.Gateways(ctx)
if err != nil {
	t.Fatal(err)
}
// gateways is sorted: a, b
a, b := gateways[0], gateways[1]
// Write through a with a's test user, read through b with the same keys.

// Remove a; b keeps serving users, buckets and objects.
if err := a.Remove(ctx); err != nil {
	t.Fatal(err)
}
```

### Users, tenants and accounts

```golang
gateway, err := rgw.Start(ctx, cluster, rgw.Config{SkipUserCreation: true})

// The same local ID and bucket names in two tenants stay isolated.
alpha, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "app", Tenant: "alpha"})
beta, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "app", Tenant: "beta"})
// alpha.ID() == "alpha$app", beta.ID() == "beta$app"

// An account with two root users and a shared quota.
account, err := gateway.CreateAccount(ctx, rgw.AccountConfig{Name: "team"})
rootA, err := gateway.CreateAccountRootUser(ctx, account, rgw.UserConfig{ID: "root-a"})
rootB, err := gateway.CreateAccountRootUser(ctx, account, rgw.UserConfig{ID: "root-b"})
err = gateway.SetAccountQuota(ctx, account, rgw.Quota{
	Enabled: true, MaxSizeBytes: 1 << 20, MaxObjects: -1,
})
// Objects written by rootA count against the same quota as rootB's.
usage, err := gateway.UserUsage(ctx, rootB) // Scope "account": includes rootA's objects

// Clean up in dependency order: objects and buckets through S3, then users,
// then the account.
err = gateway.RemoveUser(ctx, rootA)
err = gateway.RemoveUser(ctx, rootB)
err = gateway.RemoveAccount(ctx, account)
```

### Make lifecycle tests fast

Lifecycle rules count in days. `rgw_lc_debug_interval` makes one lifecycle day
last the given number of seconds. Disabling the background lifecycle threads
and processing the bucket explicitly makes the test deterministic:

```golang
cluster := startCluster(t, ceph.WithOSDCount(1))
applyConfig(t, cluster,
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_lc_debug_interval", Value: "2"},
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_enable_lc_threads", Value: "false"},
)
gateway, err := rgw.Start(ctx, cluster, rgw.Config{})
if err != nil {
	t.Fatal(err)
}
endpoint, err := gateway.S3Endpoint(ctx)
if err != nil {
	t.Fatal(err)
}
client := newS3Client(endpoint, gateway.Region, gateway.AccessKey, gateway.SecretKey)

// Create bucket "logs", a rule expiring "tmp/" after 1 day, and objects
// "tmp/a" and "keep/b" with the S3 client.

time.Sleep(3 * time.Second) // more than one lifecycle "day"
if _, err := gateway.Admin(ctx, "lc", "process", "--bucket", "logs"); err != nil {
	t.Fatal(err)
}
eventually(t, 30*time.Second, func() bool {
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("logs"), Key: aws.String("tmp/a"),
	})
	return err != nil // NotFound once expired
})
// "keep/b" must still be readable.
```

With the background threads left enabled, the gateway processes lifecycle on
its own schedule and the timing is less predictable. This tests the S3 contract
with an accelerated clock, not real day boundaries.

### Bucket notifications to an HTTP endpoint

RGW pushes bucket notifications to the endpoint of an SNS-compatible topic. The
test needs a gateway, a user and an HTTP receiver that the gateway can reach.
Run the receiver in a container attached to the cluster:

```golang
const receiverScript = `import http.server, json, sys
class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        with open("/tmp/events.log", "a") as log:
            for record in json.loads(body).get("Records", []):
                log.write(record["eventName"] + " " + record["s3"]["object"]["key"] + "\n")
        self.send_response(200)
        self.end_headers()
http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
`

cluster := startCluster(t, ceph.WithOSDCount(1))
gateway, err := rgw.Start(ctx, cluster, rgw.Config{SkipUserCreation: true})
user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "notifier"})
accessKey, secretKey, err := user.Credentials()
endpoint, err := gateway.S3Endpoint(ctx)

// The default image has Python. WithIdleEntrypoint runs under Docker's init,
// which reaps background processes started with Exec.
receiver, err := testcontainers.Run(ctx, ceph.DefaultImage,
	cluster.WithClient(), ceph.WithIdleEntrypoint())
testcontainers.CleanupContainer(t, receiver)
if err != nil {
	t.Fatal(err)
}
if err := receiver.CopyToContainer(ctx, []byte(receiverScript), "/tmp/receiver.py", 0o644); err != nil {
	t.Fatal(err)
}
if _, _, err := receiver.Exec(ctx, []string{"sh", "-c",
	"nohup python3 /tmp/receiver.py 8080 >/tmp/receiver.out 2>&1 &"}); err != nil {
	t.Fatal(err)
}

// The address the gateway uses to reach the receiver.
facts, err := ceph.FactsOf(cluster)
host := facts.PublicAddress // host mode
if !facts.UsesHostNetwork {
	if host, err = receiver.ContainerIP(ctx); err != nil {
		t.Fatal(err)
	}
}
pushEndpoint := "http://" + net.JoinHostPort(host, "8080")

topics := sns.New(sns.Options{
	BaseEndpoint: aws.String(endpoint),
	Region:       gateway.Region,
	Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
})
topic, err := topics.CreateTopic(ctx, &sns.CreateTopicInput{
	Name:       aws.String("uploads"),
	Attributes: map[string]string{"push-endpoint": pushEndpoint},
})
if err != nil {
	t.Fatal(err)
}

client := newS3Client(endpoint, gateway.Region, accessKey, secretKey)
// Create bucket "inbox", then:
_, err = client.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
	Bucket: aws.String("inbox"),
	NotificationConfiguration: &types.NotificationConfiguration{
		TopicConfigurations: []types.TopicConfiguration{{
			Id:       aws.String("incoming"),
			TopicArn: topic.TopicArn,
			Events:   []types.Event{"s3:ObjectCreated:*", "s3:ObjectRemoved:*"},
			Filter: &types.NotificationConfigurationFilter{Key: &types.S3KeyFilter{
				FilterRules: []types.FilterRule{{
					Name: types.FilterRuleNamePrefix, Value: aws.String("in/"),
				}},
			}},
		}},
	},
})
// Write "in/a" and "out/b", delete "in/a", then poll /tmp/events.log with
// receiver.Exec until it contains "ObjectCreated:Put in/a" and
// "ObjectRemoved:Delete in/a". "out/b" is outside the filter.
```

Notes:

- The topic ARN has the form `arn:aws:sns:<zonegroup>::<name>`.
- In host mode the receiver binds a port on the Docker host; choose a free one.
- Add the topic attributes `persistent=true` and, optionally,
  `retry_sleep_duration=<seconds>` for persistent delivery. RGW then queues
  events and delivers them asynchronously, so S3 writes do not wait for the
  receiver, and events written while the receiver is down arrive after it
  restarts on the same address.
- The topic API is a form-encoded `POST /` on the gateway endpoint. Any
  SigV4-signed client works; the SNS client of the AWS SDK is the simplest.
- Remove notification rules with the RGW extension `DELETE /<bucket>?notification`
  and topics with SNS `DeleteTopic` if the test reuses the bucket.

See the Ceph [bucket notifications](https://docs.ceph.com/en/tentacle/radosgw/notifications/)
documentation for event types and topic attributes.

### STS AssumeRole

STS issues temporary credentials for roles. Every gateway that should accept
the session tokens needs the same `rgw_sts_key`, so set it before starting the
gateways:

```golang
cluster := startCluster(t, ceph.WithOSDCount(1))

// Generate a CryptoKey for session tokens. Never log it.
control, err := cluster.ControlContainer(ctx)
if err != nil {
	t.Fatal(err)
}
_, out, err := control.Exec(ctx,
	[]string{"ceph-authtool", "--gen-print-key"}, tcexec.Multiplexed())
if err != nil {
	t.Fatal(err)
}
key, err := io.ReadAll(out)
if err != nil {
	t.Fatal(err)
}
applyConfig(t, cluster,
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_sts_key", Value: strings.TrimSpace(string(key))},
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_s3_auth_use_sts", Value: "true"},
)
gateway, err := rgw.Start(ctx, cluster, rgw.Config{SkipUserCreation: true})
owner, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "owner"})
assumer, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "assumer"})

// Role with a trust policy for "assumer" and an inline permission policy.
// Admin rejects control characters, so keep JSON documents on one line.
trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam:::user/assumer"},"Action":"sts:AssumeRole"}]}`
created, err := gateway.Admin(ctx, "role", "create",
	"--role-name", "reader", "--path", "/app/", "--assume-role-policy-doc", trust)
if err != nil {
	t.Fatal(err)
}
var role struct {
	ID  string `json:"RoleId"`
	ARN string `json:"Arn"`
}
if err := json.Unmarshal(created, &role); err != nil {
	t.Fatal(err)
}
permission := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::shared/*"}]}`
if _, err := gateway.Admin(ctx, "role-policy", "put", "--role-name", "reader",
	"--policy-name", "read-shared", "--policy-doc", permission); err != nil {
	t.Fatal(err)
}

endpoint, err := gateway.S3Endpoint(ctx)
accessKey, secretKey, err := assumer.Credentials()
stsClient := sts.New(sts.Options{
	BaseEndpoint: aws.String(endpoint),
	Region:       gateway.Region,
	Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
})
assumed, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
	RoleArn:         aws.String(role.ARN),
	RoleSessionName: aws.String("test"),
})
if err != nil {
	t.Fatal(err)
}
c := assumed.Credentials
temporary := s3.New(s3.Options{
	BaseEndpoint: aws.String(endpoint),
	Region:       gateway.Region,
	Credentials: credentials.NewStaticCredentialsProvider(
		*c.AccessKeyId, *c.SecretAccessKey, *c.SessionToken),
	UsePathStyle: true,
})
// temporary can read shared/*; writes and other keys are denied.
```

Notes:

- RGW re-reads the role on each S3 request made with a session token, so a
  change to the role's permission policy (for example an explicit deny)
  applies to existing sessions. A trust policy change controls new
  `AssumeRole` calls; do not rely on it to revoke tokens already issued.
- If you sign STS requests yourself, sign the `Content-Type` header and all
  `x-amz-*` headers. RGW rejects a form POST whose `Content-Type` is present
  but unsigned before it evaluates the trust policy.
- To check that a role does not exist yet, use `role get --role-name <name>`
  through `gateway.Exec` and expect exit status 2. In a fresh zone `role list`
  can fail with ENOENT because the role metadata pool does not exist yet.
- The principal of a tenant user is `arn:aws:iam::<tenant>:user/<id>`.
- OIDC/WebIdentity, Keycloak, LDAP and Keystone need separate identity
  providers and are not covered here.

See the Ceph [STS](https://docs.ceph.com/en/tentacle/radosgw/STS/) and
[role](https://docs.ceph.com/en/tentacle/radosgw/role/) documentation.

### Swift and S3 on the same gateway

The Swift API is served under `/swift` next to S3. With
`rgw_swift_account_in_url=true`, storage URLs include the account
(`/swift/v1/AUTH_<uid>`):

```golang
cluster := startCluster(t, ceph.WithOSDCount(1))
applyConfig(t, cluster,
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_swift_account_in_url", Value: "true"},
)
gateway, err := rgw.Start(ctx, cluster, rgw.Config{SkipUserCreation: true})
user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "app"})

// A Swift subuser with its own key.
swiftUser := user.ID() + ":swift"
if _, err := gateway.Admin(ctx, "subuser", "create", "--uid", user.ID(),
	"--subuser", swiftUser, "--access", "full",
	"--key-type", "swift", "--secret", swiftKey); err != nil {
	t.Fatal(err)
}

endpoint, err := gateway.S3Endpoint(ctx)
req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/auth", nil)
req.Header.Set("X-Auth-User", swiftUser)
req.Header.Set("X-Auth-Key", swiftKey)
resp, err := http.DefaultClient.Do(req)
if err != nil {
	t.Fatal(err)
}
resp.Body.Close()
token := resp.Header.Get("X-Auth-Token")
storage, err := url.Parse(resp.Header.Get("X-Storage-Url"))
// Use endpoint + storage.Path for container and object requests with the
// X-Auth-Token header. The host in X-Storage-Url may be an internal address.
```

Objects written through Swift are readable through S3 with the user's S3 keys,
and the other way round. `subuser rm --subuser <uid>:swift --purge-keys`
revokes the Swift key; tokens issued earlier stop working.

Ceph's default `rgw_enable_apis` already includes the S3, Swift, STS, IAM and
admin APIs. If you set the option, list every API the test uses (for example
`s3,swift,swift_auth,sts,iam,admin,notifications`). Moving Swift to the root
prefix (`rgw_swift_url_prefix=/`) cannot be combined with S3 on the same
gateway.

### Server-side encryption with Vault

SSE-KMS needs an external key management service. HashiCorp Vault's KV v2
engine works with the
[Testcontainers Vault module](https://golang.testcontainers.org/modules/vault/).
Vault is a test service of its own; it is not part of the Ceph images.

Order: start the cluster, start Vault where the gateway can reach it, prepare
the key and a token, apply the RGW options, start the gateway with the token
file.

1. Start Vault with `vault.Run(ctx, "hashicorp/vault:1.21.4", vault.WithToken(rootToken), ...)`.
   In bridge mode attach it to the cluster network with
   `network.WithNetworkName(nil, FactsOf(cluster).NetworkName)`. In host mode keep it
   on a Docker bridge (`network.WithBridgeNetwork()`); the host-network gateway
   reaches the container IP on the same Docker host. Register
   `testcontainers.CleanupContainer` for it.
2. Through the Vault HTTP API (`HttpHostAddress`) with the root token: mount a
   KV v2 engine at `tc`, store a base64-encoded 256-bit key at
   `tc/data/<key-id>` as `{"data":{"key":"<base64>"}}`, write a policy that
   only reads that path, and create a token with that policy. Do not give the
   root token to RGW.
3. Apply the RGW options and start the gateway with the token file:

```golang
vaultIP, err := vaultContainer.ContainerIP(ctx)
applyConfig(t, cluster,
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_crypt_s3_kms_backend", Value: "vault"},
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_crypt_vault_auth", Value: "token"},
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_crypt_vault_token_file", Value: "/tc/vault-token"},
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_crypt_vault_addr", Value: "http://" + net.JoinHostPort(vaultIP, "8200")},
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_crypt_vault_prefix", Value: "/v1/tc/data"},
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_crypt_vault_secret_engine", Value: "kv"},
	// Only because this recipe uses plain HTTP to the gateway.
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_crypt_require_ssl", Value: "false"},
)
gateway, err := rgw.Start(ctx, cluster, rgw.Config{},
	testcontainers.WithFiles(testcontainers.ContainerFile{
		Reader:            strings.NewReader(scopedToken),
		ContainerFilePath: "/tc/vault-token",
		FileMode:          0o600,
	}))

// Upload with SSE-KMS and the key ID stored in Vault.
_, err = client.PutObject(ctx, &s3.PutObjectInput{
	Bucket:               aws.String("secure"),
	Key:                  aws.String("doc"),
	Body:                 bytes.NewReader(payload),
	ServerSideEncryption: types.ServerSideEncryptionAwsKms,
	SSEKMSKeyId:          aws.String("allowed"),
})
```

Every gateway that serves encrypted objects needs its own copy of the token
file. A key ID that the token's policy does not allow makes the upload fail and
leaves no object. Deleting the key in Vault makes reads fail with 404
`NoSuchKey` while the listing still shows the object; writing the same key
material back as a new KV version makes the object readable again.

Production Vault features (HA, seal/unseal, agent token renewal, Transit, TLS
to Vault) and other KMS backends need their own setup. See Ceph's
[Vault integration](https://docs.ceph.com/en/tentacle/radosgw/vault/).

### Usage logs and Admin Ops

The usage log records dated request and byte counters per user and category.
It is disabled by default. Enable it before the gateway starts and shorten the
flush settings:

```golang
cluster := startCluster(t, ceph.WithOSDCount(1))
applyConfig(t, cluster,
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_enable_usage_log", Value: "true"},
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_usage_log_tick_interval", Value: "1"},
	ceph.ConfigSetting{Section: "client.admin", Name: "rgw_usage_log_flush_threshold", Value: "1"},
)
gateway, err := rgw.Start(ctx, cluster, rgw.Config{SkipUserCreation: true})
producer, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "producer"})
operator, err := gateway.CreateUser(ctx, rgw.UserConfig{
	ID: "operator", AdminCaps: "usage=read;ratelimit=read",
})
```

Admin Ops requests are ordinary SigV4 requests (service name `s3`) signed with
the keys of a user that has the needed capability. A small helper with the AWS
SDK signer:

```golang
import v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

func adminRequest(ctx context.Context, method, endpoint, pathAndQuery, region string, user *rgw.User) (*http.Response, error) {
	accessKey, secretKey, err := user.Credentials()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint+pathAndQuery, nil)
	if err != nil {
		return nil, err
	}
	const emptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	req.Header.Set("X-Amz-Content-Sha256", emptyHash)
	creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}
	err = v4.NewSigner().SignHTTP(ctx, creds, req, emptyHash, "s3", region, time.Now(),
		func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}
```

Read the producer's usage after its S3 traffic. Persistence is asynchronous even
with the short flush settings, so poll (up to about 90 seconds):

```golang
query := url.Values{
	"uid": {producer.ID()}, "show-entries": {"true"},
	"show-summary": {"true"}, "format": {"json"},
}
resp, err := adminRequest(ctx, http.MethodGet, endpoint, "/admin/usage?"+query.Encode(),
	gateway.Region, operator)
// Look for the put_obj and get_obj categories with successful operations and
// bytes received and sent.
```

| Request | Capability |
| --- | --- |
| `GET /admin/usage?uid=<uid>&show-entries=true&show-summary=true&format=json` | `usage=read` |
| `DELETE /admin/usage?uid=<uid>&remove-all=false&format=json` | `usage=write` |
| `GET /admin/ratelimit?ratelimit-scope=bucket&bucket=<name>&format=json` | `ratelimit=read` |
| `POST /admin/ratelimit?ratelimit-scope=bucket&bucket=<name>&...` | `ratelimit=write` |

A user without the capability gets 403 `AccessDenied`. Add capabilities later
with `gateway.Admin(ctx, "caps", "add", "--uid", operator.ID(), "--caps",
"usage=write;ratelimit=write")` and remove them with `caps rm`. Capabilities
grant an operation type, not access to particular users or buckets: scope each
request with `uid` or `bucket`. When trimming usage, always pass the `uid`;
never send `remove-all=true` or an unscoped trim, which clears the log of every
user.

See the Ceph [Admin Ops API](https://docs.ceph.com/en/tentacle/radosgw/adminops/).

### Bucket rate limits

A bucket rate limit makes RGW answer excess requests with HTTP 503 `SlowDown`.
It is separate from quotas. Read the current policy first, then post the limit
(the operator from the previous example, with `ratelimit=write` added):

```golang
path := func(values url.Values) string {
	values.Set("ratelimit-scope", "bucket")
	values.Set("bucket", "limited")
	values.Set("format", "json")
	return "/admin/ratelimit?" + values.Encode()
}
// GET path(url.Values{}) returns {"bucket_ratelimit": {...}}; keep it for restore.

limit := url.Values{
	"enabled":         {"true"},
	"max-read-ops":    {"1"},
	"max-write-ops":   {"0"},
	"max-read-bytes":  {"0"},
	"max-write-bytes": {"0"},
}
resp, err := adminRequest(ctx, http.MethodPost, endpoint, path(limit), gateway.Region, operator)
// Repeated GETs of an object in "limited" soon return 503 SlowDown; other
// buckets of the same user are not affected.
```

- On Tentacle, the limits are per-minute token buckets for read and write
  operations and bytes. `0` means no limit for that dimension. Newer fields
  such as `max-list-ops` and `max-delete-ops` do not exist in Ceph 20.
- Each gateway enforces limits for the requests it serves.
- To restore, post the original values of all five fields, not only
  `enabled=false`.
- The same API has other `ratelimit-scope` values (for example user limits);
  see the Ceph Admin Ops documentation.

### Bucket quota and resharding

`SetBucketQuota` applies to every bucket of a user. To limit one named bucket,
and to reshard a bucket on demand, use `Admin`:

```golang
cluster := startCluster(t, ceph.WithOSDCount(1))
// Stop the background resharder from racing the explicit reshard below.
applyConfig(t, cluster, ceph.ConfigSetting{
	Section: "client.admin", Name: "rgw_dynamic_resharding", Value: "false",
})
gateway, err := rgw.Start(ctx, cluster, rgw.Config{})

// After creating "limited" and "sibling" with the S3 client:
for _, args := range [][]string{
	{"quota", "set", "--quota-scope", "bucket", "--bucket", "limited", "--max-objects", "2"},
	{"quota", "enable", "--quota-scope", "bucket", "--bucket", "limited"},
} {
	if _, err := gateway.Admin(ctx, args...); err != nil {
		t.Fatal(err)
	}
}
// A third object in "limited" fails with 403 QuotaExceeded (poll: enforcement
// is asynchronous); "sibling" accepts writes. "quota disable" restores writes.

for _, args := range [][]string{
	{"reshard", "add", "--bucket", "limited", "--num-shards", "17"},
	{"reshard", "list"},
	{"reshard", "process"},
} {
	if _, err := gateway.Admin(ctx, args...); err != nil {
		t.Fatal(err)
	}
}
stats, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", "limited")
```

`reshard process` processes the whole reshard queue, so use it only when the
queue contains your own buckets. See the Ceph
[quota](https://docs.ceph.com/en/tentacle/radosgw/admin/#quota-management) and
[dynamic resharding](https://docs.ceph.com/en/tentacle/radosgw/dynamicresharding/)
documentation.

### Native HTTPS endpoint

`Config.TLS` starts Beast's HTTPS listener next to HTTP. The certificate must
name the host that `S3SecureEndpoint` returns: `localhost` or `127.0.0.1` for a
local Docker engine, or the address given with `ceph.WithHostAddress`.

```golang
// A test CA and a server certificate for localhost.
caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
now := time.Now()
ca := &x509.Certificate{
	SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
	NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
	IsCA: true, BasicConstraintsValid: true,
	KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
}
caDER, _ := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
leaf := &x509.Certificate{
	SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"},
	DNSNames:    []string{"localhost"},
	IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	NotBefore:   now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
	KeyUsage:    x509.KeyUsageDigitalSignature,
	ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
}
leafDER, _ := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, caKey)
keyDER, _ := x509.MarshalPKCS8PrivateKey(leafKey)
caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
chainPEM := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), caPEM...)
keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

gateway, err := rgw.Start(ctx, cluster, rgw.Config{
	Name: "secure",
	TLS:  &rgw.TLSConfig{CertificatePEM: chainPEM, PrivateKeyPEM: keyPEM},
})
if err != nil {
	t.Fatal(err)
}
secureEndpoint, err := gateway.S3SecureEndpoint(ctx)
if err != nil {
	t.Fatal(err)
}

roots := x509.NewCertPool()
roots.AppendCertsFromPEM(caPEM)
client := s3.New(s3.Options{
	BaseEndpoint: aws.String(secureEndpoint),
	Region:       gateway.Region,
	Credentials:  credentials.NewStaticCredentialsProvider(gateway.AccessKey, gateway.SecretKey, ""),
	UsePathStyle: true,
	HTTPClient: &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: roots},
	}},
})
```

The HTTP endpoint (`S3Endpoint`) keeps working and serves the same data on a
different port. A client without the CA fails certificate verification. See
Ceph's [Beast frontend options](https://docs.ceph.com/en/tentacle/radosgw/frontends/#beast).

### Placement targets and storage classes

Create the pools, add a target with two storage classes, activate it, and place
a bucket in it with `LocationConstraint`:

```golang
// Three OSDs for the 2+1 erasure-coded class.
cluster := startCluster(t, ceph.WithOSDCount(3))
for _, pool := range []ceph.PoolConfig{
	{Name: "app-index", Application: "rgw", PGNum: 1},
	{Name: "app-extra", Application: "rgw", PGNum: 1},
	{Name: "app-standard", Application: "rgw", PGNum: 1},
	{Name: "app-cold", Application: "rgw", PGNum: 1,
		ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1}},
} {
	if _, err := cluster.CreatePool(ctx, pool); err != nil {
		t.Fatal(err)
	}
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}

gateway, err := rgw.Start(ctx, cluster, rgw.Config{})
inline := false
placement, err := gateway.CreatePlacement(ctx, rgw.PlacementConfig{
	Name: "tiered", IndexPool: "app-index", DataExtraPool: "app-extra",
	InlineData: &inline,
	StorageClasses: []rgw.StorageClassConfig{
		{Name: "STANDARD", DataPool: "app-standard"},
		{Name: "STANDARD_IA", DataPool: "app-cold"},
	},
})
if err != nil {
	t.Fatal(err)
}
if err := gateway.ApplyPlacement(ctx, placement); err != nil { // restarts the gateway
	t.Fatal(err)
}
endpoint, err := gateway.S3Endpoint(ctx) // resolve again after the restart
client := newS3Client(endpoint, gateway.Region, gateway.AccessKey, gateway.SecretKey)

_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
	Bucket: aws.String("archive"),
	CreateBucketConfiguration: &types.CreateBucketConfiguration{
		LocationConstraint: types.BucketLocationConstraint(placement.LocationConstraint),
	},
})
_, err = client.PutObject(ctx, &s3.PutObjectInput{
	Bucket:       aws.String("archive"),
	Key:          aws.String("old.log"),
	Body:         bytes.NewReader(payload),
	StorageClass: types.StorageClassStandardIa,
})
```

To confirm where the data went, list the data pool:
`client.Exec(ctx, []string{"rados", "-p", "app-cold", "ls"})` with `client` from `cluster.ClientContainer(ctx)`.
RGW object names in the data pool do not always contain the S3 key literally.

With `Tags` on the target, only users with a matching placement tag can create
buckets in it; grant the tag and a default with `SetUserPlacement`. Lifecycle
transitions between classes are configured by the S3 client.

In a realm, every zone needs its own mapping before the master publishes:

```golang
// Create the same pools in both clusters first.
sourcePlacement, err := ms.Source.CreatePlacement(ctx, config)
destinationPlacement, err := ms.Destination.CreatePlacement(ctx, config)
if err := ms.Source.ApplyPlacement(ctx, sourcePlacement); err != nil { // metadata master publishes
	t.Fatal(err)
}
if err := ms.PullDestinationPeriod(ctx); err != nil {
	t.Fatal(err)
}
if err := ms.Destination.ReloadPlacement(ctx, destinationPlacement); err != nil {
	t.Fatal(err)
}
// Resolve both S3 endpoints again. Allow time for sync to resume after the
// gateway restarts before checking replicated objects.
```

See Ceph's [placement and storage classes](https://docs.ceph.com/en/tentacle/radosgw/placement/).


## Running native commands

The typed API covers fixture preparation. Everything else is available through
the native CLIs. See also [guides/native-commands.md](guides/native-commands.md).

### radosgw-admin through a gateway

`Gateway.Admin` runs `radosgw-admin` in the gateway container with the
gateway's scope and returns JSON:

```golang
// Bucket identity and statistics.
stats, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", "reports")

// Lifecycle, resharding, per-bucket quota.
_, err = gateway.Admin(ctx, "lc", "process", "--bucket", "logs")
_, err = gateway.Admin(ctx, "reshard", "add", "--bucket", "big", "--num-shards", "17")
_, err = gateway.Admin(ctx, "quota", "set", "--quota-scope", "bucket", "--bucket", "big", "--max-objects", "100")

// Capabilities, subusers, roles.
_, err = gateway.Admin(ctx, "caps", "add", "--uid", user.ID(), "--caps", "usage=write")
_, err = gateway.Admin(ctx, "subuser", "create", "--uid", user.ID(), "--subuser", user.ID()+":swift",
	"--access", "full", "--key-type", "swift", "--secret", swiftKey)
_, err = gateway.Admin(ctx, "role", "create", "--role-name", "reader", "--assume-role-policy-doc", trust)

// Zone and zonegroup configuration.
zonegroup, err := gateway.Admin(ctx, "zonegroup", "get")
```

`Admin` hides native output on failure. To see the error text while
investigating, run the command in the gateway container yourself; the
container has the cluster configuration and admin keyring at the default
paths:

```golang
code, out, err := gateway.Exec(ctx,
	[]string{"radosgw-admin", "bucket", "stats", "--bucket", "reports"},
	tcexec.Multiplexed())
```

For a multisite gateway add `--rgw-realm`, `--rgw-zonegroup` and `--rgw-zone`,
or use the fixture methods of [`rgw/multisite`](rgw-multisite.md#running-native-commands). The control container also has
`radosgw-admin` (`Exec` on `cluster.ControlContainer(ctx)`), which works for the
default zone of a standalone cluster even when no gateway is running.

### Admin Ops REST API

Admin Ops requests are SigV4-signed HTTP requests to the gateway endpoint.
Create a dedicated user with `UserConfig.AdminCaps` (for example
`"users=read;usage=read;buckets=read"`) and sign with its keys; see the
`adminRequest` helper in [Usage logs and Admin Ops](#usage-logs-and-admin-ops).
The fixture never grants the global `admin` flag.

### Ceph options for RGW

- `override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "client.admin", Name: "rgw_...", Value: "..."})`
  before `rgw.Start`. Many RGW options take effect only when the gateway
  starts; to apply such a change to a running gateway, restart it
  (`Stop`/`Start`) and resolve `S3Endpoint` again.
- `ceph.WithConfigFile(path)` with `[client.admin]` or `[client]` entries for
  options fixed for the whole test. Local files take precedence over central
  configuration.
- Some gateway settings are passed on the daemon command line and cannot be
  changed through configuration: the Beast frontend definition, a thread pool
  of 4 (`rgw_thread_pool_size`), a 1-second exit timeout, and for multisite
  gateways a 30-second sync lease (`rgw_sync_lease_period`).
- `cluster.Ceph(ctx, "df", "--format", "json")` and
  `client.Exec(ctx, []string{"rados", "-p", pool, "ls"})` with `client` from `cluster.ClientContainer(ctx)`
  inspect RGW pools.

## Limitations and release differences

### Native behaviour that affects tests

- **Endpoints change on restart.** In bridge mode a restarted gateway can get a
  new mapped host port. Resolve `S3Endpoint` again after any restart, including
  the ones done by `ApplyPlacement`, `ReloadPlacement`, `ApplySyncGroup`,
  `AddZone`, `AddZonegroup` and `RemoveZone`.
- **HTTP readiness is not sync readiness.** A gateway that answers HTTP may
  still be resuming replication. After an abrupt stop, the old process's sync
  leases must expire (30 seconds for fixture multisite gateways, 120 seconds by
  Ceph default), and metadata and data sync poll every 20 seconds by default.
  Use deadlines of several minutes for replication checks.
- **Caches and asynchronous statistics.** Quota enforcement, suspension,
  placement defaults and usage counters are applied through caches and
  asynchronous accounting. Poll with new requests instead of asserting
  immediately after a change.
- **Usage log persistence is asynchronous** even with a one-second flush
  interval.
- **Rate limits** are enforced by each gateway separately.
- **TLS** is native Beast TLS with no proxy. `WithMessengerMode` does not
  enable HTTPS. Clients must trust the CA themselves.
- **Admin output** may contain keys; failed `Admin` calls never include native
  output.
- **Small fixed thread pool.** Gateways run with 4 worker threads, which is
  enough for functional tests but not for load tests.
- **Docker exec cancellation** does not stop a native worker that is already
  running (for example `bucket sync run`).

### Ceph Squid (19.2.x)

Tentacle 20.2.x is the default. Squid 19.2.5 is supported with the following
differences; `FactsOf(cluster).CephVersion` reports the release of the essential image.

| Area | Tentacle | Squid |
| --- | --- | --- |
| System-mode sync pipes | The stored pipe has no `user` parameter. | RGW stores `"user": ""`. The fixture expects it when it compares readbacks; it matters only when you read `sync group get` output yourself. |
| Account root user mode | A user-mode pipe with an account root user replicates between buckets of that account. | Squid resolves no source for such a pipe and replicates nothing. `CreateSyncPipe` refuses it before any change when any zone runs Ceph 19, with `ceph.ErrUnsupported`; the `*ceph.OperationError` names the zone and its `Version`. User mode with ordinary users works. |
| Bucket sync status | `bucket sync status --format json` prints JSON. | Squid ignores `--format json` and prints text. `BucketSyncStatus` and `WaitBucketSyncReady` parse the text into the same structure and report unknown lines as errors. Raw `Admin` output is text. |
| Missing role lookup | `role get` for a missing role exits with status 2 and prints nothing. | Also exits with status 2, but prints one ENOENT log line. |

Squid 19.2.6 cannot set up multisite: `realm pull` from a secondary is rejected
by the master gateway with HTTP 403. The RGW debug log reports
`'x-amz-content-sha256' supplied, but not in CanonicalHeaders`: that release's
`radosgw-admin` sends the header without signing it, and its gateway rejects
such requests. No setting disables the check. Use 19.2.5 for multisite tests
on Squid; the rest of the module works on 19.2.6.

See [guides/releases-and-images.md](guides/releases-and-images.md).

### Out of scope

- OIDC/WebIdentity, Keycloak, LDAP and Keystone authentication.
- Production Vault setups (HA, seal/unseal, agent renewal, Transit) and other
  KMS backends (KMIP, Barbican).
- Swift large objects, temporary URLs and Keystone catalogues.
- Automatic failover, request routing between zones and recovery point
  guarantees; WAN latency and partial network partitions between zones.
- NFS, SMB and other gateways besides RGW.


For roles clusters, multisite setup clients default to each cluster's RGW
image. `SetupImage` explicitly overrides that runtime. Gateway-local `Admin` remains in
the gateway with its realm/zone scope and lifecycle checks.
