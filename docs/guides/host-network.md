# Host network and native clients

## Introduction

A native Ceph client (librados, librbd, libcephfs, go-ceph, the `rados` and
`rbd` CLIs) does not talk to a single endpoint. It asks a MON for the cluster
maps and then connects directly to every OSD, MDS and MGR at the addresses
those maps advertise. Where your client runs therefore decides how the cluster
must be networked.

| Client location | Network mode | How to connect |
| --- | --- | --- |
| A Linux container next to the cluster | Bridge (default) | `cluster.WithClient()` or `cluster.WithClientIdentity(client)` |
| The test process on a Linux Docker host | Host | `ceph.WithHostNetwork()` and `cluster.ConnectionConfig(ctx)` |
| The test process on macOS or Windows with Docker Desktop | Host, with Docker Desktop host networking enabled | Same as above; see [Docker Desktop](#docker-desktop) |
| An S3 client anywhere | Either | `rgw.Gateway.S3Endpoint(ctx)` (plain HTTP) |

S3 is the exception: RGW serves HTTP on a single port (published in bridge
mode, bound directly on the host in host mode), so a host S3 SDK works in both
modes.

The public Go module does not depend on go-ceph, librados or cgo. Native
clients are your own dependency; the module only supplies addresses,
configuration and credentials.

## Bridge mode: clients in containers

By default each cluster gets its own Docker bridge network. Daemons advertise
their container IP addresses on that bridge. A container attached to the same
bridge can reach every daemon; a process on a macOS or Windows host cannot,
even though the MON port is published. Do not assume that the MON's mapped
port is enough for RADOS, RBD or CephFS.

`WithClient` prepares another container as a Ceph client:

- it joins the cluster's bridge network (or the host network in host mode);
- it receives `/etc/ceph/ceph.conf` and `/etc/ceph/ceph.client.admin.keyring`.

It does not install Ceph client software. Use an image that contains the
clients you need. The cluster's client role (`Images().Client`) has the `ceph`,
`rados` and `rbd` CLIs and the Python `rados`, `rbd` and `cephfs` bindings.
`radosgw-admin` belongs to the rgw role.

```golang
client, err := testcontainers.Run(ctx, cluster.Images().Client,
	cluster.WithClient(),
	ceph.WithIdleEntrypoint(),
)
if client != nil {
	testcontainers.CleanupContainer(t, client)
}
if err != nil {
	t.Fatal(err)
}
code, reader, err := client.Exec(ctx,
	[]string{"rados", "-p", "volumes", "ls"}, tcexec.Multiplexed())
```

`ceph.WithIdleEntrypoint()` keeps the container running with `sleep infinity`
under Docker's init process, so commands can be executed later and the
container stops at once instead of waiting for Docker's 10 second timeout.

The admin keyring copied by `WithClient` grants full control of the test
cluster. Give it only to trusted test containers. For a restricted identity,
create a client and attach it with `WithClientIdentity`:

```golang
reader, err := cluster.CreateClient(ctx, "reader", ceph.ClientCaps{
	Mon: "allow r",
	OSD: "allow r pool=volumes",
})
if err != nil {
	t.Fatal(err)
}
app, err := testcontainers.Run(ctx, "registry.example.com/my-app:test",
	cluster.WithClientIdentity(reader),
)
testcontainers.CleanupContainer(t, app)
if err != nil {
	t.Fatal(err)
}
// The container has /etc/ceph/ceph.conf and only reader's keyring at
// reader.KeyringPath(). Connect as reader.User(), for example with
// "rados --id reader".
```

The caller owns these containers. Register their cleanup after the cluster's,
so they are terminated first (see [Contexts, ownership and cleanup](contexts-and-cleanup.md)).

Other bridge-mode endpoints:

- `cluster.NetworkName()` returns the bridge name, for containers you attach
  yourself.
- `rgw.Gateway.S3Endpoint(ctx)` returns a mapped host port for host S3 clients.
  `DaemonEndpoint(ctx)` returns the gateway's address on the cluster network
  for clients inside it.
- `Observer.ManagerServices(ctx)` returns the URLs that MGR modules such as
  `prometheus` advertise. They use container addresses reachable from
  `WithClient` containers, not mapped host ports.

## Host mode

`ceph.WithHostNetwork()` places every daemon, and every container attached
with `WithClient` or `WithClientIdentity`, in the Docker daemon's host network.
Daemons advertise an address of the Docker host, so a process on that host can
reach all of them.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithHostNetwork(),
	// ceph.WithHostAddress("192.0.2.10"), // a remote Linux Docker host
)
if cluster != nil {
	testcontainers.CleanupContainer(t, cluster)
}
if errors.Is(err, ceph.ErrHostNetworkUnavailable) {
	t.Skipf("host networking is not usable from this process: %v", err)
}
if err != nil {
	t.Fatal(err)
}
cfg, err := cluster.ConnectionConfig(ctx)
if err != nil {
	t.Fatal(err)
}
// Write cfg.Conf and cfg.Keyring to files and pass them to a native client.
_ = cfg
```

| Option or method | Meaning |
| --- | --- |
| `ceph.WithHostNetwork()` | Selects host networking for the whole cluster. |
| `ceph.WithHostAddress(address)` | IPv4 address to bind and advertise. Defaults to `127.0.0.1`. The address must be assigned on the Docker host and reachable from the test process. Requires `WithHostNetwork`. |
| `cluster.UsesHostNetwork()` | Reports the selected mode. |
| `cluster.PublicAddress()` | The advertised address; empty in bridge mode. |
| `cluster.NetworkName()` | Returns `"host"` in host mode. |

The default `127.0.0.1` suits a test process on the same machine as the Docker
engine (a local Linux engine, or Docker Desktop with host networking). With a
remote Docker engine, pass an address of that host which your test process can
reach.

### Requirements

- The Docker engine must run Linux containers.
- On Linux, host mode shares the host's network namespace directly.
- Docker Desktop needs host networking enabled; see [Docker Desktop](#docker-desktop).
- The test process must reach the advertised address. A test process running
  inside a bridge-networked container (for example a dev container) usually
  cannot reach `127.0.0.1` of the Docker host.

### Run-time verification and ErrHostNetworkUnavailable

`Run` checks the environment before it creates MGR and OSD daemons:

1. It checks that the Docker engine's OS type is `linux`.
2. It checks that the `WithHostAddress` address can be bound on the Docker
   host. An address that does not exist there fails before the first MON is
   created.
3. After the first MON starts, it connects from the test process to the MON's
   advertised v2 endpoint. A messenger v2 listener sends the banner
   `ceph v2\n` before reading anything, so receiving that banner proves that
   the test process reached this MON. Any other response means another process
   owns the port, and `Run` fails at once. Refused or timed-out connections are
   retried for up to five seconds, to allow for Docker Desktop's port
   forwarding.

On failure the error wraps `ceph.ErrHostNetworkUnavailable`, which belongs to
`ceph.ErrEnvironmentUnavailable`; test it with `errors.Is`. The message
includes a hint for the detected Docker environment.
Common causes:

- Docker Desktop host networking is disabled.
- `WithHostAddress` names an address inside the Docker Desktop VM (such as the
  VM's `eth0` address), which the host cannot reach. Use `127.0.0.1`.
- A remote Docker engine is used with the default `127.0.0.1`.
- The test process runs in a bridge container.

Docker Desktop does not expose its host networking setting through the engine
API, so the module tests the actual path instead of reading the setting.

Only the first MON's v2 endpoint is probed. MONs and gateways added later, and
the ports Ceph picks for MGR, OSD and MDS daemons, are not probed. Later MON
and RGW port reservations can also return `ErrHostNetworkUnavailable`.

### Port allocation

Host mode does not publish ports; every daemon binds directly on the Docker
host. Several clusters can run on one Docker host without manual port
selection:

| Role | Port selection |
| --- | --- |
| MON | The messenger ports (v2, and v1 unless `MessengerV2Secure` or `MessengerV2Only` is selected) are chosen before the MON starts and written into the configuration and the monmap. |
| MGR, OSD, MDS | Ceph picks free ports dynamically and registers them in the cluster maps. |
| RGW | One free port is chosen for HTTP (and one for HTTPS when TLS is configured). It is used as the bind address and returned by `S3Endpoint`. |

MON and RGW ports are reserved in the Docker host's network namespace, which
may differ from the test process's namespace. A short-lived helper container
from the essential image binds free ports and holds the sockets while the daemon
container is created, then releases them just before the daemon starts. That
handover cannot be atomic. If the daemon then fails with an actual
`EADDRINUSE`, the module retries up to five times; a MON is recreated with a
new port, monmap and store. Other startup errors are returned, and partially
created resources remain owned by the cluster for cleanup. RGW readiness first
checks that the selected socket belongs to the RGW process and then probes
HTTP.

The helper containers are released by the cluster, also in `Terminate`.

### Connecting from the test process

`ConnectionConfig(ctx)` returns a `ceph.ConnectionConfig` with independent copies of the
bootstrap `ceph.conf` (`Conf`) and the admin keyring (`Keyring`). In host mode the MON addresses in it are final.
`ConnectionConfig(ctx)` does the same while bounding the wait for
concurrent topology changes. The copy is a snapshot: after `AddMonitor` or
`MonitorContainer.Remove`, call it again.

For a restricted identity, use `client.ConnectionConfig()` on a
`*ceph.ClientConfig` from `CreateClient`. Its configuration is a snapshot taken
when the client was created.

Example with go-ceph on Linux. go-ceph needs cgo and the Ceph development
libraries of a matching release on the machine that builds and runs the test;
follow go-ceph's documentation for the build tags of your Ceph release. Keep
this code in a test file of your own module with the build constraint
`//go:build linux && cgo`.

```golang
cfg, err := cluster.ConnectionConfig(ctx)
if err != nil {
	t.Fatal(err)
}
dir := t.TempDir()
confPath := filepath.Join(dir, "ceph.conf")
keyringPath := filepath.Join(dir, "ceph.client.admin.keyring")
if err := os.WriteFile(confPath, cfg.Conf, 0o600); err != nil {
	t.Fatal(err)
}
if err := os.WriteFile(keyringPath, cfg.Keyring, 0o600); err != nil {
	t.Fatal(err)
}
conn, err := rados.NewConn()
if err != nil {
	t.Fatal(err)
}
if err := conn.ReadConfigFile(confPath); err != nil {
	t.Fatal(err)
}
if err := conn.SetConfigOption("keyring", keyringPath); err != nil {
	t.Fatal(err)
}
if err := conn.Connect(); err != nil {
	t.Fatal(err)
}
defer conn.Shutdown()
```

The keyring is the admin credential of the test cluster. Write it only to
private temporary files and do not log it.

### Client containers in host mode

`WithClient` and `WithClientIdentity` put the container in the host network
too. They remove the request's other networks, network aliases, exposed ports
and port bindings, because host-network containers cannot publish ports. The
copied `ceph.conf` already points at the advertised host address.

### Docker Desktop

Docker Desktop (4.34 or later) offers host networking as a setting that is off
by default. Enable it in the Docker Desktop settings (Resources, Network,
"Enable host networking") before using `WithHostNetwork` on macOS or Windows.
Docker Desktop implements it as layer 4 forwarding between the host and its
Linux VM, so it does not behave exactly like a Linux engine
([Docker documentation](https://docs.docker.com/engine/network/drivers/host/)).

With host networking enabled:

- a macOS process reaches the advertised MON, MGR and OSD ports over TCP,
  including the ports of OSDs added later;
- a macOS process can use RGW's `S3Endpoint` for signed S3 requests.

A TCP connection is not CephX authentication or native I/O. Authenticated
librados I/O from a macOS-native client has not been verified; native I/O was
verified from Linux client containers in the VM's host namespace and from
Linux processes. When the setting is off, `Run` fails with
`ErrHostNetworkUnavailable` as described above.

### Caveats

- `WithHostNetwork` and `WithSeparateClusterNetwork` cannot be combined; `Run`
  rejects the combination before creating anything. `WithHostAddress` without
  `WithHostNetwork` is rejected too.
- Docker cannot disconnect a host-network endpoint, so `InterruptNetwork`,
  `InterruptPeerLink` and `InterruptZoneLink` are refused in host mode.
  `ApplyNetworkProfile` is refused too: a delay there would delay the host.
  `fault.Pause` and the Ceph messenger options work in both modes (see
  [Fault injection](fault-injection.md)).
- Every host-network container shares the host's hostname. Identify CephFS
  sessions by `ID` rather than `Hostname`, and do not rely on `hostname` to
  tell containers apart.
- Multicluster fixtures (`rbdmirror.Run`, `cephfsmirror.Run`,
  `multisite.Run`, `multisite.RunTopology`) require both clusters to use the same
  network mode. In host mode the advertised addresses of each cluster must be
  reachable from the other.

## Separate cluster network

`ceph.WithSeparateClusterNetwork()` creates a second bridge for OSD
replication, recovery and heartbeat traffic. Docker selects both subnets.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithSeparateClusterNetwork())
if cluster != nil {
	testcontainers.CleanupContainer(t, cluster)
}
if err != nil {
	t.Fatal(err)
}
public := cluster.NetworkName()         // MON, MGR, MDS, RGW, clients and OSDs
backend := cluster.ClusterNetworkName() // OSDs only
_ = cluster.HasSeparateClusterNetwork() // true
_, _ = public, backend
```

- Only OSDs join both networks. MON, MGR, MDS, RGW, mirror daemons and
  `WithClient` containers use the public bridge only.
- Ceph's `public network` and `cluster network` settings make OSDs choose the
  right address for each kind of traffic.
- The option requires bridge mode. In host mode `ClusterNetworkName()` returns
  `"host"`; with a single bridge it returns the same name as `NetworkName()`.
- `fault.InterruptNetwork(ctx, cluster, osd, ceph.ClusterNetworkPlane)` cuts only the
  backend of one OSD while its public listener keeps serving clients.

## Related APIs

- [ceph.md](../ceph.md): `WithHostNetwork`, `WithHostAddress`,
  `WithSeparateClusterNetwork`, `ConnectionConfig`, `WithClient`,
  `WithClientIdentity`, `CreateClient`, `ErrHostNetworkUnavailable`.
- [rgw.md](../rgw.md): `S3Endpoint`, `S3SecureEndpoint`, `DaemonEndpoint`.
- [Running native commands](native-commands.md): client containers and the
  CLIs available in them.
- [Contexts, ownership and cleanup](contexts-and-cleanup.md): who terminates
  client containers.
- [Testing this repository](../contributing/testing.md): the host-network and
  Linux go-ceph test suites of this module.
