# RBD mirror

Import `github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror` to connect two
disposable Ceph clusters with native RBD mirroring, so that a test can check
how its application behaves with a replicated image: replication readiness,
failover and failback, split-brain recovery, the loss of a receiver. The
package starts the setup CLI containers and the `rbd-mirror` receiver daemons
and observes their readiness; it does not wrap mirroring commands such as
promote, demote or resync. See
[API purpose and purpose tags](../../docs/guides/api-purpose.md).

Import it with an alias, because the CephFS family also uses the package name
`mirror`:

```golang
import rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
```

## Order of use

1. **Start two clusters** (`rbd.Run`, or `ceph.Run` plus `rbd.InitPool`) with
   the same replicated pool, and create any named namespaces with
   `rbd.CreateNamespace`.
2. **Start the mirror** with `rbdmirror.Run(ctx, img, rbdmirror.WithConfig(cfg))`.
   `img` is the daemon image (the destination's `Images.RBDAux`, or an `all`
   image); `cfg` names `Source`, `Destination`, `Pool` and optionally the
   namespaces, `Scope`, `Mode` and the number of daemons. Replication is one
   way; create a second mirror with the clusters swapped for failback.
3. **Enable images**: `EnableImage` in image scope (snapshot or journal mode);
   pool scope enrolls journaled images automatically.
4. **Run the SUT** against the source image, and take mirror snapshots
   (`SourceRBD(ctx, "mirror", "image", "snapshot", ...)`) as checkpoints in
   snapshot mode.
5. **Observe and verify**: `WaitReceiverReady` (daemons discovered the pool and
   agree on a leader), `WaitReplayReady` / `ImageStatus` (an owned receiver
   replays the image), then compare the destination data; readiness never
   proves that a write arrived. `BindNamespace` observes extra namespace pairs.
   `Daemon.Remove`, `AddDaemon`, `Daemon.InterruptPeerLink` and `Rebootstrap`
   change the receiving side for HA and recovery scenarios.
6. **Clean up**: register the clusters' cleanup first and the mirror's after,
   so the mirror is terminated first; terminate a non-nil `*Mirror` even after
   an error.

```golang
facts, err := ceph.FactsOf(destination)
if err != nil {
	t.Fatal(err)
}
link, err := rbdmirror.Run(ctx, facts.Images.RBDAux, rbdmirror.WithConfig(rbdmirror.Config{
	Source: source, Destination: destination, Pool: "volumes",
}))
testcontainers.CleanupContainer(t, link)
if err != nil {
	t.Fatal(err)
}
if _, err := link.WaitReceiverReady(ctx); err != nil {
	t.Fatal(err)
}
```

## Concepts and limits

- The mirror owns its setup clients and daemons, not the clusters. `Terminate`
  leaves the mirroring policies, peers and Cephx users in the clusters.
- `Run` never silently reconfigures existing state: a different scope, remote
  namespace or site name is an error. Site names are cluster-wide. Typed
  methods check the policies captured at setup; do not race them with native
  changes from other clients.
- Replication is asynchronous and there is no automatic failover; split-brain
  recovery discards the losing side. Pool scope supports journal mode only.
- Both clusters must use the same network mode; `InterruptPeerLink` needs
  bridge mode. Two clusters plus daemons need noticeably more memory; in a small
  Docker VM run multi-cluster tests one at a time.
- Ceph 19 (Squid) cannot map different source and destination namespaces:
  `Run` fails with `ceph.ErrUnsupported`, which is the contract to assert there.
  Squid's `rbd mirror pool info` prints no `mirror_uuid` or `remote_namespace`,
  and its Python `rbd` binding has no `Group.id()` or `get_snap_info()`.
  Tentacle (`ceph.DefaultImage`) is the default release.
- Common pitfalls: wait until the source lists the destination as a transmit
  peer before taking snapshot checkpoints you rely on; retry a non-forced
  `promote` until a demotion has propagated; name the survivors when waiting
  for receivers while one is stopped.

## Reference

- API: [pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror)
  or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror`.
- Native `rbd mirror` commands and the setup clients:
  [Native commands](../../docs/guides/native-commands.md#rbd-mirror-setup-clients);
  multi-cluster faults: [Fault injection](../../docs/guides/fault-injection.md);
  [Releases and images](../../docs/guides/releases-and-images.md);
  [Errors](../../docs/guides/errors.md#unsupported-features).
