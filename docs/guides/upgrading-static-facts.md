# Reading static cluster facts

The module is in the 0.x line: old names are removed without aliases.

`*ceph.Container` has no accessor for the static facts of a cluster. Read them
with `ceph.FactsOf`, which returns a `ClusterFacts` value and an error:

```golang
facts, err := ceph.FactsOf(cluster)
if err != nil {
	t.Fatal(err)
}
client, err := testcontainers.Run(ctx, facts.Images.Client,
	cluster.WithClient(), ceph.WithIdleEntrypoint())
```

`ClusterFacts` holds the address family, the role images (`Images`, with the
essential image in `Images.Essential`), the messenger mode, the public and
replication network names, whether the cluster uses the host network or a
separate replication network, the advertised public address and the Ceph
version, each under the field that carries its name.

- A nil cluster returns an error that matches `ceph.ErrInvalidRequest` and the
  zero `ClusterFacts`. The former accessors differed on nil: some returned a
  default value and some panicked. Check the error instead of relying on either.
- A terminated cluster still yields its facts, because they come from the
  recorded configuration and need no lock, command or Docker call.
- The value is a copy. A later change to the cluster does not update it, and
  changing it does not change the cluster.

See [FactsOf](../ceph.md#factsof) and [ClusterFacts](../ceph.md#clusterfacts).
