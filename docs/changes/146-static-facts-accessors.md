---
change: 146-static-facts-accessors
kind: api-break
migration: docs/guides/upgrading-static-facts.md
---
`*ceph.Container` no longer has `AddressFamily`, `CephVersion`, `ClusterNetworkName`, `EssentialImage`, `HasSeparateClusterNetwork`, `Images`, `MessengerMode`, `NetworkName`, `PublicAddress` and `UsesHostNetwork`. Read these static facts with `ceph.FactsOf(cluster)`, which returns a `ClusterFacts` value (the essential image is `Images.Essential`) and an error. A nil cluster returns an error that matches `ErrInvalidRequest` and the zero value instead of a default or a panic, and a terminated cluster still yields its facts. Old names are removed without aliases.
