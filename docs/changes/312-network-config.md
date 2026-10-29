---
change: 312-network-config
kind: api-break
migration: docs/guides/upgrading-network-config.md
---
`ceph.WithAddressFamily`, `ceph.WithSeparateClusterNetwork`, `ceph.WithHostAddress`, `ceph.WithMessengerMode` and `ceph.WithMessengerPolicy` are removed. The network and Messenger settings are one plain chainable value, `ceph.NetworkConfig` (`WithAddressFamily`, `WithSeparateClusterNetwork`, `WithHostAddress`, `WithMessengerMode`, `WithMessengerPolicy`), passed to `Run`, `rbd.Run`, `cephfs.Run` and `rgw.Run` through one option function, `ceph.WithNetworkConfig`; the `Run` signatures are unchanged and `WithHostNetwork` stays a function. A field set by two `WithNetworkConfig` options is refused even when equal (the removed functions let the last one win), and an explicit default such as `AddressIPv4` is a setting, so a later option cannot silently change it. A `MessengerPolicy` is copied when it enters the value. The combination rules, defaults and runtime behavior are unchanged. There is no deprecated alias or forwarding layer.
