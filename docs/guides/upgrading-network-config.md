# Upgrading: network and Messenger settings

The five network and Messenger options are replaced by one plain value,
`ceph.NetworkConfig`, carried to `Run` by `ceph.WithNetworkConfig`. The functions
are removed without aliases; the `Run` signatures are unchanged and
`ceph.WithHostNetwork` (Docker execution policy) stays a function.

| Before | After |
| --- | --- |
| `ceph.WithAddressFamily(ceph.AddressIPv6)` | `ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressIPv6))` |
| `ceph.WithSeparateClusterNetwork()` | `ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithSeparateClusterNetwork())` |
| `ceph.WithHostAddress("192.0.2.10")` | `ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithHostAddress("192.0.2.10"))` |
| `ceph.WithMessengerMode(ceph.MessengerV2Secure)` | `ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Secure))` |
| `ceph.WithMessengerPolicy(policy)` | `ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerPolicy(policy))` |
| `WithAddressFamily(a), WithSeparateClusterNetwork()` | `ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithAddressFamily(a).WithSeparateClusterNetwork())` |

```golang
network := ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressDualStack).WithSeparateClusterNetwork()
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithNetworkConfig(network), ceph.WithOSDCount(1))
```

What differs from the removed functions:

- Pass the settings of one `NetworkConfig` through one `WithNetworkConfig`. A field
  that two `WithNetworkConfig` options both set is refused with `ErrInvalidRequest`
  even when the values agree; the removed functions let the last one win.
  Different fields in different `WithNetworkConfig` options still compose, in any
  order.
- A setting that equals the default is still a setting:
  `WithAddressFamily(ceph.AddressIPv4)` and `WithMessengerMode(ceph.MessengerDefault)`
  cannot be followed by a second option that sets the same field. Leave a field
  out to leave it unspecified.
- An invalid value is refused as before (`WithHostAddress("")`, an unknown family
  or mode, an empty list in a `MessengerPolicy`), and a refused value changes no
  setting.
- Calling the same method twice on one value replaces the earlier call.
- `WithMessengerPolicy` copies the lists of the policy, so changing the policy's
  slices afterwards does not change the value. A nil list keeps Ceph's default and
  a non-nil empty list is refused.
- The combination rules and their error messages are unchanged apart from the new
  names: a host address needs `WithHostNetwork`, host network cannot be combined
  with a separate cluster network or a family other than `AddressIPv4`,
  `AddressIPv4MappedIPv6` cannot be combined with a separate cluster network, and
  a policy cannot be combined with `MessengerV2Secure`. They are checked before
  anything is created.
- `NetworkConfig` is not a customizer: passing it straight to `Run` does not
  compile. `WithNetworkConfig` changes the cluster settings only and never reaches
  a container request.

Error messages that named the old functions now name `NetworkConfig.WithX`.
Release notes and upgrade guides of earlier versions keep the names that were
valid then. See the
[composition rules](../design/value-configuration-composition.md).
