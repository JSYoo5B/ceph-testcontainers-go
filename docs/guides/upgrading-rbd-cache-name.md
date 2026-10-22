# Upgrading the RBD immutable object cache operation

This source break targets the next minor release. Published tags remain
immutable; main provides no permanent compatibility alias.

| Previous operation | Current operation | Returned handle |
| --- | --- | --- |
| `rbd.TemporaryImmutableObjectCache` | `rbd.StartImmutableObjectCache` | `*rbd.ImmutableObjectCacheLease` (unchanged) |

Update calls, function values, wrappers, interface declarations and mocks that
refer to the old function. The configuration, defaults and status fields keep
their names. Native test names and batch selectors are unchanged.

```go
lease, err := rbd.StartImmutableObjectCache(ctx, client, rbd.ImmutableObjectCache{
    ID: "rbd-client",
})
if lease != nil {
    defer lease.Restore(context.WithoutCancel(ctx))
}
if err != nil {
    return err
}
options := lease.ClientOptions()
_ = options // apply these options to each librbd consumer that uses the cache
```

The operation starts one daemon in a caller-owned client container with the
cache executable and parent-cache plugin, such as the selected `rbd-aux` role.
It does not create or adopt a cluster and does not attach client credentials
for the caller. `ImmutableObjectCache` is the request; the returned lease owns
the daemon/socket/files and can represent an uncertain start even with an error.
Keep a non-nil error lease so that cleanup remains possible.

`Stop` stops the daemon while retaining the cache directory and socket.
`Restore` stops it and removes its owned files, removing the directory only
when the lease created it; cleanup failures remain retryable. `Status` is an
observation, and `ClientOptions` must still be applied to each consumer.
There is no native TTL, context-cancellation rollback or scope cleanup. The
rename introduces none of those behaviors and no new image capability.

See the [cache reference](../rbd.md#startimmutableobjectcache) and the
[current-source naming audit](../design/api-naming-audit.md).
