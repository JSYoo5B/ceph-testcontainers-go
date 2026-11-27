# Disposable OSD block I/O faults

`fault/blockio` owns a sparse backing file, loop device, device-mapper mapping
and privileged keeper container. It accepts no existing disk, mapping or loop.
The backing file lives on the keeper's bounded tmpfs: written pages consume
Docker-host memory. This is a fixture for observing a client's behavior under
an OSD main-device fault, not a storage benchmark.

Start the backend before the cluster and terminate the cluster before the
backend. Register backend cleanup first so reverse-order cleanup preserves the
keeper while its OSD consumes the device. Keep a non-nil backend returned with
an error and retry its cleanup; do not remove its embedded container directly.
The [compiled example](example_test.go) shows the full sequence: attach an OSD,
apply the delay, check its status and restore the device table before teardown.

Choose exactly one positive whole-millisecond `Delay` (at most `MaxDelay`) or
`EIO: true`. A successful `Apply` confirms the installed table; it does not
prove a client experienced delay or an I/O error. `Status.Requested` is the
requested profile. `Identity` and `Tables` are separately observed native state;
`PendingStep` means completion remains unconfirmed. Inspect that state and retry
`Restore` after an uncertain operation instead of replaying `Apply`.

`Restore` restores the recorded linear table. It does not restart an OSD that
exited, prove recovery or recover acknowledged bytes. An EIO fault can abort the
OSD; clients may observe bounded timeouts rather than an EIO response. Restart
the OSD separately, wait for clean state, and verify old and new data with the
client under test. A busy or unconfirmed consumer prevents `Terminate` from
releasing the mapping and keeper. Terminate the cluster, retry backend cleanup,
and use `Status.Released` to confirm release.

`blockio.HelperImage` is the helper that ceph-testcontainers-images publishes
for contract 1 of its blockio extension, for linux/amd64 and linux/arm64.
`Run` never selects it by itself: pass it, or another image that meets the
contract, as `img`. A reference you pass, a digest reference included, is
used as given.

The helper image must contain Python 3, `sh`, `sleep`, `dmsetup`, `losetup` and
`blockdev`. The Docker host must already expose usable loop/device-mapper
controls and register the selected targets. The fixture does not install tools,
load host kernel modules or alter daemon configuration. Missing host facilities
are `ceph.ErrEnvironmentUnavailable`; missing image tools are an image capability
error. A registered `linear` target does not imply `delay` is registered.
[`ceph/preflight`](../../ceph/preflight/README.md) runs the same probe before
any fixture starts and reports Delay and EIO separately.

The opt-in native recipe uses only public APIs and the native `rados` CLI. It
checks delay against baseline and restored writes when available, separates a
missing delay target from successful delay verification, checks EIO against
client and OSD evidence, then verifies acknowledged bytes and a new write after
restoration and explicit OSD restart. It also checks busy-consumer retention
and retryable final release. Select compatible Ceph images in the
environment:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch block_io \
  --directory artifacts/blockio-native
```

The batch runs `blockio.HelperImage`; set `CEPH_TEST_BLOCKIO_HELPER_IMAGE` to
run another contract-1 helper instead. With a role layout, set both
`CEPH_TEST_ESSENTIAL_IMAGE` and `CEPH_TEST_CLIENT_IMAGE`; otherwise
`CEPH_TEST_IMAGE` selects the aggregate image. An environment-unavailable setup
fails the selected batch with its classified cause; it is not a successful
native execution. An unavailable delay target is likewise recorded explicitly;
it is not a delay validation result.

The keeper and its private tmpfs must stay alive while a chain is retained.
A forced helper removal, helper restart or test-process/engine loss is outside
the retryable cleanup path; ordinary Docker resource reaping is not a device-
mapper or loop cleanup guarantee. Preserve the original cleanup error and the
keeper identity rather than forcing removal of a retained backend.

For disposable GitHub-hosted qualification, manually dispatch **Block fault
probe** with `public_api=true`. Set `prepare_kernel=true` only when admitting
module setup on that disposable runner. The library never loads modules. The
job pulls the helper by tag with the other test images, so a failed pull stops
it before the tests, and requires actual Delay and EIO effects, recovery and
release on every supported Ceph release.
An unavailable target fails this qualification instead of counting as a pass.
