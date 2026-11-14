# Read a replication destination written by an older RGW

An RGW upgrade must preserve the destination ARN returned by the S3 GetBucketReplication API. A Squid 19.2.5 writer stores the full ARN in the native replication pipe. Tentacle writers store the bare bucket name. Some Tentacle readers prepend the ARN prefix to either representation. See [Ceph Tracker 75770](https://tracker.ceph.com/issues/75770) and the [legacy readback correction](https://github.com/ceph/ceph/commit/84f908a883d6ebce9b5aa201e85b6addb49a7787).

## Trigger and controls

One MON, MGR, OSD, admin client and writer gateway use the fixed official **19.2.5** aggregate. A second named RGW on this same cluster uses the selected **reader** image. The writer release is part of the persisted-data prerequisite. The reader is the only image varied between runs. Each native run creates its own owned buckets and backing store, and independently verifies the stored writer format.

The fixture enables a zonegroup sync group and restarts the writer so it can accept a replication configuration. Three buckets are versioned. The writer writes one rule to the legacy bucket and the reader writes the same rule to a different fresh bucket. Each gateway's native `sync policy get --bucket` verifies the destination of the enabled copy pipe. The legacy pipe contains the full ARN before the reader starts and after its GET. The fresh pipe contains the bare name. The reader reads both through the S3 API, using the writer's credentials. Both requests must succeed and return the original enabled rule, and the fresh bucket must return exactly one ARN prefix.

The case prints one [result line](../../../../docs/contributing/testing.md#experiment-result-lines): REPRODUCED only for the doubled legacy prefix, and NOT_REPRODUCED for the original destination. A missing prerequisite, an unsuccessful GET, a malformed rule, or an unexpected destination fails the test. The experiment qualifies configuration readback. It does not exercise inter-zone object transfer. The corrected code is present in the 20.2.3 source tag.

## Tier and triggers

Known-defect reproducer, optional batch `rgw_legacy_replication_destination`
with a 20-minute budget; no automatic run includes it. It is an
`Experiments` case, and its fixed image there is the 20.2.4 aggregate
(`ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all`). A 20.2.2
reader REPRODUCED and a 20.2.4 reader NOT_REPRODUCED. The triggers, the
result checks and the image injection are the shared ones of
[Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case).

## Running the case

The default selects the published **20.2.2** aggregate for the reader; the
second command compares it with the **20.2.4** reader:

```sh
pkg=github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_legacy_replication_destination
go test -tags=all,native_regression -count=1 -v -timeout=20m \
  -run '^TestLegacyReplicationDestination$' "$pkg"
go test -tags=all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X ${pkg}_test.image=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all" \
  -run '^TestLegacyReplicationDestination$' "$pkg"
```

Allow 12 minutes for the test on Linux with Docker. The fixture purges the three buckets and terminates both gateways and the private cluster, including after a failed assertion. The required services are the essential, client and RGW roles.
