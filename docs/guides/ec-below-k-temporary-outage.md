# Temporary unavailability below K

`TestECBelowKTemporaryUnavailability` uses one fresh 2+1 Jerasure
`reed_sol_van` pool with explicit `min_size=2`, one PG and EC overwrites.
The existing default-min_size controls remain separate. This scenario keeps its
policy unchanged while crossing the mathematical K=2 boundary; it never lowers
min_size to make the one remaining fragment serve data.

The test shares the retained admission actor, payload and snapshot/readback
models with the paired 4+2 scenario. The actor's synthetic object names are
fixture names, not a declaration that this pool is 4+2. Actual profile, pool
features, stripe geometry, fragment positions, primary and map epochs are read
from the native cluster. Three OSDs share one Docker engine and use the configured
OSD failure domain; physical host failures are not represented.

1. Seed known bytes and a snapshot. Stop one freshly admitted healthy
   non-primary generation and acknowledge an overwrite with exactly K holders.
2. Stop the other non-primary, retaining the original primary and both stopped
   stores. Require the exact positional mapping with one holder and native `down`
   peering state, rather than reusing the K-holder `peered` policy state. On the same original clients submit one read and one unique-object
   write. Timeouts remain unknown outcomes and neither operation is replayed.
3. Close input, verify delivery accounting, require both SDK shutdown receipts
   and complete actor exit before starting either store. If shutdown is
   unconfirmed, cleanup leaves store removal to cluster termination.
4. Restore only one holder. At K, a new reader verifies the acknowledged head
   and original snapshot bytes/versions. New independent clients acknowledge a
   distinct overwrite and verify the snapshot; each native command finishes
   before the final store returns.
5. Restore the final holder and wait for clean placement. A fresh reader checks
   the latest acknowledged head and snapshot bytes/versions, and inspects the
   uncertain object's absence or whole requested value. Absence does not prove
   pre-application refusal. Original OSD/container identities and unchanged
   primary generation are checked separately.

Fixtures reject compacted fragments, changed policy/primary, stale map reports,
incorrect admission states and substituted or acknowledged below-K operations.
The scenario reuses holder-generation, acknowledgement, shutdown and independent
readback controls rather than adding another fault implementation.

Run the `recovery/ec_below_k` batch with the repository's pinned `essential` and
`client` images; see [Testing](../contributing/testing.md). The bounded result is
temporary unavailability with retained shards, not permanent data loss or a
second interruption during reconstruction.
