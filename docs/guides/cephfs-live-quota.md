# A live CephFS file-count quota transition

`TestCephFSLiveQuota` covers one directory's `ceph.quota.max_files` while
one libcephfs mount, client session, process and baseline file descriptor stay
alive. It uses the ordinary userspace client; no FUSE mount, host filesystem,
new mount API or extra capabilities are required.

The owned directory starts without a quota and contains one acknowledged,
fsynced file. An independent administrator sets its file-count limit.
The limit equals its observed recursive entry count. The retained client
polls only the quota value and recursive file, directory and total entry
counts until it sees the selected limit and unchanged baseline counts, under
a 90-second deadline. It then attempts one exclusive file create and must receive `EDQUOT`. An
independent client confirms that this rejected create left no file.

The administrator next raises the limit by one slot. After the same retained
client observes that limit, it attempts the confirmed-absent file once, writes and
fsyncs its predetermined bytes. The original descriptor still reads its exact
baseline bytes. A fresh client checks both inodes, lengths and complete byte
contents. MDS session observations and the final MDS generation check bind
the operation to the original client and active daemon.

The native admission predicate compares `rfiles + rsubdirs` with
`max_files`, not `rfiles` alone ([pinned client source](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/client/Client.cc#L17125)).
The scenario therefore records and admits the actual recursive geometry,
including the directory counted by Ceph, before setting either limit. It does
not assume that two file slots allow a second file when a directory also
contributes to the quota.

The original absent quota is restored and read back before normal actor exit.
On failure, an independently bounded cleanup context restores it before the
actor and cluster cleanups. No mutation is retried after an unknown result;
only read-only quota observations are polled. A failed or unreached gate fails
the scenario instead of admitting an unobserved transition.

Run the source-owned `short/cephfs_live_quota` batch on an eligible Linux
Docker engine with the selected essential, client and cephfs images:

```sh
python3 .github/scripts/tag_scenarios.py run \
  --category short --batch cephfs_live_quota --package ./internal/integration \
  --directory artifacts/live-quota
```

The `CEPHFS_LIVE_QUOTA_RECEIPT` records FSID, filesystem, MDS identity,
actor/client/inode identities, quota observations and their sample counts,
each create result, independent reads and verified quota restoration.
Per-phase receipts belong to the actor run's token; synthetic controls reject
changed identities, missing observations, `ENOSPC` substituted for `EDQUOT`,
contradictory create evidence and wrong bytes. These controls do not establish
native propagation on their own.

This is a file-count quota boundary, distinct from OSD fullness policy and
physical capacity exhaustion. It does not qualify byte quotas, metrics,
session reclaim, fcntl takeover, a kernel/FUSE client, exact update latency or
zero overshoot. [Ceph's quota documentation](https://docs.ceph.com/en/tentacle/cephfs/quota/)
describes cooperative and imprecise enforcement; the read-only observation
deadline is a test bound, not a service guarantee. The coverage row remains
partial, and neither a native outcome nor the presence of the quota feature
bit establishes wire coverage or consumer implementation.
