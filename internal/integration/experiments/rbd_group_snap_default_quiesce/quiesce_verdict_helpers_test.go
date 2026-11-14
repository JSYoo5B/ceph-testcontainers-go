//go:build all || integration

package rbd_group_snap_default_quiesce_test

const quiesceVerdict = `def quiesce_verdict(counts):
    if counts["image-required"] != 1 or counts["group-required"] != 1 or counts["image-skip"] != 0:
        raise ValueError(("quiesce control counts", counts))
    if counts["group-skip"]:
        return "REPRODUCED|[group-skip notified watcher]"
    return "NOT_REPRODUCED|[]"
`
