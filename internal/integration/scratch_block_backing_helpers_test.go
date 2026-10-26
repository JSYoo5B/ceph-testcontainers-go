//go:build all || integration

package integration_test

import (
	"errors"
	"fmt"
)

// scratchBlockBackingScript observes what stands behind an OSD store's `block`
// entry without writing or truncating anything (issue 63). It runs as
// `python3 -I -c <script> BLOCK SCRATCH` so the same text can later run inside
// the container that mounts the store. BLOCK is the `block` path inside an OSD
// data directory and SCRATCH is the directory of the owned size-limited
// filesystem the OSD store is meant to live on. The receipt records, for each
// path, the device, mount identity and filesystem type, and for the block
// entry whether it is a symlink, a regular file, its length and its allocated
// bytes. It only calls lstat, stat and reads /proc/self/mountinfo.
const scratchBlockBackingScript = `import errno, json, os, stat, sys

def mountinfo(dev):
    key = "%d:%d" % (os.major(dev), os.minor(dev))
    try:
        for line in open("/proc/self/mountinfo"):
            left, _, right = line.partition(" - ")
            f = left.split()
            if len(f) > 4 and f[2] == key:
                return {"id": key, "mountpoint": f[4], "fstype": right.split()[0]}
    except OSError:
        pass
    return {"id": key, "mountpoint": "", "fstype": ""}

def entry(path):
    r = {"errno": 0, "symlink": False, "regular": False, "ino": 0, "dev": 0,
         "size": 0, "allocated": 0, "mount": None}
    try:
        r["symlink"] = stat.S_ISLNK(os.lstat(path).st_mode)
        s = os.stat(path)
    except OSError as e:
        r["errno"] = e.errno or -1
        return r
    r.update(regular=stat.S_ISREG(s.st_mode), ino=s.st_ino, dev=s.st_dev,
             size=s.st_size, allocated=s.st_blocks * 512, mount=mountinfo(s.st_dev))
    return r

print(json.dumps({"version": 1, "block": entry(sys.argv[1]), "scratch": entry(sys.argv[2])}), flush=True)
`

type blockBackingEntry struct {
	Errno     *int          `json:"errno"`
	Symlink   *bool         `json:"symlink"`
	Regular   *bool         `json:"regular"`
	Ino       *uint64       `json:"ino"`
	Dev       *uint64       `json:"dev"`
	Size      *int64        `json:"size"`
	Allocated *int64        `json:"allocated"`
	Mount     *scratchMount `json:"mount"`
}

type blockBackingReceipt struct {
	Version *int               `json:"version"`
	Block   *blockBackingEntry `json:"block"`
	Scratch *blockBackingEntry `json:"scratch"`
}

func (e *blockBackingEntry) complete() bool {
	return e != nil && e.Errno != nil && e.Symlink != nil && e.Regular != nil && e.Ino != nil &&
		e.Dev != nil && e.Size != nil && e.Allocated != nil
}

// parseBlockBacking refuses a receipt with a missing, null or wrong-typed
// field. A path that could not be examined carries a nonzero errno and is
// reported by the classifier, never decoded into an empty identity.
func parseBlockBacking(out []byte) (*blockBackingReceipt, error) {
	var r blockBackingReceipt
	if err := decodeStrict(out, &r); err != nil {
		return nil, fmt.Errorf("block backing receipt: %w", err)
	}
	if r.Version == nil || *r.Version != 1 || !r.Block.complete() || !r.Scratch.complete() {
		return nil, errors.New("block backing receipt: version or entry fields missing, null or incomplete")
	}
	for _, e := range []*blockBackingEntry{r.Block, r.Scratch} {
		if *e.Errno == 0 && (e.Mount == nil || e.Mount.ID == nil || e.Mount.Mountpoint == nil || e.Mount.FSType == nil) {
			return nil, errors.New("block backing receipt: mount identity missing for an examined path")
		}
	}
	return &r, nil
}

const (
	blockBackingElsewhere   = "other-filesystem"           // block is not on the owned scratch filesystem
	blockBackingNotFile     = "not-a-regular-file"         // block resolves to a device or another file type
	blockBackingSparse      = "sparse"                     // no allocated bytes anywhere
	blockBackingBelowLength = "allocation-below-length"    // some bytes allocated, fewer than the length
	blockBackingAtLeastLen  = "allocation-at-least-length" // aggregate only: coverage of offsets within EOF is unknown
)

// classifyBlockBacking decides whether the block entry stands on the owned
// scratch filesystem and how aggregate allocation compares with its length.
// No allocation, or less than the length, means some offset within EOF has no
// backing, so a write there can need new filesystem allocation. Allocation
// of at least the length is only an aggregate: st_blocks does not say which
// offsets are backed (tmpfs fallocate with FALLOC_FL_KEEP_SIZE can allocate
// wholly beyond EOF), so coverage within EOF is unknown and this verdict never
// shows that an overwrite cannot allocate. That claim needs an independent
// within-range coverage or admitted-layout witness. Unexaminable paths are
// errors, not verdicts.
func classifyBlockBacking(r *blockBackingReceipt) (string, error) {
	if *r.Scratch.Errno != 0 {
		return "", fmt.Errorf("scratch directory could not be examined (errno %d)", *r.Scratch.Errno)
	}
	if *r.Block.Errno != 0 {
		return "", fmt.Errorf("block entry could not be examined (errno %d)", *r.Block.Errno)
	}
	sm, bm := r.Scratch.Mount, r.Block.Mount
	if *sm.FSType != "tmpfs" || *sm.ID == "" || *sm.Mountpoint == "" {
		return "", errors.New("the scratch directory is not on an identified tmpfs mount")
	}
	if *r.Block.Dev != *r.Scratch.Dev || *bm.ID != *sm.ID || *bm.Mountpoint != *sm.Mountpoint || *bm.FSType != *sm.FSType {
		return blockBackingElsewhere, nil
	}
	if !*r.Block.Regular {
		return blockBackingNotFile, nil
	}
	size, allocated := *r.Block.Size, *r.Block.Allocated
	switch {
	case size <= 0:
		return "", errors.New("block file has no length")
	case allocated == 0:
		return blockBackingSparse, nil
	case allocated < size:
		return blockBackingBelowLength, nil
	}
	return blockBackingAtLeastLen, nil
}

const (
	blockGrowthGrew      = "allocation-grew" // allocation at the later endpoint is larger
	blockGrowthNoNetGrow = "no-net-growth"   // endpoints equal: inconclusive about the workload
)

// classifyBlockGrowth compares two endpoint observations of the same block
// file. It is observational only. Larger allocation at the later endpoint is
// reported as growth. Equal endpoints are "no net growth" and inconclusive:
// they do not show an overwrite-only workload, because a write into a hole
// refused with ENOSPC leaves them equal, and an intermediate truncate and
// refill leaves them equal too. The workload's own operation results are kept
// by the caller and are never inferred from these snapshots. Shrinking
// allocation (a punched hole), a changed length or a replaced file is refused.
func classifyBlockGrowth(before, after *blockBackingReceipt) (string, error) {
	for _, r := range []*blockBackingReceipt{before, after} {
		if *r.Block.Errno != 0 || !*r.Block.Regular {
			return "", errors.New("block file is not examined as a regular file in both observations")
		}
	}
	b, a := before.Block, after.Block
	if *b.Ino != *a.Ino || *b.Dev != *a.Dev {
		return "", errors.New("block file identity changed between observations")
	}
	if *b.Size != *a.Size {
		return "", fmt.Errorf("block file length changed from %d to %d", *b.Size, *a.Size)
	}
	switch {
	case *a.Allocated < *b.Allocated:
		return "", fmt.Errorf("block file allocation shrank from %d to %d", *b.Allocated, *a.Allocated)
	case *a.Allocated > *b.Allocated:
		return blockGrowthGrew, nil
	}
	return blockGrowthNoNetGrow, nil
}
