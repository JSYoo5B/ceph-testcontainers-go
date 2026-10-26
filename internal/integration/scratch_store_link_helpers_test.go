//go:build all || integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Issue 63 attachment step: before an OSD is judged to live on the owned
// size-limited tmpfs, show that the OSD's view of the store directory and the
// keeper's view of the owned volume are one filesystem instance. The pair
// below does that without Docker: one script plants a marker file through the
// keeper's mount, another observes it read-only from any container's view, and
// a judge joins two views. Device and inode of the same marker, its content
// digest, the tmpfs type and the configured byte ceiling all have to agree.
// The check says nothing about what the OSD writes there; it only ties two
// paths to one owned filesystem.

// scratchStoreMarkerScript creates MARKER exclusively (never replacing an
// existing entry), writes the token, syncs and closes it. It runs as
// `python3 -I -c <script> MARKER TOKEN` inside the keeper. It removes nothing.
const scratchStoreMarkerScript = `import json, os, sys

path, token = sys.argv[1], sys.argv[2].encode()
r = {"version": 1, "errno": 0, "written": 0}
try:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        r["written"] = os.write(fd, token)
        os.fsync(fd)
    finally:
        os.close(fd)
except OSError as e:
    r["errno"] = e.errno or -1
print(json.dumps(r), flush=True)
`

// scratchStoreViewScript observes PATH without writing. It opens PATH once with
// O_RDONLY|O_NOFOLLOW|O_NONBLOCK and takes fstat, the content read (up to 257
// bytes as length and digest) and fstatvfs from that one descriptor, so the
// identity, content and capacity describe one opened inode. After the read it
// looks PATH up again; "stable" is true only when that lookup is a non-symlink
// with the descriptor's device and inode. A replaced path is reported as not
// stable instead of being merged into one receipt. The covering mountinfo line
// is chosen from the descriptor's device by the resolved path prefix; that is a
// separate path-based observation and is not bound to the descriptor. The line
// "# boundary-hook" marks where a fixture may inject a concurrent change. It
// runs as `python3 -I -c <script> PATH`.
const scratchStoreViewScript = `import hashlib, json, os, stat, sys

def covering(path, dev):
    key = "%d:%d" % (os.major(dev), os.minor(dev))
    best = None
    try:
        real = os.path.realpath(path)
        for line in open("/proc/self/mountinfo"):
            left, _, right = line.partition(" - ")
            f, g = left.split(), right.split()
            if len(f) < 5 or len(g) < 3 or f[2] != key:
                continue
            mp = f[4]
            if real == mp or real.startswith(mp.rstrip("/") + "/"):
                if best is None or len(mp) > len(best["mountpoint"]):
                    best = {"mount_id": f[0], "dev": key, "mountpoint": mp, "fstype": g[0], "super_options": g[2]}
    except OSError:
        pass
    return best

r = {"version": 2, "errno": 0, "stable": False, "regular": False, "dev": 0, "ino": 0, "nlink": 0, "size": 0,
     "content_len": 0, "content_sha256": "", "frsize": 0, "blocks": 0, "mount": None}
path = sys.argv[1]
try:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        s = os.fstat(fd)
        # boundary-hook
        r.update(regular=stat.S_ISREG(s.st_mode), dev=s.st_dev, ino=s.st_ino, nlink=s.st_nlink, size=s.st_size)
        v = os.fstatvfs(fd)
        r.update(frsize=v.f_frsize, blocks=v.f_blocks)
        if r["regular"]:
            data = os.read(fd, 257)
            r.update(content_len=len(data), content_sha256=hashlib.sha256(data).hexdigest())
        r["mount"] = covering(path, s.st_dev)
        # boundary-hook-after
        t = os.lstat(path)
        r["stable"] = (not stat.S_ISLNK(t.st_mode)) and t.st_dev == s.st_dev and t.st_ino == s.st_ino
    finally:
        os.close(fd)
except OSError as e:
    r["errno"] = e.errno or -1
print(json.dumps(r), flush=True)
`

type storeMountLine struct {
	MountID      *string `json:"mount_id"`
	Dev          *string `json:"dev"`
	Mountpoint   *string `json:"mountpoint"`
	FSType       *string `json:"fstype"`
	SuperOptions *string `json:"super_options"`
}

type storeViewReceipt struct {
	Version       *int            `json:"version"`
	Errno         *int            `json:"errno"`
	Stable        *bool           `json:"stable"`
	Regular       *bool           `json:"regular"`
	Dev           *uint64         `json:"dev"`
	Ino           *uint64         `json:"ino"`
	Nlink         *int64          `json:"nlink"`
	Size          *int64          `json:"size"`
	ContentLen    *int64          `json:"content_len"`
	ContentSHA256 *string         `json:"content_sha256"`
	Frsize        *int64          `json:"frsize"`
	Blocks        *int64          `json:"blocks"`
	Mount         *storeMountLine `json:"mount"`
}

// parseStoreView refuses a missing, null or wrong-typed field. A path that
// could not be examined carries a nonzero errno. A null mount (no covering
// mountinfo line) is accepted by the parser and reported by the judge, never
// read as an empty filesystem type.
func parseStoreView(out []byte) (*storeViewReceipt, error) {
	var r storeViewReceipt
	if err := decodeStrict(out, &r); err != nil {
		return nil, fmt.Errorf("store view receipt: %w", err)
	}
	if r.Version == nil || *r.Version != 2 || r.Errno == nil || r.Stable == nil || r.Regular == nil || r.Dev == nil || r.Ino == nil ||
		r.Nlink == nil || r.Size == nil || r.ContentLen == nil || r.ContentSHA256 == nil || r.Frsize == nil || r.Blocks == nil {
		return nil, errors.New("store view receipt: version or fields missing, null or incomplete")
	}
	if m := r.Mount; m != nil && (m.MountID == nil || m.Dev == nil || m.Mountpoint == nil || m.FSType == nil || m.SuperOptions == nil) {
		return nil, errors.New("store view receipt: mount line has a missing or null field")
	}
	return &r, nil
}

type storeMarkerReceipt struct {
	Version *int `json:"version"`
	Errno   *int `json:"errno"`
	Written *int `json:"written"`
}

func parseStoreMarker(out []byte) (*storeMarkerReceipt, error) {
	var r storeMarkerReceipt
	if err := decodeStrict(out, &r); err != nil {
		return nil, fmt.Errorf("store marker receipt: %w", err)
	}
	if r.Version == nil || *r.Version != 1 || r.Errno == nil || r.Written == nil {
		return nil, errors.New("store marker receipt: version or fields missing, null or incomplete")
	}
	return &r, nil
}

const (
	storeLinkSame      = "same-filesystem-instance" // both views reach one marker on one bounded tmpfs
	storeLinkDifferent = "different-filesystem"     // the two views reach different filesystem objects
)

// tmpfsSuperSizeBytes reads the `size=<n>k` superblock option that the kernel
// prints for a size-limited tmpfs. Any other spelling, or no size option, is an
// error: an unbounded or differently reported mount is never taken as bounded.
func tmpfsSuperSizeBytes(options string) (int64, error) {
	for _, o := range strings.Split(options, ",") {
		v, ok := strings.CutPrefix(o, "size=")
		if !ok {
			continue
		}
		n, ok := strings.CutSuffix(v, "k")
		if !ok {
			return 0, fmt.Errorf("size option %q is not reported in KiB", o)
		}
		kib, err := strconv.ParseInt(n, 10, 64)
		if err != nil || kib <= 0 || kib > (1<<62)/1024 {
			return 0, fmt.Errorf("size option %q is not a positive KiB count", o)
		}
		return kib * 1024, nil
	}
	return 0, errors.New("no size option on the mount")
}

// classifyStoreLink joins the keeper's and the OSD's observation of the same
// marker. wantBytes is the configured ceiling and token is the marker content
// the keeper planted. A view that cannot be examined, is not a regular file or
// does not hold the planted token is an error, never a verdict. Differing
// device or inode is the verdict "different-filesystem". For one device and
// inode the judge then requires a tmpfs mount line whose reported size and
// statvfs total both equal the configured ceiling in both views; anything else
// is an error, and the verdict is "same-filesystem-instance" only when all of
// that holds.
//
// Device numbers are comparable because both views come from containers on one
// kernel. That expectation is unconfirmed until a native run joins an OSD view
// to a keeper view. The join identifies the mounted filesystem only: it does
// not show that the OSD's block file or any daemon store entry is on it, and it
// says nothing about allocation, writes or recovery.
func classifyStoreLink(keeper, osd *storeViewReceipt, token string, wantBytes int64) (string, error) {
	sum := sha256.Sum256([]byte(token))
	wantDigest := hex.EncodeToString(sum[:])
	views := []struct {
		name string
		v    *storeViewReceipt
	}{{"keeper", keeper}, {"osd", osd}}
	for _, w := range views {
		v := w.v
		if *v.Errno != 0 {
			return "", fmt.Errorf("%s view could not be examined (errno %d)", w.name, *v.Errno)
		}
		if !*v.Stable {
			return "", fmt.Errorf("%s view path changed while it was observed", w.name)
		}
		if !*v.Regular || *v.Nlink < 1 {
			return "", fmt.Errorf("%s view marker is not a linked regular file", w.name)
		}
		if *v.ContentLen != int64(len(token)) || *v.Size != int64(len(token)) || *v.ContentSHA256 != wantDigest {
			return "", fmt.Errorf("%s view marker does not hold the planted token", w.name)
		}
	}
	if *keeper.Dev != *osd.Dev || *keeper.Ino != *osd.Ino {
		return storeLinkDifferent, nil
	}
	for _, w := range views {
		v := w.v
		m := v.Mount
		if m == nil || *m.FSType == "" || *m.Mountpoint == "" || *m.MountID == "" {
			return "", fmt.Errorf("%s view has no covering mount line", w.name)
		}
		if *m.FSType != "tmpfs" {
			return "", fmt.Errorf("%s view is on %q, not tmpfs", w.name, *m.FSType)
		}
		size, err := tmpfsSuperSizeBytes(*m.SuperOptions)
		if err != nil {
			return "", fmt.Errorf("%s view: %w", w.name, err)
		}
		if size != wantBytes {
			return "", fmt.Errorf("%s view mount ceiling is %d bytes, want %d", w.name, size, wantBytes)
		}
		if wantBytes <= 0 || *v.Frsize <= 0 || *v.Blocks <= 0 || wantBytes%*v.Frsize != 0 || wantBytes / *v.Frsize != *v.Blocks {
			return "", fmt.Errorf("%s view statvfs total is %d*%d bytes, want %d", w.name, *v.Frsize, *v.Blocks, wantBytes)
		}
	}
	if *keeper.Mount.Dev != *osd.Mount.Dev {
		return "", errors.New("the same device and inode carry different mount device identifiers")
	}
	return storeLinkSame, nil
}
