//go:build all || integration

package integration_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// journalFiles writes (seed) or reads back the known files through libcephfs
// and prints {"name": sha256} for every file it could read. verify-seed emits
// write counts, read sizes and digests, and refuses read errors. Arguments: mode,
// filesystem, prefix, count.
const journalFiles = `import cephfs, hashlib, json, sys
mode, name, prefix, count = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.conf_set("client_fs", name)
fs.mount()
out = {}
try:
    for n in range(count):
        path = "/%s-%d" % (prefix, n)
        data = hashlib.sha256(path.encode()).digest() * 512
        if mode in ("seed", "verify-seed"):
            fd = fs.open(path, "w", 0o600)
            try:
                written = fs.write(fd, data, 0)
                if written != len(data):
                    raise RuntimeError("incomplete journal file write")
                fs.fsync(fd, False)
            finally:
                fs.close(fd)
        try:
            fd = fs.open(path, "r")
            try:
                got = fs.read(fd, 0, len(data) + 1)
            finally:
                fs.close(fd)
            digest = hashlib.sha256(got).hexdigest()
            out[path] = dict(written=written, size=len(got), sha256=digest) if mode == "verify-seed" else digest
        except cephfs.Error:
            if mode == "verify-seed":
                raise
            out[path] = ""
finally:
    fs.unmount()
    fs.shutdown()
print(json.dumps(out))`

// validateJournalFreshFiles derives the expected content independently from the
// producer and requires every new path, full write, and exact readback.
func validateJournalFreshFiles(raw []byte, prefix string, count int) error {
	var got map[string]struct {
		Written *int   `json:"written"`
		Size    *int   `json:"size"`
		SHA256  string `json:"sha256"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		return err
	}
	if len(got) != count {
		return fmt.Errorf("fresh file count: got %d, want %d", len(got), count)
	}
	for n := range count {
		path := fmt.Sprintf("/%s-%d", prefix, n)
		seed := sha256.Sum256([]byte(path))
		data := bytes.Repeat(seed[:], 512)
		digest := sha256.Sum256(data)
		file, ok := got[path]
		if !ok || file.Written == nil || file.Size == nil || *file.Written != len(data) || *file.Size != len(data) || file.SHA256 != hex.EncodeToString(digest[:]) {
			return fmt.Errorf("fresh file %s: incomplete write or incorrect readback", path)
		}
	}
	return nil
}
