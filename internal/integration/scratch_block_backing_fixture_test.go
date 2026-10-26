//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// These checks run the block backing observer on ordinary temporary files and
// classify synthetic receipts. They exercise no tmpfs mount, no OSD, no Ceph
// daemon and no Docker. The mount identity of the producer controls is
// substituted, because a local temporary directory is not the owned scratch
// filesystem; nothing here shows how an OSD's block file is backed.

func backingEntry(regular bool, dev, ino uint64, size, allocated int64, mountID, mountpoint, fstype string) *blockBackingEntry {
	zero, symlink := 0, false
	return &blockBackingEntry{
		Errno: &zero, Symlink: &symlink, Regular: &regular, Ino: &ino, Dev: &dev, Size: &size, Allocated: &allocated,
		Mount: &scratchMount{ID: &mountID, Mountpoint: &mountpoint, FSType: &fstype},
	}
}

func backingReceipt(block, scratch *blockBackingEntry) *blockBackingReceipt {
	v := 1
	return &blockBackingReceipt{Version: &v, Block: block, Scratch: scratch}
}

func scratchTmpfsEntry() *blockBackingEntry {
	return backingEntry(false, 50, 2, 0, 0, "0:50", "/var/lib/ceph/osd", "tmpfs")
}

func TestBlockBackingClassifierSeparatesSparseFromPreallocated(t *testing.T) {
	const size = 64 << 20
	for name, tc := range map[string]struct {
		block *blockBackingEntry
		want  string
	}{
		"sparse":                  {backingEntry(true, 50, 7, size, 0, "0:50", "/var/lib/ceph/osd", "tmpfs"), blockBackingSparse},
		"partially allocated":     {backingEntry(true, 50, 7, size, 4096, "0:50", "/var/lib/ceph/osd", "tmpfs"), blockBackingBelowLength},
		"allocated one byte less": {backingEntry(true, 50, 7, size, size-1, "0:50", "/var/lib/ceph/osd", "tmpfs"), blockBackingBelowLength},
		"total allocation equals length, coverage unknown": {backingEntry(true, 50, 7, size, size, "0:50", "/var/lib/ceph/osd", "tmpfs"), blockBackingAtLeastLen},
		"other device":        {backingEntry(true, 51, 7, size, 0, "0:51", "/", "overlay"), blockBackingElsewhere},
		"other mount id only": {backingEntry(true, 50, 7, size, 0, "0:99", "/var/lib/ceph/osd", "tmpfs"), blockBackingElsewhere},
		"same id other mount": {backingEntry(true, 50, 7, size, 0, "0:50", "/other", "tmpfs"), blockBackingElsewhere},
		"device node":         {backingEntry(false, 50, 7, 0, 0, "0:50", "/var/lib/ceph/osd", "tmpfs"), blockBackingNotFile},
	} {
		got, err := classifyBlockBacking(backingReceipt(tc.block, scratchTmpfsEntry()))
		if err != nil || got != tc.want {
			t.Errorf("%s: %q, %v, want %q", name, got, err, tc.want)
		}
	}
	// Refusals: never a verdict.
	for name, r := range map[string]*blockBackingReceipt{
		"scratch is not tmpfs":   backingReceipt(backingEntry(true, 50, 7, size, 0, "0:50", "/x", "ext4"), backingEntry(false, 50, 2, 0, 0, "0:50", "/x", "ext4")),
		"scratch has no mount":   backingReceipt(backingEntry(true, 50, 7, size, 0, "0:50", "", ""), backingEntry(false, 50, 2, 0, 0, "0:50", "", "")),
		"zero length block file": backingReceipt(backingEntry(true, 50, 7, 0, 0, "0:50", "/var/lib/ceph/osd", "tmpfs"), scratchTmpfsEntry()),
	} {
		if got, err := classifyBlockBacking(r); err == nil {
			t.Errorf("%s: classified as %q, want an error", name, got)
		}
	}
	for name, mutate := range map[string]func(*blockBackingReceipt){
		"block missing":   func(r *blockBackingReceipt) { *r.Block.Errno = 2 },
		"scratch missing": func(r *blockBackingReceipt) { *r.Scratch.Errno = 2 },
	} {
		r := backingReceipt(backingEntry(true, 50, 7, size, 0, "0:50", "/var/lib/ceph/osd", "tmpfs"), scratchTmpfsEntry())
		mutate(r)
		if got, err := classifyBlockBacking(r); err == nil {
			t.Errorf("%s: classified as %q, want an error", name, got)
		}
	}
}

func TestBlockGrowthIsObservationalAndNeverInfersOverwriteOnly(t *testing.T) {
	const size = 1 << 20
	at := func(ino uint64, size, allocated int64) *blockBackingReceipt {
		return backingReceipt(backingEntry(true, 50, ino, size, allocated, "0:50", "/var/lib/ceph/osd", "tmpfs"), scratchTmpfsEntry())
	}
	for name, tc := range map[string]struct {
		before, after *blockBackingReceipt
		want          string
		fail          bool
	}{
		"grew into a hole":                  {at(7, size, 0), at(7, size, 8192), blockGrowthGrew, false},
		"equal endpoints, workload unknown": {at(7, size, 4096), at(7, size, 4096), blockGrowthNoNetGrow, false},
		"equal endpoints at full length":    {at(7, size, size), at(7, size, size), blockGrowthNoNetGrow, false},
		"hole punched":                      {at(7, size, size), at(7, size, size/2), "", true},
		"length changed":                    {at(7, size, 0), at(7, size*2, 0), "", true},
		"file replaced":                     {at(7, size, 0), at(8, size, 4096), "", true},
	} {
		got, err := classifyBlockGrowth(tc.before, tc.after)
		if (err != nil) != tc.fail || got != tc.want {
			t.Errorf("%s: %q, %v, want %q fail=%v", name, got, err, tc.want, tc.fail)
		}
	}
	notFile := at(7, size, 0)
	*notFile.Block.Regular = false
	if _, err := classifyBlockGrowth(notFile, at(7, size, 0)); err == nil {
		t.Error("a non-regular block entry was compared")
	}
}

func TestBlockBackingParserRefusesMissingNullAndWrongTypes(t *testing.T) {
	requireScratchPython(t)
	dir := t.TempDir()
	block := filepath.Join(dir, "block")
	if err := os.WriteFile(block, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	good := runScratchScript(t, scratchBlockBackingScript, block, dir)
	if _, err := parseBlockBacking(good); err != nil {
		t.Fatalf("producer receipt refused: %v: %s", err, good)
	}
	text := string(bytes.TrimSpace(good))
	mountField := regexp.MustCompile(`"mount": \{[^}]*\}`)
	mountID := regexp.MustCompile(`"id": "[^"]*", `)
	for name, bad := range map[string]string{
		"empty":                    "",
		"not JSON":                 "garbage",
		"missing version":          strings.Replace(text, `"version": 1, `, "", 1),
		"wrong version":            strings.Replace(text, `"version": 1`, `"version": 2`, 1),
		"null block":               strings.Replace(text, `"block": {`, `"block": null, "x": {`, 1),
		"missing scratch":          text[:strings.Index(text, `, "scratch"`)] + "}",
		"null allocated":           strings.Replace(text, `"allocated": 0`, `"allocated": null`, 1),
		"string size":              strings.Replace(text, `"size": 0`, `"size": "0"`, 1),
		"unknown field":            strings.Replace(text, `"version": 1`, `"version": 1, "extra": true`, 1),
		"two receipts":             text + "\n" + text,
		"null mount":               mountField.ReplaceAllString(text, `"mount": null`),
		"missing mount identifier": mountID.ReplaceAllString(text, ""),
		"null mount identifier":    mountID.ReplaceAllString(text, `"id": null, `),
	} {
		if r, err := parseBlockBacking([]byte(bad)); err == nil {
			t.Errorf("%s: accepted %+v", name, r)
		}
	}
}

func TestBlockBackingObserverSeesSparseAndFullFilesWithoutChangingThem(t *testing.T) {
	requireScratchPython(t)
	dir := t.TempDir()
	const size = 4 << 20
	sparse := filepath.Join(dir, "sparse")
	full := filepath.Join(dir, "full")
	f, err := os.Create(sparse)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, bytes.Repeat([]byte{0xa5}, size), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(full, link); err != nil {
		t.Fatal(err)
	}
	observe := func(path string) *blockBackingReceipt {
		out := runScratchScript(t, scratchBlockBackingScript, path, dir)
		r, err := parseBlockBacking(out)
		if err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		return r
	}
	sp, fu, lk := observe(sparse), observe(full), observe(link)
	if *sp.Block.Size != size || !*sp.Block.Regular || *sp.Block.Symlink {
		t.Errorf("sparse file identity: %+v", sp.Block)
	}
	if *sp.Block.Allocated >= size {
		t.Skipf("this filesystem allocates a truncated file (%d of %d bytes); the sparse control is not observable", *sp.Block.Allocated, size)
	}
	if *fu.Block.Allocated < size {
		t.Errorf("fully written file allocates only %d of %d bytes", *fu.Block.Allocated, size)
	}
	if !*lk.Block.Symlink || *lk.Block.Ino != *fu.Block.Ino || *lk.Block.Dev != *fu.Block.Dev {
		t.Errorf("symlink was not resolved to the target file: %+v", lk.Block)
	}
	// Observing twice yields the same allocation: the observer does not touch the file.
	if g, err := classifyBlockGrowth(sp, observe(sparse)); err != nil || g != blockGrowthNoNetGrow {
		t.Errorf("repeated observation: %q, %v", g, err)
	}
	// A real write into a hole grows allocation at the later endpoint.
	wf, err := os.OpenFile(sparse, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wf.WriteAt(bytes.Repeat([]byte{1}, 1<<20), 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}
	if g, err := classifyBlockGrowth(sp, observe(sparse)); err != nil || g != blockGrowthGrew {
		t.Errorf("write into a hole: %q, %v", g, err)
	}
	// A non-truncating overwrite of allocated bytes leaves the endpoints equal.
	before := observe(full)
	of, err := os.OpenFile(full, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := of.WriteAt(bytes.Repeat([]byte{0x5a}, size), 0); err != nil {
		t.Fatal(err)
	}
	if err := of.Close(); err != nil {
		t.Fatal(err)
	}
	if g, err := classifyBlockGrowth(before, observe(full)); err != nil || g != blockGrowthNoNetGrow {
		t.Errorf("non-truncating overwrite: %q, %v", g, err)
	}
	// Truncate to zero and refill: same inode and equal endpoints, so the judge
	// stays at no net growth. It must not read that as overwrite-only.
	if err := os.Truncate(full, 0); err != nil {
		t.Fatal(err)
	}
	if mid := observe(full); *mid.Block.Allocated != 0 {
		t.Skipf("filesystem kept %d bytes after truncation", *mid.Block.Allocated)
	}
	if err := os.WriteFile(full, bytes.Repeat([]byte{0x33}, size), 0o600); err != nil {
		t.Fatal(err)
	}
	if g, err := classifyBlockGrowth(before, observe(full)); err != nil || g != blockGrowthNoNetGrow {
		t.Errorf("truncate and refill: %q, %v", g, err)
	}
	// Local temp directories are not the owned tmpfs, whatever this host is.
	if v, err := classifyBlockBacking(sp); err == nil && v != blockBackingElsewhere && v != blockBackingSparse {
		t.Errorf("unexpected verdict %q for a local file", v)
	}
	// A missing block entry is an errno, not an identity.
	miss, err := parseBlockBacking(runScratchScript(t, scratchBlockBackingScript, filepath.Join(dir, "absent"), dir))
	if err != nil {
		t.Fatal(err)
	}
	if *miss.Block.Errno == 0 {
		t.Error("missing block entry reported no errno")
	}
	if v, err := classifyBlockBacking(miss); err == nil {
		t.Errorf("missing block entry classified as %q", v)
	}
}

// The verdict wording is part of the contract: aggregate allocation of at
// least the length must not read as backing coverage, and equal endpoints must
// not read as an overwrite-only or unreached workload.
func TestBlockBackingVerdictsDoNotOverclaim(t *testing.T) {
	if blockBackingAtLeastLen != "allocation-at-least-length" {
		t.Errorf("aggregate allocation verdict reads %q", blockBackingAtLeastLen)
	}
	if blockGrowthNoNetGrow != "no-net-growth" {
		t.Errorf("equal-endpoint verdict reads %q", blockGrowthNoNetGrow)
	}
	for _, v := range []string{blockBackingAtLeastLen, blockBackingBelowLength, blockBackingSparse, blockGrowthGrew, blockGrowthNoNetGrow} {
		for _, bad := range []string{"preallocated", "unreached", "overwrite", "unchanged", "partial"} {
			if strings.Contains(v, bad) {
				t.Errorf("verdict %q implies %q", v, bad)
			}
		}
	}
}
