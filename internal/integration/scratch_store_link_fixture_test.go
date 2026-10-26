//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// These checks join synthetic receipts and run the marker and view scripts on
// ordinary temporary files. They involve no tmpfs mount, no OSD, no keeper, no
// Ceph daemon and no Docker. The mount and statvfs fields of the producer
// controls are substituted, because a local temporary directory is not the
// owned volume; nothing here shows how a real OSD's mount relates to the
// keeper's.

const (
	storeToken = "tc-store-link-0123456789abcdef"
	storeBytes = 32 << 20
)

func storeView(dev, ino uint64, token, fstype, options string) *storeViewReceipt {
	v, errno, regular, nlink, stable := 2, 0, true, int64(1), true
	size := int64(len(token))
	sum := sha256.Sum256([]byte(token))
	digest := hex.EncodeToString(sum[:])
	frsize, blocks := int64(4096), int64(storeBytes/4096)
	mountID, mountDev, mp := "812", "0:55", "/store"
	return &storeViewReceipt{
		Version: &v, Errno: &errno, Stable: &stable, Regular: &regular, Dev: &dev, Ino: &ino, Nlink: &nlink, Size: &size,
		ContentLen: &size, ContentSHA256: &digest, Frsize: &frsize, Blocks: &blocks,
		Mount: &storeMountLine{MountID: &mountID, Dev: &mountDev, Mountpoint: &mp, FSType: &fstype, SuperOptions: &options},
	}
}

func goodStoreView() *storeViewReceipt {
	return storeView(55, 9, storeToken, "tmpfs", "rw,size=32768k,mode=700")
}

func TestStoreLinkJudgeJoinsOneBoundedTmpfsInstance(t *testing.T) {
	got, err := classifyStoreLink(goodStoreView(), goodStoreView(), storeToken, storeBytes)
	if err != nil || got != storeLinkSame {
		t.Fatalf("matching views: %q, %v", got, err)
	}
	// A different inode or device is a verdict, not an error: the OSD is elsewhere.
	otherIno := goodStoreView()
	*otherIno.Ino = 10
	otherDev := goodStoreView()
	*otherDev.Dev = 56
	for name, osd := range map[string]*storeViewReceipt{"other inode": otherIno, "other device": otherDev} {
		got, err := classifyStoreLink(goodStoreView(), osd, storeToken, storeBytes)
		if err != nil || got != storeLinkDifferent {
			t.Errorf("%s: %q, %v", name, got, err)
		}
	}
	// The mount mountpoint may differ between containers; only the object and the bound matter.
	elsewhere := goodStoreView()
	*elsewhere.Mount.Mountpoint = "/var/lib/ceph/osd"
	if got, err := classifyStoreLink(goodStoreView(), elsewhere, storeToken, storeBytes); err != nil || got != storeLinkSame {
		t.Errorf("different mountpoint: %q, %v", got, err)
	}
}

func TestStoreLinkJudgeRefusesUnexaminedAndContradictoryViews(t *testing.T) {
	mutations := map[string]func(*storeViewReceipt){
		"errno":                  func(v *storeViewReceipt) { *v.Errno = 2 },
		"path changed":           func(v *storeViewReceipt) { *v.Stable = false },
		"int64 product wraps":    func(v *storeViewReceipt) { *v.Frsize, *v.Blocks = 1<<33, 1<<32+1 },
		"uint64 product wraps":   func(v *storeViewReceipt) { *v.Frsize, *v.Blocks = 1<<32, 1<<32 },
		"negative blocks":        func(v *storeViewReceipt) { *v.Blocks = -int64(storeBytes / 4096) },
		"negative fragment":      func(v *storeViewReceipt) { *v.Frsize = -4096 },
		"fragment not a divisor": func(v *storeViewReceipt) { *v.Frsize, *v.Blocks = 3, storeBytes/3 },
		"not a regular file":     func(v *storeViewReceipt) { *v.Regular = false },
		"unlinked marker":        func(v *storeViewReceipt) { *v.Nlink = 0 },
		"other token":            func(v *storeViewReceipt) { *v.ContentSHA256 = strings.Repeat("0", 64) },
		"short content":          func(v *storeViewReceipt) { *v.ContentLen = 3 },
		"grown size":             func(v *storeViewReceipt) { *v.Size = 99 },
		"no covering mount":      func(v *storeViewReceipt) { v.Mount = nil },
		"empty filesystem type":  func(v *storeViewReceipt) { *v.Mount.FSType = "" },
		"empty mountpoint":       func(v *storeViewReceipt) { *v.Mount.Mountpoint = "" },
		"not tmpfs":              func(v *storeViewReceipt) { *v.Mount.FSType = "overlay" },
		"no size option":         func(v *storeViewReceipt) { *v.Mount.SuperOptions = "rw,mode=700" },
		"size in MiB spelling":   func(v *storeViewReceipt) { *v.Mount.SuperOptions = "rw,size=32m" },
		"size not a number":      func(v *storeViewReceipt) { *v.Mount.SuperOptions = "rw,size=xk" },
		"zero size":              func(v *storeViewReceipt) { *v.Mount.SuperOptions = "rw,size=0k" },
		"other size":             func(v *storeViewReceipt) { *v.Mount.SuperOptions = "rw,size=65536k" },
		"statvfs total differs":  func(v *storeViewReceipt) { *v.Blocks-- },
		"statvfs zero fragment":  func(v *storeViewReceipt) { *v.Frsize = 0 },
		"mount device mismatch":  func(v *storeViewReceipt) { *v.Mount.Dev = "0:56" },
		"statvfs overflow guard": func(v *storeViewReceipt) { *v.Frsize, *v.Blocks = 1<<40, 1<<40 },
	}
	for name, mutate := range mutations {
		for _, side := range []string{"keeper", "osd"} {
			keeper, osd := goodStoreView(), goodStoreView()
			target := keeper
			if side == "osd" {
				target = osd
			}
			mutate(target)
			// A mount device mismatch is only contradictory when seen against the other view.
			if got, err := classifyStoreLink(keeper, osd, storeToken, storeBytes); err == nil {
				t.Errorf("%s on %s: verdict %q, want an error", name, side, got)
			}
		}
	}
	// Different objects are decided before the bound is read, so an OSD on an
	// unbounded other filesystem still reports a verdict once its marker matches.
	osd := storeView(77, 9, storeToken, "overlay", "rw")
	if got, err := classifyStoreLink(goodStoreView(), osd, storeToken, storeBytes); err != nil || got != storeLinkDifferent {
		t.Errorf("OSD on another filesystem: %q, %v", got, err)
	}
	// The expected token comes from the caller; the same views judged against another token fail.
	if got, err := classifyStoreLink(goodStoreView(), goodStoreView(), storeToken+"x", storeBytes); err == nil {
		t.Errorf("wrong expected token: %q", got)
	}
	if got, err := classifyStoreLink(goodStoreView(), goodStoreView(), storeToken, storeBytes*2); err == nil {
		t.Errorf("wrong configured ceiling: %q", got)
	}
}

func TestStoreLinkVerdictWordingDoesNotOverclaim(t *testing.T) {
	if storeLinkSame != "same-filesystem-instance" || storeLinkDifferent != "different-filesystem" {
		t.Errorf("verdicts read %q and %q", storeLinkSame, storeLinkDifferent)
	}
	for _, v := range []string{storeLinkSame, storeLinkDifferent} {
		for _, bad := range []string{"attached", "block", "store", "osd", "safe", "full"} {
			if strings.Contains(v, bad) {
				t.Errorf("verdict %q implies %q", v, bad)
			}
		}
	}
}

func TestStoreLinkParsersRefuseMissingNullAndWrongTypes(t *testing.T) {
	requireScratchPython(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "marker")
	wr, err := parseStoreMarker(runScratchScript(t, scratchStoreMarkerScript, marker, storeToken))
	if err != nil || *wr.Errno != 0 || *wr.Written != len(storeToken) {
		t.Fatalf("marker receipt: %+v, %v", wr, err)
	}
	good := runScratchScript(t, scratchStoreViewScript, marker)
	r, err := parseStoreView(good)
	if err != nil {
		t.Fatalf("producer view refused: %v: %s", err, good)
	}
	if *r.Errno != 0 || !*r.Regular || *r.Nlink != 1 || *r.Size != int64(len(storeToken)) || *r.ContentLen != int64(len(storeToken)) {
		t.Errorf("producer view of a fresh marker: %+v", r)
	}
	sum := sha256.Sum256([]byte(storeToken))
	if *r.ContentSHA256 != hex.EncodeToString(sum[:]) {
		t.Errorf("producer digest %s", *r.ContentSHA256)
	}
	text := string(bytes.TrimSpace(good))
	mountLine := regexp.MustCompile(`"mount": (null|\{[^}]*\})`)
	for name, bad := range map[string]string{
		"empty":           "",
		"not JSON":        "garbage",
		"missing version": strings.Replace(text, `"version": 2, `, "", 1),
		"wrong version":   strings.Replace(text, `"version": 2`, `"version": 3`, 1),
		"missing errno":   strings.Replace(text, `"errno": 0, `, "", 1),
		"null ino":        regexp.MustCompile(`"ino": \d+`).ReplaceAllString(text, `"ino": null`),
		"string size":     regexp.MustCompile(`"size": \d+`).ReplaceAllString(text, `"size": "0"`),
		"null digest":     regexp.MustCompile(`"content_sha256": "[0-9a-f]*"`).ReplaceAllString(text, `"content_sha256": null`),
		"missing blocks":  regexp.MustCompile(`, "blocks": \d+`).ReplaceAllString(text, ""),
		"unknown field":   strings.Replace(text, `"version": 2`, `"version": 2, "extra": true`, 1),
		"two receipts":    text + "\n" + text,
	} {
		if v, err := parseStoreView([]byte(bad)); err == nil {
			t.Errorf("view %s: accepted %+v", name, v)
		}
	}
	if m := regexp.MustCompile(`"mount": \{[^}]*\}`).FindString(text); m != "" {
		for name, bad := range map[string]string{
			"null mount field":    regexp.MustCompile(`"fstype": "[^"]*"`).ReplaceAllString(text, `"fstype": null`),
			"missing mount field": regexp.MustCompile(`"super_options": "[^"]*"`).ReplaceAllString(text, `"x": 1`),
		} {
			if v, err := parseStoreView([]byte(bad)); err == nil {
				t.Errorf("view %s: accepted %+v", name, v)
			}
		}
	}
	// A null mount is parsed; only the judge refuses it.
	if _, err := parseStoreView([]byte(mountLine.ReplaceAllString(text, `"mount": null`))); err != nil {
		t.Errorf("null mount refused by the parser: %v", err)
	}
	for name, bad := range map[string]string{
		"empty":           "",
		"missing version": `{"errno": 0, "written": 1}`,
		"null written":    `{"version": 1, "errno": 0, "written": null}`,
		"string errno":    `{"version": 1, "errno": "0", "written": 1}`,
		"unknown field":   `{"version": 1, "errno": 0, "written": 1, "x": 1}`,
	} {
		if v, err := parseStoreMarker([]byte(bad)); err == nil {
			t.Errorf("marker %s: accepted %+v", name, v)
		}
	}
}

func TestStoreMarkerIsExclusiveAndFollowsNoLinks(t *testing.T) {
	requireScratchPython(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "marker")
	plant := func(path, token string) *storeMarkerReceipt {
		r, err := parseStoreMarker(runScratchScript(t, scratchStoreMarkerScript, path, token))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := plant(marker, storeToken); *r.Errno != 0 {
		t.Fatalf("first plant failed: %+v", r)
	}
	// A second plant must neither replace nor extend the existing marker.
	if r := plant(marker, "other"); *r.Errno == 0 || *r.Written != 0 {
		t.Errorf("existing marker replaced: %+v", r)
	}
	if got, err := os.ReadFile(marker); err != nil || string(got) != storeToken {
		t.Errorf("marker content %q, %v", got, err)
	}
	// A symlink at the marker path is refused and its target is left alone.
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if r := plant(link, storeToken); *r.Errno == 0 || *r.Written != 0 {
		t.Errorf("symlink followed: %+v", r)
	}
	if got, _ := os.ReadFile(target); string(got) != "keep" {
		t.Errorf("symlink target changed to %q", got)
	}
	// A missing parent directory is an errno, not a success.
	if r := plant(filepath.Join(dir, "absent", "m"), storeToken); *r.Errno == 0 {
		t.Errorf("missing parent reported success: %+v", r)
	}
}

func TestStoreLinkProducerViewsJoinOnDeviceInodeAndDigest(t *testing.T) {
	requireScratchPython(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "marker")
	if _, err := parseStoreMarker(runScratchScript(t, scratchStoreMarkerScript, marker, storeToken)); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "alias")
	if err := os.Link(marker, alias); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(dir, "other")
	if _, err := parseStoreMarker(runScratchScript(t, scratchStoreMarkerScript, other, storeToken)); err != nil {
		t.Fatal(err)
	}
	view := func(path string) *storeViewReceipt {
		v, err := parseStoreView(runScratchScript(t, scratchStoreViewScript, path))
		if err != nil {
			t.Fatal(err)
		}
		// A local temporary directory is not the owned volume: substitute the
		// mount and capacity fields, keep the observed device, inode and digest.
		fstype, options, id, dev, mp := "tmpfs", "rw,size=32768k", "812", "0:55", "/store"
		frsize, blocks := int64(4096), int64(storeBytes/4096)
		v.Mount = &storeMountLine{MountID: &id, Dev: &dev, Mountpoint: &mp, FSType: &fstype, SuperOptions: &options}
		v.Frsize, v.Blocks = &frsize, &blocks
		return v
	}
	// Two paths to one inode (a hard link) are one object; two files with equal
	// content are not, which is why the join compares inodes and not only digests.
	if got, err := classifyStoreLink(view(marker), view(alias), storeToken, storeBytes); err != nil || got != storeLinkSame {
		t.Errorf("hard link: %q, %v", got, err)
	}
	if got, err := classifyStoreLink(view(marker), view(other), storeToken, storeBytes); err != nil || got != storeLinkDifferent {
		t.Errorf("equal content in another file: %q, %v", got, err)
	}
	// A missing marker is an errno, so the judge refuses it.
	missing := view(filepath.Join(dir, "absent"))
	if *missing.Errno == 0 {
		t.Error("missing marker reported no errno")
	}
	if got, err := classifyStoreLink(view(marker), missing, storeToken, storeBytes); err == nil {
		t.Errorf("missing OSD marker produced %q", got)
	}
}

// A path replaced by another inode at an observation boundary must not yield a
// receipt that merges the old identity with the replacement content. The hook
// lines of the view script are replaced here by a rename of a second file
// (with the same token) into the observed path.
func TestStoreViewDetectsPathReplacedAtObservationBoundary(t *testing.T) {
	requireScratchPython(t)
	for _, hook := range []string{"# boundary-hook\n", "# boundary-hook-after\n"} {
		dir := t.TempDir()
		marker := filepath.Join(dir, "marker")
		replacement := filepath.Join(dir, "replacement")
		for _, p := range []string{marker, replacement} {
			if _, err := parseStoreMarker(runScratchScript(t, scratchStoreMarkerScript, p, storeToken)); err != nil {
				t.Fatal(err)
			}
		}
		keeper, err := parseStoreView(runScratchScript(t, scratchStoreViewScript, marker))
		if err != nil || !*keeper.Stable {
			t.Fatalf("undisturbed view: %v, %+v", err, keeper)
		}
		inject := "os.rename(" + pyQuote(replacement) + ", sys.argv[1])\n"
		script := strings.Replace(scratchStoreViewScript, hook, inject, 1)
		if script == scratchStoreViewScript {
			t.Fatalf("hook %q not found", hook)
		}
		osd, err := parseStoreView(runScratchScript(t, script, marker))
		if err != nil {
			t.Fatal(err)
		}
		if *osd.Stable {
			t.Errorf("%q: replacement at the boundary reported a stable path: %+v", hook, osd)
		}
		// The replacement carries the same token and mount substitutes make the
		// remaining fields agree, so only the stability flag can refuse it.
		sub := func(v *storeViewReceipt) {
			fstype, options, id, mdev, mp := "tmpfs", "rw,size=32768k", "812", "0:55", "/store"
			frsize, blocks := int64(4096), int64(storeBytes/4096)
			v.Mount = &storeMountLine{MountID: &id, Dev: &mdev, Mountpoint: &mp, FSType: &fstype, SuperOptions: &options}
			v.Frsize, v.Blocks = &frsize, &blocks
		}
		sub(keeper)
		sub(osd)
		*osd.Dev, *osd.Ino = *keeper.Dev, *keeper.Ino // the old identity the unfixed receipt would carry
		if got, err := classifyStoreLink(keeper, osd, storeToken, storeBytes); err == nil {
			t.Errorf("%q: replaced path judged %q", hook, got)
		}
	}
}

func pyQuote(s string) string { return strconv.Quote(s) }

// Capacity is validated by division and remainder, so a product that wraps
// int64 back to the configured ceiling is not accepted.
func TestStoreLinkJudgeRejectsWrappedCapacityProduct(t *testing.T) {
	const want = int64(1) << 33
	view := func(frsize, blocks int64) *storeViewReceipt {
		v := goodStoreView()
		*v.Mount.SuperOptions = "rw,size=8388608k"
		*v.Frsize, *v.Blocks = frsize, blocks
		return v
	}
	if got, err := classifyStoreLink(view(4096, want/4096), view(4096, want/4096), storeToken, want); err != nil || got != storeLinkSame {
		t.Fatalf("honest 8 GiB views: %q, %v", got, err)
	}
	for name, v := range map[string]*storeViewReceipt{
		"2^33 * (2^32+1) wraps to 2^33": view(1<<33, 1<<32+1),
		"2^32 * 2^32 wraps to zero":     view(1<<32, 1<<32),
		"2^62 * 4 wraps":                view(1<<62, 4),
		"zero blocks":                   view(4096, 0),
		"zero fragment":                 view(0, want/4096),
		"negative fragment":             view(-4096, -want/4096),
	} {
		if got, err := classifyStoreLink(v, goodStoreViewSized(), storeToken, want); err == nil {
			t.Errorf("%s accepted as %q", name, got)
		}
	}
}

func goodStoreViewSized() *storeViewReceipt {
	v := goodStoreView()
	*v.Mount.SuperOptions = "rw,size=8388608k"
	*v.Frsize, *v.Blocks = 4096, (1<<33)/4096
	return v
}
