//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// These checks run the scratch filler writer, its release observer and the
// receipt judges on an ordinary temporary directory and on synthetic receipts.
// They test descriptor closure, receipt completeness and the classification
// rules. They exercise no tmpfs mount, no real ENOSPC, no Ceph daemon and no
// Docker, so they show nothing about capacity behavior on the CI kernel.

func requireScratchPython(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("the scratch writer checks need python3")
	}
}

func scratchCommand(script string, args ...string) *exec.Cmd {
	return exec.Command("python3", append([]string{"-I", "-c", script}, args...)...)
}

func runScratchScript(t *testing.T, script string, args ...string) []byte {
	t.Helper()
	out, err := scratchCommand(script, args...).Output()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	return out
}

func writeScratchSeed(t *testing.T, dir string) string {
	t.Helper()
	seed := []byte(strings.Repeat("seed-bytes-", 100))
	if err := os.WriteFile(filepath.Join(dir, "seed"), seed, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(seed)
	return hex.EncodeToString(sum[:])
}

func mustScratchReceipt(t *testing.T, out []byte) *scratchFillReceipt {
	t.Helper()
	r, err := parseScratchFillReceipt(out)
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	return r
}

func TestScratchWriterBudgetReceiptAndRelease(t *testing.T) {
	requireScratchPython(t)
	dir := t.TempDir()
	seed := writeScratchSeed(t, dir)
	out := runScratchScript(t, scratchFillerScript, "write", dir, "filler", "4096", "20000", "30", "0")
	r := mustScratchReceipt(t, out)
	verdict, err := classifyScratchFill(r)
	if err != nil || verdict != scratchUnreached {
		t.Fatalf("budget run: %q, %v", verdict, err)
	}
	if !*r.Opened || !*r.Closed || *r.File.Size != 20000 || *r.File.NLink != 1 {
		t.Fatalf("receipt %+v file %+v", r, r.File)
	}
	// 20000 bytes in 4096-byte chunks end with a short final request.
	last := r.Ops[len(r.Ops)-2]
	if *last.Op != "write" || *last.Requested != 20000-4*4096 {
		t.Fatalf("final write %+v", last)
	}

	release := runScratchScript(t, scratchFillerScript, "release", dir, "filler",
		strconv.FormatUint(*r.File.Ino, 10), strconv.FormatUint(*r.File.Dev, 10), "seed")
	rel, err := parseScratchRelease(release)
	if err != nil {
		t.Fatal(err)
	}
	if !*rel.Identified || !*rel.Unlinked || !*rel.PathAbsent {
		t.Fatalf("release %s", release)
	}
	if _, err := os.Lstat(filepath.Join(dir, "filler")); !os.IsNotExist(err) {
		t.Fatalf("filler still present: %v", err)
	}
	// The capacity-growth rule is judged on synthetic observations below, since
	// an ordinary filesystem need not report released blocks immediately.
	if *rel.SeedSHA256 != seed || *rel.SeedLen != 1100 {
		t.Fatalf("seed digest %s length %d", *rel.SeedSHA256, *rel.SeedLen)
	}

	// A different file at the same path is not the identified filler.
	if err := os.WriteFile(filepath.Join(dir, "filler"), []byte("other"), 0o600); err != nil {
		t.Fatal(err)
	}
	other := runScratchScript(t, scratchFillerScript, "release", dir, "filler",
		strconv.FormatUint(*r.File.Ino+1<<40, 10), strconv.FormatUint(*r.File.Dev, 10), "seed")
	relOther, err := parseScratchRelease(other)
	if err != nil || *relOther.Identified || *relOther.Unlinked || *relOther.PathAbsent {
		t.Fatalf("unidentified file: %s, %v", other, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "filler")); err != nil {
		t.Fatalf("an unidentified file was removed: %v", err)
	}
}

func TestScratchWriterClosesOnErrorAndCreateRefusal(t *testing.T) {
	requireScratchPython(t)
	dir := t.TempDir()
	// The file-size limit turns a write into EFBIG after a short write.
	limited := exec.Command("sh", "-c", `ulimit -f 1 && exec python3 -I -c "$0" write "$1" filler 4096 100000 30 0`, scratchFillerScript, dir)
	out, err := limited.Output()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	r := mustScratchReceipt(t, out)
	verdict, err := classifyScratchFill(r)
	if err != nil || verdict != scratchOtherErrno {
		t.Fatalf("size-limit run: %q, %v: %s", verdict, err, out)
	}
	var failing scratchOp
	for _, o := range r.Ops {
		if *o.Errno != 0 {
			failing = o
		}
	}
	if *failing.Op != "write" || *failing.ErrName != "EFBIG" || !*r.Closed || *r.File.Size <= 0 || *r.File.Size >= 4096 {
		t.Fatalf("failing %s closed %t size %d", out, *r.Closed, *r.File.Size)
	}

	// An existing filler name is a create refusal and no descriptor is left.
	out = runScratchScript(t, scratchFillerScript, "write", dir, "filler", "4096", "4096", "30", "0")
	r = mustScratchReceipt(t, out)
	verdict, err = classifyScratchFill(r)
	if err != nil || verdict != scratchOtherErrno || *r.Opened || !*r.Closed || *r.Ops[0].Op != "create" || *r.Ops[0].ErrName != "EEXIST" {
		t.Fatalf("exclusive create: %q, %v: %s", verdict, err, out)
	}
}

func TestScratchWriterClosesOnCancellation(t *testing.T) {
	requireScratchPython(t)
	dir := t.TempDir()
	cmd := scratchCommand(scratchFillerScript, "write", dir, "filler", "4096", "1000000000000", "60", "0.01")
	var stdout strings.Builder
	cmd.Stdout = &stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if info, err := os.Stat(filepath.Join(dir, "filler")); err == nil && info.Size() > 0 {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatal("the writer made no progress")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("%v: %s", err, stdout.String())
	}
	r := mustScratchReceipt(t, []byte(stdout.String()))
	verdict, err := classifyScratchFill(r)
	if err != nil || verdict != scratchCancelled || !*r.Closed || *r.File.Size == 0 {
		t.Fatalf("cancelled run: %q, %v: %s", verdict, err, stdout.String())
	}
}

// A writer that leaves its descriptor open must be refused, and the refusal
// must come from the receipt's own confirmation, not from path state.
func TestScratchWriterNegativeControlLeavesDescriptorOpen(t *testing.T) {
	requireScratchPython(t)
	const closeCall = "os.close(fd)\n            except OSError as e:\n                r[\"close_errno\"]"
	if strings.Count(scratchFillerScript, closeCall) != 1 {
		t.Fatal("the writer's single close call moved; update this control")
	}
	broken := strings.Replace(scratchFillerScript, closeCall, "pass\n            except OSError as e:\n                r[\"close_errno\"]", 1)
	dir := t.TempDir()
	out := runScratchScript(t, broken, "write", dir, "filler", "4096", "4096", "30", "0")
	r := mustScratchReceipt(t, out)
	if *r.Closed {
		t.Fatalf("a writer that never closes was recorded as closed: %s", out)
	}
	if _, err := classifyScratchFill(r); err == nil {
		t.Fatal("an unconfirmed closure was classified")
	}
	// The honest writer, run on the same name after cleanup, is confirmed closed.
	if err := os.Remove(filepath.Join(dir, "filler")); err != nil {
		t.Fatal(err)
	}
	honest := mustScratchReceipt(t, runScratchScript(t, scratchFillerScript, "write", dir, "filler", "4096", "4096", "30", "0"))
	if !*honest.Closed {
		t.Fatal("the honest writer was not confirmed closed")
	}
}

// Path absence after unlink does not release space while a descriptor is held.
func TestScratchHeldDescriptorSurvivesUnlink(t *testing.T) {
	requireScratchPython(t)
	dir := t.TempDir()
	out := runScratchScript(t, scratchFillerScript, "hold", dir, "held")
	var held struct {
		Before struct {
			Ino uint64
		} `json:"before_unlink"`
		Held struct {
			Ino       uint64
			NLink     int64
			Size      int64
			Allocated int64
		}
		PathAbsent bool `json:"path_absent"`
		Readable   bool
	}
	if err := json.Unmarshal(out, &held); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if !held.PathAbsent || !held.Readable || held.Held.NLink != 0 || held.Held.Size != 4096 ||
		held.Held.Allocated < 4096 || held.Held.Ino != held.Before.Ino {
		t.Fatalf("held descriptor: %s", out)
	}
	if _, err := os.Lstat(filepath.Join(dir, "held")); !os.IsNotExist(err) {
		t.Fatalf("held path still present: %v", err)
	}
}

func scratchVFSOf(bsize, bavail, favail int64) *scratchVFS {
	blocks, files := int64(8192), int64(1000)
	return &scratchVFS{BSize: &bsize, Blocks: &blocks, BFree: &bavail, BAvail: &bavail, Files: &files, FFree: &favail, FAvail: &favail}
}

func syntheticScratchFill(op, fstype string, errno int, requested, refusalAvail, refusalInodes int64) *scratchFillReceipt {
	one, size, allocated, ino, dev, name := int64(1), int64(0), int64(0), uint64(7), uint64(9), ""
	zero, version := 0, 1
	closed, opened := true, true
	chunk, budget := int64(4096), int64(1<<30)
	outcome, exception, errname := "errno", "", "ENOSPC"
	id, mp := "0:50", "/scratch"
	fs := fstype
	fail := scratchOp{Op: &op, Requested: &requested, Written: new(int64), Errno: &errno, ErrName: &errname}
	return &scratchFillReceipt{
		Version: &version, Dir: &mp, Name: &name, Chunk: &chunk, Budget: &budget,
		Ops: []scratchOp{fail}, Outcome: &outcome, Exception: &exception, Opened: &opened, Closed: &closed, CloseErrno: &zero,
		VFSBefore: scratchVFSOf(4096, 8000, 1000), VFSRefusal: scratchVFSOf(4096, refusalAvail, refusalInodes),
		VFSAfterClose: scratchVFSOf(4096, refusalAvail, refusalInodes),
		File:          &scratchFile{Ino: &ino, Dev: &dev, NLink: &one, Size: &size, Allocated: &allocated},
		Mount:         &scratchMount{ID: &id, Mountpoint: &mp, FSType: &fs},
	}
}

func TestScratchClassifierDoesNotTreatEveryENOSPCAsTheByteCeiling(t *testing.T) {
	enospc, eio := int(syscall.ENOSPC), int(syscall.EIO)
	for name, tc := range map[string]struct {
		r    *scratchFillReceipt
		want string
	}{
		"write refused with no space and inodes left": {syntheticScratchFill("write", "tmpfs", enospc, 4096, 0, 900), scratchByteCeiling},
		"write refused with space for the request":    {syntheticScratchFill("write", "tmpfs", enospc, 4096, 100, 900), scratchUnclassifiedNS},
		"write refused on a non-tmpfs filesystem":     {syntheticScratchFill("write", "ext4", enospc, 4096, 0, 900), scratchUnclassifiedNS},
		"create refused with no inodes":               {syntheticScratchFill("create", "tmpfs", enospc, 0, 5000, 0), scratchInodeLimit},
		"create refused with inodes left":             {syntheticScratchFill("create", "tmpfs", enospc, 0, 5000, 10), scratchUnclassifiedNS},
		"write refused with an I/O error":             {syntheticScratchFill("write", "tmpfs", eio, 4096, 0, 900), scratchOtherErrno},
		"write refused by space but no inodes left":   {syntheticScratchFill("write", "tmpfs", enospc, 4096, 0, 0), scratchUnclassifiedNS},
	} {
		got, err := classifyScratchFill(tc.r)
		if err != nil || got != tc.want {
			t.Errorf("%s: %q, %v, want %q", name, got, err, tc.want)
		}
	}
}

func TestScratchReceiptParserRefusesMissingNullAndWrongTypes(t *testing.T) {
	requireScratchPython(t)
	dir := t.TempDir()
	good := runScratchScript(t, scratchFillerScript, "write", dir, "filler", "4096", "4096", "30", "0")
	mustScratchReceipt(t, good)
	mutate := func(edit func(map[string]any)) []byte {
		var doc map[string]any
		if err := json.Unmarshal(good, &doc); err != nil {
			t.Fatal(err)
		}
		edit(doc)
		out, _ := json.Marshal(doc)
		return out
	}
	for name, edit := range map[string]func(map[string]any){
		"closed missing":        func(d map[string]any) { delete(d, "closed") },
		"closed null":           func(d map[string]any) { d["closed"] = nil },
		"closed wrong type":     func(d map[string]any) { d["closed"] = "true" },
		"ops empty":             func(d map[string]any) { d["ops"] = []any{} },
		"ops null":              func(d map[string]any) { d["ops"] = nil },
		"operation errno null":  func(d map[string]any) { d["ops"].([]any)[0].(map[string]any)["errno"] = nil },
		"vfs_before null":       func(d map[string]any) { d["vfs_before"] = nil },
		"vfs_after_close empty": func(d map[string]any) { d["vfs_after_close"] = map[string]any{} },
		"vfs field missing":     func(d map[string]any) { delete(d["vfs_before"].(map[string]any), "favail") },
		"unknown field":         func(d map[string]any) { d["extra"] = 1 },
		"version two":           func(d map[string]any) { d["version"] = 2 },
	} {
		if _, err := parseScratchFillReceipt(mutate(edit)); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if _, err := parseScratchFillReceipt(nil); err == nil {
		t.Error("an absent receipt (a lost writer) was accepted")
	}
	if _, err := parseScratchFillReceipt(append(append([]byte{}, good...), good...)); err == nil {
		t.Error("two receipts were accepted")
	}

	// A receipt whose file size disagrees with the recorded writes, or that
	// claims a budget it did not write, cannot be classified.
	for name, edit := range map[string]func(map[string]any){
		"file size differs":              func(d map[string]any) { d["file"].(map[string]any)["size"] = 1 },
		"budget not written":             func(d map[string]any) { d["budget"] = 8192 },
		"file identity null":             func(d map[string]any) { d["file"] = nil },
		"outcome unknown":                func(d map[string]any) { d["outcome"] = "mystery" },
		"errno outcome without an error": func(d map[string]any) { d["outcome"] = "errno" },
	} {
		r, err := parseScratchFillReceipt(mutate(edit))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if verdict, err := classifyScratchFill(r); err == nil {
			t.Errorf("%s was classified %q", name, verdict)
		}
	}
}

func TestScratchReleaseGateRequiresClosureIdentityAndCapacity(t *testing.T) {
	requireScratchPython(t)
	dir := t.TempDir()
	out := runScratchScript(t, scratchFillerScript, "write", dir, "filler", "4096", "8192", "30", "0")
	fill := mustScratchReceipt(t, out)
	allocated := int64(8192)
	fill.File.Allocated = &allocated
	yes, no, zero, version := true, false, 0, 1
	sha, seedLen := "abc", int64(10)
	release := func(before, after int64) *scratchRelease {
		return &scratchRelease{Version: &version, Identified: &yes, Unlinked: &yes, PathAbsent: &yes, Errno: &zero,
			VFSBefore: scratchVFSOf(4096, before, 10), VFSAfter: scratchVFSOf(4096, after, 10), SeedSHA256: &sha, SeedLen: &seedLen}
	}
	if err := checkScratchRelease(fill, release(100, 102), "abc"); err != nil {
		t.Fatalf("complete release refused: %v", err)
	}

	// Path absence with no released capacity is the held-descriptor case.
	if checkScratchRelease(fill, release(100, 100), "abc") == nil {
		t.Error("an unchanged capacity was accepted as release")
	}
	if checkScratchRelease(fill, release(100, 101), "abc") == nil {
		t.Error("a partial release was accepted")
	}
	if checkScratchRelease(fill, release(100, 102), "different") == nil {
		t.Error("a changed seed was accepted")
	}
	unidentified := release(100, 102)
	unidentified.Identified, unidentified.Unlinked = &no, &no
	if checkScratchRelease(fill, unidentified, "abc") == nil {
		t.Error("an unidentified file was accepted")
	}
	present := release(100, 102)
	present.PathAbsent = &no
	if checkScratchRelease(fill, present, "abc") == nil {
		t.Error("a present path was accepted")
	}
	open := *fill
	open.Closed = &no
	if checkScratchRelease(&open, release(100, 102), "abc") == nil {
		t.Error("an unconfirmed writer closure was accepted")
	}
	for name, edit := range map[string]func(map[string]any){
		"seed_sha256 null": func(d map[string]any) { d["seed_sha256"] = nil },
		"vfs_after null":   func(d map[string]any) { d["vfs_after"] = nil },
		"identified gone":  func(d map[string]any) { delete(d, "identified") },
	} {
		raw := runScratchScript(t, scratchFillerScript, "release", dir, "absent", "1", "1", "seed")
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		if _, err := parseScratchRelease(raw); err != nil {
			t.Fatalf("baseline release receipt: %v", err)
		}
		edit(doc)
		mutated, _ := json.Marshal(doc)
		if _, err := parseScratchRelease(mutated); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// The real producer must give the classifier what a create refusal needs: the
// containing filesystem's capacity at the refusal and its mount identity,
// observed without a created file. Only os.open is made to raise ENOSPC; the
// capacity and mount observations are replaced to stand in for a tmpfs.
func TestScratchCreateRefusalProducerToClassifier(t *testing.T) {
	requireScratchPython(t)
	patch := func(favail int, fstype string) string {
		hook := "import errno as _e\n" +
			"def _nospace(*a, **k): raise OSError(_e.ENOSPC, 'injected')\n" +
			"os.open = _nospace\n"
		if favail >= 0 {
			hook += "_v = vfs\n" +
				"def vfs(p):\n    v = _v(p)\n    v['favail'] = " + strconv.Itoa(favail) + "\n    return v\n" +
				"def mountinfo(dev): return {'id': '0:50', 'mountpoint': '/scratch', 'fstype': '" + fstype + "'}\n"
		}
		return strings.Replace(scratchFillerScript, "m, a = sys.argv[1]", hook+"m, a = sys.argv[1]", 1)
	}
	for name, tc := range map[string]struct {
		favail int
		fstype string
		want   string
	}{
		"no free inodes on tmpfs":  {0, "tmpfs", scratchInodeLimit},
		"inodes remain on tmpfs":   {10, "tmpfs", scratchUnclassifiedNS},
		"no free inodes elsewhere": {0, "ext4", scratchUnclassifiedNS},
		"unmodified observations":  {-1, "", scratchUnclassifiedNS},
	} {
		t.Run(name, func(t *testing.T) {
			out := runScratchScript(t, patch(tc.favail, tc.fstype), "write", t.TempDir(), "filler", "4096", "4096", "30", "0")
			r := mustScratchReceipt(t, out)
			if *r.Opened || !*r.Closed || *r.Ops[0].Op != "create" || *r.Ops[0].ErrName != "ENOSPC" {
				t.Fatalf("receipt %s", out)
			}
			if r.VFSRefusal == nil || r.Mount == nil {
				t.Fatalf("a create refusal left no capacity or mount identity: %s", out)
			}
			got, err := classifyScratchFill(r)
			if err != nil || got != tc.want {
				t.Fatalf("%q, %v, want %q: %s", got, err, tc.want, out)
			}
		})
	}
}
