//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_scratch_tmpfs))))

//ci: timeout=20m job-timeout=30 image-roles=essential

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	scratchTmpfsDir  = "/scratch"
	scratchTmpfsSize = 32 << 20
	scratchSeedLen   = 64 << 10
)

func scratchContainerExec(t *testing.T, parent context.Context, ctr testcontainers.Container, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	code, reader, err := ctr.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal("scratch container exec", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("scratch container command failed: exit=%d error=%v output=%s", code, err, data)
	}
	return data
}

func scratchWriter(t *testing.T, ctx context.Context, ctr testcontainers.Container, name string, chunk, budget int64) *scratchFillReceipt {
	t.Helper()
	out := scratchContainerExec(t, ctx, ctr, "python3", "-I", "-c", scratchFillerScript, "write",
		scratchTmpfsDir, name, strconv.FormatInt(chunk, 10), strconv.FormatInt(budget, 10), "120", "0")
	t.Logf("SCRATCH_TMPFS_FILL_RECEIPT %s", bytesTrim(out))
	r, err := parseScratchFillReceipt(out)
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	return r
}

func bytesTrim(b []byte) string {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return string(b)
}

func scratchReleaseOf(t *testing.T, ctx context.Context, ctr testcontainers.Container, name string, fill *scratchFillReceipt) *scratchRelease {
	t.Helper()
	out := scratchContainerExec(t, ctx, ctr, "python3", "-I", "-c", scratchFillerScript, "release",
		scratchTmpfsDir, name, strconv.FormatUint(*fill.File.Ino, 10), strconv.FormatUint(*fill.File.Dev, 10), "seed")
	t.Logf("SCRATCH_TMPFS_RELEASE_RECEIPT %s", bytesTrim(out))
	rel, err := parseScratchRelease(out)
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	return rel
}

// This runs the bounded scratch writer of issue 63 inside a container that
// mounts a separately owned, size-limited tmpfs. It is the scratch gate only:
// no Ceph daemon, OSD store or keeper volume is attached, and the tmpfs is a
// container mount rather than the product's owned volume.
//
// An under-budget run must be "unreached", not success. The exhausting run
// must classify as a byte-ceiling refusal from the write's own receipt, with
// closure confirmed. Releasing only the identified filler must return capacity
// and leave the seed's digest unchanged.
func TestScratchTmpfsFillerNative(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image := harness.EssentialImage(t)
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: image, Entrypoint: []string{"sleep"}, Cmd: []string{"infinity"},
		HostConfigModifier: func(h *container.HostConfig) {
			h.NetworkMode = "none"
			h.Mounts = append(h.Mounts, mount.Mount{
				Type: mount.TypeTmpfs, Target: scratchTmpfsDir,
				TmpfsOptions: &mount.TmpfsOptions{SizeBytes: scratchTmpfsSize, Mode: 0o700},
			})
		},
	}, Started: true})
	if ctr != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), time.Minute)
			defer done()
			if err := ctr.Terminate(cleanup); err != nil {
				t.Error("scratch container cleanup", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}

	// The seed is fixed bytes written before any filler, with its digest kept here.
	seed := make([]byte, scratchSeedLen)
	for i := range seed {
		seed[i] = byte(i*7 + 3)
	}
	sum := sha256.Sum256(seed)
	seedSHA := hex.EncodeToString(sum[:])
	scratchContainerExec(t, ctx, ctr, "python3", "-I", "-c",
		`import os,sys
fd=os.open(sys.argv[1]+"/seed",os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
os.write(fd,bytes((i*7+3)%256 for i in range(int(sys.argv[2]))))
os.fsync(fd)
os.close(fd)`, scratchTmpfsDir, strconv.Itoa(scratchSeedLen))

	// A budget far below the ceiling ends "unreached" and must not be reported as exhaustion.
	small := scratchWriter(t, ctx, ctr, "small", 4096, 1<<20)
	verdict, err := classifyScratchFill(small)
	if err != nil || verdict != scratchUnreached {
		t.Fatalf("under-budget run: %q, %v", verdict, err)
	}
	if err := checkScratchRelease(small, scratchReleaseOf(t, ctx, ctr, "small", small), seedSHA); err != nil {
		t.Fatal("under-budget release", err)
	}

	// The configured ceiling, as the mount reports it before any filler.
	fill := scratchWriter(t, ctx, ctr, "filler", 64<<10, 4*scratchTmpfsSize)
	if *fill.Mount.FSType != "tmpfs" || *fill.Mount.Mountpoint != scratchTmpfsDir {
		t.Fatalf("scratch directory is not the owned tmpfs mount: %+v", fill.Mount)
	}
	if total := *fill.VFSBefore.Blocks * *fill.VFSBefore.BSize; total != scratchTmpfsSize {
		t.Fatalf("tmpfs capacity %d, want the configured %d", total, int64(scratchTmpfsSize))
	}
	verdict, err = classifyScratchFill(fill)
	if err != nil {
		t.Fatal(err)
	}
	if !*fill.Closed || !*fill.Opened {
		t.Fatalf("filler closure not confirmed: opened=%v closed=%v", *fill.Opened, *fill.Closed)
	}
	var tail scratchOp
	for _, o := range fill.Ops {
		if *o.Errno != 0 {
			tail = o
		}
	}
	t.Logf("SCRATCH_TMPFS_FILL verdict=%s file_size=%d allocated=%d failing_op=%s errname=%s",
		verdict, *fill.File.Size, *fill.File.Allocated, deref(tail.Op), deref(tail.ErrName))
	if verdict != scratchByteCeiling {
		t.Fatalf("exhausting run classified %q, want %q", verdict, scratchByteCeiling)
	}
	if *fill.File.Size+scratchSeedLen > scratchTmpfsSize {
		t.Fatalf("filler plus seed exceed the ceiling: %d", *fill.File.Size)
	}

	rel := scratchReleaseOf(t, ctx, ctr, "filler", fill)
	if err := checkScratchRelease(fill, rel, seedSHA); err != nil {
		t.Fatal("filler release", err)
	}
	if *rel.SeedLen != scratchSeedLen {
		t.Fatalf("seed length %d after release", *rel.SeedLen)
	}

	// Cleanup-trap control on the same tmpfs: an unlinked path with an open
	// descriptor keeps its bytes.
	out := scratchContainerExec(t, ctx, ctr, "python3", "-I", "-c", scratchFillerScript, "hold", scratchTmpfsDir, "held")
	var held struct {
		Before struct{ Ino uint64 } `json:"before_unlink"`
		Held   struct {
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
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
