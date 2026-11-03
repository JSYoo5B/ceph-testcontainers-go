package cephfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// maxFileSizeControl keeps the MDSMap max_file_size of "fixture" and answers
// fs get with it. lostReply applies the matching fs set but fails its reply,
// refused fails the matching fs set without applying it, and afterSet runs
// after every applied fs set.
type maxFileSizeControl struct {
	*clustertest.CannedCommandContainer
	fsID               int64
	size               int64
	lostReply, refused string
	afterSet           func()
}

func (ctr *maxFileSizeControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	ctr.Output["fs get fixture --format json"] = fmt.Sprintf(`{"mdsmap":{"fs_name":"fixture","max_file_size":%d},"id":%d}`, ctr.size, ctr.fsID)
	command := strings.Join(clustertest.ModuleArgs(args), " ")
	ctr.Fail = ""
	if command == ctr.lostReply || command == ctr.refused {
		ctr.Fail = command
	}
	code, reader, err := ctr.CannedCommandContainer.Exec(ctx, args, opts...)
	var value int64
	if _, scanErr := fmt.Sscanf(command, "fs set fixture max_file_size %d", &value); scanErr == nil && (code == 0 || command == ctr.lostReply) {
		ctr.size = value
		if ctr.afterSet != nil {
			ctr.afterSet()
		}
	}
	return code, reader, err
}

func (ctr *maxFileSizeControl) mutations() []string {
	var commands []string
	for _, call := range ctr.Calls {
		if command := strings.Join(call, " "); strings.HasPrefix(command, "fs set") {
			commands = append(commands, command)
		}
	}
	return commands
}

// maxFileSizeFixture is the registered filesystem "fixture" (FSMap ID 41)
// with Ceph's default max_file_size of 1 TiB.
func maxFileSizeFixture(t *testing.T) (*Filesystem, *maxFileSizeControl) {
	t.Helper()
	ctr := &maxFileSizeControl{CannedCommandContainer: &clustertest.CannedCommandContainer{Output: map[string]string{}}, fsID: 41, size: 1 << 40}
	cluster := testhelper.TestCluster(ctr, testhelper.TestConfig{StartupTimeout: time.Second, OSDs: 1})
	fs := newFilesystem(cluster, Config{Name: "fixture", MetadataPool: ceph.PoolConfig{Name: "metadata"}, DataPool: ceph.PoolConfig{Name: "data"}}, nil)
	fs.base().ReplaceNative(&fsbase.NativeIdentity{ID: 41, Attachments: make(map[string]*fsbase.PoolAttachment)})
	filesystems(cluster).byName = map[string]*Filesystem{"fixture": fs}
	return fs, ctr
}

func TestCephFSMaxFileSizeAppliesReadsBackAndRestores(t *testing.T) {
	fs, ctr := maxFileSizeFixture(t)
	change, err := fs.OverrideMaxFileSize(t.Context(), MinMaxFileSize)
	if err != nil {
		t.Fatal(err)
	}
	if ctr.size != MinMaxFileSize || change.state.previous != 1<<40 || change.state.fsID != 41 {
		t.Fatalf("applied size=%d state=%+v", ctr.size, *change.state)
	}
	if second, err := fs.OverrideMaxFileSize(t.Context(), 1<<20); second != nil || err == nil {
		t.Fatal("a second max_file_size override of the same filesystem was accepted")
	}
	copied := *change
	if err := copied.Restore(t.Context()); err != nil || ctr.size != 1<<40 {
		t.Fatalf("a copy did not restore the shared override: size=%d error=%v", ctr.size, err)
	}
	if filesystems(fs.base().Cluster()).maxFileSizes[fs] != nil {
		t.Fatal("a restored override is still tracked")
	}
	if got := ctr.mutations(); len(got) != 2 {
		t.Fatalf("mutations = %q, want one apply and one restore", got)
	}
	testhelper.TestSetClosed(fs.base().Cluster(), true)
	if err := change.Restore(t.Context()); err != nil {
		t.Fatalf("a confirmed restore did not stay complete: %v", err)
	}
	if again, err := fs.OverrideMaxFileSize(t.Context(), MinMaxFileSize); again != nil || err == nil {
		t.Fatal("a closed cluster admitted a new override")
	}
}

func TestCephFSMaxFileSizeRefusesValuesBelowTheNativeMinimum(t *testing.T) {
	for _, bytes := range []int64{-1, 0, MinMaxFileSize - 1} {
		fs, ctr := maxFileSizeFixture(t)
		change, err := fs.OverrideMaxFileSize(t.Context(), bytes)
		if change != nil || !errors.Is(err, ceph.ErrInvalidRequest) || len(ctr.Calls) != 0 {
			t.Fatalf("%d: handle=%v error=%v calls=%q", bytes, change, err, ctr.Calls)
		}
	}
}

// An outside edit is refused instead of overwritten; once the owned value is
// back, Restore proceeds.
func TestCephFSMaxFileSizeRestoreRefusesAnOutsideEdit(t *testing.T) {
	fs, ctr := maxFileSizeFixture(t)
	change, err := fs.OverrideMaxFileSize(t.Context(), MinMaxFileSize)
	if err != nil {
		t.Fatal(err)
	}
	ctr.size = 1 << 30
	if err := change.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "outside this override") {
		t.Fatalf("restore over an outside edit: %v", err)
	}
	if ctr.size != 1<<30 || len(ctr.mutations()) != 1 {
		t.Fatalf("refused restore changed the outside edit: size=%d mutations=%q", ctr.size, ctr.mutations())
	}
	ctr.size = MinMaxFileSize
	if err := change.Restore(t.Context()); err != nil || ctr.size != 1<<40 {
		t.Fatalf("restore after the owned value returned: size=%d error=%v", ctr.size, err)
	}
}

// Real Ceph cannot swap the filesystem ID under one name between two calls
// or lose one reply on demand, so these use the fake.
func TestCephFSMaxFileSizeRestoreRefusesARecreatedFilesystem(t *testing.T) {
	fs, ctr := maxFileSizeFixture(t)
	change, err := fs.OverrideMaxFileSize(t.Context(), MinMaxFileSize)
	if err != nil {
		t.Fatal(err)
	}
	ctr.fsID = 42
	if err := change.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "filesystem ID") {
		t.Fatalf("Restore on a recreated filesystem: %v", err)
	}
	if got := ctr.mutations(); len(got) != 1 {
		t.Fatalf("recreated filesystem was changed: %q", got)
	}
	// The registry replaced the descriptor under the same name.
	replacement := newFilesystem(fs.base().Cluster(), fs.config, nil)
	replacement.base().ReplaceNative(&fsbase.NativeIdentity{ID: 42, Attachments: make(map[string]*fsbase.PoolAttachment)})
	filesystems(fs.base().Cluster()).byName["fixture"] = replacement
	if err := change.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "original registered filesystem") {
		t.Fatalf("Restore through a replaced descriptor: %v", err)
	}
	if fresh, err := replacement.OverrideMaxFileSize(t.Context(), 1<<20); err != nil || fresh.state == change.state {
		t.Fatalf("the replacement filesystem shares the old override: %v", err)
	}
}

func TestCephFSMaxFileSizeKeepsOwnershipAfterAnUncertainApply(t *testing.T) {
	fs, ctr := maxFileSizeFixture(t)
	ctr.lostReply = fmt.Sprintf("fs set fixture max_file_size %d", MinMaxFileSize)
	change, err := fs.OverrideMaxFileSize(t.Context(), MinMaxFileSize)
	if change == nil || err == nil {
		t.Fatalf("uncertain apply returned handle=%v error=%v", change, err)
	}
	ctr.lostReply = ""
	if err := change.Restore(t.Context()); err != nil || ctr.size != 1<<40 {
		t.Fatalf("restore after an uncertain apply: size=%d error=%v", ctr.size, err)
	}
	// A refused apply that did not change the value restores with no command.
	fs, ctr = maxFileSizeFixture(t)
	ctr.refused = fmt.Sprintf("fs set fixture max_file_size %d", MinMaxFileSize)
	change, err = fs.OverrideMaxFileSize(t.Context(), MinMaxFileSize)
	if change == nil || err == nil || ctr.size != 1<<40 {
		t.Fatalf("refused apply returned handle=%v error=%v size=%d", change, err, ctr.size)
	}
	if err := change.Restore(t.Context()); err != nil || len(ctr.mutations()) != 1 {
		t.Fatalf("restore after a refused apply: error=%v mutations=%q", err, ctr.mutations())
	}
}

// The caller's cancellation during the native write does not hide the
// outcome: the readback survives it, and the handle keeps ownership.
func TestCephFSMaxFileSizeCancellationKeepsRestoreOwnership(t *testing.T) {
	fs, ctr := maxFileSizeFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	ctr.afterSet = cancel
	change, err := fs.OverrideMaxFileSize(ctx, MinMaxFileSize)
	if change == nil || err != nil || ctr.size != MinMaxFileSize {
		t.Fatalf("canceled after the write: handle=%v error=%v size=%d", change, err, ctr.size)
	}
	ctr.afterSet = nil
	if err := change.Restore(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Restore with a canceled context: %v", err)
	}
	if err := change.Restore(t.Context()); err != nil || ctr.size != 1<<40 {
		t.Fatalf("Restore with a fresh context: size=%d error=%v", ctr.size, err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	calls := len(ctr.Calls)
	if change, err := fs.OverrideMaxFileSize(canceled, MinMaxFileSize); change != nil || !errors.Is(err, context.Canceled) || len(ctr.Calls) != calls {
		t.Fatalf("a canceled caller reached the cluster: handle=%v error=%v", change, err)
	}
}

func TestCephFSMaxFileSizeAdmissionWaitsWithTheCallersContext(t *testing.T) {
	for _, gate := range []string{"setup", "owner"} {
		t.Run(gate, func(t *testing.T) {
			fs, ctr := maxFileSizeFixture(t)
			assertCephFSContextAdmissionGate(t, fs.base().Cluster(), gate, func(ctx context.Context) error {
				_, err := fs.OverrideMaxFileSize(ctx, MinMaxFileSize)
				return err
			})
			change, err := fs.OverrideMaxFileSize(t.Context(), MinMaxFileSize)
			if err != nil {
				t.Fatal(err)
			}
			assertCephFSContextAdmissionGate(t, fs.base().Cluster(), gate, change.Restore)
			if len(ctr.mutations()) != 1 {
				t.Fatalf("a queued restore wrote: %q", ctr.mutations())
			}
			assertCephFSContextAdmissionReleased(t, fs.base().Cluster())
		})
	}
}

func TestParseMaxFileSize(t *testing.T) {
	if size, err := parseMaxFileSize([]byte(`{"mdsmap":{"max_file_size":1099511627776},"id":1}`), 1); err != nil || size != 1<<40 {
		t.Fatalf("size = %d, %v", size, err)
	}
	for _, bad := range []string{`{}`, `{"mdsmap":{},"id":1}`, `[]`, `{"mdsmap":{"max_file_size":"x"},"id":1}`} {
		if _, err := parseMaxFileSize([]byte(bad), 1); err == nil {
			t.Fatalf("%s was accepted", bad)
		}
	}
	if _, err := parseMaxFileSize([]byte(`{"mdsmap":{"max_file_size":65536},"id":2}`), 1); err == nil || !strings.Contains(err.Error(), "filesystem ID") {
		t.Fatalf("another filesystem ID was accepted: %v", err)
	}
}
