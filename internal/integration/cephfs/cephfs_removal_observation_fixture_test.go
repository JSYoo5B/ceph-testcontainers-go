//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// cephFSCommandRecorder is the container behind the counter: it records the
// argv it receives and answers every command with one fixed reply.
type cephFSCommandRecorder struct {
	testcontainers.Container
	received [][]string
	code     int
	reader   io.Reader
	err      error
}

func (r *cephFSCommandRecorder) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	r.received = append(r.received, slices.Clone(args))
	return r.code, r.reader, r.err
}

func cephCLI(args ...string) []string {
	return append([]string{"ceph", "--connect-timeout", "5"}, args...)
}

// TestCephFSMutationCounterForwardsUnchanged keeps the counter transparent:
// the inner container receives the exact argv and its outcome comes back
// unchanged, including a failure, so the counter never decides a native
// result.
func TestCephFSMutationCounterForwardsUnchanged(t *testing.T) {
	reply := strings.NewReader("{}")
	failure := errors.New("inner failure")
	for _, tc := range []struct {
		name   string
		code   int
		reader io.Reader
		err    error
	}{{"success", 0, reply, nil}, {"exit status", 2, reply, nil}, {"exec error", 0, nil, failure}} {
		t.Run(tc.name, func(t *testing.T) {
			inner := &cephFSCommandRecorder{code: tc.code, reader: tc.reader, err: tc.err}
			counter := &cephFSMutationCounter{Container: inner}
			args := cephCLI("fs", "snapshot", "mirror", "remove", "a", "/d")
			code, reader, err := counter.Exec(t.Context(), args)
			if code != tc.code || reader != tc.reader || !errors.Is(err, tc.err) || (tc.err == nil && err != nil) {
				t.Fatalf("counter changed the native outcome: %d %v %v", code, reader, err)
			}
			if len(inner.received) != 1 || !slices.Equal(inner.received[0], args) {
				t.Fatalf("counter changed the dispatched argv: %q", inner.received)
			}
			if got := counter.counts()["ceph fs snapshot mirror remove"]; got != 1 {
				t.Fatalf("dispatched remove counted %d times", got)
			}
		})
	}
}

func TestCephFSRebalanceMatchesEachFilesystemAndDirectory(t *testing.T) {
	eligible := []string{"/first", "/second"}
	for _, tc := range []struct {
		name, secondFS, secondPath string
		matches                    bool
	}{
		{"both eligible paths", "source", "/second", true},
		{"one cycle repeated and one omitted", "source", "/first", false},
		{"same path in another filesystem", "destination", "/second", false},
		{"pending path in place of eligible path", "source", "/pending", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counter := &cephFSMutationCounter{Container: &cephFSCommandRecorder{}, includeArguments: true}
			for _, op := range []string{"remove", "add"} {
				for _, target := range [][2]string{{"source", "/first"}, {tc.secondFS, tc.secondPath}} {
					args := cephCLI("fs", "snapshot", "mirror", op, target[0], target[1])
					if _, _, err := counter.Exec(t.Context(), args); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := cephFSCheckRebalanceMutations(counter.counts(), "source", eligible); (err == nil) != tc.matches {
				t.Fatalf("matches=%t: %v", tc.matches, err)
			}
		})
	}
}

// TestCephFSMutationKindSeparatesReadsFromWrites classifies the reads that
// removal observation issues as reads and the state-changing commands of the
// mirror fixture as mutations, wherever their flags appear.
func TestCephFSMutationKindSeparatesReadsFromWrites(t *testing.T) {
	for _, args := range [][]string{
		cephCLI("fs", "snapshot", "mirror", "ls", "a"),
		cephCLI("fs", "snapshot", "mirror", "peer_list", "a"),
		cephCLI("fs", "snapshot", "mirror", "dirmap", "a", "/d"),
		cephCLI("fs", "snapshot", "mirror", "daemon", "status"),
		cephCLI("fs", "snapshot", "mirror", "show", "distribution", "a"),
		cephCLI("fs", "get", "a", "--format", "json"),
		cephCLI("fsid"),
		cephCLI("quorum_status", "--format", "json"),
		{"rados", "--pool", "meta", "listwatchers", "cephfs_mirror"},
		append([]string{"python3", "-c", supervised.Script, "0", "0", "30"}, cephCLI("fs", "snapshot", "mirror", "peer_list", "a")...),
		{"python3", "-c", supervised.CancelScript, "marker", "5"},
	} {
		if kind := cephFSMutationKind(args); kind != "" {
			t.Errorf("read %q classified as %q", args, kind)
		}
	}
	for want, args := range map[string][]string{
		"ceph fs snapshot mirror remove":         cephCLI("fs", "snapshot", "mirror", "remove", "a", "/d"),
		"ceph fs snapshot mirror add":            cephCLI("--format", "json", "fs", "snapshot", "mirror", "add", "a", "/d"),
		"ceph fs snapshot mirror peer_remove":    cephCLI("fs", "snapshot", "mirror", "peer_remove", "a", "uuid"),
		"ceph fs snapshot mirror peer_bootstrap": cephCLI("fs", "snapshot", "mirror", "peer_bootstrap", "import", "a", "token"),
		"ceph fs snapshot mirror enable":         cephCLI("fs", "snapshot", "mirror", "enable", "a"),
		"ceph mgr module enable":                 cephCLI("mgr", "module", "enable", "mirroring"),
		"ceph auth get-or-create":                cephCLI("auth", "get-or-create", "client.mirror"),
		"rados rm":                               {"rados", "-p", "meta", "rm", "cephfs_mirror"},
		"exec sh":                                {"sh", "-c", "true"},
		"exec python3":                           {"python3", "-c", "print()"},
		"ceph fs snapshot mirror peer_add":       append([]string{"python3", "-c", supervised.Script, "0", "0", "30"}, cephCLI("fs", "snapshot", "mirror", "peer_add", "a", "uuid")...),
		"exec rm":                                append([]string{"sh", "-c", supervised.ShellScript, "sh", "marker", "5", "1"}, "rm", "-f", "/tmp/key"),
	} {
		if kind := cephFSMutationKind(args); kind != want {
			t.Errorf("%q classified as %q, want %q", args, kind, want)
		}
	}
}

func TestCephFSRemovalObservationRejectsUnlistedCommands(t *testing.T) {
	for _, args := range [][]string{cephCLI("mgr", "fail"), cephCLI("quorum_status", "unexpected"), cephCLI("osd", "blocklist", "add", "client"), cephCLI("tell", "mds.a", "injectargs"), cephCLI("fs", "subvolume", "rm", "fs", "volume"), cephCLI("fs", "mirror", "disable", "fs"), {"rados", "unknown-command"}} {
		if cephFSMutationKind(args) == "" {
			t.Fatalf("mutating or unknown command accepted as a read: %q", args)
		}
	}
}

// TestCephFSObservationMutationCheckRejectsIdempotentWrite shows that the
// observation check rejects an observational call that re-issues an
// idempotent remove whose reply is success and whose final state is
// unchanged, while it accepts any number of reads.
func TestCephFSObservationMutationCheckRejectsIdempotentWrite(t *testing.T) {
	inner := &cephFSCommandRecorder{reader: strings.NewReader("{}")}
	counter := &cephFSMutationCounter{Container: inner}
	if _, _, err := counter.Exec(t.Context(), cephCLI("fs", "snapshot", "mirror", "remove", "a", "/d")); err != nil {
		t.Fatal(err)
	}
	before := counter.counts()
	for range 3 {
		if _, _, err := counter.Exec(t.Context(), cephCLI("fs", "snapshot", "mirror", "ls", "a")); err != nil {
			t.Fatal(err)
		}
	}
	if err := cephFSMutationsChanged(before, counter.counts()); err != nil {
		t.Fatalf("reads rejected as mutations: %v", err)
	}
	if _, _, err := counter.Exec(t.Context(), cephCLI("fs", "snapshot", "mirror", "remove", "a", "/d")); err != nil {
		t.Fatal(err)
	}
	err := cephFSMutationsChanged(before, counter.counts())
	if err == nil || !strings.Contains(err.Error(), "ceph fs snapshot mirror remove: 1 -> 2") {
		t.Fatalf("idempotent observational remove accepted: %v", err)
	}
	if err := cephFSMutationsChanged(map[string]int{}, map[string]int{"ceph auth del": 1}); err == nil {
		t.Fatal("first mutation of a new kind accepted")
	}
}

// cephFSPublishedClient models the cluster's published client pointer: swap
// publishes next only while current is still published, as the unit seam does.
type cephFSPublishedClient struct {
	published testcontainers.Container
	swaps     int
	refuse    bool
}

func (p *cephFSPublishedClient) swap(_ context.Context, current, next testcontainers.Container) (bool, error) {
	p.swaps++
	if p.refuse || p.published != current {
		return false, nil
	}
	p.published = next
	return true, nil
}

// TestCephFSWrapHandlesCountsASeparateClientContainer covers a separate
// client image: the client container gets its own transparent counter, so a
// write that an observation sends through ClientContainer is rejected, and the
// restore puts the CLI handle back before the client container.
func TestCephFSWrapHandlesCountsASeparateClientContainer(t *testing.T) {
	control := &cephFSCommandRecorder{reader: strings.NewReader("")}
	client := &cephFSCommandRecorder{reader: strings.NewReader("")}
	published := &cephFSPublishedClient{published: client}
	var handle testcontainers.Container = control
	var order []string
	setControl := func(next testcontainers.Container) {
		handle = next
		if next == control {
			order = append(order, "control")
		}
	}
	swap := func(ctx context.Context, current, next testcontainers.Container) (bool, error) {
		if next == client {
			order = append(order, "client")
		}
		return published.swap(ctx, current, next)
	}
	counters, restore, err := cephFSWrapHandles(t.Context(), control, client, setControl, swap)
	if err != nil {
		t.Fatal(err)
	}
	if counters.client == nil || published.published != counters.client || handle != counters.control || len(counters.all()) != 2 {
		t.Fatalf("counters not installed on both handles: published=%v handle=%v", published.published, handle)
	}
	before := counters.client.counts()
	read := []string{"rados", "--pool", "meta", "listwatchers", "cephfs_mirror"}
	if _, _, err := published.published.Exec(t.Context(), read); err != nil {
		t.Fatal(err)
	}
	if err := cephFSMutationsChanged(before, counters.client.counts()); err != nil || counters.client.commands.Load() != 1 {
		t.Fatalf("client read counted as a mutation or not seen: %v", err)
	}
	write := []string{"rados", "--pool", "meta", "rm", "cephfs_mirror"}
	if _, _, err := published.published.Exec(t.Context(), write); err != nil {
		t.Fatal(err)
	}
	if err := cephFSMutationsChanged(before, counters.client.counts()); err == nil || !strings.Contains(err.Error(), "rados rm: 0 -> 1") {
		t.Fatalf("client write accepted: %v", err)
	}
	if len(client.received) != 2 || !slices.Equal(client.received[1], write) || len(control.received) != 0 {
		t.Fatalf("client counter did not forward to the client container: client=%q control=%q", client.received, control.received)
	}
	if err := restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if handle != control || published.published != client || !slices.Equal(order, []string{"control", "client"}) {
		t.Fatalf("restore order or result: order=%v handle=%v published=%v", order, handle, published.published)
	}
	if err := restore(t.Context()); err != nil || published.swaps != 2 {
		t.Fatalf("a second restore swapped again: %v swaps=%d", err, published.swaps)
	}
}

// TestCephFSWrapHandlesSharedOrRefusedClient keeps an aggregate image, whose
// client is the CLI handle, to one counter, and refuses to start counting
// when the client container cannot be replaced or cannot be restored.
func TestCephFSWrapHandlesSharedOrRefusedClient(t *testing.T) {
	control := &cephFSCommandRecorder{}
	published := &cephFSPublishedClient{published: control}
	var handle testcontainers.Container = control
	counters, _, err := cephFSWrapHandles(t.Context(), control, control, func(next testcontainers.Container) { handle = next }, published.swap)
	if err != nil || counters.client != nil || published.swaps != 0 || handle != counters.control || len(counters.all()) != 1 {
		t.Fatalf("shared client: %v client=%v swaps=%d", err, counters.client, published.swaps)
	}

	client := &cephFSCommandRecorder{}
	refused := &cephFSPublishedClient{published: client, refuse: true}
	handle = control
	if _, _, err := cephFSWrapHandles(t.Context(), control, client, func(next testcontainers.Container) { handle = next }, refused.swap); err == nil || handle != control || refused.published != client {
		t.Fatalf("a refused client swap still installed counters: %v handle=%v", err, handle)
	}

	stale := &cephFSPublishedClient{published: client}
	_, restore, err := cephFSWrapHandles(t.Context(), control, client, func(next testcontainers.Container) { handle = next }, stale.swap)
	if err != nil {
		t.Fatal(err)
	}
	stale.published = &cephFSCommandRecorder{}
	if err := restore(t.Context()); err == nil || handle != control {
		t.Fatalf("a client container replaced behind the counter was restored silently: %v", err)
	}
}
