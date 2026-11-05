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
