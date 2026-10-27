package mirror

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// cephFSIdentityControl answers `ceph fs get <name>` from a per-name hook and
// delegates every other command to the draft control. It records the names in
// the order they were read.
type cephFSIdentityControl struct {
	*cephFSObserverDraftControl
	answer func(name string) (string, error)
	reads  *[]string
}

func (c *cephFSIdentityControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	arguments := args
	if len(args) >= 3 && args[0] == "ceph" && args[1] == "--connect-timeout" {
		arguments = args[3:]
	}
	if len(arguments) > 2 && slices.Equal(arguments[:2], []string{"fs", "get"}) {
		*c.reads = append(*c.reads, arguments[2])
		data, err := c.answer(arguments[2])
		if err != nil {
			return 0, nil, err
		}
		return 0, cephFSObserverDraftStream(data), nil
	}
	return c.cephFSObserverDraftControl.Exec(ctx, args, opts...)
}

func newCephFSIdentityFixture(answer func(name string) (string, error)) (*Mirror, *[]string) {
	mirror, source, destination, _, _ := newCephFSObserverDraft()
	reads := &[]string{}
	mirror.source = &ceph.Container{Container: &cephFSIdentityControl{source, answer, reads}}
	mirror.destination = &ceph.Container{Container: &cephFSIdentityControl{destination, answer, reads}}
	return mirror, reads
}

// TestCephFSOriginalFilesystemIdentityDiagnostics pins the exact text, error
// class and read order of both original-identity checks. The draft mirror
// records source filesystem ID 1 / pool 7 and destination ID 2 / pool 9.
func TestCephFSOriginalFilesystemIdentityDiagnostics(t *testing.T) {
	checks := []struct {
		name     string
		run      func(*Mirror, context.Context) error
		readText string
		mismatch string
	}{
		{"status", (*Mirror).checkCephFSObservedIdentities, "read CephFS filesystem identity failed", "CephFS original filesystem or metadata pool identity differs"},
		{"refresh", (*Mirror).checkCephFSRefreshFilesystems, "read original CephFS bootstrap filesystem failed", "CephFS original bootstrap filesystem or metadata pool identity differs"},
	}
	good := map[string]string{"source": `{"id":1,"mdsmap":{"metadata_pool":7}}`, "destination": `{"id":2,"mdsmap":{"metadata_pool":9}}`}
	transport := errors.New("native transport")
	cases := []struct {
		name      string
		source    string
		sourceErr error
		dest      string
		destErr   error
		want      string // "" means the check accepts
		query     bool
		reads     []string
	}{
		{name: "match", source: good["source"], dest: good["destination"], reads: []string{"source", "destination"}},
		{name: "source id", source: `{"id":3,"mdsmap":{"metadata_pool":7}}`, dest: good["destination"], want: "mismatch", reads: []string{"source"}},
		{name: "source pool", source: `{"id":1,"mdsmap":{"metadata_pool":8}}`, dest: good["destination"], want: "mismatch", reads: []string{"source"}},
		{name: "destination id", source: good["source"], dest: `{"id":1,"mdsmap":{"metadata_pool":9}}`, want: "mismatch", reads: []string{"source", "destination"}},
		{name: "destination pool", source: good["source"], dest: `{"id":2,"mdsmap":{"metadata_pool":7}}`, want: "mismatch", reads: []string{"source", "destination"}},
		{name: "source empty object", source: `{}`, dest: good["destination"], want: "mismatch", reads: []string{"source"}},
		{name: "source missing mdsmap", source: `{"id":1}`, dest: good["destination"], want: "mismatch", reads: []string{"source"}},
		{name: "source null mdsmap", source: `{"id":1,"mdsmap":null}`, dest: good["destination"], want: "mismatch", reads: []string{"source"}},
		{name: "source missing pool", source: `{"id":1,"mdsmap":{}}`, dest: good["destination"], want: "mismatch", reads: []string{"source"}},
		{name: "source null pool", source: `{"id":1,"mdsmap":{"metadata_pool":null}}`, dest: good["destination"], want: "mismatch", reads: []string{"source"}},
		{name: "source null id", source: `{"id":null,"mdsmap":{"metadata_pool":7}}`, dest: good["destination"], want: "mismatch", reads: []string{"source"}},
		{name: "source mistyped id", source: `{"id":"1","mdsmap":{"metadata_pool":7}}`, dest: good["destination"], want: "mismatch", reads: []string{"source"}},
		{name: "source not json", source: `not json`, dest: good["destination"], want: "mismatch", reads: []string{"source"}},
		{name: "destination empty object", source: good["source"], dest: `{}`, want: "mismatch", reads: []string{"source", "destination"}},
		{name: "destination null pool", source: good["source"], dest: `{"id":2,"mdsmap":{"metadata_pool":null}}`, want: "mismatch", reads: []string{"source", "destination"}},
		{name: "source read fails", sourceErr: transport, dest: good["destination"], want: "query", query: true, reads: []string{"source"}},
		{name: "destination read fails", source: good["source"], destErr: transport, want: "query", query: true, reads: []string{"source", "destination"}},
	}
	for _, check := range checks {
		for _, tc := range cases {
			t.Run(check.name+"/"+tc.name, func(t *testing.T) {
				mirror, reads := newCephFSIdentityFixture(func(name string) (string, error) {
					if name == "source" {
						return tc.source, tc.sourceErr
					}
					return tc.dest, tc.destErr
				})
				err := check.run(mirror, t.Context())
				if !slices.Equal(*reads, tc.reads) {
					t.Fatalf("filesystems read %v, want %v", *reads, tc.reads)
				}
				switch tc.want {
				case "":
					if err != nil {
						t.Fatalf("matching identities rejected: %v", err)
					}
				case "mismatch":
					if err == nil || err.Error() != check.mismatch || !errors.Is(err, errCephFSObservationGuard) || errors.Unwrap(err) != nil {
						t.Fatalf("identity mismatch = %v, want permanent guard %q", err, check.mismatch)
					}
				case "query":
					if err == nil || err.Error() != check.readText || errors.Is(err, errCephFSObservationGuard) || !errors.Is(err, transport) {
						t.Fatalf("identity read failure = %v, want transient %q wrapping the cause", err, check.readText)
					}
				}
			})
		}
	}
}

// TestCephFSRefreshFilesystemsReportsCancellationAfterMatch pins the trailing
// context check that only the refresh variant has.
func TestCephFSRefreshFilesystemsReportsCancellationAfterMatch(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	good := map[string]string{"source": `{"id":1,"mdsmap":{"metadata_pool":7}}`, "destination": `{"id":2,"mdsmap":{"metadata_pool":9}}`}
	mirror, _ := newCephFSIdentityFixture(func(name string) (string, error) {
		if name == "destination" {
			cancel()
		}
		return good[name], nil
	})
	// The fake ignores a context canceled while it answers, so the identities
	// match and only the trailing check can report the cancellation.
	if err := mirror.checkCephFSRefreshFilesystems(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("refresh check after cancellation = %v", err)
	}
}

type removalWaitStatus struct {
	done  bool
	label string
}

// TestCephFSRemovalWaitCompletionContract pins the loop shared by the
// directory release and peer drain waits on a neutral status type.
func TestCephFSRemovalWaitCompletionContract(t *testing.T) {
	complete := func(s removalWaitStatus) bool { return s.done }
	clearDone := func(s *removalWaitStatus) { s.done = false }
	native := errors.New("native query cause")

	t.Run("complete and error free returns immediately", func(t *testing.T) {
		calls := 0
		got, err := waitCephFSRemovalCompletion(t.Context(), time.Hour, func(context.Context) (removalWaitStatus, bool, error) {
			calls++
			return removalWaitStatus{done: true, label: "final"}, true, nil
		}, complete, clearDone)
		if err != nil || !got.done || got.label != "final" || calls != 1 {
			t.Fatalf("got %+v %v calls=%d", got, err, calls)
		}
	})
	t.Run("complete report with an error keeps polling", func(t *testing.T) {
		calls := 0
		got, err := waitCephFSRemovalCompletion(t.Context(), time.Millisecond, func(context.Context) (removalWaitStatus, bool, error) {
			calls++
			if calls == 1 {
				return removalWaitStatus{done: true, label: "first"}, true, cephFSObserveQuery("read", native)
			}
			return removalWaitStatus{done: true, label: "second"}, true, nil
		}, complete, clearDone)
		if err != nil || got.label != "second" || calls != 2 {
			t.Fatalf("got %+v %v calls=%d", got, err, calls)
		}
	})
	t.Run("guard error stops at once with the completion flag cleared and no join", func(t *testing.T) {
		guard := cephFSObserveGuard("identity changed")
		calls := 0
		got, err := waitCephFSRemovalCompletion(t.Context(), time.Millisecond, func(context.Context) (removalWaitStatus, bool, error) {
			calls++
			if calls == 1 {
				return removalWaitStatus{label: "transient"}, true, cephFSObserveQuery("read", native)
			}
			return removalWaitStatus{done: true, label: "guarded"}, true, guard
		}, complete, clearDone)
		if calls != 2 || err != guard || got.done || got.label != "guarded" {
			t.Fatalf("got %+v %v calls=%d", got, err, calls)
		}
	})
	t.Run("pre-canceled context never observes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		got, err := waitCephFSRemovalCompletion(ctx, time.Millisecond, func(context.Context) (removalWaitStatus, bool, error) {
			t.Fatal("observed with a canceled context")
			return removalWaitStatus{}, false, nil
		}, complete, clearDone)
		if !errors.Is(err, context.Canceled) || got.done {
			t.Fatalf("got %+v %v", got, err)
		}
	})
	t.Run("cancellation during an observation clears a complete report and keeps the cause", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		got, err := waitCephFSRemovalCompletion(ctx, time.Millisecond, func(attempt context.Context) (removalWaitStatus, bool, error) {
			cancel()
			return removalWaitStatus{done: true, label: "late"}, true, nil
		}, complete, clearDone)
		if !errors.Is(err, context.Canceled) || got.done || got.label != "late" {
			t.Fatalf("got %+v %v", got, err)
		}
	})
	t.Run("deadline between polls keeps the last native cause", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		got, err := waitCephFSRemovalCompletion(ctx, time.Hour, func(context.Context) (removalWaitStatus, bool, error) {
			return removalWaitStatus{label: "partial"}, true, cephFSObserveQuery("read", native)
		}, complete, clearDone)
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, native) || got.done || got.label != "partial" {
			t.Fatalf("got %+v %v", got, err)
		}
	})
	t.Run("context-only failure after a native failure retains the native cause", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		calls := 0
		_, err := waitCephFSRemovalCompletion(ctx, time.Millisecond, func(context.Context) (removalWaitStatus, bool, error) {
			calls++
			if calls == 1 {
				return removalWaitStatus{}, true, cephFSObserveQuery("read", native)
			}
			cancel()
			return removalWaitStatus{}, true, context.Canceled
		}, complete, clearDone)
		if calls != 2 || !errors.Is(err, native) || !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v calls=%d", err, calls)
		}
	})
	t.Run("unadmitted report fills only until an admitted one arrives", func(t *testing.T) {
		calls := 0
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		got, _ := waitCephFSRemovalCompletion(ctx, time.Millisecond, func(context.Context) (removalWaitStatus, bool, error) {
			calls++
			switch calls {
			case 1:
				return removalWaitStatus{label: "unadmitted-first"}, false, errors.New("lock wait")
			case 2:
				return removalWaitStatus{label: "unadmitted-second"}, false, errors.New("lock wait")
			case 3:
				return removalWaitStatus{label: "admitted"}, true, errors.New("query")
			}
			cancel()
			return removalWaitStatus{label: "unadmitted-after"}, false, context.Canceled
		}, complete, clearDone)
		if got.label != "admitted" || calls != 4 {
			t.Fatalf("got %+v calls=%d", got, calls)
		}
	})
}

// TestCephFSRemovalWaitsClearTheirOwnFlag runs the real directory and peer
// wrappers: the shared loop must clear Released respectively Drained, and only
// that flag, on every non-success return.
func TestCephFSRemovalWaitsClearTheirOwnFlag(t *testing.T) {
	guard := cephFSObserveGuard("identity changed")
	t.Run("directory", func(t *testing.T) {
		got, err := waitCephFSDirectoryReleasedWithAdmission(t.Context(), time.Hour, func(context.Context) (DirectoryRemovalStatus, bool, error) {
			return DirectoryRemovalStatus{Directory: "/owned", Released: true, PolicyRemoved: true}, true, guard
		})
		if err != guard || got.Released || !got.PolicyRemoved || got.Directory != "/owned" {
			t.Fatalf("got %+v %v", got, err)
		}
		got, err = waitCephFSDirectoryReleasedWithAdmission(t.Context(), time.Hour, func(context.Context) (DirectoryRemovalStatus, bool, error) {
			return DirectoryRemovalStatus{Released: true}, true, nil
		})
		if err != nil || !got.Released {
			t.Fatalf("success lost: %+v %v", got, err)
		}
	})
	t.Run("peer", func(t *testing.T) {
		got, err := waitCephFSPeerDrainWithAdmission(t.Context(), time.Hour, func(context.Context) (PeerRemovalStatus, bool, error) {
			return PeerRemovalStatus{PeerID: "original", Drained: true, PolicyRemoved: true}, true, guard
		})
		if err != guard || got.Drained || !got.PolicyRemoved || got.PeerID != "original" {
			t.Fatalf("got %+v %v", got, err)
		}
		got, err = waitCephFSPeerDrainWithAdmission(t.Context(), time.Hour, func(context.Context) (PeerRemovalStatus, bool, error) {
			return PeerRemovalStatus{Drained: true}, true, nil
		})
		if err != nil || !got.Drained {
			t.Fatalf("success lost: %+v %v", got, err)
		}
	})
}
