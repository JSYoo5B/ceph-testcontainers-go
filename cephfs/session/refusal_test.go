package session

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
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fstest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// refusalFixture keeps the refuse_client_session flag of "app" and answers
// fs get with it; an absent flag models an MDSMap without it. lostReply
// applies the matching fs set but fails its reply, and afterSet runs after
// every applied fs set.
type refusalFixture struct {
	*sessionFixture
	refused   *bool
	lostReply string
	afterSet  func()
}

func (ctr *refusalFixture) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	flags := "{}"
	if ctr.refused != nil {
		flags = fmt.Sprintf(`{"joinable":true,"refuse_client_session":%t}`, *ctr.refused)
	}
	ctr.Output["fs get app --format json"] = fmt.Sprintf(`{"mdsmap":{"session_timeout":%d,"session_autoclose":%d,"flags_state":%s},"id":%d}`, ctr.timeout, ctr.autoclose, flags, ctr.fsID)
	command := strings.Join(clustertest.ModuleArgs(args), " ")
	ctr.Fail = ""
	if command == ctr.lostReply {
		ctr.Fail = command
	}
	code, reader, err := ctr.CannedCommandContainer.Exec(ctx, args, opts...)
	var value bool
	var seconds int
	if _, scanErr := fmt.Sscanf(command, "fs set app session_timeout %d", &seconds); scanErr == nil && code == 0 {
		ctr.timeout = seconds
	} else if _, scanErr := fmt.Sscanf(command, "fs set app session_autoclose %d", &seconds); scanErr == nil && code == 0 {
		ctr.autoclose = seconds
	} else if _, scanErr := fmt.Sscanf(command, "fs set app refuse_client_session %t", &value); scanErr == nil && (code == 0 || command == ctr.lostReply) {
		ctr.refused = &value
		if ctr.afterSet != nil {
			ctr.afterSet()
		}
	}
	return code, reader, err
}

func refusalFilesystem(t *testing.T) (*sessionCore, *refusalFixture) {
	t.Helper()
	fs, inner := sessionFilesystem(t)
	refused := false
	ctr := &refusalFixture{sessionFixture: inner, refused: &refused}
	testhelper.TestSetControlPlane(fs.Cluster(), ctr)
	return fs, ctr
}

func TestOverrideRefusalAppliesReadsBackAndRestores(t *testing.T) {
	fs, ctr := refusalFilesystem(t)
	change, err := overrideRefusal(t.Context(), fs.Core)
	if err != nil {
		t.Fatal(err)
	}
	if !*ctr.refused || change.state.fsID != 7 {
		t.Fatalf("refusal not applied: %v %+v", *ctr.refused, *change.state)
	}
	if second, err := overrideRefusal(t.Context(), fs.Core); second != nil || err == nil {
		t.Fatal("a second refusal of the same filesystem was accepted")
	}
	// The timeouts override of the same filesystem is independent.
	timeouts, err := overrideTimeouts(t.Context(), fs.Core, Limits{Timeout: 30 * time.Second, Autoclose: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	copied := *change
	if err := copied.Restore(t.Context()); err != nil || *ctr.refused {
		t.Fatalf("a copy did not restore the shared refusal: %v", err)
	}
	if overridesOf(fs.Core).refusals[fs.Core] != nil || overridesOf(fs.Core).byCore[fs.Core] != timeouts.state {
		t.Fatal("restoring the refusal changed the tracked overrides")
	}
	if got := ctr.mutations(); len(got) != 4 || got[0] != "fs set app refuse_client_session true" || got[3] != "fs set app refuse_client_session false" {
		t.Fatalf("mutations = %q", got)
	}
	testhelper.TestSetClosed(fs.Cluster(), true)
	if err := change.Restore(t.Context()); err != nil {
		t.Fatalf("a confirmed restore did not stay complete: %v", err)
	}
}

// A filesystem that already refuses new sessions gives the override nothing
// to own, and a cluster whose MDSMap lacks the flag is unsupported; neither
// changes anything.
func TestOverrideRefusalRefusesWithoutAnOwnedChange(t *testing.T) {
	fs, ctr := refusalFilesystem(t)
	*ctr.refused = true
	if change, err := overrideRefusal(t.Context(), fs.Core); change != nil || err == nil || !strings.Contains(err.Error(), "already refuses") {
		t.Fatalf("already refused: handle=%v error=%v", change, err)
	}
	fs, ctr = refusalFilesystem(t)
	ctr.refused = nil
	if change, err := overrideRefusal(t.Context(), fs.Core); change != nil || !errors.Is(err, ceph.ErrUnsupported) {
		t.Fatalf("missing flag: handle=%v error=%v", change, err)
	}
	if len(ctr.mutations()) != 0 {
		t.Fatalf("a refused override wrote: %q", ctr.mutations())
	}
}

// An outside clear of the flag already ends the owned change: Restore
// confirms it without a command. A recreated filesystem is left alone.
func TestRefusalRestoreAcceptsAnOutsideClearAndRefusesARecreatedFilesystem(t *testing.T) {
	fs, ctr := refusalFilesystem(t)
	change, err := overrideRefusal(t.Context(), fs.Core)
	if err != nil {
		t.Fatal(err)
	}
	*ctr.refused = false
	if err := change.Restore(t.Context()); err != nil || len(ctr.mutations()) != 1 {
		t.Fatalf("restore after an outside clear: %v %q", err, ctr.mutations())
	}
	fs, ctr = refusalFilesystem(t)
	change, err = overrideRefusal(t.Context(), fs.Core)
	if err != nil {
		t.Fatal(err)
	}
	ctr.fsID = 8
	if err := change.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "filesystem ID") {
		t.Fatalf("Restore on a recreated filesystem: %v", err)
	}
	fs.unregistered = true
	if err := change.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "active filesystem created by this cluster") {
		t.Fatalf("Restore on an unregistered filesystem: %v", err)
	}
	if got := ctr.mutations(); len(got) != 1 || !*ctr.refused {
		t.Fatalf("recreated filesystem was changed: %q", got)
	}
	// The same name registered again is a new core with no tracked refusal.
	other := fsbase.NewCore(fsbase.Params{Cluster: fs.Cluster(), Name: "app"})
	if overridesOf(other).refusals[other] != nil {
		t.Fatal("a new core inherited the old refusal")
	}
}

func TestOverrideRefusalKeepsOwnershipAfterAnUncertainApplyOrCancellation(t *testing.T) {
	fs, ctr := refusalFilesystem(t)
	ctr.lostReply = "fs set app refuse_client_session true"
	change, err := overrideRefusal(t.Context(), fs.Core)
	if change == nil || err == nil || !*ctr.refused {
		t.Fatalf("uncertain apply returned handle=%v error=%v", change, err)
	}
	ctr.lostReply = ""
	if err := change.Restore(t.Context()); err != nil || *ctr.refused {
		t.Fatalf("restore after an uncertain apply: %v", err)
	}

	fs, ctr = refusalFilesystem(t)
	ctx, cancel := context.WithCancel(t.Context())
	ctr.afterSet = cancel
	change, err = overrideRefusal(ctx, fs.Core)
	if change == nil || err != nil || !*ctr.refused {
		t.Fatalf("canceled after the write: handle=%v error=%v", change, err)
	}
	ctr.afterSet = nil
	if err := change.Restore(ctx); !errors.Is(err, context.Canceled) || !*ctr.refused {
		t.Fatalf("Restore with a canceled context: %v", err)
	}
	if err := change.Restore(t.Context()); err != nil || *ctr.refused {
		t.Fatalf("Restore with a fresh context: %v", err)
	}
	fstest.AssertAdmissionReleased(t, fs.Cluster(), fs.setupMu)
}

func TestOverrideRefusalAdmissionWaitsWithTheCallersContext(t *testing.T) {
	for _, gate := range []string{"setup", "owner"} {
		t.Run(gate, func(t *testing.T) {
			fs, ctr := refusalFilesystem(t)
			fstest.AssertAdmissionGate(t, fs.Cluster(), fs.setupMu, gate, func(ctx context.Context) error {
				_, err := overrideRefusal(ctx, fs.Core)
				return err
			})
			change, err := overrideRefusal(t.Context(), fs.Core)
			if err != nil {
				t.Fatal(err)
			}
			if gate == "setup" {
				clustertest.AssertContextLockQueued(t, fs.setupMu, change.Restore)
			} else {
				fstest.AssertAdmissionGate(t, fs.Cluster(), fs.setupMu, gate, change.Restore)
			}
			if len(ctr.mutations()) != 1 {
				t.Fatalf("a queued restore wrote: %q", ctr.mutations())
			}
			fstest.AssertAdmissionReleased(t, fs.Cluster(), fs.setupMu)
		})
	}
}

func TestParseRefusal(t *testing.T) {
	for data, want := range map[string]bool{
		`{"mdsmap":{"flags_state":{"refuse_client_session":true}},"id":7}`:  true,
		`{"mdsmap":{"flags_state":{"refuse_client_session":false}},"id":7}`: false,
	} {
		if got, err := parseRefusal([]byte(data), 7); err != nil || got != want {
			t.Fatalf("%s = %t, %v", data, got, err)
		}
	}
	if _, err := parseRefusal([]byte(`{"mdsmap":{"flags_state":{}},"id":7}`), 7); !errors.Is(err, errNoRefusalFlag) {
		t.Fatalf("missing flag: %v", err)
	}
	if _, err := parseRefusal([]byte(`{"mdsmap":{"flags_state":{"refuse_client_session":true}},"id":8}`), 7); err == nil || !strings.Contains(err.Error(), "filesystem ID") {
		t.Fatalf("another filesystem ID: %v", err)
	}
	if _, err := parseRefusal([]byte(`[]`), 7); err == nil || errors.Is(err, errNoRefusalFlag) {
		t.Fatalf("malformed output: %v", err)
	}
}
