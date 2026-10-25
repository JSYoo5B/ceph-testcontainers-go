//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// These are receipt/model controls, not claims of native quota propagation.
func TestCephFSLiveQuotaReceipt(t *testing.T) {
	base, created := []byte("acknowledged"), []byte("after relaxation")
	first := cephFSActorReceipt{Token: "one-run", PID: 123, Client: 45, Inode: 67, DirInode: 89}
	current := first
	current.Size, current.Full = int64(len(base)), hex.EncodeToString(base)
	limited := `{"limit":2,"files":1,"directories":1,"entries":2,"samples":3,"elapsed_ms":400,"create_errno":122}`
	relaxed := `{"limit":3,"files":1,"directories":1,"entries":2,"samples":1,"elapsed_ms":0,"create_errno":0,"created_inode":68,"created_size":16,"created_data":"` + hex.EncodeToString(created) + `"}`
	t.Run("observed limit and EDQUOT", func(t *testing.T) {
		current.Ops = json.RawMessage(limited)
		if _, err := validateLiveQuotaStep(first, current, 2, base, nil, 122); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("relaxation preserves old fd and new data", func(t *testing.T) {
		current.Ops = json.RawMessage(relaxed)
		if _, err := validateLiveQuotaStep(first, current, 3, base, created, 0); err != nil {
			t.Fatal(err)
		}
	})
	for name, mutate := range map[string]func(*cephFSActorReceipt){
		"new client":      func(r *cephFSActorReceipt) { r.Client++ },
		"new process":     func(r *cephFSActorReceipt) { r.PID++ },
		"new fd inode":    func(r *cephFSActorReceipt) { r.Inode++ },
		"new directory":   func(r *cephFSActorReceipt) { r.DirInode++ },
		"other run":       func(r *cephFSActorReceipt) { r.Token = "another-run" },
		"wrong old bytes": func(r *cephFSActorReceipt) { r.Full = hex.EncodeToString([]byte("changed")) },
		"wrong old size":  func(r *cephFSActorReceipt) { r.Size++ },
	} {
		t.Run(name, func(t *testing.T) {
			r := current
			r.Ops = json.RawMessage(limited)
			mutate(&r)
			if _, err := validateLiveQuotaStep(first, r, 2, base, nil, 122); err == nil {
				t.Fatal("changed identity/data accepted")
			}
		})
	}
	for name, data := range map[string]string{
		"no observations":    `{}`,
		"missing errno":      `{"limit":2,"files":1,"directories":1,"entries":2,"samples":1}`,
		"null limit":         `{"limit":null,"files":1,"directories":1,"entries":2,"samples":1,"create_errno":122}`,
		"not yet enforced":   `{"limit":0,"files":1,"directories":1,"entries":2,"samples":1,"create_errno":122}`,
		"not at file limit":  `{"limit":2,"files":0,"samples":1,"create_errno":122}`,
		"no actual sample":   `{"limit":2,"files":1,"directories":1,"entries":2,"samples":0,"create_errno":122}`,
		"wrong errno ENOSPC": `{"limit":2,"files":1,"directories":1,"entries":2,"samples":1,"create_errno":28}`,
		"create succeeded":   `{"limit":2,"files":1,"directories":1,"entries":2,"samples":1,"create_errno":0}`,
		"denied but created": `{"limit":2,"files":1,"directories":1,"entries":2,"samples":1,"create_errno":122,"created_inode":68}`,
	} {
		t.Run(name, func(t *testing.T) {
			r := current
			r.Ops = json.RawMessage(data)
			if _, err := validateLiveQuotaStep(first, r, 2, base, nil, 122); err == nil {
				t.Fatal("incomplete or contradictory quota evidence accepted")
			}
		})
	}
	t.Run("new bytes must match selected payload", func(t *testing.T) {
		current.Ops = json.RawMessage(relaxed)
		if _, err := validateLiveQuotaStep(first, current, 3, base, []byte("wrong payload"), 0); err == nil {
			t.Fatal("wrong created bytes accepted")
		}
	})
	t.Run("file-only headroom ignores the directory contribution", func(t *testing.T) {
		// Native admission compares rfiles+rsubdirs, not rfiles. With one
		// file and one directory, max_files=2 is still full, not relaxed.
		r := current
		r.Ops = json.RawMessage(`{"limit":2,"files":1,"directories":1,"entries":2,"samples":1,"create_errno":0,"created_inode":68,"created_size":16,"created_data":"` + hex.EncodeToString(created) + `"}`)
		if _, err := validateLiveQuotaStep(first, r, 2, base, created, 0); err == nil {
			t.Fatal("a full recursive entry quota was accepted as relaxed")
		}
	})
	for _, elapsed := range []int64{90_000, 90_500} {
		t.Run("expired receipt", func(t *testing.T) {
			r := current
			r.Ops = json.RawMessage(strings.Replace(limited, `"elapsed_ms":400`, `"elapsed_ms":`+strconv.FormatInt(elapsed, 10), 1))
			if _, err := validateLiveQuotaStep(first, r, 2, base, nil, 122); err == nil {
				t.Fatalf("matching receipt at %dms passed the 90s phase bound", elapsed)
			}
		})
	}
}

func TestCephFSLiveQuotaObservation(t *testing.T) {
	const state = `
ready = dict(limit=2, files=1, directories=1, entries=2)
created = []
`
	for name, control := range map[string]string{
		"fast readiness permits one create": `
calls = []
def sample():
    calls.append(1)
    return ready
prior_handler = lambda signum, frame: None
signal.signal(signal.SIGALRM, prior_handler)
out = observe_quota(sample, 2, 2)
created.append(1)
assert out["samples"] == len(calls) == len(created) == 1
assert 0 <= out["elapsed_ms"] < 90000
assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
assert signal.getsignal(signal.SIGALRM) is prior_handler
`,
		"late matching sample never permits a create": `
now = [0.0]
def sample():
    now[0] = 90.5
    return ready
try:
    observe_quota(sample, 2, 2, clock=lambda: now[0])
    created.append(1)
    raise AssertionError("late matching sample admitted a create")
except TimeoutError:
    pass
assert created == []
assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
`,
		"nonmatching samples consume one finite read-only budget": `
now, calls = [0.0], []
def sample():
    calls.append(1)
    return dict(ready, limit=1)
def sleep(seconds):
    assert 0 < seconds <= 0.2
    now[0] += seconds
try:
    observe_quota(sample, 2, 2, timeout=0.5, clock=lambda: now[0], sleep=sleep)
    created.append(1)
    raise AssertionError("nonmatching observations admitted a create")
except TimeoutError:
    pass
assert len(calls) == 3 and now[0] == 0.5 and created == []
`,
		"existing actor timer is not overwritten": `
signal.setitimer(signal.ITIMER_REAL, 10)
try:
    try:
        observe_quota(lambda: ready, 2, 2)
        raise AssertionError("existing timer was overwritten")
    except RuntimeError:
        pass
    assert signal.getitimer(signal.ITIMER_REAL)[0] > 0
finally:
    signal.setitimer(signal.ITIMER_REAL, 0)
`,
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			output, err := exec.CommandContext(ctx, "python3", "-c", cephFSQuotaObservation+state+control).CombinedOutput()
			if err != nil {
				t.Fatalf("quota observation control: %v: %s", err, output)
			}
		})
	}
	t.Run("blocked native sample terminates owned process before create", func(t *testing.T) {
		// The C sleep does not return to a Python exception handler. A
		// default-action SIGALRM must stop and reap this owned subprocess;
		// the outer context is only a failed-control fallback.
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		control := `
import ctypes
def sample():
    print("blocked-sample-started", flush=True)
    ctypes.CDLL(None).sleep(10)
    return ready
observe_quota(sample, 2, 2, timeout=0.1)
print("CREATE_WAS_ADMITTED", flush=True)
`
		started := time.Now()
		cmd := exec.CommandContext(ctx, "python3", "-c", cephFSQuotaObservation+state+control)
		output, err := cmd.CombinedOutput()
		var exited *exec.ExitError
		if !errors.As(err, &exited) || ctx.Err() != nil || time.Since(started) > 2*time.Second ||
			!strings.Contains(exited.ProcessState.String(), "alarm clock") ||
			string(output) != "blocked-sample-started\n" {
			t.Fatalf("blocked sample was not bounded by its own watchdog: err=%v context=%v elapsed=%v output=%q", err, ctx.Err(), time.Since(started), output)
		}
	})
}
