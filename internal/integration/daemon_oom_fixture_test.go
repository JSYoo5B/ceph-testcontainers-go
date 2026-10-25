//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"fmt"
	mobycl "github.com/moby/moby/client"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDaemonMemoryOOMOracle(t *testing.T) {
	before := map[string]uint64{"max": 10, "oom": 2, "oom_kill": 1}
	after := map[string]uint64{"max": 20, "oom": 3, "oom_kill": 2}
	if err := daemonMemoryOOM(before, after, true, true, true, true); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name                                string
		generation, singleton, oom, stopped bool
	}{
		{"wrong generation", false, true, true, true}, {"helper victim", true, false, true, true},
		{"SIGKILL is not OOM", true, true, false, true}, {"daemon survives", true, true, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if daemonMemoryOOM(before, after, c.generation, c.singleton, c.oom, c.stopped) == nil {
				t.Fatal("incomplete victim qualified")
			}
		})
	}
	for _, key := range []string{"max", "oom", "oom_kill"} {
		t.Run(key, func(t *testing.T) {
			for _, delta := range []int{-1, 0} {
				values := map[string]uint64{"max": 20, "oom": 3, "oom_kill": 2}
				values[key] = before[key] + uint64(delta)
				if daemonMemoryOOM(before, values, true, true, true, true) == nil {
					t.Fatal("nonincreasing counter qualified")
				}
			}
			values := map[string]uint64{"max": 20, "oom": 3, "oom_kill": 2}
			delete(values, key)
			if daemonMemoryOOM(before, values, true, true, true, true) == nil {
				t.Fatal("missing counter qualified")
			}
		})
	}
}

func TestDaemonMemoryLocalCounterParser(t *testing.T) {
	for _, bad := range []string{"max 1\noom 2\n", "max 1\noom 2\noom_kill nope\n", "max 1\nmax 2\noom 2\noom_kill 3\n", "max 1 2\noom 2\noom_kill 3\n"} {
		if _, err := daemonMemoryCounters(bad); err == nil {
			t.Fatal("malformed/local witness accepted")
		}
	}
	if _, err := daemonMemoryCounters("max 1\noom 2\noom_kill 3\noom_group_kill 0\n"); err != nil {
		t.Fatal(err)
	}
}

func TestDaemonMemoryProcessIdentityParser(t *testing.T) {
	if _, err := daemonProcessStart("123 (ceph-mon with ) spaces) R 1 2"); err == nil {
		t.Fatal("short stat accepted")
	}
	if _, err := daemonProcessStart("not stat"); err == nil {
		t.Fatal("missing comm accepted")
	}
}

// These controls call the real observer, not an independently copied model.
func daemonMemoryFileTarget(t *testing.T) *daemonMemoryTarget {
	t.Helper()
	group := filepath.Join(t.TempDir(), "owned-cgroup")
	if err := os.Mkdir(group, 0700); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{"memory.max": "33554432", "memory.swap.max": "0", "memory.current": "1000", "memory.events.local": "max 20\noom 3\noom_kill 2\n", "cgroup.procs": "41\n"} {
		if err := os.WriteFile(filepath.Join(group, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	identity, err := os.Stat(group)
	if err != nil {
		t.Fatal(err)
	}
	events, err := os.Open(filepath.Join(group, "memory.events.local"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = events.Close() })
	return &daemonMemoryTarget{ID: "owned-container", Started: "first-start", PID: 41, Group: group, Identity: identity, Events: events}
}

func TestDaemonMemoryObserverRejectsReplacedCgroup(t *testing.T) {
	target := daemonMemoryFileTarget(t)
	if _, err := target.sample(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(target.Group, target.Group+"-retired"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(target.Group, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := target.sample(); err == nil {
		t.Fatal("replacement cgroup path accepted")
	}
	// An open plain fixture file remains readable after rename. This proves the
	// reader uses its handle, not that a removed native cgroup behaves the same.
	if _, err := target.events(); err != nil {
		t.Fatal(err)
	}
}

func TestDaemonMemoryObserverRecordsHelperVictim(t *testing.T) {
	target := daemonMemoryFileTarget(t)
	if err := os.WriteFile(filepath.Join(target.Group, "cgroup.procs"), []byte("41\n42\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(t.Context())
	stop()
	window := <-target.record(ctx, map[string]uint64{"max": 10, "oom": 2, "oom_kill": 1})
	if !window.PressureObserved || !window.OtherProcess {
		t.Fatalf("observer lost helper/limit witness: %+v", window)
	}
	if daemonMemoryOOM(map[string]uint64{"max": 10, "oom": 2, "oom_kill": 1}, window.Events, true, !window.OtherProcess, true, true) == nil {
		t.Fatal("helper cgroup OOM qualified as daemon OOM")
	}
}

type daemonMemoryInspectTransport struct{ started string }

func (d daemonMemoryInspectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != "GET" || !strings.HasSuffix(req.URL.Path, "/containers/owned-container/json") {
		return nil, fmt.Errorf("unexpected observer request")
	}
	body := fmt.Sprintf(`{"Id":"owned-container","State":{"Running":true,"StartedAt":%q,"Pid":41},"RestartCount":0}`, d.started)
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
}
func TestDaemonMemoryObserverRejectsWrongGeneration(t *testing.T) {
	target := daemonMemoryFileTarget(t)
	client, err := mobycl.New(mobycl.WithHost("http://docker.invalid"), mobycl.WithAPIVersion("1.56"), mobycl.WithHTTPClient(&http.Client{Transport: daemonMemoryInspectTransport{started: "new-start"}}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if err := target.fence(t.Context(), client); err == nil || err.Error() != "target daemon generation changed" {
		t.Fatal("wrong generation did not fail at engine fence", err)
	}
}
