//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import "testing"

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
