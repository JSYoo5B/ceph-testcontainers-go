//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"testing"
	"time"
)

func killedSamples(address string, states ...any) []killedSessionSample {
	var samples []killedSessionSample
	for i := 0; i < len(states); i += 2 {
		samples = append(samples, killedSessionSample{State: states[i].(string), Address: address, At: time.Duration(states[i+1].(int)) * time.Second})
	}
	return samples
}

func TestJudgeKilledSessionFollowsTheAutocloseEviction(t *testing.T) {
	const addr = "v1:172.18.0.9:0/1234"
	timeout, latest := 30*time.Second, 75*time.Second
	accepted := map[string][]killedSessionSample{
		// Issue 379: the close was still being journaled at the last sample.
		"open then killing": killedSamples(addr, "open", 5, "open", 55, "killing", 58),
		"only open":         killedSamples(addr, "open", 5, "open", 57),
	}
	for name, samples := range accepted {
		if state, deadline := judgeKilledSession(samples, addr, 60*time.Second, timeout, latest); state != "" || deadline != "" {
			t.Errorf("%s: state %q deadline %q", name, state, deadline)
		}
	}
	rejected := map[string][]killedSessionSample{
		"stale":                  killedSamples(addr, "open", 5, "stale", 35),
		"closing":                killedSamples(addr, "open", 5, "closing", 55),
		"open after killing":     killedSamples(addr, "open", 5, "killing", 55, "open", 57),
		"killing before timeout": killedSamples(addr, "open", 5, "killing", 10),
		"not open first":         killedSamples(addr, "killing", 40),
		"no sample":              nil,
		"another session":        append(killedSamples(addr, "open", 5), killedSessionSample{State: "open", Address: "v1:172.18.0.9:0/999", At: 40 * time.Second}),
	}
	for name, samples := range rejected {
		state, deadline := judgeKilledSession(samples, addr, 60*time.Second, timeout, latest)
		if state == "" {
			t.Errorf("%s: the state contract accepted %v", name, samples)
		}
		if deadline != "" {
			t.Errorf("%s: an in-time eviction was reported late: %s", name, deadline)
		}
	}
	// A late or early eviction is a deadline problem, not a state problem.
	for _, gone := range []time.Duration{20 * time.Second, 90 * time.Second} {
		state, deadline := judgeKilledSession(killedSamples(addr, "open", 5, "killing", 50), addr, gone, timeout, latest)
		if state != "" || deadline == "" {
			t.Errorf("gone %s: state %q deadline %q", gone, state, deadline)
		}
	}
	if got := killedSessionStates(killedSamples(addr, "open", 5, "open", 7, "killing", 58)); len(got) != 2 || got[0] != "open" || got[1] != "killing" {
		t.Errorf("distinct states %v", got)
	}
}
