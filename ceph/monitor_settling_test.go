package ceph

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// settlingFake answers the commands of monitorSettling for two MONs, a and b.
// elections lists the election epoch that each quorum_status read returns in
// turn; barrierReaches lists the MONs that each barrier CLI logs reaching.
type settlingFake struct {
	t              *testing.T
	nonce          string
	elections      []int
	barrierReaches map[string][]string
	commands       []string
}

var settlingFakeIPs = map[string]string{"a": "10.0.0.1", "b": "10.0.0.2"}

func (f *settlingFake) settling() monitorSettling {
	return monitorSettling{command: f.command, commandResult: f.commandResult}
}

func (f *settlingFake) command(_ context.Context, argv ...string) ([]byte, error) {
	line := strings.Join(argv, " ")
	f.commands = append(f.commands, line)
	switch {
	case argv[0] == "sh":
		return []byte(f.nonce), nil
	case strings.Contains(line, "quorum_status"):
		if len(f.elections) == 0 {
			f.t.Fatal("unexpected quorum_status read")
		}
		election := f.elections[0]
		f.elections = f.elections[1:]
		var mons []string
		for _, name := range []string{"a", "b"} {
			mons = append(mons, fmt.Sprintf(`{"name":%q,"public_addrs":{"addrvec":[{"type":"v2","addr":"%s:3300","nonce":0}]}}`, name, settlingFakeIPs[name]))
		}
		return fmt.Appendf(nil, `{"election_epoch":%d,"quorum_names":["a","b"],"quorum_leader_name":"a","monmap":{"epoch":3,"mons":[%s]}}`, election, strings.Join(mons, ",")), nil
	case strings.Contains(line, " sessions "):
		return []byte(`[]`), nil
	case strings.Contains(line, "auth rm"):
		return nil, nil
	}
	f.t.Fatalf("unexpected command %q", line)
	return nil, nil
}

func (f *settlingFake) commandResult(_ context.Context, _, _ int, argv ...string) (supervised.Result, error) {
	line := strings.Join(argv, " ")
	f.commands = append(f.commands, line)
	if !strings.Contains(line, "auth add") {
		f.t.Fatalf("unexpected supervised command %q", line)
	}
	target := ""
	for name, ip := range settlingFakeIPs {
		if strings.Contains(line, "--mon_host_override [v2:"+ip+":3300]") {
			target = name
		}
	}
	var log []string
	for _, name := range f.barrierReaches[target] {
		log = append(log, fmt.Sprintf("monclient(hunting): picked mon.%s con 0x1 addr [v2:%s:3300/0]", name, settlingFakeIPs[name]))
	}
	return supervised.Result{Stderr: []byte(strings.Join(log, "\n"))}, nil
}

func TestMonitorSettlingSkipsClientsThatNeverConnected(t *testing.T) {
	f := &settlingFake{t: t}
	if err := f.settling().settle(t.Context(), []string{"/tmp/a.nonce", "/tmp/b.nonce"}); err != nil {
		t.Fatal(err)
	}
	if len(f.commands) != 2 {
		t.Fatalf("a client that stored no nonce sent no command, so only its file is read; ran %q", f.commands)
	}
}

func TestMonitorSettlingPassesEveryMONAloneUnderOneQuorum(t *testing.T) {
	f := &settlingFake{t: t, nonce: "7", elections: []int{5, 5}, barrierReaches: map[string][]string{"a": {"a"}, "b": {"b"}}}
	if err := f.settling().settle(t.Context(), []string{"/tmp/a.nonce"}); err != nil {
		t.Fatal(err)
	}
	barriers := 0
	for _, line := range f.commands {
		if strings.Contains(line, "auth add") {
			barriers++
		}
	}
	if barriers != 2 {
		t.Fatalf("every MON needs its own barrier, ran %d", barriers)
	}
}

func TestMonitorSettlingRefusesAQuorumChange(t *testing.T) {
	f := &settlingFake{t: t, nonce: "7", elections: []int{5, 6}, barrierReaches: map[string][]string{"a": {"a"}, "b": {"b"}}}
	err := f.settling().settle(t.Context(), []string{"/tmp/a.nonce"})
	if err == nil || !strings.Contains(err.Error(), "the quorum changed while settling") {
		t.Fatalf("an election during settling must refuse, got %v", err)
	}
}

func TestMonitorSettlingRefusesABarrierThatReachedAnotherMON(t *testing.T) {
	f := &settlingFake{t: t, nonce: "7", elections: []int{5}, barrierReaches: map[string][]string{"a": {"a", "b"}, "b": {"b"}}}
	err := f.settling().settle(t.Context(), []string{"/tmp/a.nonce"})
	if err == nil || !strings.Contains(err.Error(), "did not reach mon.a alone") {
		t.Fatalf("a barrier that reached a second MON proves nothing, got %v", err)
	}
}
