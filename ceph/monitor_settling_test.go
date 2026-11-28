package ceph

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

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
		return fmt.Appendf(nil, `{"election_epoch":%d,"quorum_names":["a","b"],"quorum_leader_name":"a","monmap":{"fsid":"9b2d8f1e-0c5a-4f3e-8a6b-1d2c3e4f5a6b","epoch":3,"mons":[%s]}}`, election, strings.Join(mons, ",")), nil
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

// Incomplete native replies cannot advance to an auth barrier. A later
// complete observation may settle the same stopped client.
func TestMonitorSettlingRequiresCompleteSessionEvidence(t *testing.T) {
	for _, data := range []string{`null`, `[{}]`, `[null]`, `[{"entity_name":"client.probe","con_type":"client","global_id":7,"addrs":{"addrvec":[{"nonce":"7"}]}}]`, `[{"entity_name":"client.probe","con_type":"client","global_id":7,"addrs":{"addrvec":[{"nonce":7}]}}]`} {
		t.Run(data, func(t *testing.T) {
			f := &settlingFake{t: t, nonce: "7", elections: []int{5, 5, 5}, barrierReaches: map[string][]string{"a": {"a"}, "b": {"b"}}}
			m := f.settling()
			command := m.command
			m.command = func(ctx context.Context, args ...string) ([]byte, error) {
				if strings.Contains(strings.Join(args, " "), " sessions ") {
					return []byte(data), nil
				}
				return command(ctx, args...)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			if err := m.settle(ctx, []string{"nonce"}); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("incomplete/live session settled: %v", err)
			}
			for _, cmd := range f.commands {
				if strings.Contains(cmd, "auth add") {
					t.Fatal("barrier ran without complete closed sessions")
				}
			}
			m.command = command
			if err := m.settle(t.Context(), []string{"nonce"}); err != nil {
				t.Fatalf("valid empty observation did not recover: %v", err)
			}
		})
	}
}

func TestMonitorSettlingRefusesAMalformedQuorumReport(t *testing.T) {
	member := func(name, ip string) string {
		return fmt.Sprintf(`{"name":%q,"public_addrs":{"addrvec":[{"type":"v2","addr":"%s:3300","nonce":0}]}}`, name, ip)
	}
	report := func(quorum, mons string) []byte {
		return []byte(`{"election_epoch":4,"quorum_names":` + quorum + `,"quorum_leader_name":"a","monmap":{"fsid":"9b2d8f1e-0c5a-4f3e-8a6b-1d2c3e4f5a6b","epoch":3,"mons":[` + mons + `]}}`)
	}
	for name, data := range map[string][]byte{
		"duplicate member":     report(`["a","b"]`, member("a", "10.0.0.1")+","+member("a", "10.0.0.2")+","+member("b", "10.0.0.3")),
		"unnamed member":       report(`["a"]`, member("a", "10.0.0.1")+","+member("", "10.0.0.2")),
		"quorum outside map":   report(`["a","c"]`, member("a", "10.0.0.1")+","+member("b", "10.0.0.2")),
		"duplicate quorum":     report(`["a","a"]`, member("a", "10.0.0.1")+","+member("b", "10.0.0.2")),
		"missing nonce":        report(`["a"]`, `{"name":"a","public_addrs":{"addrvec":[{"type":"v2","addr":"10.0.0.1:3300"}]}}`),
		"missing quorum names": []byte(`{"election_epoch":4,"quorum_leader_name":"a","monmap":{"fsid":"f","epoch":3,"mons":[` + member("a", "10.0.0.1") + `]}}`),
	} {
		t.Run(name, func(t *testing.T) {
			m := monitorSettling{command: func(context.Context, ...string) ([]byte, error) { return data, nil }}
			if view, err := m.view(t.Context()); err == nil {
				t.Fatalf("the settling barrier accepted the report: %+v", view)
			}
		})
	}
	good := monitorSettling{command: func(context.Context, ...string) ([]byte, error) {
		return report(`["a","b"]`, member("a", "10.0.0.1")+","+member("b", "10.0.0.2")), nil
	}}
	view, err := good.view(t.Context())
	if err != nil || view.election != 4 || view.epoch != 3 || len(view.mons) != 2 || view.mons[1].name != "b" || len(view.mons[1].logged) != 1 || view.mons[1].logged[0] != "v2:10.0.0.2:3300/0" {
		t.Fatalf("view = %+v, %v", view, err)
	}
}
