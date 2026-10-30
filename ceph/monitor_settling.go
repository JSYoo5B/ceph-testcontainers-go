package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/google/uuid"
)

// monitorSettling is the MON side of settling an auth change: settle proves
// that no command that stopped MON clients sent can apply any more. It runs
// every native command through command and commandResult, which select the
// control container, and holds no state and no lock of its own, so it adds
// nothing to its caller's lock contract: an auth change settles with
// Container.mu held.
type monitorSettling struct {
	command       func(ctx context.Context, argv ...string) ([]byte, error)
	commandResult func(ctx context.Context, stdoutLimit, stderrLimit int, argv ...string) (supervised.Result, error)
}

// monitorSettling runs the settling protocol through the rotation commands.
func (l authLedger) monitorSettling() monitorSettling {
	return monitorSettling{command: l.conn.rotationCommand, commandResult: l.conn.rotationResult}
}

// monitorView is the quorum and monmap that settling relies on.
type monitorView struct {
	election, epoch int
	mons            []monitorRoute
}

// monitorRoute is one MON: the address vector that the mon_host_override
// option takes, and the forms in which a MON client logs its addresses.
type monitorRoute struct {
	name, override string
	logged         []string
}

func (m monitorSettling) view(ctx context.Context) (monitorView, error) {
	out, err := m.command(ctx, "ceph", "--connect-timeout", "5", "quorum_status", "--format", "json")
	if err != nil {
		return monitorView{}, fmt.Errorf("read the MON quorum: %w", err)
	}
	var status QuorumStatus
	var view struct {
		Election *int `json:"election_epoch"`
		MonMap   struct {
			Epoch *int `json:"epoch"`
			Mons  []struct {
				PublicAddrs struct {
					Addrvec []struct {
						Type  string `json:"type"`
						Addr  string `json:"addr"`
						Nonce uint64 `json:"nonce"`
					} `json:"addrvec"`
				} `json:"public_addrs"`
			} `json:"mons"`
		} `json:"monmap"`
	}
	if json.Unmarshal(out, &status) != nil || json.Unmarshal(out, &view) != nil || view.Election == nil || view.MonMap.Epoch == nil || len(view.MonMap.Mons) != len(status.MonMap.Mons) || len(status.MonMap.Mons) == 0 {
		return monitorView{}, errors.New("read the MON quorum: unexpected quorum_status")
	}
	result := monitorView{election: *view.Election, epoch: *view.MonMap.Epoch}
	for i, member := range status.MonMap.Mons {
		single := status
		single.QuorumNames = []string{member.Name}
		single.MonMap.Mons = append(single.MonMap.Mons[:0:0], member)
		override, err := monitorBootstrapAddresses(single)
		if err != nil {
			return monitorView{}, fmt.Errorf("read the MON quorum: %w", err)
		}
		route := monitorRoute{name: member.Name, override: override}
		for _, addr := range view.MonMap.Mons[i].PublicAddrs.Addrvec {
			route.logged = append(route.logged, addr.Type+":"+addr.Addr+"/"+strconv.FormatUint(addr.Nonce, 10))
		}
		result.mons = append(result.mons, route)
	}
	return result, nil
}

// settle returns once no command that the clients whose nonce files
// are listed sent to a MON can apply any more. The caller has made sure that
// those clients stopped. First every MON must have closed their sessions: TCP
// delivers a connection's data before its end, and a MON drops a command whose
// connection has closed before it handles it (PaxosService::dispatch), so
// each command has then been handled or dropped. A MON forwards the commands
// it handles to the leader in order, and the leader commits auth changes in
// order, so a harmless auth change committed through every MON, each reached
// alone, comes after every handled command. The quorum must stay the same
// meanwhile, because an election can send a forwarded command on through
// another MON.
func (m monitorSettling) settle(ctx context.Context, clients []string) error {
	nonces, err := m.clientNonces(ctx, clients)
	if err != nil || len(nonces) == 0 {
		// A client that stored no nonce has not sent its command.
		return err
	}
	before, err := m.view(ctx)
	if err != nil {
		return fmt.Errorf("settle the MONs: %w", err)
	}
	for _, mon := range before.mons {
		if err := m.waitSessionsClosed(ctx, mon, nonces); err != nil {
			return fmt.Errorf("settle the MONs: %w", err)
		}
	}
	for _, mon := range before.mons {
		if err := m.barrier(ctx, mon, before.mons); err != nil {
			return fmt.Errorf("settle the MONs through mon.%s: %w", mon.name, err)
		}
	}
	after, err := m.view(ctx)
	if err != nil {
		return fmt.Errorf("settle the MONs: %w", err)
	}
	if after.election != before.election || after.epoch != before.epoch {
		return errors.New("settle the MONs: the quorum changed while settling")
	}
	return nil
}

// clientNonces reads the nonce files that monitorMutationScript writes; a
// missing file means that its client never connected.
func (m monitorSettling) clientNonces(ctx context.Context, clients []string) (map[uint64]bool, error) {
	nonces := map[uint64]bool{}
	for _, path := range clients {
		out, err := m.command(ctx, "sh", "-c", `test ! -e "$1" || cat "$1"`, "sh", path)
		if err != nil {
			return nil, fmt.Errorf("read the nonce of a MON client: %w", err)
		}
		if text := strings.TrimSpace(string(out)); text != "" {
			nonce, err := strconv.ParseUint(text, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("read the nonce of a MON client: %q", text)
			}
			nonces[nonce] = true
		}
	}
	return nonces, nil
}

var sessionNonce = regexp.MustCompile(`"nonce":\s*(\d+)`)

// waitSessionsClosed returns once mon lists no session of a client
// with one of nonces.
func (m monitorSettling) waitSessionsClosed(ctx context.Context, mon monitorRoute, nonces map[uint64]bool) error {
	for {
		out, err := m.command(ctx, "ceph", "--connect-timeout", "5", "tell", "mon."+mon.name, "sessions", "--format", "json")
		var sessions []json.RawMessage
		if err == nil {
			if err = json.Unmarshal(out, &sessions); err == nil {
				open := false
				for _, session := range sessions {
					for _, match := range sessionNonce.FindAllSubmatch(session, -1) {
						nonce, _ := strconv.ParseUint(string(match[1]), 10, 64)
						open = open || nonces[nonce]
					}
				}
				if !open {
					return nil
				}
				err = errors.New("a stopped client still has a session")
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for mon.%s to close the sessions of stopped clients: %w", mon.name, errors.Join(ctx.Err(), err))
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// barrier adds and removes an identity without capabilities through
// mon. mon_host_override makes the CLI contact only mon, and its MON client
// log shows that it never reached another MON.
func (m monitorSettling) barrier(ctx context.Context, mon monitorRoute, mons []monitorRoute) error {
	entity := "client.tc-key-rotation-" + uuid.NewString()
	result, err := m.commandResult(ctx, 64<<10, 1<<20, "ceph", "--mon_host_override", mon.override, "--connect-timeout", "5",
		"--debug_monc", "10/10", "--log_to_stderr", "true", "--err_to_stderr", "true", "auth", "add", entity)
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("auth add exited %d", result.ExitCode)
	}
	// The identity has no capabilities; remove it best effort.
	_, _ = m.command(ctx, "ceph", "--connect-timeout", "5", "auth", "rm", entity)
	picked := 0
	for _, line := range strings.Split(string(result.Stderr), "\n") {
		if !strings.Contains(line, "picked mon.") {
			continue
		}
		picked++
		for _, other := range mons {
			reached := false
			for _, addr := range other.logged {
				reached = reached || strings.Contains(line, addr)
			}
			if reached != (other.name == mon.name) {
				return fmt.Errorf("the CLI did not reach mon.%s alone: %s", mon.name, strings.TrimSpace(line))
			}
		}
	}
	if picked == 0 {
		return errors.New("the CLI logged no MON it contacted")
	}
	return nil
}
