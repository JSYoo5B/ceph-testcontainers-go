package fault

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
)

// BlocklistOverride owns one newly added exact client-session entry. Address
// includes a nonzero nonce, so two clients on one host remain distinct. Copies
// share restoration state. Removing the entry restores OSDMap admission; session
// recovery depends on the client library and callers may need to reconnect.
// External blocklist edits must not race helpers.
type BlocklistOverride struct {
	owner   *ceph.Container
	address string
	state   *blocklistOverrideState
}

type blocklistOverrideState struct {
	until           time.Time
	known, restored bool
	// token is the override's entry in the owner's fault registry.
	token faulthook.Token
}

func (change *BlocklistOverride) Address() string { return change.address }

// TemporaryBlocklist fences a specific native session for a positive duration
// of at least one second. Use librados GetAddrs (or its equivalent) after connect.
// A v1/v2 address vector is accepted only when every entry denotes the same IP,
// port and nonce; otherwise select one address explicitly. Protocol types become
// native ANY entries. Bare IPs, CIDRs, nonce zero and existing entries are refused.
//
// The handle is retained on uncertain command/readback failure. Restore checks
// its captured expiration before removal, preserving outside renewal. An unknown
// readback cannot authorize removal of a present entry; inspect native state or
// terminate the disposable cluster. Natural expiry is reconciled as absence.
func TemporaryBlocklist(ctx context.Context, c *ceph.Container, address string, duration time.Duration) (*BlocklistOverride, error) {
	if gateErr := faulthook.Admit(c, "fault.TemporaryBlocklist"); gateErr != nil {
		return nil, gateErr
	}
	address, err := faulthook.NormalizeBlocklistSession(address)
	if err != nil {
		return nil, err
	}
	if duration < time.Second {
		return nil, errors.New("client blocklist duration must be at least one second")
	}
	var change *BlocklistOverride
	faulthook.WithTopologyNoContext(c, func() {
		change, err = temporaryBlocklistLocked(ctx, c, address, duration)
	})
	return change, err
}

// The caller holds the topology gate for reservation, apply and readback.
func temporaryBlocklistLocked(ctx context.Context, c *ceph.Container, address string, duration time.Duration) (*BlocklistOverride, error) {
	if err := faulthook.PoolPolicyReady(c); err != nil {
		return nil, err
	}
	// A restored override has left the registry, so a held one is active.
	token, _, err := faulthook.Reserve(ctx, c, faulthook.Blocklist, address)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, faulthook.StartupTimeout(c))
	defer cancel()
	entries, err := faulthook.BlocklistEntries(ctx, c)
	if err != nil {
		_, _ = faulthook.Settle(c, token, faulthook.Held{})
		return nil, err
	}
	if findBlocklistEntry(entries, address) != nil {
		_, _ = faulthook.Settle(c, token, faulthook.Held{})
		return nil, invalidRequest("client session already has a native blocklist entry; refusing to renew or adopt it")
	}
	change := &BlocklistOverride{owner: c, address: address, state: &blocklistOverrideState{token: token}}
	// Tracked before the command: an uncertain add still needs Restore.
	_, _ = faulthook.Settle(c, token, faulthook.Held{Handle: change})
	_, setErr := faulthook.CLI(ctx, c, "osd", "blocklist", "add", address, strconv.FormatFloat(duration.Seconds(), 'f', 6, 64))
	readCtx, readCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer readCancel()
	entries, readErr := faulthook.BlocklistEntries(readCtx, c)
	if readErr == nil {
		if current := findBlocklistEntry(entries, address); current != nil {
			change.state.until, change.state.known = current.Until, true
		}
	}
	if setErr != nil || readErr != nil {
		return change, errors.Join(setErr, readErr)
	}
	if !change.state.known {
		return change, errors.New("client blocklist add returned success without a stored entry")
	}
	return change, nil
}

// Restore removes only this newly created exact entry. Prior native entries and
// ranges are preserved. A changed expiration is an outside edit and is refused.
// Lost removal replies can be retried; after confirmed absence copies are no-ops.
func (change *BlocklistOverride) Restore(ctx context.Context) error {
	if change == nil || change.owner == nil || change.state == nil {
		return errors.New("client blocklist override is unavailable")
	}
	c := change.owner
	var err error
	faulthook.WithTopologyNoContext(c, func() { err = change.restoreLocked(ctx) })
	return err
}

func (change *BlocklistOverride) restoreLocked(ctx context.Context) error {
	c := change.owner
	if change.state.restored {
		return nil
	}
	if err := faulthook.PoolPolicyReady(c); err != nil {
		return err
	}
	if !faulthook.Tracks(c, change.state.token) {
		return errors.New("client blocklist override is not tracked by this cluster")
	}
	ctx, cancel := context.WithTimeout(ctx, faulthook.StartupTimeout(c))
	defer cancel()
	entries, err := faulthook.BlocklistEntries(ctx, c)
	if err != nil {
		return err
	}
	current := findBlocklistEntry(entries, change.address)
	if current == nil {
		change.finish()
		return nil
	}
	if !change.state.known {
		return errors.New("client blocklist apply readback was unavailable; inspect native state or terminate the cluster")
	}
	if !current.Until.Equal(change.state.until) {
		return invalidRequest("client blocklist entry changed outside this override; refusing removal")
	}
	if _, err := faulthook.CLI(ctx, c, "osd", "blocklist", "rm", change.address); err != nil {
		return err
	}
	entries, err = faulthook.BlocklistEntries(ctx, c)
	if err != nil {
		return err
	}
	if findBlocklistEntry(entries, change.address) != nil {
		return errors.New("client blocklist removal readback still contains the entry")
	}
	change.finish()
	return nil
}

func (change *BlocklistOverride) finish() {
	change.state.restored = true
	faulthook.Release(change.owner, change.state.token)
}

func findBlocklistEntry(entries []ceph.BlocklistEntry, address string) *ceph.BlocklistEntry {
	for _, entry := range entries {
		if !entry.Range && entry.Address == address {
			return &entry
		}
	}
	return nil
}
