package override

import (
	"context"
	"errors"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
)

// BlocklistHandle owns one newly added exact client-session entry. Address
// includes a nonzero nonce, so two clients on one host remain distinct. Copies
// share restoration state. Removing the entry restores OSDMap admission; session
// recovery depends on the client library and callers may need to reconnect.
// External blocklist edits must not race helpers.
//
// AddBlocklist returns a non-nil handle for every outcome. A handle of a call
// that was refused or never applied owns nothing: its Restore returns nil, takes
// no lock and never reports a restoration. A handle returned together with an
// error for an uncertain outcome is real and still restorable. The zero value
// is unavailable, and its Restore returns an error.
type BlocklistHandle struct {
	owner   *ceph.Container
	address string
	state   *blocklistState
}

type blocklistState struct {
	until           time.Time
	known, restored bool
	// token is the entry in the owner's registry of held overrides.
	token overridehook.Token
	// refused is why the call was not applied. A handle with it owns no entry
	// and has no token.
	refused error
}

// Address returns the exact client session that the handle fences, as
// AddBlocklist canonicalized it: the IP, port and nonce without a protocol
// type. A call refused before the address was accepted returns the address as
// the caller passed it.
func (h *BlocklistHandle) Address() string {
	if h == nil {
		return ""
	}
	return h.address
}

// notApplied returns the handle of a call that changed nothing.
func notApplied(address string, cause error) (*BlocklistHandle, error) {
	return &BlocklistHandle{address: address, state: &blocklistState{refused: cause}}, cause
}

// AddBlocklist fences a specific native session for a positive duration
// of at least one second. Use librados GetAddrs (or its equivalent) after connect.
// A v1/v2 address vector is accepted only when every entry denotes the same IP,
// port and nonce; otherwise select one address explicitly. Protocol types become
// native ANY entries. Bare IPs, CIDRs, nonce zero and existing entries are refused.
//
// The handle is non-nil for every outcome. A refusal returns a not-applied handle
// whose Restore is a no-op (see BlocklistHandle). A nil cluster is refused with
// ceph.ErrInvalidRequest and Op "override.AddBlocklist".
//
// The handle is retained on uncertain command/readback failure. Restore checks
// its captured expiration before removal, preserving outside renewal. An unknown
// readback cannot authorize removal of a present entry; inspect native state or
// terminate the disposable cluster. Natural expiry is reconciled as absence.
func AddBlocklist(ctx context.Context, c *ceph.Container, address string, duration time.Duration) (*BlocklistHandle, error) {
	const op = "override.AddBlocklist"
	if c == nil {
		return notApplied(address, &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: op, Err: errors.New("the cluster is nil")})
	}

	normalized, err := overridehook.NormalizeBlocklistSession(address)
	if err != nil {
		return notApplied(address, err)
	}
	address = normalized
	if duration < time.Second {
		return notApplied(address, errors.New("client blocklist duration must be at least one second"))
	}
	var (
		handle   *BlocklistHandle
		applyErr error
	)
	overridehook.WithTopologyNoContext(c, func() {
		handle, applyErr = addBlocklistLocked(ctx, c, address, duration)
	})
	return handle, applyErr
}

// addBlocklistLocked runs the apply with the cluster mutex held.
func addBlocklistLocked(ctx context.Context, c *ceph.Container, address string, duration time.Duration) (*BlocklistHandle, error) {
	if err := overridehook.Ready(c); err != nil {
		return notApplied(address, err)
	}
	// A restored override has left the registry, so a held one is active.
	token, err := overridehook.Reserve(ctx, c, overridehook.Blocklist, address)
	if err != nil {
		return notApplied(address, err)
	}
	ctx, cancel := context.WithTimeout(ctx, overridehook.StartupTimeout(c))
	defer cancel()
	entries, err := overridehook.BlocklistEntries(ctx, c)
	if err != nil {
		_, _ = overridehook.Settle(c, token, nil)
		return notApplied(address, err)
	}
	if findBlocklistEntry(entries, address) != nil {
		_, _ = overridehook.Settle(c, token, nil)
		return notApplied(address, overridehook.Invalid(errors.New("client session already has a native blocklist entry; refusing to renew or adopt it")))
	}
	handle := &BlocklistHandle{owner: c, address: address, state: &blocklistState{token: token}}
	// Tracked before the command: an uncertain add still needs Restore.
	_, _ = overridehook.Settle(c, token, handle)
	setErr := overridehook.BlocklistAdd(ctx, c, address, duration)
	readCtx, readCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer readCancel()
	entries, readErr := overridehook.BlocklistEntries(readCtx, c)
	if readErr == nil {
		if current := findBlocklistEntry(entries, address); current != nil {
			handle.state.until, handle.state.known = current.Until, true
		}
	}
	if setErr != nil || readErr != nil {
		return handle, errors.Join(setErr, readErr)
	}
	if !handle.state.known {
		return handle, errors.New("client blocklist add returned success without a stored entry")
	}
	return handle, nil
}

// Restore removes only this newly created exact entry. Prior native entries and
// ranges are preserved. A changed expiration is an outside edit and is refused.
// Lost removal replies can be retried; after confirmed absence copies are no-ops.
// A handle of a call that was not applied returns nil without taking a lock.
// A cluster that has been terminated refuses the restore with the error of an
// operation on a terminated cluster, and the entry stays registered.
func (h *BlocklistHandle) Restore(ctx context.Context) error {
	if h == nil {
		return errors.New("client blocklist override is unavailable")
	}
	if h.state != nil && h.state.refused != nil {
		return nil
	}
	if h.owner == nil || h.state == nil {
		return errors.New("client blocklist override is unavailable")
	}
	c := h.owner
	var err error
	overridehook.WithTopologyNoContext(c, func() { err = h.restoreLocked(ctx, c) })
	return err
}

func (h *BlocklistHandle) restoreLocked(ctx context.Context, c *ceph.Container) error {
	if h.state.restored {
		return nil
	}
	if err := overridehook.Ready(c); err != nil {
		return err
	}
	if !overridehook.Tracks(c, h.state.token) {
		return errors.New("client blocklist override is not tracked by this cluster")
	}
	ctx, cancel := context.WithTimeout(ctx, overridehook.StartupTimeout(c))
	defer cancel()
	entries, err := overridehook.BlocklistEntries(ctx, c)
	if err != nil {
		return err
	}
	current := findBlocklistEntry(entries, h.address)
	if current == nil {
		h.finish(c)
		return nil
	}
	if !h.state.known {
		return errors.New("client blocklist apply readback was unavailable; inspect native state or terminate the cluster")
	}
	if !current.Until.Equal(h.state.until) {
		return overridehook.Invalid(errors.New("client blocklist entry changed outside this override; refusing removal"))
	}
	if err := overridehook.BlocklistRemove(ctx, c, h.address); err != nil {
		return err
	}
	entries, err = overridehook.BlocklistEntries(ctx, c)
	if err != nil {
		return err
	}
	if findBlocklistEntry(entries, h.address) != nil {
		return errors.New("client blocklist removal readback still contains the entry")
	}
	h.finish(c)
	return nil
}

func (h *BlocklistHandle) finish(c *ceph.Container) {
	h.state.restored = true
	overridehook.Release(c, h.state.token)
}

func findBlocklistEntry(entries []ceph.BlocklistEntry, address string) *ceph.BlocklistEntry {
	for _, entry := range entries {
		if !entry.Range && entry.Address == address {
			return &entry
		}
	}
	return nil
}
