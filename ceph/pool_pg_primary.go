package ceph

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// PGPrimary selects the acting primary that Pool.OverridePGPrimary gives one
// PG of the pool.
type PGPrimary struct {
	// PG is a PG of the pool, as Pool.PGs reports it.
	PG PGID
	// OSD is the owned OSD that is to serve the PG as its acting primary. It
	// must be up and a member of the PG's current acting set.
	OSD *OSDContainer
	// RaiseMinCompatClient lets the override raise the cluster's
	// require_min_compat_client to the release that the running MON names
	// as the minimum for pg-upmap-primary, when the cluster is below it.
	// Restore lowers it again once no override that relies on the raised
	// level remains. Without it, such a cluster is refused with
	// ErrInvalidRequest before any change.
	RaiseMinCompatClient bool
}

// PGPrimaryOverrideHandle owns one PG's pg-upmap-primary entry, and possibly
// a raised require_min_compat_client, for Pool.OverridePGPrimary. It retains
// the exact prior entry, including its absence, and possibly uncertain
// application: keep a non-nil handle returned with an error, because the
// change may have been applied. Copies share its state.
//
// Restore puts back only what the handle introduced. It refuses an entry or
// a compatibility level that changed outside the override, rather than
// overwriting it. Ceph has no compare-and-swap for these values, so an
// outside write of the same value cannot be detected. Ceph itself cancels an
// entry whose OSD left the PG's raw placement or became its CRUSH primary;
// such a cancellation is an outside change when it differs from the prior
// entry. Terminate does not undo the override: it ends with the cluster.
type PGPrimaryOverrideHandle struct {
	owner *Container
	state *pgPrimaryState
}

// pgPrimaryState is the state of one PG primary override; the cluster's mutex
// guards it.
type pgPrimaryState struct {
	pool *Pool
	name string
	// poolID is the native ID of the pool that the override binds.
	poolID int64
	pg     PGID
	osd    int
	// prior is the entry before the override and applied the entry that it
	// owns; noUpmapPrimary marks an absent entry.
	prior, applied int
	// token is the registry entry of the PG.
	token *faultEntry
	// lease is the raised require_min_compat_client that the entry relies on,
	// or nil.
	lease *minCompatLease
	// entryRestored means the entry holds prior again; restored that the lease
	// was left too.
	entryRestored, restored bool
}

// minCompatLease is one raise of require_min_compat_client that PG primary
// overrides share: the last holder to restore lowers it to prior.
type minCompatLease struct {
	prior, raised string
	holders       map[*pgPrimaryState]bool
	token         *faultEntry
}

// noUpmapPrimary is the prior or applied entry of a PG without one.
const noUpmapPrimary = -1

// minCompatLeaseKey is the registry key of the one compatibility lease of a
// cluster.
const minCompatLeaseKey = "require_min_compat_client"

// upmapPrimaryCompatRefusal is the MON's refusal of pg-upmap-primary and
// rm-pg-upmap-primary on a cluster whose require_min_compat_client is below
// the release that the command needs; it names both releases.
var upmapPrimaryCompatRefusal = regexp.MustCompile(`min_compat_client ([a-z]+) < ([a-z]+), which is required for pg-upmap-primary`)

// OverridePGPrimary makes primary.OSD the acting primary of one PG of this
// replicated pool through the native pg-upmap-primary entry, so a test can
// aim a fault at, or route SUT I/O through, a known serving OSD. Replica
// placement is unchanged: only the order of the acting set changes.
//
// The call validates the request and binds the original cluster FSID, the
// pool's native ID, the PG and the owned OSD's native UUID before any change.
// It refuses with ErrInvalidRequest: a nil or foreign OSD handle, an invalid
// or foreign PG, an erasure-coded pool, an OSD that is down or outside the
// PG's acting set, a PG with a primary_temp entry, a PG that another handle
// overrides, a terminated cluster, and a cluster whose
// require_min_compat_client is below what pg-upmap-primary needs unless
// primary.RaiseMinCompatClient is set. The needed release is the one that the
// running MON names in its refusal, not a release string of this module; the
// raise is refused natively, without --yes-i-really-mean-it, while connected
// clients look older. An OSD that already is the acting primary takes no
// native write: the handle then owns the unchanged entry.
//
// It returns only when the condition is ready: the OSDMap holds the applied
// entry and maps primary.OSD first in the acting set, the serving PG answers
// pg query with primary.OSD as acting primary and active+clean, and the MGR
// PG report (Pool.PGs) agrees. That proves which OSD serves the PG at that
// moment. It does not prove that the SUT's client has received the new map
// (a client with an older map is redirected by the OSDs), that reads go to
// the primary only, or that the condition lasts: CRUSH, weight, primary
// affinity and balancer changes (in read or upmap-read mode the balancer
// writes these entries too) can move it again, and Ceph cancels the entry
// when its OSD leaves the PG's placement. It applies to replicated pools only
// and changes no balancer or other cluster policy.
//
// A non-nil handle returned with an error owns a change that may have been
// applied; Restore it. Lock admission, every native command and the readiness
// wait are bounded by ctx and the cluster's startup timeout.
//
//purpose:condition
func (p *Pool) OverridePGPrimary(ctx context.Context, primary PGPrimary) (*PGPrimaryOverrideHandle, error) {
	const op = "Pool.OverridePGPrimary"
	c, err := p.cluster(op)
	if err != nil {
		return nil, err
	}
	name := p.Name
	if err := validateExistingPoolName(name); err != nil {
		return nil, err
	}
	if err := validatePGID(string(primary.PG)); err != nil {
		return nil, err
	}
	if primary.OSD == nil || primary.OSD.owner != c {
		return nil, invalidRequest("%s: the OSD must be an owned OSD handle of this pool's cluster", op)
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	handle, err := p.applyPGPrimaryLocked(ctx, c, name, primary)
	c.mu.Unlock()
	if handle == nil || err != nil {
		return handle, err
	}
	return handle, handle.waitReady(ctx)
}

// applyPGPrimaryLocked validates, reserves and applies the override with
// c.mu held. It returns a nil handle only when nothing was changed.
func (p *Pool) applyPGPrimaryLocked(ctx context.Context, c *Container, name string, primary PGPrimary) (*PGPrimaryOverrideHandle, error) {
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	if err := p.matchLocked(name, -1); err != nil {
		return nil, err
	}
	if err := c.fleet.osdHandles.current(primary.OSD); err != nil {
		return nil, err
	}
	fsid, err := c.pgPrimaryFSID(ctx)
	if err != nil {
		return nil, err
	}
	dump, err := c.readPGPrimaryMap(ctx, fsid)
	if err != nil {
		return nil, err
	}
	pool, ok := dump.pools[name]
	if !ok {
		return nil, invalid(fmt.Errorf("%w: pool %q does not exist", errStalePool, name))
	}
	if err := p.matchLocked(name, pool.id); err != nil {
		return nil, err
	}
	if err := checkPGOfPool(primary.PG, pool); err != nil {
		return nil, err
	}
	osd := primary.OSD.ID
	if native, ok := dump.osds[osd]; !ok || native.uuid != primary.OSD.nativeUUID || !native.up {
		return nil, invalidRequest("Pool.OverridePGPrimary: osd.%d is not up as the OSD that this cluster owns", osd)
	}
	if slices.Contains(dump.primaryTemp, string(primary.PG)) {
		return nil, invalidRequest("Pool.OverridePGPrimary: PG %s has a primary_temp entry, which overrides any upmap primary", primary.PG)
	}
	acting, err := c.pgMapActing(ctx, primary.PG)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(acting, osd) {
		return nil, invalidRequest("Pool.OverridePGPrimary: osd.%d is not in the acting set of PG %s", osd, primary.PG)
	}
	token, _, err := c.faults.reserve(ctx, faultPGPrimary, string(primary.PG))
	if err != nil {
		return nil, err
	}
	prior := noUpmapPrimary
	if entry, ok := dump.entries[string(primary.PG)]; ok {
		prior = entry
	}
	state := &pgPrimaryState{pool: p, name: name, poolID: pool.id, pg: primary.PG, osd: osd, prior: prior, applied: prior, token: token}
	handle := &PGPrimaryOverrideHandle{owner: c, state: state}
	if acting[0] == osd {
		// The OSD already is the primary: own the unchanged entry.
		state.entryRestored = true
		c.faults.settle(token, "", handle, nil, nil)
		return handle, nil
	}
	state.applied = osd
	lease := c.minCompatLease()
	setErr := c.upmapPrimary(ctx, primary.PG, osd)
	if need, have, refused := compatRefusal(setErr); refused {
		if lease != nil || !primary.RaiseMinCompatClient {
			c.faults.settle(token, "", nil, nil, nil)
			if lease != nil {
				return nil, invalidRequest("Pool.OverridePGPrimary: require_min_compat_client changed outside the raise that another PG primary override holds")
			}
			return nil, invalidRequest("Pool.OverridePGPrimary: require_min_compat_client %s is below %s, which pg-upmap-primary requires; set PGPrimary.RaiseMinCompatClient to raise it for this override", have, need)
		}
		if have != dump.requireMinCompat {
			c.faults.settle(token, "", nil, nil, nil)
			return nil, errors.New("Pool.OverridePGPrimary: the native compatibility refusal names another require_min_compat_client than the OSDMap")
		}
		lease, err = c.raiseMinCompat(ctx, have, need)
		if lease == nil {
			c.faults.settle(token, "", nil, nil, nil)
			return nil, err
		}
		state.join(lease)
		c.faults.settle(token, "", handle, nil, nil)
		if err != nil {
			return handle, err
		}
		setErr = c.upmapPrimary(ctx, primary.PG, osd)
	} else {
		if definiteRefusal(setErr) {
			c.faults.settle(token, "", nil, nil, nil)
			return nil, setErr
		}
		if lease != nil {
			state.join(lease)
		}
		c.faults.settle(token, "", handle, nil, nil)
	}
	if definiteRefusal(setErr) {
		// Only the raised level remains to restore.
		state.applied, state.entryRestored = prior, true
		return handle, setErr
	}
	if setErr != nil {
		return handle, setErr
	}
	after, err := c.readPGPrimaryMap(ctx, fsid)
	if err != nil {
		return handle, err
	}
	if entry, ok := after.entries[string(primary.PG)]; !ok || entry != osd {
		return handle, errors.New("Pool.OverridePGPrimary: the pg-upmap-primary readback differs from the requested OSD")
	}
	return handle, nil
}

// join makes the override a holder of lease. The caller holds c.mu.
func (s *pgPrimaryState) join(lease *minCompatLease) {
	s.lease = lease
	lease.holders[s] = true
}

// waitReady waits until the OSDMap, the serving PG and the MGR report agree on
// the requested acting primary with the PG active+clean. An entry changed
// outside the override, a removed pool and a terminated cluster end the wait
// at once.
func (h *PGPrimaryOverrideHandle) waitReady(ctx context.Context) error {
	s := h.state
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		ready, err := h.ready(ctx)
		if err != nil {
			var final *pgPrimaryFinal
			if errors.As(err, &final) {
				return final.err
			}
			last = err
		}
		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), fmt.Errorf("PG %s did not report osd.%d as its serving acting primary: %w", s.pg, s.osd, last))
		}
		if ready && err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), fmt.Errorf("PG %s did not report osd.%d as its serving acting primary: %w", s.pg, s.osd, last))
		case <-ticker.C:
		}
	}
}

// pgPrimaryFinal marks a readiness failure that no later read can undo.
type pgPrimaryFinal struct{ err error }

func (e *pgPrimaryFinal) Error() string { return e.err.Error() }
func (e *pgPrimaryFinal) Unwrap() error { return e.err }

// ready is one readiness observation.
func (h *PGPrimaryOverrideHandle) ready(ctx context.Context) (bool, error) {
	c, s := h.owner, h.state
	snapshot, err := s.pool.observePGs(ctx, c)
	if cause := observationEndCause(err); cause != nil {
		return false, &pgPrimaryFinal{err: oneShotError(err)}
	}
	if err != nil {
		return false, err
	}
	reported := false
	for _, pg := range snapshot.PGs {
		if pg.PGID == s.pg {
			reported = pg.ActingPrimary == s.osd && pgActiveClean(pg.State)
		}
	}
	if err := c.lockTopology(ctx); err != nil {
		return false, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return false, &pgPrimaryFinal{err: errClusterTerminated}
	}
	fsid, err := c.pgPrimaryFSID(ctx)
	if err != nil {
		return false, err
	}
	dump, err := c.readPGPrimaryMap(ctx, fsid)
	if err != nil {
		return false, err
	}
	if pool, ok := dump.pools[s.name]; !ok || pool.id != s.poolID {
		return false, &pgPrimaryFinal{err: invalid(fmt.Errorf("%w: pool %q was removed or replaced", errStalePool, s.name))}
	}
	if dump.entry(s.pg) != s.applied {
		return false, &pgPrimaryFinal{err: invalid(fmt.Errorf("the pg-upmap-primary entry of PG %s changed outside this override", s.pg))}
	}
	acting, err := c.pgMapActing(ctx, s.pg)
	if err != nil || len(acting) == 0 || acting[0] != s.osd {
		return false, err
	}
	serving, state, err := c.pgServingPrimary(ctx, s.pg)
	if err != nil {
		return false, err
	}
	return reported && serving == s.osd && pgActiveClean(state), nil
}

// Restore puts back the PG's prior pg-upmap-primary entry: it removes an
// entry that did not exist, or sets the prior OSD again. When the override is
// the last holder of a raised require_min_compat_client, it then lowers the
// level to its prior release; another holder keeps it raised. It never runs
// rm-pg-upmap-primary-all and touches no other PG. It refuses an entry or a
// level that changed outside the override with ErrInvalidRequest and leaves
// it alone. A pool that was removed took its entries with it, which counts
// as restored.
//
// Restore reads the stored OSDMap state back; it does not wait for the PG to
// be served by the prior primary, because a test may restore while that OSD
// is stopped. Use Observer.WaitForClean and Pool.PGs for that. It is
// idempotent across copies and reconciles a lost reply before any further
// change; retry with a fresh context. A terminated cluster refuses it with
// ErrInvalidRequest.
//
//purpose:condition,lifetime
func (h *PGPrimaryOverrideHandle) Restore(ctx context.Context) error {
	if h == nil || h.owner == nil || h.state == nil {
		return errors.New("PG primary override is unavailable")
	}
	c, s := h.owner, h.state
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if s.restored {
		return nil
	}
	if err := c.poolPolicyReady(); err != nil {
		return err
	}
	if !c.faults.tracks(s.token) {
		return errors.New("PG primary override is not tracked by this cluster")
	}
	fsid, err := c.pgPrimaryFSID(ctx)
	if err != nil {
		return err
	}
	if !s.entryRestored {
		if err := c.restorePGPrimaryEntry(ctx, fsid, s); err != nil {
			return err
		}
	}
	if s.lease != nil {
		if err := c.leaveMinCompatLease(ctx, fsid, s); err != nil {
			return err
		}
	}
	s.restored = true
	c.faults.release(s.token)
	return nil
}

func (c *Container) restorePGPrimaryEntry(ctx context.Context, fsid string, s *pgPrimaryState) error {
	dump, err := c.readPGPrimaryMap(ctx, fsid)
	if err != nil {
		return err
	}
	current := dump.entry(s.pg)
	if pool, ok := dump.pools[s.name]; (!ok || pool.id != s.poolID) && current == noUpmapPrimary {
		// Ceph removes the entries of a removed pool.
		s.entryRestored = true
		return nil
	}
	switch current {
	case s.prior:
		s.entryRestored = true
		return nil
	case s.applied:
	default:
		return invalid(fmt.Errorf("the pg-upmap-primary entry of PG %s changed outside this override; refusing restoration", s.pg))
	}
	if s.prior == noUpmapPrimary {
		_, err = c.conn().ceph(ctx, "osd", "rm-pg-upmap-primary", string(s.pg))
	} else {
		err = c.upmapPrimary(ctx, s.pg, s.prior)
	}
	if err != nil {
		return err
	}
	dump, err = c.readPGPrimaryMap(ctx, fsid)
	if err != nil {
		return err
	}
	if dump.entry(s.pg) != s.prior {
		return errors.New("pg-upmap-primary restoration readback differs from the prior entry")
	}
	s.entryRestored = true
	return nil
}

// leaveMinCompatLease drops s from its lease, lowering the level first when s
// is the last holder. The caller holds c.mu.
func (c *Container) leaveMinCompatLease(ctx context.Context, fsid string, s *pgPrimaryState) error {
	lease := s.lease
	if len(lease.holders) > 1 || !lease.holders[s] {
		delete(lease.holders, s)
		return nil
	}
	dump, err := c.readPGPrimaryMap(ctx, fsid)
	if err != nil {
		return err
	}
	switch dump.requireMinCompat {
	case lease.prior:
	case lease.raised:
		if _, err := c.conn().ceph(ctx, "osd", "set-require-min-compat-client", lease.prior); err != nil {
			return err
		}
		dump, err = c.readPGPrimaryMap(ctx, fsid)
		if err != nil {
			return err
		}
		if dump.requireMinCompat != lease.prior {
			return errors.New("require_min_compat_client restoration readback differs from the prior release")
		}
	default:
		return invalid(errors.New("require_min_compat_client changed outside the PG primary override; refusing restoration"))
	}
	delete(lease.holders, s)
	c.faults.release(lease.token)
	return nil
}

// minCompatLease returns the cluster's held compatibility lease, or nil. The
// caller holds c.mu.
func (c *Container) minCompatLease() *minCompatLease {
	lease, _ := c.faults.handles(faultMinCompatClient)[minCompatLeaseKey].(*minCompatLease)
	return lease
}

// raiseMinCompat raises require_min_compat_client from have to need and
// returns the lease that owns the raise. A nil lease means that nothing
// changed; a lease with an error means that the raise is uncertain. The
// caller holds c.mu.
func (c *Container) raiseMinCompat(ctx context.Context, have, need string) (*minCompatLease, error) {
	token, _, err := c.faults.reserve(ctx, faultMinCompatClient, minCompatLeaseKey)
	if err != nil {
		return nil, err
	}
	_, err = c.conn().ceph(ctx, "osd", "set-require-min-compat-client", need)
	if definiteRefusal(err) {
		c.faults.settle(token, "", nil, nil, nil)
		return nil, err
	}
	lease := &minCompatLease{prior: have, raised: need, holders: map[*pgPrimaryState]bool{}, token: token}
	c.faults.settle(token, "", lease, nil, nil)
	return lease, err
}

// upmapPrimary sets the pg-upmap-primary entry of pg to osd.
func (c *Container) upmapPrimary(ctx context.Context, pg PGID, osd int) error {
	_, err := c.conn().ceph(ctx, "osd", "pg-upmap-primary", string(pg), strconv.Itoa(osd))
	return err
}

// compatRefusal reports the MON's refusal of pg-upmap-primary for a low
// require_min_compat_client, with the needed and the current release.
func compatRefusal(err error) (need, have string, refused bool) {
	var failure *NativeFailure
	if !errors.As(err, &failure) || failure.Errno != "EPERM" {
		return "", "", false
	}
	match := upmapPrimaryCompatRefusal.FindStringSubmatch(failure.Reason)
	if match == nil {
		return "", "", false
	}
	return match[2], match[1], true
}

// definiteRefusal reports a native failure in which Ceph named its error
// code: the MON answered and refused, so nothing changed.
func definiteRefusal(err error) bool {
	var failure *NativeFailure
	return errors.As(err, &failure) && failure.Errno != ""
}

func pgActiveClean(state string) bool {
	tokens := strings.Split(state, "+")
	return slices.Contains(tokens, "active") && slices.Contains(tokens, "clean")
}

// checkPGOfPool refuses a PG that is not one of pool's current PGs.
func checkPGOfPool(pg PGID, pool pgPrimaryPool) error {
	poolID, seed, _ := strings.Cut(string(pg), ".")
	ps, err := strconv.ParseUint(seed, 16, 32)
	if poolID != strconv.FormatInt(pool.id, 10) || err != nil || ps >= uint64(pool.pgNum) {
		return invalidRequest("Pool.OverridePGPrimary: PG %s is not a PG of pool %q", pg, pool.name)
	}
	if !pool.replicated {
		return invalidRequest("Pool.OverridePGPrimary: pool %q is not replicated; pg-upmap-primary supports replicated pools only", pool.name)
	}
	return nil
}

// pgPrimaryFSID returns the original bootstrap FSID. The caller holds c.mu.
func (c *Container) pgPrimaryFSID(ctx context.Context) (string, error) {
	identity, err := c.bootstrap.identity(ctx)
	if err != nil {
		return "", err
	}
	if identity.err != nil || !identity.confirmed {
		return "", clientOperationError(ctx, "ceph original bootstrap identity is unavailable", identity.err)
	}
	return identity.fsid, nil
}

// pgPrimaryMap is the part of an osd dump that a PG primary override needs.
type pgPrimaryMap struct {
	requireMinCompat string
	pools            map[string]pgPrimaryPool
	osds             map[int]pgPrimaryOSD
	entries          map[string]int
	primaryTemp      []string
}

type pgPrimaryPool struct {
	id         int64
	name       string
	pgNum      int
	replicated bool
}

type pgPrimaryOSD struct {
	uuid string
	up   bool
}

// entry returns the pg-upmap-primary entry of pg, or noUpmapPrimary.
func (m pgPrimaryMap) entry(pg PGID) int {
	if osd, ok := m.entries[string(pg)]; ok {
		return osd
	}
	return noUpmapPrimary
}

// readPGPrimaryMap reads the OSDMap and checks that it belongs to fsid.
func (c *Container) readPGPrimaryMap(ctx context.Context, fsid string) (pgPrimaryMap, error) {
	data, err := c.conn().ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return pgPrimaryMap{}, err
	}
	dump, nativeFSID, err := decodePGPrimaryMap(data)
	if err != nil {
		return pgPrimaryMap{}, err
	}
	if nativeFSID != fsid {
		return pgPrimaryMap{}, &pgPrimaryFinal{err: invalid(errForeignFSID)}
	}
	return dump, nil
}

func decodePGPrimaryMap(data []byte) (pgPrimaryMap, string, error) {
	var native struct {
		FSID    *string `json:"fsid"`
		Require *string `json:"require_min_compat_client"`
		Pools   *[]struct {
			ID    *int64  `json:"pool"`
			Name  *string `json:"pool_name"`
			Type  *uint8  `json:"type"`
			PGNum *uint32 `json:"pg_num"`
		} `json:"pools"`
		OSDs *[]struct {
			ID   *int    `json:"osd"`
			UUID *string `json:"uuid"`
			Up   *int    `json:"up"`
		} `json:"osds"`
		Entries *[]struct {
			PGID *string `json:"pgid"`
			OSD  *int    `json:"primary_osd"`
		} `json:"pg_upmap_primaries"`
		// primary_temp alternates PG IDs and OSD IDs in one array.
		PrimaryTemp *[]any `json:"primary_temp"`
	}
	failure := errors.New("incomplete or invalid native OSDMap for a PG primary override")
	if err := healthDetailsJSON(data, &native); err != nil || native.FSID == nil || native.Require == nil || *native.Require == "" ||
		native.Pools == nil || native.OSDs == nil || native.Entries == nil || native.PrimaryTemp == nil {
		return pgPrimaryMap{}, "", failure
	}
	result := pgPrimaryMap{requireMinCompat: *native.Require, pools: map[string]pgPrimaryPool{}, osds: map[int]pgPrimaryOSD{}, entries: map[string]int{}}
	for _, pool := range *native.Pools {
		if pool.ID == nil || *pool.ID < 0 || pool.Name == nil || *pool.Name == "" || pool.Type == nil || pool.PGNum == nil {
			return pgPrimaryMap{}, "", failure
		}
		if _, dup := result.pools[*pool.Name]; dup {
			return pgPrimaryMap{}, "", failure
		}
		result.pools[*pool.Name] = pgPrimaryPool{id: *pool.ID, name: *pool.Name, pgNum: int(*pool.PGNum), replicated: *pool.Type == 1}
	}
	for _, osd := range *native.OSDs {
		if osd.ID == nil || *osd.ID < 0 || osd.UUID == nil || osd.Up == nil {
			return pgPrimaryMap{}, "", failure
		}
		if _, dup := result.osds[*osd.ID]; dup {
			return pgPrimaryMap{}, "", failure
		}
		result.osds[*osd.ID] = pgPrimaryOSD{uuid: *osd.UUID, up: *osd.Up == 1}
	}
	for _, entry := range *native.Entries {
		if entry.PGID == nil || entry.OSD == nil || *entry.OSD < 0 {
			return pgPrimaryMap{}, "", failure
		}
		if _, dup := result.entries[*entry.PGID]; dup {
			return pgPrimaryMap{}, "", failure
		}
		result.entries[*entry.PGID] = *entry.OSD
	}
	for _, item := range *native.PrimaryTemp {
		if pg, ok := item.(string); ok {
			result.primaryTemp = append(result.primaryTemp, pg)
		}
	}
	return result, *native.FSID, nil
}

// pgMapActing returns the acting set that the OSDMap computes for pg. For a
// replicated pool without a primary_temp entry, its first member is the
// acting primary: Ceph moves the primary, an upmap primary included, to the
// front.
func (c *Container) pgMapActing(ctx context.Context, pg PGID) ([]int, error) {
	data, err := c.conn().ceph(ctx, "pg", "map", string(pg), "--format", "json")
	if err != nil {
		return nil, err
	}
	var native struct {
		PGID   *string `json:"pgid"`
		Acting *[]int  `json:"acting"`
	}
	if err := healthDetailsJSON(data, &native); err != nil || native.PGID == nil || *native.PGID != string(pg) || native.Acting == nil || len(*native.Acting) == 0 {
		return nil, errors.New("incomplete or invalid native PG mapping")
	}
	return *native.Acting, nil
}

// pgServingPrimary asks the PG itself, through pg query, which acting
// primary serves it and in which state.
func (c *Container) pgServingPrimary(ctx context.Context, pg PGID) (int, string, error) {
	data, err := c.conn().ceph(ctx, "pg", string(pg), "query", "--format", "json")
	if err != nil {
		return 0, "", err
	}
	var native struct {
		State *string `json:"state"`
		Info  *struct {
			PGID  *string `json:"pgid"`
			Stats *struct {
				ActingPrimary *int `json:"acting_primary"`
			} `json:"stats"`
		} `json:"info"`
	}
	if err := healthDetailsJSON(data, &native); err != nil || native.State == nil || native.Info == nil || native.Info.PGID == nil ||
		*native.Info.PGID != string(pg) || native.Info.Stats == nil || native.Info.Stats.ActingPrimary == nil {
		return 0, "", errors.New("incomplete or invalid native PG query")
	}
	return *native.Info.Stats.ActingPrimary, *native.State, nil
}
