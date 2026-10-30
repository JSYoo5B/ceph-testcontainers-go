package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"time"
)

// osdMap is the cluster's native OSD map view: the OSD dump and flags, the
// up/in wait, device classes and the cluster FSID the OSDs carry. It holds no
// state and takes no lock; callers keep the topology lock and the OSD registry.
type osdMap struct {
	conn cephConn
}

// OSDState reports authoritative OSDMap state for an owned OSD. Weight is the
// native map reweight in [0,1], not the CRUSH capacity weight. Out OSDs retain
// their containers, identities and data; absence or a replaced UUID is an error.
type OSDState struct {
	ID     int
	UUID   string
	Up, In bool
	Weight float64
}

// osdStates implements Observer.OSDStates.
func (c *Container) osdStates(ctx context.Context) ([]OSDState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	owned := make(map[int]*osdRegistration, c.osdHandles.count())
	for id, osd := range c.osdHandles.each() {
		owned[id] = registrationOf(osd)
	}
	return readOwnedOSDStates(ctx, c.conn().ceph, owned)
}

// OSDFlag is a global OSDMap flag token such as "noout". The constants name
// the flags that override.SetOSDFlag can change. OSDFlags returns the flags that
// the cluster reports, which can include other tokens, so the type stays open: a
// value outside the constants is representable and override.SetOSDFlag refuses
// it at run time.
type OSDFlag string

// The reversible global OSD flags that override.SetOSDFlag accepts. Composite
// pause and irreversible compatibility flags are not among them.
const (
	OSDFlagNoUp        OSDFlag = "noup"
	OSDFlagNoDown      OSDFlag = "nodown"
	OSDFlagNoOut       OSDFlag = "noout"
	OSDFlagNoIn        OSDFlag = "noin"
	OSDFlagNoBackfill  OSDFlag = "nobackfill"
	OSDFlagNoRebalance OSDFlag = "norebalance"
	OSDFlagNoRecover   OSDFlag = "norecover"
	OSDFlagNoScrub     OSDFlag = "noscrub"
	OSDFlagNoDeepScrub OSDFlag = "nodeep-scrub"
	OSDFlagNoSnapTrim  OSDFlag = "nosnaptrim"
	OSDFlagNoAutoscale OSDFlag = "noautoscale"
)

// String returns the native flag token.
func (f OSDFlag) String() string { return string(f) }

// osdFlags implements Observer.OSDFlags.
func (c *Container) osdFlags(ctx context.Context) ([]OSDFlag, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	return readOSDFlags(ctx, c.conn().ceph)
}

func (om osdMap) setFlag(ctx context.Context, flag OSDFlag, enabled bool) error {
	op := "unset"
	if enabled {
		op = "set"
	}
	_, err := om.conn.ceph(ctx, "osd", op, string(flag))
	return err
}

type osdPolicySnapshot struct {
	flags []string
	osds  map[int]OSDState
}

func (om osdMap) policyDump(ctx context.Context) (osdPolicySnapshot, error) {
	return readOSDPolicyDump(ctx, om.conn.ceph)
}

// decodeOSDPolicyDump decodes the flags and OSDs of the output of osd dump.
func decodeOSDPolicyDump(data []byte) (osdPolicySnapshot, error) {
	result := osdPolicySnapshot{osds: make(map[int]OSDState)}
	var dump struct {
		Flags *string `json:"flags"`
		OSDs  []struct {
			ID     *int   `json:"osd"`
			UUID   string `json:"uuid"`
			Up, In *int
			Weight *float64 `json:"weight"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(data, &dump); err != nil || dump.Flags == nil || dump.OSDs == nil {
		return result, errors.New("decode native OSDMap state")
	}
	if *dump.Flags != "" {
		result.flags = strings.Split(*dump.Flags, ",")
		for _, flag := range result.flags {
			if flag == "" || strings.TrimSpace(flag) != flag {
				return result, errors.New("invalid native OSDMap flag token")
			}
		}
		slices.Sort(result.flags)
		if len(slices.Compact(slices.Clone(result.flags))) != len(result.flags) {
			return result, errors.New("duplicate native OSDMap flag token")
		}
	}
	for _, osd := range dump.OSDs {
		if osd.ID == nil || *osd.ID < 0 || osd.Up == nil || osd.In == nil || osd.Weight == nil || (*osd.Up != 0 && *osd.Up != 1) || (*osd.In != 0 && *osd.In != 1) || math.IsNaN(*osd.Weight) || math.IsInf(*osd.Weight, 0) || *osd.Weight < 0 || *osd.Weight > 1 {
			return result, errors.New("invalid native OSDMap membership state")
		}
		if _, exists := result.osds[*osd.ID]; exists {
			return result, errors.New("duplicate native OSDMap OSD ID")
		}
		result.osds[*osd.ID] = OSDState{ID: *osd.ID, UUID: osd.UUID, Up: *osd.Up == 1, In: *osd.In == 1, Weight: *osd.Weight}
	}
	return result, nil
}

// ownedOSDState resolves id against a native snapshot and the OSD registry.
// The caller holds c.mu.
func (c *Container) ownedOSDState(dump osdPolicySnapshot, id int) (OSDState, error) {
	return resolveOwnedOSDState(dump, id, registrationOf(c.osdHandles.get(id)))
}

// WaitForPGClean waits for every existing PG to be exactly active+clean. Unlike
// WaitForClean it allows intentionally out/down OSDs, and does not assert any
// OSD topology. At least one PG and an available manager are required. This is
// a recovery barrier, not a promise of cluster health or daemon availability.
func (c *Container) WaitForPGClean(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		// Serialize each observation with topology changes/termination without
		// holding the cluster lock across the recovery wait.
		if err := c.lockTopology(ctx); err != nil {
			return errors.Join(err, last)
		}
		if err := c.waitPolicyReady(ctx); err != nil {
			c.mu.Unlock()
			if ctx.Err() != nil {
				return errors.Join(ctx.Err(), last, err)
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return errors.Join(err, last)
		}
		s, err := readStatus(ctx, c.conn().ceph)
		c.mu.Unlock()
		if err != nil {
			last = err
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(err, last)
		}
		if err == nil && pgsAreClean(s) {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

func pgsAreClean(s Status) bool {
	if !s.MgrMap.Available || s.PGMap.NumPGs == 0 {
		return false
	}
	clean := 0
	for _, state := range s.PGMap.PGsByState {
		if state.StateName != "active+clean" || state.Count <= 0 {
			return false
		}
		clean += state.Count
	}
	return clean == s.PGMap.NumPGs
}
