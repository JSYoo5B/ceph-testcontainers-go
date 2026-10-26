package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// OSDState reports authoritative OSDMap state for an owned OSD. Weight is the
// native map reweight in [0,1], not the CRUSH capacity weight. Out OSDs retain
// their containers, identities and data; absence or a replaced UUID is an error.
type OSDState struct {
	ID     int
	UUID   string
	Up, In bool
	Weight float64
}

// OSDStates returns owned OSD states sorted by native ID. It refuses an owned
// ID that is missing, purged or now belongs to a different native UUID.
func (c *Container) OSDStates(ctx context.Context) ([]OSDState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	dump, err := c.osdPolicyDump(ctx)
	if err != nil {
		return nil, err
	}
	states := make([]OSDState, 0, len(c.osds))
	for id := range c.osds {
		state, err := c.ownedOSDState(dump, id)
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	slices.SortFunc(states, func(a, b OSDState) int { return a.ID - b.ID })
	return states, nil
}

// setOSDIn implements OSDContainer.SetIn, which documents the behavior. The caller
// admitted the shared-cluster policy; the handle is checked under the lock.
func (c *Container) setOSDIn(ctx context.Context, handle *OSDContainer, in bool) error {
	id := handle.ID
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return err
	}
	if err := c.currentOSD(handle); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	dump, err := c.osdPolicyDump(ctx)
	if err != nil {
		return err
	}
	state, err := c.ownedOSDState(dump, id)
	if err != nil {
		return err
	}
	if state.In == in {
		return nil
	}
	op := "out"
	if in {
		op = "in"
	}
	if _, err := c.cephCLI(ctx, "osd", op, strconv.Itoa(id)); err != nil {
		return err
	}
	dump, err = c.osdPolicyDump(ctx)
	if err != nil {
		return err
	}
	state, err = c.ownedOSDState(dump, id)
	if err != nil {
		return err
	}
	if state.In != in {
		return fmt.Errorf("osd.%d membership readback differs from requested in=%v", id, in)
	}
	return nil
}

// OSDFlag is a global OSDMap flag token such as "noout". The constants name
// the flags that OverrideOSDFlag can change. OSDFlags returns the flags that the
// cluster reports, which can include other tokens, so the type stays open: a
// value outside the constants is representable and OverrideOSDFlag refuses it
// at run time.
type OSDFlag string

// The reversible global OSD flags that OverrideOSDFlag accepts. Composite
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

// OSDFlags lists exact global OSDMap flag tokens, including native compatibility
// flags that are not among the OSDFlag constants. Per-OSD, CRUSH-node or
// device-class flags are separate native state.
func (c *Container) OSDFlags(ctx context.Context) ([]OSDFlag, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	dump, err := c.osdPolicyDump(ctx)
	if err != nil {
		return nil, err
	}
	flags := make([]OSDFlag, len(dump.flags))
	for i, flag := range dump.flags {
		flags[i] = OSDFlag(flag)
	}
	return flags, nil
}

// OSDFlagOverrideHandle manages restoration of one global OSDMap flag, retaining
// prior state and ownership even when application is uncertain. Copies share
// lifecycle state.
// Restore explicitly when the condition should end; termination disposes of
// the entire OSDMap. A non-nil handle returned with an error remains tracked and
// must be restored or the cluster terminated. Unrelated flags are never changed.
type OSDFlagOverrideHandle struct {
	owner *Container
	flag  OSDFlag
	state *osdFlagOverrideState
}

type osdFlagOverrideState struct {
	previous, applied, restored bool
}

// OverrideOSDFlag changes one reversible global flag while remembering its
// prior state. Supported flags are the OSDFlag constants: OSDFlagNoUp,
// OSDFlagNoDown, OSDFlagNoOut, OSDFlagNoIn, OSDFlagNoBackfill,
// OSDFlagNoRebalance, OSDFlagNoRecover, OSDFlagNoScrub, OSDFlagNoDeepScrub,
// OSDFlagNoSnapTrim and OSDFlagNoAutoscale. Any other value, including an
// untyped string constant that names another flag, is refused with an error.
// Composite pause and irreversible compatibility flags are excluded. Overlapping
// handles for a flag are refused. External edits to the same flag must not race
// apply/restore: a boolean flag has no generation and Ceph provides no CAS.
func (c *Container) OverrideOSDFlag(ctx context.Context, flag OSDFlag, enabled bool) (*OSDFlagOverrideHandle, error) {
	if gateErr := c.sharedGate("OverrideOSDFlag"); gateErr != nil {
		return nil, gateErr
	}
	if !slices.Contains(reversibleOSDFlags, flag) {
		return nil, errors.New("unsupported reversible global OSD flag")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	if prior := c.flagOverrides[flag]; prior != nil && !prior.state.restored {
		return nil, errors.New("restore the existing OSD flag override first")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	dump, err := c.osdPolicyDump(ctx)
	if err != nil {
		return nil, err
	}
	change := &OSDFlagOverrideHandle{owner: c, flag: flag, state: &osdFlagOverrideState{previous: slices.Contains(dump.flags, string(flag)), applied: enabled}}
	if c.flagOverrides == nil {
		c.flagOverrides = make(map[OSDFlag]*OSDFlagOverrideHandle)
	}
	c.flagOverrides[flag] = change
	if change.state.previous == enabled {
		return change, nil
	}
	if err := c.setOSDFlag(ctx, flag, enabled); err != nil {
		return change, err
	}
	dump, err = c.osdPolicyDump(ctx)
	if err != nil {
		return change, err
	}
	if slices.Contains(dump.flags, string(flag)) != enabled {
		return change, errors.New("OSD flag readback differs from requested state")
	}
	return change, nil
}

// Restore returns only this flag to its original state. It is idempotent and
// reconciles a lost restoration reply before another native mutation. A no-op
// lease refuses an outside change rather than overwriting it. Outside writes
// indistinguishable from applied/prior boolean states cannot be detected.
func (change *OSDFlagOverrideHandle) Restore(ctx context.Context) error {
	if change == nil || change.owner == nil || change.state == nil {
		return errors.New("OSD flag override is unavailable")
	}
	c := change.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if change.state.restored {
		return nil
	}
	if err := c.poolPolicyReady(); err != nil {
		return err
	}
	tracked := c.flagOverrides[change.flag]
	if tracked == nil || tracked.state != change.state {
		return errors.New("OSD flag override is not tracked by this cluster")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	dump, err := c.osdPolicyDump(ctx)
	if err != nil {
		return err
	}
	current := slices.Contains(dump.flags, string(change.flag))
	if current == change.state.previous {
		change.finish()
		return nil
	}
	if current != change.state.applied {
		return invalid(errors.New("OSD flag changed outside this override; refusing restoration"))
	}
	if err := c.setOSDFlag(ctx, change.flag, change.state.previous); err != nil {
		return err
	}
	dump, err = c.osdPolicyDump(ctx)
	if err != nil {
		return err
	}
	if slices.Contains(dump.flags, string(change.flag)) != change.state.previous {
		return errors.New("OSD flag restoration readback differs from original state")
	}
	change.finish()
	return nil
}

func (change *OSDFlagOverrideHandle) finish() {
	change.state.restored = true
	delete(change.owner.flagOverrides, change.flag)
}

var reversibleOSDFlags = []OSDFlag{OSDFlagNoUp, OSDFlagNoDown, OSDFlagNoOut, OSDFlagNoIn, OSDFlagNoBackfill, OSDFlagNoRebalance, OSDFlagNoRecover, OSDFlagNoScrub, OSDFlagNoDeepScrub, OSDFlagNoSnapTrim, OSDFlagNoAutoscale}

func (c *Container) setOSDFlag(ctx context.Context, flag OSDFlag, enabled bool) error {
	op := "unset"
	if enabled {
		op = "set"
	}
	_, err := c.cephCLI(ctx, "osd", op, string(flag))
	return err
}

type osdPolicySnapshot struct {
	flags []string
	osds  map[int]OSDState
}

func (c *Container) osdPolicyDump(ctx context.Context) (osdPolicySnapshot, error) {
	data, err := c.cephCLI(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return osdPolicySnapshot{osds: make(map[int]OSDState)}, err
	}
	return decodeOSDPolicyDump(data)
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

func (c *Container) ownedOSDState(dump osdPolicySnapshot, id int) (OSDState, error) {
	osd := c.osds[id]
	if osd == nil || osd.purged {
		return OSDState{}, fmt.Errorf("osd.%d is not an active owned OSD", id)
	}
	expected, err := uuid.Parse(osd.nativeUUID)
	if err != nil || expected == uuid.Nil {
		return OSDState{}, fmt.Errorf("osd.%d registration UUID is unavailable", id)
	}
	state, exists := dump.osds[id]
	if !exists {
		return OSDState{}, fmt.Errorf("owned osd.%d is missing from native OSDMap", id)
	}
	actual, err := uuid.Parse(state.UUID)
	if err != nil || actual != expected {
		return OSDState{}, fmt.Errorf("native osd.%d UUID no longer matches its owned registration", id)
	}
	return state, nil
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
		s, err := c.Status(ctx)
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
