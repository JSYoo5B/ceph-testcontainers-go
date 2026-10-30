package fault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
)

var pgIDPattern = regexp.MustCompile(`^[0-9]+\.[0-9a-f]+$`)

func validatePGID(pgid string) error {
	if !pgIDPattern.MatchString(pgid) {
		return invalid(errors.New("invalid native PG ID"))
	}
	return nil
}

func invalid(err error) error { return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Err: err} }

// RADOS accepts arbitrary object names, but the fixture passes them as CLI
// arguments; reject names that the CLI would parse as options or truncate.
func validateObjectName(name string) error {
	if name == "" || len(name) > 4096 || strings.HasPrefix(name, "-") || strings.ContainsRune(name, 0) {
		return invalid(errors.New("object name must be non-empty, at most 4096 bytes, without NUL and not start with a dash"))
	}
	return nil
}

// validateInjectObjectName additionally refuses a slash. The native
// injectdataerr command splits its object word at the first slash into
// namespace and object, while the preflight "osd map" call takes the word as a
// literal object in the default namespace. A name with a slash would therefore
// be mapped as one identity and injected as another, so it is refused before
// any native command instead of being reinterpreted.
func validateInjectObjectName(name string) error {
	if err := validateObjectName(name); err != nil {
		return err
	}
	if strings.ContainsRune(name, '/') {
		return invalid(errors.New("object name must not contain a slash: injectdataerr would read the part before it as a namespace"))
	}
	return nil
}

// InjectObjectDataError makes one replica of an object on an owned OSD return
// a read error to scrub and recovery, through the native injectdataerr debug
// command. The OSD must be in the object's current acting set of a replicated
// pool. The command needs bluestore_debug_inject_read_err, which this function
// enables on that daemon only through its runtime configuration; it is not
// stored centrally and lasts until the OSD restarts. The setting has no effect
// on objects without an injected error.
//
// The injection is one-shot and returns no handle: the cluster does not
// retain it and Terminate does not undo it. It stays in OSD memory until the
// replica is rewritten, such as by ceph.Container.RepairPG, or the OSD
// restarts. Client reads served by a healthy primary are unaffected. Use
// ceph.Container.DeepScrubPG to report the damage and
// ceph.Observer.PGInconsistencies, ceph.Observer.HealthDetails
// (OSD_SCRUB_ERRORS, PG_DAMAGED) or ceph.Pool.PGs to observe it. It
// needs BlueStore OSDs and refuses a cluster that uses OSDMemStore. A failure
// after the debug setting was enabled leaves it enabled and is not retried or
// disarmed.
//
// Inject into a non-primary replica to leave scrub evidence: client reads
// served by the primary are unaffected, a deep scrub reports read_error on
// that shard, the PG becomes active+clean+inconsistent and health reports
// OSD_SCRUB_ERRORS and PG_DAMAGED. An error on the primary does not make
// client reads fail either, and a later deep scrub reports no inconsistency.
//
// The object is addressed in the default namespace. A name containing a slash
// is refused before any native command, because the native command would
// reinterpret the part before the first slash as a namespace.
//
//purpose:condition,native
func InjectObjectDataError(ctx context.Context, c *ceph.Container, pool, object string, osdID int) error {
	if !faulthook.BlueStore(c) {
		return errors.New("InjectObjectDataError needs BlueStore OSDs")
	}
	if !naming.ExistingPool.MatchString(pool) {
		return invalid(errors.New("invalid native pool name"))
	}
	if err := validateInjectObjectName(object); err != nil {
		return err
	}
	return faulthook.WithTopology(ctx, c, func() error {
		if err := faulthook.PoolPolicyReady(c); err != nil {
			return err
		}
		if !faulthook.OwnedOSD(c, osdID) {
			return invalid(fmt.Errorf("OSD %d is not owned by this cluster", osdID))
		}
		ctx, cancel := context.WithTimeout(ctx, faulthook.StartupTimeout(c))
		defer cancel()
		state, err := faulthook.PoolState(ctx, c, pool)
		if err != nil {
			return err
		}
		if state.Type != "replicated" {
			return errors.New("data error injection supports replicated pools")
		}
		data, err := faulthook.CLI(ctx, c, "osd", "map", pool, object, "--format", "json")
		if err != nil {
			return err
		}
		var mapping struct {
			PoolID *int64 `json:"pool_id"`
			PGID   string `json:"pgid"`
			Acting []int  `json:"acting"`
		}
		if err := json.Unmarshal(data, &mapping); err != nil || mapping.PoolID == nil || *mapping.PoolID != state.ID || validatePGID(mapping.PGID) != nil {
			return errors.New("decode native object mapping")
		}
		if !slices.Contains(mapping.Acting, osdID) {
			return fmt.Errorf("OSD %d is not in the acting set %v of PG %s", osdID, mapping.Acting, mapping.PGID)
		}
		daemon := "osd." + strconv.Itoa(osdID)
		if _, err := faulthook.CLI(ctx, c, "tell", daemon, "config", "set", "bluestore_debug_inject_read_err", "true"); err != nil {
			return fmt.Errorf("enable data error injection on %s: %w", daemon, err)
		}
		if err := faulthook.CheckPoolID(ctx, c, state); err != nil {
			return err
		}
		if _, err := faulthook.CLI(ctx, c, "tell", daemon, "injectdataerr", pool, object); err != nil {
			return fmt.Errorf("inject data error on %s: %w", daemon, err)
		}
		return nil
	})
}
