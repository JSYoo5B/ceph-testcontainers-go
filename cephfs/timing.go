package cephfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// TimingProfile selects qualified fixture conditions. Its zero value changes
// nothing. It does not speed up clocks, I/O, failure detection or lease expiry.
type TimingProfile struct {
	// DirectoryPins reduces MDS upkeep ticks from five seconds to one for
	// directory-pin setup on Ceph 19.2.5 and 20.2.4. At least two healthy,
	// owned active ranks are required. Other MDS upkeep also runs more often;
	// do not use it to qualify session expiry, reconnect or failover behavior.
	DirectoryPins bool
}

// OverrideTiming applies profile to this filesystem's current owned active
// MDSs. DirectoryPins requires their effective mds_tick_interval to be five
// seconds and sets a separate central entry for each selected daemon to one.
// Every tracked MDS, including a standby, must retain confirmed ownership;
// an edited or partially started tracked daemon makes the profile unavailable.
// Every target's native version, ownership and effective value is checked before
// the first write; effective application is then awaited within the cluster's
// startup timeout. A local file or runtime override can prevent application.
// An already scheduled tick keeps its original deadline, so the first pin
// transition may still wait five seconds. Future scaled or replaced MDSs are
// outside the returned handles' scope.
//
// The returned handles are the existing central-configuration overrides, not
// a new cleanup owner. Retain all of them even on error and Restore them in
// reverse order when the condition should end. An error can leave earlier or
// uncertain entries changed. Restoration restores database entries; allow the
// native daemons time to consume the restored values before timing-sensitive
// assertions. Do not concurrently edit these entries, restart daemons, change
// topology or remove the filesystem while applying the profile or restoring it.
// Cluster termination disposes of the configuration database.
//
//purpose:condition
func (fs *Filesystem) OverrideTiming(ctx context.Context, profile TimingProfile) ([]*override.ConfigHandle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !profile.DirectoryPins {
		return nil, nil
	}
	core := fs.base()
	var daemons []*MDS
	ctx, done, err := core.Admit(ctx, fsbase.AdmissionSpec{
		RecheckCanceled: true,
		Refusal:         "CephFS timing requires the original registered filesystem and confirmed native identity",
		Check: func(context.Context) (bool, error) {
			if !core.Registered() || !core.DescriptorIntact() || core.Native() == nil {
				return false, nil
			}
			var err error
			daemons, err = fs.fleet().timingSnapshot()
			return err == nil, err
		},
	})
	if err != nil {
		return nil, err
	}
	defer done()
	if _, err := core.ReadNativePools(ctx); err != nil {
		return nil, err
	}
	status, err := fs.MDSStatus(ctx)
	if err != nil {
		return nil, err
	}
	if status.FilesystemID != core.Native().ID || status.MaxMDS < 2 || len(status.Active) != status.MaxMDS {
		return nil, errors.New("directory-pin timing requires at least two healthy owned active ranks")
	}
	owned := make(map[string]bool, len(daemons))
	for _, daemon := range daemons {
		owned[daemon.identity.name] = true
	}
	targets := make([]string, 0, len(status.Active))
	for rank, active := range status.Active {
		if active.Rank != rank || !active.Owned || !owned[active.Name] || active.State != "up:active" || active.GID == 0 {
			return nil, errors.New("directory-pin timing requires healthy original owned MDSs for every active rank")
		}
		target := "mds." + active.Name
		if err := checkTimingDaemon(ctx, core.Cluster(), target); err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	var handles []*override.ConfigHandle
	for _, target := range targets {
		handle, err := override.SetConfig(ctx, core.Cluster(), ceph.ConfigSetting{Section: target, Name: "mds_tick_interval", Value: "1"})
		if handle != nil {
			handles = append(handles, handle)
		}
		if err != nil {
			return handles, fmt.Errorf("apply directory-pin timing to %s: %w", target, err)
		}
		if err := clusterutil.Poll(ctx, func() (bool, error) {
			value, err := readTimingInterval(ctx, core.Cluster(), target)
			if err != nil {
				return false, err
			}
			if value == 1 {
				return true, nil
			}
			return false, fmt.Errorf("%s has not consumed directory-pin timing", target)
		}); err != nil {
			return handles, fmt.Errorf("verify directory-pin timing on %s: %w", target, err)
		}
	}
	return handles, nil
}

// timingSnapshot runs in Admit's owner-held check. It keeps the roster read
// inside the MDS fleet and rejects edited or uncertain daemon descriptors.
func (f mdsFleet) timingSnapshot() ([]*MDS, error) {
	daemons := f.mds.roster.snapshot()
	for _, daemon := range daemons {
		if daemon == nil || daemon.identity == nil {
			return nil, errors.New("CephFS timing requires confirmed original MDS ownership")
		}
		if err := f.checkStoppedMDSOwner(daemon, daemon.identity); err != nil {
			return nil, err
		}
	}
	return daemons, nil
}

func checkTimingDaemon(ctx context.Context, cluster *ceph.Container, target string) error {
	data, err := cluster.Ceph(ctx, "tell", target, "version")
	if err != nil {
		return err
	}
	var version struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(data, &version); err != nil || (version.Version != "19.2.5" && version.Version != "20.2.4") {
		return fmt.Errorf("directory-pin timing is not qualified for %s's native version", target)
	}
	value, err := readTimingInterval(ctx, cluster, target)
	if err != nil {
		return err
	}
	if value != 5 {
		return fmt.Errorf("directory-pin timing requires %s's effective mds_tick_interval to be five seconds", target)
	}
	return nil
}

func readTimingInterval(ctx context.Context, cluster *ceph.Container, target string) (float64, error) {
	data, err := cluster.Ceph(ctx, "config", "show", target, "mds_tick_interval")
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
	if err != nil {
		return 0, fmt.Errorf("read effective MDS upkeep interval on %s: %w", target, err)
	}
	return value, nil
}
