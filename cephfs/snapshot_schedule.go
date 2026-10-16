package cephfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// snapshotSchedulePeriod is a schedule's repeat or a retention count's
// period, as the MGR snap_schedule module reads it: minutes, hours, days,
// weeks, months or years.
var snapshotSchedulePeriod = regexp.MustCompile(`^[1-9][0-9]*[mhdwMy]$`)

// SnapshotSchedule configures TemporarySnapshotSchedule.
type SnapshotSchedule struct {
	// Path is the directory to snapshot, an absolute path in the filesystem,
	// such as "/" or "/data". It must exist.
	Path string
	// Repeat is how often a snapshot is taken: a positive number and one of
	// m (minutes), h, d, w, M (months) or y, such as "6h" or "1m". The module
	// takes minutes only while mgr/snap_schedule/allow_m_granularity is true,
	// which TemporaryConfig can set for a test.
	Repeat string
	// Start is the schedule's first time slot; the zero time leaves the
	// module's default, midnight of the day the schedule is added, in UTC.
	Start time.Time
	// Retention keeps this many of the schedule's snapshots per period, keyed
	// by m, h, d, w, M or y, or n for the newest snapshots of any period. Nil
	// keeps every snapshot that the module takes.
	Retention map[string]int
}

// SnapshotScheduleStatus is what the snap_schedule module reports for a
// schedule: native state, not the requested SnapshotSchedule.
type SnapshotScheduleStatus struct {
	// Path and Schedule identify the schedule; Active is false after the
	// module deactivated it.
	Path, Schedule string
	Active         bool
	// Retention is the module's retention specification.
	Retention map[string]int
	// Created and Pruned count the snapshots that the module took and
	// removed; First and Last are its first and last snapshot times, zero
	// before the first.
	Created, Pruned int
	First, Last     time.Time
	// Raw is the module's JSON for the schedule.
	Raw json.RawMessage
}

// SnapshotScheduleLease owns one snapshot schedule of a directory. Restore
// removes the schedule and its retention; the snapshots it took stay. Retain
// a non-nil lease returned with an error: the schedule may have been added.
type SnapshotScheduleLease struct {
	// Path, Repeat and Retention are the requested schedule.
	Path, Repeat string
	Retention    map[string]int

	filesystem *Filesystem
	mu         sync.Mutex
	restored   bool
}

func validateSnapshotSchedule(schedule SnapshotSchedule) error {
	switch {
	case !strings.HasPrefix(schedule.Path, "/") || path.Clean(schedule.Path) != schedule.Path:
		return invalid(fmt.Errorf("snapshot schedule path %q must be a clean absolute path", schedule.Path))
	case !snapshotSchedulePeriod.MatchString(schedule.Repeat):
		return invalid(fmt.Errorf("snapshot schedule repeat %q must be a number and one of m, h, d, w, M or y", schedule.Repeat))
	}
	for period, count := range schedule.Retention {
		if !strings.Contains("mhdwMyn", period) || len(period) != 1 || count <= 0 {
			return invalid(fmt.Errorf("snapshot retention %s=%d: want a positive count of m, h, d, w, M, y or n", period, count))
		}
	}
	return nil
}

// TemporarySnapshotSchedule adds a snapshot schedule, and its retention, to a
// directory of the filesystem through the MGR snap_schedule module, which
// takes "scheduled-…" snapshots in the directory's .snap. It never enables the
// module: enable it with ceph.Container.TemporaryMGRModule first, and wait
// until the active MGR, which restarts to load it, answers again. A disabled
// module is ceph.ErrInvalidRequest, and so is a directory that already has a
// schedule, which the lease would not own.
func (fs *Filesystem) TemporarySnapshotSchedule(ctx context.Context, schedule SnapshotSchedule) (*SnapshotScheduleLease, error) {
	if err := validateSnapshotSchedule(schedule); err != nil {
		return nil, err
	}
	modules, err := fs.cluster.MGRModules(ctx)
	if err != nil {
		return nil, err
	}
	enabled := false
	for _, module := range modules {
		if module.Name == "snap_schedule" {
			enabled = module.Enabled || module.AlwaysOn
		}
	}
	if !enabled {
		return nil, invalid(errors.New("the MGR snap_schedule module is not enabled: enable it with TemporaryMGRModule"))
	}
	if strings.HasSuffix(schedule.Repeat, "m") {
		data, err := fs.cluster.Ceph(ctx, "config", "get", "mgr", "mgr/snap_schedule/allow_m_granularity")
		if err != nil {
			return nil, fmt.Errorf("read mgr/snap_schedule/allow_m_granularity: %w", err)
		}
		if strings.TrimSpace(string(data)) != "true" {
			return nil, invalid(fmt.Errorf("snapshot schedule repeat %s is in minutes, which the module refuses while mgr/snap_schedule/allow_m_granularity is %s", schedule.Repeat, strings.TrimSpace(string(data))))
		}
	}
	existing, err := fs.snapshotSchedules(ctx, schedule.Path)
	if err != nil {
		return nil, err
	}
	if len(existing) > 0 {
		return nil, invalid(fmt.Errorf("directory %s already has snapshot schedules %v", schedule.Path, existing))
	}
	args := []string{"fs", "snap-schedule", "add", schedule.Path, schedule.Repeat}
	if !schedule.Start.IsZero() {
		args = append(args, schedule.Start.UTC().Format("2006-01-02T15:04:05"))
	}
	lease := &SnapshotScheduleLease{Path: schedule.Path, Repeat: schedule.Repeat, Retention: maps.Clone(schedule.Retention), filesystem: fs}
	if _, err := fs.cluster.Ceph(ctx, append(args, "--fs", fs.config.Name)...); err != nil {
		// A lost reply may have added it; Restore removes it either way.
		return lease, fmt.Errorf("add the snapshot schedule of %s: %w", schedule.Path, err)
	}
	for _, period := range slices.Sorted(maps.Keys(schedule.Retention)) {
		if _, err := fs.cluster.Ceph(ctx, "fs", "snap-schedule", "retention", "add", schedule.Path, period,
			strconv.Itoa(schedule.Retention[period]), "--fs", fs.config.Name); err != nil {
			return lease, fmt.Errorf("add the snapshot retention %s of %s: %w", period, schedule.Path, err)
		}
	}
	status, err := lease.Status(ctx)
	if err != nil {
		return lease, err
	}
	if status.Schedule != schedule.Repeat || !status.Active || !maps.Equal(status.Retention, nonNil(schedule.Retention)) {
		return lease, fmt.Errorf("the module reports schedule %q active=%t retention %v instead of the request", status.Schedule, status.Active, status.Retention)
	}
	return lease, nil
}

func nonNil(m map[string]int) map[string]int {
	if m == nil {
		return map[string]int{}
	}
	return m
}

// snapshotSchedules lists the repeats of the schedules of one directory.
func (fs *Filesystem) snapshotSchedules(ctx context.Context, dir string) ([]string, error) {
	data, err := fs.cluster.Ceph(ctx, "fs", "snap-schedule", "list", dir, "--fs", fs.config.Name, "--format", "json")
	if err != nil {
		return nil, fmt.Errorf("list the snapshot schedules of %s: %w", dir, err)
	}
	var list struct {
		Path     string   `json:"path"`
		Schedule []string `json:"schedule"`
	}
	// A directory without schedules prints {} or, after a removal, nothing.
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, nil
	}
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("decode the snapshot schedules of %s: %w", dir, err)
	}
	return list.Schedule, nil
}

// parseSnapshotScheduleStatus reads "fs snap-schedule status" for one
// directory: a list with one entry per schedule.
func parseSnapshotScheduleStatus(data []byte, dir, repeat string) (SnapshotScheduleStatus, error) {
	var entries []json.RawMessage
	if err := json.Unmarshal(data, &entries); err != nil {
		return SnapshotScheduleStatus{}, fmt.Errorf("decode the snapshot schedule status: %w", err)
	}
	for _, raw := range entries {
		var entry struct {
			Path      string         `json:"path"`
			Schedule  string         `json:"schedule"`
			Retention map[string]int `json:"retention"`
			First     *string        `json:"first"`
			Last      *string        `json:"last"`
			Created   *int           `json:"created_count"`
			Pruned    *int           `json:"pruned_count"`
			Active    *bool          `json:"active"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return SnapshotScheduleStatus{}, fmt.Errorf("decode a snapshot schedule: %w", err)
		}
		if entry.Path != dir || entry.Schedule != repeat {
			continue
		}
		if entry.Created == nil || entry.Pruned == nil || entry.Active == nil {
			return SnapshotScheduleStatus{}, errors.New("the snapshot schedule status lacks its counts or active flag")
		}
		status := SnapshotScheduleStatus{Path: entry.Path, Schedule: entry.Schedule, Active: *entry.Active, Retention: nonNil(entry.Retention),
			Created: *entry.Created, Pruned: *entry.Pruned, Raw: raw}
		for _, field := range []struct {
			text *string
			into *time.Time
		}{{entry.First, &status.First}, {entry.Last, &status.Last}} {
			if field.text == nil {
				continue
			}
			parsed, err := time.Parse("2006-01-02T15:04:05", *field.text)
			if err != nil {
				return SnapshotScheduleStatus{}, fmt.Errorf("snapshot schedule time %q: %w", *field.text, err)
			}
			*field.into = parsed
		}
		return status, nil
	}
	return SnapshotScheduleStatus{}, fmt.Errorf("no snapshot schedule %s of %s", repeat, dir)
}

// Status reads the schedule's state from the snap_schedule module.
func (l *SnapshotScheduleLease) Status(ctx context.Context) (SnapshotScheduleStatus, error) {
	data, err := l.filesystem.cluster.Ceph(ctx, "fs", "snap-schedule", "status", l.Path, "--fs", l.filesystem.config.Name, "--format", "json")
	if err != nil {
		return SnapshotScheduleStatus{}, fmt.Errorf("read the snapshot schedule of %s: %w", l.Path, err)
	}
	return parseSnapshotScheduleStatus(data, l.Path, l.Repeat)
}

// Snapshots lists the snapshots in the directory's .snap that the module
// took, named "scheduled-…", oldest first, through libcephfs in the control
// container.
func (l *SnapshotScheduleLease) Snapshots(ctx context.Context) ([]string, error) {
	control, err := l.filesystem.cluster.ControlContainerContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("select the CephFS CLI container: %w", err)
	}
	data, err := servicehook.Command(ctx, control, "python3", "-c", cephFSSnapshotListScript, l.filesystem.config.Name, l.Path)
	if err != nil {
		return nil, fmt.Errorf("list the snapshots of %s: %w", l.Path, err)
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("decode the snapshots of %s: %w", l.Path, err)
	}
	var scheduled []string
	for _, name := range names {
		if strings.HasPrefix(name, "scheduled-") {
			scheduled = append(scheduled, name)
		}
	}
	slices.Sort(scheduled)
	return scheduled, nil
}

const cephFSSnapshotListScript = `import cephfs, json, os, sys, threading
filesystem, directory = sys.argv[1:]
deadline = threading.Timer(30, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=filesystem.encode())
try:
    snap = directory.rstrip("/") + "/.snap"
    handle = fs.opendir(snap.encode())
    names = []
    entry = fs.readdir(handle)
    while entry:
        name = entry.d_name.decode()
        if name not in (".", ".."):
            names.append(name)
        entry = fs.readdir(handle)
    fs.closedir(handle)
    print(json.dumps(names))
finally:
    fs.unmount()
    fs.shutdown()
`

// Restore removes the schedule and its retention from the directory; the
// snapshots it took stay. It is retryable, and a schedule that is already
// gone needs no removal.
func (l *SnapshotScheduleLease) Restore(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.restored {
		return nil
	}
	schedules, err := l.filesystem.snapshotSchedules(ctx, l.Path)
	if err != nil {
		return err
	}
	if slices.Contains(schedules, l.Repeat) {
		// The retention belongs to the directory, not to the schedule: remove
		// what the lease added first.
		for _, period := range slices.Sorted(maps.Keys(l.Retention)) {
			if _, err := l.filesystem.cluster.Ceph(ctx, "fs", "snap-schedule", "retention", "remove", l.Path, period,
				strconv.Itoa(l.Retention[period]), "--fs", l.filesystem.config.Name); err != nil {
				return fmt.Errorf("remove the snapshot retention %s of %s: %w", period, l.Path, err)
			}
		}
		if _, err := l.filesystem.cluster.Ceph(ctx, "fs", "snap-schedule", "remove", l.Path, l.Repeat, "--fs", l.filesystem.config.Name); err != nil {
			return fmt.Errorf("remove the snapshot schedule of %s: %w", l.Path, err)
		}
	}
	if schedules, err = l.filesystem.snapshotSchedules(ctx, l.Path); err != nil {
		return err
	}
	if slices.Contains(schedules, l.Repeat) {
		return fmt.Errorf("the snapshot schedule %s of %s is still listed", l.Repeat, l.Path)
	}
	l.restored = true
	return nil
}
