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

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// snapshotSchedulePeriod is a schedule's repeat or a retention count's
// period, as the MGR snap_schedule module reads it: minutes, hours, days,
// weeks, months or years.
var snapshotSchedulePeriod = regexp.MustCompile(`^[1-9][0-9]*[mhdwMy]$`)

// SnapshotSchedule configures AddSnapshotSchedule.
type SnapshotSchedule struct {
	// Path is the directory to snapshot, an absolute path in the filesystem,
	// such as "/" or "/data". It must exist.
	Path string
	// Repeat is how often a snapshot is taken: a positive number and one of
	// m (minutes), h, d, w, M (months) or y, such as "6h" or "1m". The module
	// takes minutes only while mgr/snap_schedule/allow_m_granularity is true,
	// which override.SetConfig can set for a test.
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
	// Start is the schedule's first time slot and Added the time the module
	// added it, both in UTC; with Path and Schedule they identify one
	// schedule, since a directory can have several with the same repeat.
	Start, Added time.Time
	// Retention is the module's retention specification. The module keeps it
	// per directory: every schedule of the directory shares it, and it stays
	// after the directory's last schedule is removed.
	Retention map[string]int
	// Created and Pruned count the snapshots that the module took and
	// removed; First and Last are its first and last snapshot times, zero
	// before the first.
	Created, Pruned int
	First, Last     time.Time
	// Raw is the module's JSON for the schedule.
	Raw json.RawMessage

	// start and added are the module's own texts, compared exactly.
	start, added string
}

// SnapshotScheduleLease owns one snapshot schedule of a directory and the
// retention rules that it added. Restore removes them; the snapshots stay.
// Retain a non-nil lease returned with an error: the schedule may have been
// added.
type SnapshotScheduleLease struct {
	// Path, Repeat, Start and Retention are the requested schedule.
	Path, Repeat string
	Start        time.Time
	Retention    map[string]int

	filesystem *Filesystem
	mu         sync.Mutex
	// start and added identify the native schedule once the lease read it
	// back after adding it; both are empty while the add is unconfirmed.
	start, added string
	// rules are the retention rules that the lease sent, confirmed or not;
	// Restore removes each one that the module still lists with that count.
	rules map[string]int
	// cleared are the rules whose removal Restore confirmed.
	cleared  map[string]bool
	restored bool
}

func validateSnapshotSchedule(schedule SnapshotSchedule) error {
	switch {
	case !strings.HasPrefix(schedule.Path, "/") || path.Clean(schedule.Path) != schedule.Path:
		return servicehook.Invalid(fmt.Errorf("snapshot schedule path %q must be a clean absolute path", schedule.Path))
	case !snapshotSchedulePeriod.MatchString(schedule.Repeat):
		return servicehook.Invalid(fmt.Errorf("snapshot schedule repeat %q must be a number and one of m, h, d, w, M or y", schedule.Repeat))
	}
	for period, count := range schedule.Retention {
		if !strings.Contains("mhdwMyn", period) || len(period) != 1 || count <= 0 {
			return servicehook.Invalid(fmt.Errorf("snapshot retention %s=%d: want a positive count of m, h, d, w, M, y or n", period, count))
		}
	}
	return nil
}

const snapshotScheduleTime = "2006-01-02T15:04:05"

// AddSnapshotSchedule adds a snapshot schedule, and its retention, to a
// directory of the filesystem through the MGR snap_schedule module, which
// takes "scheduled-…" snapshots in the directory's .snap. It never enables the
// module: enable it with override.SetMGRModule first, and wait
// until the active MGR, which restarts to load it, answers again. A disabled
// module is ceph.ErrInvalidRequest, and so is a directory that already has a
// schedule or another lease. The module keeps retention per directory, and a
// directory keeps retention rules after its last schedule is removed: rules
// that the new schedule inherits are refused with ceph.ErrInvalidRequest
// after the schedule was added, with a lease that removes it.
func (fs *Filesystem) AddSnapshotSchedule(ctx context.Context, schedule SnapshotSchedule) (*SnapshotScheduleLease, error) {
	if err := validateSnapshotSchedule(schedule); err != nil {
		return nil, err
	}
	observer, err := ceph.Observe(fs.cluster)
	if err != nil {
		return nil, err
	}
	modules, err := observer.MGRModules(ctx)
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
		return nil, servicehook.Invalid(errors.New("the MGR snap_schedule module is not enabled: enable it with override.SetMGRModule"))
	}
	if strings.HasSuffix(schedule.Repeat, "m") {
		data, err := fs.cluster.Ceph(ctx, "config", "get", "mgr", "mgr/snap_schedule/allow_m_granularity")
		if err != nil {
			return nil, fmt.Errorf("read mgr/snap_schedule/allow_m_granularity: %w", err)
		}
		if strings.TrimSpace(string(data)) != "true" {
			return nil, servicehook.Invalid(fmt.Errorf("snapshot schedule repeat %s is in minutes, which the module refuses while mgr/snap_schedule/allow_m_granularity is %s", schedule.Repeat, strings.TrimSpace(string(data))))
		}
	}
	lease := &SnapshotScheduleLease{Path: schedule.Path, Repeat: schedule.Repeat, Start: schedule.Start.UTC(), Retention: maps.Clone(schedule.Retention),
		filesystem: fs, rules: map[string]int{}, cleared: map[string]bool{}}
	// Two leases of one directory would each take the other's schedule for
	// their own.
	fs.snapshotScheduleMu.Lock()
	if fs.scheduledPaths[schedule.Path] != nil {
		fs.snapshotScheduleMu.Unlock()
		return nil, servicehook.Invalid(fmt.Errorf("directory %s already has a snapshot schedule lease", schedule.Path))
	}
	if fs.scheduledPaths == nil {
		fs.scheduledPaths = map[string]*SnapshotScheduleLease{}
	}
	fs.scheduledPaths[schedule.Path] = lease
	fs.snapshotScheduleMu.Unlock()
	existing, err := fs.volumes().snapshotScheduleStatuses(ctx, schedule.Path)
	if err == nil && len(existing) > 0 {
		err = servicehook.Invalid(fmt.Errorf("directory %s already has %d snapshot schedules", schedule.Path, len(existing)))
	}
	if err != nil {
		lease.release()
		return nil, err
	}
	args := []string{"fs", "snap-schedule", "add", schedule.Path, schedule.Repeat}
	if !schedule.Start.IsZero() {
		args = append(args, lease.Start.Format(snapshotScheduleTime))
	}
	if _, err := fs.cluster.Ceph(ctx, append(args, "--fs", fs.name)...); err != nil {
		// A lost reply may have added it; Restore reads the directory back.
		return lease, fmt.Errorf("add the snapshot schedule of %s: %w", schedule.Path, err)
	}
	added, err := lease.find(ctx)
	if err != nil {
		return lease, err
	}
	if added == nil {
		return lease, fmt.Errorf("the module does not list the added snapshot schedule %s of %s", schedule.Repeat, schedule.Path)
	}
	lease.start, lease.added = added.start, added.added
	if len(added.Retention) > 0 {
		return lease, servicehook.Invalid(fmt.Errorf("directory %s keeps retention rules %v from earlier schedules, which the new schedule inherits: remove them first", schedule.Path, added.Retention))
	}
	for _, period := range slices.Sorted(maps.Keys(schedule.Retention)) {
		lease.rules[period] = schedule.Retention[period]
		if _, err := fs.cluster.Ceph(ctx, "fs", "snap-schedule", "retention", "add", schedule.Path, period,
			strconv.Itoa(schedule.Retention[period]), "--fs", fs.name); err != nil {
			return lease, fmt.Errorf("add the snapshot retention %s of %s: %w", period, schedule.Path, err)
		}
	}
	status, err := lease.Status(ctx)
	if err != nil {
		return lease, err
	}
	if !status.Active || !maps.Equal(status.Retention, nonNil(schedule.Retention)) {
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

// release lets another lease schedule the directory.
func (l *SnapshotScheduleLease) release() {
	l.filesystem.snapshotScheduleMu.Lock()
	defer l.filesystem.snapshotScheduleMu.Unlock()
	if l.filesystem.scheduledPaths[l.Path] == l {
		delete(l.filesystem.scheduledPaths, l.Path)
	}
}

// snapshotScheduleStatuses reads every schedule of one directory.
func (v volumeCommands) snapshotScheduleStatuses(ctx context.Context, dir string) ([]SnapshotScheduleStatus, error) {
	data, err := v.cluster.Ceph(ctx, "fs", "snap-schedule", "status", dir, "--fs", v.name, "--format", "json")
	if err != nil {
		return nil, fmt.Errorf("read the snapshot schedules of %s: %w", dir, err)
	}
	return parseSnapshotScheduleStatuses(data, dir)
}

// parseSnapshotScheduleStatuses reads "fs snap-schedule status" for one
// directory: a list with one entry per schedule, empty without schedules.
func parseSnapshotScheduleStatuses(data []byte, dir string) ([]SnapshotScheduleStatus, error) {
	var entries []json.RawMessage
	if len(strings.TrimSpace(string(data))) > 0 {
		if err := json.Unmarshal(data, &entries); err != nil {
			return nil, fmt.Errorf("decode the snapshot schedule status: %w", err)
		}
	}
	var statuses []SnapshotScheduleStatus
	for _, raw := range entries {
		var entry struct {
			Path      string         `json:"path"`
			Schedule  string         `json:"schedule"`
			Retention map[string]int `json:"retention"`
			Start     string         `json:"start"`
			Added     string         `json:"created"`
			First     *string        `json:"first"`
			Last      *string        `json:"last"`
			Created   *int           `json:"created_count"`
			Pruned    *int           `json:"pruned_count"`
			Active    *bool          `json:"active"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, fmt.Errorf("decode a snapshot schedule: %w", err)
		}
		if entry.Path != dir {
			continue
		}
		if entry.Created == nil || entry.Pruned == nil || entry.Active == nil || entry.Start == "" || entry.Added == "" {
			return nil, errors.New("the snapshot schedule status lacks its counts, active flag, start or creation time")
		}
		status := SnapshotScheduleStatus{Path: entry.Path, Schedule: entry.Schedule, Active: *entry.Active, Retention: nonNil(entry.Retention),
			Created: *entry.Created, Pruned: *entry.Pruned, Raw: raw, start: entry.Start, added: entry.Added}
		for _, field := range []struct {
			text *string
			into *time.Time
		}{{&entry.Start, &status.Start}, {&entry.Added, &status.Added}, {entry.First, &status.First}, {entry.Last, &status.Last}} {
			if field.text == nil {
				continue
			}
			parsed, err := time.Parse(snapshotScheduleTime, *field.text)
			if err != nil {
				return nil, fmt.Errorf("snapshot schedule time %q: %w", *field.text, err)
			}
			*field.into = parsed
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

// find reads the directory's schedules and returns the lease's: the one with
// its native start and creation time once bound. Before that, it is the only
// schedule with the requested repeat and start, a requested start being
// compared only when one was given. A schedule with the lease's repeat that is
// not the lease's is ceph.ErrInvalidRequest; none is nil.
func (l *SnapshotScheduleLease) find(ctx context.Context) (*SnapshotScheduleStatus, error) {
	statuses, err := l.filesystem.volumes().snapshotScheduleStatuses(ctx, l.Path)
	if err != nil {
		return nil, err
	}
	return l.match(statuses)
}

// match returns the lease's schedule among the directory's, as find does.
func (l *SnapshotScheduleLease) match(statuses []SnapshotScheduleStatus) (*SnapshotScheduleStatus, error) {
	var candidates []SnapshotScheduleStatus
	for _, status := range statuses {
		if status.Schedule != l.Repeat {
			continue
		}
		if l.added != "" {
			if status.start == l.start && status.added == l.added {
				return &status, nil
			}
		} else if l.Start.IsZero() || status.Start.Equal(l.Start.Truncate(time.Second)) {
			candidates = append(candidates, status)
			continue
		}
		return nil, servicehook.Invalid(fmt.Errorf("the snapshot schedule %s of %s starting %s, added %s, is not the lease's", status.Schedule, l.Path, status.start, status.added))
	}
	switch len(candidates) {
	case 0:
		return nil, nil
	case 1:
		return &candidates[0], nil
	}
	return nil, servicehook.Invalid(fmt.Errorf("directory %s has %d snapshot schedules %s that could be the lease's", l.Path, len(candidates), l.Repeat))
}

// Status reads the schedule's state from the snap_schedule module.
func (l *SnapshotScheduleLease) Status(ctx context.Context) (SnapshotScheduleStatus, error) {
	status, err := l.find(ctx)
	if err != nil {
		return SnapshotScheduleStatus{}, err
	}
	if status == nil {
		return SnapshotScheduleStatus{}, fmt.Errorf("no snapshot schedule %s of %s", l.Repeat, l.Path)
	}
	return *status, nil
}

// Snapshots lists the snapshots in the directory's .snap that the module
// took, named "scheduled-…", oldest first, through libcephfs in the control
// container.
func (l *SnapshotScheduleLease) Snapshots(ctx context.Context) ([]string, error) {
	control, err := l.filesystem.cluster.ClientContainer(ctx)
	if err != nil {
		return nil, fmt.Errorf("select the CephFS CLI container: %w", err)
	}
	data, err := servicehook.Command(ctx, control, "python3", "-c", cephFSSnapshotListScript, l.filesystem.name, l.Path)
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

// Restore removes the retention rules that the lease added, then its
// schedule; the snapshots stay. The module keeps retention per directory:
// every schedule of the directory shares it, and it stays after the last
// schedule is removed. Restore reads the directory back on every call, so a
// retry continues where an earlier call stopped, and it completes only once
// both the lease's rules and its schedule are confirmed gone:
//   - a rule that the directory no longer lists, or that the module reports
//     as not set when no schedule shows the retention, is done;
//   - while another schedule of the directory, of any repeat, would lose a
//     rule of the lease, Restore refuses with ceph.ErrInvalidRequest without
//     a change, and so it does for a rule whose count changed or a schedule
//     with the lease's repeat that is not the lease's. The lease stays
//     retryable, for example after the other schedule's owner removed it.
func (l *SnapshotScheduleLease) Restore(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.restored {
		return nil
	}
	statuses, err := l.filesystem.volumes().snapshotScheduleStatuses(ctx, l.Path)
	if err != nil {
		return err
	}
	owned, err := l.match(statuses)
	if err != nil {
		return err
	}
	if err := l.removeRules(ctx, statuses, owned); err != nil {
		return err
	}
	if owned != nil {
		// The start selects this schedule among those with its repeat.
		if _, err := l.filesystem.cluster.Ceph(ctx, "fs", "snap-schedule", "remove", l.Path, l.Repeat, owned.start,
			"--fs", l.filesystem.name); err != nil {
			return fmt.Errorf("remove the snapshot schedule of %s: %w", l.Path, err)
		}
		// Bind an unconfirmed add, so the read back looks for this schedule.
		l.start, l.added = owned.start, owned.added
		if owned, err = l.find(ctx); err != nil {
			return err
		}
		if owned != nil {
			return fmt.Errorf("the snapshot schedule %s of %s is still listed", l.Repeat, l.Path)
		}
	}
	l.restored = true
	l.release()
	return nil
}

// removeRules removes the lease's retention rules that remain. statuses are
// the directory's schedules and owned is the lease's among them, or nil.
func (l *SnapshotScheduleLease) removeRules(ctx context.Context, statuses []SnapshotScheduleStatus, owned *SnapshotScheduleStatus) error {
	var pending []string
	for _, period := range slices.Sorted(maps.Keys(l.rules)) {
		if !l.cleared[period] {
			pending = append(pending, period)
		}
	}
	if len(statuses) > 0 {
		// Every schedule shows the directory's retention.
		retention := statuses[0].Retention
		var remaining []string
		for _, period := range pending {
			switch current, ok := retention[period]; {
			case !ok:
				l.cleared[period] = true
			case current != l.rules[period]:
				return servicehook.Invalid(fmt.Errorf("the snapshot retention %s of %s is %d, not the lease's %d", period, l.Path, current, l.rules[period]))
			default:
				remaining = append(remaining, period)
			}
		}
		if len(remaining) > 0 {
			for _, status := range statuses {
				if owned == nil || status.start != owned.start || status.added != owned.added || status.Schedule != owned.Schedule {
					return servicehook.Invalid(fmt.Errorf("removing the lease's snapshot retention %v of %s would change the retention of the schedule %s starting %s, which the lease did not add: remove that schedule first",
						remaining, l.Path, status.Schedule, status.start))
				}
			}
		}
		pending = remaining
	}
	for _, period := range pending {
		_, err := l.filesystem.cluster.Ceph(ctx, "fs", "snap-schedule", "retention", "remove", l.Path, period,
			strconv.Itoa(l.rules[period]), "--fs", l.filesystem.name)
		if err != nil && (len(statuses) > 0 || !snapshotRetentionAbsent(err)) {
			return fmt.Errorf("remove the snapshot retention %s of %s: %w", period, l.Path, err)
		}
		l.cleared[period] = true
	}
	return nil
}

// snapshotRetentionAbsent reports a retention removal that the module refused
// because the rule is not set: with no schedule to show the directory's
// retention, that is the only observation of its absence. The module says
// "Retention for <period>: <count> was not set for <path>" while it keeps the
// directory's retention, and "No schedule found for <path>" when it keeps
// nothing for the directory, which then has no retention either.
func snapshotRetentionAbsent(err error) bool {
	var failure *ceph.NativeFailure
	return errors.As(err, &failure) && failure.Errno == "ENOENT" &&
		(strings.Contains(failure.Reason, " was not set for ") || strings.Contains(failure.Reason, "No schedule found for "))
}
