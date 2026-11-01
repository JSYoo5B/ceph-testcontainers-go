package snapschedule

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
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// period is a schedule's repeat or a retention count's
// period, as the MGR snap_schedule module reads it: minutes, hours, days,
// weeks, months or years.
var period = regexp.MustCompile(`^[1-9][0-9]*[mhdwMy]$`)

// Config configures Add.
type Config struct {
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

// Status is what the snap_schedule module reports for a schedule: native
// state, not the requested Config.
type Status struct {
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

// Lease owns one snapshot schedule of a directory and the retention rules
// that it added. Restore removes them; the snapshots stay. Retain a non-nil
// lease returned with an error: the schedule may have been added.
type Lease struct {
	// Path, Repeat, Start and Retention are the requested schedule.
	Path, Repeat string
	Start        time.Time
	Retention    map[string]int

	filesystem *fsbase.Core
	// leases is the cluster's lease set that reserved the directory.
	leases *leaseSet
	mu     sync.Mutex
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

func validate(schedule Config) error {
	switch {
	case !strings.HasPrefix(schedule.Path, "/") || path.Clean(schedule.Path) != schedule.Path:
		return servicehook.Invalid(fmt.Errorf("snapshot schedule path %q must be a clean absolute path", schedule.Path))
	case !period.MatchString(schedule.Repeat):
		return servicehook.Invalid(fmt.Errorf("snapshot schedule repeat %q must be a number and one of m, h, d, w, M or y", schedule.Repeat))
	}
	for period, count := range schedule.Retention {
		if !strings.Contains("mhdwMyn", period) || len(period) != 1 || count <= 0 {
			return servicehook.Invalid(fmt.Errorf("snapshot retention %s=%d: want a positive count of m, h, d, w, M, y or n", period, count))
		}
	}
	return nil
}

const nativeTime = "2006-01-02T15:04:05"

// Add adds a snapshot schedule, and its retention, to a directory of fs
// through the MGR snap_schedule module, which
// takes "scheduled-…" snapshots in the directory's .snap. It never enables the
// module: enable it with override.SetMGRModule first, and wait
// until the active MGR, which restarts to load it, answers again. A disabled
// module is ceph.ErrInvalidRequest, and so is a directory that already has a
// schedule or another lease. The module keeps retention per directory, and a
// directory keeps retention rules after its last schedule is removed: rules
// that the new schedule inherits are refused with ceph.ErrInvalidRequest
// after the schedule was added, with a lease that removes it.
//
// A non-nil lease returned with an error must be restored: after an add whose
// reply was lost, Restore takes the only schedule with the requested repeat and
// start as the lease's.
//
//purpose:construction
func Add(ctx context.Context, fs *cephfs.Filesystem, schedule Config) (*Lease, error) {
	var core *fsbase.Core
	if fs != nil {
		core = fsbase.Of(fs)
	}
	return add(ctx, core, schedule)
}

func add(ctx context.Context, fs *fsbase.Core, schedule Config) (*Lease, error) {
	if err := validate(schedule); err != nil {
		return nil, err
	}
	observer, err := ceph.Observe(fs.Cluster())
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
		data, err := fs.Cluster().Ceph(ctx, "config", "get", "mgr", "mgr/snap_schedule/allow_m_granularity")
		if err != nil {
			return nil, fmt.Errorf("read mgr/snap_schedule/allow_m_granularity: %w", err)
		}
		if strings.TrimSpace(string(data)) != "true" {
			return nil, servicehook.Invalid(fmt.Errorf("snapshot schedule repeat %s is in minutes, which the module refuses while mgr/snap_schedule/allow_m_granularity is %s", schedule.Repeat, strings.TrimSpace(string(data))))
		}
	}
	lease := &Lease{Path: schedule.Path, Repeat: schedule.Repeat, Start: schedule.Start.UTC(), Retention: maps.Clone(schedule.Retention),
		filesystem: fs, leases: leasesOf(fs.Cluster()), rules: map[string]int{}, cleared: map[string]bool{}}
	// Two leases of one directory would each take the other's schedule for
	// their own.
	if !lease.leases.reserve(lease) {
		return nil, servicehook.Invalid(fmt.Errorf("directory %s already has a snapshot schedule lease", schedule.Path))
	}
	existing, err := statuses(ctx, fs.Volumes(), schedule.Path)
	if err == nil && len(existing) > 0 {
		err = servicehook.Invalid(fmt.Errorf("directory %s already has %d snapshot schedules", schedule.Path, len(existing)))
	}
	if err != nil {
		lease.release()
		return nil, err
	}
	args := []string{"fs", "snap-schedule", "add", schedule.Path, schedule.Repeat}
	if !schedule.Start.IsZero() {
		args = append(args, lease.Start.Format(nativeTime))
	}
	if _, err := fs.Cluster().Ceph(ctx, append(args, "--fs", fs.Name())...); err != nil {
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
		if _, err := fs.Cluster().Ceph(ctx, "fs", "snap-schedule", "retention", "add", schedule.Path, period,
			strconv.Itoa(schedule.Retention[period]), "--fs", fs.Name()); err != nil {
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
func (l *Lease) release() { l.leases.release(l) }

// leaseKind is the cluster extension that holds the lease set.
const leaseKind = "cephfs/snapschedule.leases"

// leaseKey is one directory of one filesystem; the core identifies the
// filesystem, so a filesystem created later under the same name has other
// keys.
type leaseKey struct {
	filesystem *fsbase.Core
	path       string
}

// leaseSet is the set of directories of the cluster's filesystems that hold
// a snapshot schedule lease not restored yet. Two leases of one directory
// would each take the other's schedule for their own, so a directory holds at
// most one. Its own mutex guards the set; it is a leaf lock, taken for the map
// update alone and never while a native command runs. A removed filesystem's
// entries stay until their leases are restored; no other lease can share
// their keys. It lives as long as its cluster.
type leaseSet struct {
	mu    sync.Mutex
	paths map[leaseKey]*Lease
}

// leasesOf returns the lease set of cluster c, creating it on first use.
func leasesOf(c *ceph.Container) *leaseSet {
	return servicehook.Extension(c, leaseKind, func() any {
		return &leaseSet{paths: map[leaseKey]*Lease{}}
	}).(*leaseSet)
}

// reserve records lease for its directory and reports false when another
// lease already holds it.
func (s *leaseSet) reserve(lease *Lease) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := leaseKey{lease.filesystem, lease.Path}
	if s.paths[key] != nil {
		return false
	}
	s.paths[key] = lease
	return true
}

// release frees the directory of lease, unless another lease holds it.
func (s *leaseSet) release(lease *Lease) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := leaseKey{lease.filesystem, lease.Path}
	if s.paths[key] == lease {
		delete(s.paths, key)
	}
}

// statuses reads every schedule of one directory.
func statuses(ctx context.Context, v fsbase.VolumeCommands, dir string) ([]Status, error) {
	data, err := v.Cluster().Ceph(ctx, "fs", "snap-schedule", "status", dir, "--fs", v.Name(), "--format", "json")
	if err != nil {
		return nil, fmt.Errorf("read the snapshot schedules of %s: %w", dir, err)
	}
	return parseStatuses(data, dir)
}

// parseStatuses reads "fs snap-schedule status" for one directory: a list
// with one entry per schedule, empty without schedules.
func parseStatuses(data []byte, dir string) ([]Status, error) {
	var entries []json.RawMessage
	if len(strings.TrimSpace(string(data))) > 0 {
		if err := json.Unmarshal(data, &entries); err != nil {
			return nil, fmt.Errorf("decode the snapshot schedule status: %w", err)
		}
	}
	var result []Status
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
		status := Status{Path: entry.Path, Schedule: entry.Schedule, Active: *entry.Active, Retention: nonNil(entry.Retention),
			Created: *entry.Created, Pruned: *entry.Pruned, Raw: raw, start: entry.Start, added: entry.Added}
		for _, field := range []struct {
			text *string
			into *time.Time
		}{{&entry.Start, &status.Start}, {&entry.Added, &status.Added}, {entry.First, &status.First}, {entry.Last, &status.Last}} {
			if field.text == nil {
				continue
			}
			parsed, err := time.Parse(nativeTime, *field.text)
			if err != nil {
				return nil, fmt.Errorf("snapshot schedule time %q: %w", *field.text, err)
			}
			*field.into = parsed
		}
		result = append(result, status)
	}
	return result, nil
}

// find reads the directory's schedules and returns the lease's: the one with
// its native start and creation time once bound. Before that, it is the only
// schedule with the requested repeat and start, a requested start being
// compared only when one was given. A schedule with the lease's repeat that is
// not the lease's is ceph.ErrInvalidRequest; none is nil.
func (l *Lease) find(ctx context.Context) (*Status, error) {
	listed, err := statuses(ctx, l.filesystem.Volumes(), l.Path)
	if err != nil {
		return nil, err
	}
	return l.match(listed)
}

// match returns the lease's schedule among the directory's, as find does.
func (l *Lease) match(statuses []Status) (*Status, error) {
	var candidates []Status
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
//
//purpose:observation
func (l *Lease) Status(ctx context.Context) (Status, error) {
	status, err := l.find(ctx)
	if err != nil {
		return Status{}, err
	}
	if status == nil {
		return Status{}, fmt.Errorf("no snapshot schedule %s of %s", l.Repeat, l.Path)
	}
	return *status, nil
}

// Snapshots lists the snapshots in the directory's .snap that the module
// took, named "scheduled-…", oldest first, through libcephfs in the client
// container.
//
//purpose:observation
func (l *Lease) Snapshots(ctx context.Context) ([]string, error) {
	control, err := l.filesystem.Cluster().ClientContainer(ctx)
	if err != nil {
		return nil, fmt.Errorf("select the CephFS CLI container: %w", err)
	}
	data, err := servicehook.Command(ctx, control, "python3", "-c", snapshotListScript, l.filesystem.Name(), l.Path)
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

const snapshotListScript = `import cephfs, json, os, sys, threading
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
//
//purpose:construction,lifetime
func (l *Lease) Restore(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.restored {
		return nil
	}
	listed, err := statuses(ctx, l.filesystem.Volumes(), l.Path)
	if err != nil {
		return err
	}
	owned, err := l.match(listed)
	if err != nil {
		return err
	}
	if err := l.removeRules(ctx, listed, owned); err != nil {
		return err
	}
	if owned != nil {
		// The start selects this schedule among those with its repeat.
		if _, err := l.filesystem.Cluster().Ceph(ctx, "fs", "snap-schedule", "remove", l.Path, l.Repeat, owned.start,
			"--fs", l.filesystem.Name()); err != nil {
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
func (l *Lease) removeRules(ctx context.Context, statuses []Status, owned *Status) error {
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
		_, err := l.filesystem.Cluster().Ceph(ctx, "fs", "snap-schedule", "retention", "remove", l.Path, period,
			strconv.Itoa(l.rules[period]), "--fs", l.filesystem.Name())
		if err != nil && (len(statuses) > 0 || !retentionAbsent(err)) {
			return fmt.Errorf("remove the snapshot retention %s of %s: %w", period, l.Path, err)
		}
		l.cleared[period] = true
	}
	return nil
}

// retentionAbsent reports a retention removal that the module refused
// because the rule is not set: with no schedule to show the directory's
// retention, that is the only observation of its absence. The module says
// "Retention for <period>: <count> was not set for <path>" while it keeps the
// directory's retention, and "No schedule found for <path>" when it keeps
// nothing for the directory, which then has no retention either.
func retentionAbsent(err error) bool {
	var failure *ceph.NativeFailure
	return errors.As(err, &failure) && failure.Errno == "ENOENT" &&
		(strings.Contains(failure.Reason, " was not set for ") || strings.Contains(failure.Reason, "No schedule found for "))
}
