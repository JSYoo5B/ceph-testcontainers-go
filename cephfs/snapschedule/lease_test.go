package snapschedule

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

type fakeSchedule struct {
	path, repeat, start, added string
}

// fakeSnapSchedule models the MGR snap_schedule module as the native probes
// showed it: schedules are identified by path, repeat and start, several may
// share a repeat, retention is kept per directory, shared by its schedules
// and kept after the last one is removed, and removing a rule that is not
// set fails with ENOENT. A directory that never had a schedule has no
// retention, and its retention commands fail with "No schedule found".
type fakeSnapSchedule struct {
	testcontainers.Container
	mu        sync.Mutex
	schedules []fakeSchedule
	retention map[string]map[string]int
	// known are the directories that had a schedule.
	known map[string]bool
	clock time.Time
	// lost applies the command whose words start with the key and then
	// fails its reply; refused fails it without applying. Each fires once.
	lost, refused map[string]bool
	// fsID is the FSMap ID of "tc"; unregistered drops it from the registry.
	fsID         int
	unregistered bool
	// commands counts the commands the fake answered.
	commands int
	// progress replaces status fields of the schedule keyed by its path and
	// repeat, as the module reports its runs.
	progress map[string]map[string]any
}

func newFakeSnapSchedule() *fakeSnapSchedule {
	return &fakeSnapSchedule{fsID: 41, retention: map[string]map[string]int{}, known: map[string]bool{}, clock: time.Date(2026, 10, 17, 1, 0, 0, 0, time.UTC),
		lost: map[string]bool{}, refused: map[string]bool{}}
}

func (f *fakeSnapSchedule) Exec(_ context.Context, argv []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	args := clustertest.ModuleArgs(argv)
	if i := slices.Index(args, "--fs"); i >= 0 {
		args = slices.Delete(slices.Clone(args), i, i+2)
	}
	command := strings.Join(args, " ")
	f.commands++
	for key := range f.refused {
		if strings.HasPrefix(command, key) {
			delete(f.refused, key)
			return clustertest.Reply(argv, 1, []byte("injected refusal"))
		}
	}
	code, out := f.apply(args)
	for key := range f.lost {
		if strings.HasPrefix(command, key) {
			delete(f.lost, key)
			return 0, nil, errors.New("injected lost reply")
		}
	}
	return clustertest.Reply(argv, code, []byte(out))
}

func (f *fakeSnapSchedule) apply(args []string) (int, string) {
	command := strings.Join(args, " ")
	switch {
	case command == "fs dump --format json":
		return 0, fmt.Sprintf(`{"filesystems":[{"id":%d,"mdsmap":{"fs_name":"tc","max_mds":1,"metadata_pool":1,"data_pools":[2],"info":{}}}],"standbys":[]}`, f.fsID)
	case command == "osd pool ls detail --format json":
		return 0, `[{"pool_id":1,"pool_name":"metadata","type":1,"size":2,"min_size":1,"pg_num":8},{"pool_id":2,"pool_name":"data","type":1,"size":2,"min_size":1,"pg_num":8}]`
	case command == "mgr module ls --format json":
		return 0, `{"always_on_modules": [], "enabled_modules": ["snap_schedule"], "disabled_modules": []}`
	case command == "mgr dump --format json":
		return 0, `{"available": true, "modules": ["snap_schedule"], "available_modules": [{"name": "snap_schedule", "can_run": true, "error_string": ""}]}`
	case strings.HasPrefix(command, "config get mgr mgr/snap_schedule/allow_m_granularity"):
		return 0, "true\n"
	case strings.HasPrefix(command, "fs snap-schedule status "):
		var entries []map[string]any
		for _, s := range f.schedules {
			if s.path == args[3] {
				entry := map[string]any{"fs": "tc", "path": s.path, "schedule": s.repeat, "retention": nonNil(f.retention[s.path]),
					"start": s.start, "created": s.added, "first": nil, "last": nil, "created_count": 0, "pruned_count": 0, "active": true}
				maps.Copy(entry, f.progress[s.path+" "+s.repeat])
				entries = append(entries, entry)
			}
		}
		if entries == nil {
			return 0, "\n[]\n"
		}
		data, _ := json.Marshal(entries)
		return 0, string(data)
	case strings.HasPrefix(command, "fs snap-schedule list "):
		var repeats []string
		for _, s := range f.schedules {
			if s.path == args[3] {
				repeats = append(repeats, s.repeat)
			}
		}
		if repeats == nil {
			return 0, "{}"
		}
		data, _ := json.Marshal(map[string]any{"path": args[3], "schedule": repeats})
		return 0, string(data)
	case strings.HasPrefix(command, "fs snap-schedule add "):
		start := "2026-10-17T00:00:00"
		if len(args) > 5 {
			start = args[5]
		}
		f.clock = f.clock.Add(time.Second)
		f.known[args[3]] = true
		f.schedules = append(f.schedules, fakeSchedule{args[3], args[4], start, f.clock.Format(nativeTime)})
		return 0, "Schedule set for path " + args[3]
	case strings.HasPrefix(command, "fs snap-schedule remove "):
		before := len(f.schedules)
		f.schedules = slices.DeleteFunc(f.schedules, func(s fakeSchedule) bool {
			return s.path == args[3] && (len(args) < 5 || s.repeat == args[4]) && (len(args) < 6 || s.start == args[5])
		})
		if len(f.schedules) == before {
			return 2, "Error ENOENT: no such schedule"
		}
		return 0, "Schedule removed for path " + args[3]
	case strings.HasPrefix(command, "fs snap-schedule retention ") && !f.known[args[4]]:
		return 2, "Error ENOENT: No schedule found for " + args[4]
	case strings.HasPrefix(command, "fs snap-schedule retention add "):
		count, _ := strconv.Atoi(args[6])
		if f.retention[args[4]] == nil {
			f.retention[args[4]] = map[string]int{}
		}
		f.retention[args[4]][args[5]] = count
		return 0, "Retention added to path " + args[4]
	case strings.HasPrefix(command, "fs snap-schedule retention remove "):
		count, _ := strconv.Atoi(args[6])
		if current, ok := f.retention[args[4]][args[5]]; !ok || current != count {
			return 2, fmt.Sprintf("Error ENOENT: Retention for %s: %d was not set for %s can't remove", args[5], count, args[4])
		}
		delete(f.retention[args[4]], args[5])
		return 0, "Retention removed from path " + args[4]
	}
	return 2, "unexpected command " + command
}

// native runs a command as another client would, outside any lease.
func (f *fakeSnapSchedule) native(t *testing.T, command string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if code, out := f.apply(strings.Fields(command)); code != 0 {
		t.Fatalf("%s: %s", command, out)
	}
}

// report sets status fields of the schedule with path and repeat.
func (f *fakeSnapSchedule) report(path, repeat string, fields map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.progress == nil {
		f.progress = map[string]map[string]any{}
	}
	key := path + " " + repeat
	if f.progress[key] == nil {
		f.progress[key] = map[string]any{}
	}
	maps.Copy(f.progress[key], fields)
}

func (f *fakeSnapSchedule) state() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var parts []string
	for _, s := range f.schedules {
		parts = append(parts, s.path+" "+s.repeat+" "+s.start)
	}
	for _, dir := range slices.Sorted(maps.Keys(f.retention)) {
		if len(f.retention[dir]) > 0 {
			parts = append(parts, fmt.Sprintf("retention %s %v", dir, f.retention[dir]))
		}
	}
	return strings.Join(parts, "; ")
}

// snapScheduleFilesystem returns the registered core of the filesystem "tc"
// over a synthetic cluster whose control container is fake, with its native
// identity captured as FSMap ID 41 and pools 1 and 2. fake.unregistered
// removes it from the registry and fake.fsID replaces it natively.
func snapScheduleFilesystem(fake *fakeSnapSchedule) *fsbase.Core {
	descriptor := "tc"
	core := fsbase.NewCore(fsbase.Params{
		Cluster: testhelper.TestCluster(fake, testhelper.TestConfig{StartupTimeout: time.Second, OSDs: 1}), Name: "tc",
		DescriptorName: &descriptor, MetadataPool: "metadata", DataPool: "data", SetupMu: &sync.Mutex{},
		Live: func(*fsbase.Core) bool {
			fake.mu.Lock()
			defer fake.mu.Unlock()
			return !fake.unregistered
		},
	})
	core.ReplaceNative(&fsbase.NativeIdentity{ID: 41, MetadataPool: 1, DefaultPool: 2, Attachments: map[string]*fsbase.PoolAttachment{}})
	return core
}

var leaseSchedule = Config{Path: "/", Repeat: "2h", Start: time.Date(2030, 3, 1, 0, 0, 0, 0, time.UTC), Retention: map[string]int{"d": 1, "n": 2}}

func TestLeaseLifecycle(t *testing.T) {
	fake := newFakeSnapSchedule()
	fs := snapScheduleFilesystem(fake)
	lease, err := add(t.Context(), fs, leaseSchedule)
	if err != nil {
		t.Fatal(err)
	}
	status, err := lease.Status(t.Context())
	if err != nil || status.Schedule != "2h" || !status.Start.Equal(leaseSchedule.Start) || !maps.Equal(status.Retention, leaseSchedule.Retention) {
		t.Fatalf("status %+v, %v", status, err)
	}
	if again, err := add(t.Context(), fs, Config{Path: "/", Repeat: "1h"}); !errors.Is(err, ceph.ErrInvalidRequest) || again != nil {
		t.Fatalf("a second lease of the directory: %v", err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if state := fake.state(); state != "" {
		t.Fatalf("after Restore: %s", state)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	next, err := add(t.Context(), fs, Config{Path: "/", Repeat: "1h"})
	if err != nil {
		t.Fatalf("a lease after Restore: %v", err)
	}
	if err := next.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// A schedule with the lease's path and repeat that another client added after
// removing the lease's is not the lease's: Restore refuses it and stays
// retryable.
func TestLeaseRefusesAReplacement(t *testing.T) {
	fake := newFakeSnapSchedule()
	lease, err := add(t.Context(), snapScheduleFilesystem(fake), Config{Path: "/", Repeat: "1h", Start: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	fake.native(t, "fs snap-schedule remove / 1h 2030-01-01T00:00:00")
	fake.native(t, "fs snap-schedule add / 1h 2030-02-01T00:00:00")
	if _, err := lease.Status(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("status of a replaced schedule: %v", err)
	}
	if err := lease.Restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Restore of a replaced schedule: %v", err)
	}
	if state := fake.state(); state != "/ 1h 2030-02-01T00:00:00" {
		t.Fatalf("the refused Restore changed the replacement: %s", state)
	}
	// The same repeat added again in the same slot is still another schedule.
	fake.native(t, "fs snap-schedule remove / 1h 2030-02-01T00:00:00")
	fake.native(t, "fs snap-schedule add / 1h 2030-01-01T00:00:00")
	if err := lease.Restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Restore of a schedule added again: %v", err)
	}
	fake.native(t, "fs snap-schedule remove / 1h")
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatalf("Restore after the owner removed it: %v", err)
	}
}

// Restore continues from the directory's current state: retention rules that
// are already gone, by another client or an earlier call, are skipped.
func TestLeaseResumesCleanup(t *testing.T) {
	for name, interrupt := range map[string]func(*testing.T, *fakeSnapSchedule, *Lease){
		"rule removed natively": func(t *testing.T, fake *fakeSnapSchedule, _ *Lease) {
			fake.native(t, "fs snap-schedule retention remove / d 1")
		},
		"earlier Restore stopped at the schedule": func(t *testing.T, fake *fakeSnapSchedule, lease *Lease) {
			fake.refused["fs snap-schedule remove"] = true
			if err := lease.Restore(t.Context()); err == nil {
				t.Fatal("the injected failure did not stop Restore")
			}
		},
		"earlier Restore stopped at the second rule": func(t *testing.T, fake *fakeSnapSchedule, lease *Lease) {
			fake.refused["fs snap-schedule retention remove / n"] = true
			if err := lease.Restore(t.Context()); err == nil {
				t.Fatal("the injected failure did not stop Restore")
			}
		},
		"reply of the schedule removal lost": func(t *testing.T, fake *fakeSnapSchedule, lease *Lease) {
			fake.lost["fs snap-schedule remove"] = true
			if err := lease.Restore(t.Context()); err == nil {
				t.Fatal("the injected failure did not stop Restore")
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			fake := newFakeSnapSchedule()
			lease, err := add(t.Context(), snapScheduleFilesystem(fake), leaseSchedule)
			if err != nil {
				t.Fatal(err)
			}
			interrupt(t, fake, lease)
			if err := lease.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if state := fake.state(); state != "" {
				t.Fatalf("after Restore: %s", state)
			}
		})
	}
}

// A lease returned with an error removes what the module lists of its
// request: a schedule whose add reply was lost, and of its retention only
// the rules that were set.
func TestLeaseAfterAFailedAdd(t *testing.T) {
	for name, fault := range map[string]func(*fakeSnapSchedule){
		"schedule add reply lost": func(f *fakeSnapSchedule) { f.lost["fs snap-schedule add"] = true },
		"schedule add refused":    func(f *fakeSnapSchedule) { f.refused["fs snap-schedule add"] = true },
		"second rule refused":     func(f *fakeSnapSchedule) { f.refused["fs snap-schedule retention add / n"] = true },
		"second rule reply lost":  func(f *fakeSnapSchedule) { f.lost["fs snap-schedule retention add / n"] = true },
	} {
		t.Run(name, func(t *testing.T) {
			fake := newFakeSnapSchedule()
			fault(fake)
			lease, err := add(t.Context(), snapScheduleFilesystem(fake), leaseSchedule)
			if err == nil || lease == nil {
				t.Fatalf("%v, %v", lease, err)
			}
			if err := lease.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if state := fake.state(); state != "" {
				t.Fatalf("after Restore: %s", state)
			}
		})
	}
}

// Retention belongs to the directory: a rule of the lease that another
// schedule shares is refused, and a rule that outlived the lease's schedule
// is still removed.
func TestLeaseDirectoryRetention(t *testing.T) {
	schedule := Config{Path: "/", Repeat: "3h", Start: time.Date(2030, 4, 1, 0, 0, 0, 0, time.UTC), Retention: map[string]int{"n": 2}}
	t.Run("schedule of another repeat", func(t *testing.T) {
		fake := newFakeSnapSchedule()
		lease, err := add(t.Context(), snapScheduleFilesystem(fake), schedule)
		if err != nil {
			t.Fatal(err)
		}
		fake.native(t, "fs snap-schedule add / 6h 2030-06-01T00:00:00")
		before := fake.state()
		if err := lease.Restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || fake.state() != before {
			t.Fatalf("Restore beside another schedule: %v, %s", err, fake.state())
		}
		fake.native(t, "fs snap-schedule remove / 6h")
		if err := lease.Restore(t.Context()); err != nil || fake.state() != "" {
			t.Fatalf("Restore after the other schedule was removed: %v, %s", err, fake.state())
		}
	})
	t.Run("schedule removed natively", func(t *testing.T) {
		fake := newFakeSnapSchedule()
		lease, err := add(t.Context(), snapScheduleFilesystem(fake), schedule)
		if err != nil {
			t.Fatal(err)
		}
		fake.native(t, "fs snap-schedule remove / 3h")
		if err := lease.Restore(t.Context()); err != nil || fake.state() != "" {
			t.Fatalf("Restore after the schedule was removed: %v, %s", err, fake.state())
		}
	})
	t.Run("schedule removed, another one added", func(t *testing.T) {
		fake := newFakeSnapSchedule()
		lease, err := add(t.Context(), snapScheduleFilesystem(fake), schedule)
		if err != nil {
			t.Fatal(err)
		}
		fake.native(t, "fs snap-schedule remove / 3h")
		fake.native(t, "fs snap-schedule add / 8h 2030-08-01T00:00:00")
		before := fake.state()
		if err := lease.Restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || fake.state() != before {
			t.Fatalf("Restore with the rule shared by another schedule: %v, %s", err, fake.state())
		}
		fake.native(t, "fs snap-schedule remove / 8h")
		if err := lease.Restore(t.Context()); err != nil || fake.state() != "" {
			t.Fatalf("Restore after the other schedule was removed: %v, %s", err, fake.state())
		}
	})
	t.Run("rule removal interrupted without schedules", func(t *testing.T) {
		for name, fault := range map[string]func(*fakeSnapSchedule){
			"reply lost": func(f *fakeSnapSchedule) { f.lost["fs snap-schedule retention remove"] = true },
			"refused":    func(f *fakeSnapSchedule) { f.refused["fs snap-schedule retention remove"] = true },
		} {
			t.Run(name, func(t *testing.T) {
				fake := newFakeSnapSchedule()
				lease, err := add(t.Context(), snapScheduleFilesystem(fake), schedule)
				if err != nil {
					t.Fatal(err)
				}
				fake.native(t, "fs snap-schedule remove / 3h")
				fault(fake)
				if err := lease.Restore(t.Context()); err == nil {
					t.Fatal("the interrupted rule removal completed the lease")
				}
				if err := lease.Restore(t.Context()); err != nil || fake.state() != "" {
					t.Fatalf("retry: %v, %s", err, fake.state())
				}
			})
		}
	})
}

func TestLeaseRefusals(t *testing.T) {
	t.Run("changed rule", func(t *testing.T) {
		fake := newFakeSnapSchedule()
		lease, err := add(t.Context(), snapScheduleFilesystem(fake), leaseSchedule)
		if err != nil {
			t.Fatal(err)
		}
		fake.native(t, "fs snap-schedule retention add / n 5")
		before := fake.state()
		if err := lease.Restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || fake.state() != before {
			t.Fatalf("Restore with a changed rule: %v, %s", err, fake.state())
		}
		fake.native(t, "fs snap-schedule retention remove / n 5")
		if err := lease.Restore(t.Context()); err != nil || fake.state() != "" {
			t.Fatalf("Restore after the change was undone: %v, %s", err, fake.state())
		}
	})
	t.Run("existing schedule", func(t *testing.T) {
		fake := newFakeSnapSchedule()
		fake.native(t, "fs snap-schedule add / 6h")
		before := fake.state()
		if lease, err := add(t.Context(), snapScheduleFilesystem(fake), leaseSchedule); !errors.Is(err, ceph.ErrInvalidRequest) || lease != nil || fake.state() != before {
			t.Fatalf("%v, %v, %s", lease, err, fake.state())
		}
	})
	t.Run("inherited retention", func(t *testing.T) {
		fake := newFakeSnapSchedule()
		fake.native(t, "fs snap-schedule add / 6h")
		fake.native(t, "fs snap-schedule retention add / n 3")
		fake.native(t, "fs snap-schedule remove / 6h")
		lease, err := add(t.Context(), snapScheduleFilesystem(fake), leaseSchedule)
		if !errors.Is(err, ceph.ErrInvalidRequest) || lease == nil {
			t.Fatalf("a directory with retention left: %v, %v", lease, err)
		}
		if err := lease.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
		if state := fake.state(); state != "retention / map[n:3]" {
			t.Fatalf("Restore changed the inherited retention: %s", state)
		}
	})
}

// Add and every lease operation take the filesystem admission: a filesystem
// that left the registry is refused before any command, and a lease refuses a
// filesystem whose native ID is not the one it was added on.
func TestLeaseAdmission(t *testing.T) {
	fake := newFakeSnapSchedule()
	fs := snapScheduleFilesystem(fake)
	lease, err := add(t.Context(), fs, leaseSchedule)
	if err != nil {
		t.Fatal(err)
	}
	before := fake.state()
	fake.mu.Lock()
	fake.unregistered = true
	commands := fake.commands
	fake.mu.Unlock()
	if _, err := add(t.Context(), fs, Config{Path: "/data", Repeat: "1h"}); err == nil {
		t.Fatal("Add on an unregistered filesystem")
	}
	if _, err := lease.Status(t.Context()); err == nil {
		t.Fatal("Status on an unregistered filesystem")
	}
	if _, err := lease.Snapshots(t.Context()); err == nil {
		t.Fatal("Snapshots on an unregistered filesystem")
	}
	if err := lease.Restore(t.Context()); err == nil {
		t.Fatal("Restore on an unregistered filesystem")
	}
	fake.mu.Lock()
	fake.unregistered = false
	ran := fake.commands - commands
	fake.mu.Unlock()
	if ran != 0 || fake.state() != before {
		t.Fatalf("refused operations ran %d commands: %s", ran, fake.state())
	}
	// The filesystem was replaced natively under its name.
	fake.mu.Lock()
	fake.fsID = 42
	fake.mu.Unlock()
	if err := lease.Restore(t.Context()); err == nil || fake.state() != before {
		t.Fatalf("Restore on a replaced filesystem: %v, %s", err, fake.state())
	}
	fake.mu.Lock()
	fake.fsID = 41
	fake.mu.Unlock()
	// A lease added on another FSMap ID of the filesystem is not this one's.
	lease.filesystemID = 40
	if _, err := lease.Status(t.Context()); err == nil || !strings.Contains(err.Error(), "replaced") {
		t.Fatalf("Status of a lease of another filesystem ID: %v", err)
	}
	if err := lease.Restore(t.Context()); err == nil || fake.state() != before {
		t.Fatalf("Restore of a lease of another filesystem ID: %v, %s", err, fake.state())
	}
	lease.filesystemID = 41
	if err := lease.Restore(t.Context()); err != nil || fake.state() != "" {
		t.Fatalf("Restore: %v, %s", err, fake.state())
	}
}

// reorder lists the fake's schedules in reverse, as a module whose status
// lists them in another order would.
func (f *fakeSnapSchedule) reorder() {
	f.mu.Lock()
	defer f.mu.Unlock()
	slices.Reverse(f.schedules)
}

// Another schedule of the directory with the lease's repeat is refused
// wherever the module lists it, before or after the lease's own, by Status,
// a watch read and Restore, which change nothing; once it is gone the lease's
// own schedule is accepted again.
func TestLeaseRefusesAnotherScheduleWithItsRepeatInAnyOrder(t *testing.T) {
	for name, schedule := range map[string]Config{
		"default start":   {Path: "/", Repeat: "1h"},
		"requested start": {Path: "/", Repeat: "1h", Start: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(name, func(t *testing.T) {
			fake := newFakeSnapSchedule()
			lease, err := add(t.Context(), snapScheduleFilesystem(fake), schedule)
			if err != nil {
				t.Fatal(err)
			}
			owned := fake.state()
			fake.native(t, "fs snap-schedule add / 1h 2031-01-01T00:00:00")
			for _, order := range []string{"owned first", "other first"} {
				before := fake.state()
				if _, err := lease.Status(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
					t.Errorf("%s: Status = %v", order, err)
				}
				var ended leaseWatchEnded
				if _, err := lease.watchRead(t.Context()); !errors.As(err, &ended) || !errors.Is(err, ceph.ErrInvalidRequest) {
					t.Errorf("%s: watch read = %v", order, err)
				}
				if err := lease.Restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || fake.state() != before || lease.restored {
					t.Errorf("%s: Restore = %v, %s", order, err, fake.state())
				}
				fake.reorder()
			}
			fake.native(t, "fs snap-schedule remove / 1h 2031-01-01T00:00:00")
			if fake.state() != owned {
				t.Fatalf("the lease's schedule changed: %s", fake.state())
			}
			if status, err := lease.watchRead(t.Context()); err != nil || status.Schedule != "1h" {
				t.Fatalf("the lease's own schedule: %+v, %v", status, err)
			}
			if err := lease.Restore(t.Context()); err != nil || fake.state() != "" {
				t.Fatalf("Restore: %v, %s", err, fake.state())
			}
		})
	}
}

// A lease whose add reply was lost takes the only schedule with its repeat and
// requested start as its own; another one with that repeat is refused in either
// order and leaves both schedules.
func TestLeaseAfterALostAddRefusesAnotherScheduleWithItsRepeatInAnyOrder(t *testing.T) {
	fake := newFakeSnapSchedule()
	fake.lost["fs snap-schedule add"] = true
	lease, err := add(t.Context(), snapScheduleFilesystem(fake), Config{Path: "/", Repeat: "1h", Start: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)})
	if err == nil || lease == nil || lease.added != "" {
		t.Fatalf("%v, %v", lease, err)
	}
	fake.native(t, "fs snap-schedule add / 1h 2031-01-01T00:00:00")
	for _, order := range []string{"owned first", "other first"} {
		before := fake.state()
		if err := lease.Restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || fake.state() != before {
			t.Errorf("%s: Restore = %v, %s", order, err, fake.state())
		}
		fake.reorder()
	}
	fake.native(t, "fs snap-schedule remove / 1h 2031-01-01T00:00:00")
	if err := lease.Restore(t.Context()); err != nil || fake.state() != "" {
		t.Fatalf("Restore: %v, %s", err, fake.state())
	}
}

// The lease's operations and Restore use the schedule that Add recorded:
// editing the exported Path, Repeat or Start retargets nothing, and Restore
// removes the original schedule and its rules, leaves a schedule at the
// edited target and releases the original directory.
func TestLeaseIgnoresEditedDescriptors(t *testing.T) {
	edits := map[string]func(*Lease){
		"path":   func(l *Lease) { l.Path = "/different" },
		"repeat": func(l *Lease) { l.Repeat = "6h" },
		"start":  func(l *Lease) { l.Start = time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC) },
		"all": func(l *Lease) {
			l.Path, l.Repeat, l.Start, l.Retention = "/different", "6h", time.Time{}, map[string]int{"n": 9}
		},
	}
	for _, failure := range []string{"", "fs snap-schedule add", "fs snap-schedule retention add / n"} {
		for name, edit := range edits {
			label := name
			if failure != "" {
				label += " after a lost reply of " + failure
			}
			t.Run(label, func(t *testing.T) {
				fake := newFakeSnapSchedule()
				fs := snapScheduleFilesystem(fake)
				fake.native(t, "fs snap-schedule add /different 6h 2031-01-01T00:00:00")
				decoy := fake.state()
				if failure != "" {
					fake.lost[failure] = true
				}
				lease, err := add(t.Context(), fs, leaseSchedule)
				if (err != nil) != (failure != "") || lease == nil {
					t.Fatalf("%v, %v", lease, err)
				}
				edit(lease)
				if failure == "" {
					if status, err := lease.Status(t.Context()); err != nil || status.Path != "/" || status.Schedule != "2h" {
						t.Fatalf("Status of the edited lease: %+v, %v", status, err)
					}
				}
				if err := lease.Restore(t.Context()); err != nil || !lease.restored {
					t.Fatalf("Restore of the edited lease: %v", err)
				}
				if state := fake.state(); state != decoy {
					t.Fatalf("after Restore: %s, want only %s", state, decoy)
				}
				if n := len(lease.leases.paths); n != 0 {
					t.Fatalf("Restore left %d reservations", n)
				}
				next, err := add(t.Context(), fs, Config{Path: "/", Repeat: "1h"})
				if err != nil {
					t.Fatalf("a lease of the original directory: %v", err)
				}
				if err := next.Restore(t.Context()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// Concurrent Restore calls of one lease remove its schedule and rules once:
// the lease's mutex orders them, every call succeeds, and the directory is
// released.
func TestLeaseConcurrentRestoresRemoveOnce(t *testing.T) {
	fake := newFakeSnapSchedule()
	lease, err := add(t.Context(), snapScheduleFilesystem(fake), leaseSchedule)
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for range cap(errs) {
		wg.Go(func() { errs <- lease.Restore(t.Context()) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	if state := fake.state(); state != "" || !lease.restored || len(lease.leases.paths) != 0 {
		t.Fatalf("after concurrent Restores: %q, restored %t, %d reservations", state, lease.restored, len(lease.leases.paths))
	}
}
