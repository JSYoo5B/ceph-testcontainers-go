package cephfs

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
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
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
}

func newFakeSnapSchedule() *fakeSnapSchedule {
	return &fakeSnapSchedule{retention: map[string]map[string]int{}, known: map[string]bool{}, clock: time.Date(2026, 10, 17, 1, 0, 0, 0, time.UTC),
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
				entries = append(entries, map[string]any{"fs": "tc", "path": s.path, "schedule": s.repeat, "retention": nonNil(f.retention[s.path]),
					"start": s.start, "created": s.added, "first": nil, "last": nil, "created_count": 0, "pruned_count": 0, "active": true})
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
		f.schedules = append(f.schedules, fakeSchedule{args[3], args[4], start, f.clock.Format(snapshotScheduleTime)})
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

func snapScheduleFilesystem(fake *fakeSnapSchedule) *Filesystem {
	return &Filesystem{cluster: poolFixtureCluster(fake, 1), config: Config{Name: "tc"}, FilesystemName: "tc", name: "tc"}
}

var leaseSchedule = SnapshotSchedule{Path: "/", Repeat: "2h", Start: time.Date(2030, 3, 1, 0, 0, 0, 0, time.UTC), Retention: map[string]int{"d": 1, "n": 2}}

func TestSnapshotScheduleLeaseLifecycle(t *testing.T) {
	fake := newFakeSnapSchedule()
	fs := snapScheduleFilesystem(fake)
	lease, err := fs.AddSnapshotSchedule(t.Context(), leaseSchedule)
	if err != nil {
		t.Fatal(err)
	}
	status, err := lease.Status(t.Context())
	if err != nil || status.Schedule != "2h" || !status.Start.Equal(leaseSchedule.Start) || !maps.Equal(status.Retention, leaseSchedule.Retention) {
		t.Fatalf("status %+v, %v", status, err)
	}
	if again, err := fs.AddSnapshotSchedule(t.Context(), SnapshotSchedule{Path: "/", Repeat: "1h"}); !errors.Is(err, ceph.ErrInvalidRequest) || again != nil {
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
	next, err := fs.AddSnapshotSchedule(t.Context(), SnapshotSchedule{Path: "/", Repeat: "1h"})
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
func TestSnapshotScheduleLeaseRefusesAReplacement(t *testing.T) {
	fake := newFakeSnapSchedule()
	lease, err := snapScheduleFilesystem(fake).AddSnapshotSchedule(t.Context(), SnapshotSchedule{Path: "/", Repeat: "1h", Start: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)})
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
func TestSnapshotScheduleLeaseResumesCleanup(t *testing.T) {
	for name, interrupt := range map[string]func(*testing.T, *fakeSnapSchedule, *SnapshotScheduleLease){
		"rule removed natively": func(t *testing.T, fake *fakeSnapSchedule, _ *SnapshotScheduleLease) {
			fake.native(t, "fs snap-schedule retention remove / d 1")
		},
		"earlier Restore stopped at the schedule": func(t *testing.T, fake *fakeSnapSchedule, lease *SnapshotScheduleLease) {
			fake.refused["fs snap-schedule remove"] = true
			if err := lease.Restore(t.Context()); err == nil {
				t.Fatal("the injected failure did not stop Restore")
			}
		},
		"earlier Restore stopped at the second rule": func(t *testing.T, fake *fakeSnapSchedule, lease *SnapshotScheduleLease) {
			fake.refused["fs snap-schedule retention remove / n"] = true
			if err := lease.Restore(t.Context()); err == nil {
				t.Fatal("the injected failure did not stop Restore")
			}
		},
		"reply of the schedule removal lost": func(t *testing.T, fake *fakeSnapSchedule, lease *SnapshotScheduleLease) {
			fake.lost["fs snap-schedule remove"] = true
			if err := lease.Restore(t.Context()); err == nil {
				t.Fatal("the injected failure did not stop Restore")
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			fake := newFakeSnapSchedule()
			lease, err := snapScheduleFilesystem(fake).AddSnapshotSchedule(t.Context(), leaseSchedule)
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
func TestSnapshotScheduleLeaseAfterAFailedAdd(t *testing.T) {
	for name, fault := range map[string]func(*fakeSnapSchedule){
		"schedule add reply lost": func(f *fakeSnapSchedule) { f.lost["fs snap-schedule add"] = true },
		"schedule add refused":    func(f *fakeSnapSchedule) { f.refused["fs snap-schedule add"] = true },
		"second rule refused":     func(f *fakeSnapSchedule) { f.refused["fs snap-schedule retention add / n"] = true },
		"second rule reply lost":  func(f *fakeSnapSchedule) { f.lost["fs snap-schedule retention add / n"] = true },
	} {
		t.Run(name, func(t *testing.T) {
			fake := newFakeSnapSchedule()
			fault(fake)
			lease, err := snapScheduleFilesystem(fake).AddSnapshotSchedule(t.Context(), leaseSchedule)
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
func TestSnapshotScheduleLeaseDirectoryRetention(t *testing.T) {
	schedule := SnapshotSchedule{Path: "/", Repeat: "3h", Start: time.Date(2030, 4, 1, 0, 0, 0, 0, time.UTC), Retention: map[string]int{"n": 2}}
	t.Run("schedule of another repeat", func(t *testing.T) {
		fake := newFakeSnapSchedule()
		lease, err := snapScheduleFilesystem(fake).AddSnapshotSchedule(t.Context(), schedule)
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
		lease, err := snapScheduleFilesystem(fake).AddSnapshotSchedule(t.Context(), schedule)
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
		lease, err := snapScheduleFilesystem(fake).AddSnapshotSchedule(t.Context(), schedule)
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
				lease, err := snapScheduleFilesystem(fake).AddSnapshotSchedule(t.Context(), schedule)
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

func TestSnapshotScheduleLeaseRefusals(t *testing.T) {
	t.Run("changed rule", func(t *testing.T) {
		fake := newFakeSnapSchedule()
		lease, err := snapScheduleFilesystem(fake).AddSnapshotSchedule(t.Context(), leaseSchedule)
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
		if lease, err := snapScheduleFilesystem(fake).AddSnapshotSchedule(t.Context(), leaseSchedule); !errors.Is(err, ceph.ErrInvalidRequest) || lease != nil || fake.state() != before {
			t.Fatalf("%v, %v, %s", lease, err, fake.state())
		}
	})
	t.Run("inherited retention", func(t *testing.T) {
		fake := newFakeSnapSchedule()
		fake.native(t, "fs snap-schedule add / 6h")
		fake.native(t, "fs snap-schedule retention add / n 3")
		fake.native(t, "fs snap-schedule remove / 6h")
		lease, err := snapScheduleFilesystem(fake).AddSnapshotSchedule(t.Context(), leaseSchedule)
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
