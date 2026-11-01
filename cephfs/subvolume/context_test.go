package subvolume

import (
	"context"
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fstest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

// contextAdmissionFixture is the shared filesystem with an empty group and
// subvolume listing and a confirmed additional data pool attachment.
func contextAdmissionFixture() (*fstest.Filesystem, *poolFixtureContainer, *fsbase.PoolAttachment) {
	fs, control := subvolumeFixture()
	control.Output["fs subvolumegroup ls fixture --format json"] = "[]"
	control.Output["fs subvolume ls fixture --format json"] = "[]"
	attachment := &fsbase.PoolAttachment{Name: "additional", ID: 3, Confirmed: true}
	fs.Core.Native().Attachments["additional"] = attachment
	return fs, control, attachment
}

func TestCephFSSubvolumeContextPublicAdmissionDeadline(t *testing.T) {
	operations := map[string]func(context.Context, *fsbase.Core) error{
		"GroupNames": func(ctx context.Context, fs *fsbase.Core) error {
			_, err := groupNames(ctx, fs)
			return err
		},
		"Names": func(ctx context.Context, fs *fsbase.Core) error {
			_, err := names(ctx, fs, "group")
			return err
		},
		"CreateGroup": func(ctx context.Context, fs *fsbase.Core) error {
			_, err := createGroup(ctx, fs, GroupConfig{Name: "new-group"})
			return err
		},
		"Create": func(ctx context.Context, fs *fsbase.Core) error {
			_, err := create(ctx, fs, Config{Name: "new-volume"})
			return err
		},
	}
	for name, operation := range operations {
		for _, gate := range []string{"setup", "owner", "control"} {
			t.Run(name+"/"+gate, func(t *testing.T) {
				fs, control, attempt := contextAdmissionFixture()
				originalIdentity := fs.Core.Native()
				assertAdmissionGate(t, fs, gate, func(ctx context.Context) error {
					return operation(ctx, fs.Core)
				})
				if len(control.Calls) != 0 {
					t.Fatalf("queued admission sent CLI queries: calls=%v", control.Calls)
				}
				if fs.Core.Native() != originalIdentity || len(originalIdentity.Attachments) != 1 || originalIdentity.Attachments["additional"] != attempt ||
					!attempt.Confirmed || attempt.Used || attempt.Removed || attempt.RemovalAttempted {
					t.Fatal("queued admission changed the filesystem or pool ownership")
				}
				assertAdmissionReleased(t, fs)
				// Query the same fixture after the canceled waiter has returned.
				// This path contains only fake CLI calls and cannot start Docker.
				if names, err := groupNames(t.Context(), fs.Core); err != nil || len(names) != 0 {
					t.Fatalf("fresh context could not reuse the same filesystem: names=%v error=%v", names, err)
				}
			})
		}
	}
}

func TestCephFSSubvolumeContextCanceledFreeAdmissionHasNoEffects(t *testing.T) {
	fs, control, _ := contextAdmissionFixture()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := groupNames(ctx, fs.Core); !errors.Is(err, context.Canceled) || len(control.Calls) != 0 {
		t.Fatalf("canceled free admission acquired resources or lost cancellation: %v", err)
	}
	assertAdmissionReleased(t, fs)
	if _, err := groupNames(t.Context(), fs.Core); err != nil {
		t.Fatalf("canceled free admission blocked the next query: %v", err)
	}
}

func TestCephFSContextPinAdmissionDeadline(t *testing.T) {
	for _, operation := range []string{"read", "temporary", "restore"} {
		for _, gate := range []string{"setup", "owner", "control"} {
			t.Run(operation+"/"+gate, func(t *testing.T) {
				fs, control, volume, _ := cephFSPinFixture()
				change := &PinOverrideHandle{
					filesystem: fs.Core, target: pinTarget{identity: volume.identity},
					setting: PinSetting{Type: PinExport, Value: 1},
					state: &pinOverrideState{
						path: "/volumes/group/volume", inode: 103, previous: 0, applied: 1,
					},
				}
				key := change.state.path + "\x00" + string(change.setting.Type)
				pins := pinOverridesOf(fs.Core)
				pins.byField = map[string]*PinOverrideHandle{key: change}
				control.policies[change.state.path].ExportRank = 1
				assertAdmissionGate(t, fs, gate, func(ctx context.Context) error {
					switch operation {
					case "read":
						_, err := volume.PinPolicy(ctx)
						return err
					case "temporary":
						_, err := volume.OverridePin(ctx, PinSetting{Type: PinDistributed, Value: 1})
						return err
					default:
						return change.Restore(ctx)
					}
				})
				if len(control.calls) != 0 || len(control.native.Calls) != 0 || len(control.mutations) != 0 ||
					change.state.restored || len(pins.byField) != 1 || pins.byField[key] != change ||
					control.policies[change.state.path].ExportRank != 1 ||
					control.policies[change.state.path].Distributed {
					t.Fatal("queued pin operation queried native state, changed policy or lost the recovery handle")
				}
				assertAdmissionReleased(t, fs)
				if policy, err := volume.PinPolicy(t.Context()); err != nil || policy.ExportRank != 1 {
					t.Fatalf("fresh pin read could not reuse the original directory: policy=%+v error=%v", policy, err)
				}
			})
		}
	}
}

// Pin overrides are kept per filesystem: an override of one filesystem never
// blocks or restores the same directory field of another filesystem of the
// cluster, and each core gets its own entry.
func TestCephFSPinOverridesAreKeptPerFilesystem(t *testing.T) {
	fs, _, _, _ := cephFSPinFixture()
	other := fsbase.NewCore(fsbase.Params{Cluster: fs.Core.Cluster(), Name: "other"})
	first := pinOverridesOf(fs.Core)
	first.track("/volumes/group/volume\x00export", &PinOverrideHandle{})
	if again := pinOverridesOf(fs.Core); again != first || len(again.byField) != 1 {
		t.Fatal("a filesystem's override set is not stable")
	}
	if len(pinOverridesOf(other).byField) != 0 {
		t.Fatal("another filesystem of the cluster shares the override set")
	}
}

func TestCephFSDirectControlSnapshotsHonorBusyPublication(t *testing.T) {
	t.Run("pin", func(t *testing.T) {
		fs, control, _, _ := cephFSPinFixture()
		fstest.AssertSnapshotReadLockQueued(t, testhelper.TestState(fs.Core.Cluster()).ControlMutex, func(ctx context.Context) error {
			policy, err := readPinPolicy(ctx, fs.Core.Volumes(), "/volumes/group/volume")
			if policy != nil {
				t.Error("busy control returned a pin policy")
			}
			return err
		})
		if len(control.calls) != 0 || len(control.mutations) != 0 {
			t.Fatal("busy control queried or mutated pin policy")
		}
		policy, err := readPinPolicy(t.Context(), fs.Core.Volumes(), "/volumes/group/volume")
		if err != nil || policy == nil || policy.Inode != 103 || len(control.mutations) != 0 {
			t.Fatalf("fresh context could not read original pin identity: policy=%v err=%v", policy, err)
		}
	})
	t.Run("clone", func(t *testing.T) {
		fs, control, clone := cloneLifecycleFixture(t)
		before := *clone.identity.incarnation
		fstest.AssertSnapshotReadLockQueued(t, testhelper.TestState(fs.Core.Cluster()).ControlMutex, func(ctx context.Context) error {
			incarnation, err := readCloneIncarnation(ctx, fs.Core.Volumes(), clone.identity)
			if incarnation != nil {
				t.Error("busy control returned clone incarnation")
			}
			return err
		})
		if len(control.Calls) != 0 || *clone.identity.incarnation != before {
			t.Fatal("busy control queried or changed captured clone identity")
		}
		incarnation, err := readCloneIncarnation(t.Context(), fs.Core.Volumes(), clone.identity)
		if err != nil || incarnation == nil || *incarnation != before {
			t.Fatalf("fresh context could not inspect original clone incarnation: %v", err)
		}
	})
}
