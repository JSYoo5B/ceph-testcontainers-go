package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const osdLifecycleUUID = "36cb0de3-1526-4d8a-8e0b-519610e0b2db"
const osdLifecycleOtherUUID = "32d13cb4-0e57-41bd-83d1-3f0933eb3331"
const osdLifecycleForeignUUID = "41174ab4-654f-47e3-a7b2-c826b634f7bc"

func TestOSDRemovalDrainsExactIdentityAndCleansOwnedContainer(t *testing.T) {
	cluster, control, daemon := newOSDLifecycleFixture()
	if err := cluster.RemoveOSD(t.Context(), 3); err != nil {
		t.Fatal(err)
	}
	if cluster.osds[3] != nil || cluster.osds[4] == nil || control.native[4].UUID != osdLifecycleOtherUUID || daemon.stops != 1 || daemon.terminations != 1 {
		t.Fatal("removal lost a neighbor, retained ownership or skipped daemon cleanup")
	}
	want := []string{"osd crush reweight osd.3 0", "osd out 3", "osd safe-to-destroy 3", "osd purge 3 --yes-i-really-mean-it"}
	if !slices.Equal(control.actions, want) {
		t.Fatalf("unexpected removal actions: %v", control.actions)
	}
	// Reweight, out, drain, stop, down and purge must each use a fresh map;
	// a single preflight identity snapshot cannot guard the later phases.
	if control.dumps < 7 {
		t.Fatalf("removal reused an early identity snapshot: %d reads", control.dumps)
	}
}

func TestOSDRemovalRejectsMissingMalformedAndForeignIdentity(t *testing.T) {
	for _, scenario := range []string{"missing", "foreign", "native UUID malformed", "captured UUID unavailable", "captured UUID nil", "unissued absence"} {
		t.Run(scenario, func(t *testing.T) {
			cluster, control, daemon := newOSDLifecycleFixture()
			switch scenario {
			case "missing", "unissued absence":
				delete(control.native, 3)
			case "foreign":
				state := control.native[3]
				state.UUID = osdLifecycleForeignUUID
				control.native[3] = state
			case "native UUID malformed":
				state := control.native[3]
				state.UUID = "not-a-uuid"
				control.native[3] = state
			case "captured UUID unavailable":
				cluster.osds[3].nativeUUID = ""
			case "captured UUID nil":
				cluster.osds[3].nativeUUID = "urn:uuid:00000000-0000-0000-0000-000000000000"
			}
			owned := cluster.osds[3]
			if err := cluster.RemoveOSD(t.Context(), 3); err == nil {
				t.Fatal("absent/malformed/replaced registration was accepted")
			}
			if len(control.actions) != 0 || daemon.stops != 0 || daemon.terminations != 0 || cluster.osds[3] != owned || owned.purged || owned.purgeIssued {
				t.Fatal("identity rejection mutated native membership, daemon or ownership")
			}
		})
	}
}

func TestOSDRemovalRejectsMalformedNativeSnapshotBeforeMutation(t *testing.T) {
	for _, raw := range []string{
		`null`, `{}`, `{"flags":"","osds":null}`,
		`{"flags":"","osds":[{"osd":3,"uuid":"36cb0de3-1526-4d8a-8e0b-519610e0b2db","up":1,"in":1}]}`,
		`{"flags":"","osds":[{"osd":3,"uuid":"36cb0de3-1526-4d8a-8e0b-519610e0b2db","up":2,"in":1,"weight":1}]}`,
	} {
		t.Run(raw, func(t *testing.T) {
			cluster, control, daemon := newOSDLifecycleFixture()
			control.raw = raw
			// Even a pending own purge cannot infer absence from malformed data.
			cluster.osds[3].purgeIssued = true
			if err := cluster.RemoveOSD(t.Context(), 3); err == nil {
				t.Fatal("malformed OSDMap was accepted as a purge acknowledgement")
			}
			if len(control.actions) != 0 || daemon.stops != 0 || daemon.terminations != 0 || cluster.osds[3].purged {
				t.Fatal("malformed snapshot reached native or Docker changes")
			}
		})
	}
}

func TestOSDRemovalRechecksIdentityBetweenAllRemovalPhases(t *testing.T) {
	for _, phase := range []string{"reweight", "out", "safe-to-destroy", "stop", "down read"} {
		t.Run(phase, func(t *testing.T) {
			cluster, control, daemon := newOSDLifecycleFixture()
			replace := func() {
				state := control.native[3]
				state.UUID = osdLifecycleForeignUUID
				control.native[3] = state
			}
			if phase == "stop" {
				daemon.afterStop = replace
			} else if phase == "down read" {
				control.afterDump = func() {
					if daemon.stops != 0 {
						replace()
					}
				}
			} else {
				control.afterAction = func(action string) {
					if strings.Contains(action, phase) {
						replace()
					}
				}
			}
			if err := cluster.RemoveOSD(t.Context(), 3); err == nil {
				t.Fatalf("replacement after %s reached successful purge", phase)
			}
			if control.purges != 0 || daemon.terminations != 0 || cluster.osds[3] == nil || cluster.osds[3].purgeIssued {
				t.Fatal("a later phase purged a replacement or discarded cleanup ownership")
			}
			if phase != "stop" && phase != "down read" && daemon.stops != 0 {
				t.Fatal("a daemon was stopped after its native identity changed")
			}
			if phase == "reweight" && slices.Contains(control.actions, "osd out 3") {
				t.Fatal("out was issued without rechecking UUID after reweight")
			}
		})
	}
}

func TestOSDRemovalLostPurgeReplyReconcilesFreshAbsence(t *testing.T) {
	cluster, control, daemon := newOSDLifecycleFixture()
	lost := errors.New("purge reply lost after commit")
	control.purgeErr, control.applyPurgeOnError = lost, true
	owned := cluster.osds[3]
	if err := cluster.RemoveOSD(t.Context(), 3); !errors.Is(err, lost) {
		t.Fatalf("uncertain purge response lost its original error: %v", err)
	}
	if !owned.purgeIssued || owned.purged || cluster.osds[3] != owned || daemon.terminations != 0 || control.purges != 1 {
		t.Fatal("uncertain purge discarded ownership or prematurely cleaned up")
	}
	beforeActions, beforeDumps := len(control.actions), control.dumps
	control.purgeErr = nil
	if err := cluster.RemoveOSD(t.Context(), 3); err != nil {
		t.Fatal(err)
	}
	if cluster.osds[3] != nil || daemon.stops != 1 || daemon.terminations != 1 || control.purges != 1 || len(control.actions) != beforeActions || control.dumps != beforeDumps+1 {
		t.Fatal("lost-reply retry repeated mutation/stop or skipped fresh absence observation")
	}
}

func TestOSDRemovalPendingPurgeStillPresentCanRetryExactIdentity(t *testing.T) {
	cluster, control, daemon := newOSDLifecycleFixture()
	lost := errors.New("purge request response unavailable")
	control.purgeErr = lost
	if err := cluster.RemoveOSD(t.Context(), 3); !errors.Is(err, lost) {
		t.Fatal("uncertain purge error was lost", err)
	}
	if cluster.osds[3].purged || !cluster.osds[3].purgeIssued || control.native[3].UUID != osdLifecycleUUID || daemon.terminations != 0 {
		t.Fatal("an uncommitted purge was treated as absence")
	}
	control.purgeErr = nil
	if err := cluster.RemoveOSD(t.Context(), 3); err != nil {
		t.Fatal(err)
	}
	if control.purges != 2 || daemon.terminations != 1 || cluster.osds[3] != nil {
		t.Fatal("fresh matching registration did not remain retryable")
	}
}

func TestOSDRemovalPendingPurgeRejectsForeignReplacementAndReadError(t *testing.T) {
	for _, scenario := range []string{"replacement", "read error"} {
		t.Run(scenario, func(t *testing.T) {
			cluster, control, daemon := newOSDLifecycleFixture()
			cluster.osds[3].purgeIssued = true
			readErr := errors.New("cannot read fresh OSDMap")
			if scenario == "replacement" {
				state := control.native[3]
				state.UUID = osdLifecycleForeignUUID
				control.native[3] = state
			} else {
				control.dumpErr = readErr
			}
			err := cluster.RemoveOSD(t.Context(), 3)
			if err == nil || (scenario == "read error" && !errors.Is(err, readErr)) {
				t.Fatalf("fresh state was adopted or error lost: %v", err)
			}
			if len(control.actions) != 0 || daemon.stops != 0 || daemon.terminations != 0 || cluster.osds[3].purged {
				t.Fatal("unknown/replaced state reached purge or daemon cleanup")
			}
		})
	}
}

func TestOSDRemovalCompletedPurgeRetriesOnlyDockerCleanup(t *testing.T) {
	cluster, control, daemon := newOSDLifecycleFixture()
	cleanupErr := errors.New("Docker cleanup temporarily unavailable")
	daemon.terminateErr = cleanupErr
	owned := cluster.osds[3]
	if err := cluster.RemoveOSD(t.Context(), 3); !errors.Is(err, cleanupErr) || !owned.purged || !owned.purgeIssued || cluster.osds[3] != owned {
		t.Fatalf("completed native purge lost retryable Docker ownership: %v", err)
	}
	beforeDumps, beforeActions := control.dumps, len(control.actions)
	// The acknowledged purge does not adopt a later native reuse of the ID.
	control.native[3] = OSDState{ID: 3, UUID: osdLifecycleForeignUUID, Up: true, In: true, Weight: 1}
	daemon.terminateErr = nil
	if err := cluster.RemoveOSD(t.Context(), 3); err != nil {
		t.Fatal(err)
	}
	if cluster.osds[3] != nil || control.dumps != beforeDumps || len(control.actions) != beforeActions || control.native[3].UUID != osdLifecycleForeignUUID || daemon.stops != 1 || daemon.terminations != 2 {
		t.Fatal("cleanup retry touched native replacement or repeated draining/stop")
	}
}

func TestOSDRemovalAcceptsAlreadyMissingOwnedDockerContainer(t *testing.T) {
	cluster, control, daemon := newOSDLifecycleFixture()
	daemon.terminateErr = errdefs.ErrNotFound
	if err := cluster.RemoveOSD(t.Context(), 3); err != nil || cluster.osds[3] != nil || control.purges != 1 {
		t.Fatalf("missing acknowledged-purge Docker container prevented cleanup: %v", err)
	}
}

func TestOSDRemovalProtectsLastNativeOwnedRegistration(t *testing.T) {
	for _, scenario := range []string{"last descriptor", "purged neighbor", "absent pending neighbor", "foreign neighbor"} {
		t.Run(scenario, func(t *testing.T) {
			cluster, control, daemon := newOSDLifecycleFixture()
			switch scenario {
			case "last descriptor":
				delete(cluster.osds, 4)
				delete(control.native, 4)
			case "purged neighbor":
				cluster.osds[4].purged = true
				delete(control.native, 4)
			case "absent pending neighbor":
				cluster.osds[4].purgeIssued = true
				delete(control.native, 4)
			case "foreign neighbor":
				state := control.native[4]
				state.UUID = osdLifecycleForeignUUID
				control.native[4] = state
			}
			if err := cluster.RemoveOSD(t.Context(), 3); err == nil || !strings.Contains(err.Error(), "last OSD") {
				t.Fatalf("last actual owned registration was removable: %v", err)
			}
			if len(control.actions) != 0 || daemon.stops != 0 || daemon.terminations != 0 {
				t.Fatal("last-OSD guard ran after draining or daemon changes")
			}
		})
	}
	// A last retained descriptor whose own purge was already issued can still
	// reconcile absent native state and finish its Docker-only cleanup.
	cluster, control, daemon := newOSDLifecycleFixture()
	delete(cluster.osds, 4)
	delete(control.native, 4)
	delete(control.native, 3)
	cluster.osds[3].purgeIssued = true
	if err := cluster.RemoveOSD(t.Context(), 3); err != nil || daemon.terminations != 1 || len(control.actions) != 0 {
		t.Fatalf("last pending purge cleanup was blocked or changed native state: %v", err)
	}
}

func TestOSDRemovalDeadlineRetainsPartialOwnershipAndOriginalFailure(t *testing.T) {
	for _, phase := range []string{"drain", "down"} {
		t.Run(phase, func(t *testing.T) {
			cluster, control, daemon := newOSDLifecycleFixture()
			cluster.settings.startupTimeout = 20 * time.Millisecond
			busy := errors.New("not yet safe to destroy")
			if phase == "drain" {
				control.safeErr = busy
			} else {
				daemon.keepUp = true
			}
			err := cluster.RemoveOSD(t.Context(), 3)
			if !errors.Is(err, context.DeadlineExceeded) || (phase == "drain" && !errors.Is(err, busy)) {
				t.Fatalf("partial removal lost deadline or native failure: %v", err)
			}
			if cluster.osds[3] == nil || cluster.osds[3].purged || cluster.osds[3].purgeIssued || control.purges != 0 || daemon.terminations != 0 {
				t.Fatal("deadline discarded ownership or reached purge")
			}
			if phase == "drain" && daemon.stops != 0 {
				t.Fatal("undrained OSD was stopped")
			}
			cluster.settings.startupTimeout = time.Second
			control.safeErr, daemon.keepUp = nil, false
			if err := cluster.RemoveOSD(t.Context(), 3); err != nil || cluster.osds[3] != nil {
				t.Fatalf("fresh-context partial removal retry failed: %v", err)
			}
		})
	}
}

func TestOSDRemovalCanceledAndClosedDoNotChangeState(t *testing.T) {
	for _, scenario := range []string{"canceled parent", "closed", "zero startup timeout", "cancel during map read"} {
		t.Run(scenario, func(t *testing.T) {
			cluster, control, daemon := newOSDLifecycleFixture()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch scenario {
			case "canceled parent":
				cancel()
			case "closed":
				cluster.closed = true
			case "zero startup timeout":
				cluster.settings.startupTimeout = 0
			case "cancel during map read":
				control.afterDump = cancel
			}
			if err := cluster.RemoveOSD(ctx, 3); err == nil {
				t.Fatal("expired/closed operation was accepted")
			}
			if len(control.actions) != 0 || daemon.stops != 0 || daemon.terminations != 0 || cluster.osds[3] == nil || cluster.osds[3].purgeIssued {
				t.Fatal("expired/closed operation changed native or cleanup state")
			}
		})
	}
}

func TestOSDAdditionRefusesPendingPurgeBeforeReusingNativeID(t *testing.T) {
	for _, phase := range []string{"pending purge", "completed purge"} {
		t.Run(phase, func(t *testing.T) {
			cluster, control, _ := newOSDLifecycleFixture()
			owned := cluster.osds[3]
			owned.purgeIssued = true
			owned.purged = phase == "completed purge"
			osd, err := cluster.AddOSD(t.Context())
			if err == nil || osd != nil || control.dumps != 0 || len(control.actions) != 0 || cluster.osds[3] != owned {
				t.Fatal("new registration reused an ID before pending container cleanup")
			}
		})
	}
}

type osdLifecycleControl struct {
	testcontainers.Container
	native            map[int]OSDState
	actions           []string
	dumps, purges     int
	raw               string
	dumpErr, safeErr  error
	purgeErr          error
	applyPurgeOnError bool
	afterAction       func(string)
	afterDump         func()
}

func (control *osdLifecycleControl) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	if len(args) < 4 || args[0] != "ceph" {
		return 0, nil, fmt.Errorf("unexpected lifecycle command: %v", args)
	}
	action := strings.Join(args[3:], " ")
	var output []byte
	if action == "osd dump --format json" {
		control.dumps++
		if control.dumpErr != nil {
			return 0, nil, control.dumpErr
		}
		rows := make([]map[string]any, 0, len(control.native))
		for _, state := range control.native {
			up, in := 0, 0
			if state.Up {
				up = 1
			}
			if state.In {
				in = 1
			}
			rows = append(rows, map[string]any{"osd": state.ID, "uuid": state.UUID, "up": up, "in": in, "weight": state.Weight})
		}
		output, _ = json.Marshal(map[string]any{"flags": "", "osds": rows})
		if control.raw != "" {
			output = []byte(control.raw)
		}
		if control.afterDump != nil {
			control.afterDump()
		}
	} else {
		control.actions = append(control.actions, action)
		switch action {
		case "osd crush reweight osd.3 0":
		case "osd out 3":
			state := control.native[3]
			state.In, state.Weight = false, 0
			control.native[3] = state
		case "osd safe-to-destroy 3":
			if control.safeErr != nil {
				return 0, nil, control.safeErr
			}
		case "osd purge 3 --yes-i-really-mean-it":
			control.purges++
			if control.purgeErr == nil || control.applyPurgeOnError {
				delete(control.native, 3)
			}
			if control.purgeErr != nil {
				return 0, nil, control.purgeErr
			}
		default:
			return 0, nil, fmt.Errorf("unexpected removal action: %s", action)
		}
		if control.afterAction != nil {
			control.afterAction(action)
		}
	}
	var header [8]byte
	header[0] = 1
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.Write(output)
	return 0, &stream, nil
}

type osdLifecycleDaemon struct {
	testcontainers.Container
	control             *osdLifecycleControl
	stops, terminations int
	terminateErr        error
	keepUp              bool
	afterStop           func()
}

func (daemon *osdLifecycleDaemon) Stop(ctx context.Context, _ *time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	daemon.stops++
	if !daemon.keepUp {
		state := daemon.control.native[3]
		state.Up = false
		daemon.control.native[3] = state
	}
	if daemon.afterStop != nil {
		daemon.afterStop()
	}
	return nil
}

func (daemon *osdLifecycleDaemon) Terminate(ctx context.Context, _ ...testcontainers.TerminateOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	daemon.terminations++
	return daemon.terminateErr
}

func newOSDLifecycleFixture() (*Container, *osdLifecycleControl, *osdLifecycleDaemon) {
	control := &osdLifecycleControl{native: map[int]OSDState{
		3: {ID: 3, UUID: osdLifecycleUUID, Up: true, In: true, Weight: 1},
		4: {ID: 4, UUID: osdLifecycleOtherUUID, Up: true, In: true, Weight: 1},
	}}
	daemon := &osdLifecycleDaemon{control: control}
	cluster := &Container{Container: control, settings: options{startupTimeout: time.Second}, osds: map[int]*OSDContainer{
		3: {Container: daemon, ID: 3, nativeUUID: osdLifecycleUUID},
		4: {ID: 4, nativeUUID: osdLifecycleOtherUUID},
	}}
	return cluster, control, daemon
}
