package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestMonitorRemovalCleansNeverJoinedDescriptor(t *testing.T) {
	control := &monitorLifecycleControl{members: []string{"a"}, quorum: []string{"a"}}
	cluster := monitorLifecycleFixture(control)
	cluster.monitors["b"] = &MonitorContainer{DaemonName: "b"}
	if err := cluster.RemoveMonitor(t.Context(), "b"); err != nil {
		t.Fatal(err)
	}
	if cluster.monitors["b"] != nil || control.removals != 0 || control.copies != 1 || cluster.monitorTerminated {
		t.Fatal("never-joined cleanup changed native membership or retained ownership")
	}
}

func TestMonitorRemovalRetriesDockerCleanupAfterMembershipDisappears(t *testing.T) {
	control := &monitorLifecycleControl{members: []string{"a", "b", "c"}, quorum: []string{"a", "b", "c"}}
	cluster := monitorLifecycleFixture(control)
	daemon := &monitorLifecycleDaemon{terminateErr: errors.New("temporary Docker cleanup failure")}
	cluster.monitors["b"] = &MonitorContainer{Container: daemon, DaemonName: "b"}
	if err := cluster.RemoveMonitor(t.Context(), "b"); err == nil || cluster.monitors["b"] == nil || slices.Contains(control.members, "b") {
		t.Fatal("failed Docker cleanup did not retain the removed member's container")
	}
	// Another completed topology operation can leave only the surviving MON.
	// Retrying b changes no membership and must not refuse that last member.
	control.members, control.quorum = []string{"a"}, []string{"a"}
	daemon.terminateErr = nil
	if err := cluster.RemoveMonitor(t.Context(), "b"); err != nil {
		t.Fatal(err)
	}
	if cluster.monitors["b"] != nil || control.removals != 1 || daemon.terminations != 2 || cluster.monitorTerminated {
		t.Fatal("retry repeated membership removal or lost Docker cleanup")
	}
}

func TestMonitorRemovalProtectsQuorumForActualMembers(t *testing.T) {
	for _, scenario := range []string{"last member", "remaining minority"} {
		t.Run(scenario, func(t *testing.T) {
			control := &monitorLifecycleControl{members: []string{"a"}, quorum: []string{"a"}}
			cluster := monitorLifecycleFixture(control)
			name := "a"
			if scenario == "remaining minority" {
				control.members, control.quorum = []string{"a", "b", "c"}, []string{"a", "b"}
				cluster.monitors["b"] = &MonitorContainer{DaemonName: "b"}
				name = "b"
			}
			if err := cluster.RemoveMonitor(t.Context(), name); err == nil || control.removals != 0 || cluster.monitorTerminated {
				t.Fatal("unsafe native membership removal was accepted")
			}
		})
	}
}

func TestMonitorRemovalConvergesAfterTransientNativeRead(t *testing.T) {
	control := &monitorLifecycleControl{members: []string{"a", "b", "c", "replacement"}, quorum: []string{"a", "b", "c", "replacement"},
		afterRemoval: []monitorLifecycleRead{{timeout: true}}}
	cluster := monitorLifecycleFixture(control)
	daemon := &monitorLifecycleDaemon{}
	cluster.Container, cluster.controlPlane = daemon, control
	if err := cluster.RemoveMonitor(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	if control.removals != 1 || daemon.terminations != 1 || control.copies != 1 || !cluster.monitorTerminated || len(cluster.Monitors()) != 0 {
		t.Fatal("transient read repeated native mutation or lost primary cleanup")
	}
	if strings.Count(string(cluster.config), "[v2:") != 3 || len(control.afterRemoval) != 0 {
		t.Fatal("control bootstrap addresses were not rebuilt from the surviving native map")
	}
}

func TestMonitorRemovalWaitsForAbsentMemberAndWorkingQuorum(t *testing.T) {
	for _, scenario := range []struct {
		name string
		read monitorLifecycleRead
	}{
		{"stale monmap", monitorLifecycleRead{members: []string{"a", "b", "c", "replacement"}}},
		{"stale quorum", monitorLifecycleRead{quorum: []string{"a", "b", "c", "replacement"}}},
		{"minority quorum", monitorLifecycleRead{quorum: []string{"b"}}},
		{"empty monmap", monitorLifecycleRead{members: []string{}}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			control := &monitorLifecycleControl{members: []string{"a", "b", "c", "replacement"}, quorum: []string{"a", "b", "c", "replacement"},
				afterRemoval: []monitorLifecycleRead{scenario.read}}
			cluster := monitorLifecycleFixture(control)
			cluster.Container, cluster.controlPlane = &monitorLifecycleDaemon{}, control
			if err := cluster.RemoveMonitor(t.Context(), "a"); err != nil {
				t.Fatal(err)
			}
			if control.removals != 1 || control.copies != 1 || strings.Count(string(cluster.config), "[v2:") != 3 {
				t.Fatal("an incomplete native view was published as the surviving membership")
			}
			reads := 0
			for _, call := range control.calls {
				if call == "quorum_status --format json" {
					reads++
				}
			}
			if reads != 3 { // preflight, incomplete view, converged view
				t.Fatalf("native membership did not converge before publishing: %v", control.calls)
			}
		})
	}
}

func TestMonitorRemovalReadDeadlineRetainsOwnershipForRetry(t *testing.T) {
	control := &monitorLifecycleControl{members: []string{"a", "b", "c", "replacement"}, quorum: []string{"a", "b", "c", "replacement"},
		afterRemoval: []monitorLifecycleRead{{timeout: true}}}
	cluster := monitorLifecycleFixture(control)
	cluster.settings.startupTimeout = 20 * time.Millisecond
	daemon := &monitorLifecycleDaemon{}
	cluster.Container, cluster.controlPlane = daemon, control
	err := cluster.RemoveMonitor(t.Context(), "a")
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "after removing monitor a") || cluster.monitorTerminated || control.removals != 1 || control.copies != 0 || daemon.terminations != 1 {
		t.Fatalf("failed refresh lost retry ownership or its deadline: %v", err)
	}
	cluster.settings.startupTimeout = time.Second
	if err := cluster.RemoveMonitor(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	if control.removals != 1 || control.copies != 1 || daemon.terminations != 2 || !cluster.monitorTerminated {
		t.Fatal("retry repeated committed membership removal or skipped owned cleanup")
	}
}

func TestMonitorRemovalConfigCopyFailureRetainsOwnershipForRetry(t *testing.T) {
	control := &monitorLifecycleControl{members: []string{"a", "b", "c"}, quorum: []string{"a", "b", "c"}, copyErr: errors.New("temporary config copy failure")}
	cluster := monitorLifecycleFixture(control)
	daemon := &monitorLifecycleDaemon{}
	cluster.monitors["b"] = &MonitorContainer{Container: daemon, DaemonName: "b"}
	before := bytes.Clone(cluster.config)
	if err := cluster.RemoveMonitor(t.Context(), "b"); !errors.Is(err, control.copyErr) || cluster.monitors["b"] == nil || control.removals != 1 || !bytes.Equal(cluster.config, before) {
		t.Fatalf("failed copy published config or lost removed member ownership: %v", err)
	}
	control.copyErr = nil
	if err := cluster.RemoveMonitor(t.Context(), "b"); err != nil {
		t.Fatal(err)
	}
	if cluster.monitors["b"] != nil || control.removals != 1 || daemon.terminations != 2 || control.copies != 2 || strings.Count(string(cluster.config), "[v2:") != 2 {
		t.Fatal("copy retry repeated native mutation or failed to publish the current map")
	}
}

func TestMonitorRemovalExpiredContextDoesNotMutate(t *testing.T) {
	for _, scenario := range []string{"zero startup timeout", "canceled parent"} {
		t.Run(scenario, func(t *testing.T) {
			control := &monitorLifecycleControl{members: []string{"a", "b", "c"}, quorum: []string{"a", "b", "c"}}
			cluster := monitorLifecycleFixture(control)
			cluster.monitors["b"] = &MonitorContainer{DaemonName: "b"}
			ctx := t.Context()
			if scenario == "zero startup timeout" {
				cluster.settings.startupTimeout = 0
			} else {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			if err := cluster.RemoveMonitor(ctx, "b"); err == nil || len(control.calls) != 0 || control.removals != 0 || control.copies != 0 || cluster.monitors["b"] == nil {
				t.Fatal("expired operation changed membership, ownership or config")
			}
		})
	}
}

func TestAddMonitorRejectsForeignMembershipBeforeCreatingResources(t *testing.T) {
	control := &monitorLifecycleControl{members: []string{"a", "external"}, quorum: []string{"a", "external"}}
	cluster := monitorLifecycleFixture(control)
	mon, err := cluster.AddMonitor(t.Context(), "external")
	if err == nil || mon != nil || cluster.monitors["external"] != nil || cluster.controlPlane != nil {
		t.Fatal("existing native member was adopted or caused control-plane creation")
	}
	if !slices.Equal(control.calls, []string{"quorum_status --format json"}) {
		t.Fatalf("existing member preflight read keys or created resources: %v", control.calls)
	}
}

func monitorLifecycleFixture(control *monitorLifecycleControl) *Container {
	control.config = []byte("[global]\nmon host = old\n")
	return &Container{Container: control, monitors: make(map[string]*MonitorContainer),
		settings: options{startupTimeout: time.Second}, config: bytes.Clone(control.config)}
}

type monitorLifecycleDaemon struct {
	testcontainers.Container
	terminateErr error
	terminations int
}

func (daemon *monitorLifecycleDaemon) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	daemon.terminations++
	return daemon.terminateErr
}

func (daemon *monitorLifecycleDaemon) GetContainerID() string { return "removed-monitor" }

type monitorLifecycleControl struct {
	testcontainers.Container
	members, quorum, calls []string
	removals, copies       int
	afterRemoval           []monitorLifecycleRead
	copyErr                error
	config                 []byte
}

type monitorLifecycleRead struct {
	timeout         bool
	members, quorum []string
}

func (control *monitorLifecycleControl) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	call := strings.Join(args[3:], " ")
	control.calls = append(control.calls, call)
	var output []byte
	exitCode := 0
	switch {
	case call == "quorum_status --format json":
		nativeMembers, nativeQuorum := control.members, control.quorum
		if control.removals != 0 && len(control.afterRemoval) != 0 {
			read := control.afterRemoval[0]
			control.afterRemoval = control.afterRemoval[1:]
			if read.timeout {
				output, exitCode = []byte("timed out"), 1
				break
			}
			if read.members != nil {
				nativeMembers = read.members
			}
			if read.quorum != nil {
				nativeQuorum = read.quorum
			}
		}
		members := make([]map[string]any, 0, len(nativeMembers))
		for _, name := range nativeMembers {
			members = append(members, map[string]any{"name": name, "public_addrs": map[string]any{"addrvec": []map[string]string{{"type": "v2", "addr": "127.0.0.1:3300/0"}}}})
		}
		output, _ = json.Marshal(map[string]any{"quorum_names": nativeQuorum, "monmap": map[string]any{"mons": members}})
	case strings.HasPrefix(call, "mon remove "):
		name := strings.TrimPrefix(call, "mon remove ")
		control.members = slices.DeleteFunc(control.members, func(member string) bool { return member == name })
		control.quorum = slices.DeleteFunc(control.quorum, func(member string) bool { return member == name })
		control.removals++
	default:
		return 0, nil, errors.New("unexpected monitor CLI call: " + call)
	}
	var stream bytes.Buffer
	header := make([]byte, 8)
	header[0] = 1
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	stream.Write(header)
	stream.Write(output)
	return exitCode, &stream, nil
}

func (control *monitorLifecycleControl) GetContainerID() string { return "control" }

func (control *monitorLifecycleControl) CopyFileFromContainer(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(control.config)), nil
}

func (control *monitorLifecycleControl) CopyToContainer(_ context.Context, data []byte, _ string, _ int64) error {
	control.copies++
	if control.copyErr == nil {
		control.config = bytes.Clone(data)
	}
	return control.copyErr
}
