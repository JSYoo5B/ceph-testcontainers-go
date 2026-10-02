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
	return &Container{Container: control, monitors: make(map[string]*MonitorContainer),
		settings: options{startupTimeout: time.Second}, config: []byte("mon host = old\n")}
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

type monitorLifecycleControl struct {
	testcontainers.Container
	members, quorum, calls []string
	removals, copies       int
}

func (control *monitorLifecycleControl) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	call := strings.Join(args[3:], " ")
	control.calls = append(control.calls, call)
	var output []byte
	switch {
	case call == "quorum_status --format json":
		members := make([]map[string]any, 0, len(control.members))
		for _, name := range control.members {
			members = append(members, map[string]any{"name": name, "public_addrs": map[string]any{"addrvec": []map[string]string{{"type": "v2", "addr": "127.0.0.1:3300/0"}}}})
		}
		output, _ = json.Marshal(map[string]any{"quorum_names": control.quorum, "monmap": map[string]any{"mons": members}})
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
	return 0, &stream, nil
}

func (control *monitorLifecycleControl) CopyToContainer(context.Context, []byte, string, int64) error {
	control.copies++
	return nil
}
