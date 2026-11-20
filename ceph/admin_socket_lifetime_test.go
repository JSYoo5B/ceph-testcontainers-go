package ceph

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// adminLifetimeModel emits supervisor replies without running a process.
// Its callback observes admission invariants while an Exec is in progress.
type adminLifetimeModel struct {
	testcontainers.Container
	id                         string
	stdout                     string
	during                     func()
	fail, killed, refuseCancel bool
	cancels                    int
}

func (m *adminLifetimeModel) GetContainerID() string { return m.id }
func (m *adminLifetimeModel) State(context.Context) (*container.State, error) {
	return &container.State{Running: true}, nil
}
func (m *adminLifetimeModel) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if args[2] == supervised.CancelScript {
		m.cancels++
		if m.refuseCancel {
			return 0, nil, errors.New("cleanup unavailable")
		}
		return 0, strings.NewReader(""), nil
	}
	if m.during != nil {
		m.during()
	}
	if m.killed {
		return 137, strings.NewReader(""), nil
	}
	if m.fail {
		return 0, nil, errors.New("transport unavailable")
	}
	output := m.stdout
	if output == "" {
		output = `{"version":"20.2.4"}`
	}
	raw := base64.StdEncoding.EncodeToString([]byte(output))
	return 0, strings.NewReader(fmt.Sprintf(`{"schema_version":1,"stdout":%q,"stderr":"","exit_code":0,"timed_out":false,"stdout_truncated":false,"stderr_truncated":false}`, raw)), nil
}
func TestAdminSocketRegistersBeforeDispatchWithoutHoldingTopologyLock(t *testing.T) {
	for _, kind := range []string{"osd", "mon", "mgr"} {
		for _, args := range [][]string{{"version"}, {"status"}, {"config", "set", "debug_ms", "1"}} {
			t.Run(kind+"/"+strings.Join(args, "-"), func(t *testing.T) {
				c, _ := adminSocketCluster()
				control := &adminLifetimeModel{id: "admin-control"}
				daemon := &adminLifetimeModel{id: "daemon-instance"}
				c.Container = control
				var handle interface {
					Version(context.Context) (string, error)
					AdminSocket(context.Context, ...string) ([]byte, error)
				}
				switch kind {
				case "osd":
					osd := c.fleet.osdHandles.get(0)
					osd.owner, osd.Container = c, daemon
					handle = osd
				case "mon":
					mon := &MonitorContainer{Container: daemon, DaemonName: "b", owner: c}
					c.members.roster.addMonitor("b", mon)
					handle = mon
				case "mgr":
					mgr := c.members.roster.manager("a")
					mgr.owner, mgr.Container = c, daemon
					handle = mgr
				}
				control.during = func() {
					if !c.mu.TryLock() {
						t.Error("topology lock held during native command")
					} else {
						c.mu.Unlock()
					}
					if c.native.calls.registered() != 1 {
						t.Error("admin command not registered")
					}
				}
				var err error
				if args[0] == "version" {
					_, err = handle.Version(t.Context())
				} else {
					_, err = handle.AdminSocket(t.Context(), args...)
				}
				if err != nil {
					t.Fatal(err)
				}
				if c.native.calls.registered() != 0 {
					t.Fatal("completed invocation retained")
				}
			})
		}
	}
}
func TestAdminSocketRetainsUnconfirmedCleanup(t *testing.T) {
	for _, killed := range []bool{false, true} {
		for _, refused := range []bool{false, true} {
			t.Run(fmt.Sprintf("killed=%t/refused=%t", killed, refused), func(t *testing.T) {
				c, _ := adminSocketCluster()
				control := &adminLifetimeModel{id: "admin-control", fail: true, killed: killed, refuseCancel: refused}
				c.Container = control
				handle := c.fleet.osdHandles.get(0)
				handle.owner = c
				handle.Container = &adminLifetimeModel{id: "daemon-instance"}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				control.during = cancel
				_, err := handle.AdminSocket(ctx, "status")
				var unstopped *NativeCommandUnstoppedError
				if errors.As(err, &unstopped) != refused || control.cancels != 1 {
					t.Fatalf("cleanup outcome %v cancels %d", err, control.cancels)
				}
				if !refused {
					if c.native.calls.registered() != 0 {
						t.Fatal("confirmed end retained")
					}
					return
				}
				if c.native.calls.registered() != 1 {
					t.Fatal("unconfirmed invocation lost")
				}
				if unstopped.invocation.target != control || unstopped.invocation.targetID != handle.GetContainerID() {
					t.Fatal("cleanup executor and daemon ownership identity were conflated")
				}
				if err := unstopped.Stop(t.Context()); err == nil || c.native.calls.registered() != 1 {
					t.Fatal("failed Stop lost the cleanup obligation")
				}
				if err := c.native.fence(t.Context(), handle.GetContainerID()); err == nil {
					t.Fatal("removal admitted with unconfirmed cleanup")
				}
				control.refuseCancel = false
				if err := unstopped.Stop(t.Context()); err != nil {
					t.Fatal(err)
				}
				if c.native.calls.registered() != 0 {
					t.Fatal("successful Stop did not release invocation")
				}
				if err := c.native.fence(t.Context(), handle.GetContainerID()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestMemberRemovalStopsBeforeUnsettledAdminCommands(t *testing.T) {
	t.Run("monitor", func(t *testing.T) {
		control := &monitorLifecycleControl{members: []string{"a", "b", "c"}, quorum: []string{"a", "b", "c"}}
		c := monitorLifecycleFixture(control)
		daemon := &monitorLifecycleDaemon{}
		handle := &MonitorContainer{Container: daemon, DaemonName: "b"}
		c.members.roster.addMonitor("b", handle)
		topology := c.monitorTopology()
		blocked := errors.New("admin command remains unsettled")
		calls := 0
		topology.fenceNative = func(_ context.Context, id string) error {
			calls++
			if id != daemon.GetContainerID() {
				t.Errorf("wrong daemon instance %q", id)
			}
			return blocked
		}
		if err := topology.retireMonitor(t.Context(), handle); !errors.Is(err, blocked) {
			t.Fatal(err)
		}
		if calls != 1 || daemon.terminations != 0 || control.removals != 0 {
			t.Fatal("removal passed unsettled invocation")
		}
	})
	t.Run("manager", func(t *testing.T) {
		c, control, daemon, _ := managerLifecycleFixture()
		handle := managerHandle(t, t.Context(), c, "a")
		topology := c.monitorTopology()
		blocked := errors.New("admin command remains unsettled")
		calls := 0
		topology.fenceNative = func(_ context.Context, id string) error {
			calls++
			if id != daemon.GetContainerID() {
				t.Errorf("wrong daemon instance %q", id)
			}
			return blocked
		}
		if err := topology.retireManager(t.Context(), handle); !errors.Is(err, blocked) {
			t.Fatal(err)
		}
		if calls != 1 || daemon.terminations != 0 || len(control.events) != 0 {
			t.Fatal("removal passed unsettled invocation")
		}
	})
}
