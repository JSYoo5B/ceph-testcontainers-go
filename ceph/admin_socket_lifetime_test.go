package ceph

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"io"
	"strings"
	"testing"
)

// adminLifetimeModel emits supervisor replies without running a process.
// Its callback observes admission invariants while an Exec is in progress.
type adminLifetimeModel struct {
	testcontainers.Container
	during                     func()
	fail, killed, refuseCancel bool
	cancels                    int
}

func (m *adminLifetimeModel) GetContainerID() string { return "admin-control" }
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
	raw := base64.StdEncoding.EncodeToString([]byte(`{"version":"20.2.4"}`))
	return 0, strings.NewReader(fmt.Sprintf(`{"schema_version":1,"stdout":%q,"stderr":"","exit_code":0,"timed_out":false,"stdout_truncated":false,"stderr_truncated":false}`, raw)), nil
}
func TestAdminSocketHoldsAdmissionThroughDispatch(t *testing.T) {
	for _, args := range [][]string{{"version"}, {"status"}, {"config", "set", "debug_osd", "1"}} {
		t.Run(strings.Join(args, "-"), func(t *testing.T) {
			c, _ := adminSocketCluster()
			control := &adminLifetimeModel{}
			c.Container = control
			handle := c.fleet.osdHandles.get(0)
			handle.owner = c
			handle.Container = &adminLifetimeModel{}
			control.during = func() {
				if c.mu.TryLock() {
					c.mu.Unlock()
					t.Error("topology admission released before command completion")
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
func TestAdminSocketRetainsUnconfirmedCleanup(t *testing.T) {
	for _, killed := range []bool{false, true} {
		for _, refused := range []bool{false, true} {
			t.Run(fmt.Sprintf("killed=%t/refused=%t", killed, refused), func(t *testing.T) {
				c, _ := adminSocketCluster()
				control := &adminLifetimeModel{fail: true, killed: killed, refuseCancel: refused}
				c.Container = control
				handle := c.fleet.osdHandles.get(0)
				handle.owner = c
				handle.Container = &adminLifetimeModel{}
				_, err := handle.AdminSocket(t.Context(), "status")
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
