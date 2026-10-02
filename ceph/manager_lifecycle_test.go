package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestManagerRemovalPreflightDoesNotMutate(t *testing.T) {
	for _, scenario := range []string{"invalid", "closed", "unowned", "last", "unowned auth", "partial replacement", "stopped replacement", "foreign replacement"} {
		t.Run(scenario, func(t *testing.T) {
			cluster, control, target, replacement := managerLifecycleFixture()
			name := "a"
			switch scenario {
			case "invalid":
				name = "--manager"
			case "closed":
				cluster.closed = true
			case "unowned":
				name = "external"
			case "last":
				delete(cluster.managers, "b")
			case "unowned auth":
				cluster.managers["a"].authOwned = false
			case "partial replacement":
				cluster.managers["b"].Container = nil
			case "stopped replacement":
				replacement.running = false
			case "foreign replacement":
				control.status = `{"active_name":"a","active_gid":101,"available":true,"standbys":[{"name":"external","gid":303}]}`
			}
			if err := cluster.RemoveManager(t.Context(), name); err == nil {
				t.Fatal("unsafe removal was accepted")
			}
			if target.terminations != 0 || replacement.terminations != 0 {
				t.Fatal("preflight terminated a manager")
			}
			for _, call := range control.calls {
				if call != "mgr dump --format json" {
					t.Fatalf("preflight mutated Ceph: %v", control.calls)
				}
			}
		})
	}
}

func TestManagerActiveRemovalWaitsForAvailableReplacement(t *testing.T) {
	cluster, control, target, replacement := managerLifecycleFixture()
	control.onFail = func() {
		control.status = `{"active_name":"b","active_gid":202,"available":false,"standbys":[]}`
		control.onDump = func() {
			control.onDump = nil
			control.status = `{"active_name":"b","active_gid":202,"available":true,"standbys":[]}`
		}
	}
	if err := cluster.RemoveManager(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(control.events, []string{"terminate:a", "mgr fail 101", "starting:b", "auth del mgr.a"}) {
		t.Fatalf("active removal skipped shutdown or initialized replacement: %v", control.events)
	}
	if target.terminations != 1 || replacement.terminations != 0 || cluster.ManagerContainer() != nil {
		t.Fatal("primary manager cleanup changed another daemon or retained its primary pointer")
	}
	if got := cluster.Managers(); len(got) != 1 || got[0].DaemonName != "b" {
		t.Fatalf("unexpected remaining ownership: %+v", got)
	}
}

func TestManagerStandbyRemovalUsesObservedGIDForNumericName(t *testing.T) {
	cluster, control, target, replacement := managerLifecycleFixture()
	cluster.managers["42"] = cluster.managers["b"]
	cluster.managers["42"].DaemonName = "42"
	delete(cluster.managers, "b")
	replacement.name = "42"
	control.status = `{"active_name":"a","active_gid":101,"available":true,"standbys":[{"name":"42","gid":202}]}`
	control.onFail = func() { control.status = `{"active_name":"a","active_gid":101,"available":true,"standbys":[]}` }
	if err := cluster.RemoveManager(t.Context(), "42"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(control.events, []string{"terminate:42", "mgr fail 202", "auth del mgr.42"}) || target.terminations != 0 || cluster.ManagerContainer() != target {
		t.Fatalf("standby removal damaged active ownership or interpreted its name as a GID: %v", control.events)
	}
}

func TestManagerRemovalFailureRetainsHandleAndAuthForRetry(t *testing.T) {
	for _, failure := range []string{"terminate", "mgr fail 101", "auth del mgr.a", "unavailable"} {
		t.Run(failure, func(t *testing.T) {
			cluster, control, target, _ := managerLifecycleFixture()
			if failure == "terminate" {
				target.terminateErr = errors.New("Docker teardown failed")
			} else if failure == "unavailable" {
				cluster.settings.startupTimeout = 20 * time.Millisecond
				control.onFail = func() { control.status = `{"active_name":"b","active_gid":202,"available":false,"standbys":[]}` }
			} else {
				control.fail = failure
			}
			err := cluster.RemoveManager(t.Context(), "a")
			if err == nil || cluster.managers["a"] == nil || cluster.ManagerContainer() != target {
				t.Fatalf("failed removal lost cleanup ownership: %v", err)
			}
			if strings.Contains(err.Error(), "PRIVATE-MANAGER-KEY") {
				t.Fatal("auth command exposed credential output")
			}
			if failure != "auth del mgr.a" && slices.Contains(control.calls, "auth del mgr.a") {
				t.Fatal("auth was removed before successful replacement")
			}
			previousTerminations := target.terminations
			target.terminateErr, control.fail = nil, ""
			cluster.settings.startupTimeout = time.Second
			control.onFail = func() { control.status = `{"active_name":"b","active_gid":202,"available":true,"standbys":[]}` }
			if failure == "unavailable" {
				control.onFail()
			}
			if err := cluster.RemoveManager(t.Context(), "a"); err != nil {
				t.Fatal(err)
			}
			wantTerminations := previousTerminations
			if failure == "terminate" {
				wantTerminations++
			}
			if target.terminations != wantTerminations || cluster.managers["a"] != nil || cluster.ManagerContainer() != nil {
				t.Fatal("retry repeated completed Docker teardown or retained removed ownership")
			}
		})
	}
}

func TestAddManagerRejectsExistingForeignIdentity(t *testing.T) {
	for _, scenario := range []string{"existing auth", "existing registration", "auth command error", "invalid auth JSON"} {
		t.Run(scenario, func(t *testing.T) {
			cluster, control, _, _ := managerLifecycleFixture()
			control.authList = `{"auth_dump":[]}`
			switch scenario {
			case "existing auth":
				control.authList = `{"auth_dump":[{"entity":"mgr.external","key":"PRIVATE-MANAGER-KEY"}]}`
			case "existing registration":
				control.status = `{"active_name":"external","active_gid":303,"available":true,"standbys":[]}`
			case "auth command error":
				control.fail = "auth ls --format json"
			case "invalid auth JSON":
				control.authList = `{"key":"PRIVATE-MANAGER-KEY"}`
			}
			mgr, err := cluster.AddManager(t.Context(), "external")
			if mgr != nil || err == nil || strings.Contains(err.Error(), "PRIVATE-MANAGER-KEY") {
				t.Fatalf("foreign identity was adopted or leaked: manager=%v error=%v", mgr, err)
			}
			for _, call := range control.calls {
				if call != "auth ls --format json" && call != "mgr dump --format json" {
					t.Fatalf("foreign manager preflight mutated Ceph: %v", control.calls)
				}
			}
		})
	}
}

func managerLifecycleFixture() (*Container, *managerLifecycleControl, *managerLifecycleDaemon, *managerLifecycleDaemon) {
	control := &managerLifecycleControl{status: `{"active_name":"a","active_gid":101,"available":true,"standbys":[{"name":"b","gid":202}]}`}
	target := &managerLifecycleDaemon{name: "a", running: true, control: control}
	replacement := &managerLifecycleDaemon{name: "b", running: true, control: control}
	cluster := &Container{Container: control, manager: target, settings: options{startupTimeout: time.Second},
		managers: map[string]*ManagerContainer{
			"a": {Container: target, DaemonName: "a", authOwned: true},
			"b": {Container: replacement, DaemonName: "b", authOwned: true},
		}}
	control.onFail = func() { control.status = `{"active_name":"b","active_gid":202,"available":true,"standbys":[]}` }
	return cluster, control, target, replacement
}

type managerLifecycleDaemon struct {
	testcontainers.Container
	name         string
	running      bool
	terminations int
	terminateErr error
	control      *managerLifecycleControl
}

func (daemon *managerLifecycleDaemon) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	daemon.terminations++
	daemon.control.events = append(daemon.control.events, "terminate:"+daemon.name)
	if daemon.terminateErr == nil {
		daemon.running = false
	}
	return daemon.terminateErr
}

func (daemon *managerLifecycleDaemon) State(context.Context) (*container.State, error) {
	return &container.State{Running: daemon.running}, nil
}

type managerLifecycleControl struct {
	testcontainers.Container
	status, authList, fail string
	calls, events          []string
	onFail, onDump         func()
}

func (control *managerLifecycleControl) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	call := strings.Join(args[3:], " ")
	control.calls = append(control.calls, call)
	output, code := "", 0
	switch {
	case call == control.fail:
		output, code = "injected manager command failure", 1
		if strings.HasPrefix(call, "auth ") {
			output = "PRIVATE-MANAGER-KEY"
		}
	case call == "mgr dump --format json":
		output = control.status
		if strings.Contains(output, `"available":false`) {
			control.events = append(control.events, "starting:b")
		}
		if control.onDump != nil {
			control.onDump()
		}
	case call == "auth ls --format json":
		output = control.authList
	case strings.HasPrefix(call, "mgr fail "):
		control.events = append(control.events, call)
		control.onFail()
	case strings.HasPrefix(call, "auth del "):
		control.events = append(control.events, call)
	}
	var stream bytes.Buffer
	var header [8]byte
	header[0] = 1
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	stream.Write(header[:])
	stream.WriteString(output)
	return code, &stream, nil
}
