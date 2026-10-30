package ceph

import (
	"context"
	"errors"
	"io"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// observerControl answers `ceph` commands of the control container from a
// table of stdout by command line and records every command it received.
type observerControl struct {
	testcontainers.Container
	mu      sync.Mutex
	replies map[string]string
	calls   []string
	err     error
}

func (ctr *observerControl) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	ctr.mu.Lock()
	defer ctr.mu.Unlock()
	key := strings.Join(args[3:], " ")
	ctr.calls = append(ctr.calls, key)
	if ctr.err != nil {
		return 0, nil, ctr.err
	}
	return 0, healthDetailsFixtureStream(ctr.replies[key]), nil
}

func (ctr *observerControl) commands() []string {
	ctr.mu.Lock()
	defer ctr.mu.Unlock()
	return slices.Clone(ctr.calls)
}

func (ctr *observerControl) set(command, reply string) {
	ctr.mu.Lock()
	defer ctr.mu.Unlock()
	ctr.replies[command] = reply
}

const (
	observerStatusKey  = "status --format json"
	observerMgrKey     = "mgr dump --format json"
	observerStatusOK   = `{"fsid":"f1","health":{"status":"HEALTH_OK"}}`
	observerStatusWarn = `{"fsid":"f2","health":{"status":"HEALTH_WARN"}}`
	observerMgrJSON    = `{"available":true,"active_name":"a","active_gid":7}`
)

func newObserverCluster() (*Container, *observerControl) {
	ctr := &observerControl{replies: map[string]string{observerStatusKey: observerStatusOK, observerMgrKey: observerMgrJSON}}
	return &Container{Container: ctr}, ctr
}

func TestObserverClusterFormReadsActualStateOnEveryQuery(t *testing.T) {
	cluster, ctr := newObserverCluster()
	observer, err := Observe(cluster)
	if err != nil || observer == nil {
		t.Fatalf("Observe = %v, %v", observer, err)
	}
	if got := ctr.commands(); len(got) != 0 {
		t.Fatalf("construction ran commands: %q", got)
	}
	first, err := observer.Status(t.Context())
	if err != nil || first.FSID != "f1" {
		t.Fatalf("first status = %+v, %v", first, err)
	}
	ctr.set(observerStatusKey, observerStatusWarn)
	second, err := observer.Status(t.Context())
	if err != nil || second.FSID != "f2" || second.Health.Status != "HEALTH_WARN" {
		t.Fatalf("a retained observer returned an earlier result: %+v, %v", second, err)
	}
	mgr, err := observer.ManagerStatus(t.Context())
	if err != nil || !mgr.Available || mgr.ActiveName != "a" || mgr.ActiveGID != 7 {
		t.Fatalf("manager status = %+v, %v", mgr, err)
	}
	if want := []string{observerStatusKey, observerStatusKey, observerMgrKey}; !reflect.DeepEqual(ctr.commands(), want) {
		t.Fatalf("commands = %q, want %q", ctr.commands(), want)
	}
	// The Container method of the same name is unchanged and returns the same.
	direct, err := readStatus(t.Context(), cluster.conn().ceph)
	if err != nil || direct.FSID != second.FSID {
		t.Fatalf("direct status = %+v, %v", direct, err)
	}
}

func TestObserverDaemonAddressesUsesTheDaemonMapCommands(t *testing.T) {
	cluster, ctr := newObserverCluster()
	observer, _ := Observe(cluster)
	// Empty maps are not evidence: the observation fails like the method does.
	_, observerErr := observer.DaemonAddresses(t.Context())
	_, directErr := readDaemonAddresses(t.Context(), cluster.conn().ceph)
	if observerErr == nil || directErr == nil || observerErr.Error() != directErr.Error() {
		t.Fatalf("observer error %v differs from method error %v", observerErr, directErr)
	}
	if got := ctr.commands(); len(got) < 2 || got[0] != "mon dump --format json" {
		t.Fatalf("commands = %q", got)
	}
}

func TestObserverClusterTerminatedKeepsTheCommandPathError(t *testing.T) {
	cluster, ctr := newObserverCluster()
	observer, _ := Observe(cluster)
	if _, err := observer.Status(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctr.mu.Lock()
	ctr.err = errors.New("container is gone")
	ctr.mu.Unlock()
	cluster.mu.Lock()
	cluster.closed = true
	cluster.mu.Unlock()
	status, err := observer.Status(t.Context())
	if err == nil || !strings.Contains(err.Error(), "container is gone") || !reflect.DeepEqual(status, Status{}) {
		t.Fatalf("a terminated cluster answered: %+v, %v", status, err)
	}
	if got := ctr.commands(); len(got) != 2 {
		t.Fatalf("the second query did not reach the command path: %q", got)
	}
}

func TestObserverRefusesNilInputAndUnconstructedObservers(t *testing.T) {
	if o, err := Observe(nil); o != nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Observe(nil) = %v, %v", o, err)
	}
	var nilObserver *Observer
	for name, o := range map[string]*Observer{"nil": nilObserver, "zero": {}} {
		if s, err := o.Status(t.Context()); !errors.Is(err, ErrInvalidRequest) || !reflect.DeepEqual(s, Status{}) {
			t.Errorf("%s observer Status = %+v, %v", name, s, err)
		}
		if _, err := o.ManagerStatus(t.Context()); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s observer ManagerStatus: %v", name, err)
		}
		if _, err := o.DaemonAddresses(t.Context()); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s observer DaemonAddresses: %v", name, err)
		}
	}
}

func TestObserverKeepsTheCallerContext(t *testing.T) {
	cluster, _ := newObserverCluster()
	observer, _ := Observe(cluster)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := observer.Status(ctx); err == nil {
		t.Fatal("a canceled context was ignored")
	}
}
