package multicluster

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/internal/cluster"
	"github.com/moby/moby/api/types/container"
	mobynet "github.com/moby/moby/api/types/network"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

type cephFSManagerCandidate struct {
	testcontainers.Container
	id      string
	running bool
}

func (candidate *cephFSManagerCandidate) GetContainerID() string { return candidate.id }
func (candidate *cephFSManagerCandidate) IsRunning() bool        { return candidate.running }

func TestCephFSManagerCandidatesSurviveInitialManagerRemoval(t *testing.T) {
	b := &ceph.ManagerContainer{DaemonName: "b", Container: &cephFSManagerCandidate{id: "manager-b", running: true}}
	c := &ceph.ManagerContainer{DaemonName: "c", Container: &cephFSManagerCandidate{id: "manager-c"}}
	status := ceph.ManagerStatus{Available: true, ActiveName: "b"}
	ids, err := selectCephFSManagerCandidates(status, []*ceph.ManagerContainer{nil, b, c, {DaemonName: "partial"}})
	if err != nil || !reflect.DeepEqual(ids, []string{"manager-b", "manager-c"}) {
		t.Fatalf("surviving active and stopped standby candidates: ids=%v error=%v", ids, err)
	}
	for name, configure := range map[string]func(*ceph.ManagerStatus){
		"unavailable":    func(s *ceph.ManagerStatus) { s.Available = false },
		"foreign active": func(s *ceph.ManagerStatus) { s.ActiveName = "foreign" },
		"stopped active": func(s *ceph.ManagerStatus) { s.ActiveName = "c" },
	} {
		t.Run(name, func(t *testing.T) {
			modified := status
			configure(&modified)
			if _, err := selectCephFSManagerCandidates(modified, []*ceph.ManagerContainer{b, c}); err == nil {
				t.Fatal("accepted a topology without a live owned active manager")
			}
		})
	}
}

func TestCephFSManagerCandidatesWaitForNativeOwnedActiveRecovery(t *testing.T) {
	b := &ceph.ManagerContainer{DaemonName: "b", Container: &cephFSManagerCandidate{id: "manager-b", running: true}}
	c := &ceph.ManagerContainer{DaemonName: "c", Container: &cephFSManagerCandidate{id: "manager-c", running: true}}
	calls := 0
	ids, err := waitForCephFSManagerCandidates(t.Context(), time.Millisecond, func(context.Context) (ceph.ManagerStatus, []*ceph.ManagerContainer, error) {
		calls++
		managers := []*ceph.ManagerContainer{b, c}
		switch calls {
		case 1:
			return ceph.ManagerStatus{ActiveName: "b"}, managers, nil
		case 2:
			return ceph.ManagerStatus{}, managers, errors.New("transient manager map query failure")
		case 3:
			return ceph.ManagerStatus{Available: true, ActiveName: "foreign", ActiveGID: 42}, managers, nil
		default:
			return ceph.ManagerStatus{Available: true, ActiveName: "b", ActiveGID: 43}, managers, nil
		}
	})
	if err != nil || calls != 4 || !reflect.DeepEqual(ids, []string{"manager-b", "manager-c"}) {
		t.Fatalf("native readiness was skipped or transient state was treated as final: ids=%v calls=%d error=%v", ids, calls, err)
	}
}

func TestCephFSManagerCandidatesWaitHonorsContextAndReportsLastNativeState(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	_, err := waitForCephFSManagerCandidates(ctx, time.Millisecond, func(context.Context) (ceph.ManagerStatus, []*ceph.ManagerContainer, error) {
		calls++
		cancel()
		return ceph.ManagerStatus{ActiveName: "b", ActiveGID: 77}, nil, nil
	})
	if calls != 1 || !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), `available=false active="b" gid=77`) {
		t.Fatalf("wait did not preserve cancellation and final native observation: calls=%d error=%v", calls, err)
	}
	deadlineCtx, deadlineCancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer deadlineCancel()
	_, err = waitForCephFSManagerCandidates(deadlineCtx, time.Millisecond, func(context.Context) (ceph.ManagerStatus, []*ceph.ManagerContainer, error) {
		t.Fatal("expired caller context still queried the manager")
		return ceph.ManagerStatus{}, nil, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait exceeded caller deadline: %v", err)
	}
}

type cephFSManagerDockerFake struct {
	attachments   map[string]bool
	connectErr    map[string]error
	disconnectErr map[string]error
	inspectErr    map[string]error
	connects      []string
	disconnects   []string
	closeCalls    int
	closeErr      error
	applyOnError  bool
}

func (docker *cephFSManagerDockerFake) ContainerInspect(_ context.Context, id string, _ mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error) {
	if err := docker.inspectErr[id]; err != nil {
		return mobycl.ContainerInspectResult{}, err
	}
	attached, exists := docker.attachments[id]
	if !exists {
		return mobycl.ContainerInspectResult{}, errdefs.ErrNotFound
	}
	settings := &container.NetworkSettings{Networks: make(map[string]*mobynet.EndpointSettings)}
	if attached {
		settings.Networks["peer-network"] = &mobynet.EndpointSettings{}
	}
	return mobycl.ContainerInspectResult{Container: container.InspectResponse{NetworkSettings: settings}}, nil
}

func (docker *cephFSManagerDockerFake) NetworkConnect(_ context.Context, _ string, options mobycl.NetworkConnectOptions) (mobycl.NetworkConnectResult, error) {
	docker.connects = append(docker.connects, options.Container)
	err := docker.connectErr[options.Container]
	if err == nil || docker.applyOnError {
		docker.attachments[options.Container] = true
	}
	return mobycl.NetworkConnectResult{}, err
}

func (docker *cephFSManagerDockerFake) NetworkDisconnect(_ context.Context, _ string, options mobycl.NetworkDisconnectOptions) (mobycl.NetworkDisconnectResult, error) {
	docker.disconnects = append(docker.disconnects, options.Container)
	err := docker.disconnectErr[options.Container]
	if err == nil {
		docker.attachments[options.Container] = false
	}
	return mobycl.NetworkDisconnectResult{}, err
}

func (docker *cephFSManagerDockerFake) Close() error {
	docker.closeCalls++
	return docker.closeErr
}

func newCephFSManagerNetworkingFake() (*cephFSManagerNetworking, *cephFSManagerDockerFake) {
	docker := &cephFSManagerDockerFake{attachments: make(map[string]bool), connectErr: make(map[string]error), disconnectErr: make(map[string]error), inspectErr: make(map[string]error)}
	return &cephFSManagerNetworking{docker: docker, network: "peer-network", owned: make(map[string]bool)}, docker
}

func TestCephFSManagerNetworkReconciliationPreservesCallerAttachments(t *testing.T) {
	networking, docker := newCephFSManagerNetworkingFake()
	docker.attachments = map[string]bool{"active": false, "standby": true, "late-candidate": false}
	if err := networking.attach(t.Context(), []string{"active", "standby"}); err != nil {
		t.Fatal(err)
	}
	if err := networking.attach(t.Context(), []string{"active", "standby", "late-candidate"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(docker.connects, []string{"active", "late-candidate"}) {
		t.Fatalf("reconciliation reconnected an existing endpoint: %v", docker.connects)
	}
	if err := networking.cleanup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !docker.attachments["standby"] || docker.attachments["active"] || docker.attachments["late-candidate"] {
		t.Fatalf("cleanup changed attachment ownership: %+v", docker.attachments)
	}
	if !reflect.DeepEqual(docker.disconnects, []string{"active", "late-candidate"}) || docker.closeCalls != 1 {
		t.Fatalf("cleanup did not remove precisely owned endpoints: disconnects=%v closes=%d", docker.disconnects, docker.closeCalls)
	}
	if err := networking.cleanup(t.Context()); err != nil || docker.closeCalls != 1 {
		t.Fatal("completed networking cleanup was not idempotent")
	}
	if err := networking.attach(t.Context(), []string{"standby"}); err == nil {
		t.Fatal("closed networking accepted a new topology mutation")
	}
}

func TestCephFSManagerNetworkCleanupRetainsClientAndRetriesFailedEndpoint(t *testing.T) {
	networking, docker := newCephFSManagerNetworkingFake()
	docker.attachments = map[string]bool{"a": false, "b": false}
	if err := networking.attach(t.Context(), []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	docker.disconnectErr["a"] = errors.New("temporary disconnect failure")
	if err := networking.cleanup(t.Context()); err == nil || docker.closeCalls != 0 {
		t.Fatal("cleanup lost failure or closed the client before disconnect retry")
	}
	if !docker.attachments["a"] || docker.attachments["b"] || len(networking.owned) != 1 {
		t.Fatalf("failure prevented cleanup of another endpoint: %+v", docker.attachments)
	}
	delete(docker.disconnectErr, "a")
	if err := networking.cleanup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(docker.disconnects, []string{"a", "b", "a"}) || docker.closeCalls != 1 {
		t.Fatalf("retry repeated successful actions: disconnects=%v closes=%d", docker.disconnects, docker.closeCalls)
	}
}

func TestCephFSManagerNetworkTracksUncertainConnectAndRemovedManagers(t *testing.T) {
	networking, docker := newCephFSManagerNetworkingFake()
	docker.attachments = map[string]bool{"uncertain": false, "removed": false, "detached": false}
	if err := networking.attach(t.Context(), []string{"removed", "detached"}); err != nil {
		t.Fatal(err)
	}
	docker.applyOnError = true
	docker.connectErr["uncertain"] = errors.New("response lost after Docker applied connection")
	if err := networking.attach(t.Context(), []string{"uncertain"}); err == nil || !networking.owned["uncertain"] {
		t.Fatal("uncertain connection was omitted from partial cleanup")
	}
	delete(docker.attachments, "removed")
	docker.attachments["detached"] = false
	docker.closeErr = errors.New("close failure")
	if err := networking.cleanup(t.Context()); err == nil {
		t.Fatal("Docker close failure was lost")
	}
	if !reflect.DeepEqual(docker.disconnects, []string{"uncertain"}) || len(networking.owned) != 0 {
		t.Fatalf("removed or detached managers prevented cleanup: disconnects=%v owned=%v", docker.disconnects, networking.owned)
	}
	docker.closeErr = nil
	if err := networking.cleanup(t.Context()); err != nil || docker.closeCalls != 2 || len(docker.disconnects) != 1 {
		t.Fatal("close retry repeated completed endpoint cleanup")
	}
}

func TestCephFSManagerReconciliationRefusesUninitializedAndTerminatedMirrors(t *testing.T) {
	if err := (&CephFSMirror{}).AttachManagers(t.Context()); err == nil {
		t.Fatal("uninitialized mirror accepted reconciliation")
	}
	mirror := &CephFSMirror{}
	if err := mirror.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := mirror.AttachManagers(t.Context()); err == nil {
		t.Fatal("terminated mirror accepted reconciliation")
	}
}
