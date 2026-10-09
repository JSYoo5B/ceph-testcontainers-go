package multicluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/internal/cluster"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

func TestMirrorInitialDaemonCountContracts(t *testing.T) {
	for _, protocol := range []string{"rbd", "cephfs"} {
		for _, tc := range []struct {
			name   string
			count  int
			zero   bool
			want   int
			reject bool
		}{
			{"legacy-default", 0, false, 1, false},
			{"legacy-positive", 3, false, 3, false},
			{"explicit-zero", 0, true, 0, false},
			{"zero-positive-conflict", 1, true, 0, true},
			{"zero-many-conflict", 3, true, 0, true},
			{"negative-legacy", -1, false, 0, true},
			{"negative-zero", -1, true, 0, true},
		} {
			t.Run(protocol+"/"+tc.name, func(t *testing.T) {
				var count int
				var err error
				if protocol == "rbd" {
					var normalized RBDMirrorConfig
					normalized, err = normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: "rbd", DaemonCount: tc.count, NoInitialDaemons: tc.zero})
					count = normalized.DaemonCount
					if err == nil && (normalized.SourceSite != "source" || normalized.DestinationSite != "destination" || normalized.Mode != RBDMirrorModeSnapshot || normalized.Scope != RBDMirrorScopeImage || normalized.NoInitialDaemons != tc.zero) {
						t.Fatal("count option changed existing RBD policy defaults")
					}
				} else {
					var normalized CephFSMirrorConfig
					original := []string{"/data"}
					normalized, err = normalizeCephFSMirrorConfig(CephFSMirrorConfig{SourceFilesystem: "source", DestinationFilesystem: "destination", Directories: original, DaemonCount: tc.count, NoInitialDaemons: tc.zero})
					count = normalized.DaemonCount
					if err == nil && (normalized.DestinationSite != "destination" || len(normalized.Directories) != 1 || normalized.Directories[0] != "/data" || normalized.NoInitialDaemons != tc.zero) {
						t.Fatal("count option changed existing CephFS directory/site defaults")
					}
					if err == nil {
						normalized.Directories[0] = "/caller-copy"
						if original[0] != "/data" {
							t.Fatal("normalization aliased caller directories")
						}
					}
				}
				if tc.reject != (err != nil) || err == nil && count != tc.want {
					t.Fatalf("count admission: got=%d want=%d reject=%t err=%v", count, tc.want, tc.reject, err)
				}
			})
		}
	}
}

type mirrorInitialAdmissionContainer struct {
	testcontainers.Container
	calls *atomic.Int32
}

func (c *mirrorInitialAdmissionContainer) GetContainerID() string {
	c.calls.Add(1)
	return "unexpected-control-query"
}

func TestMirrorInitialDaemonRejectionPrecedesRuntimeAndCustomization(t *testing.T) {
	for _, protocol := range []string{"rbd", "cephfs"} {
		for _, admission := range []struct {
			count int
			zero  bool
		}{{-1, false}, {-1, true}, {1, true}, {3, true}} {
			count, zero := admission.count, admission.zero
			t.Run(fmt.Sprintf("%s/count-%d/zero-%t", protocol, count, zero), func(t *testing.T) {
				var controlCalls, customizerCalls, factoryCalls atomic.Int32
				source := &ceph.Container{Container: &mirrorInitialAdmissionContainer{calls: &controlCalls}}
				destination := &ceph.Container{Container: &mirrorInitialAdmissionContainer{calls: &controlCalls}}
				customizer := testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error {
					customizerCalls.Add(1)
					t.Error("rejected topology applied daemon customizer")
					return errors.New("unexpected customization")
				})
				var nonnil bool
				var err error
				if protocol == "rbd" {
					fixture, e := RunRBDMirror(t.Context(), "example/ceph:version", RBDMirrorConfig{Source: source, Destination: destination, Pool: "rbd", DaemonCount: count, NoInitialDaemons: zero}, customizer)
					nonnil, err = fixture != nil, e
				} else {
					fixture, e := RunCephFSMirror(t.Context(), "example/ceph:version", CephFSMirrorConfig{Source: source, Destination: destination, SourceFilesystem: "source", DestinationFilesystem: "destination", Directories: []string{"/data"}, DaemonCount: count, NoInitialDaemons: zero, OriginalProcessClientFactory: func(context.Context) (*mobycl.Client, error) {
						factoryCalls.Add(1)
						return nil, errors.New("unexpected factory")
					}}, customizer)
					nonnil, err = fixture != nil, e
				}
				if err == nil || nonnil || controlCalls.Load() != 0 || customizerCalls.Load() != 0 || factoryCalls.Load() != 0 {
					t.Fatalf("invalid initial topology reached runtime: result=%t err=%v control=%d customizer=%d factory=%d", nonnil, err, controlCalls.Load(), customizerCalls.Load(), factoryCalls.Load())
				}
				if count > 0 && !strings.Contains(err.Error(), "NoInitialDaemons") {
					t.Fatal("conflict hidden by unrelated pair validation")
				}
			})
		}
	}
}

func TestMirrorInitialDaemonCanceledAdmissionHasNoRuntimeEffects(t *testing.T) {
	for _, protocol := range []string{"rbd", "cephfs"} {
		for _, tc := range []struct{ deadline, zero bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
			deadline, zero := tc.deadline, tc.zero
			t.Run(fmt.Sprintf("%s/%s/zero-%t", protocol, map[bool]string{false: "canceled", true: "deadline"}[deadline], zero), func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				wanted := context.Canceled
				if deadline {
					cancel()
					ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
					wanted = context.DeadlineExceeded
				} else {
					cancel()
				}
				defer cancel()
				var native, customized, factory atomic.Int32
				source := &ceph.Container{Container: &mirrorInitialAdmissionContainer{calls: &native}}
				destination := &ceph.Container{Container: &mirrorInitialAdmissionContainer{calls: &native}}
				customizer := testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customized.Add(1); return nil })
				var err error
				var nonnil bool
				if protocol == "rbd" {
					result, e := RunRBDMirror(ctx, "example/ceph:version", RBDMirrorConfig{Source: source, Destination: destination, Pool: "rbd", NoInitialDaemons: zero}, customizer)
					nonnil, err = result != nil, e
				} else {
					result, e := RunCephFSMirror(ctx, "example/ceph:version", CephFSMirrorConfig{Source: source, Destination: destination, SourceFilesystem: "source", DestinationFilesystem: "destination", Directories: []string{"/data"}, NoInitialDaemons: zero, OriginalProcessClientFactory: func(context.Context) (*mobycl.Client, error) {
						factory.Add(1)
						return nil, errors.New("unexpected factory")
					}}, customizer)
					nonnil, err = result != nil, e
				}
				if !errors.Is(err, wanted) || nonnil || native.Load() != 0 || customized.Load() != 0 || factory.Load() != 0 {
					t.Fatalf("canceled count admission crossed runtime: result=%t err=%v native=%d customizer=%d factory=%d", nonnil, err, native.Load(), customized.Load(), factory.Load())
				}
			})
		}
	}
}

func TestMirrorInitialZeroRBDObserverDoesNotLaunchDaemons(t *testing.T) {
	h := newReceiverHarness(t, "", "", 0)
	config := h.m.config
	config.NoInitialDaemons = true
	var err error
	h.m.config, err = normalizeRBDMirrorConfig(config)
	if err != nil || h.m.config.DaemonCount != 0 {
		t.Fatalf("explicit zero fixture normalization: %+v %v", h.m.config, err)
	}
	status, err := h.m.ReceiverStatus(t.Context())
	if err != nil || status.Ready || status.PeerID != receiverPeerUUID || len(status.Daemons) != 0 || h.m.Container != nil {
		t.Fatalf("zero observer adopted process: %+v %v", status, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	status, err = h.m.WaitReceiverReady(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || status.Ready || status.PeerID != receiverPeerUUID || len(h.m.Daemons()) != 0 || h.m.Container != nil {
		t.Fatalf("zero wait changed topology: %+v %v", status, err)
	}
}
