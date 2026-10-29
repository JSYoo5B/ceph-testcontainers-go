package cephfs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

type startFunc = func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error)

const startMDSCaps = " mon allow profile mds mgr allow profile mds osd allow rw tag cephfs *=* mds allow"

func startMDSNever(t *testing.T) startFunc {
	return func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		t.Error("a refused start reached the service launch")
		return nil, nil
	}
}

func startMDSTopologyFree(t *testing.T, fs *Filesystem) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := servicehook.LockTopology(ctx, fs.cluster); err != nil {
		t.Fatal("the topology mutex is retained after startMDSWithAdmission returned")
	}
	servicehook.Unlock(fs.cluster)
}

// TestStartMDSAdmissionKeepsEachModesRefusalsAndOwnership characterizes the
// admission half of startMDSWithAdmission, which runs under the topology mutex
// and releases it before any auth command: the ownership refusal, the cold-start
// ownership and control checks, the single consumption of cold permission, the
// replacement admission hook, and which refusals reserve a name index. It is a
// Docker-free control, not native evidence.
func TestStartMDSAdmissionKeepsEachModesRefusalsAndOwnership(t *testing.T) {
	const owner = "cephfs cluster is terminated or filesystem ownership changed"
	t.Run("ownership/unregistered-filesystem", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		delete(filesystems(fs.cluster).byName, fs.FilesystemName)
		before := n.mutationCount()
		d, err := fs.startMDSWithAdmission(t.Context(), startMDSNever(t), nil, nil)
		if err == nil || err.Error() != owner || d != nil || fs.coldMDS.attempted || fs.roster.nextIndex != 0 || n.mutationCount() != before {
			t.Fatalf("ownership refusal changed: %v %v", d, err)
		}
		startMDSTopologyFree(t, fs)
	})
	t.Run("cold/identity-drift", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		fs.nativeIdentity = &cephFSNativeIdentity{}
		before := len(n.calls)
		_, err := fs.startMDSWithAdmission(t.Context(), startMDSNever(t), nil, nil)
		if err == nil || err.Error() != "original cold MDS ownership changed before startup" || fs.coldMDS.attempted || fs.roster.nextIndex != 0 || len(n.calls) != before {
			t.Fatalf("cold ownership refusal changed: %v", err)
		}
		startMDSTopologyFree(t, fs)
	})
	t.Run("cold/embedded-container-present", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		fs.Container = &mdsScaleFixtureContainer{id: "foreign"}
		before := len(n.calls)
		_, err := fs.startMDSWithAdmission(t.Context(), startMDSNever(t), nil, nil)
		if err == nil || err.Error() != "original cold MDS ownership changed before startup" || fs.coldMDS.attempted || len(n.calls) != before {
			t.Fatalf("cold ownership refusal changed: %v", err)
		}
		startMDSTopologyFree(t, fs)
	})
	t.Run("cold/selected-control-drift", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		n.fsid = "b011602b-37aa-496b-a8b5-b3e7a5d13ff3"
		before := n.mutationCount()
		_, err := fs.startMDSWithAdmission(t.Context(), startMDSNever(t), nil, nil)
		if err == nil || err.Error() != "original cold cluster identity is invalid or changed" || fs.coldMDS.attempted || fs.roster.nextIndex != 0 || n.mutationCount() != before {
			t.Fatalf("selected control refusal changed: %v", err)
		}
		startMDSTopologyFree(t, fs)
	})
	t.Run("cold/control-read-failure", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		n.fail = "fsid"
		_, err := fs.startMDSWithAdmission(t.Context(), startMDSNever(t), nil, nil)
		if err == nil || !strings.HasPrefix(err.Error(), "read selected initial MDS control identity") || fs.coldMDS.attempted || fs.roster.nextIndex != 0 {
			t.Fatalf("control read failure changed: %v", err)
		}
		startMDSTopologyFree(t, fs)
	})
	t.Run("cold/canceled-after-control-check", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		n.hook = func(cmd string) {
			if cmd == "fsid" {
				cancel()
			}
		}
		before := n.mutationCount()
		_, err := fs.startMDSWithAdmission(ctx, startMDSNever(t), nil, nil)
		if !errors.Is(err, context.Canceled) || fs.coldMDS.attempted || fs.roster.nextIndex != 0 || n.mutationCount() != before {
			t.Fatalf("cancellation before permission consumption changed: %v", err)
		}
		startMDSTopologyFree(t, fs)
	})
	t.Run("cold/permission-consumed-once", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		n.fail = "auth get-or-create mds.cold-target-0" + startMDSCaps
		_, err := fs.startMDSWithAdmission(t.Context(), startMDSNever(t), nil, nil)
		if err == nil || !strings.HasPrefix(err.Error(), "create initial MDS credentials") || strings.Contains(err.Error(), "never-log") || !fs.coldMDS.attempted || fs.roster.nextIndex != 1 {
			t.Fatalf("cold credentials failure changed: %v", err)
		}
		n.fail = ""
		_, err = fs.startMDSWithAdmission(t.Context(), func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
			return nil, errors.New("stop here")
		}, nil, nil)
		if err == nil || err.Error() != "stop here" || fs.roster.nextIndex != 2 {
			t.Fatalf("the consumed cold permission was used again or the second start took the cold path: %v", err)
		}
	})
	t.Run("ordinary/credentials-failure", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		fs.coldMDS = nil
		n.fail = "auth get-or-create mds.cold-target-0" + startMDSCaps
		_, err := fs.startMDSWithAdmission(t.Context(), startMDSNever(t), nil, nil)
		if err == nil || !strings.HasPrefix(err.Error(), "create mds.cold-target-0 credentials: ") || fs.roster.nextIndex != 1 {
			t.Fatalf("ordinary credentials failure changed: %v", err)
		}
		startMDSTopologyFree(t, fs)
	})
	t.Run("replacement/admit-failure", func(t *testing.T) {
		// The cause is not wrapped: only the operation text is returned.
		r := newReplacementMDSFixture(t)
		cause := errors.New("admission cause")
		var sawID string
		admit := func(_ context.Context, id string) (testcontainers.Container, error) {
			sawID = id
			return nil, cause
		}
		d, err := r.fs.startMDSWithAdmission(t.Context(), startMDSNever(t), admit, nil)
		if err == nil || err.Error() != "admit replacement MDS startup" || d != nil || sawID != "target-1" || r.fs.roster.nextIndex != 1 || r.n.mutations != 0 {
			t.Fatalf("replacement admission failure changed: %v (id %q)", err, sawID)
		}
		startMDSTopologyFree(t, r.fs)
	})
	t.Run("replacement/admit-nil-control", func(t *testing.T) {
		r := newReplacementMDSFixture(t)
		admit := func(context.Context, string) (testcontainers.Container, error) { return nil, nil }
		_, err := r.fs.startMDSWithAdmission(t.Context(), startMDSNever(t), admit, nil)
		if err == nil || !strings.HasPrefix(err.Error(), "admit replacement MDS startup") || r.fs.roster.nextIndex != 1 || r.n.mutations != 0 {
			t.Fatalf("replacement nil-control refusal changed: %v", err)
		}
		startMDSTopologyFree(t, r.fs)
	})
	t.Run("replacement/credentials-failure", func(t *testing.T) {
		r := newReplacementMDSFixture(t)
		r.n.fail = "auth get-or-create mds.target-1" + startMDSCaps
		admit := func(context.Context, string) (testcontainers.Container, error) { return r.n, nil }
		d, err := r.fs.startMDSWithAdmission(t.Context(), startMDSNever(t), admit, nil)
		if err == nil || !strings.HasPrefix(err.Error(), "create replacement MDS credentials") || d != nil || r.fs.roster.nextIndex != 2 {
			t.Fatalf("replacement credentials failure changed: %v", err)
		}
		startMDSTopologyFree(t, r.fs)
	})
}

// TestStartMDSPublicationAndResultClassificationPerMode characterizes the half
// after the auth command: only a real handle is published, the published hook
// runs once with the daemon, and each mode classifies a start failure with its
// own text and daemon result.
func TestStartMDSPublicationAndResultClassificationPerMode(t *testing.T) {
	cause := errors.New("native service cause")
	t.Run("ordinary/partial-handle-with-error", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		fs.coldMDS = nil
		worker := &mdsScaleFixtureContainer{id: "partial"}
		start := coldMDSTestService(t, fs, n, worker, cause, false)
		var hooked []*MDS
		d, err := fs.startMDSWithAdmission(t.Context(), start, nil, func(m *MDS) { hooked = append(hooked, m) })
		if err != cause || d == nil || d.Container != worker || d.ID != "cold-target-0" || len(hooked) != 1 || hooked[0] != d || fs.Container != worker || len(fs.MDSs()) != 1 {
			t.Fatalf("ordinary partial result changed: %v %v", d, err)
		}
		startMDSTopologyFree(t, fs)
	})
	t.Run("ordinary/typed-nil-handle-is-not-published", func(t *testing.T) {
		fs, _ := coldMDSTestConstruct(t, false)
		fs.coldMDS = nil
		var typedNil *mdsScaleFixtureContainer
		d, err := fs.startMDSWithAdmission(t.Context(), func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
			return typedNil, cause
		}, nil, func(*MDS) { t.Error("published hook ran for a typed-nil handle") })
		if err != cause || d != nil || fs.Container != nil || len(fs.MDSs()) != 0 || fs.roster.nextIndex != 1 {
			t.Fatalf("typed-nil handle was published or the error changed: %v %v", d, err)
		}
	})
	t.Run("ordinary/second-handle-keeps-embedded-container", func(t *testing.T) {
		r := newReplacementMDSFixture(t)
		first := &mdsScaleFixtureContainer{id: "first"}
		r.fs.Container = first
		d, err := r.fs.startMDSWithAdmission(t.Context(), r.start, nil, nil)
		if err != nil || d == nil || r.fs.Container != first || len(r.fs.MDSs()) != 2 {
			t.Fatalf("the embedded Container was replaced or the start failed: %v %v", d, err)
		}
	})
	t.Run("cold/no-handle", func(t *testing.T) {
		fs, _ := coldMDSTestConstruct(t, false)
		d, err := fs.startMDSWithAdmission(t.Context(), func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
			return nil, cause
		}, nil, nil)
		if d != nil || err == nil || err.Error() != "start initial MDS service" || !fs.coldMDS.attempted {
			t.Fatalf("cold start failure changed: %v %v", d, err)
		}
	})
	t.Run("cold/partial-handle-is-not-returned", func(t *testing.T) {
		fs, n := coldMDSTestConstruct(t, false)
		worker := &mdsScaleFixtureContainer{id: "partial-cold"}
		d, err := fs.startMDSWithAdmission(t.Context(), coldMDSTestService(t, fs, n, worker, cause, false), nil, nil)
		if d != nil || err == nil || err.Error() != "start initial MDS service" || fs.Container != worker || len(fs.MDSs()) != 1 {
			t.Fatalf("cold partial result changed: %v %v", d, err)
		}
	})
	admit := func(r *replacementMDSFixture) func(context.Context, string) (testcontainers.Container, error) {
		return func(context.Context, string) (testcontainers.Container, error) { return r.n, nil }
	}
	t.Run("replacement/no-handle", func(t *testing.T) {
		r := newReplacementMDSFixture(t)
		d, err := r.fs.startMDSWithAdmission(t.Context(), func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
			return nil, cause
		}, admit(r), nil)
		if d != nil || err == nil || err.Error() != "start replacement MDS service" {
			t.Fatalf("replacement start failure changed: %v %v", d, err)
		}
	})
	t.Run("replacement/partial-handle-is-returned", func(t *testing.T) {
		r := newReplacementMDSFixture(t)
		r.startErr = cause
		var hooked *MDS
		d, err := r.fs.startMDSWithAdmission(t.Context(), r.start, admit(r), func(m *MDS) { hooked = m })
		if d == nil || hooked != d || err == nil || err.Error() != "start replacement MDS service" {
			t.Fatalf("replacement partial result changed: %v %v", d, err)
		}
	})
	t.Run("replacement/success-after-cancellation", func(t *testing.T) {
		r := newReplacementMDSFixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		r.afterStart = func(context.Context) { cancel() }
		d, err := r.fs.startMDSWithAdmission(ctx, r.start, admit(r), nil)
		if d == nil || !errors.Is(err, context.Canceled) || !strings.HasPrefix(err.Error(), "start replacement MDS service") {
			t.Fatalf("replacement late cancellation changed: %v %v", d, err)
		}
	})
	t.Run("replacement/success", func(t *testing.T) {
		r := newReplacementMDSFixture(t)
		d, err := r.fs.startMDSWithAdmission(t.Context(), r.start, admit(r), nil)
		if err != nil || d == nil || d.ID != "target-1" || len(r.fs.MDSs()) != 2 {
			t.Fatalf("replacement success changed: %v %v", d, err)
		}
	})
}
