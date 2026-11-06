package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// The MGR role is the one role with a default: Essential, after All
// expansion. All may be combined with MGR, which replaces only the MGR role.
func TestImageSetMGRDefaultRule(t *testing.T) {
	for name, test := range map[string]struct {
		set  ImageSet
		want ImageSet
	}{
		"essential default": {ImageSet{Essential: "core", Client: "toolkit"}, ImageSet{Essential: "core", Client: "toolkit", MGR: "core"}},
		"explicit MGR":      {ImageSet{Essential: "core", MGR: "official"}, ImageSet{Essential: "core", MGR: "official"}},
		"all default":       {ImageSet{All: "complete"}, ImageSet{Essential: "complete", Client: "complete", CephFS: "complete", RBDAux: "complete", RGW: "complete", MGR: "complete"}},
		"all with MGR":      {ImageSet{All: "complete", MGR: "official"}, ImageSet{Essential: "complete", Client: "complete", CephFS: "complete", RBDAux: "complete", RGW: "complete", MGR: "official"}},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := test.set.normalized()
			if err != nil || got != test.want {
				t.Fatalf("normalized %+v: %+v %v", test.set, got, err)
			}
			again, err := got.normalized()
			if err != nil || again != got {
				t.Fatalf("a resolved set is not stable: %+v %v", again, err)
			}
		})
	}
	// The default does not reach any other role, and a MGR image alone is not
	// a set: Essential is still required.
	for _, set := range []ImageSet{{MGR: "official"}, {Essential: "core", MGR: " official"}, {Essential: "core", MGR: "official\n"}, {All: "complete", MGR: "official", Client: "toolkit"}} {
		if got, err := set.normalized(); err == nil {
			t.Fatalf("accepted %+v as %+v", set, got)
		}
	}
	if got, _ := (ImageSet{Essential: "core", MGR: "official"}).normalized(); got.Client != "" || got.CephFS != "" || got.RBDAux != "" || got.RGW != "" {
		t.Fatalf("the MGR default leaked into another role: %+v", got)
	}
}

// The resolved MGR image is reported by ClusterFacts, for an image set as for
// the aggregate Run image, and the Run image stays the essential image.
func TestClusterFactsReportTheResolvedMGRImage(t *testing.T) {
	for name, test := range map[string]struct {
		img  string
		set  *ImageSet
		want string
	}{
		"aggregate":       {"single", nil, "single"},
		"set default":     {"", &ImageSet{Essential: "core", Client: "toolkit"}, "core"},
		"set MGR":         {"core", &ImageSet{Essential: "core", Client: "toolkit", MGR: "official"}, "official"},
		"all with MGR":    {"", &ImageSet{All: "complete", MGR: "official"}, "official"},
		"all without MGR": {"complete", &ImageSet{All: "complete"}, "complete"},
	} {
		t.Run(name, func(t *testing.T) {
			var opts []testcontainers.ContainerCustomizer
			if test.set != nil {
				opts = append(opts, WithImageOptions(ImageOptions{}.WithImageSet(*test.set)))
			}
			effective, err := admit(t, test.img, opts...)
			wantAdmitted(t, err)
			facts, err := FactsOf(&Container{settings: *effective})
			if err != nil || facts.Images.MGR != test.want || facts.Images.Essential != effective.essentialImage {
				t.Fatalf("facts images %+v (Run image %q): %v", facts.Images, effective.essentialImage, err)
			}
		})
	}
	// Run refuses the image argument when it names the MGR image instead of
	// the essential image: the MGR image is not a second Run image.
	_, err := admit(t, "official", WithImageOptions(ImageOptions{}.WithImageSet(ImageSet{Essential: "core", MGR: "official"})))
	wantRefused(t, err)
}

// The precedence of a new MGR's image: the DaemonOptions.WithImage override,
// then the cluster's MGR image, then the essential image. Only a cluster MGR
// image other than the essential image carries a release to check.
func TestManagerLaunchPrecedence(t *testing.T) {
	override := DaemonOptions{}.WithImage("per-daemon")
	for name, test := range map[string]struct {
		set      *ImageSet
		daemon   DaemonOptions
		want     managerLaunch
		monitors string
	}{
		"no set":                    {nil, DaemonOptions{}, managerLaunch{image: "core"}, "core"},
		"set without MGR":           {&ImageSet{Essential: "core", MGR: "core"}, DaemonOptions{}, managerLaunch{image: "core"}, "core"},
		"MGR slot":                  {&ImageSet{Essential: "core", MGR: "official"}, DaemonOptions{}, managerLaunch{image: "official", release: "20.2.4"}, "core"},
		"override over MGR slot":    {&ImageSet{Essential: "core", MGR: "official"}, override, managerLaunch{image: "per-daemon"}, "per-daemon"},
		"override over essential":   {&ImageSet{Essential: "core", MGR: "core"}, override, managerLaunch{image: "per-daemon"}, "per-daemon"},
		"unresolved set falls back": {&ImageSet{Essential: "core"}, DaemonOptions{}, managerLaunch{image: "core"}, "core"},
	} {
		t.Run(name, func(t *testing.T) {
			c := &Container{settings: options{essentialImage: "core", imageSet: test.set}, bootstrap: bootstrapRecord{cephVersion: "20.2.4"}}
			if got := c.managerLaunch(test.daemon); got != test.want {
				t.Fatalf("manager launch %+v, want %+v", got, test.want)
			}
			if got := c.settings.monitorImage(test.daemon); got != test.monitors {
				t.Fatalf("a MGR image changed the MON image: %q", got)
			}
		})
	}
}

// mgrImageControl answers the identity commands of startManager and registers
// every MGR whose key it creates, the first as active, so that AddManager and
// the initial managers see their registration.
type mgrImageControl struct {
	*noInitialManagerControl
	registered []string
	// imageIDs maps an image reference to the ID of the image it names, as
	// the local Docker image store would; a test moves a reference by
	// changing its entry.
	imageIDs map[string]string
}

func (c *mgrImageControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	call := strings.Join(monitorQuorumTestModuleArgs(args), " ")
	if name, ok := strings.CutPrefix(call, "auth get-or-create mgr."); ok {
		name, _, _ = strings.Cut(name, " ")
		c.calls = append(c.calls, call)
		c.registered = append(c.registered, name)
		status := map[string]any{"available": true, "active_name": c.registered[0], "active_gid": 100, "standbys": []map[string]any{}}
		var standbys []map[string]any
		for i, standby := range c.registered[1:] {
			standbys = append(standbys, map[string]any{"name": standby, "gid": 101 + i})
		}
		if standbys != nil {
			status["standbys"] = standbys
		}
		data, _ := json.Marshal(status)
		c.mgr = string(data)
		key := []byte("[mgr." + name + "]\n\tkey = AQDsyntheticmgrkey0000000000000000000==\n")
		return 0, noInitialOSDStream(string(key)), nil
	}
	return c.noInitialManagerControl.Exec(ctx, args, opts...)
}

// mgrImageDaemon is a container that the fixture started from image, the
// reference it was created with, which resolved to the image id: a release
// probe or a MGR daemon. It answers ceph-mgr --version with its release.
type mgrImageDaemon struct {
	testcontainers.Container
	image, id, release string
	// daemon is set when the container runs mgr.sh, and keyring when it
	// receives a MGR keyring.
	daemon, keyring bool
	execs           []string
	terminations    int
}

func (d *mgrImageDaemon) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	d.execs = append(d.execs, strings.Join(args, " "))
	if slices.Equal(args, []string{"ceph-mgr", "--version"}) {
		return 0, noInitialOSDStream(fmt.Sprintf("ceph version %s (0000000000000000000000000000000000000000) tentacle (stable)\n", d.release)), nil
	}
	return 1, noInitialOSDStream("unexpected MGR container command"), nil
}

func (d *mgrImageDaemon) Inspect(context.Context) (*container.InspectResponse, error) {
	return &container.InspectResponse{Image: d.id}, nil
}

func (d *mgrImageDaemon) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	d.terminations++
	return nil
}

// mgrImageRequest applies the options of a started container to a request.
func mgrImageRequest(image string, opts []testcontainers.ContainerCustomizer) (testcontainers.GenericContainerRequest, error) {
	req := testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Image: image}}
	for _, opt := range opts {
		if err := opt.Customize(&req); err != nil {
			return req, err
		}
	}
	return req, nil
}

// mgrImageFixture is a cluster of the essential image "core" and the given
// image set whose containers report the release that releases names for
// their image ID (20.2.4 by default). A reference resolves through
// control.imageIDs, by default to "sha256:" and the reference; a reference
// that starts with "sha256:" is an ID. started lists every container the
// cluster started, probes and MGR daemons, in order.
func mgrImageFixture(t *testing.T, set ImageSet, releases map[string]string) (*Container, *mgrImageControl, *[]*mgrImageDaemon) {
	t.Helper()
	normalized, err := set.normalized()
	if err != nil {
		t.Fatal(err)
	}
	cluster, base := noInitialManagerFixture()
	control := &mgrImageControl{noInitialManagerControl: base, imageIDs: map[string]string{}}
	cluster.Container = control
	cluster.settings.essentialImage, cluster.settings.imageSet = normalized.Essential, &normalized
	cluster.bootstrap.cephVersion = "20.2.4"
	var started []*mgrImageDaemon
	cluster.daemonRunner = func(_ context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		req, err := mgrImageRequest(image, opts)
		if err != nil {
			return nil, err
		}
		id := control.imageIDs[image]
		if id == "" {
			id = image
			if !strings.HasPrefix(image, "sha256:") {
				id = "sha256:" + image
			}
		}
		release := releases[id]
		if release == "" {
			release = releases[strings.TrimPrefix(id, "sha256:")]
		}
		if release == "" {
			release = "20.2.4"
		}
		daemon := &mgrImageDaemon{image: image, id: id, release: release, daemon: slices.Contains(req.Entrypoint, "/tc/mgr.sh")}
		for _, file := range req.Files {
			daemon.keyring = daemon.keyring || file.ContainerFilePath == "/etc/ceph/mgr.keyring"
		}
		started = append(started, daemon)
		return daemon, nil
	}
	return cluster, control, &started
}

// mgrImageStarts splits the containers of a fixture into MGR daemons and
// release probes, and fails when a probe received a keyring or was not removed.
func mgrImageStarts(t *testing.T, started []*mgrImageDaemon) (daemons, probes []*mgrImageDaemon) {
	t.Helper()
	for _, ctr := range started {
		if ctr.daemon {
			daemons = append(daemons, ctr)
			continue
		}
		if ctr.keyring || ctr.terminations != 1 || !slices.Equal(ctr.execs, []string{"ceph-mgr --version"}) {
			t.Fatalf("release probe of %s: keyring=%t terminations=%d commands %v", ctr.image, ctr.keyring, ctr.terminations, ctr.execs)
		}
		probes = append(probes, ctr)
	}
	return daemons, probes
}

// The MGR slot starts the initial active and standby candidates and every
// later AddManager without an override; an override starts its one MGR from
// its own image without a release check. Handles report what they started
// from, and the essential image stays the cluster's MON image.
func TestMGRImageAppliesToInitialAndAddedManagers(t *testing.T) {
	cluster, control, started := mgrImageFixture(t, ImageSet{Essential: "core", Client: "toolkit", MGR: "official"}, nil)
	cluster.settings.managers = 2
	if err := cluster.startInitialManagers(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := cluster.finishInitialManagers(t.Context()); err != nil {
		t.Fatal(err)
	}
	added, err := cluster.AddManager(t.Context(), "c")
	if err != nil {
		t.Fatal(err)
	}
	overridden, err := cluster.AddManager(t.Context(), "d", DaemonOptions{}.WithImage("per-daemon"))
	if err != nil {
		t.Fatal(err)
	}
	// Each MGR of the slot image is preceded by its release probe and is
	// created from the image ID that the probe checked; the override is not
	// probed and keeps its reference, and no MGR daemon runs a release check
	// itself.
	var sequence []string
	for _, ctr := range *started {
		kind := "probe:"
		if ctr.daemon {
			kind = "mgr:"
			if !ctr.keyring || len(ctr.execs) != 0 {
				t.Fatalf("MGR daemon of %s: keyring=%t commands %v", ctr.image, ctr.keyring, ctr.execs)
			}
		}
		sequence = append(sequence, kind+ctr.image)
	}
	want := []string{"probe:official", "mgr:sha256:official", "probe:official", "mgr:sha256:official", "probe:official", "mgr:sha256:official", "mgr:per-daemon"}
	if !slices.Equal(sequence, want) {
		t.Fatalf("started %v, want %v", sequence, want)
	}
	mgrImageStarts(t, *started)
	for name, want := range map[string]string{"a": "official", "b": "official", "c": "official", "d": "per-daemon"} {
		if got := cluster.members.roster.manager(name).Image(); got != want {
			t.Fatalf("mgr.%s reports image %q, want %q", name, got, want)
		}
	}
	if added.Image() != "official" || overridden.Image() != "per-daemon" || !slices.Equal(control.registered, []string{"a", "b", "c", "d"}) {
		t.Fatalf("handles %q %q, registered %v", added.Image(), overridden.Image(), control.registered)
	}
	facts, err := FactsOf(cluster)
	if err != nil || facts.Images.MGR != "official" || facts.Images.Essential != "core" || cluster.settings.essentialImage != "core" {
		t.Fatalf("an override or start changed the cluster images: %+v %v", facts.Images, err)
	}
}

// Without a MGR selection the managers keep the essential image and no
// release check runs, as before the MGR slot existed.
func TestMGRImageDefaultKeepsEssentialWithoutReleaseCheck(t *testing.T) {
	cluster, _, started := mgrImageFixture(t, ImageSet{Essential: "core", Client: "toolkit"}, nil)
	cluster.settings.managers = 1
	if err := cluster.startInitialManagers(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.AddManager(t.Context(), "b"); err != nil {
		t.Fatal(err)
	}
	for _, daemon := range *started {
		if !daemon.daemon || daemon.image != "core" || len(daemon.execs) != 0 {
			t.Fatalf("default MGR started from %q (daemon %t) with commands %v", daemon.image, daemon.daemon, daemon.execs)
		}
	}
	if len(*started) != 2 {
		t.Fatalf("started %d containers", len(*started))
	}
}

// A MGR image of another release is refused by its release probe before the
// MGR daemon could start: no identity or key is created, no container gets the
// keyring or runs mgr.sh, nothing is owned or awaited, and the probe is
// removed. An override of another release is not checked.
func TestMGRImageOfAnotherReleaseNeverStartsTheDaemon(t *testing.T) {
	for _, initial := range []bool{true, false} {
		t.Run(fmt.Sprintf("initial=%t", initial), func(t *testing.T) {
			cluster, control, started := mgrImageFixture(t, ImageSet{Essential: "core", MGR: "other-release"}, map[string]string{"other-release": "19.2.5", "per-daemon": "19.2.5"})
			var mgr *ManagerContainer
			var err error
			if initial {
				cluster.settings.managers = 1
				err = cluster.startInitialManagers(t.Context())
			} else {
				mgr, err = cluster.AddManager(t.Context(), "a")
			}
			if err == nil || !strings.Contains(err.Error(), "MGR image Ceph release 19.2.5 differs from core 20.2.4") {
				t.Fatalf("mismatched release admitted: %v", err)
			}
			daemons, probes := mgrImageStarts(t, *started)
			if len(daemons) != 0 || len(probes) != 1 || probes[0].image != "other-release" {
				t.Fatalf("a mismatched MGR image ran %d daemons after %d probes", len(daemons), len(probes))
			}
			if mgr != nil || cluster.members.roster.managerCount() != 0 || cluster.members.roster.initialManager() != nil {
				t.Fatalf("a refused MGR is owned: %+v managers=%d", mgr, cluster.members.roster.managerCount())
			}
			for _, call := range control.calls {
				if strings.HasPrefix(call, "auth get-or-create") {
					t.Fatalf("a refused MGR created its identity: %v", control.calls)
				}
			}
			if len(control.registered) != 0 {
				t.Fatalf("a refused MGR created its key: %v", control.registered)
			}
		})
	}
	cluster, _, started := mgrImageFixture(t, ImageSet{Essential: "core", MGR: "official"}, map[string]string{"per-daemon": "19.2.5"})
	if _, err := cluster.AddManager(t.Context(), "a", DaemonOptions{}.WithImage("per-daemon")); err != nil {
		t.Fatalf("an explicit per-daemon image was release-checked: %v", err)
	}
	if len(*started) != 1 || !(*started)[0].daemon || len((*started)[0].execs) != 0 {
		t.Fatalf("override started %d containers, commands %v", len(*started), (*started)[0].execs)
	}
}

// A MGR image without ceph-mgr fails its release probe with the image
// capability error, and no MGR identity or daemon is created.
func TestMGRImageWithoutTheDaemonIsAnImageCapabilityFailure(t *testing.T) {
	cluster, control, _ := mgrImageFixture(t, ImageSet{Essential: "core", MGR: "official"}, nil)
	var probe missingMGRDaemon
	runs := 0
	cluster.daemonRunner = func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		runs++
		return &probe, nil
	}
	mgr, err := cluster.AddManager(t.Context(), "a")
	if !errors.Is(err, ErrImageCapability) || mgr != nil || cluster.members.roster.managerCount() != 0 {
		t.Fatalf("missing ceph-mgr: %v %+v", err, mgr)
	}
	if runs != 1 || probe.terminations != 1 || len(control.registered) != 0 {
		t.Fatalf("missing ceph-mgr: runs=%d probe terminations=%d keys %v", runs, probe.terminations, control.registered)
	}
}

// A release probe whose removal fails reports that failure with the probe's
// result.
func TestMGRImageProbeRemovalFailureIsReported(t *testing.T) {
	cluster, _, _ := mgrImageFixture(t, ImageSet{Essential: "core", MGR: "official"}, nil)
	cluster.daemonRunner = func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		return &stuckMGRProbe{mgrImageDaemon{image: "official", release: "19.2.5"}}, nil
	}
	_, err := cluster.AddManager(t.Context(), "a")
	if err == nil || !strings.Contains(err.Error(), "differs from core") || !strings.Contains(err.Error(), "remove MGR image release probe: probe stuck") {
		t.Fatalf("probe removal failure: %v", err)
	}
}

type stuckMGRProbe struct{ mgrImageDaemon }

func (*stuckMGRProbe) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	return errors.New("probe stuck")
}

type missingMGRDaemon struct {
	testcontainers.Container
	terminations int
}

func (*missingMGRDaemon) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	return 127, noInitialOSDStream(dockerExecFailed + ": exec: \"ceph-mgr\": executable file not found in $PATH"), nil
}

func (d *missingMGRDaemon) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	d.terminations++
	return nil
}

// A MGR image reference that moves to another image after its release probe,
// as a rebuild or retag of a local image between the two Docker creates
// would, does not change the image of the MGR daemon: the daemon is created
// from the image ID that passed the probe, so the MGR key never reaches an
// unchecked image, while the handle and the cluster facts keep the requested
// reference.
func TestMGRImageDaemonRunsTheProbedImage(t *testing.T) {
	for _, initial := range []bool{true, false} {
		t.Run(fmt.Sprintf("initial=%t", initial), func(t *testing.T) {
			cluster, control, started := mgrImageFixture(t, ImageSet{Essential: "core", MGR: "official"}, map[string]string{"sha256:rebuilt": "19.2.5"})
			probed := cluster.daemonRunner
			cluster.daemonRunner = func(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
				ctr, err := probed(ctx, image, opts...)
				if daemon, ok := ctr.(*mgrImageDaemon); ok && !daemon.daemon {
					control.imageIDs["official"] = "sha256:rebuilt"
				}
				return ctr, err
			}
			var mgr *ManagerContainer
			var err error
			if initial {
				cluster.settings.managers = 1
				err = cluster.startInitialManagers(t.Context())
				mgr = cluster.members.roster.manager("a")
			} else {
				mgr, err = cluster.AddManager(t.Context(), "a")
			}
			if err != nil {
				t.Fatal(err)
			}
			daemons, probes := mgrImageStarts(t, *started)
			if len(probes) != 1 || probes[0].image != "official" || probes[0].id != "sha256:official" {
				t.Fatalf("probes %+v", probes)
			}
			if len(daemons) != 1 || daemons[0].image != probes[0].id || daemons[0].id != "sha256:official" || !daemons[0].keyring {
				t.Fatalf("the MGR daemon ran image %q (%q) after the probe checked %q", daemons[0].image, daemons[0].id, probes[0].id)
			}
			if mgr == nil || mgr.Image() != "official" {
				t.Fatalf("handle %+v reports another image than requested", mgr)
			}
			facts, err := FactsOf(cluster)
			if err != nil || facts.Images.MGR != "official" {
				t.Fatalf("facts report MGR image %q: %v", facts.Images.MGR, err)
			}
		})
	}
}

// A release probe that cannot report the image it ran refuses the MGR, since
// the daemon could not be bound to the checked image.
func TestMGRImageProbeWithoutImageIDRefusesTheManager(t *testing.T) {
	cluster, control, _ := mgrImageFixture(t, ImageSet{Essential: "core", MGR: "official"}, nil)
	probe := &uninspectableMGRProbe{mgrImageDaemon{image: "official", release: "20.2.4"}}
	cluster.daemonRunner = func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		return probe, nil
	}
	mgr, err := cluster.AddManager(t.Context(), "a")
	if err == nil || !strings.Contains(err.Error(), "read MGR image release probe image ID") || mgr != nil {
		t.Fatalf("a probe without an image ID admitted the MGR: %v %+v", err, mgr)
	}
	if probe.terminations != 1 || len(control.registered) != 0 {
		t.Fatalf("probe terminations=%d keys %v", probe.terminations, control.registered)
	}
}

type uninspectableMGRProbe struct{ mgrImageDaemon }

func (*uninspectableMGRProbe) Inspect(context.Context) (*container.InspectResponse, error) {
	return nil, errors.New("inspect refused")
}

// The removal of a release probe is bounded by managerProbeRemovalTimeout,
// not only by the caller's context: a removal that waits until its context
// ends, as a stalled Docker engine does, returns after that bound, the
// topology lock is released, and the removal error is reported with the
// probe's result. A start whose own context is cancelled still removes the
// probe under that bound.
func TestMGRImageProbeRemovalIsBounded(t *testing.T) {
	previous := managerProbeRemovalTimeout
	managerProbeRemovalTimeout = 50 * time.Millisecond
	t.Cleanup(func() { managerProbeRemovalTimeout = previous })
	for name, release := range map[string]string{"passing probe": "20.2.4", "refusing probe": "19.2.5"} {
		t.Run(name, func(t *testing.T) {
			cluster, control, _ := mgrImageFixture(t, ImageSet{Essential: "core", MGR: "official"}, nil)
			probe := &stalledMGRProbe{mgrImageDaemon: mgrImageDaemon{image: "official", id: "sha256:official", release: release}}
			cluster.daemonRunner = func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
				return probe, nil
			}
			done := make(chan error, 1)
			go func() {
				_, err := cluster.AddManager(context.Background(), "a")
				done <- err
			}()
			var err error
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("a stalled probe removal held the start past its bound")
			}
			if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "remove MGR image release probe") {
				t.Fatalf("stalled probe removal: %v", err)
			}
			if release != "20.2.4" && !strings.Contains(err.Error(), "differs from core") {
				t.Fatalf("the probe result was lost: %v", err)
			}
			if !probe.deadline || len(control.registered) != 0 || cluster.members.roster.managerCount() != 0 {
				t.Fatalf("removal deadline=%t keys %v managers=%d", probe.deadline, control.registered, cluster.members.roster.managerCount())
			}
			if !cluster.mu.TryLock() {
				t.Fatal("the topology lock is still held")
			}
			cluster.mu.Unlock()
		})
	}
	cluster, _, _ := mgrImageFixture(t, ImageSet{Essential: "core", MGR: "official"}, nil)
	probe := &stalledMGRProbe{mgrImageDaemon: mgrImageDaemon{image: "official", id: "sha256:official", release: "20.2.4"}}
	ctx, cancel := context.WithCancel(t.Context())
	cluster.daemonRunner = func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		cancel()
		return probe, nil
	}
	done := make(chan error, 1)
	go func() {
		_, err := cluster.AddManager(ctx, "a")
		done <- err
	}()
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a stalled probe removal held a cancelled start past its bound")
	}
	if err == nil || probe.removals != 1 || !probe.deadline || !probe.waited {
		t.Fatalf("cancelled start: %v removals=%d deadline=%t waited=%t", err, probe.removals, probe.deadline, probe.waited)
	}
}

// stalledMGRProbe is a release probe whose removal waits until its context
// ends and fails with the context's error. It records whether that context had
// a deadline and whether the removal waited for it rather than seeing an
// already cancelled context.
type stalledMGRProbe struct {
	mgrImageDaemon
	removals         int
	deadline, waited bool
}

func (p *stalledMGRProbe) Terminate(ctx context.Context, _ ...testcontainers.TerminateOption) error {
	p.removals++
	_, p.deadline = ctx.Deadline()
	p.waited = ctx.Err() == nil
	<-ctx.Done()
	return ctx.Err()
}
