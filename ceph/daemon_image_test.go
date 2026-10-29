package ceph

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestWithDaemonImageValidation(t *testing.T) {
	for name, test := range map[string]struct {
		opts []DaemonOption
		want string
	}{
		"empty":               {[]DaemonOption{WithDaemonImage("")}, "empty"},
		"spaces only":         {[]DaemonOption{WithDaemonImage("   ")}, "whitespace"},
		"leading space":       {[]DaemonOption{WithDaemonImage(" example.invalid/ceph:new")}, "whitespace"},
		"trailing newline":    {[]DaemonOption{WithDaemonImage("example.invalid/ceph:new\n")}, "whitespace"},
		"inner tab":           {[]DaemonOption{WithDaemonImage("example.invalid/ceph\t:new")}, "whitespace"},
		"NUL":                 {[]DaemonOption{WithDaemonImage("example.invalid/ceph:\x00new")}, "control"},
		"twice":               {[]DaemonOption{WithDaemonImage("a:1"), WithDaemonImage("b:2")}, "twice"},
		"nil option":          {[]DaemonOption{nil}, "nil"},
		"nil after valid one": {[]DaemonOption{WithDaemonImage("a:1"), nil}, "nil"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := resolveDaemonOptions(test.opts)
			if err == nil || !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), test.want) || got != (daemonOptions{}) {
				t.Fatalf("resolved %+v, err %v", got, err)
			}
		})
	}
	got, err := resolveDaemonOptions([]DaemonOption{WithDaemonImage("example.invalid/ceph:new")})
	if err != nil || got.image != "example.invalid/ceph:new" || !got.imageSet {
		t.Fatalf("valid image: %+v %v", got, err)
	}
	if got, err := resolveDaemonOptions(nil); err != nil || got.imageSet {
		t.Fatalf("no options: %+v %v", got, err)
	}
}

func TestDaemonImageDefaultsToTheEssentialImageWithoutChangingIt(t *testing.T) {
	c := &Container{settings: options{essentialImage: "example.invalid/ceph:cluster"}}
	override, _ := resolveDaemonOptions([]DaemonOption{WithDaemonImage("example.invalid/ceph:new")})
	if got := c.daemonImage(override); got != "example.invalid/ceph:new" {
		t.Fatalf("override image %q", got)
	}
	if got := c.daemonImage(daemonOptions{}); got != "example.invalid/ceph:cluster" {
		t.Fatalf("default image %q", got)
	}
	if c.settings.essentialImage != "example.invalid/ceph:cluster" || c.staticFacts().Images.Essential != "example.invalid/ceph:cluster" {
		t.Fatal("resolving an override changed the cluster image")
	}
}

// A refused option leaves the topology untouched and does not wait for the
// topology lock: the call returns while the test holds it.
func TestDaemonImageRefusalHappensBeforeLockAndState(t *testing.T) {
	bad := WithDaemonImage(" ")
	calls := map[string]func(context.Context, *Container) (any, error){
		"AddMonitor": func(ctx context.Context, c *Container) (any, error) {
			m, err := c.AddMonitor(ctx, "b", bad)
			return m, err
		},
		"AddManager": func(ctx context.Context, c *Container) (any, error) {
			m, err := c.AddManager(ctx, "b", bad)
			return m, err
		},
		"AddOSD": func(ctx context.Context, c *Container) (any, error) {
			o, err := c.AddOSD(ctx, OSDOptions{}.WithImage(" "))
			return o, err
		},
		"AddOSD with placement": func(ctx context.Context, c *Container) (any, error) {
			o, err := c.AddOSD(ctx, OSDOptions{}.WithPlacement(OSDConfig{Host: "host-a"}).WithImage(" "))
			return o, err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			c := &Container{settings: options{essentialImage: "example.invalid/ceph:cluster", startupTimeout: time.Second}}
			c.mu.Lock()
			defer c.mu.Unlock()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			started := time.Now()
			got, err := call(ctx, c)
			var op *OperationError
			if !errors.Is(err, ErrInvalidRequest) || !errors.As(err, &op) || ctx.Err() != nil || time.Since(started) > 2*time.Second {
				t.Fatalf("err %v after %v", err, time.Since(started))
			}
			isNil := false
			switch v := got.(type) {
			case *MonitorContainer:
				isNil = v == nil
			case *ManagerContainer:
				isNil = v == nil
			case *OSDContainer:
				isNil = v == nil
			}
			if !isNil || c.roster.monitorCount() != 0 || c.roster.managerCount() != 0 || c.osdHandles.count() != 0 || c.closed {
				t.Fatalf("refusal left state: %v monitors=%d managers=%d osds=%d", got, c.roster.monitorCount(), c.roster.managerCount(), c.osdHandles.count())
			}
		})
	}
}

// The shared gate is checked first, valid option or not, so a tenant of a
// shared cluster cannot use the option.
func TestDaemonImageOptionIsRefusedForTenantsOfASharedCluster(t *testing.T) {
	for _, image := range []string{"example.invalid/ceph:new", " "} {
		shared := &Container{settings: options{essentialImage: "example.invalid/ceph:cluster"}}
		shared.sharing.Store(int32(sharedWithTenants))
		option := WithDaemonImage(image)
		ctx := t.Context()
		_, monErr := shared.AddMonitor(ctx, "b", option)
		_, mgrErr := shared.AddManager(ctx, "b", option)
		_, osdErr := shared.AddOSD(ctx, OSDOptions{}.WithImage(image))
		for name, err := range map[string]error{"AddMonitor": monErr, "AddManager": mgrErr, "AddOSD": osdErr} {
			if !errors.Is(err, ErrSharedCluster) {
				t.Errorf("%s with image %q: %v, want ErrSharedCluster", name, image, err)
			}
		}
		if shared.roster.monitorCount()+shared.roster.managerCount()+shared.osdHandles.count() != 0 {
			t.Fatal("a refused tenant call changed the topology")
		}
	}
	for _, op := range []string{"AddMonitor", "AddManager", "AddOSD"} {
		if sharedMethodPolicies[op] != sharedRecycle {
			t.Errorf("%s is no longer a topology change in the shared gate", op)
		}
	}
	for _, op := range []string{"OSDContainer.Version", "MonitorContainer.Version", "ManagerContainer.Version"} {
		if sharedHandlePolicies[op] != sharedExclusive {
			t.Errorf("%s must be refused to tenants", op)
		}
	}
}

// AddOSD with a placement and an image end to end against the registration fakes: the overridden
// OSD starts from its image and the cluster, and a second OSD, keep the default.
func TestOverriddenOSDRecordsItsImageWithoutChangingTheClusterOrOtherOSDs(t *testing.T) {
	c, _, runner := newInitialOSDCluster(options{essentialImage: "example.invalid/ceph:cluster", osds: 2})
	c.bootstrap.config, c.bootstrap.keyring = []byte("[global]\nfsid = synthetic-cluster\n"), []byte("synthetic-admin-keyring")
	var images []string
	inner := c.stores.runner
	c.stores.runner = func(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		images = append(images, image)
		return inner(ctx, image, opts...)
	}
	overridden, err := c.AddOSD(t.Context(), OSDOptions{}.WithPlacement(OSDConfig{Host: "host-a"}), OSDOptions{}.WithImage("example.invalid/ceph:new"))
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err := c.AddOSD(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(images) != 2 || images[0] != "example.invalid/ceph:new" || images[1] != "example.invalid/ceph:cluster" {
		t.Fatalf("container images %v", images)
	}
	if overridden.Placement().Host != "host-a" || ordinary.Placement().Host == "host-a" {
		t.Fatalf("placements %+v %+v", overridden.Placement(), ordinary.Placement())
	}
	if overridden.Image() != "example.invalid/ceph:new" || ordinary.Image() != "example.invalid/ceph:cluster" {
		t.Fatalf("handle images %q %q", overridden.Image(), ordinary.Image())
	}
	if c.staticFacts().Images.Essential != "example.invalid/ceph:cluster" {
		t.Fatal("an override changed the cluster-wide images")
	}
	if facts, err := FactsOf(c); err != nil || facts.Images.Essential != "example.invalid/ceph:cluster" || facts.Images.Client != "example.invalid/ceph:cluster" {
		t.Fatalf("facts changed: %+v %v", facts.Images, err)
	}
	if runner.daemons()[overridden.ID] == nil {
		t.Fatal("the overridden OSD has no container")
	}
	// A refusal after the override keeps the first OSD intact.
	if refused, err := c.AddOSD(t.Context(), OSDOptions{}.WithImage("")); refused != nil || !errors.Is(err, ErrInvalidRequest) || c.osdHandles.count() != 2 {
		t.Fatalf("refused add: %v %v owned=%d", refused, err, c.osdHandles.count())
	}
}

func TestOSDLaunchRecipeKeepsTheOSDImageNotTheClusterImage(t *testing.T) {
	c := &Container{settings: options{essentialImage: "example.invalid/ceph:cluster", startupTimeout: time.Minute}, bootstrap: bootstrapRecord{config: []byte("[global]\n")}}
	overridden := &OSDContainer{ID: 7, image: "example.invalid/ceph:new"}
	ordinary := &OSDContainer{ID: 8, image: "example.invalid/ceph:cluster"}
	unset := &OSDContainer{ID: 9}
	recipe := c.newOSDLaunchRecipe(overridden, nil)
	c.settings.essentialImage = "example.invalid/ceph:changed-later"
	if recipe.image != "example.invalid/ceph:new" {
		t.Fatalf("recipe image %q", recipe.image)
	}
	if got := c.newOSDLaunchRecipe(overridden, nil).image; got != "example.invalid/ceph:new" {
		t.Fatalf("a later recipe of the same OSD uses %q", got)
	}
	if got := c.newOSDLaunchRecipe(ordinary, nil).image; got != "example.invalid/ceph:cluster" {
		t.Fatalf("ordinary OSD recipe %q", got)
	}
	if got := c.newOSDLaunchRecipe(unset, nil).image; got != "example.invalid/ceph:changed-later" {
		t.Fatalf("a handle without a recorded image uses the cluster image, got %q", got)
	}
}

// Detach and Reopen start the same OSD from the same image, whatever the
// cluster image is by then, and leave other OSDs alone.
func TestReopenKeepsTheOverriddenImage(t *testing.T) {
	f := newRetainedFixture(t)
	f.cluster.settings.essentialImage = "example.invalid/ceph:cluster"
	f.cluster.osdHandles.get(3).image = "example.invalid/ceph:new"
	otherBefore := f.cluster.osdHandles.get(4).image
	var images []string
	inner := f.cluster.stores.runner
	f.cluster.stores.runner = func(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		images = append(images, image)
		return inner(ctx, image, opts...)
	}
	detached, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if detached.Image() != "example.invalid/ceph:new" {
		t.Fatalf("detached image %q", detached.Image())
	}
	f.cluster.settings.essentialImage = "example.invalid/ceph:changed-later"
	reopened, err := detached.Reopen(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(images) != 1 || images[0] != "example.invalid/ceph:new" || reopened.Image() != "example.invalid/ceph:new" {
		t.Fatalf("reopen images %v handle %q", images, reopened.Image())
	}
	if f.cluster.osdHandles.get(4).image != otherBefore || f.cluster.settings.essentialImage != "example.invalid/ceph:changed-later" {
		t.Fatal("reopening one OSD changed another daemon or the cluster")
	}
}

func TestDaemonImageAccessorsAreNilSafe(t *testing.T) {
	if (*MonitorContainer)(nil).Image() != "" || (*ManagerContainer)(nil).Image() != "" || (*OSDContainer)(nil).Image() != "" || (*DetachedOSD)(nil).Image() != "" {
		t.Fatal("a nil handle reported an image")
	}
	if (&MonitorContainer{image: "m"}).Image() != "m" || (&ManagerContainer{image: "g"}).Image() != "g" ||
		(&OSDContainer{image: "o"}).Image() != "o" || (&DetachedOSD{image: "d"}).Image() != "d" {
		t.Fatal("a handle did not report its recorded image")
	}
}

func TestParseDaemonVersion(t *testing.T) {
	for name, test := range map[string]struct {
		in, want string
	}{
		"release":         {`{"version":"20.2.4","release":"tentacle","release_type":"stable"}`, "20.2.4"},
		"squid":           {`{"version":"19.2.5","release":"squid","release_type":"stable"}` + "\n", "19.2.5"},
		"dev suffix":      {`{"version":"20.3.0-123-gabcdef","release":"tentacle","release_type":"dev"}`, "20.3.0"},
		"only fields":     {`{"version":"18.2.7"}`, "18.2.7"},
		"whitespace tail": {"{\"version\":\"20.2.4\"} \n\t ", "20.2.4"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := parseDaemonVersion([]byte(test.in))
			if err != nil || got != test.want {
				t.Fatalf("%q %v", got, err)
			}
		})
	}
	for name, in := range map[string]string{
		"empty output":      ``,
		"not JSON":          `ceph version 20.2.4 (abc) tentacle (stable)`,
		"array":             `["20.2.4"]`,
		"null document":     `null`,
		"missing version":   `{"release":"tentacle"}`,
		"null version":      `{"version":null}`,
		"numeric version":   `{"version":20}`,
		"empty version":     `{"version":""}`,
		"two components":    `{"version":"20.2"}`,
		"padded version":    `{"version":" 20.2.4"}`,
		"object version":    `{"version":{"v":"20.2.4"}}`,
		"trailing document": `{"version":"20.2.4"}{"version":"19.2.5"}`,
		"closing bracket":   `{"version":"20.2.4"}]`,
		"extra brace":       `{"version":"20.2.4"}}`,
		"trailing garbage":  `{"version":"20.2.4"} x`,
		"trailing scalar":   `{"version":"20.2.4"} 1`,
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := parseDaemonVersion([]byte(in)); err == nil || got != "" {
				t.Fatalf("accepted %q as %q", in, got)
			}
		})
	}
}

// Version goes through the admin socket admission, so a stale or foreign handle
// and a shared cluster are refused before any command runs.
func TestDaemonVersionRefusesLikeTheAdminSocket(t *testing.T) {
	c, control := adminSocketCluster()
	ctx := t.Context()
	if _, err := osdHandle(t, ctx, c, 7).Version(ctx); err == nil || !strings.Contains(err.Error(), "not owned") || len(control.Calls) != 0 {
		t.Fatalf("unknown OSD: %v %v", err, control.Calls)
	}
	c.closed = true
	if _, err := monitorHandle(t, ctx, c, "a").Version(ctx); err == nil || len(control.Calls) != 0 {
		t.Fatalf("closed cluster: %v %v", err, control.Calls)
	}
	if _, err := (&ManagerContainer{DaemonName: "a"}).Version(ctx); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("handle without a cluster: %v", err)
	}
	shared, _ := adminSocketCluster()
	shared.sharing.Store(int32(sharedWithTenants))
	if _, err := managerHandle(t, ctx, shared, "a").Version(ctx); !errors.Is(err, ErrSharedCluster) {
		t.Fatalf("shared cluster: %v", err)
	}
}

// managerImageControl adds the key creation that startManager performs.
type managerImageControl struct{ *noInitialManagerControl }

func (c managerImageControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	call := strings.Join(monitorQuorumTestModuleArgs(args), " ")
	if strings.HasPrefix(call, "auth get-or-create mgr.") {
		c.calls = append(c.calls, call)
		key := []byte("[mgr.x]\n\tkey = AQDsyntheticmgrkey0000000000000000000==\n")
		if monitorQuorumTestCommand(args) {
			return monitorQuorumTestReader(args, 0, key)
		}
		return 0, noInitialOSDStream(string(key)), nil
	}
	return c.noInitialManagerControl.Exec(ctx, args, opts...)
}

func TestStartManagerLaunchesFromTheGivenImage(t *testing.T) {
	cluster, control := noInitialManagerFixture()
	cluster.Container = managerImageControl{control}
	cluster.settings.essentialImage = "example.invalid/ceph:cluster"
	var images []string
	cluster.daemonRunner = func(_ context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		images = append(images, image)
		return &initialOSDDaemon{}, nil
	}
	overridden, err := cluster.startManager(t.Context(), "b", cluster.daemonImage(daemonOptions{image: "example.invalid/ceph:new", imageSet: true}))
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err := cluster.startManager(t.Context(), "c", cluster.daemonImage(daemonOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(images) != 2 || images[0] != "example.invalid/ceph:new" || images[1] != "example.invalid/ceph:cluster" {
		t.Fatalf("container images %v", images)
	}
	if overridden.Image() != "example.invalid/ceph:new" || ordinary.Image() != "example.invalid/ceph:cluster" || cluster.settings.essentialImage != "example.invalid/ceph:cluster" {
		t.Fatalf("handle images %q %q, cluster %q", overridden.Image(), ordinary.Image(), cluster.settings.essentialImage)
	}
}
