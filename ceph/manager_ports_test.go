package ceph

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// WithManagerPorts keeps its own copy of valid TCP ports and refuses an
// invalid, repeated or empty list, a second option and host networking
// before anything is created.
func TestWithManagerPortsValidation(t *testing.T) {
	ports := []int{8443, 9283}
	settings, err := resolveOptions("img", []testcontainers.ContainerCustomizer{WithManagerPorts(ports...)})
	if err != nil || !slices.Equal(settings.managerPorts, []int{8443, 9283}) {
		t.Fatalf("manager ports %v: %v", settings.managerPorts, err)
	}
	ports[0] = 1
	if settings.managerPorts[0] != 8443 || !slices.Equal(settings.managerExposedPorts(), []string{"8443/tcp", "9283/tcp"}) {
		t.Fatalf("the settings share the caller's slice or render %v", settings.managerExposedPorts())
	}
	if settings, err := resolveOptions("img", nil); err != nil || len(settings.managerExposedPorts()) != 0 {
		t.Fatalf("default manager ports %v: %v", settings.managerExposedPorts(), err)
	}
	for name, opts := range map[string][]testcontainers.ContainerCustomizer{
		"zero":         {WithManagerPorts(0)},
		"too large":    {WithManagerPorts(65536)},
		"repeated":     {WithManagerPorts(8443, 8443)},
		"empty":        {WithManagerPorts()},
		"second":       {WithManagerPorts(8443), WithManagerPorts(9283)},
		"host network": {WithHostNetwork(), WithManagerPorts(8443)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := resolveOptions("img", opts); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("accepted: %v", err)
			}
		})
	}
}

// Every MGR container publishes the selected ports: the initial active and
// standby candidates and a later AddManager, also one with its own image.
// Without the option a MGR publishes none.
func TestManagerPortsReachEveryManagerContainer(t *testing.T) {
	for name, test := range map[string]struct {
		ports []int
		want  []string
	}{
		"selected": {[]int{8443, 9283}, []string{"8443/tcp", "9283/tcp"}},
		"default":  {nil, nil},
	} {
		t.Run(name, func(t *testing.T) {
			cluster, _, _ := mgrImageFixture(t, ImageSet{Essential: "core", MGR: "official"}, nil)
			cluster.settings.managers = 2
			cluster.settings.managerPorts = test.ports
			published := map[string][]string{}
			run := cluster.daemonRunner
			cluster.daemonRunner = func(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
				req, err := mgrImageRequest(image, opts)
				if err != nil {
					return nil, err
				}
				if id := req.Env["CEPH_MGR_ID"]; id != "" {
					published[id] = req.ExposedPorts
				}
				return run(ctx, image, opts...)
			}
			if err := cluster.startInitialManagers(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := cluster.finishInitialManagers(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := cluster.AddManager(t.Context(), "c"); err != nil {
				t.Fatal(err)
			}
			if _, err := cluster.AddManager(t.Context(), "d", DaemonOptions{}.WithImage("per-daemon")); err != nil {
				t.Fatal(err)
			}
			if len(published) != 4 {
				t.Fatalf("MGR containers %v", published)
			}
			for id, ports := range published {
				if !slices.Equal(ports, test.want) {
					t.Fatalf("mgr.%s publishes %v, want %v", id, ports, test.want)
				}
			}
		})
	}
}
