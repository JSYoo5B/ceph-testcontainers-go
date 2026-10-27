package ceph

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"github.com/testcontainers/testcontainers-go"
)

// DaemonOption adjusts one daemon that AddMonitor, AddManager, AddOSD or
// AddOSDWithConfig creates. It applies to that daemon only and never changes
// the cluster's settings or any other daemon.
type DaemonOption func(*daemonOptions) error

type daemonOptions struct {
	image    string
	imageSet bool
}

// WithDaemonImage starts the one new daemon from image instead of the cluster's
// essential image. The image must contain the daemon's binary and the tools the
// fixture's startup scripts use, as the essential image does; nothing here checks its
// release or compatibility with the running cluster.
//
// The override belongs to that daemon: DetachedOSD.Reopen starts the same OSD
// from the same image again, Stop and Start keep the container, and the
// daemon's handle reports it with Image. It does not change ClusterFacts, which
// describes the cluster's own settings and its first MON, and a CLI, client or helper container still
// uses the cluster's images. A cluster that a SharedCluster shares refuses the
// calls that take this option, so a tenant cannot use it.
//
// The image reference must be non-empty and free of whitespace and control
// characters. A refusal happens before any daemon identity, key or container
// is created.
func WithDaemonImage(image string) DaemonOption {
	return func(o *daemonOptions) error {
		if o.imageSet {
			return fmt.Errorf("daemon image selected twice")
		}
		if image == "" {
			return fmt.Errorf("daemon image must not be empty")
		}
		if strings.ContainsFunc(image, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
			return fmt.Errorf("daemon image must not contain whitespace or control characters")
		}
		o.image, o.imageSet = image, true
		return nil
	}
}

// resolveDaemonOptions validates opts without touching any cluster state. A nil
// option is refused like a typed nil elsewhere.
func resolveDaemonOptions(opts []DaemonOption) (daemonOptions, error) {
	var resolved daemonOptions
	for i, opt := range opts {
		if opt == nil {
			return daemonOptions{}, invalidRequest("daemon option %d is nil", i)
		}
		if err := opt(&resolved); err != nil {
			return daemonOptions{}, invalid(fmt.Errorf("daemon option %d: %w", i, err))
		}
	}
	return resolved, nil
}

// daemonImage returns the image the new daemon starts from: its override, or
// the cluster's essential image.
func (c *Container) daemonImage(o daemonOptions) string {
	if o.imageSet {
		return o.image
	}
	return c.settings.essentialImage
}

// runDaemon starts the container of a new MON or MGR from image.
func (c *Container) runDaemon(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	if c.daemonRunner != nil {
		return c.daemonRunner(ctx, image, opts...)
	}
	return runContainer(ctx, image, opts...)
}

// Image returns the image this MON container was started from: the
// WithDaemonImage override, or the cluster's essential image.
func (m *MonitorContainer) Image() string {
	if m == nil {
		return ""
	}
	return m.image
}

// Image returns the image this MGR container was started from: the
// WithDaemonImage override, or the cluster's essential image.
func (m *ManagerContainer) Image() string {
	if m == nil {
		return ""
	}
	return m.image
}

// Image returns the image this OSD container was started from: the
// WithDaemonImage override, or the cluster's essential image. DetachedOSD.Reopen
// starts the OSD from the same image again.
func (o *OSDContainer) Image() string {
	if o == nil {
		return ""
	}
	return o.image
}

// Image returns the image the detached OSD ran from, which DetachedOSD.Reopen
// uses again.
func (d *DetachedOSD) Image() string {
	if d == nil {
		return ""
	}
	return d.image
}
