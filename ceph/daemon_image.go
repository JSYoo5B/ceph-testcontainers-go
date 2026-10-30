package ceph

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"github.com/testcontainers/testcontainers-go"
)

// DaemonOptions is the configuration of one daemon that AddMonitor or AddManager
// creates (AddOSD takes OSDOptions). It is a plain value with chainable,
// value-returning methods. It applies to that daemon only and never changes the
// cluster's settings or any other daemon. The zero value changes nothing.
//
//	mon, err := cluster.AddMonitor(ctx, "b", ceph.DaemonOptions{}.WithImage(image))
//
// Within one value a later call of the same method replaces the earlier one. A
// field set by two values is refused with ErrInvalidRequest, even when they
// agree.
type DaemonOptions struct {
	image    string
	imageSet bool
}

// WithImage starts the one new daemon from image instead of the cluster's
// essential image. The image must contain the daemon's binary and the tools the
// fixture's startup scripts use, as the essential image does; nothing here checks its
// release or compatibility with the running cluster.
//
// The override belongs to that daemon: Stop and Start keep the container, and
// the daemon's handle reports it with Image. It does not change ClusterFacts,
// which describes the cluster's own settings and its first MON, and a CLI,
// client or helper container still uses the cluster's images.
//
// The image reference must be non-empty and free of whitespace and control
// characters. A refusal matches ErrInvalidRequest and happens before the
// topology lock and before any daemon identity, key or container is created;
// nothing checks that the image exists. MON and MGR containers are never
// recreated, so their image does not change for their lifetime.
// ClusterFacts.Images and ClusterFacts.CephVersion, and the images recorded in
// a finalization manifest or verification report, keep describing the cluster
// and its first MON; the diagnostics archive records each container's image
// from Docker. A client container is still refused when its release differs
// from ClusterFacts.CephVersion. This is a building block: no mixed-version or
// upgrade combination is claimed to work.
//
//purpose:construction
func (o DaemonOptions) WithImage(image string) DaemonOptions {
	o.image, o.imageSet = image, true
	return o
}

// resolveDaemonOptions merges and validates configs without touching any
// cluster state.
func resolveDaemonOptions(configs []DaemonOptions) (DaemonOptions, error) {
	var resolved DaemonOptions
	for i, config := range configs {
		if !config.imageSet {
			continue
		}
		if resolved.imageSet {
			return DaemonOptions{}, invalidRequest("daemon image set by more than one DaemonOptions")
		}
		if err := validateDaemonImage(config.image); err != nil {
			return DaemonOptions{}, invalid(fmt.Errorf("daemon options %d: %w", i, err))
		}
		resolved = config
	}
	return resolved, nil
}

// daemonImage returns the image the new daemon starts from: its override, or
// the cluster's essential image.
func (o options) daemonImage(daemon DaemonOptions) string {
	if daemon.imageSet {
		return daemon.image
	}
	return o.essentialImage
}

// runDaemon starts the container of a new MON or MGR from image.
func (c *Container) runDaemon(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	if c.daemonRunner != nil {
		return c.daemonRunner(ctx, image, opts...)
	}
	return runContainer(ctx, image, opts...)
}

// Image returns the image this MON container was started from: the
// DaemonOptions.WithImage override, or the cluster's essential image.
//
//purpose:observation
func (m *MonitorContainer) Image() string {
	if m == nil {
		return ""
	}
	return m.image
}

// Image returns the image this MGR container was started from: the
// DaemonOptions.WithImage override, or the cluster's essential image.
//
//purpose:observation
func (m *ManagerContainer) Image() string {
	if m == nil {
		return ""
	}
	return m.image
}

// Image returns the image this OSD container was started from: the
// OSDOptions.WithImage override, or the cluster's essential image. DetachedOSD.Reopen
// starts the OSD from the same image again.
//
//purpose:observation
func (o *OSDContainer) Image() string {
	if o == nil {
		return ""
	}
	return o.image
}

// Image returns the image the detached OSD ran from, which DetachedOSD.Reopen
// uses again.
//
//purpose:observation
func (d *DetachedOSD) Image() string {
	if d == nil {
		return ""
	}
	return d.image
}

// validateDaemonImage is the one validation of a per-daemon image reference,
// shared by DaemonOptions.WithImage and OSDOptions.WithImage.
func validateDaemonImage(image string) error {
	if image == "" {
		return fmt.Errorf("daemon image must not be empty")
	}
	if strings.ContainsFunc(image, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return fmt.Errorf("daemon image must not contain whitespace or control characters")
	}
	return nil
}
