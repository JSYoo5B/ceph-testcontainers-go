package ceph

import (
	"context"
	"fmt"
	"strings"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ImageSet selects the roles runtime contract. All explicitly supplies every
// role from one image. Otherwise Essential is required and optional roles are
// required when their feature is used; there is no fallback to Essential.
type ImageSet struct {
	Essential string
	Client    string
	CephFS    string
	RBDAux    string
	RGW       string
	All       string
}

func (s ImageSet) normalized() (ImageSet, error) {
	for _, image := range []string{s.Essential, s.Client, s.CephFS, s.RBDAux, s.RGW, s.All} {
		if image != strings.TrimSpace(image) {
			return s, fmt.Errorf("image references must not contain surrounding whitespace")
		}
	}
	if s.All != "" {
		if s.Essential != "" || s.Client != "" || s.CephFS != "" || s.RBDAux != "" || s.RGW != "" {
			return s, fmt.Errorf("all cannot be combined with role images")
		}
		s.Essential, s.Client, s.CephFS, s.RBDAux, s.RGW = s.All, s.All, s.All, s.All, s.All
		s.All = ""
	}
	if s.Essential == "" {
		return s, fmt.Errorf("essential image is required")
	}
	return s, nil
}

// RunWithImages starts a cluster with explicit role images. Service image options
// in opts (cephfs.WithMDSImage, rgw.WithImage) replace the corresponding role of
// images, whatever the order of the options.
func RunWithImages(ctx context.Context, images ImageSet, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	images, err := images.normalized()
	if err != nil {
		return nil, invalidRequest("image set: %v", err)
	}

	return Run(ctx, images.Essential, append([]testcontainers.ContainerCustomizer{WithImages(images)}, opts...)...)
}

// WithImages selects role images. Essential must equal the Run image. A service
// image option replaces its role of the set whichever came first.
func WithImages(images ImageSet) Option {
	return func(o *options) error {
		normalized, err := images.normalized()
		if err != nil {
			return err
		}
		if o.imageSet != nil {
			return fmt.Errorf("image set selected twice")
		}
		o.imageSet = &normalized
		return nil
	}
}

func configureImages(o *options) error {
	if o.imageSet == nil {
		images := o.defaultImages()
		o.imageSet = &images
	}
	s := o.imageSet
	if o.essentialImage != s.Essential {
		return fmt.Errorf("Run image must equal the essential image with WithImages")
	}
	// A service image option replaces its role whichever of it and WithImages
	// came first.
	if o.cephfsOverride != "" {
		s.CephFS = o.cephfsOverride
	}
	if o.rgwOverride != "" {
		s.RGW = o.rgwOverride
	}
	for _, service := range o.services {
		switch service.Kind() {
		case "rbd":
			if s.Client == "" {
				return fmt.Errorf("client image is required by initial RBD services")
			}
		case "cephfs":
			if s.CephFS == "" || s.Client == "" {
				return fmt.Errorf("cephfs and client images are required by initial CephFS services")
			}
		case "rgw":
			if s.RGW == "" {
				return fmt.Errorf("rgw image is required by initial gateways")
			}
		}
	}
	return nil
}

// ClientContainer returns the owned userspace toolkit executor. It starts
// lazily for a separate client image; aggregate images reuse the control-plane executor.
func (c *Container) ClientContainer(ctx context.Context) (testcontainers.Container, error) {
	if c == nil {
		return nil, fmt.Errorf("ceph cluster is unavailable")
	}
	if err := c.sharedGate("ClientContainer"); err != nil {
		return nil, err
	}
	// Initial attachment must observe one MON configuration: a concurrent
	// topology refresh cannot inventory a toolkit that has not been created yet.
	if images := c.staticFacts().Images; images.Client != "" && images.Client != images.Essential {
		if err := c.lockTopology(ctx); err != nil {
			return nil, err
		}
		defer c.mu.Unlock()
	}
	return c.clientContainerContext(ctx)
}

// Uses its own mutex: service callers can already hold the topology mutex.
func (c *Container) clientContainerContext(ctx context.Context) (testcontainers.Container, error) {
	if c == nil {
		return nil, fmt.Errorf("ceph cluster is unavailable")
	}
	images := c.staticFacts().Images
	if images.Client == images.Essential {
		return c.controlContainerContext(ctx)
	}
	if images.Client == "" {
		return nil, invalidRequest("client image is required for this operation")
	}
	if err := c.toolkit.lock(ctx); err != nil {
		return nil, err
	}
	defer c.toolkit.unlock()
	if c.nativeCmds.isClosed() {
		return nil, errClusterTerminated
	}
	if c.toolkit.startupAttempted() {
		if !c.toolkit.startupReady() {
			return nil, fmt.Errorf("client startup failed; terminate the partial cluster")
		}
		return c.toolkit.current(), nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	if !c.nativeCmds.beginClientStartup(cancel) {
		cancel()
		return nil, errClusterTerminated
	}
	defer func() { c.nativeCmds.endClientStartup(); cancel() }()
	c.toolkit.markAttempted()
	ctr, err := testcontainers.Run(ctx, images.Client, append([]testcontainers.ContainerCustomizer{
		c.adminClient(), c.startup.ownedLabel(), WithIdleEntrypoint(),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})),
	}, c.runDir.mounts()...)...)
	c.toolkit.publish(ctr) // A partial start remains owned and is never retried.
	if err == nil && ctr != nil {
		output, versionErr := command(ctx, ctr, "ceph", "--version")
		if versionErr != nil {
			err = versionErr
		} else {
			version, parseErr := parseCephVersion(string(output))
			if parseErr != nil {
				err = parseErr
			} else if version != c.staticFacts().CephVersion { // Unchanged: daemons with WithDaemonImage do not take part.
				err = fmt.Errorf("client Ceph release %s differs from core %s", version, c.staticFacts().CephVersion)
			}
		}
	}
	if c.nativeCmds.isClosed() {
		err = errClusterTerminated
	}
	if err == nil && ctr == nil {
		err = fmt.Errorf("client startup returned no container")
	}
	c.toolkit.markReady(err == nil && ctr != nil)
	return ctr, err
}

// featureClientSnapshot bounds publication waits during topology/native admission.
func (c *Container) featureClientSnapshot(ctx context.Context) (testcontainers.Container, error) {
	return c.toolkit.snapshot(ctx)
}
