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
// in opts override the corresponding role in images.
func RunWithImages(ctx context.Context, images ImageSet, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	images, err := images.normalized()
	if err != nil {
		return nil, invalidRequest("image set: %v", err)
	}

	return Run(ctx, images.Essential, append([]testcontainers.ContainerCustomizer{WithImages(images)}, opts...)...)
}

// WithImages selects role images. Essential must equal the Run image.
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

// Images returns the effective role images, including aggregate selections.
func (c *Container) Images() ImageSet {
	if c == nil {
		return ImageSet{}
	}
	return c.settings.effectiveImages()
}

func configureImages(o *options) error {
	if o.imageSet == nil {
		images := ImageSet{Essential: o.controlImage, Client: o.controlImage, CephFS: o.mdsImage, RBDAux: o.controlImage, RGW: o.rgwImage}
		o.imageSet = &images
	}
	s := o.imageSet
	if o.controlImage != s.Essential {
		return fmt.Errorf("Run image must equal the essential image with WithImages")
	}
	o.controlImage, o.osdImage, o.mdsImage, o.rgwImage = s.Essential, s.Essential, s.CephFS, s.RGW
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
// lazily for a separate client image; aggregate images reuse the control executor.
func (c *Container) ClientContainer(ctx context.Context) (testcontainers.Container, error) {
	if c == nil {
		return nil, fmt.Errorf("ceph cluster is unavailable")
	}
	if err := c.sharedGate("ClientContainer"); err != nil {
		return nil, err
	}
	// Initial attachment must observe one MON configuration: a concurrent
	// topology refresh cannot inventory a toolkit that has not been created yet.
	if c.Images().Client != "" && c.Images().Client != c.Images().Essential {
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
	images := c.Images()
	if images.Client == images.Essential {
		return c.controlContainerContext(ctx)
	}
	if images.Client == "" {
		return nil, invalidRequest("client image is required for this operation")
	}
	if err := lockTopologyMutex(ctx, &c.clientMu); err != nil {
		return nil, err
	}
	defer c.clientMu.Unlock()
	c.nativeMu.Lock()
	closed := c.nativeClosed
	c.nativeMu.Unlock()
	if closed {
		return nil, errClusterTerminated
	}
	if c.featureClientAttempted {
		if !c.featureClientReady {
			return nil, fmt.Errorf("client startup failed; terminate the partial cluster")
		}
		return c.featureClient, nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	c.nativeMu.Lock()
	if c.nativeClosed {
		c.nativeMu.Unlock()
		cancel()
		return nil, errClusterTerminated
	}
	c.clientStartupCancel = cancel
	c.nativeMu.Unlock()
	defer func() { c.nativeMu.Lock(); c.clientStartupCancel = nil; c.nativeMu.Unlock(); cancel() }()
	c.featureClientAttempted = true
	ctr, err := testcontainers.Run(ctx, images.Client, append([]testcontainers.ContainerCustomizer{
		c.adminClient(), c.ownedLabel(), WithIdleEntrypoint(),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})),
	}, c.runMount()...)...)
	c.clientPublishMu.Lock()
	c.featureClient = ctr // A partial start remains owned and is never retried.
	c.clientPublishMu.Unlock()
	if err == nil && ctr != nil {
		output, versionErr := command(ctx, ctr, "ceph", "--version")
		if versionErr != nil {
			err = versionErr
		} else {
			version, parseErr := parseCephVersion(string(output))
			if parseErr != nil {
				err = parseErr
			} else if version != c.CephVersion() {
				err = fmt.Errorf("client Ceph release %s differs from core %s", version, c.CephVersion())
			}
		}
	}
	c.nativeMu.Lock()
	closed = c.nativeClosed
	c.nativeMu.Unlock()
	if closed {
		err = errClusterTerminated
	}
	if err == nil && ctr == nil {
		err = fmt.Errorf("client startup returned no container")
	}
	c.featureClientReady = err == nil && ctr != nil
	return ctr, err
}

// featureClientSnapshot bounds publication waits during topology/native admission.
func (c *Container) featureClientSnapshot(ctx context.Context) (testcontainers.Container, error) {
	if err := lockTopologyReadMutex(ctx, &c.clientPublishMu); err != nil {
		return nil, err
	}
	defer c.clientPublishMu.RUnlock()
	return c.featureClient, nil
}
