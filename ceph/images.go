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

// ImageOptions is the image group of the Ceph configuration: a plain value with
// chainable, value-returning methods. It is not a customizer. Pass it to Run
// through its one grouped option function, WithImageOptions, which Run applies
// and validates like any Option before it creates a resource. The zero value
// changes nothing.
//
//	cluster, err := ceph.Run(ctx, "", ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(images)))
//
// Each method sets one field. Within one value a later call of the same method
// replaces the earlier one. Run refuses a field that two WithImageOptions
// options both set, even when they agree, so argument order never decides a
// conflict. The pattern for every configuration group, and the composition
// rules, are in docs/contributing/configuration-composition.md.
type ImageOptions struct {
	images            ImageSet
	imagesSet         bool
	cephfs, rgw       string
	cephfsSet, rgwSet bool
}

// WithImageOptions is the grouped option function that carries an ImageOptions
// to Run. Like every Option it changes the cluster settings only and never
// reaches a container request.
func WithImageOptions(config ImageOptions) Option {
	return func(o *options) error { return config.apply(o) }
}

// WithImageSet selects explicit role images. The set is the one authoritative
// image configuration: Essential starts the MON, MGR and OSD daemons, so the
// image argument of Run must be empty or equal to Essential and is not a second
// input. An All set supplies every role. A role that a requested feature needs
// and the set leaves empty is refused; there is no fallback to Essential. An
// explicit empty set is refused, not read as unspecified. WithCephFSImage and
// WithRGWImage replace the matching role of the set. The ImageSet is copied.
func (o ImageOptions) WithImageSet(images ImageSet) ImageOptions {
	o.images, o.imagesSet = images, true
	return o
}

// WithCephFSImage selects the image of the CephFS role: every metadata server
// of the cluster, those started by Run with cephfs.WithFilesystems as well as
// those started later. Without it MDS daemons use the Run image. Images().CephFS
// reports it, the usual image argument of cephfs/mirror.Run. With WithImageSet it replaces the set's CephFS role. Use matching
// Ceph versions. A blank image is refused.
func (o ImageOptions) WithCephFSImage(image string) ImageOptions {
	o.cephfs, o.cephfsSet = image, true
	return o
}

// WithRGWImage selects the image of the RGW role: every gateway of the cluster,
// those started by Run with rgw.WithGateways as well as those started later
// with rgw.Start. Without it gateways use the Run image. Images().RGW reports
// it, so the multisite setup clients use it too; multisite gateways take their
// image as an argument of multisite.Run instead. With
// WithImageSet it replaces the set's RGW role. Use matching Ceph versions. A
// blank image is refused.
func (o ImageOptions) WithRGWImage(image string) ImageOptions {
	o.rgw, o.rgwSet = image, true
	return o
}

// apply validates the fields o set and, only if all are valid, writes them to
// the settings. It refuses a field that an earlier WithImageOptions already set.
func (o ImageOptions) apply(settings *options) error {
	if o.imagesSet && settings.imageSet != nil {
		return fmt.Errorf("image set selected twice")
	}
	if o.cephfsSet && settings.cephfsOverride != "" {
		return fmt.Errorf("CephFS image selected twice")
	}
	if o.rgwSet && settings.rgwOverride != "" {
		return fmt.Errorf("RGW image selected twice")
	}
	if o.cephfsSet && strings.TrimSpace(o.cephfs) == "" {
		return fmt.Errorf("CephFS image must not be blank")
	}
	if o.rgwSet && strings.TrimSpace(o.rgw) == "" {
		return fmt.Errorf("RGW image must not be blank")
	}
	var normalized ImageSet
	if o.imagesSet {
		var err error
		if normalized, err = o.images.normalized(); err != nil {
			return err
		}
		settings.imageSet = &normalized
	}
	if o.cephfsSet {
		settings.cephfsOverride = o.cephfs
	}
	if o.rgwSet {
		settings.rgwOverride = o.rgw
	}
	return nil
}

func configureImages(o *options) error {
	if o.imageSet == nil {
		images := o.defaultImages()
		o.imageSet = &images
	}
	s := o.imageSet
	// With an image set, the Run image is not a second input: empty or the set's
	// essential image.
	if o.essentialImage == "" {
		o.essentialImage = s.Essential
	}
	if o.essentialImage != s.Essential {
		return fmt.Errorf("Run image %q must be empty or equal the essential image %q of the image set", o.essentialImage, s.Essential)
	}
	// WithCephFSImage and WithRGWImage replace their role of the image set.
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
			} else if version != c.staticFacts().CephVersion { // Unchanged: daemons with DaemonOptions.WithImage do not take part.
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
