package ceph

import (
	"slices"
	"strings"

	"github.com/testcontainers/testcontainers-go"
)

// imageSelection records how a RunOptions value selected the cluster images.
// The zero value means that the value says nothing about images, which is
// different from an explicit selection of an empty image.
type imageSelection int

const (
	imagesUnspecified imageSelection = iota
	imagesAggregate                  // WithImage: one image supplies every role.
	imagesRoles                      // WithImageSet: explicit role images.
)

// RunOptions is the grouped Ceph configuration of Run: the image selection and
// the cluster and service settings. The zero value is valid and selects every
// default. Each With method returns a modified copy and leaves its receiver
// unchanged, so one base value can derive several independent configurations:
//
//	base := ceph.RunOptions{}.WithOptions(ceph.WithOSDCount(1))
//	first := base.WithImage(imageA)
//	second := base.WithImageSet(images)
//
// Docker and container execution policies are not part of this value. They are
// the functional testcontainers customizers that Run takes after it; see Run.
// A value owns its slices: methods copy what they store, and derived values
// never share backing arrays with their base.
type RunOptions struct {
	selection imageSelection
	image     string
	images    ImageSet
	options   []Option
}

// WithImage selects one image that supplies every role: it starts the MON,
// MGR and OSD daemons and also serves as the client, CephFS, RBD auxiliary and
// RGW runtime. Use WithImageSet for separate role images. Selecting an image
// replaces an earlier WithImage or WithImageSet of the same value. The image
// reference must be non-empty and have no surrounding whitespace; Run refuses
// anything else before it creates a resource.
func (o RunOptions) WithImage(image string) RunOptions {
	o.selection, o.image, o.images = imagesAggregate, image, ImageSet{}
	o.options = slices.Clone(o.options)
	return o
}

// WithImageSet selects explicit role images. The set is the one authoritative
// image configuration: Essential starts the MON, MGR and OSD daemons and is
// not given a second time. A role that a requested feature needs and the set
// leaves empty is refused; there is no fallback to Essential. Selecting a set
// replaces an earlier WithImage or WithImageSet of the same value, and the
// caller's ImageSet is copied. Service image options (cephfs.WithMDSImage,
// rgw.WithImage) replace the matching role of the set whatever the order.
func (o RunOptions) WithImageSet(images ImageSet) RunOptions {
	o.selection, o.image, o.images = imagesRoles, "", images
	o.options = slices.Clone(o.options)
	return o
}

// WithOptions appends cluster and service settings (WithOSDCount,
// rbd.WithPools, cephfs.WithFilesystems, rgw.WithGateways, cephfs.WithMDSImage
// and the other Option values) after those already present. They are applied to
// the cluster settings in that order and validated together before anything is
// created. A nil Option is refused by Run.
//
// An Option is a module setting, not a container customizer: it must not be
// passed to Run as a trailing customizer, where it would have no effect, and
// Run refuses it there.
func (o RunOptions) WithOptions(options ...Option) RunOptions {
	o.options = append(slices.Clone(o.options), options...)
	return o
}

// merge appends other's options and, when other selects images, replaces the
// image selection of o. It serves WithSharedRunOptions, whose repeated calls
// accumulate into one value; Run itself takes one value.
func (o RunOptions) merge(other RunOptions) RunOptions {
	merged := o.WithOptions(other.options...)
	if other.selection != imagesUnspecified {
		merged.selection, merged.image, merged.images = other.selection, other.image, other.images
	}
	return merged
}

// runPlan is the effective configuration that Run executes.
type runPlan struct {
	essential   string
	images      *ImageSet // nil for an aggregate selection
	options     []Option
	customizers []testcontainers.ContainerCustomizer
}

// composeRun assembles the effective configuration from the Ceph configuration
// and the trailing container customizers. It validates everything that needs no
// cluster state and performs no external effect.
func composeRun(config RunOptions, customizers []testcontainers.ContainerCustomizer) (runPlan, error) {
	plan := runPlan{options: slices.Clone(config.options), customizers: slices.Clone(customizers)}
	for i, option := range plan.options {
		if option == nil {
			return runPlan{}, invalidRequest("RunOptions option %d is nil", i)
		}
	}
	for i, c := range plan.customizers {
		if c == nil {
			return runPlan{}, invalidRequest("container customizer %d is nil", i)
		}
		if _, ok := c.(Option); ok {
			// Option.Customize does nothing on purpose: forwarding a module
			// setting as a container customizer would drop it silently.
			return runPlan{}, invalidRequest("container customizer %d is a ceph.Option, which a container customizer cannot carry; pass it with RunOptions.WithOptions", i)
		}
	}
	switch config.selection {
	case imagesUnspecified:
		plan.essential = DefaultImage
	case imagesAggregate:
		if config.image == "" {
			return runPlan{}, invalidRequest("image: essential image is required")
		}
		if config.image != strings.TrimSpace(config.image) {
			return runPlan{}, invalidRequest("image: image references must not contain surrounding whitespace")
		}
		plan.essential = config.image
	case imagesRoles:
		normalized, err := config.images.normalized()
		if err != nil {
			return runPlan{}, invalidRequest("image set: %v", err)
		}
		plan.essential, plan.images = normalized.Essential, &normalized
	default:
		return runPlan{}, invalidRequest("unknown image selection %d", config.selection)
	}
	return plan, nil
}
