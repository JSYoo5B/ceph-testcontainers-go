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

// RunOptions is the grouped configuration of Run. The zero value is valid and
// selects every default. Each With method returns a modified copy and leaves
// its receiver unchanged, so one base value can derive several independent
// configurations:
//
//	base := ceph.RunOptions{}.WithCustomizers(ceph.WithOSDCount(1))
//	first := base.WithImage(imageA)
//	second := base.WithImageSet(images)
//
// Run accepts any number of values and composes them in argument order; see
// Run for the composition rules. A value owns its slices: methods copy what
// they store, and derived values never share backing arrays with their base.
type RunOptions struct {
	selection imageSelection
	image     string
	images    ImageSet
	// customizers keeps the caller's order. Option values configure the
	// cluster and are never forwarded to a container; every other customizer
	// applies to the first MON's container request.
	customizers []testcontainers.ContainerCustomizer
}

// WithImage selects one image that supplies every role: it starts the MON,
// MGR and OSD daemons and also serves as the client, CephFS, RBD auxiliary and
// RGW runtime. Use WithImageSet for separate role images. Selecting an image
// replaces an earlier WithImage or WithImageSet of the same value. The image
// reference must be non-empty and have no surrounding whitespace; Run refuses
// anything else before it creates a resource.
func (o RunOptions) WithImage(image string) RunOptions {
	o.selection, o.image, o.images = imagesAggregate, image, ImageSet{}
	o.customizers = slices.Clone(o.customizers)
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
	o.customizers = slices.Clone(o.customizers)
	return o
}

// WithCustomizers appends caller customizers after those already present,
// preserving their order. An Option (WithOSDCount, cephfs.WithFilesystems and
// the other cluster options) changes the cluster settings in that order. Any
// other testcontainers.ContainerCustomizer is the retained escape point to the
// dependency: it applies to the first MON's container request after the
// module's own settings, and its effects are not validated here. A nil
// customizer is refused by Run before any resource is created.
func (o RunOptions) WithCustomizers(customizers ...testcontainers.ContainerCustomizer) RunOptions {
	o.customizers = slices.Concat(o.customizers, customizers)
	return o
}

// merge appends other's customizers and, when other selects images, replaces
// the image selection of o. It serves WithSharedRunOptions, whose repeated calls
// accumulate into one value; Run itself refuses a second selection.
func (o RunOptions) merge(other RunOptions) RunOptions {
	merged := o.WithCustomizers(other.customizers...)
	if other.selection != imagesUnspecified {
		merged.selection, merged.image, merged.images = other.selection, other.image, other.images
	}
	return merged
}

// runPlan is the effective configuration that Run executes.
type runPlan struct {
	essential   string
	images      *ImageSet // nil for an aggregate selection
	customizers []testcontainers.ContainerCustomizer
}

// composeRunOptions merges configs in argument order. The rules are in
// docs/design/run-options.md. It performs no external effect.
func composeRunOptions(configs []RunOptions) (runPlan, error) {
	var plan runPlan
	selected := -1
	for i, config := range configs {
		if config.selection != imagesUnspecified {
			if selected >= 0 {
				return runPlan{}, invalidRequest("image selected by RunOptions %d and %d; select images in one value", selected, i)
			}
			selected = i
		}
		for j, c := range config.customizers {
			if c == nil {
				return runPlan{}, invalidRequest("RunOptions %d customizer %d is nil", i, j)
			}
			if opt, ok := c.(Option); ok && opt == nil {
				return runPlan{}, invalidRequest("RunOptions %d customizer %d is a nil Option", i, j)
			}
		}
		plan.customizers = append(plan.customizers, config.customizers...)
	}
	if selected < 0 {
		plan.essential = DefaultImage
		return plan, nil
	}
	config := configs[selected]
	switch config.selection {
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
