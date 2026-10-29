package ceph

// OSDOptions is the grouped configuration of AddOSD. The zero value selects
// every default: the placement that the cluster resolves for the new OSD and
// the cluster's essential image. Each With method returns a modified copy and
// leaves its receiver unchanged, so one base value can derive several
// independent configurations:
//
//	base := ceph.OSDOptions{}.WithPlacement(ceph.OSDConfig{Rack: "rack-a"})
//	first := base.WithImage(imageA)
//	second := base.WithImage(imageB)
//
// AddOSD composes any number of values; the rules are in
// docs/design/value-configuration-composition.md. Every field tracks whether
// it was set, so an explicit empty setting differs from an unspecified one, and
// the value holds only strings, so a copy never shares mutable storage.
type OSDOptions struct {
	placement    OSDConfig
	placementSet bool
	image        string
	imageSet     bool
}

// WithPlacement places the new OSD in the logical CRUSH location config
// describes (OSDConfig documents its defaults). All four fields are set
// together: an empty field is an explicit request for its default, and
// WithPlacement(OSDConfig{}) is an explicit default placement. A later call on
// the same value replaces the earlier placement entirely.
func (o OSDOptions) WithPlacement(config OSDConfig) OSDOptions {
	o.placement, o.placementSet = config, true
	return o
}

// WithImage starts the one new OSD from image instead of the cluster's
// essential image, with the same contract as WithDaemonImage: the image must
// contain the daemon's binary and the tools of the fixture's startup scripts,
// nothing here checks its release or compatibility with the running cluster,
// DetachedOSD.Reopen starts the same OSD from the same image again, and the
// OSD's handle reports it with Image. The reference must be non-empty and free
// of whitespace and control characters; an empty image is an explicit invalid
// selection, not an unspecified one. A later call on the same value replaces the
// earlier image.
func (o OSDOptions) WithImage(image string) OSDOptions {
	o.image, o.imageSet = image, true
	return o
}

// osdPlan is the effective, unvalidated-against-the-cluster configuration of
// one AddOSD call.
type osdPlan struct {
	placement OSDConfig
	image     string
	imageSet  bool
}

// composeOSDOptions merges configs in argument order and validates what needs
// no cluster state. A field may be set by at most one value; setting it in two
// is refused even when both agree. It performs no external effect.
func composeOSDOptions(configs []OSDOptions) (osdPlan, error) {
	var plan osdPlan
	placementFrom, imageFrom := -1, -1
	for i, config := range configs {
		if config.placementSet {
			if placementFrom >= 0 {
				return osdPlan{}, invalidRequest("OSD placement set by OSDOptions %d and %d; set it in one value", placementFrom, i)
			}
			placementFrom, plan.placement = i, config.placement
		}
		if config.imageSet {
			if imageFrom >= 0 {
				return osdPlan{}, invalidRequest("OSD image set by OSDOptions %d and %d; set it in one value", imageFrom, i)
			}
			if err := validateDaemonImage(config.image); err != nil {
				return osdPlan{}, invalidRequest("OSD image: %v", err)
			}
			imageFrom, plan.image, plan.imageSet = i, config.image, true
		}
	}
	return plan, nil
}
