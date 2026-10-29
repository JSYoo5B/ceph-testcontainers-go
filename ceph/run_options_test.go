package ceph

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// recorder returns Options that append their name to applied when Run applies
// them, so the order and the presence of settings can be asserted.
type recorder struct{ applied []string }

func (r *recorder) option(name string) Option {
	return func(*options) error { r.applied = append(r.applied, name); return nil }
}

// rawTag is a container customizer that is not an Option.
type rawTag struct{ tag string }

func (rawTag) Customize(*testcontainers.GenericContainerRequest) error { return nil }

func rawTags(t *testing.T, customizers []testcontainers.ContainerCustomizer) []string {
	t.Helper()
	var out []string
	for _, c := range customizers {
		tc, ok := c.(rawTag)
		if !ok {
			t.Fatalf("unexpected customizer %T", c)
		}
		out = append(out, tc.tag)
	}
	return out
}

func applyAll(t *testing.T, plan runPlan) {
	t.Helper()
	var o options
	for _, option := range plan.options {
		if err := option(&o); err != nil {
			t.Fatal(err)
		}
	}
}

func mustCompose(t *testing.T, config RunOptions, customizers ...testcontainers.ContainerCustomizer) runPlan {
	t.Helper()
	plan, err := composeRun(config, customizers)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	return plan
}

func mustRefuse(t *testing.T, config RunOptions, customizers ...testcontainers.ContainerCustomizer) error {
	t.Helper()
	plan, err := composeRun(config, customizers)
	if err == nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("composition was not refused as an invalid request: %+v %v", plan, err)
	}
	return err
}

// The zero value and one or several With calls.
func TestRunOptionsZeroOneAndSeveralCalls(t *testing.T) {
	plan := mustCompose(t, RunOptions{})
	if plan.essential != DefaultImage || plan.images != nil || len(plan.options) != 0 || len(plan.customizers) != 0 {
		t.Fatalf("defaults changed: %+v", plan)
	}
	if one := mustCompose(t, RunOptions{}.WithImage("core")); one.essential != "core" || one.images != nil {
		t.Fatalf("one aggregate image: %+v", one)
	}
	set := ImageSet{Essential: "core", Client: "toolkit"}
	if roles := mustCompose(t, RunOptions{}.WithImageSet(set)); roles.essential != "core" || roles.images == nil || *roles.images != set {
		t.Fatalf("one image set: %+v", roles)
	}
	all := mustCompose(t, RunOptions{}.WithImageSet(ImageSet{All: "complete"}))
	if want := (ImageSet{Essential: "complete", Client: "complete", CephFS: "complete", RBDAux: "complete", RGW: "complete"}); all.images == nil || *all.images != want {
		t.Fatalf("aggregate set: %+v", all)
	}
	// Options keep call order, the image selection is independent of them, and
	// the trailing customizers keep argument order.
	var r recorder
	config := RunOptions{}.WithOptions(r.option("a"), r.option("b")).WithImage("core").WithOptions(r.option("c"))
	multi := mustCompose(t, config, rawTag{"x"}, rawTag{"y"})
	applyAll(t, multi)
	if !slices.Equal(r.applied, []string{"a", "b", "c"}) || multi.essential != "core" || !slices.Equal(rawTags(t, multi.customizers), []string{"x", "y"}) {
		t.Fatalf("several calls: applied=%v %+v", r.applied, multi)
	}
}

// An explicit empty selection is different from an unspecified one.
func TestRunOptionsExplicitEmptyIsNotUnspecified(t *testing.T) {
	for name, config := range map[string]RunOptions{
		"empty image":       RunOptions{}.WithImage(""),
		"empty image set":   RunOptions{}.WithImageSet(ImageSet{}),
		"set without core":  RunOptions{}.WithImageSet(ImageSet{Client: "toolkit"}),
		"padded image":      RunOptions{}.WithImage(" core"),
		"padded role":       RunOptions{}.WithImageSet(ImageSet{Essential: "core", Client: "toolkit "}),
		"all with a role":   RunOptions{}.WithImageSet(ImageSet{All: "complete", Client: "toolkit"}),
		"empty over a real": RunOptions{}.WithImage("core").WithImage(""),
	} {
		t.Run(name, func(t *testing.T) { _ = mustRefuse(t, config) })
	}
	// No selection and no options is unspecified, not an empty selection.
	mustCompose(t, RunOptions{}.WithOptions())
}

// Within one value the later selection replaces the earlier one completely.
func TestRunOptionsLaterSelectionReplacesTheEarlier(t *testing.T) {
	set := ImageSet{Essential: "core", Client: "toolkit"}
	plan := mustCompose(t, RunOptions{}.WithImageSet(set).WithImage("single"))
	if plan.essential != "single" || plan.images != nil {
		t.Fatalf("aggregate image left a stale set: %+v", plan)
	}
	plan = mustCompose(t, RunOptions{}.WithImage("single").WithImageSet(set))
	if plan.essential != "core" || plan.images == nil || *plan.images != set {
		t.Fatalf("set left a stale image: %+v", plan)
	}
}

// Derived values and caller-owned inputs stay independent, and an explicit
// empty slice stays a non-nil empty slice (nil means unspecified for the
// validators that distinguish them).
func TestRunOptionsDerivedValuesAreIndependent(t *testing.T) {
	var r recorder
	// Appending one at a time leaves spare capacity, which would expose a
	// backing array shared with the derived values.
	base := RunOptions{}.WithImage("core").WithOptions(r.option("a")).WithOptions(r.option("b")).WithOptions(r.option("c"))
	first, second := base.WithOptions(r.option("first")), base.WithOptions(r.option("second"))
	for want, config := range map[string]RunOptions{"first": first, "second": second} {
		r.applied = nil
		applyAll(t, mustCompose(t, config))
		if !slices.Equal(r.applied, []string{"a", "b", "c", want}) {
			t.Fatalf("derived %s: %v", want, r.applied)
		}
	}
	r.applied = nil
	applyAll(t, mustCompose(t, base))
	if !slices.Equal(r.applied, []string{"a", "b", "c"}) {
		t.Fatalf("base changed by its derived values: %v", r.applied)
	}
	unselected := RunOptions{}.WithOptions(r.option("x"))
	if mustCompose(t, unselected.WithImage("a")).essential != "a" || mustCompose(t, unselected.WithImage("b")).essential != "b" || mustCompose(t, unselected).essential != DefaultImage {
		t.Fatal("derived image selections leaked into each other or into the base")
	}

	// The caller's slices and set are copied, on the way in and out of a plan.
	input := []Option{r.option("one")}
	config := RunOptions{}.WithOptions(input...)
	input[0] = r.option("changed")
	r.applied = nil
	applyAll(t, mustCompose(t, config))
	if !slices.Equal(r.applied, []string{"one"}) {
		t.Fatalf("the value shares the caller's slice: %v", r.applied)
	}
	raw := []testcontainers.ContainerCustomizer{rawTag{"one"}}
	plan := mustCompose(t, RunOptions{}, raw...)
	raw[0] = rawTag{"changed"}
	if got := rawTags(t, plan.customizers); !slices.Equal(got, []string{"one"}) {
		t.Fatalf("the plan shares the caller's customizers: %v", got)
	}
	images := ImageSet{Essential: "core", Client: "toolkit"}
	withSet := RunOptions{}.WithImageSet(images)
	images.Client = "changed"
	plan = mustCompose(t, withSet)
	plan.images.Client = "mutated"
	if again := mustCompose(t, withSet); again.images.Client != "toolkit" {
		t.Fatalf("a set is shared with a plan or the caller: %+v", again.images)
	}
	plan = mustCompose(t, unselected)
	plan.options[0] = r.option("mutated")
	r.applied = nil
	applyAll(t, mustCompose(t, unselected))
	if !slices.Equal(r.applied, []string{"x"}) {
		t.Fatalf("a plan shares the value's options: %v", r.applied)
	}

	// nil stays nil and a non-nil empty slice stays non-nil through the clones.
	empty := RunOptions{options: []Option{}}
	if got := empty.WithImage("core").options; got == nil {
		t.Fatal("WithImage collapsed an explicit empty option list to nil")
	}
	if got := empty.WithOptions().options; got == nil {
		t.Fatal("WithOptions collapsed an explicit empty option list to nil")
	}
	if got := (RunOptions{}).WithImage("core").options; got != nil {
		t.Fatal("WithImage turned an unspecified option list into an empty one")
	}
}

// A service setting is never lost: it reaches the settings through
// WithOptions, and a trailing container customizer position refuses it.
func TestRunOptionsServiceSettingIsAppliedOrRefusedNeverDropped(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	var effective *options
	capture := Option(func(o *options) error { effective = o; return nil })
	service := (serviceHooks{}).WithServiceImage("rgw", "selected-gateway")

	c, err := Run(canceled, RunOptions{}.WithImageSet(ImageSet{Essential: "core", Client: "toolkit"}).WithOptions(service, capture))
	if c != nil || !errors.Is(err, context.Canceled) || effective == nil || effective.rgwOverride != "selected-gateway" || effective.imageSet.RGW != "selected-gateway" {
		t.Fatalf("service option did not reach the settings: %v %v %+v", c, err, effective)
	}

	effective = nil
	c, err = Run(canceled, RunOptions{}.WithOptions(capture), service)
	if c != nil || !errors.Is(err, ErrInvalidRequest) || errors.Is(err, context.Canceled) || effective != nil {
		t.Fatalf("a service option as a container customizer was not refused: %v %v applied=%t", c, err, effective != nil)
	}
	// An Option is refused there whatever else is passed.
	_ = mustRefuse(t, RunOptions{}, rawTag{"ok"}, capture)
}

// A refused configuration reaches no option and no customizer, and precedes the
// context check; a valid one is applied and then meets the canceled context.
func TestRunRefusesBeforeAnyEffect(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	var r recorder
	probe := r.option("probe")
	for name, test := range map[string]struct {
		config      RunOptions
		customizers []testcontainers.ContainerCustomizer
	}{
		"empty selection":    {RunOptions{}.WithOptions(probe).WithImage(""), nil},
		"nil option":         {RunOptions{}.WithOptions(probe, nil), nil},
		"nil customizer":     {RunOptions{}.WithOptions(probe), []testcontainers.ContainerCustomizer{nil}},
		"option as raw":      {RunOptions{}.WithOptions(probe), []testcontainers.ContainerCustomizer{probe}},
		"invalid image set":  {RunOptions{}.WithOptions(probe).WithImageSet(ImageSet{All: "a", Client: "b"}), nil},
		"padded aggregate":   {RunOptions{}.WithOptions(probe).WithImage("a "), nil},
		"role image padding": {RunOptions{}.WithOptions(probe).WithImageSet(ImageSet{Essential: "a", RGW: " b"}), nil},
	} {
		r.applied = nil
		c, err := Run(canceled, test.config, test.customizers...)
		if c != nil || !errors.Is(err, ErrInvalidRequest) || errors.Is(err, context.Canceled) || len(r.applied) != 0 {
			t.Fatalf("%s: refusal must precede option application and cancellation: %v %v applied=%v", name, c, err, r.applied)
		}
	}
	r.applied = nil
	c, err := Run(canceled, RunOptions{}.WithImage("core").WithOptions(probe))
	if c != nil || !errors.Is(err, context.Canceled) || !slices.Equal(r.applied, []string{"probe"}) {
		t.Fatalf("valid configuration did not reach admission: %v %v applied=%v", c, err, r.applied)
	}
}

// Run with the zero value starts the default image and validates the rest.
func TestRunDefaultSelectionReachesAdmission(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	var effective *options
	capture := Option(func(o *options) error { effective = o; return nil })
	c, err := Run(canceled, RunOptions{}.WithOptions(capture))
	if c != nil || !errors.Is(err, context.Canceled) || effective == nil {
		t.Fatalf("default selection: %v %v", c, err)
	}
	if got := effective.effectiveImages(); got.Essential != DefaultImage || got.Client != DefaultImage || got.RGW != DefaultImage || effective.essentialImage != DefaultImage {
		t.Fatalf("default images = %+v", got)
	}
}

type kindOnly struct {
	serviceBootstrap
	kind string
}

func (k kindOnly) Kind() string { return k.kind }

// An image set that omits a role a requested feature needs is refused; the
// essential image is never a fallback.
func TestRunImageSetMissingRoleIsRefusedWithoutFallback(t *testing.T) {
	for _, test := range []struct {
		kind    string
		set     ImageSet
		wantErr bool
	}{
		{"rbd", ImageSet{Essential: "core"}, true},
		{"rbd", ImageSet{Essential: "core", Client: "toolkit"}, false},
		{"cephfs", ImageSet{Essential: "core", Client: "toolkit"}, true},
		{"cephfs", ImageSet{Essential: "core", CephFS: "mds"}, true},
		{"cephfs", ImageSet{Essential: "core", Client: "toolkit", CephFS: "mds"}, false},
		{"rgw", ImageSet{Essential: "core", Client: "toolkit"}, true},
		{"rgw", ImageSet{Essential: "core", RGW: "gateway"}, false},
	} {
		plan := mustCompose(t, RunOptions{}.WithImageSet(test.set))
		o := options{essentialImage: plan.essential, imageSet: plan.images, services: []serviceBootstrap{kindOnly{kind: test.kind}}}
		if err := configureImages(&o); (err != nil) != test.wantErr {
			t.Fatalf("%s with %+v: err=%v, wantErr=%t", test.kind, test.set, err, test.wantErr)
		}
		aggregate := options{essentialImage: "complete", services: []serviceBootstrap{kindOnly{kind: test.kind}}}
		if err := configureImages(&aggregate); err != nil {
			t.Fatalf("aggregate image refused for %s: %v", test.kind, err)
		}
	}
}

// WithSharedRunOptions accumulates its calls; a selection in the configuration
// replaces the NewShared image, and two selections are a configuration error.
func TestSharedRunOptionsSelectionPrecedence(t *testing.T) {
	var r recorder
	var settings sharedSettings
	WithSharedRunOptions(RunOptions{}.WithOptions(r.option("a")), rawTag{"x"})(&settings)
	WithSharedRunOptions(RunOptions{}.WithOptions(r.option("b")), rawTag{"y"})(&settings)
	if settings.err != nil || settings.runConfig.selection != imagesUnspecified || len(settings.runConfig.options) != 2 || !slices.Equal(rawTags(t, settings.runCustomizers), []string{"x", "y"}) {
		t.Fatalf("accumulation: %+v", settings)
	}
	if plan := mustCompose(t, sharedRunConfig("aggregate", settings.runConfig)); plan.essential != "aggregate" || plan.images != nil {
		t.Fatalf("the NewShared image was not used: %+v", plan)
	}
	WithSharedRunOptions(RunOptions{}.WithImageSet(ImageSet{Essential: "core", Client: "toolkit"}))(&settings)
	plan := mustCompose(t, sharedRunConfig("aggregate", settings.runConfig))
	if settings.err != nil || plan.essential != "core" || plan.images == nil || plan.images.Client != "toolkit" || len(plan.options) != 2 {
		t.Fatalf("the configured selection did not replace the NewShared image: %+v %v", plan, settings.err)
	}
	WithSharedRunOptions(RunOptions{}.WithImage("other"))(&settings)
	if settings.err == nil {
		t.Fatal("a second image selection was accepted")
	}
}
