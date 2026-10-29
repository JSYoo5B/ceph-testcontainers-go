package ceph

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// tagged is a caller customizer that is not an Option, so it is identified by
// its tag in a composed plan.
type tagged struct{ tag string }

func (tagged) Customize(*testcontainers.GenericContainerRequest) error { return nil }

func tags(t *testing.T, customizers []testcontainers.ContainerCustomizer) []string {
	t.Helper()
	var out []string
	for _, c := range customizers {
		tc, ok := c.(tagged)
		if !ok {
			t.Fatalf("unexpected customizer %T", c)
		}
		out = append(out, tc.tag)
	}
	return out
}

func mustCompose(t *testing.T, configs ...RunOptions) runPlan {
	t.Helper()
	plan, err := composeRunOptions(configs)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	return plan
}

func mustRefuse(t *testing.T, configs ...RunOptions) error {
	t.Helper()
	plan, err := composeRunOptions(configs)
	if err == nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("composition was not refused as an invalid request: plan=%+v err=%v", plan, err)
	}
	return err
}

// Zero, one and several configuration values.
func TestRunOptionsZeroOneAndMultipleValues(t *testing.T) {
	for name, configs := range map[string][]RunOptions{"no value": nil, "zero value": {{}}, "two zero values": {{}, {}}} {
		plan := mustCompose(t, configs...)
		if plan.essential != DefaultImage || plan.images != nil || len(plan.customizers) != 0 {
			t.Fatalf("%s: defaults changed: %+v", name, plan)
		}
	}
	one := mustCompose(t, RunOptions{}.WithImage("core"))
	if one.essential != "core" || one.images != nil {
		t.Fatalf("one aggregate image: %+v", one)
	}
	set := ImageSet{Essential: "core", Client: "toolkit"}
	roles := mustCompose(t, RunOptions{}.WithImageSet(set))
	if roles.essential != "core" || roles.images == nil || *roles.images != set {
		t.Fatalf("one image set: %+v", roles)
	}
	all := mustCompose(t, RunOptions{}.WithImageSet(ImageSet{All: "complete"}))
	if want := (ImageSet{Essential: "complete", Client: "complete", CephFS: "complete", RBDAux: "complete", RGW: "complete"}); all.images == nil || *all.images != want {
		t.Fatalf("aggregate set: %+v", all)
	}
	// Customizers of several values keep argument order, and a value that
	// says nothing about images does not erase another value's selection.
	multi := mustCompose(t,
		RunOptions{}.WithCustomizers(tagged{"a"}, tagged{"b"}),
		RunOptions{}.WithImage("core"),
		RunOptions{}.WithCustomizers(tagged{"c"}),
	)
	if got := tags(t, multi.customizers); !slices.Equal(got, []string{"a", "b", "c"}) || multi.essential != "core" {
		t.Fatalf("multiple values: %v %+v", got, multi)
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
		t.Run(name, func(t *testing.T) { mustRefuse(t, config) })
	}
	// The same refusals hold when other values carry defaults or selections.
	mustRefuse(t, RunOptions{}, RunOptions{}.WithImage(""))
	// A value with only customizers is unspecified, not an empty selection.
	mustCompose(t, RunOptions{}.WithCustomizers())
}

// Repeated and conflicting selections are refused, also when they agree.
func TestRunOptionsRefuseRepeatedSelection(t *testing.T) {
	set := ImageSet{Essential: "core", Client: "toolkit"}
	for name, configs := range map[string][]RunOptions{
		"two identical images":      {RunOptions{}.WithImage("core"), RunOptions{}.WithImage("core")},
		"two different images":      {RunOptions{}.WithImage("a"), RunOptions{}.WithImage("b")},
		"image then set":            {RunOptions{}.WithImage("core"), RunOptions{}.WithImageSet(set)},
		"set then image":            {RunOptions{}.WithImageSet(set), RunOptions{}.WithImage("core")},
		"two identical sets":        {RunOptions{}.WithImageSet(set), RunOptions{}.WithImageSet(set)},
		"selection then unselected": {RunOptions{}.WithImage("core"), {}, RunOptions{}.WithImage("core")},
	} {
		t.Run(name, func(t *testing.T) {
			err := mustRefuse(t, configs...)
			if !strings.Contains(err.Error(), "image selected by RunOptions") {
				t.Fatalf("refusal does not name the conflict: %v", err)
			}
		})
	}
}

// Within one value the later selection replaces the earlier one completely.
func TestRunOptionsLaterSelectionOfOneValueReplacesTheEarlier(t *testing.T) {
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

// Derived values and caller-owned inputs stay independent.
func TestRunOptionsDerivedValuesAreIndependent(t *testing.T) {
	// Appending one at a time leaves spare capacity, which would expose a
	// backing array shared with the derived values.
	base := RunOptions{}.WithImage("core").WithCustomizers(tagged{"a"}).WithCustomizers(tagged{"b"}).WithCustomizers(tagged{"c"})
	first := base.WithCustomizers(tagged{"first"})
	second := base.WithCustomizers(tagged{"second"})
	if got := tags(t, mustCompose(t, first).customizers); !slices.Equal(got, []string{"a", "b", "c", "first"}) {
		t.Fatalf("first derived value: %v", got)
	}
	if got := tags(t, mustCompose(t, second).customizers); !slices.Equal(got, []string{"a", "b", "c", "second"}) {
		t.Fatalf("second derived value: %v", got)
	}
	if got := tags(t, mustCompose(t, base).customizers); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("base changed by its derived values: %v", got)
	}
	// Selection methods leave the base unselected, and derived selections differ.
	unselected := RunOptions{}.WithCustomizers(tagged{"x"})
	a, b := unselected.WithImage("image-a"), unselected.WithImage("image-b")
	if mustCompose(t, a).essential != "image-a" || mustCompose(t, b).essential != "image-b" || mustCompose(t, unselected).essential != DefaultImage {
		t.Fatal("derived image selections leaked into each other or into the base")
	}

	// The caller's slice and set are copied.
	input := []testcontainers.ContainerCustomizer{tagged{"one"}}
	config := RunOptions{}.WithCustomizers(input...)
	input[0] = tagged{"changed"}
	if got := tags(t, mustCompose(t, config).customizers); !slices.Equal(got, []string{"one"}) {
		t.Fatalf("the value shares the caller's slice: %v", got)
	}
	images := ImageSet{Essential: "core", Client: "toolkit"}
	config = RunOptions{}.WithImageSet(images)
	images.Client = "changed"
	plan := mustCompose(t, config)
	if plan.images.Client != "toolkit" {
		t.Fatalf("the value shares the caller's image set: %+v", plan.images)
	}
	// The composed plan owns its slices as well.
	plan.images.Client = "mutated"
	plan.customizers = append(plan.customizers[:0], tagged{"mutated"})
	if again := mustCompose(t, config); again.images.Client != "toolkit" {
		t.Fatalf("a plan shares the value's image set: %+v", again.images)
	}
	composed := mustCompose(t, unselected, unselected.WithCustomizers(tagged{"y"}))
	composed.customizers[0] = tagged{"mutated"}
	if got := tags(t, mustCompose(t, unselected).customizers); got[0] != "x" {
		t.Fatalf("a plan shares the value's customizers: %v", got)
	}
}

// A refused configuration reaches no customizer and no external effect, even
// when the context is already canceled and even for an Option that comes first.
func TestRunRefusesBeforeAnyEffect(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	ran := false
	probe := Option(func(*options) error { ran = true; return nil })
	for name, configs := range map[string][]RunOptions{
		"conflicting selections": {RunOptions{}.WithCustomizers(probe), RunOptions{}.WithImage("a"), RunOptions{}.WithImage("b")},
		"empty selection":        {RunOptions{}.WithCustomizers(probe).WithImage("")},
		"nil customizer":         {RunOptions{}.WithCustomizers(probe, nil)},
		"nil option":             {RunOptions{}.WithCustomizers(probe, Option(nil))},
	} {
		ran = false
		c, err := Run(canceled, configs...)
		if c != nil || !errors.Is(err, ErrInvalidRequest) || errors.Is(err, context.Canceled) {
			t.Fatalf("%s: refusal must precede cancellation handling: %v %v", name, c, err)
		}
		if ran {
			t.Fatalf("%s: an option ran before the refusal", name)
		}
	}
	// A valid configuration is applied and then meets the canceled context,
	// which proves that the refusals above were decided by validation.
	ran = false
	c, err := Run(canceled, RunOptions{}.WithImage("core").WithCustomizers(probe))
	if c != nil || !errors.Is(err, context.Canceled) || !ran {
		t.Fatalf("valid configuration did not reach admission: %v %v ran=%t", c, err, ran)
	}
}

// Run without a value starts the default image and still validates the rest.
func TestRunDefaultSelectionReachesAdmission(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	var effective *options
	capture := Option(func(o *options) error { effective = o; return nil })
	c, err := Run(canceled, RunOptions{}.WithCustomizers(capture))
	if c != nil || !errors.Is(err, context.Canceled) || effective == nil {
		t.Fatalf("default selection: %v %v", c, err)
	}
	if got := effective.effectiveImages(); got.Essential != DefaultImage || got.Client != DefaultImage || got.RGW != DefaultImage {
		t.Fatalf("default images = %+v", got)
	}
	if effective.essentialImage != DefaultImage {
		t.Fatalf("essential image = %q", effective.essentialImage)
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
		err := configureImages(&o)
		if (err != nil) != test.wantErr {
			t.Fatalf("%s with %+v: err=%v, wantErr=%t", test.kind, test.set, err, test.wantErr)
		}
		// The aggregate selection supplies every role.
		aggregate := options{essentialImage: "complete", services: []serviceBootstrap{kindOnly{kind: test.kind}}}
		if err := configureImages(&aggregate); err != nil {
			t.Fatalf("aggregate image refused for %s: %v", test.kind, err)
		}
	}
}

// WithSharedRunOptions accumulates its calls into one value; a selection in
// the configuration replaces the NewShared image.
func TestSharedRunOptionsSelectionPrecedence(t *testing.T) {
	var settings sharedSettings
	WithSharedRunOptions(RunOptions{}.WithCustomizers(tagged{"a"}))(&settings)
	WithSharedRunOptions(RunOptions{}.WithCustomizers(tagged{"b"}))(&settings)
	if got := tags(t, settings.runConfig.customizers); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("shared customizers: %v", got)
	}
	if settings.runConfig.selection != imagesUnspecified {
		t.Fatal("customizers alone selected images")
	}
	WithSharedRunOptions(RunOptions{}.WithImageSet(ImageSet{Essential: "core", Client: "toolkit"}))(&settings)
	plan := mustCompose(t, sharedRunConfig("aggregate", settings.runConfig))
	if plan.essential != "core" || plan.images == nil || plan.images.Client != "toolkit" {
		t.Fatalf("the configured selection did not replace the NewShared image: %+v", plan)
	}
	if plan := mustCompose(t, sharedRunConfig("aggregate", RunOptions{}.WithCustomizers(tagged{"a"}))); plan.essential != "aggregate" || plan.images != nil {
		t.Fatalf("the NewShared image was not used: %+v", plan)
	}
}
