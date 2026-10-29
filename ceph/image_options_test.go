package ceph

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// admit runs Run with an already canceled context: a configuration that passes
// validation then returns context.Canceled before any resource exists, and a
// refused one returns ErrInvalidRequest. effective is the settings that the
// options saw.
func admit(t *testing.T, img string, opts ...testcontainers.ContainerCustomizer) (effective *options, err error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	capture := Option(func(o *options) error { effective = o; return nil })
	c, err := Run(ctx, img, append(opts, capture)...)
	if c != nil {
		t.Fatalf("Run returned a cluster: %v", c)
	}
	return effective, err
}

func wantAdmitted(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("configuration was refused: %v", err)
	}
}

func wantRefused(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrInvalidRequest) || errors.Is(err, context.Canceled) {
		t.Fatalf("configuration was not refused as an invalid request: %v", err)
	}
}

// The image argument is not a second input when an image set is supplied.
func TestRunImageArgumentWithImageSet(t *testing.T) {
	set := ImageSet{Essential: "core", Client: "toolkit"}
	for name, test := range map[string]struct {
		img     string
		set     ImageSet
		refused bool
		want    string
	}{
		"empty image":              {"", set, false, "core"},
		"image equals essential":   {"core", set, false, "core"},
		"different image":          {"other", set, true, ""},
		"padded essential":         {" core", set, true, ""},
		"aggregate with empty":     {"", ImageSet{All: "complete"}, false, "complete"},
		"aggregate equal":          {"complete", ImageSet{All: "complete"}, false, "complete"},
		"aggregate different":      {"core", ImageSet{All: "complete"}, true, ""},
		"explicit empty set":       {"core", ImageSet{}, true, ""},
		"explicit empty set blank": {"", ImageSet{}, true, ""},
		"set without essential":    {"", ImageSet{Client: "toolkit"}, true, ""},
	} {
		t.Run(name, func(t *testing.T) {
			effective, err := admit(t, test.img, WithImageOptions(ImageOptions{}.WithImageSet(test.set)))
			if test.refused {
				wantRefused(t, err)
				return
			}
			wantAdmitted(t, err)
			if effective.essentialImage != test.want || effective.imageSet == nil || effective.imageSet.Essential != test.want {
				t.Fatalf("effective images: %q %+v", effective.essentialImage, effective.imageSet)
			}
		})
	}
	// Ordinary single-image use is unchanged: the argument is the aggregate.
	effective, err := admit(t, "single")
	wantAdmitted(t, err)
	if got := effective.effectiveImages(); got != (ImageSet{Essential: "single", Client: "single", CephFS: "single", RBDAux: "single", RGW: "single"}) {
		t.Fatalf("single image: %+v", got)
	}
	// An unrelated zero ImageOptions changes nothing.
	effective, err = admit(t, "single", WithImageOptions(ImageOptions{}))
	wantAdmitted(t, err)
	if effective.imageSet == nil || effective.imageSet.Client != "single" {
		t.Fatalf("zero ImageOptions changed the images: %+v", effective.imageSet)
	}
}

// At most one ImageOptions value selects an image set, also when they agree; one
// value replaces its own earlier selection.
func TestImageOptionsRefuseRepeatedImageSetSelection(t *testing.T) {
	a := ImageSet{Essential: "core", Client: "toolkit"}
	b := ImageSet{Essential: "core", Client: "other"}
	for name, opts := range map[string][]testcontainers.ContainerCustomizer{
		"different sets": {WithImageOptions(ImageOptions{}.WithImageSet(a)), WithImageOptions(ImageOptions{}.WithImageSet(b))},
		"equal sets":     {WithImageOptions(ImageOptions{}.WithImageSet(a)), WithImageOptions(ImageOptions{}.WithImageSet(a))},
		"gap between":    {WithImageOptions(ImageOptions{}.WithImageSet(a)), WithImageOptions(ImageOptions{}), WithImageOptions(ImageOptions{}.WithImageSet(a))},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := admit(t, "", opts...)
			wantRefused(t, err)
		})
	}
	effective, err := admit(t, "", WithImageOptions(ImageOptions{}.WithImageSet(a).WithImageSet(b)), WithImageOptions(ImageOptions{}))
	wantAdmitted(t, err)
	if effective.imageSet.Client != "other" {
		t.Fatalf("later selection of one value did not replace the earlier: %+v", effective.imageSet)
	}
}

// The value is a copy: deriving does not change the base, and the caller's set
// is not shared.
func TestImageOptionsDerivedValuesAreIndependent(t *testing.T) {
	base := ImageOptions{}
	first := base.WithImageSet(ImageSet{Essential: "first"})
	second := base.WithImageSet(ImageSet{Essential: "second"})
	if base.imagesSet || first.images.Essential != "first" || second.images.Essential != "second" {
		t.Fatalf("derived values leaked: %+v %+v %+v", base, first, second)
	}
	images := ImageSet{Essential: "core", Client: "toolkit"}
	config := ImageOptions{}.WithImageSet(images)
	images.Client = "changed"
	effective, err := admit(t, "", WithImageOptions(config))
	wantAdmitted(t, err)
	effective.imageSet.Client = "mutated"
	if config.images.Client != "toolkit" {
		t.Fatalf("the value shares the caller's set or the settings' set: %+v", config.images)
	}
}

// ImageOptions is a plain value, not a customizer: it enters Run only through
// the grouped option function, which has no effect on a container request. A
// refused configuration reaches neither a later option nor a raw customizer.
func TestImageOptionsEnterRunOnlyThroughTheGroupedOption(t *testing.T) {
	if _, ok := any(ImageOptions{}).(testcontainers.ContainerCustomizer); ok {
		t.Fatal("ImageOptions is itself a customizer; it must be carried by WithImageOptions")
	}
	if _, ok := any(ImageOptions{}).(Option); ok {
		t.Fatal("ImageOptions is itself an Option")
	}
	var carrier testcontainers.ContainerCustomizer = WithImageOptions(ImageOptions{}.WithImageSet(ImageSet{Essential: "core"}))
	request := &testcontainers.GenericContainerRequest{}
	if err := carrier.Customize(request); err != nil || request.Image != "" || request.Env != nil {
		t.Fatalf("the carrier changed the request: %v %+v", err, request)
	}
	calls := 0
	raw := testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { calls++; return nil })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	c, err := Run(ctx, "other", WithImageOptions(ImageOptions{}.WithImageSet(ImageSet{Essential: "core"})), raw)
	if c != nil || calls != 0 {
		t.Fatalf("refused configuration reached a customizer: %v calls=%d", c, calls)
	}
	wantRefused(t, err)
}

// A service setting passed with an image set is applied, and the service image
// replaces its role of the set.
func TestImageOptionsKeepServiceSettings(t *testing.T) {
	effective, err := admit(t, "", WithImageOptions(ImageOptions{}.WithImageSet(ImageSet{Essential: "core", Client: "toolkit", RGW: "set-rgw"})),
		(serviceHooks{}).WithServiceImage("rgw", "selected"), WithOSDCount(3))
	wantAdmitted(t, err)
	if effective.rgwOverride != "selected" || effective.imageSet.RGW != "selected" || effective.osds != 3 {
		t.Fatalf("a setting was dropped: %+v", effective)
	}
}

type kindOnly struct {
	serviceBootstrap
	kind string
}

func (k kindOnly) Kind() string { return k.kind }

// A role that a requested feature needs and the set omits is refused; the
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
		{"cephfs", ImageSet{Essential: "core", Client: "toolkit", CephFS: "mds"}, false},
		{"rgw", ImageSet{Essential: "core", Client: "toolkit"}, true},
		{"rgw", ImageSet{Essential: "core", RGW: "gateway"}, false},
	} {
		o := options{services: []serviceBootstrap{kindOnly{kind: test.kind}}}
		if err := (ImageOptions{}.WithImageSet(test.set)).apply(&o); err != nil {
			t.Fatal(err)
		}
		if err := configureImages(&o); (err != nil) != test.wantErr {
			t.Fatalf("%s with %+v: err=%v, wantErr=%t", test.kind, test.set, err, test.wantErr)
		}
	}
}

// A grouped carrier (WithImageOptions, WithOSDStorage, WithNetworkConfig) returns an Option and
// takes its plain value, so wrapping its result in a second carrier does not
// compile. Documentation snippets
// are not compiled, so scan the godoc comments of this package and the package
// reference pages for that mistake. ExampleWithImageOptions,
// ExampleWithOSDStorage and ExampleWithNetworkConfig cover the correct form at compile time.
func TestDocumentedSnippetsDoNotWrapGroupedOptionsTwice(t *testing.T) {
	doubleWrap := regexp.MustCompile(`With(?:ImageOptions|OSDStorage|NetworkConfig)\(\s*(?:ceph\.)?With(?:ImageOptions|OSDStorage|NetworkConfig)\(`)
	var files []string
	for _, pattern := range []string{"*.go", "../docs/*.md", "../docs/guides/*.md"} {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, matches...)
	}
	if len(files) < 10 {
		t.Fatalf("scanned only %d files", len(files))
	}
	for _, file := range files {
		if filepath.Base(file) == "image_options_test.go" {
			continue // names the mistake in this test's own pattern
		}
		text, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if loc := doubleWrap.FindIndex(text); loc != nil {
			t.Errorf("%s wraps a grouped option inside another at byte %d", file, loc[0])
		}
	}
}
