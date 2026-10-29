package mirror

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/testcontainers/testcontainers-go"
)

func TestRunOptionsZeroOneAndMultipleValues(t *testing.T) {
	raw := testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { return nil })
	settings, rest, err := splitRunOptions(nil)
	if err != nil || settings.daemonImageSet || rest != nil {
		t.Fatalf("zero options: %+v %v %v", settings, rest, err)
	}
	// Only options: the daemon customizer list stays nil, not empty.
	settings, rest, err = splitRunOptions([]testcontainers.ContainerCustomizer{WithImageOptions(ImageOptions{}.WithDaemonImage("img"))})
	if err != nil || !settings.daemonImageSet || settings.daemonImage != "img" || rest != nil {
		t.Fatalf("one option: %+v %v %v", settings, rest, err)
	}
	// An unset value selects nothing and never erases or conflicts.
	settings, _, err = splitRunOptions([]testcontainers.ContainerCustomizer{WithImageOptions(ImageOptions{}), WithImageOptions(ImageOptions{}.WithDaemonImage("img")), WithImageOptions(ImageOptions{})})
	if err != nil || settings.daemonImage != "img" {
		t.Fatalf("unset values: %+v %v", settings, err)
	}
	// Within one value a later call replaces the earlier one.
	base := ImageOptions{}.WithDaemonImage("a")
	derived := base.WithDaemonImage("b")
	settings, _, err = splitRunOptions([]testcontainers.ContainerCustomizer{WithImageOptions(derived)})
	if err != nil || settings.daemonImage != "b" {
		t.Fatalf("derived value: %+v %v", settings, err)
	}
	settings, _, err = splitRunOptions([]testcontainers.ContainerCustomizer{WithImageOptions(base)})
	if err != nil || settings.daemonImage != "a" {
		t.Fatalf("derived value changed its base: %+v %v", settings, err)
	}
	// Raw customizers keep their order and are a copy of the argument.
	args := []testcontainers.ContainerCustomizer{raw, WithImageOptions(ImageOptions{}), raw}
	_, rest, err = splitRunOptions(args)
	if err != nil || len(rest) != 2 {
		t.Fatalf("raw customizers: %d %v", len(rest), err)
	}
	args[0] = nil
	if rest[0] == nil {
		t.Fatal("raw customizer list shares storage with the argument")
	}
}

func TestRunOptionsRefuseConflictsBeforeEffects(t *testing.T) {
	for name, opts := range map[string][]testcontainers.ContainerCustomizer{
		"different": {WithImageOptions(ImageOptions{}.WithDaemonImage("a")), WithImageOptions(ImageOptions{}.WithDaemonImage("b"))},
		"equal":     {WithImageOptions(ImageOptions{}.WithDaemonImage("a")), WithImageOptions(ImageOptions{}.WithDaemonImage("a"))},
		"nil":       {Option(nil)},
	} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			source := &ceph.Container{Container: &clustertest.ControlQueryCounter{Calls: &calls}}
			destination := &ceph.Container{Container: &clustertest.ControlQueryCounter{Calls: &calls}}
			result, err := Run(context.Background(), Config{Source: source, Destination: destination, Pool: "rbd"}, opts...)
			if result != nil || !errors.Is(err, ceph.ErrInvalidRequest) || calls.Load() != 0 {
				t.Fatalf("conflict reached the runtime: result=%v err=%v calls=%d", result, err, calls.Load())
			}
		})
	}
}

// An explicit empty image is a selection that is refused, never read as "derive
// the image from the cluster's role".
func TestRunExplicitEmptyImageIsRefusedNotInferred(t *testing.T) {
	opts := WithImageOptions(ImageOptions{}.WithDaemonImage(""))
	settings, _, err := splitRunOptions([]testcontainers.ContainerCustomizer{opts})
	if err != nil || !settings.daemonImageSet || settings.daemonImage != "" {
		t.Fatalf("explicit empty image was not recorded as a selection: %+v %v", settings, err)
	}
	image, err := settings.daemonImageFor(ceph.ImageSet{RBDAux: "role-image"})
	if image != "" || err != nil {
		t.Fatalf("explicit empty image fell back to the role image: %q %v", image, err)
	}
	var calls atomic.Int32
	source := &ceph.Container{Container: &clustertest.ControlQueryCounter{Calls: &calls}}
	destination := &ceph.Container{Container: &clustertest.ControlQueryCounter{Calls: &calls}}
	result, err := Run(context.Background(), Config{Source: source, Destination: destination, Pool: "rbd"}, opts)
	if result != nil || !errors.Is(err, ceph.ErrInvalidRequest) || calls.Load() != 0 {
		t.Fatalf("empty image reached the runtime: result=%v err=%v calls=%d", result, err, calls.Load())
	}
}

func TestRunDaemonImageInferenceKeepsRoleAndRefusesMissingRole(t *testing.T) {
	var unset runSettings
	image, err := unset.daemonImageFor(ceph.ImageSet{Essential: "essential", Client: "client", RBDAux: "role-image"})
	if err != nil || image != "role-image" {
		t.Fatalf("inferred image: %q %v", image, err)
	}
	// Other roles never substitute for a missing rbd-aux role.
	image, err = unset.daemonImageFor(ceph.ImageSet{Essential: "essential", Client: "client", CephFS: "cephfs", RGW: "rgw"})
	if image != "" || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("missing role was not refused: %q %v", image, err)
	}
	// An explicit image wins over an available role image.
	explicit, _, _ := splitRunOptions([]testcontainers.ContainerCustomizer{WithImageOptions(ImageOptions{}.WithDaemonImage("explicit"))})
	image, err = explicit.daemonImageFor(ceph.ImageSet{RBDAux: "role-image"})
	if err != nil || image != "explicit" {
		t.Fatalf("explicit image: %q %v", image, err)
	}
	// Run without an image on clusters that select no role fails before effects.
	var calls atomic.Int32
	source := &ceph.Container{Container: &clustertest.ControlQueryCounter{Calls: &calls}}
	destination := &ceph.Container{Container: &clustertest.ControlQueryCounter{Calls: &calls}}
	result, err := Run(context.Background(), Config{Source: source, Destination: destination, Pool: "rbd"})
	if result != nil || !errors.Is(err, ceph.ErrInvalidRequest) || calls.Load() != 0 {
		t.Fatalf("missing role reached the runtime: result=%v err=%v calls=%d", result, err, calls.Load())
	}
	if result, err := Run(context.Background(), Config{Pool: "rbd"}); result != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("missing clusters: %v %v", result, err)
	}
}
