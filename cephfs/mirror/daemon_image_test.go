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

// The daemon image is required, and the Config comes from exactly one
// WithConfig; every refusal happens before any cluster is queried.
func TestRunRefusesImageAndConfigBeforeEffects(t *testing.T) {
	var calls atomic.Int32
	source := &ceph.Container{Container: &clustertest.ControlQueryCounter{Calls: &calls}}
	destination := &ceph.Container{Container: &clustertest.ControlQueryCounter{Calls: &calls}}
	cfg := Config{Source: source, Destination: destination, SourceFilesystem: "source", DestinationFilesystem: "destination", Directories: []string{"/data"}}
	for name, opts := range map[string]struct {
		img  string
		opts []testcontainers.ContainerCustomizer
	}{
		"empty image":  {"", []testcontainers.ContainerCustomizer{WithConfig(cfg)}},
		"blank image":  {" ", []testcontainers.ContainerCustomizer{WithConfig(cfg)}},
		"no config":    {"mirror-image", nil},
		"two configs":  {"mirror-image", []testcontainers.ContainerCustomizer{WithConfig(cfg), WithConfig(cfg)}},
		"clusters nil": {"mirror-image", []testcontainers.ContainerCustomizer{WithConfig(Config{})}},
	} {
		result, err := Run(context.Background(), opts.img, opts.opts...)
		if result != nil || !errors.Is(err, ceph.ErrInvalidRequest) || calls.Load() != 0 {
			t.Fatalf("%s reached the runtime: result=%v err=%v calls=%d", name, result, err, calls.Load())
		}
	}
}

func TestWithConfigIsNotAContainerCustomization(t *testing.T) {
	request := &testcontainers.GenericContainerRequest{}
	if err := WithConfig(Config{}).Customize(request); err != nil || request.Image != "" || request.Env != nil {
		t.Fatalf("the carrier changed the request: %v %+v", err, request)
	}
}
