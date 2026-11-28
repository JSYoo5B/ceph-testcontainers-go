package mirror

import (
	"context"
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

// SourceRBD runs rbd in the source setup client supervised: a canceled call
// stops the command before it returns, or returns a
// NativeCommandUnstoppedError whose Stop stops it.
func TestRBDMirrorSourceRBDStopsItsCommandWhenTheCallerCancels(t *testing.T) {
	hang := clustertest.NewHangingCommand(newRBDNamespacePolicyFixture(), func(argv []string) bool {
		return slices.Equal(argv, []string{"rbd", "info", "images/disk"})
	})
	link := &Container{pair: rbdMirrorPair{sourceClient: hang}}
	clustertest.AssertSupervisedStop[*ceph.NativeCommandUnstoppedError](t, hang, func(ctx context.Context) error {
		_, err := link.SourceRBD(ctx, "info", "images/disk")
		return err
	})
}

// A mirror policy step runs rbd mirror pool enable supervised, so a canceled
// setup cannot leave it running for the next mutation to race.
func TestRBDMirrorPolicyStepStopsItsCommandWhenTheCallerCancels(t *testing.T) {
	source, destination := newRBDNamespacePolicyFixture(), newRBDNamespacePolicyFixture()
	hang := clustertest.NewHangingCommand(source, func(argv []string) bool {
		return len(argv) > 4 && slices.Equal(argv[:4], []string{"rbd", "mirror", "pool", "enable"})
	})
	link := &Container{pair: rbdMirrorPair{sourceClient: hang, destinationClient: destination, config: Config{
		Pool: "images", Scope: ScopeImage, SourceNamespace: "ns-a", DestinationNamespace: "ns-b", SourceSite: "source", DestinationSite: "destination",
		Source: &ceph.Container{Container: hang}, Destination: &ceph.Container{Container: destination},
	}}}
	clustertest.AssertSupervisedStop[*ceph.NativeCommandUnstoppedError](t, hang, func(ctx context.Context) error {
		return link.pair.provisionPolicies(ctx)
	})
}
