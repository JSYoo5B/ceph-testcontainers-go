package rgw

import (
	"context"
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
	"github.com/testcontainers/testcontainers-go"
)

// The gateway's S3 test user is created supervised: a canceled start stops
// radosgw-admin and returns an error that matches the context's, or returns a
// NativeCommandUnstoppedError whose Stop stops it.
func TestRGWGatewayUserCreationStopsItsCommandWhenTheCallerCancels(t *testing.T) {
	ctr := gwtest.NewAccountContainer(gwtest.NewPlacementContainer())
	hang := clustertest.NewHangingCommand(ctr, func(argv []string) bool {
		return argv[0] == "radosgw-admin" && slices.Contains(argv, "user") && slices.Contains(argv, "create")
	})
	config := Config{Name: "default"}
	c := testCluster(nil, testhelper.TestConfig{}, map[string]testcontainers.Container{rgwServiceName(config): hang})
	clustertest.AssertSupervisedStop[*ceph.NativeCommandUnstoppedError](t, hang, func(ctx context.Context) error {
		_, _, err := createGatewayUser(ctx, c, hang, config)
		return err
	})
}
