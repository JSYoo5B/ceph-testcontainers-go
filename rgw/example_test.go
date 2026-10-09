package rgw_test

import (
	"context"

	"github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

func ExampleRun() {
	ctx := context.Background()
	cluster, err := rgw.Run(ctx, ceph.DefaultImage,
		rgw.WithGateways(rgw.Config{Name: "s3", SkipUserCreation: true}),
	)
	if cluster != nil {
		defer testcontainers.TerminateContainer(cluster)
	}
	if err != nil {
		panic(err)
	}
	gateway := rgw.Gateways(cluster)[0]
	user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "app"})
	if err != nil {
		panic(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		panic(err)
	}
	// Sign S3 requests to endpoint with user.Credentials().
	_, _ = user, endpoint
}
