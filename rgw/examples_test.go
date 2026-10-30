package rgw_test

import (
	"context"
	"log"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

func ExampleRun() {
	ctx := context.Background()
	cluster, err := rgw.Run(ctx, ceph.DefaultImage,
		rgw.WithGateways(rgw.Config{Name: "s3", SkipUserCreation: true}),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(cluster); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		panic(err)
	}
	gateways, err := rgw.Gateways(ctx, cluster.Container)
	if err != nil {
		panic(err)
	}
	gateway := gateways[0]
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
