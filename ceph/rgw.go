package ceph

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RGWContainer is an optional S3 gateway owned by its Ceph cluster. AccessKey
// and SecretKey belong to a generated ordinary S3 user for trusted tests.
type RGWContainer struct {
	testcontainers.Container
	AccessKey string
	SecretKey string
	Region    string
}

// StartRGW starts a gateway using the cluster's Ceph CLI credentials, waits for
// the S3 HTTP listener and creates a test user with radosgw-admin. The daemon
// uses the same ephemeral admin CephX credentials as WithClient. Cluster
// Terminate also terminates gateways, including one returned with an error.
func (c *Container) StartRGW(ctx context.Context) (*RGWContainer, error) {
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	ctr, err := c.startService(ctx, "rgw", c.settings.rgwImage,
		testcontainers.WithEntrypoint("/bin/sh", "/tc/rgw.sh"),
		testcontainers.WithCmd(),
		testcontainers.WithFiles(scriptFile("rgw")),
		testcontainers.WithExposedPorts("7480/tcp"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/").WithPort("7480/tcp").
			WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK || status == http.StatusForbidden }).
			WithStartupTimeout(c.settings.startupTimeout)),
	)
	var rgw *RGWContainer
	if ctr != nil {
		rgw = &RGWContainer{Container: ctr, Region: "us-east-1"}
	}
	if err != nil {
		return rgw, err
	}
	user, err := command(ctx, ctr, "radosgw-admin", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "user", "create",
		"--uid", "tc-"+uuid.NewString(), "--display-name", "Testcontainers", "--format", "json")
	if err != nil {
		return rgw, fmt.Errorf("create RGW S3 user: %w", err)
	}
	var credentials struct {
		Keys []struct {
			AccessKey string `json:"access_key"`
			SecretKey string `json:"secret_key"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(user, &credentials); err != nil {
		return rgw, fmt.Errorf("decode RGW S3 credentials: %w", err)
	}
	if len(credentials.Keys) != 1 || credentials.Keys[0].AccessKey == "" || credentials.Keys[0].SecretKey == "" {
		return rgw, fmt.Errorf("RGW did not generate one S3 access/secret key pair")
	}
	rgw.AccessKey = credentials.Keys[0].AccessKey
	rgw.SecretKey = credentials.Keys[0].SecretKey
	return rgw, nil
}

// S3Endpoint returns the mapped HTTP endpoint for host S3 clients. Configure
// clients to use path-style bucket addressing and the returned test credentials.
func (c *RGWContainer) S3Endpoint(ctx context.Context) (string, error) {
	return c.PortEndpoint(ctx, "7480/tcp", "http")
}
