//go:build all || (integration && features)

package rgw_test

import (
	"context"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

// testRGWPublicSDKConsumer builds the fixture through the public entry
// points only (ceph.Run, rgw.Start, testcontainers.CleanupContainer) and
// hands the AWS SDK probe, a separate module that plays the SUT, nothing but
// what the public API returns: the HTTPS endpoint, the region, the gateway's
// S3 credentials and the caller's CA certificate. Every S3 request is made by
// a fresh SDK process over verified HTTPS: it creates the bucket, writes an
// object and reads back its exact bytes. The SDK refuses a certificate that
// another CA signed, which it reports apart from an S3 refusal, and RGW
// refuses a request signed with a wrong secret that the right CA reaches.
// After Gateway.ReplaceTLS moves the gateway to another CA, the SDK reads
// the same bytes at the endpoint read again when it trusts the new CA and
// refuses the old one.
func testRGWPublicSDKConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	// Only the image names come from the test environment.
	image, imageOpts := harness.IntegrationImages(t)
	cluster, err := ceph.Run(ctx, image, append(imageOpts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))...)
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	authority, unrelated := harness.NewRGWTestAuthority(t), harness.NewRGWTestAuthority(t)
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{Name: "sdk", TLS: authority.Issue(t)})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3SecureEndpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	trusted, untrusted := authority.File(t), unrelated.File(t)
	probe := buildS3Probe(t, ctx, endpoint, gateway.Region)
	const bucket, object = "tc-sdk-consumer", "data"
	payload := "exact bytes an SDK consumer wrote over verified HTTPS"
	run := func(endpoint, ca, secret, operation, data string) s3ProbeResult {
		t.Helper()
		return probe.exec(t, ctx, gateway.AccessKey, secret, "-endpoint", endpoint, "-region", gateway.Region, "-ca-file", ca,
			"-bucket", bucket, "-object", object, "-operation", operation, "-data", data)
	}
	if result := run(endpoint, trusted, gateway.SecretKey, "create-bucket", ""); result.Outcome != "accepted" {
		t.Fatalf("SDK CreateBucket: %+v", result)
	}
	if result := run(endpoint, trusted, gateway.SecretKey, "put", payload); result.Outcome != "accepted" || result.SHA256 != sha256Hex([]byte(payload)) {
		t.Fatalf("SDK PutObject: %+v", result)
	}
	if result := run(endpoint, trusted, gateway.SecretKey, "get", ""); result.Outcome != "accepted" || result.SHA256 != sha256Hex([]byte(payload)) {
		t.Fatalf("SDK GetObject did not read the exact bytes: %+v", result)
	}
	if result := run(endpoint, untrusted, gateway.SecretKey, "get", ""); result.Outcome != "untrusted" {
		t.Fatalf("SDK that trusts an unrelated CA: %+v", result)
	}
	if result := run(endpoint, trusted, "deliberately-wrong-secret", "get", ""); result.Outcome != "refused" {
		t.Fatalf("SDK with a wrong secret: %+v", result)
	}
	sdk := run(endpoint, trusted, gateway.SecretKey, "get", "").SDK
	facts, err := ceph.FactsOf(cluster)
	if err != nil {
		t.Fatal(err)
	}

	successor := harness.NewRGWTestAuthority(t)
	if err := gateway.ReplaceTLS(ctx, *successor.Issue(t)); err != nil {
		t.Fatal(err)
	}
	endpoint, err = gateway.S3SecureEndpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result := run(endpoint, successor.File(t), gateway.SecretKey, "get", ""); result.Outcome != "accepted" || result.SHA256 != sha256Hex([]byte(payload)) {
		t.Fatalf("SDK that trusts the new CA after the replacement: %+v", result)
	}
	if result := run(endpoint, trusted, gateway.SecretKey, "get", ""); result.Outcome != "untrusted" {
		t.Fatalf("SDK that trusts only the replaced CA: %+v", result)
	}
	t.Logf("public ceph.Run/rgw.Start, Ceph %s, essential image %s, gateway image %s; %s: fresh-process CreateBucket/PutObject/GetObject over HTTPS with the fixture CA, exact SHA-256; unrelated CA untrusted; wrong secret refused; after ReplaceTLS the new CA is accepted with the same bytes and the old CA untrusted", facts.CephVersion, facts.Images.Essential, facts.Images.RGW, sdk)
}
