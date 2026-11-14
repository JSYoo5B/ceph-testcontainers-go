//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_legacy_replication_destination)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rgw

package rgw_legacy_replication_destination_test

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

const writerImage = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.5-all"

// image selects the reader of the legacy writer's persisted policy.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.2-all"

func TestLegacyReplicationDestination(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, _ := harness.NewSingleImageCluster(t, ceph.Run, writerImage, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	writer, err := rgw.Start(ctx, cluster, rgw.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Admin(ctx, "sync", "group", "create", "--group-id", "replication-control", "--status", "allowed"); err != nil {
		t.Fatal(err)
	}
	grace := 3 * time.Second
	if err := writer.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	if err := writer.Start(ctx); err != nil {
		t.Fatal(err)
	}
	clientFor := func(g *rgw.Gateway) harness.S3HTTPClient {
		t.Helper()
		endpoint, err := g.S3Endpoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return harness.S3HTTPClient{Endpoint: endpoint, AccessKey: writer.AccessKey, SecretKey: writer.SecretKey, Region: writer.Region, HTTP: &http.Client{Timeout: 30 * time.Second}}
	}
	original := clientFor(writer)
	version := strings.TrimSpace(harness.ExecOutput(t, ctx, writer, "radosgw", "--version"))
	if !strings.HasPrefix(version, "ceph version 19.2.5 ") {
		t.Fatalf("legacy writer version: %q", version)
	}
	const legacy = "tc-replication-legacy"
	const fresh = "tc-replication-fresh"
	const destination = "tc-replication-destination"
	const prefix = "arn:aws:s3:::"
	const arn = prefix + destination
	const policy = "<ReplicationConfiguration><Role>arn:aws:iam:::role/replica</Role><Rule><ID>copy</ID><Priority>1</Priority><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Destination><Bucket>" + arn + "</Bucket></Destination><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication></Rule></ReplicationConfiguration>"
	put := func(client harness.S3HTTPClient, path, body string) {
		t.Helper()
		data := []byte(body)
		sum := md5.Sum(data)
		hdr := http.Header{"Content-Md5": {base64.StdEncoding.EncodeToString(sum[:])}}
		r := harness.RGWBackendSigned(t, ctx, client, "s3", "PUT", path, data, hdr)
		harness.RGWBackendRequire(t, r, 200)
	}
	if err := harness.RGWOwnBuckets(t, harness.RGWBackendBucketCreator(t, ctx, original), func(created []string) {
		cleanup, stop := context.WithTimeout(context.Background(), 45*time.Second)
		defer stop()
		for _, bucket := range created {
			if _, err := writer.Admin(cleanup, "bucket", "rm", "--bucket", bucket, "--purge-objects"); err != nil {
				t.Error(err)
			}
		}
	}, legacy, fresh, destination); err != nil {
		t.Fatal(err)
	}
	for _, bucket := range []string{legacy, fresh, destination} {
		put(original, "/"+bucket+"?versioning", "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
	}
	put(original, "/"+legacy+"?replication", policy)
	stored := func(g *rgw.Gateway, bucket, want string) {
		t.Helper()
		raw, err := g.Admin(ctx, "sync", "policy", "get", "--bucket", bucket)
		if err != nil {
			t.Fatal(err)
		}
		var data struct {
			Groups []struct {
				Status string
				Pipes  []struct {
					ID   string
					Dest struct{ Bucket string }
				}
			}
		}
		if err := json.Unmarshal(raw, &data); err != nil {
			t.Fatal(err)
		}
		var observed []string
		for _, group := range data.Groups {
			if group.Status == "enabled" {
				for _, pipe := range group.Pipes {
					if pipe.ID == "copy" {
						observed = append(observed, pipe.Dest.Bucket)
					}
				}
			}
		}
		if len(observed) != 1 || observed[0] != want {
			t.Fatalf("%s stored destinations %q, want %q", bucket, observed, want)
		}
	}
	stored(writer, legacy, arn)
	reader, err := rgw.Start(ctx, cluster, rgw.Config{Name: "reader", SkipUserCreation: true}, testcontainers.WithImage(image))
	if err != nil {
		t.Fatal(err)
	}
	client := clientFor(reader)
	t.Logf("backing cluster and writer %s (%s); reader %s (%s)", writerImage, version, image, strings.TrimSpace(harness.ExecOutput(t, ctx, reader, "radosgw", "--version")))
	put(client, "/"+fresh+"?replication", policy)
	stored(reader, fresh, destination)
	read := func(bucket string) string {
		t.Helper()
		r := harness.RGWBackendSigned(t, ctx, client, "s3", "GET", "/"+bucket+"?replication", nil, nil)
		harness.RGWBackendRequire(t, r, 200)
		var cfg struct {
			XMLName xml.Name `xml:"ReplicationConfiguration"`
			Rules   []struct {
				ID     string
				Status string
				Bucket string `xml:"Destination>Bucket"`
			} `xml:"Rule"`
		}
		if err := xml.Unmarshal(r.Body, &cfg); err != nil || len(cfg.Rules) != 1 || cfg.Rules[0].ID != "copy" || cfg.Rules[0].Status != "Enabled" {
			t.Fatalf("%s unexpected policy %s: %v", bucket, r.Body, err)
		}
		return cfg.Rules[0].Bucket
	}
	if got := read(fresh); got != arn {
		t.Fatalf("fresh destination %q, want %q", got, arn)
	}
	got := read(legacy)
	stored(writer, legacy, arn)
	t.Logf("stored legacy=%q fresh=%q; returned legacy=%q fresh=%q", arn, destination, got, arn)
	verdict := "NOT_REPRODUCED"
	var symptoms []string
	switch got {
	case arn:
	case prefix + arn:
		verdict = "REPRODUCED"
		symptoms = append(symptoms, "doubled legacy destination ARN prefix")
	default:
		t.Fatalf("unexpected legacy destination %q", got)
	}
	t.Logf("EXPERIMENT_RESULT case=rgw_legacy_replication_destination result=%s image=%s symptoms=%q", verdict, image, symptoms)
}
