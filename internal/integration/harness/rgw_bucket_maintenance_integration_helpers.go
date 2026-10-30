//go:build all || (integration && features)

package harness

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"
)

type rgwMaintenanceQuota struct {
	Enabled    bool  `json:"enabled"`
	MaxObjects int64 `json:"max_objects"`
	MaxSize    int64 `json:"max_size"`
}

type RGWMaintenanceBucket struct {
	Bucket string              `json:"bucket"`
	ID     string              `json:"id"`
	Owner  string              `json:"owner"`
	Shards int                 `json:"num_shards"`
	Quota  rgwMaintenanceQuota `json:"bucket_quota"`
}

func RGWMaintenanceRequest(t *testing.T, ctx context.Context, client S3HTTPClient, method, path string, payload []byte) (int, []byte, http.Header) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, client.Endpoint+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	client.Sign(request, payload, time.Now().UTC())
	response, err := client.HTTP.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	var data bytes.Buffer
	if _, err := data.ReadFrom(response.Body); err != nil {
		t.Fatal(err)
	}
	// The response metadata is usable by the client recipe without printing
	// the request's Authorization header or secret credentials.
	return response.StatusCode, data.Bytes(), response.Header.Clone()
}
