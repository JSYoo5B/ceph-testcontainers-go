//go:build all || (integration && hostnetwork && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Verify the actual Python wire transport without starting a Ceph cluster.
func TestHostNetworkHTTPTransportPreservesSignedRequest(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("python3 is required to verify the container HTTP probe")
	}
	for _, tc := range []struct {
		name, method, path string
		payload            []byte
		status             int
	}{
		{"empty bucket PUT", http.MethodPut, "/bucket", nil, http.StatusOK},
		{"binary object PUT", http.MethodPut, "/bucket/object", []byte{0, 1, 2, 255}, http.StatusOK},
		{"escaped GET with query", http.MethodGet, "/bucket/object%20name?prefix=a%2Fb&list-type=2", nil, http.StatusForbidden},
		{"object DELETE", http.MethodDelete, "/bucket/object", nil, http.StatusNoContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type receivedRequest struct {
				method, target, host string
				headers              http.Header
				payload              []byte
				err                  error
			}
			received := make(chan receivedRequest, 1)
			responseBody := []byte{255, 0, 'r', 'g', 'w'}
			if tc.status == http.StatusNoContent {
				responseBody = nil
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload, err := io.ReadAll(r.Body)
				received <- receivedRequest{r.Method, r.RequestURI, r.Host, r.Header.Clone(), payload, err}
				w.WriteHeader(tc.status)
				_, _ = w.Write(responseBody)
			}))
			defer server.Close()
			gateway := s3HTTPClient{endpoint: server.URL, accessKey: "test-access", secretKey: "test-secret", region: "us-east-1"}
			input, err := hostNetworkS3RequestInput(t.Context(), gateway, tc.method, tc.path, tc.payload)
			if err != nil {
				t.Fatal(err)
			}
			var signed struct {
				URL     string      `json:"url"`
				Headers http.Header `json:"headers"`
			}
			if err := json.Unmarshal(input, &signed); err != nil {
				t.Fatal(err)
			}
			inputPath := filepath.Join(t.TempDir(), "request.json")
			if err := os.WriteFile(inputPath, input, 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			output, err := exec.CommandContext(ctx, python, "-c", hostNetworkHTTPScript, inputPath).CombinedOutput()
			if err != nil {
				t.Fatalf("Python HTTP transport: %v: %s", err, output)
			}
			var result struct {
				Status int    `json:"status"`
				Body   string `json:"body"`
			}
			if err := json.Unmarshal(output, &result); err != nil {
				t.Fatalf("decode Python response: %v: %s", err, output)
			}
			body, err := base64.StdEncoding.DecodeString(result.Body)
			if err != nil || result.Status != tc.status || !bytes.Equal(body, responseBody) {
				t.Fatalf("Python changed HTTP response: status=%d body=%q error=%v", result.Status, body, err)
			}
			actual := <-received
			parsed, err := url.Parse(signed.URL)
			if err != nil {
				t.Fatal(err)
			}
			if actual.err != nil || actual.method != tc.method || actual.target != tc.path || actual.host != parsed.Host || !bytes.Equal(actual.payload, tc.payload) {
				t.Fatalf("Python changed signed request: %+v", actual)
			}
			if _, exists := actual.headers["Content-Type"]; exists {
				t.Fatal("transport injected an unsigned Content-Type header")
			}
			for _, header := range []string{"Authorization", "X-Amz-Date", "X-Amz-Content-Sha256"} {
				if got, want := actual.headers.Get(header), signed.Headers.Get(header); got == "" || got != want {
					t.Errorf("transport changed %s: got %q, want %q", header, got, want)
				}
			}
		})
	}
}
