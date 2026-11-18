package diagnostics

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	mobycl "github.com/moby/moby/client"
)

type crashTransportDocker struct {
	*mobycl.Client
	admitted chan struct{}
}

func (d *crashTransportDocker) CopyFromContainer(ctx context.Context, id string, opts mobycl.CopyFromContainerOptions) (mobycl.CopyFromContainerResult, error) {
	result, err := d.Client.CopyFromContainer(ctx, id, opts)
	if err == nil {
		close(d.admitted)
	}
	return result, err
}

// This uses the real Docker SDK HTTP response body against a controlled server,
// not a Docker daemon. The server sends one record, then stalls before the tar
// terminator. Cancellation must end both the reader and the request handler.
func TestCrashMetadataTransportBodyCancellation(t *testing.T) {
	for _, mode := range []string{"parent-cancel", "operation-timeout", "total-timeout"} {
		t.Run(mode, func(t *testing.T) {
			archive := crashArchive(t, crashEntry{name: "crash/r/meta", body: `{"crash_id":"r"}`})
			release, handlerDone := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/json") {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"Id":"mgr","Image":"image","State":{"Status":"running"}}`))
					return
				}
				if !strings.HasSuffix(r.URL.Path, "/archive") {
					http.NotFound(w, r)
					return
				}
				defer close(handlerDone)
				w.Header().Set("X-Docker-Container-Path-Stat", base64.StdEncoding.EncodeToString([]byte(`{"name":"crash","size":0,"mode":2147484141}`)))
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(archive[:len(archive)-1024])
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			defer server.Close()
			defer close(release)
			client, err := mobycl.New(mobycl.WithHost(server.URL), mobycl.WithAPIVersion("1.44"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			docker := &crashTransportDocker{Client: client, admitted: make(chan struct{})}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			config, err := normalizeCrashMetadataConfig(CrashMetadataConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "operation-timeout" {
				config.OperationTimeout = time.Second
			}
			if mode == "total-timeout" {
				var end context.CancelFunc
				ctx, end = context.WithTimeout(ctx, time.Second)
				defer end()
			}
			type collected struct {
				report *CrashMetadataReport
				err    error
			}
			done := make(chan collected, 1)
			go func() {
				report, err := collectCrashMetadata(ctx, config, crashSnapshot(crashContainer("mgr", "a", "mgr")), docker, &CrashMetadataReport{})
				done <- collected{report, err}
			}()
			joined := false
			defer func() {
				cancel()
				if !joined {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("collector did not terminate during cleanup")
					}
				}
			}()
			select {
			case <-docker.admitted:
			case result := <-done:
				joined = true
				t.Fatalf("archive not admitted: %v", result.err)
			case <-time.After(5 * time.Second):
				t.Fatal("archive admission stalled")
			}
			wantErr := context.DeadlineExceeded
			if mode == "parent-cancel" {
				cancel()
				wantErr = context.Canceled
			}
			select {
			case result := <-done:
				joined = true
				if !errors.Is(result.err, wantErr) || result.report.Complete || result.report.Containers[0].Outcome != CrashArchiveIncomplete {
					t.Fatalf("report=%+v error=%v", result.report, result.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("response body did not cancel")
			}
			select {
			case <-handlerDone:
			case <-time.After(5 * time.Second):
				t.Fatal("request handler did not terminate")
			}
			if mode == "operation-timeout" && ctx.Err() != nil {
				t.Fatal("operation timeout ended parent context")
			}
		})
	}
}
