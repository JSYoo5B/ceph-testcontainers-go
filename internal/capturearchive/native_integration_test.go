//go:build (all || integration) && ((capture_archive && !ci) || (ci_optional && (!ci_batch || ci_batch_capture_archive)))

//ci: timeout=20m job-timeout=30 image-roles=essential

package capturearchive_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/capture"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/dockerbridge"
	"github.com/testcontainers/testcontainers-go"
)

// helperImage is the published reference helper of packet-capture contract
// 1, which the test uses without CEPH_TEST_CAPTURE_IMAGE; the SDK chooses no
// default image.
const helperImage = "ghcr.io/jsyoo5b/ceph-testcontainers-images:extension-capture-v1"

func captureHelper(t *testing.T) string {
	t.Helper()
	image := os.Getenv("CEPH_TEST_CAPTURE_IMAGE")
	if image == "" {
		image = helperImage
	}
	t.Logf("capture helper: %s", image)
	return image
}

// controlledFailureTB records an intentional workload failure without failing
// the outer validation test. Real capture, archive and cleanup run unchanged.
// Its failed flag and recorded errors are part of the negative-control oracle.
type controlledFailureTB struct {
	testing.TB
	failed   bool
	errors   []string
	cleanups []func()
}

func (tb *controlledFailureTB) Failed() bool { return tb.failed }
func (tb *controlledFailureTB) Errorf(format string, args ...any) {
	tb.failed = true
	tb.errors = append(tb.errors, fmt.Sprintf(format, args...))
}
func (tb *controlledFailureTB) Cleanup(fn func()) { tb.cleanups = append(tb.cleanups, fn) }
func (tb *controlledFailureTB) finish() {
	for i := len(tb.cleanups) - 1; i >= 0; i-- {
		tb.cleanups[i]()
	}
	tb.cleanups = nil
}

func TestCaptureFailureArchive(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	helper := captureHelper(t)
	for _, raw := range []bool{false, true} {
		t.Run(fmt.Sprintf("raw_%t", raw), func(t *testing.T) {
			bridge, err := dockerbridge.New(ctx)
			if bridge != nil {
				t.Cleanup(func() {
					c, done := context.WithTimeout(context.Background(), time.Minute)
					defer done()
					if err := bridge.Remove(c); err != nil {
						t.Error(err)
					}
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			runTarget := func(command []string) testcontainers.Container {
				c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Image: "busybox:1.37.0", Networks: []string{bridge.Name}, Cmd: command}, Started: true})
				if c != nil {
					t.Cleanup(func() {
						cctx, done := context.WithTimeout(context.Background(), time.Minute)
						defer done()
						if err := c.Terminate(cctx); err != nil {
							t.Error(err)
						}
					})
				}
				if err != nil {
					t.Fatal(err)
				}
				return c
			}
			target := runTarget([]string{"sh", "-c", "mkdir -p /www && echo archive-payload > /www/index.html && httpd -p 7000 -h /www && exec sleep infinity"})
			// Arrange an actual byte cut inside a configured secret in this client's
			// stdout. Only the suffix and the padding fit in the archive's log tail.
			const logLimit = 64 << 10
			const secretSuffix = "opaque-suffix-probe"
			const knownSecret = "diagnostic-front-cut-" + secretSuffix
			// Replacing two short values expands the masked tail past its byte
			// limit, even after masking the cut at the front.
			const shortSecret = "q7x"
			const trailer = " q7x q7x latest observation\n"
			padding := logLimit - len(secretSuffix) - len(trailer)
			client := runTarget([]string{"sh", "-c", `printf '%s%*s%s' "$1" "$2" '' "$3"; exec sleep infinity`, "log-probe", knownSecret, strconv.Itoa(padding), trailer})
			inspected, err := target.Inspect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			endpoint := inspected.NetworkSettings.Networks[bridge.Name]
			if endpoint == nil {
				t.Fatal("no capture endpoint")
			}
			image := os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE")
			if image == "" {
				image = ceph.DefaultImage
			}
			cluster, err := ceph.Run(ctx, image, ceph.WithOSDStorage(ceph.OSDStorage{}.WithStartupBenchmark(ceph.OSDBenchmarkSkip)), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))
			if cluster != nil {
				t.Cleanup(func() {
					c, done := context.WithTimeout(context.Background(), 3*time.Minute)
					defer done()
					if err := cluster.Terminate(c); err != nil {
						t.Error(err)
					}
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			root := filepath.Join(t.ArtifactDir(), "failure-archives")
			tb := &controlledFailureTB{TB: t}
			finalization, err := finalize.TerminateOnCleanup(tb, cluster, finalize.Config{
				Archive: true, ArchiveDirectory: root, Scenario: "capture-raw-copy",
				Diagnostics: diagnostics.Config{MaxOutputBytes: logLimit, RedactValues: []string{knownSecret, shortSecret},
					AdditionalContainers: []diagnostics.Target{{Role: "client", Name: "tail-probe", Container: client}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			output := filepath.Join(t.TempDir(), "traffic.pcap")
			session, err := capture.Start(ctx, target, capture.Request{HelperImage: helper, Network: bridge.Name, Interface: "eth0", Filter: "tcp port 7000", SnapLen: 512, MaxPackets: 128, Output: output})
			if session != nil {
				t.Cleanup(func() {
					c, done := context.WithTimeout(context.Background(), 3*time.Minute)
					defer done()
					_, _ = session.Stop(c)
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			if raw {
				finalization.CaptureWithFile("traffic", session, 1<<20)
			} else {
				finalization.Capture("traffic", session)
			}
			code, body, err := client.Exec(ctx, []string{"wget", "-qO-", "http://" + endpoint.IPAddress.String() + ":7000/"})
			if err != nil {
				t.Fatal(err)
			}
			response, err := io.ReadAll(body)
			if err != nil || code != 0 || !bytes.Contains(response, []byte("archive-payload")) {
				t.Fatalf("workload %d %q %v", code, response, err)
			}
			tb.Errorf("controlled original workload failure")
			tb.finish()
			if !tb.failed || len(tb.errors) != 1 || tb.errors[0] != "controlled original workload failure" {
				t.Fatalf("negative-control errors %v", tb.errors)
			}
			report, err := session.Stop(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !report.Retrieved || !report.HelperRemoved || report.Target != capture.TargetUnchanged || report.File.Status != capture.FileValid || report.File.Packets == 0 {
				t.Fatalf("capture report %+v", report)
			}
			original, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			digest := fmt.Sprintf("%x", sha256.Sum256(original))
			if report.RetrievedSHA256 != digest || report.RetrievedBytes != int64(len(original)) {
				t.Fatal("retrieved identity mismatch")
			}
			manifests, err := filepath.Glob(filepath.Join(root, "*", "ceph-*", "manifest.json"))
			if err != nil || len(manifests) != 1 {
				t.Fatalf("archive discovery %v %v", manifests, err)
			}
			data, err := os.ReadFile(manifests[0])
			if err != nil {
				t.Fatal(err)
			}
			var m struct {
				Final   bool `json:"final"`
				Trigger struct {
					TestFailed bool `json:"test_failed"`
				} `json:"trigger"`
				Phase   string `json:"phase"`
				Archive struct {
					Retained, Complete bool
					Error              string
				} `json:"archive"`
				Collectors []struct {
					Name               string
					Reported, Complete bool
					Error              string
				} `json:"collectors"`
				Files        []struct{ Name, Error string } `json:"files"`
				CaptureFiles []struct {
					Copied bool
					Bytes  int64
					SHA256 string
					Error  string
				} `json:"capture_files"`
				Captures []struct {
					Reported bool
					Error    string
				} `json:"captures"`
			}
			if err := json.Unmarshal(data, &m); err != nil {
				t.Fatal(err)
			}
			if !m.Final || !m.Trigger.TestFailed || !m.Archive.Retained || m.Archive.Error != "" || m.Phase != "before-cleanup" || len(m.Captures) != 1 || !m.Captures[0].Reported || m.Captures[0].Error != "" {
				t.Fatalf("manifest %s", data)
			}

			// A bounded diagnostic log tail may be truncated. Preserve that
			// incomplete verdict; it is independent of capture copying. Only
			// this observed condition is admitted, never a collector error,
			// unsupported command, failed file write or unexplained omission.
			diagnosticBytes, err := os.ReadFile(filepath.Join(filepath.Dir(manifests[0]), "diagnostics.json"))
			if err != nil {
				t.Fatal(err)
			}
			var diagnostics struct {
				Complete  bool
				Artifacts []struct {
					Kind, Name, Role, Data string
					Truncated              bool
					Error                  string
				}
			}
			if err := json.Unmarshal(diagnosticBytes, &diagnostics); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(diagnosticBytes, []byte(secretSuffix)) {
				t.Fatal("the archived log tail exposed the known secret at its byte boundary")
			}
			foundProbe := false
			truncated := []string{}
			for _, artifact := range diagnostics.Artifacts {
				if artifact.Kind == "container-logs" && artifact.Name == "tail-probe" {
					// Replacing the clipped suffix saves nine bytes, then masking the
					// two short values adds fourteen. The tail drops the first five
					// bytes of its leading marker and preserves the latest trailer.
					foundProbe = artifact.Truncated && len(artifact.Data) == logLimit &&
						strings.HasPrefix(artifact.Data, "CTED] ") && strings.HasSuffix(artifact.Data, " [REDACTED] [REDACTED] latest observation\n")
				}
				if artifact.Error != "" {
					t.Fatalf("diagnostic error %s/%s", artifact.Kind, artifact.Name)
				}
				if artifact.Truncated {
					if artifact.Kind != "container-logs" {
						t.Fatalf("unexpected truncated artifact %s", artifact.Kind)
					}
					truncated = append(truncated, artifact.Role+"/"+artifact.Name)
				}
			}
			if !foundProbe {
				t.Fatal("missing redacted log tail or its newest observation in the archive")
			}
			if diagnostics.Complete != (len(truncated) == 0) || len(m.Collectors) != 2 {
				t.Fatal("unexplained diagnostic completeness")
			}
			for _, collector := range m.Collectors {
				if !collector.Reported || collector.Error != "" {
					t.Fatalf("collector outcome %+v", collector)
				}
				switch collector.Name {
				case "diagnostics":
					if collector.Complete != diagnostics.Complete {
						t.Fatal("diagnostic verdict lost")
					}
				case "crash-metadata":
					if !collector.Complete {
						t.Fatal("incomplete crash metadata")
					}
				default:
					t.Fatalf("unexpected collector %s", collector.Name)
				}
			}
			for _, file := range m.Files {
				if file.Error != "" {
					t.Fatalf("archive write failed %s", file.Name)
				}
			}
			if m.Archive.Complete != diagnostics.Complete {
				t.Fatal("aggregate archive completeness contradicts its collectors")
			}
			rawPath := filepath.Join(filepath.Dir(manifests[0]), "capture-1-traffic.pcap")
			if raw {
				kept, err := os.ReadFile(rawPath)
				if err != nil || !bytes.Equal(kept, original) || len(m.CaptureFiles) != 1 || !m.CaptureFiles[0].Copied || m.CaptureFiles[0].SHA256 != digest || m.CaptureFiles[0].Bytes != int64(len(original)) {
					t.Fatalf("raw identity %v %+v", err, m.CaptureFiles)
				}
			} else {
				if _, err := os.Stat(rawPath); !os.IsNotExist(err) || len(m.CaptureFiles) != 0 {
					t.Fatal("raw bytes included without consent")
				}
			}
			metadata, err := os.ReadFile(filepath.Join(filepath.Dir(manifests[0]), "capture-1-traffic.json"))
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(metadata, []byte(filepath.Dir(output))) || bytes.Contains(data, []byte(filepath.Dir(output))) {
				t.Fatal("source host path leaked")
			}
			// Caller-owned capture target survived the Ceph finalization unchanged.
			after, err := target.Inspect(ctx)
			if err != nil || after.State == nil || !after.State.Running || after.State.StartedAt != inspected.State.StartedAt {
				t.Fatal("caller target changed")
			}
			receipt := map[string]any{"raw_consent": raw, "source_sha256": digest, "source_bytes": len(original), "capture": report, "manifest": m, "caller_target_preserved": true, "diagnostic_truncated_logs": truncated}
			// The retained native receipt names only the basename of the caller file.
			report.Output = filepath.Base(report.Output)
			receipt["capture"] = report
			encoded, _ := json.Marshal(receipt)
			t.Logf("CAPTURE_ARCHIVE_RECEIPT %s", encoded)
		})
	}
}
