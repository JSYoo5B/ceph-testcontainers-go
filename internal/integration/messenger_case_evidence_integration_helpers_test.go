//go:build all || integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// These are compiled inputs, not files read from a possibly changed checkout.
//
//go:embed testdata/messenger_reference.json
var evidenceReference []byte

//go:embed testdata/messenger_coverage.json
var evidenceCoverage []byte

//go:embed testdata/messenger_evidence.json
var evidenceBindings []byte

//go:embed *.go
var evidenceCaseSources embed.FS

func evidenceDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Register before any owned resources: testing runs this after their cleanup.
// A cleanup failure marks the test failed and suppresses the success receipt.
// The consumer must additionally require the test AND package pass events.
func registerMessengerEvidence(t *testing.T) func(map[string]any) {
	t.Helper()
	var pending map[string]any
	t.Cleanup(func() {
		if t.Failed() || pending == nil {
			return
		}
		pending["cleanup"] = "passed"
		encoded, err := json.Marshal(pending)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("MSGR_CASE_RECEIPT %s", encoded)
	})
	return func(receipt map[string]any) { pending = receipt }
}

func targetedMessengerEvidence(t *testing.T, ctx context.Context, cluster *ceph.Container, parent []byte, fsid string) map[string]any {
	t.Helper()
	version := strings.TrimSpace(execOutput(t, ctx, cluster.ControlContainer(), "ceph", "--version"))
	match := regexp.MustCompile(`^ceph version ([0-9]+\.[0-9]+\.[0-9]+) `).FindStringSubmatch(version)
	if len(match) != 2 {
		t.Fatalf("unrecognized native Ceph version: %q", version)
	}
	containers := append([]testcontainers.Container{cluster.Container, cluster.ControlContainer()}, cluster.ServiceContainers()...)
	for _, osd := range cluster.OSDs() {
		containers = append(containers, osd.Container)
	}
	images := map[string]bool{}
	for _, ctr := range containers {
		if ctr == nil {
			continue
		}
		state, err := ctr.Inspect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(state.Image, "sha256:") {
			t.Fatalf("container has no immutable image ID: %q", state.Image)
		}
		images[state.Image] = true
	}
	ids := make([]string, 0, len(images))
	for id := range images {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	sources := map[string]string{}
	for name, data := range map[string][]byte{
		"internal/integration/testdata/messenger_reference.json": evidenceReference,
		"internal/integration/testdata/messenger_coverage.json":  evidenceCoverage,
		"internal/integration/testdata/messenger_evidence.json":  evidenceBindings,
	} {
		sources[name] = evidenceDigest(data)
	}
	entries, err := evidenceCaseSources.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var snapshot strings.Builder
	for _, entry := range entries {
		data, err := evidenceCaseSources.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&snapshot, "internal/integration/%s\x00%s\n", entry.Name(), evidenceDigest(data))
	}
	sources["internal/integration/*.go"] = evidenceDigest([]byte(snapshot.String()))
	return map[string]any{"schema": "ceph-messenger-case/v1", "binding": "targeted-command/native-identities", "case": t.Name(), "run_id": uuid.NewString(), "generation": fsid, "parent_sha256": evidenceDigest(parent), "sources": sources,
		"cohort": map[string]any{"release": match[1], "ceph_version": version, "platform": fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH), "go_version": runtime.Version(), "images": ids}}
}
