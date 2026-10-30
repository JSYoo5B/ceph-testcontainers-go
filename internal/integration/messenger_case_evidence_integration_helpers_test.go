//go:build all || integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
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

// Executors are the containers already used by the recipe, including clients
// owned separately from the cluster. Collecting evidence must not start one.
func targetedMessengerEvidence(t *testing.T, ctx context.Context, cluster *ceph.Container, parent []byte, fsid string, executors ...testcontainers.Container) map[string]any {
	t.Helper()
	version := strings.TrimSpace(harness.ExecOutput(t, ctx, clusteraccess.ControlContainer(t, cluster), "ceph", "--version"))
	match := regexp.MustCompile(`^ceph version ([0-9]+\.[0-9]+\.[0-9]+) `).FindStringSubmatch(version)
	if len(match) != 2 {
		t.Fatalf("unrecognized native Ceph version: %q", version)
	}
	containers := append([]testcontainers.Container{cluster.Container, clusteraccess.ControlContainer(t, cluster)}, cluster.ServiceContainers()...)
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		containers = append(containers, osd.Container)
	}
	ids, err := messengerEvidenceImages(ctx, containers, executors...)
	if err != nil {
		t.Fatal(err)
	}
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

func messengerEvidenceImages(ctx context.Context, containers []testcontainers.Container, executors ...testcontainers.Container) ([]string, error) {
	images := map[string]bool{}
	for i, ctr := range append(slices.Clone(containers), executors...) {
		if ctr == nil {
			return nil, fmt.Errorf("evidence container %d is missing", i)
		}
		state, err := ctr.Inspect(ctx)
		if err != nil {
			return nil, fmt.Errorf("inspect evidence container %d: %w", i, err)
		}
		if state == nil || !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(state.Image) {
			return nil, fmt.Errorf("evidence container %d has no immutable image ID", i)
		}
		images[state.Image] = true
	}
	ids := make([]string, 0, len(images))
	for id := range images {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids, nil
}
