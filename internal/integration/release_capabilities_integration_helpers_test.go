//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// capabilityID names one behaviour that differs between Ceph releases. It is
// also the Capability of the classified ceph.ErrUnsupported the fixture
// returns where the behaviour is missing.
type capabilityID string

const (
	// capabilityRBDNamespaceMapping maps a mirrored RBD namespace to another
	// name (Ceph 20 and later, or a patched image that has the option).
	capabilityRBDNamespaceMapping capabilityID = "rbd mirror pool enable --remote-namespace"
	// capabilityMessengerDump is the daemon command "messenger dump" (Ceph
	// 20). Without it the native probes prove encryption from secure-only
	// service modes and client debug_ms READY lines instead.
	capabilityMessengerDump capabilityID = "messenger dump"
	// capabilityRGWAccountUserModePipe is an RGW account principal in a
	// user-mode sync pipe. Ceph 19 resolves no source for such a pipe.
	capabilityRGWAccountUserModePipe capabilityID = "RGW account principals in user-mode sync pipes"
)

// releaseCapability is one row of the shared expectation table. A contract
// selects its expectation for the fixture in front of it from its row, never
// from a release prefix of its own.
type releaseCapability struct {
	id capabilityID
	// detect probes the target that runs the operation. A row with a probe
	// never looks at the release, so a patched image is classified by what
	// it actually has.
	detect func(t *testing.T, ctx context.Context, cluster *ceph.Container) bool
	// releases classifies the rows that no tool reveals, by major release.
	// Every supported release is listed; a release that is not is an
	// explicit failure of the test configuration, not a guess.
	releases map[int]bool
	// refusal is the text of the classified error the fixture returns where
	// the capability is missing. Empty when the fixture does not refuse and
	// the contract proves the behaviour another way.
	refusal string
}

// releaseCapabilities is the inventory of release differences that the
// integration contracts select expectations for. Product code keeps its own
// command-form choices (see docs/contributing/testing.md); this table owns
// the test expectations.
var releaseCapabilities = map[capabilityID]releaseCapability{
	capabilityRBDNamespaceMapping: {
		id:      capabilityRBDNamespaceMapping,
		detect:  detectRBDNamespaceMapping,
		refusal: "needs Ceph 20 or later",
	},
	capabilityMessengerDump: {
		id:     capabilityMessengerDump,
		detect: detectMessengerDump,
	},
	capabilityRGWAccountUserModePipe: {
		id: capabilityRGWAccountUserModePipe,
		// No tool reveals it: the pipe is accepted and replicates nothing.
		releases: map[int]bool{19: false, 20: true},
		refusal:  "RGW user-mode sync with an account principal needs Ceph 20 or later",
	},
}

// capabilitySupported reports whether cluster has capability id, after
// proving that every daemon runs the release of its control image. A
// mixed-release cluster, an unparseable version and a release without a row
// are explicit failures: none is evidence that a release lacks a feature.
func capabilitySupported(t *testing.T, ctx context.Context, cluster *ceph.Container, id capabilityID) bool {
	t.Helper()
	row, ok := releaseCapabilities[id]
	if !ok {
		t.Fatalf("capability %q has no row in releaseCapabilities", id)
	}
	requireUniformRelease(t, ctx, cluster, id)
	if row.detect != nil {
		supported := row.detect(t, ctx, cluster)
		t.Logf("capability %q detected=%v on Ceph %s (%s)", id, supported, cluster.CephVersion(), cluster.ControlImage())
		return supported
	}
	supported, err := classifyRelease(row, cluster.CephVersion())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("capability %q supported=%v by the release row of Ceph %s", id, supported, cluster.CephVersion())
	return supported
}

// classifyRelease reads a release-limited row for version. A patch release
// that the table does not list is classified by its major release.
func classifyRelease(row releaseCapability, version string) (bool, error) {
	if row.releases == nil {
		return false, errors.New("capability " + string(row.id) + " is detected, not classified by release")
	}
	major, err := cephMajor(version)
	if err != nil {
		return false, errors.New("capability " + string(row.id) + ": " + err.Error())
	}
	supported, listed := row.releases[major]
	if !listed {
		return false, errors.New("capability " + string(row.id) + " has no expectation for Ceph " + version +
			"; add the release to releaseCapabilities after observing it")
	}
	return supported, nil
}

var cephReleasePattern = regexp.MustCompile(`^(\d+)\.\d+\.\d+$`)

func cephMajor(version string) (int, error) {
	match := cephReleasePattern.FindStringSubmatch(version)
	if match == nil {
		return 0, errors.New("unrecognized Ceph release " + strconv.Quote(version))
	}
	return strconv.Atoi(match[1])
}

// requireCapabilityRefused proves that the fixture refused an operation that
// needs id as unsupported by cluster, before any change. That refusal is the
// whole contract of the case on such a cluster, so callers return afterwards
// instead of skipping.
func requireCapabilityRefused(t *testing.T, err error, cluster *ceph.Container, id capabilityID, detail string) *ceph.OperationError {
	t.Helper()
	row := releaseCapabilities[id]
	var refusal *ceph.OperationError
	if !errors.As(err, &refusal) || !errors.Is(err, ceph.ErrUnsupported) || refusal.Kind != ceph.ErrUnsupported ||
		refusal.Capability != string(id) || refusal.Version != cluster.CephVersion() ||
		!strings.Contains(err.Error(), row.refusal) || !strings.Contains(err.Error(), detail) {
		t.Fatalf("%s error = %v, want ErrUnsupported %q for capability %q from Ceph %s",
			detail, err, row.refusal, id, cluster.CephVersion())
	}
	t.Logf("%s (Ceph %s) refused %s before any change", refusal.Target, refusal.Version, detail)
	return refusal
}

var (
	uniformReleases   sync.Map // *ceph.Container -> error
	capabilityResults sync.Map // capabilityKey -> bool
)

type capabilityKey struct {
	cluster *ceph.Container
	id      capabilityID
}

// requireUniformRelease fails unless every daemon that "ceph versions"
// reports runs the release of the control image. A cluster with role images
// of several releases has no single expectation.
func requireUniformRelease(t *testing.T, ctx context.Context, cluster *ceph.Container, id capabilityID) {
	t.Helper()
	if cached, ok := uniformReleases.Load(cluster); ok {
		if cached != nil {
			t.Fatal(cached)
		}
		return
	}
	probe, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	data, err := cluster.Ceph(probe, "versions", "--format", "json")
	if err != nil {
		t.Fatalf("read daemon releases for capability %q: %v", id, err)
	}
	err = uniformRelease(data, cluster.CephVersion())
	if err != nil {
		err = errors.New("capability " + string(id) + ": " + err.Error())
	}
	uniformReleases.Store(cluster, err)
	if err != nil {
		t.Fatal(err)
	}
}

var daemonVersionPattern = regexp.MustCompile(`^ceph version (\d+\.\d+\.\d+)\S* `)

// uniformRelease checks "ceph versions" output against the control release.
func uniformRelease(data []byte, control string) error {
	var versions struct {
		Overall map[string]int `json:"overall"`
	}
	if err := json.Unmarshal(data, &versions); err != nil || len(versions.Overall) == 0 {
		return errors.New("ceph versions reported no daemon releases: " + strconv.Quote(string(data)))
	}
	for description := range versions.Overall {
		match := daemonVersionPattern.FindStringSubmatch(description + " ")
		if match == nil {
			return errors.New("unrecognized daemon release " + strconv.Quote(description))
		}
		if match[1] != control {
			return errors.New("mixed-release cluster: a daemon runs Ceph " + match[1] + " and the control image Ceph " +
				control + "; the contracts have no expectation for mixed role releases")
		}
	}
	return nil
}

func cachedCapability(t *testing.T, cluster *ceph.Container, id capabilityID, probe func() bool) bool {
	t.Helper()
	key := capabilityKey{cluster: cluster, id: id}
	if cached, ok := capabilityResults.Load(key); ok {
		return cached.(bool)
	}
	supported := probe()
	capabilityResults.Store(key, supported)
	return supported
}

// detectRBDNamespaceMapping reads the usage of "rbd mirror pool enable" in
// the client runtime, as the fixture does before it maps namespaces.
func detectRBDNamespaceMapping(t *testing.T, ctx context.Context, cluster *ceph.Container) bool {
	t.Helper()
	return cachedCapability(t, cluster, capabilityRBDNamespaceMapping, func() bool {
		client, err := cluster.ClientContainer(ctx)
		if err != nil {
			t.Fatalf("select the client container for capability %q: %v", capabilityRBDNamespaceMapping, err)
		}
		return strings.Contains(execOutput(t, ctx, client, "rbd", "help", "mirror", "pool", "enable"), "--remote-namespace")
	})
}

// detectMessengerDump reads the command list of the quorum leader.
func detectMessengerDump(t *testing.T, ctx context.Context, cluster *ceph.Container) bool {
	t.Helper()
	return cachedCapability(t, cluster, capabilityMessengerDump, func() bool {
		probe, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		data, err := cluster.Ceph(probe, "quorum_status", "--format", "json")
		var quorum struct {
			Leader string `json:"quorum_leader_name"`
		}
		if err != nil || json.Unmarshal(data, &quorum) != nil || quorum.Leader == "" {
			t.Fatalf("read the quorum leader for capability %q: %v %s", capabilityMessengerDump, err, data)
		}
		help, err := cluster.Ceph(probe, "tell", "mon."+quorum.Leader, "help")
		if err != nil {
			t.Fatalf("read the commands of mon.%s for capability %q: %v", quorum.Leader, capabilityMessengerDump, err)
		}
		return strings.Contains(string(help), "messenger dump")
	})
}
