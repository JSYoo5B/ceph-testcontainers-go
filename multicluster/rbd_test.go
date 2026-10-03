package multicluster

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestRunRBDMirrorRejectsMissingClusters(t *testing.T) {
	mirror, err := RunRBDMirror(context.Background(), "example/ceph:version", RBDMirrorConfig{Pool: "rbd"})
	if err == nil || mirror != nil {
		t.Fatalf("missing cluster pair must fail before resource allocation: mirror=%v error=%v", mirror, err)
	}
}

func TestRBDMirrorConfigDefaultsAndRejectsAmbiguousPool(t *testing.T) {
	config, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: " rbd "})
	if err != nil || config.Pool != "rbd" || config.SourceSite != "source" || config.DestinationSite != "destination" || config.Mode != RBDMirrorModeSnapshot || config.DaemonCount != 1 {
		t.Fatalf("unexpected normalized configuration: %+v error=%v", config, err)
	}
	for _, pool := range []string{"", " ", "-rbd", "rbd/namespace", "rbd images"} {
		t.Run(pool, func(t *testing.T) {
			if _, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: pool}); err == nil {
				t.Fatalf("ambiguous pool %q accepted", pool)
			}
		})
	}
	if _, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: "rbd", SourceSite: "same", DestinationSite: "same"}); err == nil {
		t.Fatal("identical source and destination site names accepted")
	}
}

func TestRBDMirrorDaemonConfigurationRejectsInvalidTopologyBeforeAllocation(t *testing.T) {
	if _, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: "rbd", DaemonCount: -1}); err == nil {
		t.Fatal("negative daemon count accepted")
	}
	config, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: "rbd", DaemonCount: 3})
	if err != nil || config.DaemonCount != 3 {
		t.Fatalf("explicit daemon count was not preserved: %+v error=%v", config, err)
	}
	for _, name := range []string{"", "--flag", "has space", "path/name", "name\x00", strings.Repeat("a", 64)} {
		if _, err := (&RBDMirror{}).AddDaemon(t.Context(), name); err == nil || strings.Contains(err.Error(), "unavailable") {
			t.Fatalf("invalid daemon name %q was not rejected before allocation: %v", name, err)
		}
	}
	for _, name := range []string{"a", "standby-1", "receiver_B", "node-27"} {
		if err := validateRBDMirrorDaemonName(name); err != nil {
			t.Fatalf("valid daemon name %q rejected: %v", name, err)
		}
	}
}

func TestRBDMirrorDaemonRemovalOwnsMembershipAndRetriesFailedCleanup(t *testing.T) {
	initialContainer := &cleanupContainer{}
	initial := &RBDMirrorDaemon{Container: initialContainer, DaemonName: "a"}
	failedContainer := &cleanupContainer{err: errors.New("temporary cleanup failure")}
	standby := &RBDMirrorDaemon{Container: failedContainer, DaemonName: "b"}
	link := &RBDMirror{Container: initial, daemons: []*RBDMirrorDaemon{standby, initial}, initialDaemonName: "a"}
	link.owned.addContainer(initial)
	link.owned.addContainer(standby)
	members := link.Daemons()
	if len(members) != 2 || members[0] != initial || members[1] != standby {
		t.Fatalf("daemon membership is not sorted: %+v", members)
	}
	members[0] = nil
	if link.Daemons()[0] != initial {
		t.Fatal("membership snapshot changed fixture membership")
	}
	if err := link.RemoveDaemon(t.Context(), "foreign"); err == nil || initialContainer.calls != 0 || failedContainer.calls != 0 {
		t.Fatal("foreign daemon removal mutated owned resources")
	}
	if err := link.RemoveDaemon(t.Context(), "b"); err == nil || len(link.Daemons()) != 2 {
		t.Fatal("failed removal lost tracked daemon")
	}
	failedContainer.err = nil
	if err := link.RemoveDaemon(t.Context(), "b"); err != nil || len(link.Daemons()) != 1 || link.Container != initial {
		t.Fatalf("retry did not remove only standby: %v", err)
	}
	if err := link.RemoveDaemon(t.Context(), "a"); err != nil || link.Container != nil || len(link.Daemons()) != 0 {
		t.Fatalf("last daemon removal did not pause topology and clear initial handle: %v", err)
	}
	if err := link.Terminate(t.Context()); err != nil || initialContainer.calls != 1 || failedContainer.calls != 2 {
		t.Fatalf("fixture cleanup repeated completed removal: initial=%d standby=%d error=%v", initialContainer.calls, failedContainer.calls, err)
	}
	if _, err := link.AddDaemon(t.Context(), "replacement"); err == nil {
		t.Fatal("terminated fixture accepted daemon allocation")
	}
}

func TestRBDMirrorTerminateClosesAdditionsButAllowsCleanupRetry(t *testing.T) {
	ctr := &cleanupContainer{err: errors.New("temporary cleanup failure")}
	daemon := &RBDMirrorDaemon{Container: ctr, DaemonName: "a"}
	link := &RBDMirror{Container: daemon, initialDaemonName: "a", daemons: []*RBDMirrorDaemon{daemon}}
	link.owned.addContainer(daemon)
	if err := link.Terminate(t.Context()); err == nil || len(link.Daemons()) != 1 {
		t.Fatal("failed fixture cleanup lost retained membership")
	}
	if _, err := link.AddDaemon(t.Context(), "new"); err == nil {
		t.Fatal("fixture cleanup failure allowed new allocation")
	}
	ctr.err = nil
	if err := link.Terminate(t.Context()); err != nil || ctr.calls != 2 || link.Container != nil || len(link.Daemons()) != 0 {
		t.Fatalf("fixture cleanup retry failed: calls=%d error=%v", ctr.calls, err)
	}
}

func TestRBDMirrorModeRejectsAmbiguousValues(t *testing.T) {
	config, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: "rbd", Mode: RBDMirrorModeJournal})
	if err != nil || config.Mode != RBDMirrorModeJournal {
		t.Fatalf("journal mode was not retained: %+v %v", config, err)
	}
	for _, mode := range []RBDMirrorMode{"pool", "image", "snapshot ", " journal", "--force", "unknown"} {
		if _, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: "rbd", Mode: mode}); err == nil {
			t.Errorf("unsupported per-image mode %q accepted", mode)
		}
	}
}

type rbdEnableFixture struct {
	testcontainers.Container
	info   string
	status string
	calls  [][]string
}

func (c *rbdEnableFixture) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.calls = append(c.calls, slices.Clone(args))
	var stream bytes.Buffer
	data := ""
	if len(args) > 1 && args[1] == "info" {
		data = c.info
	} else if len(args) > 1 && args[1] == "--admin-daemon" {
		data = c.status
	}
	if data != "" {
		header := [8]byte{byte(stdcopy.Stdout)}
		binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
		_, _ = stream.Write(header[:])
		_, _ = stream.WriteString(data)
	}
	return 0, bytes.NewReader(stream.Bytes()), nil
}

func TestRBDMirrorDaemonStatusUsesNativeElectionFields(t *testing.T) {
	ctr := &rbdEnableFixture{status: `{"pool_replayers":[{"pool":"rbd","peer":"native-peer","state":"running","instance_id":"123","leader_instance_id":"123","leader":true,"instances":["123","456"]}]}`}
	daemon := &RBDMirrorDaemon{Container: ctr, DaemonName: "a"}
	status, err := daemon.Status(t.Context())
	if err != nil || len(status.PoolReplayers) != 1 {
		t.Fatalf("native daemon status unavailable: %+v %v", status, err)
	}
	pool := status.PoolReplayers[0]
	if pool.Pool != "rbd" || pool.Peer != "native-peer" || pool.State != "running" || pool.InstanceID != "123" || pool.LeaderInstanceID != "123" || !pool.Leader || !slices.Equal(pool.Instances, []string{"123", "456"}) {
		t.Fatalf("native election state was not retained: %+v", pool)
	}
	if !slices.Equal(ctr.calls[0], []string{"ceph", "--admin-daemon", "/tmp/rbd-mirror.asok", "rbd", "mirror", "status"}) {
		t.Fatalf("status did not query owned daemon admin socket: %v", ctr.calls)
	}
	for _, data := range []string{`{`, `{}`, `{"pool_replayers":null}`} {
		ctr.status = data
		if _, err := daemon.Status(t.Context()); err == nil {
			t.Fatalf("malformed native status %s accepted", data)
		}
	}
	ctr.status = `{"pool_replayers":[]}`
	if status, err := daemon.Status(t.Context()); err != nil || len(status.PoolReplayers) != 0 {
		t.Fatalf("native discovery transition rejected: %+v error=%v", status, err)
	}
}

func TestEnableRBDJournalImageRequiresExplicitExclusiveLock(t *testing.T) {
	client := &rbdEnableFixture{info: `{"features":["layering"]}`}
	link := &RBDMirror{sourceClient: client, config: RBDMirrorConfig{Pool: "rbd", Mode: RBDMirrorModeJournal}}
	if err := link.EnableImage(t.Context(), "volume"); err == nil || !strings.Contains(err.Error(), "exclusive-lock") || len(client.calls) != 1 {
		t.Fatalf("journal setup changed an image without exclusive-lock: calls=%v error=%v", client.calls, err)
	}
	client.info = `{"features":["layering","exclusive-lock"]}`
	client.calls = nil
	if err := link.EnableImage(t.Context(), "volume"); err != nil {
		t.Fatal(err)
	}
	if len(client.calls) != 2 || !slices.Equal(client.calls[1], []string{"rbd", "mirror", "image", "enable", "rbd/volume", "journal"}) {
		t.Fatalf("journal mode was not applied to only the selected image: %v", client.calls)
	}
}

func TestEnableRBDImagePreservesExistingModeAndRejectsAmbiguousSpec(t *testing.T) {
	client := &rbdEnableFixture{info: `{"features":["exclusive-lock","journaling"],"mirroring":{"mode":"journal","state":"enabled"}}`}
	link := &RBDMirror{sourceClient: client, config: RBDMirrorConfig{Pool: "rbd", Mode: RBDMirrorModeJournal}}
	if err := link.EnableImage(t.Context(), "volume"); err != nil || len(client.calls) != 1 {
		t.Fatalf("already enabled image should remain unchanged: calls=%v error=%v", client.calls, err)
	}
	link.config.Mode = RBDMirrorModeSnapshot
	client.calls = nil
	if err := link.EnableImage(t.Context(), "volume"); err == nil || !strings.Contains(err.Error(), "already uses journal") || len(client.calls) != 1 {
		t.Fatalf("existing journal mode was implicitly changed: calls=%v error=%v", client.calls, err)
	}
	for _, name := range []string{"", " volume", "--force", "other/volume", "volume@snapshot", "volume\x00"} {
		client.calls = nil
		if err := link.EnableImage(t.Context(), name); err == nil || len(client.calls) != 0 {
			t.Errorf("ambiguous image %q reached the CLI: calls=%v error=%v", name, client.calls, err)
		}
	}
	client.info = `{"features":["exclusive-lock"],"mirroring":{"mode":"journal","state":"disabling"}}`
	client.calls = nil
	if err := link.EnableImage(t.Context(), "volume"); err == nil || len(client.calls) != 1 {
		t.Fatal("transitional mirroring state must not be mutated")
	}
}

func TestMatchingRBDMirrorPeerRestrictsDirectionChangesToSourceIdentity(t *testing.T) {
	for _, site := range []string{"source", "source-fsid"} {
		info := []byte(`{"peers":[{"uuid":"unrelated","direction":"tx-only","site_name":"other","mirror_uuid":"other-mirror"},{"uuid":"matching","direction":"tx-only","site_name":"` + site + `","mirror_uuid":"source-mirror"}]}`)
		peer, err := matchingRBDMirrorPeer(info, "source", "source-fsid", "source-mirror")
		if err != nil || peer.UUID != "matching" || peer.Direction != "tx-only" {
			t.Fatalf("site=%q: peer=%+v error=%v", site, peer, err)
		}
	}
	// A newly imported receiving peer has not necessarily learned the remote
	// mirror UUID yet. Its site identity was verified by bootstrap import.
	peer, err := matchingRBDMirrorPeer([]byte(`{"peers":[{"uuid":"new-peer","direction":"rx-only","site_name":"source"}]}`), "source", "source-fsid", "source-mirror")
	if err != nil || peer.UUID != "new-peer" || peer.Direction != "rx-only" {
		t.Fatalf("new peer=%+v error=%v", peer, err)
	}
}

func TestMatchingRBDMirrorPeerRejectsAmbiguousOrConflictingIdentity(t *testing.T) {
	for name, info := range map[string]string{
		"invalid JSON":      `{`,
		"missing peer":      `{"peers":[]}`,
		"unrelated site":    `{"peers":[{"uuid":"peer","direction":"tx-only","site_name":"other"}]}`,
		"missing UUID":      `{"peers":[{"direction":"tx-only","site_name":"source"}]}`,
		"conflicting UUID":  `{"peers":[{"uuid":"peer","direction":"tx-only","site_name":"source","mirror_uuid":"different"}]}`,
		"multiple matches":  `{"peers":[{"uuid":"peer-a","direction":"tx-only","site_name":"source"},{"uuid":"peer-b","direction":"tx-only","site_name":"source-fsid"}]}`,
		"unknown direction": `{"peers":[{"uuid":"peer","direction":"unknown","site_name":"source"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := matchingRBDMirrorPeer([]byte(info), "source", "source-fsid", "source-mirror"); err == nil {
				t.Fatal("ambiguous or conflicting peer identity accepted")
			}
		})
	}
}

func TestDecodeRBDBootstrapIdentityPreservesPeerClientWithoutSecretFields(t *testing.T) {
	const fsid = "58a9bc9a-dca0-4737-9844-a3289edfca81"
	token := base64.StdEncoding.EncodeToString([]byte(`{"fsid":"` + fsid + `","client_id":"custom-rbd-peer","key":"fixture-secret","mon_host":"v2:127.0.0.1:3300"}`))
	identity, err := decodeRBDBootstrapIdentity([]byte("\n" + token + "\n"))
	if err != nil || identity.FSID != fsid || identity.ClientName != "client.custom-rbd-peer" {
		t.Fatalf("unexpected bootstrap identity: %+v error=%v", identity, err)
	}
}

func TestDecodeRBDBootstrapIdentityRejectsInvalidIdentityWithoutExposingToken(t *testing.T) {
	for name, data := range map[string]string{
		"invalid JSON":   `{"key":"fixture-secret",`,
		"invalid FSID":   `{"fsid":"fixture-secret","client_id":"peer"}`,
		"missing client": `{"fsid":"58a9bc9a-dca0-4737-9844-a3289edfca81","key":"fixture-secret"}`,
		"invalid client": `{"fsid":"58a9bc9a-dca0-4737-9844-a3289edfca81","client_id":"-option","key":"fixture-secret"}`,
	} {
		t.Run(name, func(t *testing.T) {
			token := base64.StdEncoding.EncodeToString([]byte(data))
			if _, err := decodeRBDBootstrapIdentity([]byte(token)); err == nil || strings.Contains(err.Error(), "fixture-secret") || strings.Contains(err.Error(), token) {
				t.Fatalf("invalid identity was accepted or exposed: error=%v", err)
			}
		})
	}
	if _, err := decodeRBDBootstrapIdentity([]byte("not base64")); err == nil {
		t.Fatal("invalid token encoding accepted")
	}
}
