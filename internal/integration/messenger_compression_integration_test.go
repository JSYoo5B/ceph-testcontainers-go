//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_messenger_receipts))))

//ci: timeout=30m job-timeout=45 image-roles=essential,client,cephfs

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// compressionPhaseReceipt is what one phase observed.
type compressionPhaseReceipt struct {
	Phase      string `json:"phase"`
	Evidence   string `json:"evidence"`
	PeerOSD    int    `json:"osd_sessions"`
	PeerOther  int    `json:"other_sessions"`
	Mode       string `json:"mode"`
	Compressed bool   `json:"compressed"`
	Algorithm  string `json:"algorithm,omitempty"`
	// Client is the session of a client that asked for compression, as the
	// object's primary OSD reports it; Features are the MON sessions'
	// negotiated features.
	Client   string            `json:"client"`
	Features map[string]string `json:"features"`
}

// TestMessengerCompressionReceipt records which Messenger v2 sessions
// negotiate on-wire compression, per peer path, and the features each MON
// session negotiated. Two OSDs share a replicated pool; between phases the
// OSD policy changes and osd.1 restarts, so every session between the OSDs
// is negotiated again under the new policy.
//
//   - default: no compression policy; OSD sessions are CRC and uncompressed.
//   - force: ms_osd_compress_mode=force; OSD sessions are CRC and compressed
//     with snappy. Sessions with MONs, MGRs and clients stay uncompressed,
//     also for a client that asks for compression itself.
//   - secure: ms_cluster_mode=secure as well; OSD sessions are secure and,
//     with ms_compress_secure off, uncompressed although compression is
//     forced: the setting alone is no evidence of negotiation.
//   - secure-compressed: ms_compress_secure=true as well; OSD sessions are
//     secure and compressed.
//
// Ceph 20 reports each session's mode, cipher and compressor in "messenger
// dump". A release without that command logs each session's READY line at
// debug_ms 1 with its crypto and compression handlers; there the receipt
// records whether a compressor exists, not its name.
func TestMessengerCompressionReceipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	config := filepath.Join(t.TempDir(), "ceph.conf")
	// The READY lines are the evidence where messenger dump is missing.
	if err := os.WriteFile(config, []byte("[osd]\ndebug_ms = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cluster, _ := newServiceClusterWithOptions(t, image, roles.WithOptions(ceph.WithOSDCount(2),
		ceph.WithMessengerMode(ceph.MessengerV2Only), ceph.WithConfigFile(config)))
	const pool = "tc-compression"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 8, Replicas: 2, MinSize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	osds := clusteraccess.OSDs(t, cluster)
	if len(osds) != 2 {
		t.Fatalf("cluster has %d OSDs, want 2", len(osds))
	}
	dump := capabilitySupported(t, ctx, cluster, capabilityMessengerDump)

	force := ceph.ConfigSetting{Section: "osd", Name: "ms_osd_compress_mode", Value: "force"}
	algorithm := ceph.ConfigSetting{Section: "osd", Name: "ms_osd_compression_algorithm", Value: "snappy"}
	secure := ceph.ConfigSetting{Section: "osd", Name: "ms_cluster_mode", Value: "secure"}
	compressSecure := ceph.ConfigSetting{Section: "osd", Name: "ms_compress_secure", Value: "true"}
	phases := []compressionPhase{
		{name: "default", mode: "crc"},
		{name: "force", settings: []ceph.ConfigSetting{force, algorithm}, mode: "crc", compressed: true},
		{name: "secure", settings: []ceph.ConfigSetting{secure}, mode: "secure"},
		{name: "secure-compressed", settings: []ceph.ConfigSetting{compressSecure}, mode: "secure", compressed: true},
	}
	var receipts []compressionPhaseReceipt
	for _, phase := range phases {
		// Log offsets, so a phase reads only the READY lines of its own
		// sessions: those after osd.1 stopped. The default phase changes
		// nothing, so every session since the cluster started is its
		// evidence.
		offsets := make([]int, len(osds))
		for _, setting := range phase.settings {
			overridden, err := override.SetConfig(ctx, cluster, setting)
			if err != nil {
				t.Fatalf("%s: %s: %v", phase.name, setting.Name, err)
			}
			t.Cleanup(func() {
				restore, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if err := overridden.Restore(restore); err != nil {
					t.Error(err)
				}
			})
			// osd.0 applies the runtime options to the sessions it opens and
			// accepts from now on; osd.1 reads them when it starts again.
			waitOSDOption(t, ctx, cluster, "osd.0", setting.Name, setting.Value)
		}
		if phase.settings != nil {
			stop := 20 * time.Second
			if err := osds[1].Stop(ctx, &stop); err != nil {
				t.Fatal(err)
			}
			for i, osd := range osds {
				offsets[i] = len(containerLogLines(t, ctx, osd))
			}
			if err := osds[1].Start(ctx); err != nil {
				t.Fatal(err)
			}
			for _, setting := range phase.settings {
				waitOSDOption(t, ctx, cluster, "osd.1", setting.Name, setting.Value)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
		}
		receipt := observeCompressionPhase(t, ctx, cluster, phase, pool, dump, offsets)
		receipts = append(receipts, receipt)
	}
	data, err := json.Marshal(receipts)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MESSENGER_COMPRESSION_RECEIPT %s", data)
}

// waitOSDOption waits until daemon runs with option set to want.
func waitOSDOption(t *testing.T, ctx context.Context, cluster *ceph.Container, daemon, option, want string) {
	t.Helper()
	var last string
	for deadline := time.Now().Add(2 * time.Minute); ; {
		out, err := adminSocket(t, ctx, cluster, daemon, "config", "get", option)
		var value map[string]string
		if err == nil && json.Unmarshal(out, &value) == nil && value[option] == want {
			return
		}
		last = fmt.Sprintf("%s %v", strings.TrimSpace(string(out)), err)
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("%s %s = %s, want %s", daemon, option, last, want)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// observeCompressionPhase writes through a client that asks for compression
// and checks every session of both OSDs against the phase. Sessions open
// lazily (heartbeats, peering, replication), so it polls until the evidence
// holds or a bound passes.
func observeCompressionPhase(t *testing.T, ctx context.Context, cluster *ceph.Container, phase compressionPhase,
	pool string, dump bool, offsets []int) compressionPhaseReceipt {
	t.Helper()
	osds := clusteraccess.OSDs(t, cluster)
	receipt := compressionPhaseReceipt{Phase: phase.name, Mode: phase.mode, Compressed: phase.compressed}
	var problems []string
	for deadline := time.Now().Add(2 * time.Minute); ; {
		problems = nil
		arg := "0"
		if dump {
			arg = "1"
		}
		out := execOutput(t, ctx, toolkitContainer(t, cluster), "python3", "-c", messengerCompressionProbe, pool, "compressible-"+phase.name, arg)
		var probe struct {
			ClientGlobalID uint64                                `json:"client_global_id"`
			Bytes          int                                   `json:"bytes"`
			Acting         []int                                 `json:"acting"`
			Primary        int                                   `json:"primary"`
			Sessions       json.RawMessage                       `json:"sessions"`
			Dumps          map[string]map[string]json.RawMessage `json:"dumps"`
		}
		if err := json.Unmarshal([]byte(out), &probe); err != nil {
			t.Fatalf("%s probe: %v: %s", phase.name, err, out)
		}
		if len(probe.Acting) != 2 {
			t.Fatalf("%s: the object maps to %v, want both OSDs", phase.name, probe.Acting)
		}

		// The MON's sessions: every daemon and the client negotiated the
		// features that both supported releases require.
		sessions, err := parseFeatureSessions(probe.Sessions)
		if err != nil {
			t.Fatal(err)
		}
		receipt.Features = map[string]string{}
		for _, session := range sessions {
			entity := session.Entity
			if session.Type == "client" && session.GlobalID == probe.ClientGlobalID {
				entity = "probe-client"
			} else if session.Type == "client" {
				continue
			}
			receipt.Features[entity] = fmt.Sprintf("%#x %s", session.Features, session.Release)
			if missing := missingSessionFeatures(session.Features); len(missing) != 0 {
				problems = append(problems, fmt.Sprintf("MON session %s lacks %v", entity, missing))
			}
		}
		for _, entity := range []string{"osd.0", "osd.1", "probe-client"} {
			if _, ok := receipt.Features[entity]; !ok {
				problems = append(problems, "the MON has no session of "+entity)
			}
		}

		if dump {
			receipt.Evidence = "messenger dump"
			receipt.PeerOSD, receipt.PeerOther, receipt.Client = 0, 0, ""
			for daemon, messengers := range probe.Dumps {
				osdSessions := 0
				for name, raw := range messengers {
					connections, err := parseMessengerConnections(raw)
					if err != nil {
						t.Fatalf("%s %s: %v", daemon, name, err)
					}
					for _, c := range connections {
						if !c.ready() {
							continue
						}
						problems = append(problems, checkDumpedSession(daemon, c, phase)...)
						switch {
						case c.PeerType == "osd":
							osdSessions++
							receipt.PeerOSD++
							receipt.Algorithm = c.CompressionTx
						case c.PeerType == "client" && c.GlobalID == probe.ClientGlobalID:
							receipt.Client = fmt.Sprintf("%s %s rx=%s tx=%s", daemon, c.Mode, c.CompressionRx, c.CompressionTx)
							receipt.PeerOther++
						default:
							receipt.PeerOther++
						}
					}
				}
				if osdSessions == 0 {
					problems = append(problems, daemon+" has no READY session with an OSD")
				}
			}
			if receipt.Client == "" {
				problems = append(problems, fmt.Sprintf("no OSD reports a session of the probe client %d", probe.ClientGlobalID))
			}
		} else {
			receipt.Evidence = "debug_ms READY lines"
			receipt.PeerOSD, receipt.PeerOther, receipt.Client = 0, 0, ""
			for i, osd := range osds {
				lines := containerLogLines(t, ctx, osd)
				if offsets[i] < len(lines) {
					lines = lines[offsets[i]:]
				}
				osdSessions := 0
				for _, c := range parseReadyLogLines(lines) {
					problems = append(problems, checkLoggedSession(fmt.Sprintf("osd.%d", osd.ID), c, phase)...)
					switch {
					case c.PeerType == "osd":
						osdSessions++
						receipt.PeerOSD++
					case c.PeerType == "client" && c.PeerID == fmt.Sprint(probe.ClientGlobalID):
						receipt.Client = fmt.Sprintf("osd.%d %s compression=%v", osd.ID, c.Mode, c.Compression)
						receipt.PeerOther++
					default:
						receipt.PeerOther++
					}
				}
				if osdSessions == 0 {
					problems = append(problems, fmt.Sprintf("osd.%d logged no READY session with an OSD", osd.ID))
				}
			}
			if receipt.Client == "" {
				problems = append(problems, fmt.Sprintf("no OSD logged a session of the probe client %d", probe.ClientGlobalID))
			}
		}
		if len(problems) == 0 {
			t.Logf("%s: %+v", phase.name, receipt)
			return receipt
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("%s: %s", phase.name, strings.Join(problems, "; "))
		}
		time.Sleep(5 * time.Second)
	}
}
