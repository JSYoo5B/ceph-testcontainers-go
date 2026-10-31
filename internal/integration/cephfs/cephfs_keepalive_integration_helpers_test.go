//go:build all || integration

package cephfs_test

import (
	"context"
	"encoding/json"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// keepaliveFrames counts the keepalive and acknowledgement frames of the
// ProtocolV2 sessions that a debug_ms 20 log shows. The session carries
// these frames; neither debug_ms 1 nor messenger dump names them.
type keepaliveFrames struct {
	// SentKeepalive is "send_keepalive": a KEEPALIVE2 frame was written.
	SentKeepalive int `json:"sent_keepalive"`
	// GotKeepalive is "handle_keepalive2 got KEEPALIVE2": a KEEPALIVE2 frame
	// was received, with a stamp the responder echoes in its ack.
	GotKeepalive int `json:"got_keepalive"`
	// GotKeepaliveAck is "handle_keepalive2_ack got KEEPALIVE_ACK": a
	// KEEPALIVE2_ACK frame was received for a keepalive this side sent.
	GotKeepaliveAck int `json:"got_keepalive_ack"`
	// GotMessageAck is "handle_message_ack got ack seq": an ACK frame
	// acknowledged received messages up to a sequence.
	GotMessageAck int `json:"got_message_ack"`
	// SentMessageAck is "write_event try send msg ack, acked N messages": an
	// ACK frame was written for messages this side received.
	SentMessageAck int `json:"sent_message_ack"`
}

// countKeepaliveFrames counts the keepalive and ack frames over every
// ProtocolV2 session (the "--2-" lines) in a debug_ms 20 log. When peer is
// not empty, only lines whose connection names that peer substring are
// counted. The frame text follows the connection's "conn(...)" state on the
// same line, so each kind is matched as a substring.
func countKeepaliveFrames(lines []string, peer string) keepaliveFrames {
	var frames keepaliveFrames
	for _, line := range lines {
		if !strings.Contains(line, "--2- ") || !strings.Contains(line, " >> ") || (peer != "" && !strings.Contains(line, peer)) {
			continue
		}
		switch {
		case strings.Contains(line, "handle_keepalive2_ack got KEEPALIVE_ACK"):
			frames.GotKeepaliveAck++
		case strings.Contains(line, "handle_keepalive2 got KEEPALIVE2 "):
			frames.GotKeepalive++
		case strings.HasSuffix(line, "send_keepalive"):
			frames.SentKeepalive++
		case strings.Contains(line, "handle_message_ack got ack seq "):
			frames.GotMessageAck++
		case strings.Contains(line, "write_event try send msg ack, acked "):
			frames.SentMessageAck++
		}
	}
	return frames
}

// cephFSKeepaliveActor keeps one libcephfs mount open at debug_ms 20 logged
// to a file. It writes and fsyncs the file, which the MDS acknowledges, then
// idles long enough for the session to exchange keepalives, several times.
// It publishes receipt 1 with its client ID, and after the "finish" gate
// unmounts and publishes receipt 2. Arguments after the common ones: file,
// the log path, the idle seconds, the number of cycles.
const cephFSKeepaliveActor harness.CephFSActorScript = harness.CephFSActorHelpers + `
import time as _time
path, logfile, idle, cycles = sys.argv[4], sys.argv[5], int(sys.argv[6]), int(sys.argv[7])
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
for key, value in (("client_mount_timeout", "30"), ("log_file", logfile), ("log_to_file", "true"),
                   ("log_to_stderr", "false"), ("debug_ms", "20")):
    fs.conf_set(key, value)
fs.mount(filesystem_name=name.encode())
fd = fs.open(path.encode(), os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
publish(1, client=fs.get_instance_id())
for cycle in range(cycles):
    fs.write(fd, ("keepalive-%d" % cycle).encode(), 0)
    fs.fsync(fd, 0)
    fs.stat(path.encode())
    _time.sleep(idle)
gate("finish")
closing = {}
try:
    fs.close(fd)
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(2, closed=True, **closing)
`

// cephFSKeepaliveReceipt is the evidence of the keepalive and ack frames a
// native client and the MDS exchanged.
type cephFSKeepaliveReceipt struct {
	Release    string `json:"release"`
	Platform   string `json:"platform"`
	Run        string `json:"run,omitempty"`
	Client     uint64 `json:"client"`
	MDS        string `json:"mds"`
	ClientAddr string `json:"client_addr"`
	// ClientToMON counts the client's frames on its MON session, ClientToMDS
	// on its MDS session; MONFromClient and MDSFromClient are the daemons'
	// frames with the client, each filtered to the actor's own address.
	ClientToMON   keepaliveFrames `json:"client_to_mon"`
	ClientToMDS   keepaliveFrames `json:"client_to_mds"`
	MONFromClient keepaliveFrames `json:"mon_from_client"`
	MDSFromClient keepaliveFrames `json:"mds_from_client"`
}

// cephNetworkIP returns ctr's address on the cluster's Ceph network.
func cephNetworkIP(t *testing.T, ctx context.Context, cluster *ceph.Container, ctr testcontainers.Container) string {
	t.Helper()
	inspect, err := ctr.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := inspect.NetworkSettings.Networks[harness.StaticFacts(cluster).NetworkName]
	if endpoint == nil || !endpoint.IPAddress.IsValid() {
		t.Fatal("container has no address on the Ceph network")
	}
	return endpoint.IPAddress.String()
}

// testCephFSMessengerKeepalive keeps one libcephfs client mounted at
// debug_ms 20 while it writes, fsyncs and then idles in cycles. Its own log
// shows it sending KEEPALIVE2 frames and receiving KEEPALIVE2_ACK, and
// exchanging ACK frames for the MDS cap messages. The client's keepalives
// run on its lossy MON session, so the MON's log, also at debug_ms 20, shows
// it receiving those KEEPALIVE2 frames, while the MDS's log shows it
// acknowledging the client's cap messages. These frames are named only at
// debug_ms 20.
func testCephFSMessengerKeepalive(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	m := harness.StartCephFSSingleMDS(t, ctx)
	for _, daemon := range []string{"mds", "mon"} {
		if _, err := m.Cluster.Ceph(ctx, "config", "set", daemon, "debug_ms", "20"); err != nil {
			t.Fatalf("%s debug_ms: %v", daemon, err)
		}
	}
	receipt := cephFSKeepaliveReceipt{Platform: runtime.GOOS + "/" + runtime.GOARCH, MDS: "mds." + m.MDS.Name}
	if run := os.Getenv("GITHUB_RUN_ID"); run != "" {
		receipt.Run = run + "/" + os.Getenv("GITHUB_RUN_ATTEMPT")
	}
	if raw, err := m.Cluster.Ceph(ctx, "tell", "mds."+m.MDS.Name, "version"); err == nil {
		var version map[string]string
		if json.Unmarshal(raw, &version) == nil {
			receipt.Release = version["version"]
		}
	}
	defer func() {
		data, err := json.Marshal(receipt)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("CEPHFS_KEEPALIVE_RECEIPT %s", data)
	}()

	const logfile = "/tmp/keepalive-actor/client.log"
	// Idle gaps longer than the keepalive interval, over several cycles.
	actor := harness.StartCephFSActor(t, ctx, m.Cluster, m.Client, cephFSKeepaliveActor, m.FS.FilesystemName, "/tmp/keepalive-actor", "/keepalive-file", logfile, "9", "4")
	baseline := actor.Receipt(t, ctx, 1)
	if baseline.Client == 0 {
		t.Fatalf("baseline receipt = %+v", baseline)
	}
	receipt.Client = baseline.Client
	actor.Open(t, ctx, "finish")
	if closing := actor.Receipt(t, ctx, 2); !closing.Closed || closing.CloseErrno != 0 {
		t.Fatalf("actor close receipt = %+v", closing)
	}
	actor.Wait(t, ctx)

	clientLog, err := m.Cluster.RunNativeCommand(ctx, m.Client, ceph.NativeCommand{
		Argv: []string{"cat", logfile}, Timeout: time.Minute, MaxStdout: 64 << 20, MaxStderr: 16 << 10, Label: "keepalive client log",
	})
	if err != nil || clientLog.ExitCode != 0 || !clientLog.Complete() {
		t.Fatalf("read client log: exit %d, complete %t: %v", clientLog.ExitCode, clientLog.Complete(), err)
	}
	clientLines := strings.Split(string(clientLog.Stdout), "\n")
	// Every frame is attributed by address. The client container runs only
	// this actor as a Ceph client, so the daemons log its sessions under the
	// container's "<ip>:0/" address; the fixture's own CLI runs in other
	// containers with other addresses.
	clientIP := cephNetworkIP(t, ctx, m.Cluster, m.Client)
	receipt.ClientAddr = clientIP + ":0/"

	// The client keepalives its lossy MON session and acks the MDS's cap
	// messages, so filter the client log by each daemon's address.
	monIP := cephNetworkIP(t, ctx, m.Cluster, clusteraccess.Monitors(t, m.Cluster)[0].Container)
	mdsIP := cephNetworkIP(t, ctx, m.Cluster, mdsContainer(t, m))
	receipt.ClientToMON = countKeepaliveFrames(clientLines, "v2:"+monIP+":")
	receipt.ClientToMDS = countKeepaliveFrames(clientLines, "v2:"+mdsIP+":")
	// The daemons log their frames with this actor by its container address.
	receipt.MDSFromClient = countKeepaliveFrames(harness.ContainerLogLines(t, ctx, mdsContainer(t, m)), receipt.ClientAddr)
	for _, mon := range clusteraccess.Monitors(t, m.Cluster) {
		frames := countKeepaliveFrames(harness.ContainerLogLines(t, ctx, mon.Container), receipt.ClientAddr)
		if frames.GotKeepalive > 0 {
			receipt.MONFromClient = frames
			break
		}
	}

	// On its MON session the client sent keepalives and received their acks;
	// on its MDS session it exchanged message acks; the MON received those
	// keepalives from this actor and the MDS acked this actor's messages.
	if receipt.ClientToMON.SentKeepalive == 0 || receipt.ClientToMON.GotKeepaliveAck == 0 {
		t.Fatalf("client keepalive frames to the MON %s: %+v", monIP, receipt.ClientToMON)
	}
	if receipt.ClientToMDS.GotMessageAck == 0 || receipt.ClientToMDS.SentMessageAck == 0 {
		t.Fatalf("client message-ack frames to the MDS %s: %+v", mdsIP, receipt.ClientToMDS)
	}
	if receipt.MONFromClient.GotKeepalive == 0 {
		t.Fatalf("no MON logged a KEEPALIVE2 from %s: %+v", receipt.ClientAddr, receipt.MONFromClient)
	}
	if receipt.MDSFromClient.SentMessageAck == 0 {
		t.Fatalf("the MDS logged no message ack to %s: %+v", receipt.ClientAddr, receipt.MDSFromClient)
	}
}
