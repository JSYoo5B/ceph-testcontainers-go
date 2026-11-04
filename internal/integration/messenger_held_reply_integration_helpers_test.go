//go:build all || integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	heldReplyPool    = "held-reply"
	heldReplyObject  = "held-reply"
	heldReplyWorkdir = "/tmp/held-reply"
	// heldReplySize bounds the reply: large enough to outgrow the receiver's
	// socket buffer and the OSD's send buffer, small enough for any host.
	heldReplySize = 32 << 20
	// heldReplyQueued is the least unacknowledged send queue that counts as
	// a held reply; an idle session has none.
	heldReplyQueued = 64 << 10
	// heldReplySamples is the number of consecutive one-second samples the
	// queue must stay held.
	heldReplySamples = 4
)

// heldReplyPayload is the object's content, as heldReplyPython builds it.
func heldReplyPayload() []byte {
	payload := make([]byte, heldReplySize)
	for i := range payload {
		payload[i] = byte(i)
	}
	return payload
}

const heldReplyPython = `
def payload():
    return bytes(range(256)) * ((32 << 20) // 256)
`

// heldReplySeeder writes the object with a fresh client and reads it back.
const heldReplySeeder = `import json, sys, rados` + heldReplyPython + `
data = payload()
with rados.Rados(conffile="/etc/ceph/ceph.conf") as cluster:
    with cluster.open_ioctx(sys.argv[1]) as io:
        io.write_full(sys.argv[2], data)
        assert io.read(sys.argv[2], len(data) + 1, 0) == data
        print(json.dumps({"version": io.get_last_version()}))`

// heldReplyVerifier reads the object with a fresh client, without writing.
const heldReplyVerifier = `import hashlib, json, sys, rados
with rados.Rados(conffile="/etc/ceph/ceph.conf") as cluster:
    with cluster.open_ioctx(sys.argv[1]) as io:
        data = io.read(sys.argv[2], (32 << 20) + 1, 0)
        print(json.dumps({"size": len(data), "sha256": hashlib.sha256(data).hexdigest(), "version": io.get_last_version()}))`

// heldReplyReader is the consumer whose reads stop. It stats the object and
// reads its first byte, which opens its session with the OSD, publishes
// "ready" and waits for "read" on its standard input. It then submits one
// asynchronous read of the whole object, with no client timeout, publishes
// "submitted" and prints the result when the reply arrives. Its messenger log
// goes to client.log.
const heldReplyReader = `import hashlib, json, os, sys, threading, time, rados
pool, obj, workdir = sys.argv[1:4]
os.makedirs(workdir, exist_ok=True)
for name in ("ready", "submitted", "client.log"):
    if os.path.exists(os.path.join(workdir, name)):
        os.remove(os.path.join(workdir, name))
def publish(name, **fields):
    path = os.path.join(workdir, name)
    with open(path + ".tmp", "w") as f:
        json.dump(fields, f)
    os.rename(path + ".tmp", path)
conf = {"log_file": os.path.join(workdir, "client.log"), "log_to_file": "true", "log_to_stderr": "false",
        "err_to_stderr": "false", "debug_ms": "1", "rados_osd_op_timeout": "0"}
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf=conf) as cluster:
    with cluster.open_ioctx(pool) as io:
        size, _ = io.stat(obj)
        assert io.read(obj, 1, 0) == b"\x00"
        publish("ready", client="client.%d" % cluster.get_instance_id(), size=size, pid=os.getpid())
        if sys.stdin.readline().strip() != "read":
            raise SystemExit("no read signal")
        done, result = threading.Event(), {}
        def complete(completion, data):
            result["rc"], result["data"] = completion.get_return_value(), data
            done.set()
        started = time.monotonic()
        io.aio_read(obj, size + 1, 0, complete)
        publish("submitted", pid=os.getpid())
        done.wait()
        data = result["data"] or b""
        print(json.dumps({"rc": result["rc"], "size": len(data), "sha256": hashlib.sha256(data).hexdigest(), "version": io.get_last_version(),
                          "seconds": round(time.monotonic() - started, 3)}))`

// socketRow is one row of /proc/net/tcp or tcp6: the endpoints, the TCP state
// and the send and receive queues. TX counts bytes the peer has not yet
// acknowledged; RX bytes the local process has not yet read.
type socketRow struct {
	Local, Remote netip.AddrPort
	State         uint8
	TX, RX        uint64
}

// tcpEstablished is the kernel's TCP_ESTABLISHED.
const tcpEstablished = 1

// parseSocketTable parses the concatenated /proc/net/tcp and tcp6 of one
// network namespace. Header lines are skipped; any other malformed line is an
// error, so a changed format never reads as an empty table.
func parseSocketTable(data string) ([]socketRow, error) {
	var rows []socketRow
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] == "sl" {
			continue
		}
		if len(fields) < 5 {
			return nil, fmt.Errorf("socket row %q: too few fields", line)
		}
		local, err := parseSocketAddr(fields[1])
		if err != nil {
			return nil, fmt.Errorf("socket row %q: %w", line, err)
		}
		remote, err := parseSocketAddr(fields[2])
		if err != nil {
			return nil, fmt.Errorf("socket row %q: %w", line, err)
		}
		state, err := strconv.ParseUint(fields[3], 16, 8)
		if err != nil {
			return nil, fmt.Errorf("socket row %q: state: %w", line, err)
		}
		tx, rx, ok := strings.Cut(fields[4], ":")
		if !ok {
			return nil, fmt.Errorf("socket row %q: no queue pair", line)
		}
		row := socketRow{Local: local, Remote: remote, State: uint8(state)}
		if row.TX, err = strconv.ParseUint(tx, 16, 64); err != nil {
			return nil, fmt.Errorf("socket row %q: send queue: %w", line, err)
		}
		if row.RX, err = strconv.ParseUint(rx, 16, 64); err != nil {
			return nil, fmt.Errorf("socket row %q: receive queue: %w", line, err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// parseSocketAddr parses a kernel address: 8 or 32 hex digits of 32-bit
// words in host byte order, a colon and a big-endian hex port. An IPv4-mapped
// IPv6 address is unmapped, so a dual-stack socket compares with its peer.
func parseSocketAddr(field string) (netip.AddrPort, error) {
	host, port, ok := strings.Cut(field, ":")
	if !ok || (len(host) != 8 && len(host) != 32) {
		return netip.AddrPort{}, fmt.Errorf("address %q", field)
	}
	raw, err := hex.DecodeString(host)
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("address %q: %w", field, err)
	}
	number, err := strconv.ParseUint(port, 16, 16)
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("port %q: %w", field, err)
	}
	ip := make([]byte, len(raw))
	for i := 0; i < len(raw); i += 4 {
		binary.BigEndian.PutUint32(ip[i:], binary.LittleEndian.Uint32(raw[i:]))
	}
	addr, _ := netip.AddrFromSlice(ip)
	return netip.AddrPortFrom(addr.Unmap(), uint16(number)), nil
}

// socketPair is one TCP connection seen from both ends.
type socketPair struct {
	OSD    netip.AddrPort `json:"osd"`
	Client netip.AddrPort `json:"client"`
}

// pairSockets returns the established connections whose ends appear in the
// OSD's and the client's tables, each in its own network namespace.
func pairSockets(osd, client []socketRow) []socketPair {
	var pairs []socketPair
	for _, o := range osd {
		if o.State != tcpEstablished {
			continue
		}
		for _, c := range client {
			if c.State == tcpEstablished && c.Local == o.Remote && c.Remote == o.Local {
				pairs = append(pairs, socketPair{OSD: o.Local, Client: o.Remote})
			}
		}
	}
	return pairs
}

// find returns the pair's established row in the OSD's table.
func (p socketPair) find(rows []socketRow) (socketRow, bool) {
	return findSocket(rows, p.OSD, p.Client)
}

// findClient returns the pair's established row in the client's table.
func (p socketPair) findClient(rows []socketRow) (socketRow, bool) {
	return findSocket(rows, p.Client, p.OSD)
}

// requestDelivered reports whether a request the client logged has reached
// the frozen OSD's socket. The client logs a message when its Messenger
// queues it, before a separate handler writes it, so an empty client send
// queue alone also holds for a request still waiting to be written. Only
// bytes waiting unread at the OSD's end, which held none while idle and whose
// process reads nothing while frozen, show that the write happened; with the
// client's send queue empty, the kernel has acknowledged all of it.
func requestDelivered(logged bool, client, osd socketRow, idle heldReplySample) bool {
	return logged && client.TX == 0 && osd.RX > idle.RX
}

func findSocket(rows []socketRow, local, remote netip.AddrPort) (socketRow, bool) {
	for _, row := range rows {
		if row.State == tcpEstablished && row.Local == local && row.Remote == remote {
			return row, true
		}
	}
	return socketRow{}, false
}

// socketTable reads one container's network namespace.
func socketTable(t *testing.T, ctx context.Context, ctr testcontainers.Container) []socketRow {
	t.Helper()
	code, out, err := harness.SessionExec(ctx, ctr, "cat", "/proc/net/tcp", "/proc/net/tcp6")
	if err != nil || code != 0 {
		t.Fatalf("socket table of %s: exit %d: %v", ctr.GetContainerID(), code, err)
	}
	rows, err := parseSocketTable(out)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// heldReplySample is the OSD's end of the held connection at one instant.
type heldReplySample struct {
	Established bool   `json:"established"`
	TX          uint64 `json:"tx"`
	RX          uint64 `json:"rx"`
}

// heldReplyReceipt records one held-reply run.
type heldReplyReceipt struct {
	Case     string `json:"case"`
	Release  string `json:"release"`
	Platform string `json:"platform"`
	Run      string `json:"run,omitempty"`
	Images   struct {
		OSD    string `json:"osd"`
		Client string `json:"client"`
	} `json:"images"`
	OSD        string          `json:"osd"`
	Client     string          `json:"client"`
	Connection socketPair      `json:"connection"`
	Seeded     heldReadback    `json:"seeded"`
	Idle       heldReplySample `json:"idle"`
	// Delivered is the OSD's end once the frozen OSD holds the request.
	Delivered heldReplySample   `json:"delivered"`
	Held      []heldReplySample `json:"held"`
	// Control is the fixture's MON command and the OSD's answer to another
	// session, both while the reply is held.
	Control []string      `json:"control"`
	Drained *heldReadback `json:"drained,omitempty"`
	// Closed is how long the OSD kept the abandoned connection.
	Closed  string       `json:"closed,omitempty"`
	Fresh   heldReadback `json:"fresh"`
	Cleanup []string     `json:"cleanup"`
}

// heldReplyRun is the retained reader while it runs.
type heldReplyRun struct {
	input  *ceph.NativeInput
	done   chan struct{}
	result ceph.NativeCommandResult
	err    error
}

func (r *heldReplyRun) finished() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

// heldReplyMark waits for the reader's named file and decodes it.
func heldReplyMark(t *testing.T, ctx context.Context, ctr testcontainers.Container, name string, reader *heldReplyRun) map[string]any {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Minute); ; {
		if code, out, err := harness.SessionExec(ctx, ctr, "cat", heldReplyWorkdir+"/"+name); err == nil && code == 0 {
			var mark map[string]any
			if err := json.Unmarshal([]byte(out), &mark); err != nil {
				t.Fatalf("reader mark %s: %v %q", name, err, out)
			}
			return mark
		}
		if reader.finished() {
			t.Fatalf("the reader ended before %s: exit %d: %v: %.2048s", name, reader.result.ExitCode, reader.err, reader.result.Stderr)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the reader did not publish %s", name)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// testHeldReply holds one bounded RADOS reply behind a consumer that stops
// reading, proves the hold from the OSD's own send queue on that connection,
// and then either drains it (abandon false) or kills the consumer with the
// reply still held (abandon true). Either way a fresh client reads the exact
// bytes back without rewriting them.
//
// The order closes every race. The OSD is frozen first, so its kernel
// acknowledges the request into the OSD's receive buffer but nothing answers.
// Once the request waits unread at the OSD's end and nothing remains in the
// client's send queue, the consumer is frozen and the OSD thawed: the OSD reads the
// request and writes the reply into a connection whose receiver reads
// nothing. If the reply does not stay queued, the condition is not
// established and the test fails rather than passing on a reply that fitted
// the buffers.
func testHeldReply(t *testing.T, abandon bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, _ := harness.IntegrationImages(t)
	cluster, client := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: heldReplyPool, PGNum: 1, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	osd := clusteraccess.OSDs(t, cluster)[0]
	receipt := heldReplyReceipt{Case: "drain", Platform: runtime.GOOS + "/" + runtime.GOARCH, OSD: fmt.Sprintf("osd.%d", osd.ID)}
	if abandon {
		receipt.Case = "abandon"
	}
	if run := os.Getenv("GITHUB_RUN_ID"); run != "" {
		receipt.Run = run + "/" + os.Getenv("GITHUB_RUN_ATTEMPT")
	}
	// Registered first, so it runs last and records every restore.
	t.Cleanup(func() {
		data, err := json.Marshal(receipt)
		if err != nil {
			t.Errorf("held reply receipt: %v", err)
			return
		}
		t.Logf("HELD_REPLY_RECEIPT %s", data)
	})
	payload := heldReplyPayload()
	sum := sha256.Sum256(payload)
	want := hex.EncodeToString(sum[:])
	seed, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: []string{"python3", "-c", heldReplySeeder, heldReplyPool, heldReplyObject}, Timeout: 3 * time.Minute,
		MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "held reply seed",
	})
	if err != nil || seed.ExitCode != 0 || !seed.Complete() {
		t.Fatalf("seed: exit %d: %v: %.2048s", seed.ExitCode, err, seed.Stderr)
	}
	receipt.Seeded = heldReadback{Size: len(payload), SHA256: want}
	if err := json.Unmarshal([]byte(harness.LastJSONLine(string(seed.Stdout))), &receipt.Seeded); err != nil || receipt.Seeded.Version == 0 {
		t.Fatalf("seed output: %v: %.512s", err, seed.Stdout)
	}
	receipt.Release = targetedJSON[map[string]string](t, ctx, cluster, "tell", receipt.OSD, "version")["version"]

	// Bounded cleanup, independent of the test's own deadline: each restores
	// what it changed and records the outcome.
	restore := func(what string, fn func(context.Context) error) {
		restoreCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		defer stop()
		outcome := "restored"
		if err := fn(restoreCtx); err != nil {
			outcome = err.Error()
			t.Errorf("restore %s: %v", what, err)
		}
		receipt.Cleanup = append(receipt.Cleanup, what+": "+outcome)
	}
	// nodown keeps the frozen OSD in the map, so no peer or client acts on
	// a failure report while it is frozen.
	nodown, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoDown, true)
	t.Cleanup(func() { restore("nodown", nodown.Restore) })
	if err != nil {
		t.Fatal(err)
	}

	// The consumer has a container of its own, so freezing or killing it
	// leaves the fixture's client and every other session alone.
	consumer, err := testcontainers.Run(ctx, harness.NativeClientImage(t, image), cluster.WithClient(),
		ceph.WithIdleEntrypoint(),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})),
	)
	if consumer != nil {
		testcontainers.CleanupContainer(t, consumer)
	}
	if err != nil {
		t.Fatal(err)
	}
	receipt.Images.OSD, receipt.Images.Client = imageID(t, ctx, osd), imageID(t, ctx, consumer)

	reader := &heldReplyRun{input: ceph.NewNativeInput(), done: make(chan struct{})}
	readCtx, stopReader := context.WithCancel(ctx)
	go func() {
		defer close(reader.done)
		reader.result, reader.err = cluster.RunNativeCommand(readCtx, consumer, ceph.NativeCommand{
			Argv: []string{"python3", "-c", heldReplyReader, heldReplyPool, heldReplyObject, heldReplyWorkdir}, Timeout: 8 * time.Minute,
			MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "held reply consumer", Input: reader.input, MaxInput: 64,
		})
	}()
	t.Cleanup(func() {
		stopReader()
		<-reader.done
		var unstopped *ceph.NativeCommandUnstoppedError
		if errors.As(reader.err, &unstopped) {
			restore("held reply consumer", unstopped.Stop)
		}
	})
	ready := heldReplyMark(t, ctx, consumer, "ready", reader)
	receipt.Client, _ = ready["client"].(string)
	if size, _ := ready["size"].(float64); receipt.Client == "" || int(size) != len(payload) {
		t.Fatalf("consumer ready mark %v", ready)
	}

	// The OSD's network namespace is read through a container that joins it,
	// so its socket table stays readable while the OSD is frozen.
	osdNet, err := testcontainers.Run(ctx, harness.NativeClientImage(t, image),
		testcontainers.WithEntrypoint("sleep", "infinity"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.NetworkMode = container.NetworkMode("container:" + osd.GetContainerID())
		}),
	)
	if osdNet != nil {
		testcontainers.CleanupContainer(t, osdNet)
	}
	if err != nil {
		t.Fatal(err)
	}

	// The consumer's container runs nothing else, so the one connection both
	// tables share is its session with the OSD.
	osdRows := socketTable(t, ctx, osdNet)
	pairs := pairSockets(osdRows, socketTable(t, ctx, consumer))
	if len(pairs) != 1 {
		t.Fatalf("want one consumer session with %s, got %+v", receipt.OSD, pairs)
	}
	receipt.Connection = pairs[0]
	idle, _ := receipt.Connection.find(osdRows)
	receipt.Idle = heldReplySample{Established: true, TX: idle.TX, RX: idle.RX}
	if idle.TX != 0 {
		t.Fatalf("the idle session already queues %d bytes", idle.TX)
	}

	osdPause, err := fault.Pause(ctx, cluster, osd)
	t.Cleanup(func() {
		if osdPause != nil {
			restore(receipt.OSD+" pause", osdPause.Resume)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.input.Write(ctx, []byte("read\n")); err != nil {
		t.Fatal(err)
	}
	heldReplyMark(t, ctx, consumer, "submitted", reader)
	// The client logs its own entity as unknown.0 before it learns its global
	// ID, so the request is the read of the whole object, length size+1.
	request := fmt.Sprintf(":::%s:head [read 0~%d]", heldReplyObject, len(payload)+1)
	for deadline := time.Now().Add(time.Minute); ; {
		_, log, _ := harness.SessionExec(ctx, consumer, "cat", heldReplyWorkdir+"/client.log")
		sent, ops := false, []string{}
		for _, line := range strings.Split(log, "\n") {
			if strings.Contains(line, "osd_op(") {
				ops = append(ops, line)
			}
			if strings.Contains(line, " --> ") && strings.Contains(line, "osd_op(") && strings.Contains(line, request) {
				sent = true
			}
		}
		row, ok := receipt.Connection.findClient(socketTable(t, ctx, consumer))
		end, osdOK := receipt.Connection.find(socketTable(t, ctx, osdNet))
		if !ok || !osdOK {
			t.Fatalf("the consumer's session with %s closed while the OSD was frozen", receipt.OSD)
		}
		if requestDelivered(sent, row, end, receipt.Idle) {
			receipt.Delivered = heldReplySample{Established: true, TX: end.TX, RX: end.RX}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the read did not reach the frozen OSD: logged %t, client send queue %d, OSD receive queue %d, operations %q", sent, row.TX, end.RX, ops[max(0, len(ops)-4):])
		}
		time.Sleep(100 * time.Millisecond)
	}

	consumerPause, err := fault.Pause(ctx, cluster, consumer)
	t.Cleanup(func() {
		if consumerPause != nil {
			restore("consumer pause", consumerPause.Resume)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := osdPause.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	osdPause = nil

	// The OSD's send queue on that connection must fill and stay full while
	// the consumer reads nothing.
	for deadline := time.Now().Add(time.Minute); len(receipt.Held) < heldReplySamples; {
		row, ok := receipt.Connection.find(socketTable(t, ctx, osdNet))
		sample := heldReplySample{Established: ok, TX: row.TX, RX: row.RX}
		switch {
		case !ok:
			t.Fatalf("condition not established: the held connection closed: %+v", receipt.Held)
		case sample.TX >= heldReplyQueued:
			receipt.Held = append(receipt.Held, sample)
		case len(receipt.Held) > 0:
			t.Fatalf("condition not established: the send queue drained to %d bytes after %+v", sample.TX, receipt.Held)
		case time.Now().After(deadline):
			t.Fatalf("condition not established: the OSD queued %d bytes for the frozen consumer", sample.TX)
		}
		time.Sleep(time.Second)
	}
	if _, err := cluster.Ceph(ctx, "status", "--format", "json"); err != nil {
		t.Fatalf("fixture control while the reply is held: %v", err)
	}
	receipt.Control = append(receipt.Control, "ceph status: answered")
	if version := targetedJSON[map[string]string](t, ctx, cluster, "tell", receipt.OSD, "version")["version"]; version != receipt.Release {
		t.Fatalf("%s answered version %q while the reply is held", receipt.OSD, version)
	}
	receipt.Control = append(receipt.Control, "tell "+receipt.OSD+" version: answered")
	if row, ok := receipt.Connection.find(socketTable(t, ctx, osdNet)); !ok || row.TX < heldReplyQueued || reader.finished() {
		t.Fatalf("the hold ended during the control commands: %+v %t, reader done %t", row, ok, reader.finished())
	}

	if abandon {
		// A killed process never runs again, so the reply stays undelivered
		// and its kernel resets the connection.
		if err := consumer.Terminate(ctx, testcontainers.StopTimeout(0)); err != nil {
			t.Fatal(err)
		}
		restore("consumer pause", consumerPause.Resume)
		consumerPause = nil
		started := time.Now()
		for deadline := started.Add(2 * time.Minute); ; {
			if _, ok := receipt.Connection.find(socketTable(t, ctx, osdNet)); !ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s kept the abandoned connection", receipt.OSD)
			}
			time.Sleep(500 * time.Millisecond)
		}
		receipt.Closed = time.Since(started).Round(100 * time.Millisecond).String()
		<-reader.done
		if reader.result.ExitCode == 0 && reader.err == nil {
			t.Fatalf("the killed consumer reported a result: %.512s", reader.result.Stdout)
		}
	} else {
		if err := consumerPause.Resume(ctx); err != nil {
			t.Fatal(err)
		}
		consumerPause = nil
		<-reader.done
		if reader.err != nil || reader.result.ExitCode != 0 || !reader.result.Complete() {
			t.Fatalf("drained read: exit %d: %v: %.2048s", reader.result.ExitCode, reader.err, reader.result.Stderr)
		}
		var drained struct {
			RC int `json:"rc"`
			heldReadback
		}
		if err := json.Unmarshal([]byte(harness.LastJSONLine(string(reader.result.Stdout))), &drained); err != nil {
			t.Fatalf("drained read output: %v: %.512s", err, reader.result.Stdout)
		}
		if drained.RC != len(payload) || drained.Size != len(payload) || drained.SHA256 != want || drained.Version != receipt.Seeded.Version {
			t.Fatalf("drained read rc %d, %d bytes %s version %d; want %d bytes %s version %d", drained.RC, drained.Size, drained.SHA256, drained.Version, len(payload), want, receipt.Seeded.Version)
		}
		receipt.Drained = &drained.heldReadback
	}

	fresh, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: []string{"python3", "-c", heldReplyVerifier, heldReplyPool, heldReplyObject}, Timeout: 3 * time.Minute,
		MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "held reply verifier",
	})
	if err != nil || fresh.ExitCode != 0 || !fresh.Complete() {
		t.Fatalf("fresh read: exit %d: %v: %.2048s", fresh.ExitCode, err, fresh.Stderr)
	}
	if err := json.Unmarshal([]byte(harness.LastJSONLine(string(fresh.Stdout))), &receipt.Fresh); err != nil {
		t.Fatalf("fresh read output: %v: %.512s", err, fresh.Stdout)
	}
	if receipt.Fresh.Size != len(payload) || receipt.Fresh.SHA256 != want || receipt.Fresh.Version != receipt.Seeded.Version {
		t.Fatalf("fresh read %+v, want %+v", receipt.Fresh, receipt.Seeded)
	}
}
