//go:build all || integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/testcontainers/testcontainers-go"
)

// backoffEvent is one line of a debug_ms 1 and debug_objecter 10 log that the
// backoff chain reads: an osd_backoff, osd_op or osd_op_reply message that the
// logging side received ("in") or sent ("out"), or an Objecter session reset.
// Gen numbers the connections of one sender in that log: it grows when the
// sender's address changes, when the per-connection sequence number does not
// grow, and after the Objecter reset that session.
type backoffEvent struct {
	Line  int    `json:"line"`
	Kind  string `json:"kind"`
	Dir   string `json:"dir,omitempty"`
	Local string `json:"local,omitempty"`
	Peer  string `json:"peer,omitempty"`
	Addr  string `json:"addr,omitempty"`
	Seq   uint64 `json:"seq,omitempty"`
	Gen   int    `json:"gen"`
	// Reconnect marks the first message of a later connection.
	Reconnect bool `json:"reconnect,omitempty"`
	// osd_backoff fields.
	PGID  string `json:"pgid,omitempty"`
	Op    string `json:"op,omitempty"`
	ID    uint64 `json:"id,omitempty"`
	Begin string `json:"begin,omitempty"`
	End   string `json:"end,omitempty"`
	Epoch uint32 `json:"epoch,omitempty"`
	// osd_op and osd_op_reply fields. Source is the request's entity, which a
	// client prints as "unknown.0" before its messenger stamps it.
	Source string `json:"source,omitempty"`
	Tid    uint64 `json:"tid,omitempty"`
	Object string `json:"object,omitempty"`
	Ops    string `json:"ops,omitempty"`
	Result int    `json:"result,omitempty"`
	Text   string `json:"-"`
}

var (
	backoffInLine  = regexp.MustCompile(`-- (\S+) <== (\S+) (\S+) (\d+) ==== (.*?) ==== \d+\+\d+\+\d+ \(`)
	backoffOutLine = regexp.MustCompile(`-- (\S+) --> (\S+) -- (.*?) -- 0x[0-9a-f]+ con `)
	backoffMessage = regexp.MustCompile(`^osd_backoff\((\S+) (block|unblock|ack-block) id (\d+) \[([^,\s]*),(\S*)\) e(\d+)\)(?: v\d+)?$`)
	backoffOp      = regexp.MustCompile(`^osd_op\((\S+)\.-?\d+:(\d+) (\S+) `)
	backoffOpObj   = regexp.MustCompile(`:::([A-Za-z0-9._-]+):head \[([^\]]*)\]`)
	backoffReply   = regexp.MustCompile(`^osd_op_reply\((\d+) (\S+) \[([^\]]*)\] v\S+ uv\d+(?: [a-z]+)? = (-?\d+)`)
	backoffReset   = regexp.MustCompile(`ms_handle_reset \S+ session \S+ (osd\.\d+)`)
)

// parseBackoffLog picks the events of the backoff chain out of one log.
func parseBackoffLog(lines []string) []backoffEvent {
	type connection struct {
		gen     int
		addr    string
		seq     uint64
		pending bool
	}
	connections := map[string]*connection{}
	var events []backoffEvent
	for index, line := range lines {
		if m := backoffReset.FindStringSubmatch(line); m != nil {
			if c := connections[m[1]]; c != nil {
				c.pending = true
			}
			events = append(events, backoffEvent{Line: index, Kind: "reset", Peer: m[1], Gen: -1, Text: line})
			continue
		}
		var event backoffEvent
		var message string
		if m := backoffInLine.FindStringSubmatch(line); m != nil {
			seq, _ := strconv.ParseUint(m[4], 10, 64)
			event = backoffEvent{Dir: "in", Local: plainAddr(m[1]), Peer: m[2], Addr: plainAddr(m[3]), Seq: seq}
			message = m[5]
		} else if m := backoffOutLine.FindStringSubmatch(line); m != nil {
			event = backoffEvent{Dir: "out", Local: plainAddr(m[1]), Addr: m[2]}
			message = m[3]
		} else {
			continue
		}
		if m := backoffMessage.FindStringSubmatch(message); m != nil {
			event.Kind, event.PGID, event.Op = "backoff", m[1], m[2]
			event.ID, _ = strconv.ParseUint(m[3], 10, 64)
			event.Begin, event.End = m[4], m[5]
			epoch, _ := strconv.ParseUint(m[6], 10, 32)
			event.Epoch = uint32(epoch)
		} else if m := backoffOp.FindStringSubmatch(message); m != nil {
			event.Kind, event.Source, event.PGID = "osd_op", m[1], m[3]
			event.Tid, _ = strconv.ParseUint(m[2], 10, 64)
			if o := backoffOpObj.FindStringSubmatch(message); o != nil {
				event.Object, event.Ops = o[1], o[2]
			}
		} else if m := backoffReply.FindStringSubmatch(message); m != nil {
			event.Kind, event.Object, event.Ops = "osd_op_reply", m[2], m[3]
			event.Tid, _ = strconv.ParseUint(m[1], 10, 64)
			event.Result, _ = strconv.Atoi(m[4])
		} else if event.Dir == "in" {
			// Any other received message still advances its sender's
			// connection, so a reconnect between two chain events is seen.
			event.Kind = "other"
		} else {
			continue
		}
		event.Line, event.Text = index, line
		if event.Dir == "in" {
			c := connections[event.Peer]
			switch {
			case c == nil:
				c = &connection{}
				connections[event.Peer] = c
			case c.pending || c.addr != event.Addr || event.Seq <= c.seq:
				c.gen++
				event.Reconnect = true
			}
			c.addr, c.seq, c.pending = event.Addr, event.Seq, false
			event.Gen = c.gen
		}
		if event.Kind != "other" || event.Reconnect {
			events = append(events, event)
		}
	}
	return events
}

// backoffTarget is what the chain must concern: the retained client, the
// primary of the seeded object's PG, the PG and the object.
type backoffTarget struct {
	Client  string `json:"client"`
	Primary string `json:"primary"`
	PGID    string `json:"pgid"`
	Object  string `json:"object"`
}

// backoffChain is the joined BLOCK, ACK_BLOCK and UNBLOCK of one PG backoff
// and the read it held. Block, Unblock and Reply come from the client's log,
// AckReceived and OpAtPrimary from the primary's. ExtraBlocks and Reconnects
// keep what else happened, so that noise stays visible.
type backoffChain struct {
	ClientAddr  string         `json:"client_addr"`
	Read        backoffEvent   `json:"read"`
	Block       backoffEvent   `json:"block"`
	AckSent     *backoffEvent  `json:"ack_sent,omitempty"`
	OpAtPrimary backoffEvent   `json:"op_at_primary"`
	AckReceived backoffEvent   `json:"ack_received"`
	Unblock     *backoffEvent  `json:"unblock,omitempty"`
	Reply       *backoffEvent  `json:"reply,omitempty"`
	ExtraBlocks []backoffEvent `json:"extra_blocks"`
	Reconnects  []backoffEvent `json:"reconnects"`
}

// sameRange reports whether two backoff messages name one backoff of one PG.
func (e backoffEvent) sameRange(other backoffEvent) bool {
	return e.PGID == other.PGID && e.ID == other.ID && e.Begin == other.Begin && e.End == other.End
}

// bindBackoffBlock joins the first half of the chain: the client sent a read
// of the object, then received a BLOCK of the object's PG from the primary,
// and the primary received an ACK_BLOCK with the same PG, ID, range and epoch
// from the client's address on the connection that also carried the read.
// Backoff IDs are per session, so an ID alone joins nothing.
func bindBackoffBlock(client, primary []backoffEvent, target backoffTarget) (backoffChain, error) {
	chain := backoffChain{ExtraBlocks: []backoffEvent{}, Reconnects: []backoffEvent{}}
	read := slices.IndexFunc(client, func(e backoffEvent) bool {
		return e.Kind == "osd_op" && e.Dir == "out" && e.Object == target.Object && strings.HasPrefix(e.Ops, "read ")
	})
	if read < 0 {
		return chain, fmt.Errorf("the client log has no read of %s", target.Object)
	}
	chain.Read = client[read]
	block := slices.IndexFunc(client, func(e backoffEvent) bool {
		return e.Line > chain.Read.Line && e.Kind == "backoff" && e.Dir == "in" && e.Op == "block" && e.Peer == target.Primary && e.PGID == target.PGID
	})
	if block < 0 {
		return chain, fmt.Errorf("the client received no BLOCK of %s from %s after its read", target.PGID, target.Primary)
	}
	chain.Block, chain.ClientAddr = client[block], client[block].Local
	for _, e := range client {
		switch {
		case e.Kind == "backoff" && e.Dir == "in" && e.Op == "block" && e.Line != chain.Block.Line:
			chain.ExtraBlocks = append(chain.ExtraBlocks, e)
		case e.Kind == "backoff" && e.Dir == "out" && e.Op == "ack-block" && e.Line > chain.Block.Line && chain.AckSent == nil && e.sameRange(chain.Block):
			ack := e
			chain.AckSent = &ack
		}
		if e.Kind == "reset" || e.Reconnect {
			chain.Reconnects = append(chain.Reconnects, e)
		}
	}
	fromClient := func(e backoffEvent) bool {
		return e.Dir == "in" && e.Peer == target.Client && e.Addr == chain.ClientAddr
	}
	var acks []backoffEvent
	for _, e := range primary {
		if fromClient(e) && e.Kind == "backoff" && e.Op == "ack-block" && e.sameRange(chain.Block) {
			acks = append(acks, e)
		}
		if e.Peer == target.Client && e.Reconnect {
			chain.Reconnects = append(chain.Reconnects, e)
		}
	}
	if len(acks) == 0 {
		return chain, fmt.Errorf("%s logged no ACK_BLOCK of %s id %d from %s at %s", target.Primary, target.PGID, chain.Block.ID, target.Client, chain.ClientAddr)
	}
	for _, ack := range acks {
		op := slices.IndexFunc(primary, func(e backoffEvent) bool {
			return fromClient(e) && e.Kind == "osd_op" && e.Source == target.Client && e.Tid == chain.Read.Tid && e.Gen == ack.Gen && e.Line < ack.Line
		})
		if op < 0 {
			continue
		}
		if ack.Epoch != chain.Block.Epoch {
			return chain, fmt.Errorf("ACK_BLOCK id %d carries epoch %d, the BLOCK epoch %d", ack.ID, ack.Epoch, chain.Block.Epoch)
		}
		chain.AckReceived, chain.OpAtPrimary = ack, primary[op]
		return chain, nil
	}
	return chain, fmt.Errorf("no ACK_BLOCK id %d at %s came on the connection that carried read tid %d", chain.Block.ID, target.Primary, chain.Read.Tid)
}

// released reports whether the client log already shows an UNBLOCK of the
// chain's backoff or a reply to its read.
func (c backoffChain) released(client []backoffEvent) bool {
	return slices.ContainsFunc(client, func(e backoffEvent) bool {
		return e.Line > c.Block.Line && e.Dir == "in" && (e.Kind == "backoff" && e.Op == "unblock" && e.PGID == c.Block.PGID ||
			e.Kind == "osd_op_reply" && e.Tid == c.Read.Tid)
	})
}

// bindBackoffRelease completes a chain from bindBackoffBlock: the primary
// sent an UNBLOCK with the BLOCK's PG, ID and range on the same connection,
// at the BLOCK's epoch or a later one, and only then answered the read, with
// result 0. A reconnect between BLOCK and UNBLOCK leaves the chain unjoined.
func bindBackoffRelease(chain *backoffChain, client []backoffEvent, target backoffTarget) error {
	var others []uint64
	unblock := slices.IndexFunc(client, func(e backoffEvent) bool {
		if e.Line <= chain.Block.Line || e.Kind != "backoff" || e.Dir != "in" || e.Op != "unblock" || e.Peer != target.Primary {
			return false
		}
		if !e.sameRange(chain.Block) {
			others = append(others, e.ID)
			return false
		}
		return true
	})
	if unblock < 0 {
		return fmt.Errorf("the client received no UNBLOCK of %s id %d (other UNBLOCK ids %v)", chain.Block.PGID, chain.Block.ID, others)
	}
	e := client[unblock]
	if e.Gen != chain.Block.Gen || e.Addr != chain.Block.Addr {
		return fmt.Errorf("the connection to %s changed between BLOCK (connection %d) and UNBLOCK (connection %d)", target.Primary, chain.Block.Gen, e.Gen)
	}
	if e.Epoch < chain.Block.Epoch {
		return fmt.Errorf("UNBLOCK epoch %d precedes BLOCK epoch %d", e.Epoch, chain.Block.Epoch)
	}
	chain.Unblock = &e
	reply := slices.IndexFunc(client, func(e backoffEvent) bool {
		return e.Kind == "osd_op_reply" && e.Dir == "in" && e.Tid == chain.Read.Tid && e.Object == target.Object
	})
	if reply < 0 {
		return fmt.Errorf("the client received no reply to read tid %d", chain.Read.Tid)
	}
	r := client[reply]
	if r.Line < e.Line {
		return fmt.Errorf("read tid %d was answered before the UNBLOCK", chain.Read.Tid)
	}
	if r.Peer != target.Primary || r.Result != 0 {
		return fmt.Errorf("read tid %d answered by %s with result %d", chain.Read.Tid, r.Peer, r.Result)
	}
	chain.Reply = &r
	return nil
}

const (
	backoffPool    = "tc-backoff"
	backoffObject  = "backoff-read"
	backoffWorkdir = "/tmp/tc-backoff"
)

// backoffPayload is the seeded object's content.
func backoffPayload() []byte {
	payload := make([]byte, 0, 256*64)
	for range 64 {
		for value := range 256 {
			payload = append(payload, byte(value))
		}
	}
	return payload
}

// backoffSeeder writes the object once and reads it back.
const backoffSeeder = `import json, sys, rados
payload = bytes(range(256)) * 64
with rados.Rados(conffile="/etc/ceph/ceph.conf") as cluster:
    with cluster.open_ioctx(sys.argv[1]) as io:
        io.write_full(sys.argv[2], payload)
        assert io.read(sys.argv[2], len(payload) + 1, 0) == payload
        print(json.dumps({"version": io.get_last_version()}))`

// backoffReader is the retained client. It stats the object, which opens
// its session with the primary, publishes "ready", and waits for "read" on
// its standard input. It then submits one asynchronous read with no client
// timeout, publishes "submitted" and prints the read's result when it
// completes. Its messenger and Objecter log go to client.log.
const backoffReader = `import hashlib, json, os, sys, threading, time, rados
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
        "err_to_stderr": "false", "debug_ms": "1", "debug_objecter": "10", "rados_osd_op_timeout": "0"}
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf=conf) as cluster:
    with cluster.open_ioctx(pool) as io:
        size, _ = io.stat(obj)
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
        print(json.dumps({"rc": result["rc"], "size": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                          "seconds": round(time.monotonic() - started, 3)}))`

// backoffMark waits for the reader's named file and decodes it.
func backoffMark(t *testing.T, ctx context.Context, ctr testcontainers.Container, name string, reader *backoffRead) map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if code, out, err := sessionExec(ctx, ctr, "cat", backoffWorkdir+"/"+name); err == nil && code == 0 {
			var mark map[string]any
			if err := json.Unmarshal([]byte(out), &mark); err != nil {
				t.Fatalf("reader mark %s: %v %q", name, err, out)
			}
			return mark
		}
		select {
		case <-reader.done:
			t.Fatalf("the reader ended before %s: exit %d: %v: %.2048s", name, reader.result.ExitCode, reader.err, reader.result.Stderr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the reader did not publish %s", name)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// backoffRead is the retained reader while it runs.
type backoffRead struct {
	input  *ceph.NativeInput
	done   chan struct{}
	result ceph.NativeCommandResult
	err    error
}

func (r *backoffRead) finished() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

// backoffClientLog reads the reader's log file whole.
func backoffClientLog(t *testing.T, ctx context.Context, ctr testcontainers.Container) []string {
	t.Helper()
	code, out, err := sessionExec(ctx, ctr, "cat", backoffWorkdir+"/client.log")
	if err != nil || code != 0 {
		t.Fatalf("client log: exit %d: %v", code, err)
	}
	return strings.Split(out, "\n")
}

// backoffDaemonLog reads a daemon container's log, failing when it is longer
// than the bound, so that a truncated log never counts as a missing event.
func backoffDaemonLog(t *testing.T, ctx context.Context, ctr testcontainers.Container) []string {
	t.Helper()
	const limit = 256 << 20
	logs, err := ctr.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(io.LimitReader(logs, limit+1))
	_ = logs.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > limit {
		t.Fatalf("daemon log longer than %d bytes", limit)
	}
	return strings.Split(string(data), "\n")
}

// waitBackoffPG polls the pool's only PG until ok accepts it.
func waitBackoffPG(t *testing.T, parent context.Context, cluster *ceph.Container, what string, ok func(ceph.PGState) bool) (ceph.PGState, uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	var last string
	for {
		snapshot, err := poolHandle(t, ctx, cluster, backoffPool).PGs(ctx)
		if err == nil && len(snapshot.PGs) == 1 {
			if ok(snapshot.PGs[0]) {
				return snapshot.PGs[0], snapshot.OSDMapEpochAfter
			}
			last = fmt.Sprintf("%s up %v acting %v primary %d", snapshot.PGs[0].State, snapshot.PGs[0].Up, snapshot.PGs[0].Acting, snapshot.PGs[0].ActingPrimary)
		} else {
			last = fmt.Sprintf("%v, %d PGs", err, len(snapshot.PGs))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("PG never %s: %s", what, last)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// peeredInactive reports a PG that peered without reaching active.
func peeredInactive(state string) bool {
	parts := strings.Split(state, "+")
	return slices.Contains(parts, "peered") && !slices.Contains(parts, "active")
}

// backoffPlacement is one observation of the PG.
type backoffPlacement struct {
	State   string `json:"state"`
	Up      []int  `json:"up"`
	Acting  []int  `json:"acting"`
	Primary int    `json:"primary"`
	Epoch   uint32 `json:"osdmap_epoch"`
}

func placementOf(pg ceph.PGState, epoch uint32) backoffPlacement {
	return backoffPlacement{State: pg.State, Up: pg.Up, Acting: pg.Acting, Primary: pg.ActingPrimary, Epoch: epoch}
}

// osdBackoffReceipt is the evidence of one PG-wide backoff that a native
// client observed. It is evidence for this release and image, not a
// Messenger decoder.
type osdBackoffReceipt struct {
	Release  string `json:"release"`
	Platform string `json:"platform"`
	Run      string `json:"run,omitempty"`
	Images   struct {
		OSD    string `json:"osd"`
		Client string `json:"client"`
	} `json:"images"`
	Target      backoffTarget    `json:"target"`
	Replica     string           `json:"replica"`
	Seeded      heldReadback     `json:"seeded"`
	Clean       backoffPlacement `json:"clean"`
	Held        backoffPlacement `json:"held"`
	StillHeld   backoffPlacement `json:"still_held"`
	After       backoffPlacement `json:"after"`
	Chain       backoffChain     `json:"chain"`
	ReadSeconds float64          `json:"read_seconds"`
	Read        heldReadback     `json:"read"`
	Cleanup     []string         `json:"cleanup"`
}

// testOSDBackoffReceipt makes the primary of a PG send a PG-wide backoff to
// a native client and records it. The pool has three replicas and min_size 3,
// so with noout set and one non-primary replica stopped the PG peers but
// cannot go active, and the primary answers an operation from a client whose
// connection has RADOS_BACKOFF with a BLOCK of the whole PG. One retained
// python-rados client, connected before the stop, submits one asynchronous
// read. Its log must show the BLOCK, and the primary's log the client's
// ACK_BLOCK, before the replica starts again; then the client must receive
// the UNBLOCK and the original read must complete with the seeded bytes.
// The test never lowers min_size, never takes a timed-out command as the
// backoff and writes the object only once, before the experiment.
func testOSDBackoffReceipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithOSDCount(3))
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: backoffPool, PGNum: 1, Replicas: 3, MinSize: 3, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	receipt := osdBackoffReceipt{Platform: runtime.GOOS + "/" + runtime.GOARCH}
	if run := os.Getenv("GITHUB_RUN_ID"); run != "" {
		receipt.Run = run + "/" + os.Getenv("GITHUB_RUN_ATTEMPT")
	}
	seed, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: []string{"python3", "-c", backoffSeeder, backoffPool, backoffObject}, Timeout: 2 * time.Minute,
		MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "backoff seed",
	})
	if err != nil || seed.ExitCode != 0 || !seed.Complete() {
		t.Fatalf("seed: exit %d: %v: %.2048s", seed.ExitCode, err, seed.Stderr)
	}
	payload := backoffPayload()
	sum := sha256.Sum256(payload)
	receipt.Seeded = heldReadback{Size: len(payload), SHA256: hex.EncodeToString(sum[:])}
	if err := json.Unmarshal([]byte(lastJSONLine(string(seed.Stdout))), &receipt.Seeded); err != nil || receipt.Seeded.Version == 0 {
		t.Fatalf("seed output: %v: %.512s", err, seed.Stdout)
	}
	clean, epoch := waitBackoffPG(t, ctx, cluster, "clean on three OSDs", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && len(pg.Acting) == 3 && slices.Equal(pg.Up, pg.Acting)
	})
	receipt.Clean = placementOf(clean, epoch)
	out, err := cluster.Ceph(ctx, "osd", "map", backoffPool, backoffObject, "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var mapping heldMapping
	if err := json.Unmarshal(out, &mapping); err != nil || mapping.PGID != clean.PGID.String() || mapping.ActingPrimary != clean.ActingPrimary {
		t.Fatalf("osd map of %s: %v: %.1024s, PG %s primary %d", backoffObject, err, out, clean.PGID, clean.ActingPrimary)
	}
	var primary, replica *ceph.OSDContainer
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		switch {
		case osd.ID == clean.ActingPrimary:
			primary = osd
		case replica == nil && slices.Contains(clean.Acting, osd.ID):
			replica = osd
		}
	}
	if primary == nil || replica == nil {
		t.Fatalf("no primary and replica OSD containers for %+v", clean)
	}
	receipt.Target = backoffTarget{Primary: fmt.Sprintf("osd.%d", primary.ID), PGID: clean.PGID.String(), Object: backoffObject}
	receipt.Replica = fmt.Sprintf("osd.%d", replica.ID)
	receipt.Images.OSD, receipt.Images.Client = imageID(t, ctx, primary), imageID(t, ctx, client)
	version := targetedJSON[map[string]string](t, ctx, cluster, "tell", receipt.Target.Primary, "version")
	receipt.Release = version["version"]

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
	debug, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{Section: receipt.Target.Primary, Name: "debug_ms", Value: "1"})
	if debug != nil {
		t.Cleanup(func() { restore("debug_ms of "+receipt.Target.Primary, debug.Restore) })
	}
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(time.Minute); ; {
		value := targetedJSON[map[string]string](t, ctx, cluster, "tell", receipt.Target.Primary, "config", "get", "debug_ms")["debug_ms"]
		if strings.HasPrefix(value, "1/") || value == "1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s runs with debug_ms %q", receipt.Target.Primary, value)
		}
		time.Sleep(time.Second)
	}

	reader := &backoffRead{input: ceph.NewNativeInput(), done: make(chan struct{})}
	readCtx, stopReader := context.WithCancel(ctx)
	go func() {
		defer close(reader.done)
		reader.result, reader.err = cluster.RunNativeCommand(readCtx, client, ceph.NativeCommand{
			Argv: []string{"python3", "-c", backoffReader, backoffPool, backoffObject, backoffWorkdir}, Timeout: 12 * time.Minute,
			MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "retained backoff reader", Input: reader.input, MaxInput: 64,
		})
	}()
	t.Cleanup(func() {
		stopReader()
		<-reader.done
		var unstopped *ceph.NativeCommandUnstoppedError
		if errors.As(reader.err, &unstopped) {
			restore("retained reader", unstopped.Stop)
		}
	})
	ready := backoffMark(t, ctx, client, "ready", reader)
	receipt.Target.Client, _ = ready["client"].(string)
	if size, _ := ready["size"].(float64); receipt.Target.Client == "" || int(size) != len(payload) {
		t.Fatalf("reader ready mark %v", ready)
	}

	noout, err := cluster.OverrideOSDFlag(ctx, ceph.OSDFlagNoOut, true)
	if noout != nil {
		t.Cleanup(func() { restore("noout", noout.Restore) })
	}
	if err != nil {
		t.Fatal(err)
	}
	stopTimeout := 10 * time.Second
	if err := replica.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	stopped := true
	t.Cleanup(func() {
		if stopped {
			restore(receipt.Replica+" start", replica.Start)
		}
	})
	held, epoch := waitBackoffPG(t, ctx, cluster, "peered but inactive without "+receipt.Replica, func(pg ceph.PGState) bool {
		return peeredInactive(pg.State) && !slices.Contains(pg.Acting, replica.ID) && pg.ActingPrimary == primary.ID
	})
	receipt.Held = placementOf(held, epoch)

	if err := reader.input.Write(ctx, []byte("read\n")); err != nil {
		t.Fatal(err)
	}
	backoffMark(t, ctx, client, "submitted", reader)
	var chain backoffChain
	for deadline := time.Now().Add(2 * time.Minute); ; {
		clientEvents := parseBackoffLog(backoffClientLog(t, ctx, client))
		chain, err = bindBackoffBlock(clientEvents, parseBackoffLog(backoffDaemonLog(t, ctx, primary)), receipt.Target)
		if chain.released(clientEvents) || reader.finished() {
			t.Fatalf("the read was released while %s was stopped: %+v", receipt.Replica, chain)
		}
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no BLOCK and ACK_BLOCK chain while the PG was held: %v", err)
		}
		time.Sleep(time.Second)
	}
	still, epoch := waitBackoffPG(t, ctx, cluster, "still held", func(pg ceph.PGState) bool { return true })
	if !peeredInactive(still.State) || still.ActingPrimary != primary.ID || reader.finished() {
		t.Fatalf("the hold ended before the replica returned: %s primary %d, reader done %t", still.State, still.ActingPrimary, reader.finished())
	}
	receipt.StillHeld = placementOf(still, epoch)
	t.Logf("OSD_BACKOFF_RAW block: %s", chain.Block.Text)
	t.Logf("OSD_BACKOFF_RAW ack at %s: %s", receipt.Target.Primary, chain.AckReceived.Text)

	if err := replica.Start(ctx); err != nil {
		t.Fatal(err)
	}
	stopped = false
	select {
	case <-reader.done:
	case <-time.After(3 * time.Minute):
		t.Fatal("the held read did not complete after the replica returned")
	}
	if reader.err != nil || reader.result.ExitCode != 0 || !reader.result.Complete() {
		t.Fatalf("reader: exit %d, complete %t: %v: %.2048s", reader.result.ExitCode, reader.result.Complete(), reader.err, reader.result.Stderr)
	}
	var read struct {
		RC      int     `json:"rc"`
		Size    int     `json:"size"`
		SHA256  string  `json:"sha256"`
		Seconds float64 `json:"seconds"`
	}
	if err := json.Unmarshal([]byte(lastJSONLine(string(reader.result.Stdout))), &read); err != nil {
		t.Fatalf("reader output: %v: %.512s", err, reader.result.Stdout)
	}
	receipt.Read = heldReadback{Client: receipt.Target.Client, Size: read.Size, SHA256: read.SHA256}
	receipt.ReadSeconds = read.Seconds
	if read.RC != len(payload) || read.Size != len(payload) || read.SHA256 != receipt.Seeded.SHA256 {
		t.Fatalf("held read returned %+v, want %d bytes with SHA-256 %s", read, len(payload), receipt.Seeded.SHA256)
	}

	clientEvents := parseBackoffLog(backoffClientLog(t, ctx, client))
	final, err := bindBackoffBlock(clientEvents, parseBackoffLog(backoffDaemonLog(t, ctx, primary)), receipt.Target)
	if err != nil || final.Block.Line != chain.Block.Line || final.AckReceived.Line != chain.AckReceived.Line {
		t.Fatalf("the chain moved after the replica returned: %v: %+v, was %+v", err, final, chain)
	}
	if err := bindBackoffRelease(&final, clientEvents, receipt.Target); err != nil {
		t.Fatal(err)
	}
	receipt.Chain = final
	t.Logf("OSD_BACKOFF_RAW unblock: %s", final.Unblock.Text)
	t.Logf("OSD_BACKOFF_RAW reply: %s", final.Reply.Text)

	after, epoch := waitBackoffPG(t, ctx, cluster, "clean again on three OSDs", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && len(pg.Acting) == 3 && slices.Contains(pg.Acting, replica.ID)
	})
	receipt.After = placementOf(after, epoch)
	if after.ActingPrimary != primary.ID {
		t.Fatalf("the primary moved from osd.%d to osd.%d", primary.ID, after.ActingPrimary)
	}
	restore("noout", noout.Restore)
	restore("debug_ms of "+receipt.Target.Primary, debug.Restore)
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("OSD_BACKOFF_RECEIPT %s", data)
}
