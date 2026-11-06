//go:build (all || integration) && ((osd_encrypted_store && !ci) || (ci_optional && (!ci_batch || ci_batch_osd_encrypted_store)))

//ci: timeout=30m job-timeout=40 image-roles=essential,client

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// encPayloadBytes is the OSD's device: the crypt payload behind the LUKS2
	// header, which takes the first 16 MiB of the backing loop.
	encPayloadBytes = int64(512 << 20)
	encHeaderBytes  = int64(16 << 20)
	encKeeperDir    = "/tc-enc"
	encPool         = "encrypted-store"
	encObjectBytes  = 256 << 10
	encObjects      = 8
	encKeyBytes     = 64
)

// encEnv is one cluster whose only OSD runs on the keeper's linear node over
// a LUKS2 crypt mapping. The key lives only in this process and reaches the
// keeper on standard input. rec is what the test holds of the chain; pending
// are keeper execs whose exit the test did not see.
type encEnv struct {
	docker            *mobycl.Client
	keeper            testcontainers.Container
	node, crypt, file string
	loop              string
	rec               encChainRecord
	pending           []string
	cluster           *ceph.Container
	osd               *ceph.OSDContainer
	client            testcontainers.Container
	device            faulthook.BlockDevice
}

// exec runs one keeper action and remembers an exec whose exit it did not see.
func (e *encEnv) exec(ctx context.Context, input []byte, action string, args ...string) ([]byte, []byte, int, error) {
	out, errOut, code, err := encExec(ctx, e.docker, e.keeper.GetContainerID(), input, append([]string{"python3", "-I", "-c", encKeeperScript, action}, args...)...)
	var unsettled *encUnsettled
	if errors.As(err, &unsettled) {
		e.pending = append(e.pending, unsettled.ExecID)
	}
	return out, errOut, code, err
}

// try runs one keeper action with input as its standard input and returns
// its receipt, or why there is none.
func (e *encEnv) try(ctx context.Context, input []byte, action string, args ...string) (encReply, error) {
	out, errOut, code, err := e.exec(ctx, input, action, args...)
	if err != nil || code != 0 {
		return encReply{}, fmt.Errorf("keeper %s: exit %d %w: %s %s", action, code, err, out, errOut)
	}
	r, err := parseEncReply(out, action)
	if err != nil {
		return r, fmt.Errorf("%w: %s", err, out)
	}
	return r, nil
}

// do is try that fails the test unless the keeper printed a receipt.
func (e *encEnv) do(t *testing.T, ctx context.Context, input []byte, action string, args ...string) encReply {
	t.Helper()
	r, err := e.try(ctx, input, action, args...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// encExec runs argv in a container with input written to its standard input,
// which is then closed, and returns its output once the process has exited.
// Once the exec is started, any failure to see its exit is an *encUnsettled:
// the command may still be running.
func encExec(ctx context.Context, docker *mobycl.Client, id string, input []byte, argv ...string) ([]byte, []byte, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	created, err := docker.ExecCreate(ctx, id, mobycl.ExecCreateOptions{AttachStdin: true, AttachStdout: true, AttachStderr: true, Cmd: argv})
	if err != nil {
		return nil, nil, -1, err
	}
	attached, err := docker.ExecAttach(ctx, created.ID, mobycl.ExecAttachOptions{})
	if err != nil {
		return nil, nil, -1, &encUnsettled{ExecID: created.ID, Err: err}
	}
	defer attached.Close()
	stdout, stderr, err := encCopy(ctx, encHijacked{&attached.HijackedResponse}, input)
	if err != nil {
		return stdout, stderr, -1, &encUnsettled{ExecID: created.ID, Err: err}
	}
	for {
		inspected, err := docker.ExecInspect(ctx, created.ID, mobycl.ExecInspectOptions{})
		if err != nil {
			return stdout, stderr, -1, &encUnsettled{ExecID: created.ID, Err: err}
		}
		if !inspected.Running {
			return stdout, stderr, inspected.ExitCode, nil
		}
		select {
		case <-ctx.Done():
			return stdout, stderr, -1, &encUnsettled{ExecID: created.ID, Err: ctx.Err()}
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// encHijacked is the attached exec connection as an encStream.
type encHijacked struct{ h *mobycl.HijackedResponse }

func (s encHijacked) Read(p []byte) (int, error)  { return s.h.Reader.Read(p) }
func (s encHijacked) Write(p []byte) (int, error) { return s.h.Conn.Write(p) }
func (s encHijacked) CloseWrite() error           { return s.h.CloseWrite() }
func (s encHijacked) Close() error                { return s.h.Conn.Close() }

// settle waits, within ctx and at most a minute, for every unsettled keeper
// exec to exit, and reports whether all of them did.
func (e *encEnv) settle(ctx context.Context) (bool, string) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	states := make([]encExecState, 0, len(e.pending))
	for _, id := range e.pending {
		s := encExecState{ID: id, Running: true}
		for s.Running && s.Err == "" {
			inspected, err := e.docker.ExecInspect(ctx, id, mobycl.ExecInspectOptions{})
			switch {
			case err != nil:
				s.Err = err.Error()
			case !inspected.Running:
				s.Running = false
			default:
				select {
				case <-ctx.Done():
					s.Err = "still running at the deadline"
				case <-time.After(time.Second):
				}
			}
		}
		states = append(states, s)
	}
	ok, detail := judgeEncSettled(states)
	if ok {
		e.pending = nil
	}
	return ok, detail
}

// encOSDMapEntry is the OSD's row of the OSDMap.
type encOSDMapEntry struct {
	OSD    int    `json:"osd"`
	UUID   string `json:"uuid"`
	Up     int    `json:"up"`
	In     int    `json:"in"`
	UpFrom int    `json:"up_from"`
}

func (e *encEnv) osdMap(t *testing.T, ctx context.Context) encOSDMapEntry {
	t.Helper()
	data, err := e.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var dump struct {
		OSDs []encOSDMapEntry `json:"osds"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		t.Fatal(err)
	}
	for _, o := range dump.OSDs {
		if o.OSD == e.osd.ID {
			return o
		}
	}
	t.Fatalf("osd.%d is not in the OSDMap", e.osd.ID)
	return encOSDMapEntry{}
}

// identity reads the OSD's identity from the OSDMap and, inside its
// container, from its store and its open descriptors.
func (e *encEnv) identity(t *testing.T, ctx context.Context) (encOSDIdentity, blockAttachView) {
	t.Helper()
	entry := e.osdMap(t, ctx)
	out := harness.ExecOutput(t, ctx, e.osd, "python3", "-I", "-c", blockDeviceObserverScript, "/var/lib/ceph/osd/ceph-"+strconv.Itoa(e.osd.ID))
	var view blockAttachView
	if err := json.Unmarshal([]byte(out), &view); err != nil {
		t.Fatalf("observer: %v: %s", err, out)
	}
	number := strconv.FormatUint(uint64(e.device.Major), 10) + ":" + strconv.FormatUint(uint64(e.device.Minor), 10)
	verdict, _ := judgeBlockAttachment(view, number, entry.UUID)
	return encOSDIdentity{ID: e.osd.ID, UUID: entry.UUID, StoreFSID: view.FSID, Number: number, Attachment: verdict}, view
}

// waitUp waits until the daemon runs and the OSDMap shows it up and in from
// an epoch later than after.
func (e *encEnv) waitUp(t *testing.T, parent context.Context, after int) encOSDMapEntry {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	for {
		state, err := e.osd.State(ctx)
		if err != nil || state == nil || !state.Running {
			t.Fatalf("the OSD exited: %+v %v: %v", state, err, e.logTail(ctx, ""))
		}
		if entry := e.osdMap(t, ctx); entry.Up == 1 && entry.In == 1 && entry.UpFrom > after {
			return entry
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the OSD did not come up after epoch %d: %v", after, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func (e *encEnv) write(t *testing.T, ctx context.Context, prefix, marker string) []encLedgerEntry {
	t.Helper()
	out := harness.ExecOutput(t, ctx, e.client, "python3", "-c", encWrites, encPool, prefix, strconv.Itoa(encObjects), strconv.Itoa(encObjectBytes), marker)
	var ledger []encLedgerEntry
	if err := json.Unmarshal([]byte(lastLine(out)), &ledger); err != nil {
		t.Fatalf("writes: %v: %s", err, out)
	}
	return ledger
}

func (e *encEnv) readback(t *testing.T, ctx context.Context, ledger []encLedgerEntry) map[string]any {
	t.Helper()
	keys := make([]string, 0, len(ledger))
	for _, entry := range ledger {
		keys = append(keys, entry.Key)
	}
	out := harness.ExecOutput(t, ctx, e.client, append([]string{"python3", "-c", encReadback, encPool}, keys...)...)
	read := map[string]string{}
	if err := json.Unmarshal([]byte(lastLine(out)), &read); err != nil {
		t.Fatalf("readback: %v: %s", err, out)
	}
	ok, detail := judgeEncReadback(ledger, read)
	if !ok {
		t.Fatalf("readback: %s", detail)
	}
	return map[string]any{"verdict": "intact", "detail": detail}
}

func lastLine(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	return lines[len(lines)-1]
}

// encLogMarkers are the OSD log words that name why a start failed.
var encLogMarkers = []string{"Input/output error", "(5) ", "unable to", "failed", "error", "OSD bootstrap refused"}

// logTail keeps the OSD log lines since the RFC 3339 instant since (all when
// empty) that name a failure.
func (e *encEnv) logTail(ctx context.Context, since string) []string {
	reader, err := e.docker.ContainerLogs(ctx, e.osd.GetContainerID(), mobycl.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Tail: "400", Since: since})
	if err != nil {
		return []string{"logs unavailable: " + err.Error()}
	}
	defer func() { _ = reader.Close() }()
	var stdout, stderr bytes.Buffer
	_, _ = stdcopy.StdCopy(&stdout, &stderr, io.LimitReader(reader, 4<<20))
	var kept []string
	for _, line := range strings.Split(stdout.String()+"\n"+stderr.String(), "\n") {
		if !slices.ContainsFunc(encLogMarkers, func(w string) bool { return strings.Contains(line, w) }) {
			continue
		}
		if len(line) > 300 {
			line = line[:300]
		}
		if !slices.Contains(kept, line) && len(kept) < 20 {
			kept = append(kept, line)
		}
	}
	return kept
}

// lockedStart starts the OSD container while its node maps an error table:
// the crypt mapping under it is closed. It records whether the daemon exited
// and whether the OSDMap ever showed it up again.
func (e *encEnv) lockedStart(t *testing.T, parent context.Context, before int) encLockedStart {
	t.Helper()
	since := time.Now().Add(-3 * time.Second).UTC().Format(time.RFC3339Nano)
	startCtx, cancel := context.WithTimeout(parent, 90*time.Second)
	startErr := e.osd.Start(startCtx)
	cancel()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	s := encLockedStart{Running: true}
	for {
		state, err := e.osd.State(ctx)
		if err != nil || state == nil {
			t.Fatal("OSD state", err)
		}
		if entry := e.osdMap(t, ctx); entry.Up == 1 && entry.UpFrom > before {
			s.CameUp = true
		}
		if !state.Running {
			s.Running, s.ExitCode = false, state.ExitCode
			break
		}
		select {
		case <-ctx.Done():
			s.Log = e.logTail(parent, since)
			return s
		case <-time.After(2 * time.Second):
		}
	}
	s.Log = e.logTail(parent, since)
	if startErr != nil {
		s.Log = append(s.Log, "start: "+startErr.Error())
	}
	return s
}

// chainEvidence is the keeper's side of the release gate: the OSD container
// is gone or stopped, the observer borrowed no descriptor (it only stats the
// descriptor links), and the kernel's open count of the node is the scan.
func (e *encEnv) chainEvidence(ctx context.Context, osdID string) (faulthook.ChainEvidence, error) {
	ev := faulthook.ChainEvidence{Lease: e.node, Device: e.device, BorrowsClosed: faulthook.FactYes}
	if osdID == "" {
		ev.DaemonStopped = faulthook.FactYes
	} else if inspect, err := e.docker.ContainerInspect(ctx, osdID, mobycl.ContainerInspectOptions{}); err != nil {
		if !cerrdefs.IsNotFound(err) {
			return ev, err
		}
		ev.DaemonStopped = faulthook.FactYes
	} else if inspect.Container.State != nil && !inspect.Container.State.Running {
		ev.DaemonStopped = faulthook.FactYes
	} else {
		ev.DaemonStopped = faulthook.FactNo
	}
	r, err := e.try(ctx, nil, "stack", e.node, e.crypt, e.loop, e.file)
	if err != nil {
		return ev, err
	}
	if r.NodeOpen != nil {
		ev.ScanComplete, ev.Openers = faulthook.FactYes, *r.NodeOpen
	}
	return ev, nil
}

// release removes what the test holds of the chain, and nothing else. It
// waits for unsettled keeper execs first. A chain whose node was given to the
// OSD is released only when the cluster's claim table and the keeper's
// evidence both raise no objection. Each recorded layer is then removed only
// while it still has its recorded identity; a mapping that only has the
// test's name is reported and kept. Whatever is not released stays recorded,
// and the keeper with it.
func (e *encEnv) release(t *testing.T, ctx context.Context, osdID string, attached bool) map[string]any {
	out := map[string]any{"node": e.node}
	if ok, detail := e.settle(ctx); !ok {
		out["unsettled"] = detail
		t.Errorf("the chain %s is retained: %s", e.node, detail)
		return out
	}
	if !e.rec.held() {
		return out
	}
	if attached && e.rec.Node.UUID != "" {
		outcome, detail := faulthook.JudgeDeviceChain(ctx, e.cluster, e.device, e.node, func(ctx context.Context) (faulthook.ChainEvidence, error) {
			return e.chainEvidence(ctx, osdID)
		})
		out["verdict"], out["detail"] = outcome, detail
		if outcome != faulthook.ChainClear {
			t.Errorf("the OSD's device chain %s is retained: %s: %s", e.node, outcome, detail)
			return out
		}
	}
	loop := e.rec.Loop.Loop
	if loop == "" {
		loop = "-"
	}
	seen, err := e.try(ctx, nil, "observe", e.node, encDash(e.rec.Node.UUID), e.crypt, encDash(e.rec.Crypt.UUID), loop)
	if err != nil {
		t.Errorf("the chain %s is retained: %v", e.node, err)
		return out
	}
	own := judgeEncOwnership(e.rec, seen)
	out["ownership"] = own
	for name, l := range map[string]encLayer{"node": own.Node, "crypt": own.Crypt} {
		if l.Verdict == encLayerForeign {
			t.Logf("the %s name is kept for its owner: %s", name, l.Detail)
		}
	}
	removed, err := e.try(ctx, nil, "remove", encRemoveArgs(e.rec, own)...)
	out["removed"] = removed
	if err != nil {
		t.Errorf("release of chain %s: %v", e.node, err)
		return out
	}
	gone := encRemovedNodes(e.rec, own, removed, e.node, e.crypt)
	e.rec = encReleased(e.rec, own, removed)
	if e.rec.held() {
		out["retained"] = e.rec
		t.Errorf("the chain %s is retained: %+v, ownership %+v, removal %s", e.node, e.rec, own, encOutcomes(removed))
	}
	nodes, err := e.unlinkNodes(ctx, gone)
	out["nodes"] = nodes
	if err != nil {
		t.Errorf("the nodes of chain %s: %v", e.node, err)
	}
	return out
}

// unlinkNodes unlinks the /dev/mapper nodes of mappings the test removed,
// which a removal by DM UUID leaves behind, and then requires /dev/mapper to
// hold no stale node.
func (e *encEnv) unlinkNodes(ctx context.Context, removed []encNode) (map[string]any, error) {
	out := map[string]any{"removed_mappings": removed}
	before, err := e.try(ctx, nil, "nodes", "-")
	if err != nil {
		return out, err
	}
	unlink, refused := judgeEncNodeRemoval(removed, before.Nodes)
	out["refused"] = refused
	if len(unlink) > 0 {
		r, err := e.try(ctx, nil, "unnode", encUnnodeArgs(unlink)...)
		out["unnode"] = r.Unnode
		if err != nil {
			return out, err
		}
		for _, u := range r.Unnode {
			if u.Outcome != "removed" && u.Outcome != "gone" {
				refused = append(refused, u.Name+": "+u.Outcome+" "+u.Error)
			}
		}
	}
	after, err := e.try(ctx, nil, "nodes", "-")
	if err != nil {
		return out, err
	}
	out["after"] = after.Nodes
	clean, detail := judgeEncNodesClean([]string{e.node, e.crypt}, after.Nodes)
	out["clean"] = detail
	switch {
	case len(refused) > 0:
		return out, fmt.Errorf("nodes kept: %s; %s", strings.Join(refused, "; "), detail)
	case !clean:
		return out, errors.New(detail)
	}
	return out, nil
}

func encDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func encOutcomes(r encReply) string {
	return encJSON([]*encOutcome{r.ReleaseNode, r.ReleaseCrypt, r.ReleaseLoop, r.ReleaseFile})
}

func encJSON(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}

// startEncKeeper starts the keeper, which runs the client role image (it
// ships cryptsetup) and sees the engine host's /dev, and checks that the
// kernel has the targets the chain needs. DM_DISABLE_UDEV makes
// libdevmapper create and remove the nodes itself instead of waiting for a
// udev it cannot reach. The keeper is terminated at cleanup only when the
// test holds nothing of its chain any more.
func startEncKeeper(t *testing.T, ctx context.Context, keeperImage string) (*encEnv, encReply) {
	t.Helper()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	keeper, err := testcontainers.Run(ctx, keeperImage, testcontainers.WithEntrypoint("sleep", "infinity"),
		testcontainers.WithEnv(map[string]string{"DM_DISABLE_UDEV": "1"}),
		testcontainers.WithHostConfigModifier(func(h *container.HostConfig) {
			h.Privileged = true
			h.Binds = append(h.Binds, "/dev:/dev")
			h.Tmpfs = map[string]string{encKeeperDir: "size=" + strconv.FormatInt(encPayloadBytes+encHeaderBytes+(64<<20), 10) + ",mode=0700", "/run/cryptsetup": "size=1m,mode=0700"}
			h.NetworkMode = "none"
		}))
	name := "tcenc-" + hex.EncodeToString(encRandom(t, 4))
	e := &encEnv{docker: docker.Client, keeper: keeper, node: name, crypt: name + "-crypt", file: encKeeperDir + "/" + name}
	if keeper != nil {
		t.Cleanup(func() {
			if e.rec.held() || len(e.pending) > 0 {
				t.Errorf("block device keeper %s kept: it still holds %+v, unsettled execs %v", keeper.GetContainerID(), e.rec, e.pending)
				return
			}
			if err := keeper.Terminate(context.Background()); err != nil {
				t.Error("block device keeper cleanup", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	admit := e.do(t, ctx, nil, "admit", "-")
	autoload := os.Getenv("RUNNER_ENVIRONMENT") == "github-hosted"
	for _, target := range []string{"linear", "error"} {
		if !slices.Contains(admit.Targets, target) {
			t.Fatalf("encrypted OSD store refused: the device-mapper %s target is not registered", target)
		}
	}
	if !slices.Contains(admit.Targets, "crypt") && !autoload {
		t.Fatal("encrypted OSD store refused: the device-mapper crypt target is not registered on the engine host; load dm_crypt there first, the test does not load modules")
	}
	return e, admit
}

// build makes the chain bottom up: the backing file and loop, the LUKS2
// header, the crypt mapping and the OSD's linear node. Each layer is recorded
// only once the keeper confirms that this step created it, with the identity
// the release checks; a failure leaves exactly those layers recorded.
func (e *encEnv) build(ctx context.Context, key []byte) (encHeader, encReply, error) {
	e.rec.File = e.file
	created, err := e.try(ctx, nil, "create", e.file, strconv.FormatInt(encPayloadBytes+encHeaderBytes, 10))
	if err != nil {
		return encHeader{}, encReply{}, err
	}
	loop, ok := encLoopAcquired(created)
	if !ok {
		return encHeader{}, encReply{}, fmt.Errorf("backing loop: %s", encJSON(created))
	}
	e.rec.Loop, e.loop = loop, loop.Loop
	formatted, err := e.try(ctx, key, "format", e.loop)
	if err != nil {
		return encHeader{}, encReply{}, err
	}
	header := encHeaderOf(formatted)
	if formatted.Error != "" || !header.known() || header.Offset != encHeaderBytes {
		return header, encReply{}, fmt.Errorf("LUKS2 format: %s", encJSON(formatted))
	}
	if err := e.open(ctx, key, header); err != nil {
		return header, encReply{}, fmt.Errorf("crypt mapping: %w", err)
	}
	uuid := "TC-ENC-" + e.node + "-" + hex.EncodeToString(encRandomBytes(4))
	e.rec.Node = encDMRecord{UUID: uuid}
	linear, err := e.try(ctx, nil, "linear", e.node, uuid, e.rec.Crypt.UUID, e.rec.Crypt.Number)
	if err != nil {
		return header, linear, err
	}
	node, why := encNodeAcquired(linear, uuid)
	e.rec.Node = node
	if why != "" || linear.Bytes != encPayloadBytes {
		return header, linear, fmt.Errorf("OSD node: %s: %s", why, encJSON(linear))
	}
	return header, linear, nil
}

// open opens the crypt mapping with key and records it when this open
// created it with the test's header.
func (e *encEnv) open(ctx context.Context, key []byte, header encHeader) error {
	e.rec.Crypt = encDMRecord{UUID: encCryptDMUUID(header.UUID, e.crypt)}
	opened, err := e.try(ctx, key, "open", e.loop, e.crypt)
	if err != nil {
		return err
	}
	crypt, why := encCryptAcquired(opened, header.UUID, e.crypt, e.rec.Loop)
	e.rec.Crypt = crypt
	switch {
	case why != "":
		return fmt.Errorf("%s: %s", why, encJSON(opened))
	case opened.Bytes != encPayloadBytes || encHeaderOf(opened) != header:
		return fmt.Errorf("opened %d bytes, header %+v: %s", opened.Bytes, encHeaderOf(opened), encJSON(opened))
	}
	return nil
}

func encRandom(t *testing.T, n int) []byte {
	t.Helper()
	return encRandomBytes(n)
}

// encRandomBytes is n random bytes; crypto/rand.Read never fails.
func encRandomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// TestEncryptedBlockDeviceOSD runs one BlueStore OSD on a test-owned
// encrypted backing store: a backing file on the keeper's tmpfs, a loop
// device, a LUKS2 dm-crypt mapping (aes-xts-plain64) with a random key that
// only the test holds, and a linear device-mapper node over the mapping,
// which the OSD gets through the block-device path. The OSD has no separate
// DB or WAL device, so BlueFS and RocksDB share the encrypted main device;
// the OSD data directory in its container (keyring, fsid, block link) is not
// on it.
//
// The test shows the stack and that the daemon holds the node read-write,
// writes known objects through an independent client and finds their bytes
// in the decrypted view but not in the backing file. It then locks the store:
// the OSD stops, the node gets an error table, and the crypt mapping closes.
// A start of the OSD in that state fails without a new store. Opening the
// mapping with no key and with a wrong key is refused, creates no mapping
// and leaves the LUKS header unchanged. Opened with the right key and mapped
// under the same node again, the original OSD starts with its original ID,
// UUID and store, the objects read back and new writes are acknowledged.
// Cleanup releases the node, the mapping, the loop and the file only once
// neither the cluster nor the keeper sees a holder.
//
// The kernel needs the device-mapper crypt target. The test reads the
// registered targets before any crypt step and never loads a module, except
// that on a GitHub-hosted runner, which is discarded after the job, the
// kernel may load dm_crypt on demand.
func TestEncryptedBlockDeviceOSD(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	receipt := map[string]any{"payload_bytes": encPayloadBytes}
	defer func() {
		data, _ := json.Marshal(receipt)
		t.Logf("ENCRYPTED_STORE_RECEIPT %s", data)
	}()
	e, admit := startEncKeeper(t, ctx, harness.NativeClientImage(t, image))
	receipt["admission"] = map[string]any{"kernel": admit.Kernel, "targets": admit.Targets, "cryptsetup": admit.Cryptsetup,
		"autoload_allowed": os.Getenv("RUNNER_ENVIRONMENT") == "github-hosted"}

	key, wrong := encRandom(t, encKeyBytes), encRandom(t, encKeyBytes)
	var cluster *ceph.Container
	var osdID string
	attached := false
	// Registered before the cluster's Terminate, so it runs after it.
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
		defer done()
		data, _ := json.Marshal(e.release(t, cleanup, osdID, attached))
		t.Logf("ENCRYPTED_STORE_RELEASE_RECEIPT %s", data)
	})

	// The chain, bottom up. Each layer is recorded once it exists, so a
	// failure leaves only the parts that cleanup then removes.
	header, linear, err := e.build(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	major, minor, ok := strings.Cut(linear.Number, ":")
	maj, err1 := strconv.ParseUint(major, 10, 32)
	mnr, err2 := strconv.ParseUint(minor, 10, 32)
	if !ok || err1 != nil || err2 != nil {
		t.Fatalf("OSD node number %q", linear.Number)
	}
	e.device = faulthook.BlockDevice{HostPath: linear.Path, Major: uint32(maj), Minor: uint32(mnr), SizeBytes: linear.Bytes}
	stack := e.do(t, ctx, nil, "stack", e.node, e.crypt, e.loop, e.file)
	verdict, detail := judgeEncStack(stack, linear.Number)
	receipt["chain"] = map[string]any{"loop": e.rec.Loop.Number, "header": header, "crypt": e.rec.Crypt, "node": e.rec.Node, "stack": verdict, "detail": detail,
		"crypt_table": stack.CryptTable}
	if verdict != encStacked {
		t.Fatalf("stack: %s", detail)
	}

	opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs().WithPoolDefaults(1, 1)),
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithStartupBenchmark(ceph.OSDBenchmarkSkip)))
	cluster, err = ceph.Run(ctx, image, opts...)
	if cluster != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error("encrypted store cluster cleanup", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	e.cluster = cluster
	attached = true
	osd, err := faulthook.AddBlockDeviceOSD(ctx, cluster, e.device, e.node)
	if osd != nil {
		osdID = osd.GetContainerID()
	}
	if err != nil {
		t.Fatal("encrypted OSD:", err)
	}
	e.osd = osd
	e.client = harness.ToolkitContainer(t, cluster)
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: encPool, Application: "rados", Replicas: 1, MinSize: 1, PGNum: 8}); err != nil {
		t.Fatal(err)
	}

	// The daemon holds the node read-write and the store is its own; its
	// BlueFS has no dedicated DB or WAL device.
	original, view := e.identity(t, ctx)
	receipt["original"], receipt["attachment"] = original, view
	if original.Attachment != blockAttached || original.StoreFSID != original.UUID {
		t.Fatalf("original identity: %+v", original)
	}
	metaRaw, err := cluster.Ceph(ctx, "osd", "metadata", strconv.Itoa(osd.ID), "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var meta map[string]any
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		t.Fatal(err)
	}
	layout := map[string]any{}
	for _, field := range []string{"osd_objectstore", "bluefs_dedicated_db", "bluefs_dedicated_wal", "bluefs_single_shared_device", "bluestore_bdev_type", "bluestore_bdev_size"} {
		layout[field] = meta[field]
	}
	receipt["layout"] = layout
	if meta["osd_objectstore"] != "bluestore" || meta["bluefs_dedicated_db"] != "0" || meta["bluefs_dedicated_wal"] != "0" {
		t.Fatalf("the OSD's store is not one BlueStore main device: %v", layout)
	}

	// Known bytes through an independent client; the marker is in the
	// decrypted view and not in the backing file under the crypt mapping.
	marker := hex.EncodeToString(encRandom(t, 48))
	ledger := e.write(t, ctx, "before-lock", marker)
	receipt["readback_before"] = e.readback(t, ctx, ledger)
	decrypted := e.do(t, ctx, nil, "scan", "/dev/mapper/"+e.crypt, marker)
	backing := e.do(t, ctx, nil, "scan", e.file, marker)
	cipher := judgeEncCiphertext(decrypted, backing)
	receipt["ciphertext"] = map[string]any{"verdict": cipher, "decrypted": decrypted.Count, "backing": backing.Count}
	if cipher != encCiphertext {
		t.Fatalf("ciphertext: %s (decrypted %v, backing %v)", cipher, decrypted.Count, backing.Count)
	}

	// Lock: stop the OSD, give the node an error table so that nothing holds
	// the crypt mapping, and close the mapping.
	before := e.osdMap(t, ctx)
	stop := 30 * time.Second
	if err := osd.Stop(ctx, &stop); err != nil {
		t.Fatal("stop the OSD:", err)
	}
	retarget := e.do(t, ctx, nil, "retarget", e.rec.Node.UUID, e.rec.Node.Number, "error")
	closed := e.do(t, ctx, nil, "close", e.rec.Crypt.UUID, e.rec.Crypt.Number)
	var closedNodes map[string]any
	if closed.Closed {
		gone := []encNode{{Name: e.crypt, Number: e.rec.Crypt.Number}}
		e.rec.Crypt = encDMRecord{}
		if closedNodes, err = e.unlinkNodes(ctx, gone); err != nil {
			t.Fatalf("the closed crypt mapping's node: %v", err)
		}
	}
	receipt["lock"] = map[string]any{"table": retarget.Table, "close": closed, "nodes": closedNodes}
	if retarget.Error != "" || encLower(retarget.Table) != "error" || closed.Error != "" || !closed.Closed {
		t.Fatalf("lock: table %q %s, close %+v", retarget.Table, retarget.Error, closed)
	}

	// A start with the store locked fails; the launch admits only the store
	// it made, so it does not make a new one.
	locked := e.lockedStart(t, ctx, before.UpFrom)
	lockedVerdict := judgeEncLockedStart(locked)
	receipt["locked_start"] = map[string]any{"verdict": lockedVerdict, "state": locked}
	if lockedVerdict != encStartRefused {
		t.Fatalf("start with the store locked: %s: %+v", lockedVerdict, locked)
	}

	// No key and a wrong key are refused without a mapping or a header change.
	refusals := map[string]any{}
	for _, attempt := range []struct {
		name  string
		input []byte
	}{{"missing", nil}, {"wrong", wrong}} {
		e.rec.Crypt = encDMRecord{UUID: encCryptDMUUID(header.UUID, e.crypt)}
		r := e.do(t, ctx, attempt.input, "open", e.loop, e.crypt)
		// A mapping that this open created is the test's, whatever the verdict.
		e.rec.Crypt, _ = encCryptAcquired(r, header.UUID, e.crypt, e.rec.Loop)
		v, d := judgeEncRefusal(r, header)
		refusals[attempt.name] = map[string]any{"verdict": v, "detail": d}
		if v != encRefused {
			receipt["refusals"] = refusals
			t.Fatalf("open with a %s key: %s: %s", attempt.name, v, d)
		}
	}
	receipt["refusals"] = refusals

	// Unlock with the right key, map the node over the mapping again and
	// start the same OSD container.
	if err := e.open(ctx, key, header); err != nil {
		t.Fatalf("reopen with the right key: %v", err)
	}
	back := e.do(t, ctx, nil, "retarget", e.rec.Node.UUID, e.rec.Node.Number, e.rec.Crypt.UUID, e.rec.Crypt.Number)
	if back.Error != "" {
		t.Fatalf("map the node over the reopened mapping: %+v", back)
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatalf("start the unlocked OSD: %v: %v", err, e.logTail(ctx, ""))
	}
	if osd.GetContainerID() != osdID {
		t.Fatal("the restart replaced the OSD container")
	}
	upAgain := e.waitUp(t, ctx, before.UpFrom)
	stack = e.do(t, ctx, nil, "stack", e.node, e.crypt, e.loop, e.file)
	verdict, detail = judgeEncStack(stack, linear.Number)
	reopenedIdentity, view := e.identity(t, ctx)
	same, sameDetail := judgeEncIdentity(original, reopenedIdentity)
	unlock := map[string]any{"crypt": e.rec.Crypt, "stack": verdict, "detail": detail, "up_from": [2]int{before.UpFrom, upAgain.UpFrom},
		"identity": reopenedIdentity, "same": same, "same_detail": sameDetail, "attachment": view}
	receipt["unlock"] = unlock
	if verdict != encStacked || !same {
		t.Fatalf("unlock: stack %s (%s), identity %s", verdict, detail, sameDetail)
	}
	unlock["readback"] = e.readback(t, ctx, ledger)
	post := e.write(t, ctx, "after-unlock", marker)
	unlock["readback_post"] = e.readback(t, ctx, post)
	unlock["readback_all"] = e.readback(t, ctx, append(slices.Clone(ledger), post...))
}

// TestEncryptedBlockStoreKeepsACollidingMapping shows that the release of
// the chain removes only what the test created. Before the chain is built,
// the test creates a one-sector error mapping, with a DM UUID of its own,
// under the name that the OSD's node or the crypt mapping is to get. Building
// the chain then fails at that layer without recording it, the release
// removes the layers that were built and keeps the colliding mapping with its
// UUID and number, and the test then removes that mapping by its UUID.
func TestEncryptedBlockStoreKeepsACollidingMapping(t *testing.T) {
	image, _ := harness.IntegrationImages(t)
	keeperImage := harness.NativeClientImage(t, image)
	for _, layer := range []string{"node", "crypt"} {
		t.Run(layer, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
			defer cancel()
			e, _ := startEncKeeper(t, ctx, keeperImage)
			name := e.node
			if layer == "crypt" {
				name = e.crypt
			}
			uuid := "TC-ENC-COLLIDE-" + hex.EncodeToString(encRandom(t, 4))
			theirs := e.do(t, ctx, nil, "foreign", name, uuid)
			if theirs.RC == nil || *theirs.RC != 0 || theirs.UUID != uuid || theirs.Number == "" {
				t.Fatalf("colliding mapping: %+v", theirs)
			}
			// Cleanups run in reverse: the chain's release, then the
			// colliding mapping, then the keeper.
			t.Cleanup(func() {
				cleanup, done := context.WithTimeout(context.Background(), time.Minute)
				defer done()
				r, err := e.try(cleanup, nil, "remove", uuid, theirs.Number, "-", "-", "-", "-", "-", "-", "-")
				if err != nil || r.ReleaseNode == nil || r.ReleaseNode.Outcome != "removed" {
					t.Errorf("remove the colliding mapping %s: %v %+v", uuid, err, r.ReleaseNode)
					return
				}
				nodes, err := e.unlinkNodes(cleanup, []encNode{{Name: name, Number: theirs.Number}})
				t.Logf("ENCRYPTED_STORE_COLLISION_NODES %s", encJSON(nodes))
				if err != nil {
					t.Errorf("the colliding mapping's node: %v", err)
				}
			})
			t.Cleanup(func() {
				cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
				defer done()
				e.release(t, cleanup, "", false)
			})

			_, _, err := e.build(ctx, encRandom(t, encKeyBytes))
			if err == nil {
				t.Fatalf("the chain was built over the colliding %s mapping", layer)
			}
			t.Logf("build refused: %v", err)
			if (layer == "node" && e.rec.Node != encDMRecord{}) || (layer == "crypt" && e.rec.Crypt != encDMRecord{}) {
				t.Fatalf("the colliding mapping was recorded as the test's: %+v", e.rec)
			}
			if !e.rec.held() || e.rec.Loop.Loop == "" {
				t.Fatalf("nothing of the chain was built before the collision: %+v", e.rec)
			}
			out := e.release(t, ctx, "", false)
			data, _ := json.Marshal(out)
			t.Logf("ENCRYPTED_STORE_COLLISION_RELEASE %s", data)
			own, _ := out["ownership"].(encOwnership)
			collided := own.Node
			if layer == "crypt" {
				collided = own.Crypt
			}
			if collided.Verdict != encLayerForeign || e.rec.held() {
				t.Fatalf("release: the %s layer is %+v, still held %+v", layer, collided, e.rec)
			}
			seen := e.do(t, ctx, nil, "observe", name, uuid, e.crypt+"-unused", "-", "-")
			if s := seen.SeenNode; s == nil || !s.Exists || s.UUID != uuid || s.Number != theirs.Number || encLower(s.Table) != "error" {
				t.Fatalf("the colliding mapping did not survive the release: %+v", s)
			}
			listed := e.do(t, ctx, nil, "nodes", "-")
			if !slices.Contains(listed.Nodes, encNodeSeen{Name: name, Kind: "block", Number: theirs.Number, Live: true, DMName: name}) {
				t.Fatalf("the colliding mapping's node did not survive the release: %+v", listed.Nodes)
			}
		})
	}
}
