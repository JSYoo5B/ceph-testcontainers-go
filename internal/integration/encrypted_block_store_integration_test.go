//go:build (all || integration) && ((osd_encrypted_store && !ci) || (ci_optional && (!ci_batch || ci_batch_osd_encrypted_store)))

//ci: timeout=30m job-timeout=40 image-roles=essential,client

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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
// keeper on standard input.
type encEnv struct {
	docker            *mobycl.Client
	keeper            testcontainers.Container
	node, crypt, file string
	loop              string
	cluster           *ceph.Container
	osd               *ceph.OSDContainer
	client            testcontainers.Container
	device            faulthook.BlockDevice
}

// do runs one keeper action with input as its standard input and fails the
// test unless the keeper printed a receipt for it.
func (e *encEnv) do(t *testing.T, ctx context.Context, input []byte, action string, args ...string) encReply {
	t.Helper()
	out, errOut, code, err := encExec(ctx, e.docker, e.keeper.GetContainerID(), input, append([]string{"python3", "-I", "-c", encKeeperScript, action}, args...)...)
	if err != nil || code != 0 {
		t.Fatalf("keeper %s: exit %d %v: %s %s", action, code, err, out, errOut)
	}
	r, err := parseEncReply(out, action)
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	return r
}

// encExec runs argv in a container with input written to its standard input,
// which is then closed, and returns its output once the process has exited.
func encExec(ctx context.Context, docker *mobycl.Client, id string, input []byte, argv ...string) ([]byte, []byte, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	created, err := docker.ExecCreate(ctx, id, mobycl.ExecCreateOptions{AttachStdin: true, AttachStdout: true, AttachStderr: true, Cmd: argv})
	if err != nil {
		return nil, nil, -1, err
	}
	attached, err := docker.ExecAttach(ctx, created.ID, mobycl.ExecAttachOptions{})
	if err != nil {
		return nil, nil, -1, err
	}
	defer attached.Close()
	if len(input) > 0 {
		if _, err := attached.Conn.Write(input); err != nil {
			return nil, nil, -1, err
		}
	}
	if err := attached.CloseWrite(); err != nil {
		return nil, nil, -1, err
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, attached.Reader); err != nil {
		return stdout.Bytes(), stderr.Bytes(), -1, err
	}
	for {
		inspected, err := docker.ExecInspect(ctx, created.ID, mobycl.ExecInspectOptions{})
		if err != nil {
			return stdout.Bytes(), stderr.Bytes(), -1, err
		}
		if !inspected.Running {
			return stdout.Bytes(), stderr.Bytes(), inspected.ExitCode, nil
		}
		select {
		case <-ctx.Done():
			return stdout.Bytes(), stderr.Bytes(), -1, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
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
	out, _, code, err := encExec(ctx, e.docker, e.keeper.GetContainerID(), nil, "python3", "-I", "-c", encKeeperScript, "stack", e.node, e.crypt, e.loop, e.file)
	if err != nil || code != 0 {
		return ev, fmt.Errorf("keeper stack: exit %d %v", code, err)
	}
	r, err := parseEncReply(out, "stack")
	if err != nil {
		return ev, err
	}
	if r.NodeOpen != nil {
		ev.ScanComplete, ev.Openers = faulthook.FactYes, *r.NodeOpen
	}
	return ev, nil
}

// release removes what the test created of the chain. A chain whose node was
// given to the OSD is released only when the cluster's claim table and the
// keeper's evidence both raise no objection; otherwise all of it is kept.
func (e *encEnv) release(t *testing.T, ctx context.Context, osdID string, attached bool) (map[string]any, bool) {
	out := map[string]any{"node": e.node}
	if attached {
		outcome, detail := faulthook.JudgeDeviceChain(ctx, e.cluster, e.device, e.node, func(ctx context.Context) (faulthook.ChainEvidence, error) {
			return e.chainEvidence(ctx, osdID)
		})
		out["verdict"], out["detail"] = outcome, detail
		if outcome != faulthook.ChainClear {
			t.Errorf("the OSD's device chain %s is retained: %s: %s", e.node, outcome, detail)
			return out, false
		}
	}
	raw, _, code, err := encExec(ctx, e.docker, e.keeper.GetContainerID(), nil, "python3", "-I", "-c", encKeeperScript, "remove", e.node, e.crypt, e.file)
	r, perr := parseEncReply(raw, "remove")
	out["removed"] = r
	if err != nil || code != 0 || perr != nil || !r.NodeGone || !r.CryptGone || !r.LoopReleased || !r.FileRemoved {
		t.Errorf("release of chain %s: exit %d %v %v %s", e.node, code, err, perr, raw)
		return out, false
	}
	return out, true
}

func encRandom(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
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
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	image, opts := harness.IntegrationImages(t)
	keeperImage := harness.NativeClientImage(t, image)
	receipt := map[string]any{"payload_bytes": encPayloadBytes}
	defer func() {
		data, _ := json.Marshal(receipt)
		t.Logf("ENCRYPTED_STORE_RECEIPT %s", data)
	}()

	// The keeper runs the client role image, which ships cryptsetup, and sees
	// the engine host's /dev. DM_DISABLE_UDEV makes libdevmapper create and
	// remove the nodes itself instead of waiting for a udev it cannot reach.
	keeper, err := testcontainers.Run(ctx, keeperImage, testcontainers.WithEntrypoint("sleep", "infinity"),
		testcontainers.WithEnv(map[string]string{"DM_DISABLE_UDEV": "1"}),
		testcontainers.WithHostConfigModifier(func(h *container.HostConfig) {
			h.Privileged = true
			h.Binds = append(h.Binds, "/dev:/dev")
			h.Tmpfs = map[string]string{encKeeperDir: "size=" + strconv.FormatInt(encPayloadBytes+encHeaderBytes+(64<<20), 10) + ",mode=0700", "/run/cryptsetup": "size=1m,mode=0700"}
			h.NetworkMode = "none"
		}))
	released := false
	if keeper != nil {
		t.Cleanup(func() {
			if !released {
				t.Errorf("block device keeper %s kept: its chain was not released", keeper.GetContainerID())
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
	name := "tcenc-" + hex.EncodeToString(encRandom(t, 4))
	e := &encEnv{docker: docker.Client, keeper: keeper, node: name, crypt: name + "-crypt", file: encKeeperDir + "/" + name}
	released = true // nothing exists yet

	admit := e.do(t, ctx, nil, "admit", "-")
	autoload := os.Getenv("RUNNER_ENVIRONMENT") == "github-hosted"
	receipt["admission"] = map[string]any{"kernel": admit.Kernel, "targets": admit.Targets, "cryptsetup": admit.Cryptsetup, "autoload_allowed": autoload}
	for _, target := range []string{"linear", "error"} {
		if !slices.Contains(admit.Targets, target) {
			t.Fatalf("encrypted OSD store refused: the device-mapper %s target is not registered", target)
		}
	}
	if !slices.Contains(admit.Targets, "crypt") && !autoload {
		t.Fatal("encrypted OSD store refused: the device-mapper crypt target is not registered on the engine host; load dm_crypt there first, the test does not load modules")
	}

	key, wrong := encRandom(t, encKeyBytes), encRandom(t, encKeyBytes)
	var cluster *ceph.Container
	var osdID string
	attached := false
	// Registered before the cluster's Terminate, so it runs after it.
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
		defer done()
		out, ok := e.release(t, cleanup, osdID, attached)
		released = ok
		data, _ := json.Marshal(out)
		t.Logf("ENCRYPTED_STORE_RELEASE_RECEIPT %s", data)
	})

	// The chain, bottom up. Each receipt names what exists, so a failure
	// leaves only the parts that cleanup then removes.
	released = false
	created := e.do(t, ctx, nil, "create", e.file, strconv.FormatInt(encPayloadBytes+encHeaderBytes, 10))
	if created.Error != "" || created.Loop == "" {
		t.Fatalf("backing loop: %+v", created)
	}
	e.loop = created.Loop
	formatted := e.do(t, ctx, key, "format", e.loop)
	header := encHeaderOf(formatted)
	if formatted.Error != "" || !header.known() || header.Offset != encHeaderBytes {
		t.Fatalf("LUKS2 format: %+v", formatted)
	}
	opened := e.do(t, ctx, key, "open", e.loop, e.crypt)
	if opened.RC == nil || *opened.RC != 0 || !opened.Mapped || opened.Bytes != encPayloadBytes {
		t.Fatalf("crypt mapping: %+v", opened)
	}
	linear := e.do(t, ctx, nil, "linear", e.node, e.crypt, "TC-ENC-"+e.node)
	if linear.Error != "" || linear.Path == "" || linear.Bytes != encPayloadBytes {
		t.Fatalf("OSD node: %+v", linear)
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
	receipt["chain"] = map[string]any{"loop": created.LoopNumber, "header": header, "crypt": opened.Number, "node": linear.Number, "stack": verdict, "detail": detail,
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
	retarget := e.do(t, ctx, nil, "retarget", e.node, "error")
	closed := e.do(t, ctx, nil, "close", e.crypt)
	receipt["lock"] = map[string]any{"table": retarget.Table, "close": closed}
	if retarget.Error != "" || !strings.HasSuffix(retarget.Table, " error") || closed.Error != "" || !closed.Closed {
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
		r := e.do(t, ctx, attempt.input, "open", e.loop, e.crypt)
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
	reopened := e.do(t, ctx, key, "open", e.loop, e.crypt)
	if reopened.RC == nil || *reopened.RC != 0 || !reopened.Mapped || reopened.Existed || encHeaderOf(reopened) != header {
		t.Fatalf("reopen with the right key: %+v", reopened)
	}
	back := e.do(t, ctx, nil, "retarget", e.node, e.crypt)
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
	unlock := map[string]any{"crypt": reopened.Number, "stack": verdict, "detail": detail, "up_from": [2]int{before.UpFrom, upAgain.UpFrom},
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
