//go:build all || integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// heldReplicationReceipt records one replicated write that an OSD held while
// the cluster plane of one OSD was cut: which PG and OSDs it involved, the
// request the primary held and for how long, the controls that kept working
// meanwhile, and how the writer and an independent reader saw the result.
// It is evidence for this release and profile, not a Messenger decoder.
type heldReplicationReceipt struct {
	Family string `json:"family"`
	FSID   string `json:"fsid"`
	Images struct {
		Monitor string `json:"monitor"`
		OSD     string `json:"osd"`
		Client  string `json:"client"`
	} `json:"images"`
	Mapping     heldMapping `json:"mapping"`
	Interrupted struct {
		OSD     int                  `json:"osd"`
		CID     string               `json:"cid"`
		Public  []ceph.EntityAddress `json:"public"`
		Cluster []ceph.EntityAddress `json:"cluster"`
	} `json:"interrupted"`
	Held struct {
		Primary    string        `json:"primary"`
		Request    string        `json:"request"`
		WaitingFor string        `json:"waiting_for"`
		FlagPoint  string        `json:"flag_point"`
		Events     []string      `json:"events"`
		For        time.Duration `json:"for_ns"`
	} `json:"held"`
	Controls struct {
		OSDMapEpoch    uint32      `json:"osdmap_epoch"`
		Mapping        heldMapping `json:"mapping"`
		PublicListener string      `json:"public_listener"`
	} `json:"controls"`
	Writer       heldReadback `json:"writer"`
	Reader       heldReadback `json:"reader"`
	MappingAfter heldMapping  `json:"mapping_after"`
}

// heldMapping is the native "osd map" of one object.
type heldMapping struct {
	Epoch         uint32 `json:"epoch"`
	Pool          string `json:"pool"`
	PoolID        int64  `json:"pool_id"`
	Object        string `json:"objname"`
	PGID          string `json:"pgid"`
	Up            []int  `json:"up"`
	UpPrimary     int    `json:"up_primary"`
	Acting        []int  `json:"acting"`
	ActingPrimary int    `json:"acting_primary"`
}

// heldReadback is what one native client saw of the object.
type heldReadback struct {
	Client  string `json:"client,omitempty"`
	Size    int    `json:"size"`
	SHA256  string `json:"sha256"`
	Version uint64 `json:"version"`
}

// parseHeldMapping decodes "osd map" output for pool and object and requires a
// two-OSD acting set that contains its primary.
func parseHeldMapping(data []byte, pool, object string) (heldMapping, error) {
	var mapping heldMapping
	if err := json.Unmarshal(data, &mapping); err != nil {
		return mapping, fmt.Errorf("osd map: %w", err)
	}
	if mapping.Pool != pool || mapping.Object != object || mapping.PGID == "" || mapping.Epoch == 0 {
		return mapping, fmt.Errorf("osd map describes %s/%s in %q at epoch %d, want %s/%s", mapping.Pool, mapping.Object, mapping.PGID, mapping.Epoch, pool, object)
	}
	if len(mapping.Acting) != 2 || !slices.Contains(mapping.Acting, mapping.ActingPrimary) || !slices.Equal(mapping.Up, mapping.Acting) || mapping.UpPrimary != mapping.ActingPrimary {
		return mapping, fmt.Errorf("osd map of %s: up %v/%d, acting %v/%d, want the same two OSDs", object, mapping.Up, mapping.UpPrimary, mapping.Acting, mapping.ActingPrimary)
	}
	return mapping, nil
}

// samePlacement reports an unchanged PG, acting set and primary, whatever the
// epoch; unrelated map changes do not undo a held phase.
func (m heldMapping) samePlacement(other heldMapping) bool {
	return m.PoolID == other.PoolID && m.PGID == other.PGID && slices.Equal(m.Acting, other.Acting) && m.ActingPrimary == other.ActingPrimary
}

// heldOp is an operation from an OSD's dump_ops_in_flight.
type heldOp struct {
	Description string `json:"description"`
	TypeData    struct {
		FlagPoint string `json:"flag_point"`
		Events    []struct {
			Event string `json:"event"`
		} `json:"events"`
	} `json:"type_data"`
}

// request returns the client request that the operation serves, such as
// "client.4211.0:1".
func (op heldOp) request() string {
	request, _, _ := strings.Cut(strings.TrimPrefix(op.Description, "osd_op("), " ")
	return request
}

// findHeldOp returns the operation in a dump_ops_in_flight output that writes
// object and waits for its replica, which has not reported a commit. The
// description names the object as ":::<object>:head".
func findHeldOp(data []byte, object string) (heldOp, bool, error) {
	var dump struct {
		Ops []heldOp `json:"ops"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return heldOp{}, false, fmt.Errorf("dump_ops_in_flight: %w", err)
	}
	for _, op := range dump.Ops {
		if !strings.HasPrefix(op.Description, "osd_op(") || !strings.Contains(op.Description, ":::"+object+":head") || op.TypeData.FlagPoint != "waiting for sub ops" {
			continue
		}
		committed := slices.ContainsFunc(op.TypeData.Events, func(event struct {
			Event string `json:"event"`
		}) bool {
			return strings.HasPrefix(event.Event, "sub_op_commit_rec")
		})
		if !committed && op.request() != "" {
			return op, true, nil
		}
	}
	return heldOp{}, false, nil
}

// waitingFor returns the replicas that the operation's "waiting for subops
// from" event names, such as "0" or "1,2", or "" without that event.
func (op heldOp) waitingFor() string {
	for _, event := range op.TypeData.Events {
		if replicas, found := strings.CutPrefix(event.Event, "waiting for subops from "); found {
			return replicas
		}
	}
	return ""
}

// replica returns the acting OSD that is not the primary.
func (m heldMapping) replica() int {
	for _, osd := range m.Acting {
		if osd != m.ActingPrimary {
			return osd
		}
	}
	return -1
}

// events returns the operation's event names, at most 32.
func (op heldOp) events() []string {
	var names []string
	for _, event := range op.TypeData.Events {
		if len(names) == 32 {
			break
		}
		names = append(names, event.Event)
	}
	return names
}

// heldWriter writes the held object with no client-side timeout and reports
// the version and digest it wrote. The read through the same handle stays as
// the writer's own check; heldReader is the independent one.
const heldWriter = `import hashlib, json, rados
payload = bytes(range(256)) * 64
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"rados_osd_op_timeout": "0"}) as cluster:
    with cluster.open_ioctx("tc-topology") as io:
        io.write_full("held", payload)
        version = io.get_last_version()
        assert io.read("held", len(payload), 0) == payload
        print(json.dumps({"size": len(payload), "sha256": hashlib.sha256(payload).hexdigest(), "version": version}))`

// heldReader reads the held object through a client of its own and reports
// its client ID, size, digest and version.
const heldReader = `import hashlib, json, rados
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"rados_osd_op_timeout": "30"}) as cluster:
    with cluster.open_ioctx("tc-topology") as io:
        size, _ = io.stat("held")
        data = io.read("held", size + 1, 0)
        print(json.dumps({"client": "client.%d" % cluster.get_instance_id(), "size": len(data), "sha256": hashlib.sha256(data).hexdigest(), "version": io.get_last_version()}))`

// heldReplicationWrite is a write that the primary holds.
type heldReplicationWrite struct {
	done   chan struct{}
	result ceph.NativeCommandResult
	err    error
}

// startHeldReplicationWrite maps the held object, starts the write from client
// and waits until the primary has reported the same request waiting for its
// replica, without a replica commit, for 10 seconds, while the writer still
// waits. A normal replicated write passes that state in milliseconds. It
// fills the receipt's mapping and held fields.
func startHeldReplicationWrite(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, receipt *heldReplicationReceipt) *heldReplicationWrite {
	t.Helper()
	out, err := cluster.Ceph(ctx, "osd", "map", "tc-topology", "held", "--format", "json")
	if err != nil {
		t.Fatal("map of the held object:", err)
	}
	if receipt.Mapping, err = parseHeldMapping(out, "tc-topology", "held"); err != nil {
		t.Fatal(err)
	}
	t.Logf("HELD_REPLICATION_RAW osd map: %.2048s", out)
	write := &heldReplicationWrite{done: make(chan struct{})}
	writeCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(write.done)
		write.result, write.err = cluster.RunNativeCommand(writeCtx, client, ceph.NativeCommand{
			Argv: []string{"python3", "-c", heldWriter}, Timeout: 5 * time.Minute, MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "held replicated write",
		})
	}()
	t.Cleanup(func() {
		cancel()
		<-write.done
		var unstopped *ceph.NativeCommandUnstoppedError
		if errors.As(write.err, &unstopped) {
			stopCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := unstopped.Stop(stopCtx); err != nil {
				t.Errorf("stop held write: %v", err)
			}
		}
	})
	primary := fmt.Sprintf("osd.%d", receipt.Mapping.ActingPrimary)
	var current heldOp
	var since time.Time
	for deadline := time.Now().Add(2 * time.Minute); ; {
		select {
		case <-write.done:
			t.Fatalf("the write finished while the backend was cut: %v", write.err)
		default:
		}
		data, err := cluster.AdminSocket(ctx, primary, "dump_ops_in_flight")
		if err != nil {
			t.Fatal(err)
		}
		op, found, err := findHeldOp(data, "held")
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case !found:
			current = heldOp{}
		case op.request() != current.request():
			current, since = op, time.Now()
		case time.Since(since) >= 10*time.Second:
			select {
			case <-write.done:
				t.Fatalf("the write finished while the backend was cut: %v", write.err)
			default:
			}
			// The primary names the replica it waits for: the other acting OSD.
			if want := strconv.Itoa(receipt.Mapping.replica()); op.waitingFor() != want {
				t.Fatalf("%s waits for subops from %q, want osd.%s: %.4096s", primary, op.waitingFor(), want, data)
			}
			receipt.Held.Primary, receipt.Held.Request, receipt.Held.FlagPoint = primary, op.request(), op.TypeData.FlagPoint
			receipt.Held.WaitingFor = op.waitingFor()
			receipt.Held.Events, receipt.Held.For = op.events(), time.Since(since).Round(time.Second)
			t.Logf("HELD_REPLICATION_RAW dump_ops_in_flight: %.4096s", data)
			t.Logf("%s held %s to held for %v, waiting for its replica across the cut backend", primary, receipt.Held.Request, receipt.Held.For)
			return write
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not hold the write waiting for its replica for 10 seconds: %.2048s", primary, data)
		}
		time.Sleep(time.Second)
	}
}

// checkHeldControls checks, while the write is held, that a MON command still
// answers, that the placement is the one the write was held in, with every
// OSD up, and that the interrupted OSD's public listener accepts TCP. A
// changed placement or a down OSD leaves the held phase unproven.
func checkHeldControls(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, receipt *heldReplicationReceipt, write *heldReplicationWrite) {
	t.Helper()
	out, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		t.Fatal("MON command during the held write:", err)
	}
	var dump struct {
		FSID  string `json:"fsid"`
		Epoch uint32 `json:"epoch"`
		OSDs  []struct {
			ID int `json:"osd"`
			Up int `json:"up"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		t.Fatalf("osd dump: %v", err)
	}
	receipt.FSID, receipt.Controls.OSDMapEpoch = dump.FSID, dump.Epoch
	for _, osd := range dump.OSDs {
		if slices.Contains(receipt.Mapping.Acting, osd.ID) && osd.Up != 1 {
			t.Fatalf("held phase unproven: osd.%d went down during the hold at epoch %d", osd.ID, dump.Epoch)
		}
	}
	out, err = cluster.Ceph(ctx, "osd", "map", "tc-topology", "held", "--format", "json")
	if err != nil {
		t.Fatal("osd map during the held write:", err)
	}
	if receipt.Controls.Mapping, err = parseHeldMapping(out, "tc-topology", "held"); err != nil || !receipt.Controls.Mapping.samePlacement(receipt.Mapping) {
		t.Fatalf("held phase unproven: placement %+v, held in %+v: %v", receipt.Controls.Mapping, receipt.Mapping, err)
	}
	address := receipt.Interrupted.Public[0].Addr
	execCommand(t, ctx, client, "python3", "-c", `import socket,sys; host,_,port=sys.argv[1].rpartition(":"); s=socket.create_connection((host.strip("[]"),int(port)),5); s.close()`, address)
	receipt.Controls.PublicListener = address
	select {
	case <-write.done:
		t.Fatalf("the write finished while the backend was cut: %v", write.err)
	default:
	}
}

// finishHeldReplicationWrite waits for the writer after the backend is
// restored, then reads the object with a client of its own in the control
// container and requires the same bytes and version.
func finishHeldReplicationWrite(t *testing.T, ctx context.Context, cluster *ceph.Container, receipt *heldReplicationReceipt, write *heldReplicationWrite) {
	t.Helper()
	select {
	case <-write.done:
	case <-time.After(2 * time.Minute):
		t.Fatal("the held write did not finish after the backend was restored")
	}
	if write.err != nil || write.result.ExitCode != 0 || !write.result.Complete() {
		t.Fatalf("held write: exit %d, complete %t: %v: %.2048s", write.result.ExitCode, write.result.Complete(), write.err, write.result.Stderr)
	}
	if err := json.Unmarshal([]byte(lastJSONLine(string(write.result.Stdout))), &receipt.Writer); err != nil {
		t.Fatalf("held writer output: %v: %.512s", err, write.result.Stdout)
	}
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	read, err := cluster.RunNativeCommand(readCtx, toolkitContainer(t, cluster), ceph.NativeCommand{
		Argv: []string{"python3", "-c", heldReader}, Timeout: 2 * time.Minute, MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "independent held reader",
	})
	cancel()
	if err != nil || read.ExitCode != 0 || !read.Complete() {
		t.Fatalf("independent reader: exit %d: %v: %.2048s", read.ExitCode, err, read.Stderr)
	}
	if err := json.Unmarshal([]byte(lastJSONLine(string(read.Stdout))), &receipt.Reader); err != nil {
		t.Fatalf("independent reader output: %v: %.512s", err, read.Stdout)
	}
	payload := make([]byte, 0, 256*64)
	for range 64 {
		for value := range 256 {
			payload = append(payload, byte(value))
		}
	}
	sum := sha256.Sum256(payload)
	want := heldReadback{Size: len(payload), SHA256: hex.EncodeToString(sum[:]), Version: receipt.Writer.Version}
	got := receipt.Reader
	got.Client = ""
	if receipt.Writer != want || got != want || receipt.Writer.Version == 0 {
		t.Fatalf("writer %+v and independent reader %+v, want %+v", receipt.Writer, receipt.Reader, want)
	}
}

// imageID returns the image ID that Docker runs ctr from.
func imageID(t *testing.T, ctx context.Context, ctr testcontainers.Container) string {
	t.Helper()
	inspection, err := ctr.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return inspection.Image
}
