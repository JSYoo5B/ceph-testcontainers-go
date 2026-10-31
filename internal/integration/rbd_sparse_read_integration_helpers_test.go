//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// sparseReadOp is one sparse-read that a log line shows: the request a
// client sent, or the reply an OSD sent or a client received. Out is the
// reply's output length, which holds the extent map and the data of the
// extents, and -1 on a request.
type sparseReadOp struct {
	Line   int    `json:"line"`
	Dir    string `json:"dir"`
	Kind   string `json:"kind"`
	Tid    uint64 `json:"tid"`
	Object string `json:"object"`
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
	Out    int64  `json:"out"`
	Result int    `json:"result"`
	Text   string `json:"-"`
}

var (
	sparseRequestLine = regexp.MustCompile(` --> \S+ -- osd_op\(\S+\.\d+:(\d+) \S+ \S+:::([^:\s]+):head \[sparse-read (\d+)~(\d+)`)
	sparseReplyLine   = regexp.MustCompile(` (-->|<==) .*osd_op_reply\((\d+) (\S+) \[sparse-read (\d+)~(\d+)(?: \[[^\]]*\])?(?: out=(\d+)b)?\] v\S+ uv\d+ ondisk = (-?\d+)`)
)

// parseSparseReads picks the sparse-read requests and replies out of a log.
func parseSparseReads(lines []string) []sparseReadOp {
	var ops []sparseReadOp
	number := func(s string) uint64 {
		n, _ := strconv.ParseUint(s, 10, 64)
		return n
	}
	for index, line := range lines {
		if m := sparseRequestLine.FindStringSubmatch(line); m != nil {
			ops = append(ops, sparseReadOp{Line: index, Dir: "out", Kind: "request", Tid: number(m[1]), Object: m[2], Offset: number(m[3]), Length: number(m[4]), Out: -1, Text: line})
		} else if m := sparseReplyLine.FindStringSubmatch(line); m != nil {
			dir := map[string]string{"-->": "out", "<==": "in"}[m[1]]
			out := int64(-1)
			if m[6] != "" {
				out = int64(number(m[6]))
			}
			result, _ := strconv.Atoi(m[7])
			ops = append(ops, sparseReadOp{Line: index, Dir: dir, Kind: "reply", Tid: number(m[2]), Object: m[3], Offset: number(m[4]), Length: number(m[5]), Out: out, Result: result, Text: line})
		}
	}
	return ops
}

// sparseObject is what one data object of the image holds: the bytes
// written to it, as one extent, or nothing.
type sparseObject struct {
	Object string `json:"object"`
	Data   uint64 `json:"data"`
}

// sparseExtentMapSize is the encoded extent map of a sparse-read with one
// extent: its count and the extent's offset and length, then the data
// length.
const sparseExtentMapSize = 4 + 16 + 4

// sparseReadJoin is one object's sparse-read as the client sent it, as an
// OSD answered it and as the client received the answer.
type sparseReadJoin struct {
	Object   sparseObject `json:"object"`
	Request  sparseReadOp `json:"request"`
	Sent     sparseReadOp `json:"sent"`
	Received sparseReadOp `json:"received"`
}

// judgeSparseReads requires every object to be read with one sparse-read
// that the client sent, an OSD answered and the client received with the
// same tid, object and offset. The reply's length is what the OSD returned,
// never more than the request's. An object with data must be answered with
// result 0, a length of exactly its data and an output of that data and a
// one-extent map, less than the range read; an absent object with -ENOENT
// and the requested length.
func judgeSparseReads(client, osds []sparseReadOp, objects []sparseObject) ([]sparseReadJoin, []string) {
	var joins []sparseReadJoin
	var problems []string
	for _, object := range objects {
		request := slices.IndexFunc(client, func(op sparseReadOp) bool { return op.Kind == "request" && op.Object == object.Object })
		if request < 0 {
			problems = append(problems, fmt.Sprintf("the client sent no sparse-read of %s", object.Object))
			continue
		}
		join := sparseReadJoin{Object: object, Request: client[request]}
		same := func(op sparseReadOp) bool {
			return op.Kind == "reply" && op.Tid == join.Request.Tid && op.Object == object.Object && op.Offset == join.Request.Offset && op.Length <= join.Request.Length
		}
		sent := slices.IndexFunc(osds, func(op sparseReadOp) bool { return op.Dir == "out" && same(op) })
		received := slices.IndexFunc(client, func(op sparseReadOp) bool { return op.Dir == "in" && same(op) })
		if sent < 0 || received < 0 {
			problems = append(problems, fmt.Sprintf("sparse-read tid %d of %s: sent by an OSD %t, received %t", join.Request.Tid, object.Object, sent >= 0, received >= 0))
			continue
		}
		join.Sent, join.Received = osds[sent], client[received]
		switch reply := join.Received; {
		case reply.Result != join.Sent.Result || reply.Out != join.Sent.Out || reply.Length != join.Sent.Length:
			problems = append(problems, fmt.Sprintf("%s: the client received result %d length %d out %d, the OSD sent %d, %d, %d", object.Object,
				reply.Result, reply.Length, reply.Out, join.Sent.Result, join.Sent.Length, join.Sent.Out))
		case object.Data == 0 && (reply.Result != -2 || reply.Length != join.Request.Length):
			problems = append(problems, fmt.Sprintf("absent %s answered with result %d and length %d", object.Object, reply.Result, reply.Length))
		case object.Data > 0 && (reply.Result != 0 || reply.Length != object.Data || reply.Out != int64(object.Data)+sparseExtentMapSize || uint64(reply.Out) >= join.Request.Length):
			problems = append(problems, fmt.Sprintf("%s with %d bytes answered with result %d, length %d and %d bytes out of %d read", object.Object, object.Data,
				reply.Result, reply.Length, reply.Out, join.Request.Length))
		}
		joins = append(joins, join)
	}
	return joins, problems
}

// rbdSparseWriter creates a 16 MiB image with 4 MiB objects, writes 64 KiB
// at 1 MiB and 4 KiB at 5 MiB, so object 0 and 1 hold one extent each and
// objects 2 and 3 do not exist, and prints the image's block name prefix
// and the SHA-256 of the whole image as written.
const rbdSparseWriter = `import hashlib, json, sys, rados, rbd
pool, name = sys.argv[1:3]
size, writes = 16 << 20, [(1 << 20, b"S" * (64 << 10)), (5 << 20, b"P" * 4096)]
image = bytearray(size)
for offset, data in writes:
    image[offset:offset + len(data)] = data
with rados.Rados(conffile="/etc/ceph/ceph.conf") as cluster:
    with cluster.open_ioctx(pool) as io:
        rbd.RBD().create(io, name, size, order=22)
        with rbd.Image(io, name) as img:
            for offset, data in writes:
                img.write(data, offset)
            img.flush()
            prefix = img.stat()["block_name_prefix"]
print(json.dumps({"prefix": prefix, "sha256": hashlib.sha256(bytes(image)).hexdigest()}))`

// rbdSparseExport exports the image with librbd's rbd CLI, logging its
// messages, and prints the SHA-256 of the exported file.
const rbdSparseExport = `set -e
rm -f /tmp/tc-sparse.img /tmp/tc-sparse.log
rbd export "$1/$2" /tmp/tc-sparse.img --debug_ms 1 --log-file /tmp/tc-sparse.log --log-to-file true --log-to-stderr false >/dev/null 2>&1
sha256sum /tmp/tc-sparse.img | cut -d' ' -f1`

// osdSparseReadReceipt is the evidence of librbd's sparse reads.
type osdSparseReadReceipt struct {
	Release  string           `json:"release"`
	Platform string           `json:"platform"`
	Run      string           `json:"run,omitempty"`
	Prefix   string           `json:"prefix"`
	Written  string           `json:"written_sha256"`
	Exported string           `json:"exported_sha256"`
	Joins    []sparseReadJoin `json:"joins"`
}

// testRBDSparseReadReceipt writes two extents of an RBD image through
// librbd and exports it with the rbd CLI, which reads each 4 MiB data object
// with CEPH_OSD_OP_SPARSE_READ: python-rados and the rados CLI of the
// official image issue no sparse-read. The client and the OSDs log their
// messages at debug_ms 1, and judgeSparseReads joins each object's request
// with its reply; the export equals the written image byte for byte.
func testRBDSparseReadReceipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	config := filepath.Join(t.TempDir(), "ceph.conf")
	if err := os.WriteFile(config, []byte("[osd]\ndebug_ms = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cluster, client := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(2)), ceph.WithConfigFile(config))
	const pool, image = "tc-sparse-read", "sparse"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 1, Replicas: 2, Application: "rbd"}); err != nil {
		t.Fatal(err)
	}
	receipt := osdSparseReadReceipt{Platform: runtime.GOOS + "/" + runtime.GOARCH}
	if run := os.Getenv("GITHUB_RUN_ID"); run != "" {
		receipt.Run = run + "/" + os.Getenv("GITHUB_RUN_ATTEMPT")
	}
	defer func() {
		data, err := json.Marshal(receipt)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("OSD_SPARSE_READ_RECEIPT %s", data)
	}()
	receipt.Release = targetedJSON[map[string]string](t, ctx, cluster, "tell", "osd.0", "version")["version"]
	run := func(label string, argv ...string) string {
		result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: argv, Timeout: 2 * time.Minute, MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: label})
		if err != nil || result.ExitCode != 0 || !result.Complete() {
			t.Fatalf("%s: exit %d: %v: %.2048s", label, result.ExitCode, err, result.Stderr)
		}
		return string(result.Stdout)
	}
	var written struct {
		Prefix string `json:"prefix"`
		SHA256 string `json:"sha256"`
	}
	if err := json.Unmarshal([]byte(harness.LastJSONLine(run("sparse image writer", "python3", "-c", rbdSparseWriter, pool, image))), &written); err != nil || written.Prefix == "" {
		t.Fatalf("writer output: %v %+v", err, written)
	}
	receipt.Prefix, receipt.Written = written.Prefix, written.SHA256
	receipt.Exported = strings.TrimSpace(run("sparse image export", "sh", "-c", rbdSparseExport, "export", pool, image))
	if receipt.Exported != receipt.Written {
		t.Fatalf("the export has SHA-256 %s, the written image %s", receipt.Exported, receipt.Written)
	}
	clientLog := strings.Split(run("export log", "cat", "/tmp/tc-sparse.log"), "\n")
	var osdOps []sparseReadOp
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		osdOps = append(osdOps, parseSparseReads(backoffDaemonLog(t, ctx, osd.Container))...)
	}
	object := func(n int) string { return fmt.Sprintf("%s.%016x", written.Prefix, n) }
	objects := []sparseObject{{object(0), 64 << 10}, {object(1), 4096}, {object(2), 0}, {object(3), 0}}
	joins, problems := judgeSparseReads(parseSparseReads(clientLog), osdOps, objects)
	receipt.Joins = joins
	for _, join := range joins {
		t.Logf("OSD_SPARSE_READ_RAW request: %s", join.Request.Text)
		t.Logf("OSD_SPARSE_READ_RAW sent: %s", join.Sent.Text)
		t.Logf("OSD_SPARSE_READ_RAW received: %s", join.Received.Text)
	}
	if len(problems) != 0 {
		t.Fatalf("sparse reads: %s", strings.Join(problems, "; "))
	}
}
