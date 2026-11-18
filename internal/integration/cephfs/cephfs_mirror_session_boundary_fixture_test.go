//go:build all || (integration && multicluster && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestCephFSMirrorSessionBoundaryReadsBothExecEncodings(t *testing.T) {
	const session = `{"rados_inst":"session-address","peers":{"peer":{"remote":{"client_name":"client.peer","cluster_name":"site","fs_name":"destination"}}}}`
	var framed bytes.Buffer
	header := [8]byte{byte(stdcopy.Stdout)}
	binary.BigEndian.PutUint32(header[4:], uint32(len(session)))
	framed.Write(header[:])
	framed.WriteString(session)
	for _, raw := range [][]byte{[]byte(session), framed.Bytes()} {
		before := bytes.Clone(raw)
		boundary := &cephFSMirrorSessionBoundary{Container: cephFSBoundaryReply{raw: raw}, fired: true}
		code, reader, readErr := boundary.Exec(t.Context(), []string{"ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "peer", "status", "filesystem@1", "peer"})
		if readErr != nil || code != 0 || !boundary.replayObserved {
			t.Fatalf("replay capture failed: code=%d error=%v", code, readErr)
		}
		returned, readErr := io.ReadAll(reader)
		if readErr != nil || !bytes.Equal(returned, before) || !bytes.Equal(boundary.replayRaw, before) {
			t.Fatalf("replay framing changed: returned=%q retained=%q error=%v", returned, boundary.replayRaw, readErr)
		}
		identity, err := cephFSBoundarySession(raw, "peer")
		if err != nil || identity.Address != "session-address" || identity.Peer.Client != "client.peer" || identity.Peer.Site != "site" || identity.Peer.Filesystem != "destination" || !bytes.Equal(raw, before) {
			t.Fatalf("original session bytes changed or identity lost: %+v %v", identity, err)
		}
	}
	for _, raw := range []string{
		`{}`, `{"rados_inst":"new","peers":{}}`,
		strings.Replace(session, `"session-address"`, `""`, 1),
		strings.Replace(session, `"client.peer"`, `""`, 1),
		`{"state":"failed",` + session[1:],
	} {
		if _, err := cephFSBoundarySession([]byte(raw), "peer"); err == nil {
			t.Fatalf("incomplete or unavailable replacement session accepted: %s", raw)
		}
	}
}

// cephFSBoundaryReply supplies raw Exec bytes to verify that evidence capture
// preserves the original reader's framing for its consumer.
type cephFSBoundaryReply struct {
	testcontainers.Container
	raw []byte
}

func (r cephFSBoundaryReply) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	return 0, bytes.NewReader(r.raw), nil
}
