//go:build all || (integration && multicluster)

package harness

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// metadataPolicyControl provides explicit native fields to the independent
// original-process policy capture, including omitted and null fields.
type metadataPolicyControl struct {
	testcontainers.Container
	fsid, filesystem string
}

func (c metadataPolicyControl) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	a := args[3:]
	var text string
	switch {
	case slices.Equal(a, []string{"fsid"}):
		text = c.fsid
	case len(a) == 5 && a[0] == "fs" && a[1] == "get":
		text = c.filesystem
	default:
		return 0, nil, fmt.Errorf("unexpected capture command %q", args)
	}
	var stream bytes.Buffer
	header := [8]byte{1}
	binary.BigEndian.PutUint32(header[4:], uint32(len(text)))
	stream.Write(header[:])
	stream.WriteString(text)
	return 0, bytes.NewReader(stream.Bytes()), nil
}

func TestCephFSOriginalProcessCaptureMetadataPoolZero(t *testing.T) {
	for _, side := range []int{0, 1} {
		for _, field := range []string{`"metadata_pool":0`, `"metadata_pool":7`, `"metadata_pool":-1`, `"metadata_pool":null`, ""} {
			t.Run(fmt.Sprintf("%d/%s", side, field), func(t *testing.T) {
				replies := [2]string{`{"mdsmap":{"metadata_pool":7}}`, `{"mdsmap":{"metadata_pool":9}}`}
				replies[side] = `{"mdsmap":{` + field + `}}`
				p := CephFSOriginalProcessPolicy{
					Source:        &ceph.Container{Container: metadataPolicyControl{fsid: "source-fsid", filesystem: replies[0]}},
					Destination:   &ceph.Container{Container: metadataPolicyControl{fsid: "destination-fsid", filesystem: replies[1]}},
					SourceFS:      &cephfs.Filesystem{FilesystemName: "source"},
					DestinationFS: &cephfs.Filesystem{FilesystemName: "destination"},
				}
				err := p.Capture(t.Context())
				valid := field == `"metadata_pool":0` || field == `"metadata_pool":7`
				if (err == nil) != valid {
					t.Fatalf("capture field %q: %v", field, err)
				}
				if field == `"metadata_pool":0` && p.Pools[side] != 0 {
					t.Fatalf("zero pool changed: %v", p.Pools)
				}
			})
		}
	}
}
