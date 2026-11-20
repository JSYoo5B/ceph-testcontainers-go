package mirror

import (
	"context"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// metadataCaptureControl supplies the pool catalog used by construction;
// filesystem and operation replies come from the existing lifecycle fixture.
type metadataCaptureControl struct {
	testcontainers.Container
	pools, filesystem string
}

func (c *metadataCaptureControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	a := args
	if len(a) > 3 && a[0] == "ceph" && a[1] == "--connect-timeout" {
		a = a[3:]
	}
	if slices.Equal(a, []string{"osd", "pool", "ls", "detail", "--format", "json"}) {
		return 0, cephFSObserverDraftStream(c.pools), nil
	}
	if c.filesystem != "" && len(a) >= 2 && slices.Equal(a[:2], []string{"fs", "get"}) {
		return 0, cephFSObserverDraftStream(c.filesystem), nil
	}
	return c.Container.Exec(ctx, args, opts...)
}

func captureMetadataFixture(m *Container, pool int64) (*metadataCaptureControl, *metadataCaptureControl) {
	source := &metadataCaptureControl{Container: m.source.Container, pools: fmt.Sprintf(`[{"pool_id":%d,"pool_name":"source-metadata"}]`, pool)}
	destination := &metadataCaptureControl{Container: m.destination.Container}
	m.source.Container, m.destination.Container = source, destination
	m.filesystemIdentityConfirmed = false
	return source, destination
}

func TestCephFSMirrorExplicitZeroMetadataIdentity(t *testing.T) {
	for _, side := range []string{"source", "destination"} {
		for _, operation := range []string{"addition", "directory removal", "peer removal", "observation"} {
			t.Run(side+"/"+operation, func(t *testing.T) {
				if operation == "observation" {
					m, source, destination, _, _ := newCephFSObserverDraft()
					if side == "source" {
						source.metadataPool = 0
					} else {
						destination.metadataPool = 0
					}
					captureMetadataFixture(m, source.metadataPool)
					if err := m.loadFilesystemIdentity(t.Context()); err != nil || !m.filesystemIdentityConfirmed {
						t.Fatal("capture", err)
					}
					status, err := m.DirectoryStatus(t.Context(), "/owned")
					if err != nil || !status.Ready {
						t.Fatalf("zero metadata pool observation: %+v %v", status, err)
					}
					return
				}
				m, source, destination, _, _ := newDirectoryRemovalFixture()
				m.ops.peerGeneration = 1
				if side == "source" {
					source.pool = 0
				} else {
					destination.pool = 0
				}
				captureMetadataFixture(m, source.pool)
				if err := m.loadFilesystemIdentity(t.Context()); err != nil || !m.filesystemIdentityConfirmed {
					t.Fatal("capture", err)
				}
				var err error
				switch operation {
				case "addition":
					_, err = m.beginDirectoryAddition(t.Context(), "/added", peerRemovalClusterReader)
				case "directory removal":
					_, err = m.beginDirectoryRemoval(t.Context(), "/owned", peerRemovalClusterReader)
				case "peer removal":
					_, err = m.beginPeerRemoval(t.Context(), m.peerID, peerRemovalClusterReader)
				}
				if err != nil {
					t.Fatalf("zero metadata pool %s: %v", operation, err)
				}
			})
		}
	}
}

func TestCephFSMirrorMetadataIdentityRequiresConfirmedFields(t *testing.T) {
	for _, scenario := range []string{"source missing", "source null", "source negative", "destination missing", "destination null", "destination negative", "pool catalog missing ID", "pool catalog null ID"} {
		t.Run(scenario, func(t *testing.T) {
			m, _, _, _, _ := newDirectoryRemovalFixture()
			source, destination := captureMetadataFixture(m, 0)
			source.filesystem = `{"id":1,"mdsmap":{"metadata_pool":0}}`
			switch scenario {
			case "source missing":
				source.filesystem = `{"id":1,"mdsmap":{}}`
			case "source null":
				source.filesystem = `{"id":1,"mdsmap":{"metadata_pool":null}}`
			case "source negative":
				source.filesystem = `{"id":1,"mdsmap":{"metadata_pool":-1}}`
			case "destination missing":
				destination.filesystem = `{"id":2,"mdsmap":{}}`
			case "destination null":
				destination.filesystem = `{"id":2,"mdsmap":{"metadata_pool":null}}`
			case "destination negative":
				destination.filesystem = `{"id":2,"mdsmap":{"metadata_pool":-1}}`
			case "pool catalog missing ID":
				source.pools = `[{"pool_name":"source-metadata"}]`
			case "pool catalog null ID":
				source.pools = `[{"pool_id":null,"pool_name":"source-metadata"}]`
			}
			m.filesystemIdentityConfirmed = true
			if err := m.loadFilesystemIdentity(t.Context()); err == nil || m.filesystemIdentityConfirmed {
				t.Fatal("failed recapture retained authority", err)
			}
			if _, err := m.captureOriginalScope(t.Context(), "test", m.peerID, peerRemovalClusterReader); err == nil {
				t.Fatal("unconfirmed metadata authorized a tracked operation")
			}
		})
	}
	for _, operation := range []string{"addition", "directory removal", "peer removal"} {
		t.Run("unconfirmed/"+operation, func(t *testing.T) {
			m, source, _, _, _ := newDirectoryRemovalFixture()
			m.ops.peerGeneration = 1
			m.filesystemIdentityConfirmed, m.metadataPoolID, m.destinationMetadataPoolID = false, 0, 0
			var err error
			switch operation {
			case "addition":
				_, err = m.beginDirectoryAddition(t.Context(), "/added", peerRemovalClusterReader)
			case "directory removal":
				_, err = m.beginDirectoryRemoval(t.Context(), "/owned", peerRemovalClusterReader)
			case "peer removal":
				_, err = m.beginPeerRemoval(t.Context(), m.peerID, peerRemovalClusterReader)
			}
			if err == nil || source.adds != 0 || source.removals != 0 {
				t.Fatal("unconfirmed identity dispatched a mutation", err)
			}
		})
	}
}
