package cephfs

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fstest"
)

// Every FSMap reader decodes through fsbase.DecodeFSMap, so a reply without
// the filesystems or the standbys list gets the same verdict from each of
// them, and a complete map is accepted by each.
func TestFSMapReadersShareOnePresenceRule(t *testing.T) {
	const valid = `{"filesystems":[{"id":41,"mdsmap":{"fs_name":"fixture","max_mds":1,"metadata_pool":1,"data_pools":[2],"info":{}}}],"standbys":[]}`
	readers := map[string]func(t *testing.T, data string) error{
		"DecodeFSMap": func(_ *testing.T, data string) error {
			_, err := fsbase.DecodeFSMap([]byte(data))
			return err
		},
		"ReadNativePools": func(t *testing.T, data string) error {
			fs := fstest.NewFilesystem()
			fs.Control.Output["fs dump --format json"] = data
			_, err := fs.Core.ReadNativePools(t.Context())
			return err
		},
		"readExistingFilesystems": func(t *testing.T, data string) error {
			fs := fstest.NewFilesystem()
			fs.Control.Output["fs dump --format json"] = data
			_, err := readExistingFilesystems(t.Context(), fs.Core.Cluster(), "new")
			return err
		},
		"MDSStatus": func(_ *testing.T, data string) error {
			_, err := decodeCephFSMDSTopology([]byte(data), "fixture", nil)
			return err
		},
		"retirement absence": func(_ *testing.T, data string) error {
			_, err := cephFSMDSAbsent([]byte(data), "fixture-0", 7)
			return err
		},
		"removal survivor affinity": func(_ *testing.T, data string) error {
			_, err := survivorAffinityLost([]byte(data), nil)
			return err
		},
	}
	for name, test := range map[string]struct {
		data   string
		refuse bool
	}{
		"empty object":     {data: `{}`, refuse: true},
		"null filesystems": {data: `{"filesystems":null,"standbys":[]}`, refuse: true},
		"missing standbys": {data: `{"filesystems":[{"id":41,"mdsmap":{"fs_name":"fixture","max_mds":1,"metadata_pool":1,"data_pools":[2],"info":{}}}]}`, refuse: true},
		"valid":            {data: valid},
	} {
		for reader, read := range readers {
			t.Run(name+"/"+reader, func(t *testing.T) {
				if err := read(t, test.data); (err != nil) != test.refuse {
					t.Fatalf("%s of %s: %v, want refused=%t", reader, name, err, test.refuse)
				}
			})
		}
	}
}
