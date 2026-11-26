package fsbase_test

import (
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fstest"
)

func TestVolumeInfoUsagePresenceAtBothReaders(t *testing.T) {
	for _, test := range []struct {
		name, replacement string
		valid             bool
		used              int64
	}{
		{"missing", "", false, 0},
		{"null", `"bytes_used":null,`, false, 0},
		{"zero", `"bytes_used":0,`, true, 0},
		{"positive", `"bytes_used":8192,`, true, 8192},
		{"negative", `"bytes_used":-1,`, false, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs := fstest.NewFilesystem()
			for _, target := range []struct {
				key  string
				read func() (int64, error)
			}{
				{"fs subvolumegroup info fixture group --format json", func() (int64, error) {
					info, err := fs.Core.Volumes().SubvolumeGroupInfo(t.Context(), "group")
					if err != nil {
						return 0, err
					}
					return info.BytesUsed, nil
				}},
				{"fs subvolume info fixture volume --group_name group --format json", func() (int64, error) {
					info, err := fs.Core.Volumes().SubvolumeInfo(t.Context(), "volume", "group")
					if err != nil {
						return 0, err
					}
					return info.BytesUsed, nil
				}},
			} {
				original := fs.Control.Output[target.key]
				fs.Control.Output[target.key] = strings.Replace(original, `"bytes_used":0,`, test.replacement, 1)
				fs.Control.Output[target.key] = strings.Replace(fs.Control.Output[target.key], `"bytes_used":8192,`, test.replacement, 1)
				used, err := target.read()
				if (err == nil) != test.valid || (test.valid && used != test.used) {
					t.Fatalf("%s: used=%d error=%v", target.key, used, err)
				}
				fs.Control.Output[target.key] = original
			}
		})
	}
	fs := fstest.NewFilesystem()
	fs.Control.Output["fs subvolume info fixture volume --group_name group --format json"] = `{"state":"snapshot-retained","type":"subvolume"}`
	if info, err := fs.Core.Volumes().SubvolumeInfo(t.Context(), "volume", "group"); err != nil || info.State != "snapshot-retained" {
		t.Fatalf("reduced retained record: %+v %v", info, err)
	}
}
