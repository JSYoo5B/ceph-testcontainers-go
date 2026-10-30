package mirror

import "testing"

// A duplicate key inside a directory's stats entry used to reach map decoding,
// which keeps the last value. It is now refused like a duplicate path.
func TestDirectoryRemovalStatsRefuseNestedDuplicateKeys(t *testing.T) {
	const entry = `"state":"idle","last_synced_snap":{"id":120,"name":"checkpoint"},"snaps_synced":1,"snaps_deleted":0,"snaps_renamed":0`
	if present, err := decodeCephFSDirectoryRemovalStats([]byte(`{"/a":{`+entry+`}}`), "/a"); err != nil || !present {
		t.Fatalf("a valid entry was refused: %v", err)
	}
	for name, data := range map[string]string{
		"nested duplicate":  `{"/a":{"state":"syncing",` + entry + `}}`,
		"duplicate in snap": `{"/a":{` + entry + `,"current_sycning_snap":{"id":1,"id":2,"name":"x"}}}`,
		"duplicate path":    `{"/a":{` + entry + `},"/a":{` + entry + `}}`,
	} {
		if _, err := decodeCephFSDirectoryRemovalStats([]byte(data), "/a"); err == nil {
			t.Fatalf("%s passed the peer stats decoder", name)
		}
	}
}
