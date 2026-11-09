package mirror

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// umbrellaPeerStatus nests a flat Squid/Tentacle peer status as Umbrella
// 21.2.0 prints it for cephFSObserverDraftPeer.
func umbrellaPeerStatus(flat string) string {
	var entries map[string]json.RawMessage
	if err := json.Unmarshal([]byte(flat), &entries); err != nil {
		panic(err)
	}
	nested := map[string]any{}
	for directory, entry := range entries {
		nested[directory] = map[string]any{"peer": map[string]json.RawMessage{cephFSObserverDraftPeer: entry}}
	}
	data, err := json.Marshal(map[string]any{"metrics": nested})
	if err != nil {
		panic(err)
	}
	return string(data)
}

func TestCephFSPeerStatusDirectoriesReadsBothNativeShapes(t *testing.T) {
	// The reply of a 21.2.0 cephfs-mirror after one synced snapshot.
	umbrella := fmt.Sprintf(`{"metrics":{"/federation":{"peer":{%q:{"state":"idle","last_synced_snap":{"id":2,"name":"backup-1","crawl_duration":"0s","datasync_queue_wait_duration":"0s","sync_duration":"0s","sync_time_stamp":"2026-11-09T00:45:35.026520+0000","sync_bytes":"128.09 KiB","sync_files":6},"snaps_synced":1,"snaps_deleted":0,"snaps_renamed":0}}}}}`, cephFSObserverDraftPeer)
	// The reply of a 20.2.4 cephfs-mirror at the same point.
	tentacle := `{"/federation":{"state":"idle","last_synced_snap":{"id":2,"name":"backup-1","sync_duration":0,"sync_time_stamp":"1928633.881435s","sync_bytes":131165},"snaps_synced":1,"snaps_deleted":0,"snaps_renamed":0}}`
	for name, data := range map[string]string{"umbrella": umbrella, "tentacle": tentacle} {
		flat, err := cephFSPeerStatusDirectories([]byte(data), cephFSObserverDraftPeer)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		replay, present, err := decodeCephFSObservedReplay(flat, "/federation")
		if err != nil || !present || replay.State != "idle" || replay.Last == nil || *replay.Last != (Snapshot{ID: 2, Name: "backup-1"}) || replay.Synced != 1 {
			t.Fatalf("%s: %+v present=%t %v", name, replay, present, err)
		}
	}
	if flat, err := cephFSPeerStatusDirectories([]byte(tentacle), cephFSObserverDraftPeer); err != nil || string(flat) != tentacle {
		t.Fatalf("the flat shape changed: %s %v", flat, err)
	}
	for _, empty := range []string{`{}`, `{"metrics":{}}`} {
		flat, err := cephFSPeerStatusDirectories([]byte(empty), cephFSObserverDraftPeer)
		var entries map[string]json.RawMessage
		if err != nil || json.Unmarshal(flat, &entries) != nil || !reflect.DeepEqual(entries, map[string]json.RawMessage{}) {
			t.Fatalf("%s: %s %v", empty, flat, err)
		}
	}
}

func TestCephFSPeerStatusDirectoriesRefusesAnAmbiguousNesting(t *testing.T) {
	entry := `{"state":"idle","snaps_synced":0,"snaps_deleted":0,"snaps_renamed":0}`
	for name, data := range map[string]string{
		"metrics beside a directory": `{"metrics":{},"/owned":` + entry + `}`,
		"metrics not an object":      `{"metrics":[]}`,
		"metrics null":               `{"metrics":null}`,
		"directory without peer":     `{"metrics":{"/owned":` + entry + `}}`,
		"directory extra key":        `{"metrics":{"/owned":{"rate":1,"peer":{"` + cephFSObserverDraftPeer + `":` + entry + `}}}}`,
		"another peer":               `{"metrics":{"/owned":{"peer":{"another":` + entry + `}}}}`,
		"two peers":                  `{"metrics":{"/owned":{"peer":{"another":` + entry + `,"` + cephFSObserverDraftPeer + `":` + entry + `}}}}`,
		"no peer":                    `{"metrics":{"/owned":{"peer":{}}}}`,
		"duplicate directory":        `{"metrics":{"/owned":{"peer":{}},"/owned":{"peer":{}}}}`,
		"duplicate flat directory":   `{"/owned":` + entry + `,"/owned":` + entry + `}`,
		"trailing":                   `{"metrics":{}} {}`,
		"null":                       `null`,
	} {
		if flat, err := cephFSPeerStatusDirectories([]byte(data), cephFSObserverDraftPeer); err == nil || !strings.Contains(err.Error(), "CephFS") {
			t.Errorf("%s: accepted as %s (%v)", name, flat, err)
		}
	}
}

// The directory observation reads Umbrella's nested reply as the flat one.
func TestCephFSObserverDraftReadsUmbrellaPeerStatus(t *testing.T) {
	mirror, _, _, a, _ := newCephFSObserverDraft()
	a.replay = umbrellaPeerStatus(`{"/owned":{"state":"idle","last_synced_snap":{"id":120,"name":"checkpoint"},"snaps_synced":1,"snaps_deleted":0,"snaps_renamed":0}}`)
	status, err := mirror.DirectoryStatus(t.Context(), "/owned")
	if err != nil || !status.Ready || status.State != "idle" || status.LastSyncedSnapshot == nil || *status.LastSyncedSnapshot != (Snapshot{ID: 120, Name: "checkpoint"}) || status.SnapshotsSynced != 1 {
		t.Fatalf("Umbrella directory status: %+v %v", status, err)
	}
	a.replay = `{"metrics":{"/owned":{"peer":{"another":{"state":"idle","snaps_synced":1,"snaps_deleted":0,"snaps_renamed":0}}}}}`
	if status, err := mirror.DirectoryStatus(t.Context(), "/owned"); err == nil || status.Ready {
		t.Fatalf("another peer's entry was read as this peer's: %+v %v", status, err)
	}
}
