package mirror

import (
	"errors"
	"testing"
)

// The cases below are the inputs on which the former copies of each reader
// disagreed; every reader now gives one verdict for them.

func TestDecodeFilesystemIdentity(t *testing.T) {
	for _, c := range []struct {
		name, data string
		ok         bool
	}{
		{"complete", `{"id":1,"mdsmap":{"fs_name":"source","metadata_pool":7}}`, true},
		{"mirror info", `{"id":1,"mdsmap":{"fs_name":"source","metadata_pool":7},"mirror_info":{"peers":{}}}`, true},
		{"missing fs_name", `{"id":1,"mdsmap":{"metadata_pool":7}}`, false},
		{"other fs_name", `{"id":1,"mdsmap":{"fs_name":"other","metadata_pool":7}}`, false},
		{"zero id", `{"id":0,"mdsmap":{"fs_name":"source","metadata_pool":7}}`, false},
		{"missing pool", `{"id":1,"mdsmap":{"fs_name":"source"}}`, false},
		{"negative pool", `{"id":1,"mdsmap":{"fs_name":"source","metadata_pool":-1}}`, false},
		{"mirror info without peers", `{"id":1,"mdsmap":{"fs_name":"source","metadata_pool":7},"mirror_info":{}}`, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			identity, err := decodeFilesystemIdentity([]byte(c.data), "source")
			if c.ok != (err == nil) || (err != nil && !errors.Is(err, errCephFSObservationGuard)) {
				t.Fatalf("verdict %v: %v", c.ok, err)
			}
			if c.ok && (identity.id != 1 || identity.metadataPool != 7) {
				t.Fatalf("decoded %+v", identity)
			}
		})
	}
}

func TestDecodePeerList(t *testing.T) {
	entry := `{"client_name":"client.peer","site_name":"destination-site","fs_name":"destination"}`
	for _, c := range []struct {
		name, data string
		ok         bool
	}{
		{"canonical", `{"` + cephFSObserverDraftPeer + `":` + entry + `}`, true},
		{"empty", `{}`, true},
		{"braced key", `{"{` + cephFSObserverDraftPeer + `}":` + entry + `}`, false},
		{"urn key", `{"urn:uuid:` + cephFSObserverDraftPeer + `":` + entry + `}`, false},
		{"upper-case key", `{"02117353-8CD1-44DB-976B-EB20609AA160":` + entry + `}`, false},
		{"nil key", `{"00000000-0000-0000-0000-000000000000":` + entry + `}`, false},
		{"null", `null`, false},
		{"missing field", `{"` + cephFSObserverDraftPeer + `":{"client_name":"client.peer","site_name":"destination-site"}}`, false},
		{"session shape", `{"` + cephFSObserverDraftPeer + `":{"client_name":"client.peer","cluster_name":"destination-site","fs_name":"destination"}}`, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			peers, err := decodePeerList([]byte(c.data))
			if c.ok != (err == nil) || (err != nil && !errors.Is(err, errCephFSObservationGuard)) {
				t.Fatalf("verdict %v: %v", c.ok, err)
			}
			if c.name == "canonical" && peers[cephFSObserverDraftPeer] != (cephFSPeerIdentity{"client.peer", "destination-site", "destination"}) {
				t.Fatalf("decoded %+v", peers)
			}
		})
	}
}

func TestDecodeDaemonSession(t *testing.T) {
	peers := `{"` + cephFSObserverDraftPeer + `":{"remote":{"client_name":"client.peer","cluster_name":"destination-site","fs_name":"destination"}}}`
	for _, c := range []struct {
		name, data   string
		ok, failed   bool
		peerReported bool
	}{
		{"normal", `{"rados_inst":"172.20.0.8:0/1234","peers":` + peers + `,"snap_dirs":{"dir_count":1}}`, true, false, true},
		{"no peer", `{"rados_inst":"172.20.0.8:0/1234","peers":{},"snap_dirs":{"dir_count":0}}`, true, false, false},
		{"failed", `{"state":"failed"}`, true, true, false},
		{"blocklisted", `{"state":"blocklisted"}`, true, true, false},
		{"other state", `{"state":"starting"}`, false, false, false},
		{"missing snap_dirs", `{"rados_inst":"172.20.0.8:0/1234","peers":` + peers + `}`, false, false, false},
		{"missing dir_count", `{"rados_inst":"172.20.0.8:0/1234","peers":` + peers + `,"snap_dirs":{}}`, false, false, false},
		{"missing address", `{"peers":` + peers + `,"snap_dirs":{"dir_count":1}}`, false, false, false},
		{"missing peers", `{"rados_inst":"172.20.0.8:0/1234","snap_dirs":{"dir_count":1}}`, false, false, false},
		{"non-canonical peer", `{"rados_inst":"172.20.0.8:0/1234","peers":{"{` + cephFSObserverDraftPeer + `}":{"remote":{"client_name":"client.peer","cluster_name":"destination-site","fs_name":"destination"}}},"snap_dirs":{"dir_count":1}}`, false, false, false},
		{"peer without remote", `{"rados_inst":"172.20.0.8:0/1234","peers":{"` + cephFSObserverDraftPeer + `":{}},"snap_dirs":{"dir_count":1}}`, false, false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			report, err := decodeDaemonSession([]byte(c.data))
			if c.ok != (err == nil) || (err != nil && !errors.Is(err, errCephFSObservationGuard)) {
				t.Fatalf("verdict %v: %v", c.ok, err)
			}
			_, reported := report.peers[cephFSObserverDraftPeer]
			if report.failed != c.failed || reported != c.peerReported {
				t.Fatalf("decoded %+v", report)
			}
		})
	}
}

func TestDecodeDirectoryMapping(t *testing.T) {
	for _, c := range []struct {
		name, data, state string
		ok                bool
	}{
		{"mapped", `{"state":"mapped","instance_id":"4262"}`, "mapped", true},
		{"stalled", `{"state":"stalled"}`, "stalled", true},
		{"null", `{"state":null}`, "unassociated", true},
		{"unknown state", `{"state":"initializing"}`, "", false},
		{"missing state", `{"instance_id":"4262"}`, "", false},
		{"mapped without instance", `{"state":"mapped"}`, "", false},
		{"mapped to zero", `{"state":"mapped","instance_id":"0"}`, "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			mapping, err := decodeDirectoryMapping([]byte(c.data))
			if c.ok != (err == nil) || (err != nil && !errors.Is(err, errCephFSObservationGuard)) {
				t.Fatalf("verdict %v: %v", c.ok, err)
			}
			if c.ok && mapping.State != c.state {
				t.Fatalf("decoded %+v", mapping)
			}
		})
	}
}

// TestUnknownDirectoryStateEndsThePolicyIdleWait checks that a dirmap
// state no reader knows is refused at once by the idle-policy wait of a
// directory removal, as DirectoryStatus refuses it, instead of being waited
// on as a busy directory until the wait expires.
func TestUnknownDirectoryStateEndsThePolicyIdleWait(t *testing.T) {
	m, s, _, _, _ := newDirectoryRemovalFixture()
	s.mappingOverride = `{"state":"initializing"}`
	err := m.waitNativePolicyIdle(t.Context())
	if !errors.Is(err, errCephFSObservationGuard) {
		t.Fatalf("unknown state: %v", err)
	}
}
