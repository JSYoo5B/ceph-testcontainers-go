package multicluster

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

var _ testcontainers.Container = (*CephFSMirror)(nil)

func TestCephFSMirrorDirectoryPolicyValidation(t *testing.T) {
	base := CephFSMirrorConfig{SourceFilesystem: "source-fs", DestinationFilesystem: "backup-fs"}
	for _, directories := range [][]string{
		nil,
		{"relative"},
		{"/a", "/a/child"},
		{"/a/child", "/a"},
		{"/a", "/a/../a"},
		{"/", "/anywhere"},
		{"/valid", "/invalid\x00path"},
	} {
		config := base
		config.Directories = directories
		if _, err := normalizeCephFSMirrorConfig(config); err == nil {
			t.Errorf("expected ambiguous or invalid directory policy %q to fail", directories)
		}
	}
	config := base
	config.Directories = []string{"/first/../a", "/a-prefix", "/spaces allowed"}
	normalized, err := normalizeCephFSMirrorConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.DestinationSite != "destination" || !reflect.DeepEqual(normalized.Directories, []string{"/a", "/a-prefix", "/spaces allowed"}) {
		t.Fatalf("unexpected normalization: %+v", normalized)
	}
	if config.Directories[0] != "/first/../a" {
		t.Fatal("normalization changed the caller's directory slice")
	}
}

func TestCephFSMirrorIdentityValidation(t *testing.T) {
	valid := CephFSMirrorConfig{SourceFilesystem: "source-fs", DestinationFilesystem: "backup-fs", Directories: []string{"/"}}
	for name, configure := range map[string]func(*CephFSMirrorConfig){
		"empty source filesystem": func(c *CephFSMirrorConfig) { c.SourceFilesystem = "" },
		"filesystem slash":        func(c *CephFSMirrorConfig) { c.DestinationFilesystem = "backup/fs" },
		"filesystem whitespace":   func(c *CephFSMirrorConfig) { c.SourceFilesystem = "source fs" },
		"site separator":          func(c *CephFSMirrorConfig) { c.DestinationSite = "peer@site" },
		"site newline":            func(c *CephFSMirrorConfig) { c.DestinationSite = "site\n" },
	} {
		t.Run(name, func(t *testing.T) {
			config := valid
			configure(&config)
			if _, err := normalizeCephFSMirrorConfig(config); err == nil {
				t.Fatal("expected invalid CephFS identity to fail before allocating resources")
			}
		})
	}
}

func TestCephFSMirrorPendingImportIdentity(t *testing.T) {
	const serverUUID = "8d8c4729-3701-4f3f-8e9b-a527061bc449"
	expected := cephFSPeerIdentity{ClientName: "client.unique-peer", SiteName: "destination", FilesystemName: "backup-fs"}
	valid := map[string]json.RawMessage{serverUUID: json.RawMessage(`{"client_name":"client.unique-peer","site_name":"destination","fs_name":"backup-fs"}`)}
	if id, err := matchPendingCephFSPeer(valid, &expected); err != nil || id != serverUUID {
		t.Fatalf("matching pending import did not recover server UUID: id=%q error=%v", id, err)
	}
	for name, peer := range map[string]string{
		"foreign client":      `{"client_name":"client.somebody-else","site_name":"destination","fs_name":"backup-fs"}`,
		"foreign site":        `{"client_name":"client.unique-peer","site_name":"another-site","fs_name":"backup-fs"}`,
		"foreign filesystem":  `{"client_name":"client.unique-peer","site_name":"destination","fs_name":"other-fs"}`,
		"wrong status schema": `{"client_name":"client.unique-peer","cluster_name":"destination","fs_name":"backup-fs"}`,
		"missing identity":    `{}`,
		"malformed identity":  `{"client_name":42,"site_name":"destination","fs_name":"backup-fs"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if id, err := matchPendingCephFSPeer(map[string]json.RawMessage{serverUUID: json.RawMessage(peer)}, &expected); err == nil || id != "" {
				t.Fatalf("adopted unrelated or invalid peer: id=%q error=%v", id, err)
			}
		})
	}
	if _, err := matchPendingCephFSPeer(valid, nil); err == nil {
		t.Fatal("adopted existing peer without an owned pending import")
	}
	if _, err := matchPendingCephFSPeer(nil, &expected); err == nil {
		t.Fatal("recovered a peer absent from the server")
	}
	if _, err := matchPendingCephFSPeer(map[string]json.RawMessage{"not-a-server-uuid": valid[serverUUID]}, &expected); err == nil {
		t.Fatal("accepted invalid server UUID")
	}
	multiple := map[string]json.RawMessage{serverUUID: valid[serverUUID], "eaefb4ab-5011-419d-9420-ac7f71f11d73": valid[serverUUID]}
	if _, err := matchPendingCephFSPeer(multiple, &expected); err == nil {
		t.Fatal("adopted an ambiguous multiple-peer policy")
	}
	// The state transition must retain an uncertain import after mismatch, then
	// assign only the server UUID and clear the pending flag on exact recovery.
	mirror := &CephFSMirror{pendingPeerImport: &expected}
	if _, err := mirror.reconcilePendingPeer(map[string]json.RawMessage{serverUUID: json.RawMessage(`{"client_name":"client.foreign"}`)}); err == nil {
		t.Fatal("recovered a foreign peer")
	}
	if mirror.pendingPeerImport == nil || mirror.peerID != "" {
		t.Fatal("failed reconciliation changed peer ownership")
	}
	if _, err := mirror.reconcilePendingPeer(valid); err != nil {
		t.Fatal(err)
	}
	if mirror.peerID != serverUUID || mirror.pendingPeerImport != nil {
		t.Fatal("successful reconciliation did not settle peer ownership")
	}
}
