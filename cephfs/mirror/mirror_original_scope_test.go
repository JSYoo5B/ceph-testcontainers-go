package mirror

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// TestOriginalScopeRefusesEveryEditedIdentity edits each exported descriptor
// and each field of the private pair after a scope captured them, and
// requires sameFixture to refuse the fixture.
func TestOriginalScopeRefusesEveryEditedIdentity(t *testing.T) {
	descriptors := map[string]func(*Container){
		"SourceFilesystem":        func(m *Container) { m.SourceFilesystem = "other" },
		"DestinationFilesystem":   func(m *Container) { m.DestinationFilesystem = "other" },
		"SourceClientEntity":      func(m *Container) { m.SourceClientEntity = "client.other" },
		"DestinationClientEntity": func(m *Container) { m.DestinationClientEntity = "client.other" },
	}
	pairFields := map[string]func(*cephFSMirrorPair){
		"source":                    func(p *cephFSMirrorPair) { p.source = &ceph.Container{} },
		"destination":               func(p *cephFSMirrorPair) { p.destination = &ceph.Container{} },
		"sourceFilesystem":          func(p *cephFSMirrorPair) { p.sourceFilesystem = "other" },
		"destinationFilesystem":     func(p *cephFSMirrorPair) { p.destinationFilesystem = "other" },
		"sourceFilesystemID":        func(p *cephFSMirrorPair) { p.sourceFilesystemID++ },
		"destinationFilesystemID":   func(p *cephFSMirrorPair) { p.destinationFilesystemID++ },
		"metadataPool":              func(p *cephFSMirrorPair) { p.metadataPool = "other" },
		"sourceMetadataPoolID":      func(p *cephFSMirrorPair) { p.sourceMetadataPoolID++ },
		"destinationMetadataPoolID": func(p *cephFSMirrorPair) { p.destinationMetadataPoolID++ },
		"identityConfirmed":         func(p *cephFSMirrorPair) { p.identityConfirmed = false },
		"sourceClient":              func(p *cephFSMirrorPair) { p.sourceClient = "client.other" },
		"destinationClient":         func(p *cephFSMirrorPair) { p.destinationClient = "client.other" },
		"destinationSite":           func(p *cephFSMirrorPair) { p.destinationSite = "other" },
	}
	pairType := reflect.TypeFor[cephFSMirrorPair]()
	for i := range pairType.NumField() {
		if pairFields[pairType.Field(i).Name] == nil {
			t.Fatalf("pair field %s has no edit case", pairType.Field(i).Name)
		}
	}
	capture := func(t *testing.T) (*Container, *cephFSOriginalScope) {
		m, _, _, _, _ := newPeerRemovalFixture()
		scope, err := m.captureOriginalScope(t.Context(), "test", m.peer.id, peerRemovalClusterReader)
		if err != nil {
			t.Fatal(err)
		}
		if !scope.sameFixture(false) {
			t.Fatal("unchanged fixture refused")
		}
		return m, scope
	}
	for name, edit := range descriptors {
		t.Run(name, func(t *testing.T) {
			m, scope := capture(t)
			edit(m)
			if scope.sameFixture(false) || scope.sameFixture(true) {
				t.Fatalf("edited descriptor %s accepted", name)
			}
		})
	}
	for name, edit := range pairFields {
		t.Run("pair."+name, func(t *testing.T) {
			m, scope := capture(t)
			edit(&m.pair)
			if scope.sameFixture(false) || scope.sameFixture(true) {
				t.Fatalf("edited pair field %s accepted", name)
			}
		})
	}
	t.Run("peer", func(t *testing.T) {
		m, scope := capture(t)
		m.peer.id = peerRemovalNewPeer
		if scope.sameFixture(false) || scope.sameFixture(true) {
			t.Fatal("replaced peer accepted")
		}
		m.peer.id = ""
		if scope.sameFixture(false) || !scope.sameFixture(true) {
			t.Fatal("cleared peer: only a peer removal may accept it")
		}
	})
}

// TestPeerSelectorsRefuseAnEditedSourceFilesystem edits the exported
// SourceFilesystem and requires RemovePeer and PeerIDs to refuse before any
// peer command reaches the source.
func TestPeerSelectorsRefuseAnEditedSourceFilesystem(t *testing.T) {
	for _, operation := range []string{"RemovePeer", "PeerIDs"} {
		t.Run(operation, func(t *testing.T) {
			m, s, _, _, _ := newPeerRemovalFixture()
			m.SourceFilesystem = "other"
			var err error
			if operation == "RemovePeer" {
				err = m.RemovePeer(t.Context(), m.peer.id)
			} else {
				_, err = m.PeerIDs(t.Context())
			}
			if !errors.Is(err, errCephFSObservationGuard) {
				t.Fatalf("%s with an edited descriptor: %v", operation, err)
			}
			for _, call := range s.calls {
				if slices.Contains(call, "peer_remove") || slices.Contains(call, "peer_list") {
					t.Fatalf("%s sent %q", operation, call)
				}
			}
			if m.peer.id != cephFSObserverDraftPeer {
				t.Fatalf("%s forgot the owned peer", operation)
			}
		})
	}
}
