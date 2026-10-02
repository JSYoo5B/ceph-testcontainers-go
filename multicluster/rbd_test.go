package multicluster

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
)

func TestRunRBDMirrorRejectsMissingClusters(t *testing.T) {
	mirror, err := RunRBDMirror(context.Background(), "example/ceph:version", RBDMirrorConfig{Pool: "rbd"})
	if err == nil || mirror != nil {
		t.Fatalf("missing cluster pair must fail before resource allocation: mirror=%v error=%v", mirror, err)
	}
}

func TestRBDMirrorConfigDefaultsAndRejectsAmbiguousPool(t *testing.T) {
	config, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: " rbd "})
	if err != nil || config.Pool != "rbd" || config.SourceSite != "source" || config.DestinationSite != "destination" {
		t.Fatalf("unexpected normalized configuration: %+v error=%v", config, err)
	}
	for _, pool := range []string{"", " ", "-rbd", "rbd/namespace", "rbd images"} {
		t.Run(pool, func(t *testing.T) {
			if _, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: pool}); err == nil {
				t.Fatalf("ambiguous pool %q accepted", pool)
			}
		})
	}
	if _, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: "rbd", SourceSite: "same", DestinationSite: "same"}); err == nil {
		t.Fatal("identical source and destination site names accepted")
	}
}

func TestMatchingRBDMirrorPeerRestrictsDirectionChangesToSourceIdentity(t *testing.T) {
	for _, site := range []string{"source", "source-fsid"} {
		info := []byte(`{"peers":[{"uuid":"unrelated","direction":"tx-only","site_name":"other","mirror_uuid":"other-mirror"},{"uuid":"matching","direction":"tx-only","site_name":"` + site + `","mirror_uuid":"source-mirror"}]}`)
		peer, err := matchingRBDMirrorPeer(info, "source", "source-fsid", "source-mirror")
		if err != nil || peer.UUID != "matching" || peer.Direction != "tx-only" {
			t.Fatalf("site=%q: peer=%+v error=%v", site, peer, err)
		}
	}
	// A newly imported receiving peer has not necessarily learned the remote
	// mirror UUID yet. Its site identity was verified by bootstrap import.
	peer, err := matchingRBDMirrorPeer([]byte(`{"peers":[{"uuid":"new-peer","direction":"rx-only","site_name":"source"}]}`), "source", "source-fsid", "source-mirror")
	if err != nil || peer.UUID != "new-peer" || peer.Direction != "rx-only" {
		t.Fatalf("new peer=%+v error=%v", peer, err)
	}
}

func TestMatchingRBDMirrorPeerRejectsAmbiguousOrConflictingIdentity(t *testing.T) {
	for name, info := range map[string]string{
		"invalid JSON":      `{`,
		"missing peer":      `{"peers":[]}`,
		"unrelated site":    `{"peers":[{"uuid":"peer","direction":"tx-only","site_name":"other"}]}`,
		"missing UUID":      `{"peers":[{"direction":"tx-only","site_name":"source"}]}`,
		"conflicting UUID":  `{"peers":[{"uuid":"peer","direction":"tx-only","site_name":"source","mirror_uuid":"different"}]}`,
		"multiple matches":  `{"peers":[{"uuid":"peer-a","direction":"tx-only","site_name":"source"},{"uuid":"peer-b","direction":"tx-only","site_name":"source-fsid"}]}`,
		"unknown direction": `{"peers":[{"uuid":"peer","direction":"unknown","site_name":"source"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := matchingRBDMirrorPeer([]byte(info), "source", "source-fsid", "source-mirror"); err == nil {
				t.Fatal("ambiguous or conflicting peer identity accepted")
			}
		})
	}
}

func TestDecodeRBDBootstrapIdentityPreservesPeerClientWithoutSecretFields(t *testing.T) {
	const fsid = "58a9bc9a-dca0-4737-9844-a3289edfca81"
	token := base64.StdEncoding.EncodeToString([]byte(`{"fsid":"` + fsid + `","client_id":"custom-rbd-peer","key":"fixture-secret","mon_host":"v2:127.0.0.1:3300"}`))
	identity, err := decodeRBDBootstrapIdentity([]byte("\n" + token + "\n"))
	if err != nil || identity.FSID != fsid || identity.ClientName != "client.custom-rbd-peer" {
		t.Fatalf("unexpected bootstrap identity: %+v error=%v", identity, err)
	}
}

func TestDecodeRBDBootstrapIdentityRejectsInvalidIdentityWithoutExposingToken(t *testing.T) {
	for name, data := range map[string]string{
		"invalid JSON":   `{"key":"fixture-secret",`,
		"invalid FSID":   `{"fsid":"fixture-secret","client_id":"peer"}`,
		"missing client": `{"fsid":"58a9bc9a-dca0-4737-9844-a3289edfca81","key":"fixture-secret"}`,
		"invalid client": `{"fsid":"58a9bc9a-dca0-4737-9844-a3289edfca81","client_id":"-option","key":"fixture-secret"}`,
	} {
		t.Run(name, func(t *testing.T) {
			token := base64.StdEncoding.EncodeToString([]byte(data))
			if _, err := decodeRBDBootstrapIdentity([]byte(token)); err == nil || strings.Contains(err.Error(), "fixture-secret") || strings.Contains(err.Error(), token) {
				t.Fatalf("invalid identity was accepted or exposed: error=%v", err)
			}
		})
	}
	if _, err := decodeRBDBootstrapIdentity([]byte("not base64")); err == nil {
		t.Fatal("invalid token encoding accepted")
	}
}
