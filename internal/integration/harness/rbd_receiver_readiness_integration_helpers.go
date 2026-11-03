//go:build all || (integration && multicluster)

package harness

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"

	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/testcontainers/testcontainers-go"
)

type RBDReceiverIndependentIdentity struct{ mirrorUUID, PeerUUID, Site, Client, remoteMirrorUUID string }

func RBDReceiverNativeIdentity(t *testing.T, ctx context.Context, client testcontainers.Container, pool string) RBDReceiverIndependentIdentity {
	t.Helper()
	var info struct {
		MirrorUUID *string `json:"mirror_uuid"`
		Peers      []struct {
			UUID             *string `json:"uuid"`
			Site             *string `json:"site_name"`
			Client           *string `json:"client_name"`
			Direction        *string `json:"direction"`
			RemoteMirrorUUID *string `json:"mirror_uuid"`
		} `json:"peers"`
	}
	data, err := RBDMirrorPoolInfo(ctx, client, pool, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &info); err != nil || info.MirrorUUID == nil || *info.MirrorUUID == "" || len(info.Peers) != 1 {
		t.Fatalf("independent receiving pool/peer identity missing: %+v %v", info, err)
	}
	p := info.Peers[0]
	if p.UUID == nil || *p.UUID == "" || p.Site == nil || *p.Site == "" || p.Client == nil || *p.Client == "" || p.RemoteMirrorUUID == nil || p.Direction == nil || !slices.Contains([]string{"rx-only", "rx-tx"}, *p.Direction) {
		t.Fatal("independent receiving peer identity/receiving fields missing or invalid")
	}
	return RBDReceiverIndependentIdentity{*info.MirrorUUID, *p.UUID, *p.Site, *p.Client, *p.RemoteMirrorUUID}
}

func RBDReceiverNativePeerMatches(t *testing.T, current, original RBDReceiverIndependentIdentity, sourceMirrorUUID string) {
	t.Helper()
	if current.remoteMirrorUUID != "" && current.remoteMirrorUUID != sourceMirrorUUID || original.remoteMirrorUUID != "" && original.remoteMirrorUUID != sourceMirrorUUID {
		t.Fatal("independent receiving remote mirror UUID contradicts original source base UUID")
	}
	// An explicitly present empty UUID is async learning, not a new identity.
	current.remoteMirrorUUID = original.remoteMirrorUUID
	if current != original {
		t.Fatalf("independent original native receiving tuple changed: before=%+v after=%+v", original, current)
	}
}

type RBDReceiverIndependentPolicy struct{ Mode, MirrorUUID, Remote, site string }

func RBDReceiverNativePolicy(t *testing.T, ctx context.Context, client testcontainers.Container, pool, namespace string) RBDReceiverIndependentPolicy {
	t.Helper()
	var native struct {
		Mode       *string `json:"mode"`
		MirrorUUID *string `json:"mirror_uuid"`
		Remote     *string `json:"remote_namespace"`
		Site       *string `json:"site_name"`
	}
	data, err := RBDMirrorPoolInfo(ctx, client, pool, namespace)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &native); err != nil || native.Mode == nil || !slices.Contains([]string{"init-only", "image", "pool"}, *native.Mode) || native.MirrorUUID == nil || *native.MirrorUUID == "" || native.Remote == nil || namespace == "" && (native.Site == nil || *native.Site == "") {
		t.Fatalf("independent native original policy identity missing/invalid: namespace=%q error=%v", namespace, err)
	}
	site := ""
	if native.Site != nil {
		site = *native.Site
	}
	return RBDReceiverIndependentPolicy{*native.Mode, *native.MirrorUUID, *native.Remote, site}
}

func RBDReceiverNativeElection(t *testing.T, ctx context.Context, link *rbdmirror.Mirror, status rbdmirror.ReceiverStatus, identity RBDReceiverIndependentIdentity, mapping [2]string) {
	t.Helper()
	peer := "uuid: " + identity.PeerUUID + " cluster: " + identity.Site + " client: " + identity.Client
	var actualIDs, leaderMembers []string
	var leaderID, leaderName string
	for _, daemon := range link.Daemons() {
		if !slices.Contains(status.ExpectedDaemons, daemon.DaemonName) {
			continue
		}
		code, data, err := RBDMultiClusterExec(ctx, daemon.Container, "ceph", "--admin-daemon", "/tmp/rbd-mirror.asok", "rbd", "mirror", "status")
		if err != nil || code != 0 {
			t.Fatalf("fresh native socket query: exit=%d error=%v", code, err)
		}
		type namespaceReplayer struct {
			Local  *string           `json:"local_namespace"`
			Remote *string           `json:"remote_namespace"`
			Images []json.RawMessage `json:"image_replayers"`
		}
		var native struct {
			Pools []struct {
				Pool         string              `json:"pool"`
				Peer         string              `json:"peer"`
				State        string              `json:"state"`
				Instance     string              `json:"instance_id"`
				LeaderID     string              `json:"leader_instance_id"`
				Leader       bool                `json:"leader"`
				Members      []string            `json:"instances"`
				Namespaces   []namespaceReplayer `json:"namespace_replayers"`
				LegacyImages []json.RawMessage   `json:"image_replayers"`
				LegacyNames  []struct {
					Name   *string           `json:"name"`
					Images []json.RawMessage `json:"image_replayers"`
				} `json:"namespaces"`
			} `json:"pool_replayers"`
		}
		if err := json.Unmarshal(data, &native); err != nil {
			t.Fatal(err)
		}
		selected := 0
		for _, row := range native.Pools {
			if row.Pool != status.Pool || row.Peer != peer {
				continue
			}
			selected++
			// Ceph 19 reports the default namespace's image replayers on the
			// pool row and each named namespace as {name, image_replayers}.
			if row.Namespaces == nil && row.LegacyImages != nil && row.LegacyNames != nil {
				defaultNamespace := ""
				row.Namespaces = []namespaceReplayer{{&defaultNamespace, &defaultNamespace, row.LegacyImages}}
				for _, ns := range row.LegacyNames {
					row.Namespaces = append(row.Namespaces, namespaceReplayer{ns.Name, ns.Name, ns.Images})
				}
			}
			foundNamespace := false
			for _, ns := range row.Namespaces {
				if ns.Local == nil || ns.Remote == nil || ns.Images == nil {
					t.Fatal("independent namespace identity fields missing/null")
				}
				if *ns.Local == mapping[1] && *ns.Remote == mapping[0] {
					foundNamespace = true
				}
			}
			observed := status.Daemons[daemon.DaemonName]
			if row.State != "running" || !foundNamespace || row.Instance == "" || observed.InstanceID != row.Instance || observed.Leader != row.Leader || observed.LeaderInstanceID != row.LeaderID || observed.ContainerID != daemon.GetContainerID() {
				t.Fatalf("public observation differs from independent socket: report=%+v native=%+v", observed, row)
			}
			actualIDs = append(actualIDs, row.Instance)
			if row.Leader {
				if leaderName != "" {
					t.Fatal("independent socket saw multiple leaders")
				}
				leaderName, leaderID, leaderMembers = daemon.DaemonName, row.Instance, slices.Clone(row.Members)
			}
		}
		if selected != 1 {
			t.Fatal("independent native scope was ambiguous or missing")
		}
	}
	slices.Sort(actualIDs)
	slices.Sort(leaderMembers)
	if leaderName != status.LeaderDaemonName || leaderID != status.LeaderInstanceID || !slices.Equal(actualIDs, leaderMembers) || !slices.Equal(leaderMembers, status.Instances) || len(slices.Compact(slices.Clone(actualIDs))) != len(status.ExpectedDaemons) {
		t.Fatalf("independent exact cohort differs: leader=%s/%s members=%v public=%+v", leaderName, leaderID, leaderMembers, status)
	}
	if strings.TrimSpace(peer) != peer {
		t.Fatal("independent peer identity unexpectedly padded")
	}
}

func RBDReceiverNativeOriginalClusterPool(t *testing.T, ctx context.Context, client testcontainers.Container, pool, fsid string, poolID int64) {
	t.Helper()
	code, data, err := RBDMultiClusterExec(ctx, client, "ceph", "fsid")
	if err != nil || code != 0 || strings.TrimSpace(string(data)) != fsid {
		t.Fatalf("independent original FSID differs: exit=%d error=%v", code, err)
	}
	code, data, err = RBDMultiClusterExec(ctx, client, "ceph", "osd", "pool", "ls", "detail", "--format", "json")
	var pools []struct {
		Name string `json:"pool_name"`
		ID   int64  `json:"pool_id"`
		Type int    `json:"type"`
	}
	if err != nil || code != 0 || json.Unmarshal(data, &pools) != nil || pools == nil {
		t.Fatalf("independent native pool query failed: exit=%d error=%v", code, err)
	}
	found := 0
	for _, current := range pools {
		if current.Name == pool {
			found++
			if current.ID != poolID || current.Type != 1 {
				t.Fatal("independent original replicated pool identity changed")
			}
		}
	}
	if found != 1 {
		t.Fatal("independent original pool missing/ambiguous")
	}
}

func RBDReceiverNativeSnapshot(t *testing.T, ctx context.Context, client testcontainers.Container, pool, namespace, image string) uint64 {
	t.Helper()
	spec := RBDScopeImage(pool, namespace, image)
	output := strings.TrimSpace(string(RBDOutput(t, ctx, client, "mirror", "image", "snapshot", spec)))
	value, valid := strings.CutPrefix(output, "Snapshot ID: ")
	id, err := strconv.ParseUint(value, 10, 64)
	if !valid || err != nil || id == 0 || strconv.FormatUint(id, 10) != value {
		t.Fatalf("explicit native mirror checkpoint ID invalid: %q", output)
	}
	var status struct {
		Snapshots []struct {
			ID *uint64 `json:"id"`
		} `json:"snapshots"`
	}
	if err := json.Unmarshal(RBDOutput(t, ctx, client, "mirror", "image", "status", spec, "--format", "json"), &status); err != nil {
		t.Fatal(err)
	}
	for _, snap := range status.Snapshots {
		if snap.ID != nil && *snap.ID == id {
			return id
		}
	}
	t.Fatal("explicit checkpoint ID absent from independent source mirror status")
	return 0
}
