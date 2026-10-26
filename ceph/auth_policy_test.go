package ceph

import (
	"bytes"
	"strings"
	"testing"
)

func TestClientCapsReplaceWithoutChangingKeyOrKeepingOmittedServices(t *testing.T) {
	ctr := &authFixtureContainer{}
	cluster := authFixtureCluster(ctr)
	client, err := cluster.CreateClient(t.Context(), "reader", ClientCaps{Mon: "allow r"})
	if err != nil {
		t.Fatal(err)
	}
	cc, _ := client.ConnectionConfig()
	keyBefore := cc.Keyring
	want := ClientCaps{Mon: "allow r", OSD: "allow r pool=tenant"}
	if err := client.UpdateCaps(t.Context(), want); err != nil {
		t.Fatal(err)
	}
	got, err := client.Capabilities(t.Context())
	if err != nil || got != want {
		t.Fatalf("caps not replaced exactly: %+v %v", got, err)
	}
	cc2, _ := client.ConnectionConfig()
	keyAfter := cc2.Keyring
	if !bytes.Equal(keyBefore, keyAfter) {
		t.Fatal("caps update changed key")
	}
}

func TestClientCapsRejectForeignRevokedOrReplacedIdentities(t *testing.T) {
	ctr := &authFixtureContainer{}
	cluster := authFixtureCluster(ctr)
	client, err := cluster.CreateClient(t.Context(), "reader", ClientCaps{Mon: "allow r"})
	if err != nil {
		t.Fatal(err)
	}
	// A descriptor that names this identity but belongs to another cluster
	// acts on that cluster, which does not own the identity.
	other := authFixtureCluster(&authFixtureContainer{})
	foreign := *client
	foreign.owner = other
	if err := foreign.UpdateCaps(t.Context(), ClientCaps{Mon: "allow r"}); err == nil {
		t.Fatal("foreign identity edited")
	}
	before := len(ctr.calls)
	if err := client.UpdateCaps(t.Context(), ClientCaps{OSD: "allow *\n"}); err == nil || len(ctr.calls) != before {
		t.Fatal("invalid caps reached native command")
	}
	ctr.key = "EXTERNAL-KEY"
	if err := client.UpdateCaps(t.Context(), ClientCaps{Mon: "allow *"}); err == nil {
		t.Fatal("replaced identity edited")
	}
	ctr.key = "PRIVATE-KEY"
	if err := client.Delete(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Capabilities(t.Context()); err == nil {
		t.Fatal("revoked identity accepted")
	}
}

func TestClientCapsErrorsRedactNativeKeys(t *testing.T) {
	ctr := &authFixtureContainer{}
	cluster := authFixtureCluster(ctr)
	client, err := cluster.CreateClient(t.Context(), "reader", ClientCaps{Mon: "allow r"})
	if err != nil {
		t.Fatal(err)
	}
	ctr.fail = "auth caps client.reader mon allow r"
	if err := client.UpdateCaps(t.Context(), ClientCaps{Mon: "allow r"}); err == nil || strings.Contains(err.Error(), "PRIVATE-KEY") {
		t.Fatal("failure leaked secret or ignored error")
	}
}
