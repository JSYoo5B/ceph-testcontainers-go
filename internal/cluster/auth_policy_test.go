package cluster

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
	_, keyBefore, _ := client.ConnectionConfig()
	want := ClientCaps{Mon: "allow r", OSD: "allow r pool=tenant"}
	if err := cluster.UpdateClientCaps(t.Context(), client, want); err != nil {
		t.Fatal(err)
	}
	got, err := cluster.ClientCapabilities(t.Context(), client)
	if err != nil || got != want {
		t.Fatalf("caps not replaced exactly: %+v %v", got, err)
	}
	_, keyAfter, _ := client.ConnectionConfig()
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
	other := authFixtureCluster(&authFixtureContainer{})
	if err := other.UpdateClientCaps(t.Context(), client, ClientCaps{Mon: "allow r"}); err == nil {
		t.Fatal("foreign identity edited")
	}
	before := len(ctr.calls)
	if err := cluster.UpdateClientCaps(t.Context(), client, ClientCaps{OSD: "allow *\n"}); err == nil || len(ctr.calls) != before {
		t.Fatal("invalid caps reached native command")
	}
	ctr.key = "EXTERNAL-KEY"
	if err := cluster.UpdateClientCaps(t.Context(), client, ClientCaps{Mon: "allow *"}); err == nil {
		t.Fatal("replaced identity edited")
	}
	ctr.key = "PRIVATE-KEY"
	if err := cluster.DeleteClient(t.Context(), client); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.ClientCapabilities(t.Context(), client); err == nil {
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
	if err := cluster.UpdateClientCaps(t.Context(), client, ClientCaps{Mon: "allow r"}); err == nil || strings.Contains(err.Error(), "PRIVATE-KEY") {
		t.Fatal("failure leaked secret or ignored error")
	}
}
