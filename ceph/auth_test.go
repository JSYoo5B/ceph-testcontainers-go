package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestClientValidationBeforeCephCommands(t *testing.T) {
	for _, name := range []string{"", "client.", "--admin", "has space", "a/b", "x\n[client.admin]", strings.Repeat("a", 129)} {
		ctr := &authFixtureContainer{}
		if _, err := authFixtureCluster(ctr).CreateClient(t.Context(), name, ClientCaps{Mon: "allow r"}); err == nil || len(ctr.calls) != 0 {
			t.Fatalf("invalid name reached Ceph: %q", name)
		}
	}
	ctr := &authFixtureContainer{}
	if _, err := authFixtureCluster(ctr).CreateClient(t.Context(), "reader", ClientCaps{OSD: "allow r\nallow *"}); err == nil || len(ctr.calls) != 0 {
		t.Fatal("control characters reached the capabilities command")
	}
}

func TestCreateClientRejectsExistingWithoutMutation(t *testing.T) {
	ctr := &authFixtureContainer{listing: `{"auth_dump":[{"entity":"client.reader","key":"DO-NOT-LOG"}]}`}
	client, err := authFixtureCluster(ctr).CreateClient(t.Context(), "client.reader", ClientCaps{Mon: "allow r", OSD: "allow *"})
	if err == nil || client != nil || len(ctr.calls) != 1 || !slices.Contains(ctr.calls[0], "ls") || strings.Contains(err.Error(), "DO-NOT-LOG") {
		t.Fatalf("existing identity was accepted or modified: calls=%v", ctr.calls)
	}
}

func TestClientConfigCopiesCredentialsAndUsesCurrentMonitorConfig(t *testing.T) {
	ctr := &authFixtureContainer{}
	cluster := authFixtureCluster(ctr)
	client, err := cluster.CreateClient(t.Context(), "reader", ClientCaps{Mon: "allow r", OSD: "allow r pool=tenant namespace=blue"})
	if err != nil {
		t.Fatal(err)
	}
	if client.Name() != "client.reader" || client.User() != "reader" || client.KeyringPath() != "/etc/ceph/ceph.client.reader.keyring" {
		t.Fatal("client identity naming is inconsistent")
	}
	config, keyring, err := client.ConnectionConfig()
	if err != nil || !bytes.Contains(config, []byte("[client.reader]\nkeyring = "+client.KeyringPath())) {
		t.Fatalf("missing explicit client keyring config: %v", err)
	}
	config[0], keyring[0] = '!', '!'
	againConfig, againKeyring, _ := client.ConnectionConfig()
	if againConfig[0] == '!' || againKeyring[0] == '!' {
		t.Fatal("connection configuration aliases private memory")
	}
	for _, format := range []string{"%v", "%+v", "%#v"} {
		for _, descriptor := range []any{client, *client} {
			if fmt.Sprintf(format, descriptor) != "Cephx client client.reader" {
				t.Fatal("formatting descriptor did not redact its private fields")
			}
		}
	}
	cluster.config = []byte("[global]\nmon_host = new-mon\n")
	var request testcontainers.GenericContainerRequest
	if err := cluster.WithClientIdentity(client)(&request); err != nil {
		t.Fatal(err)
	}
	if len(request.Files) != 2 {
		t.Fatal("identity attachment included unexpected credentials")
	}
	for _, file := range request.Files {
		data, err := io.ReadAll(file.Reader)
		if err != nil {
			t.Fatal(err)
		}
		if file.ContainerFilePath == "/etc/ceph/ceph.conf" && !bytes.Contains(data, []byte("new-mon")) {
			t.Fatal("identity attachment used stale MON addresses")
		}
		if file.ContainerFilePath == client.KeyringPath() && file.FileMode != 0o600 {
			t.Fatal("keyring was not private")
		}
		if file.ContainerFilePath == "/etc/ceph/ceph.client.admin.keyring" {
			t.Fatal("restricted client received an admin keyring")
		}
	}
}

func TestClientFailurePreservesDescriptorAndRedactsOutput(t *testing.T) {
	for _, failure := range []string{"auth add client.reader mon allow r", "auth get client.reader"} {
		ctr := &authFixtureContainer{fail: failure}
		client, err := authFixtureCluster(ctr).CreateClient(t.Context(), "reader", ClientCaps{Mon: "allow r"})
		if err == nil || client == nil || client.Name() != "client.reader" || strings.Contains(err.Error(), "PRIVATE-KEY") {
			t.Fatalf("creation failure lost identity or leaked output: err=%v", err)
		}
		if _, _, err := client.ConnectionConfig(); err == nil {
			t.Fatal("partial identity returned usable credentials")
		}
		for _, call := range ctr.calls {
			if slices.Contains(call, "del") || slices.Contains(call, "caps") {
				t.Fatalf("partial creation changed or deleted an identity: %v", call)
			}
		}
	}
}

func TestDeleteClientOnlyRevokesOwnedUnchangedIdentity(t *testing.T) {
	ctr := &authFixtureContainer{}
	cluster := authFixtureCluster(ctr)
	client, err := cluster.CreateClient(t.Context(), "reader", ClientCaps{Mon: "allow r"})
	if err != nil {
		t.Fatal(err)
	}
	other := authFixtureCluster(&authFixtureContainer{})
	if err := other.DeleteClient(t.Context(), client); err == nil {
		t.Fatal("another cluster revoked an unowned client")
	}
	var request testcontainers.GenericContainerRequest
	if err := other.WithClientIdentity(client)(&request); err == nil {
		t.Fatal("another cluster attached an unowned client")
	}
	ctr.key = "REPLACEMENT-KEY"
	before := len(ctr.calls)
	if err := cluster.DeleteClient(t.Context(), client); err == nil || len(ctr.calls) != before+1 {
		t.Fatal("revocation deleted an externally replaced key")
	}
	ctr.key = "PRIVATE-KEY"
	if err := cluster.DeleteClient(t.Context(), client); err != nil {
		t.Fatal(err)
	}
	before = len(ctr.calls)
	if err := cluster.DeleteClient(t.Context(), client); err != nil || len(ctr.calls) != before {
		t.Fatal("repeat revocation was not a no-op")
	}
	if err := cluster.WithClientIdentity(client)(&request); err == nil {
		t.Fatal("revoked identity was attached to a new client")
	}
}

func TestClientCreationUsesFreshKeyAndRemovesTemporaryKeyring(t *testing.T) {
	ctr := &authFixtureContainer{}
	if _, err := authFixtureCluster(ctr).CreateClient(t.Context(), "reader", ClientCaps{Mon: "allow r"}); err != nil {
		t.Fatal(err)
	}
	if ctr.copyMode != 0o600 || clientKey(ctr.copied, "client.reader") != "PRIVATE-KEY" || !strings.HasPrefix(ctr.copyPath, "/tmp/tc-client-") {
		t.Fatal("a private fresh keyring was not copied")
	}
	var add, cleanup bool
	for _, call := range ctr.calls {
		if len(call) > 4 && call[0] == "auth" && call[1] == "add" {
			add = call[3] == "-i" && call[4] == ctr.copyPath
		}
		if slices.Equal(call, []string{"rm", "-f", ctr.copyPath}) {
			cleanup = true
		}
	}
	if !add || !cleanup {
		t.Fatal("fresh key input or temporary keyring cleanup is missing")
	}
}

func authFixtureCluster(ctr testcontainers.Container) *Container {
	return &Container{Container: ctr, config: []byte("[global]\nmon_host = old-mon\n"), settings: options{startupTimeout: time.Second, hostNetwork: true, publicAddress: "127.0.0.1"}}
}

type authFixtureContainer struct {
	testcontainers.Container
	calls    [][]string
	listing  string
	key      string
	fail     string
	copied   []byte
	copyPath string
	copyMode int64
	caps     map[string]string
}

func (ctr *authFixtureContainer) CopyToContainer(_ context.Context, content []byte, path string, mode int64) error {
	ctr.copied, ctr.copyPath, ctr.copyMode = bytes.Clone(content), path, mode
	return nil
}

func (ctr *authFixtureContainer) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if args[0] == "ceph" {
		args = args[3:]
	}
	args = slices.Clone(args)
	ctr.calls = append(ctr.calls, args)
	output := ""
	if args[0] == "ceph-authtool" {
		output = "PRIVATE-KEY\n"
	} else if len(args) >= 2 && args[0] == "auth" && args[1] == "ls" {
		output = ctr.listing
		if output == "" {
			output = `{"auth_dump":[]}`
		}
	} else if len(args) >= 3 && args[0] == "auth" && args[1] == "get" {
		key := ctr.key
		if key == "" {
			key = "PRIVATE-KEY"
		}
		output = "[" + args[2] + "]\n\tkey = " + key + "\n"
		if slices.Contains(args, "json") {
			caps := ctr.caps
			if caps == nil {
				caps = map[string]string{"mon": "allow r", "osd": "allow rw pool=tenant", "mgr": "allow *"}
			}
			data, _ := json.Marshal([]any{map[string]any{"entity": args[2], "key": key, "caps": caps}})
			output = string(data)
		}
	} else if len(args) >= 3 && args[0] == "auth" && args[1] == "caps" && strings.Join(args, " ") != ctr.fail {
		ctr.caps = map[string]string{}
		for i := 3; i+1 < len(args); i += 2 {
			ctr.caps[args[i]] = args[i+1]
		}
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	code := 0
	failureArgs := args
	if len(args) > 4 && args[0] == "auth" && args[1] == "add" {
		failureArgs = append(slices.Clone(args[:3]), args[5:]...)
	}
	if strings.Join(failureArgs, " ") == ctr.fail {
		header[0], code, output = byte(stdcopy.Stderr), 1, "PRIVATE-KEY injected failure"
	}
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.WriteString(output)
	return code, &stream, nil
}
