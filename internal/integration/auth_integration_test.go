//go:build integration && auth

package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestClientIdentities(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			image, opts := integrationImages(t)
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, err := ceph.Run(ctx, image, opts...)
			if cluster != nil {
				testcontainers.CleanupContainer(t, cluster)
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, pool := range []string{"tc-auth", "tc-auth-other"} {
				cephCommand(t, ctx, cluster, "osd", "pool", "create", pool, "8")
				cephCommand(t, ctx, cluster, "osd", "pool", "application", "enable", pool, "rados")
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			writer, err := cluster.CreateClient(ctx, "writer", ceph.ClientCaps{Mon: "allow r", OSD: "allow rw pool=tc-auth namespace=blue"})
			if err != nil {
				t.Fatal(err)
			}
			reader, err := cluster.CreateClient(ctx, "reader", ceph.ClientCaps{Mon: "allow r", OSD: "allow r pool=tc-auth namespace=blue"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cluster.CreateClient(ctx, "writer", ceph.ClientCaps{Mon: "allow *", OSD: "allow *"}); err == nil {
				t.Fatal("existing identity was accepted with broader caps")
			}
			newClient := func(identity *ceph.ClientConfig) testcontainers.Container {
				t.Helper()
				client, err := testcontainers.Run(ctx, image, cluster.WithClientIdentity(identity),
					testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity"),
					testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import rados"})))
				if client != nil {
					testcontainers.CleanupContainer(t, client)
				}
				if err != nil {
					t.Fatal(err)
				}
				return client
			}
			writerClient, readerClient := newClient(writer), newClient(reader)
			// The outer Python process imposes a wall-clock deadline even if a
			// native call is blocked. Every probe creates a fresh authenticated session.
			probe := func(client testcontainers.Container, identity *ceph.ClientConfig, phase, keyring string) {
				t.Helper()
				execCommand(t, ctx, client, "python3", "-c", `import subprocess, sys
subprocess.run([sys.executable, "-c", sys.argv[1], *sys.argv[2:]], timeout=35, check=True)`, authNativeProbe,
					identity.Name(), phase, keyring)
			}
			probe(writerClient, writer, "write", writer.KeyringPath())
			probe(readerClient, reader, "readonly", reader.KeyringPath())
			// Use another real key under the writer's entity name so this tests
			// authentication failure rather than malformed keyring parsing.
			_, otherKeyring, err := reader.ConnectionConfig()
			if err != nil {
				t.Fatal(err)
			}
			wrongKeyring := bytes.ReplaceAll(otherKeyring, []byte("["+reader.Name()+"]"), []byte("["+writer.Name()+"]"))
			if err := writerClient.CopyToContainer(ctx, wrongKeyring, "/tmp/wrong.keyring", 0o600); err != nil {
				t.Fatal("copy deliberately wrong keyring failed")
			}
			probe(writerClient, writer, "bad-auth", "/tmp/wrong.keyring")
			// Positive I/O after negative probes proves that caps were neither
			// broadened by the duplicate request nor damaged by authentication failure.
			probe(writerClient, writer, "verify", writer.KeyringPath())
			if err := cluster.DeleteClient(ctx, writer); err != nil {
				t.Fatal(err)
			}
			probe(writerClient, writer, "bad-auth", writer.KeyringPath())
			probe(readerClient, reader, "readonly", reader.KeyringPath())
			t.Log("native librados: scoped read/write, readonly and cross-boundary denials; wrong-key and revoked fresh connections failed")
		})
	}
}

const authNativeProbe = `import rados, sys
entity, phase, keyring = sys.argv[1:]
payload = bytes(range(256)) * 256
cluster = rados.Rados(conffile="/etc/ceph/ceph.conf", name=entity)
cluster.conf_set("keyring", keyring)
cluster.conf_set("rados_mon_op_timeout", "5")
cluster.conf_set("rados_osd_op_timeout", "5")
if phase == "bad-auth":
    try:
        cluster.connect(timeout=10)
    except rados.Error:
        print("fresh authentication rejected")
    else:
        raise AssertionError("invalid or revoked credentials authenticated")
    finally:
        cluster.shutdown()
    sys.exit(0)
cluster.connect(timeout=10)
def denied(operation):
    try:
        operation()
    except rados.PermissionError:
        return
    raise AssertionError("unauthorized object operation succeeded")
try:
    with cluster.open_ioctx("tc-auth") as io:
        io.set_namespace("blue")
        if phase == "write":
            io.write_full("shared", payload)
        assert io.read("shared", len(payload)) == payload
        if phase == "readonly":
            denied(lambda: io.write_full("denied-write", payload))
        else:
            io.write_full("allowed-write", payload[::-1])
            assert io.read("allowed-write", len(payload)) == payload[::-1]
        io.set_namespace("red")
        denied(lambda: io.read("shared", len(payload)))
        denied(lambda: io.write_full("denied-namespace", payload))
    with cluster.open_ioctx("tc-auth-other") as io:
        io.set_namespace("blue")
        denied(lambda: io.read("shared", len(payload)))
        denied(lambda: io.write_full("denied-pool", payload))
    print("fresh scoped native object I/O and access denials passed")
finally:
    cluster.shutdown()
`
