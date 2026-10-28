package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// createdOverride wraps a sharedBackend and reports its own created value for
// every client, whatever the client's private field says.
type createdOverride struct {
	sharedBackend
	created func(*ClientConfig) bool
}

func (o createdOverride) createClient(ctx context.Context, entity string, caps ClientCaps) (*ClientConfig, bool, error) {
	client, _, err := o.sharedBackend.createClient(ctx, entity, caps)
	return client, o.created(client), err
}

// The tenant registers a client for deletion on the backend's report, not on
// the private field of ClientConfig, which a harness in another package could
// not read.
func TestTenantRegistersClientsOnTheBackendsCreatedReport(t *testing.T) {
	for _, tc := range []struct {
		name       string
		clear      bool
		reported   bool
		registered int
	}{
		{"reported created, private field unset", true, true, 1},
		{"reported not created, private field set", false, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shared := newFakeShared(&fakeFleet{})
			tb := newFakeTB(t)
			tenant := shared.Lease(tb)
			cluster := backendOf(tenant)
			tenant.inst.backend = createdOverride{sharedBackend: cluster, created: func(c *ClientConfig) bool {
				if tc.clear {
					c.created = false
				}
				return tc.reported
			}}
			tenant.mu.Lock()
			before := len(tenant.clients)
			tenant.mu.Unlock()
			if _, err := tenant.CreateClient(t.Context(), "extra", TenantReadWrite); err != nil {
				t.Fatal(err)
			}
			tenant.mu.Lock()
			got := len(tenant.clients) - before
			tenant.mu.Unlock()
			if got != tc.registered {
				t.Fatalf("registered clients = %d, want %d", got, tc.registered)
			}
			if !tc.reported {
				// Nothing deletes it, so remove it by hand to let the release verify.
				cluster.change(func(f *fakeShared) { delete(f.auth, "client."+tenant.ID()+"-extra") })
			}
			tb.finish()
			if tb.Failed() {
				t.Fatalf("release failed: %s", tb.errorText())
			}
		})
	}
}

// createScriptContainer answers the shared script with a fixed JSON result
// and every other command with an empty success.
type createScriptContainer struct {
	testcontainers.Container
	result  string
	execErr error
	scripts int
}

func (ctr *createScriptContainer) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	out := ""
	if len(args) >= 2 && args[0] == "python3" && args[1] == "-c" {
		ctr.scripts++
		if ctr.execErr != nil {
			return 0, nil, ctr.execErr
		}
		out = ctr.result
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(header[4:], uint32(len(out)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.WriteString(out)
	return 0, &stream, nil
}

func (ctr *createScriptContainer) CopyToContainer(context.Context, []byte, string, int64) error {
	return nil
}

func createScriptResult(t *testing.T, key string, ret int, keyring string) string {
	t.Helper()
	data, err := json.Marshal(map[string]any{"created": map[string]any{"key": key, "ret": ret, "keyring": keyring}})
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// createSharedClient reports created exactly from the point where the MON
// accepted the entity, and keeps returning the partial client after it.
func TestCreateSharedClientReportsCreatedFromTheMonAcceptance(t *testing.T) {
	const key = "AQBnZ2VuZXJhdGVkLWtleS1mb3ItdGVzdA=="
	keyring := "[client.reader]\n\tkey = " + key + "\n"
	for _, tc := range []struct {
		name      string
		result    string
		execErr   error
		created   bool
		wantErr   bool
		wantReady bool
		keyring   bool
	}{
		{name: "success", result: createScriptResult(t, key, 0, keyring), created: true, wantReady: true, keyring: true},
		{name: "MON refused", result: createScriptResult(t, key, -17, ""), wantErr: true},
		{name: "no key", result: createScriptResult(t, "", 0, ""), wantErr: true},
		{name: "missing result", result: `{}`, wantErr: true},
		{name: "keyring without the key", result: createScriptResult(t, key, 0, "[client.reader]\n\tkey = other\n"), created: true, wantErr: true},
		{name: "script failed", execErr: errors.New("exec failed"), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctr := &createScriptContainer{result: tc.result, execErr: tc.execErr}
			cluster := &Container{Container: ctr, bootstrap: bootstrapRecord{config: []byte("[global]\nmon_host = old-mon\n")},
				settings: options{startupTimeout: time.Second, hostNetwork: true, publicAddress: "127.0.0.1"}}
			client, created, err := containerBackend{c: cluster}.createClient(t.Context(), "client.reader", ClientCaps{Mon: "allow r"})
			if created != tc.created || (err != nil) != tc.wantErr {
				t.Fatalf("created = %v, err = %v; want created %v, error %v", created, err, tc.created, tc.wantErr)
			}
			if client == nil || client.name != "client.reader" || client.owner != cluster {
				t.Fatalf("the partial client is lost: %+v", client)
			}
			if client.created != created || client.ready != tc.wantReady || (len(client.keyring) > 0) != tc.keyring {
				t.Fatalf("client created %v ready %v keyring %d bytes", client.created, client.ready, len(client.keyring))
			}
			if tc.execErr == nil && ctr.scripts != 1 {
				t.Fatalf("scripts = %d", ctr.scripts)
			}
		})
	}
	t.Run("no bootstrap", func(t *testing.T) {
		cluster := &Container{Container: &createScriptContainer{}, settings: options{startupTimeout: time.Second}}
		client, created, err := containerBackend{c: cluster}.createClient(t.Context(), "client.reader", ClientCaps{Mon: "allow r"})
		if client != nil || created || err == nil {
			t.Fatalf("incomplete bootstrap gave %v, %v, %v", client, created, err)
		}
	})
	t.Run("terminated", func(t *testing.T) {
		cluster := &Container{Container: &createScriptContainer{}, closed: true, settings: options{startupTimeout: time.Second}}
		client, created, err := containerBackend{c: cluster}.createClient(t.Context(), "client.reader", ClientCaps{Mon: "allow r"})
		if client != nil || created || !errors.Is(err, errClusterTerminated) {
			t.Fatalf("terminated cluster gave %v, %v, %v", client, created, err)
		}
	})
}
