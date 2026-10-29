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
			client, created, err := containerBackend{c: cluster}.CreateClient(t.Context(), "client.reader", ClientCaps{Mon: "allow r"})
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
		client, created, err := containerBackend{c: cluster}.CreateClient(t.Context(), "client.reader", ClientCaps{Mon: "allow r"})
		if client != nil || created || err == nil {
			t.Fatalf("incomplete bootstrap gave %v, %v, %v", client, created, err)
		}
	})
	t.Run("terminated", func(t *testing.T) {
		cluster := &Container{Container: &createScriptContainer{}, closed: true, settings: options{startupTimeout: time.Second}}
		client, created, err := containerBackend{c: cluster}.CreateClient(t.Context(), "client.reader", ClientCaps{Mon: "allow r"})
		if client != nil || created || !errors.Is(err, errClusterTerminated) {
			t.Fatalf("terminated cluster gave %v, %v, %v", client, created, err)
		}
	})
}
