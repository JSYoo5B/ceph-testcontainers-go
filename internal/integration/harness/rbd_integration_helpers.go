//go:build all || integration

package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func RBDOutput(t *testing.T, ctx context.Context, client testcontainers.Container, args ...string) []byte {
	t.Helper()
	args = append([]string{"rbd"}, args...)
	code, r, err := client.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("%s exited %d: %s", strings.Join(args, " "), code, out)
	}
	return out
}

// RBDMirrorPoolInfo returns `rbd mirror pool info --format json` for pool or
// pool/namespace. Ceph 19's rbd prints neither mirror_uuid nor
// remote_namespace, so on that release this adds them the way the fixture
// reads them: the rbd_mirroring object's mirror_uuid omap value and the
// same-named namespace. Other releases are returned unchanged, so a field
// missing there still fails the caller.
func RBDMirrorPoolInfo(ctx context.Context, client testcontainers.Container, pool, namespace string) ([]byte, error) {
	spec := pool
	if namespace != "" {
		spec += "/" + namespace
	}
	data, err := rbdClientExec(ctx, client, "rbd", "mirror", "pool", "info", spec, "--format", "json")
	if err != nil {
		return nil, err
	}
	legacy, err := rbdClientCeph19(ctx, client)
	if err != nil || !legacy {
		return data, err
	}
	var info map[string]any
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("decode RBD mirror pool info: %w", err)
	}
	if mode, _ := info["mode"].(string); mode == "disabled" {
		return data, nil
	}
	if _, ok := info["remote_namespace"]; !ok {
		info["remote_namespace"] = namespace
	}
	if _, ok := info["mirror_uuid"]; !ok {
		args := []string{"rados", "--pool", pool}
		if namespace != "" {
			args = append(args, "--namespace", namespace)
		}
		value, err := rbdClientExec(ctx, client, append(args, "getomapval", "rbd_mirroring", "mirror_uuid", "/dev/stdout")...)
		if err != nil {
			return nil, err
		}
		info["mirror_uuid"] = string(value)
	}
	return json.Marshal(info)
}

var rbdClientReleases sync.Map

// rbdClientCeph19 reports whether client runs Ceph 19, reading rbd --version
// once per container.
func rbdClientCeph19(ctx context.Context, client testcontainers.Container) (bool, error) {
	if legacy, ok := rbdClientReleases.Load(client.GetContainerID()); ok {
		return legacy.(bool), nil
	}
	data, err := rbdClientExec(ctx, client, "rbd", "--version")
	if err != nil {
		return false, err
	}
	legacy := strings.HasPrefix(string(data), "ceph version 19.")
	rbdClientReleases.Store(client.GetContainerID(), legacy)
	return legacy, nil
}

// rbdClientExec returns stdout alone: rados getomapval reports its output
// file on stderr, which must not join the value.
func rbdClientExec(ctx context.Context, client testcontainers.Container, args ...string) ([]byte, error) {
	code, reader, err := client.Exec(ctx, args)
	if err != nil {
		return nil, err
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, reader); err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, fmt.Errorf("%s exited %d: %s%s", strings.Join(args, " "), code, stdout.Bytes(), stderr.Bytes())
	}
	return stdout.Bytes(), nil
}

// RBDNamespaceMappingUnsupported reports whether cluster's client toolkit
// cannot map differently named namespaces: its rbd has no
// --remote-namespace, as in Ceph 19 (see ReleaseCapabilities).
func RBDNamespaceMappingUnsupported(t *testing.T, ctx context.Context, cluster *ceph.Container) bool {
	t.Helper()
	return !CapabilitySupported(t, ctx, cluster, CapabilityRBDNamespaceMapping)
}

func VerifyRBDBytes(t *testing.T, ctx context.Context, client testcontainers.Container, image string, expected []byte) {
	t.Helper()
	const path = "/tmp/rbd-result"
	// The export command refuses to overwrite an existing local destination.
	ExecCommand(t, ctx, client, "rm", "-f", path)
	ExecCommand(t, ctx, client, "rbd", "export", image, path, "--no-progress")
	r, err := client.CopyFileFromContainer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatalf("%s payload changed: got %d bytes, want %d bytes", image, len(actual), len(expected))
	}
}
