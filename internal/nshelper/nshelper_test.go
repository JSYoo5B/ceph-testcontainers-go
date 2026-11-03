package nshelper

import (
	"errors"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestCategoryMatchesItsKindOnly(t *testing.T) {
	err := Category("helper refused", ceph.ErrImageCapability)
	if !errors.Is(err, ceph.ErrImageCapability) || errors.Is(err, ceph.ErrEnvironmentUnavailable) || err.Error() != "helper refused" {
		t.Fatalf("category error %q does not match only its kind", err)
	}
}

func TestAdmitRefusesInOrderWithTheCallersEndpointCheck(t *testing.T) {
	joinable := Target{Running: true, NetworkMode: "ceph-net", SandboxID: "sandbox", Driver: "bridge", MAC: "02:42:ac:11:00:02"}
	noMAC := errors.New("no MAC")
	endpoint := func(target Target) error {
		if target.MAC == "" {
			return noMAC
		}
		return nil
	}
	if err := Admit(joinable, "capture", "the target", endpoint); err != nil {
		t.Fatalf("joinable target refused: %v", err)
	}
	for name, refusal := range map[string]struct {
		change func(*Target)
		text   string
	}{
		"stopped":  {func(t *Target) { t.Running = false }, "capture requires a running, unpaused container"},
		"paused":   {func(t *Target) { t.Paused = true }, "capture requires a running, unpaused container"},
		"host":     {func(t *Target) { t.NetworkMode = "host" }, "capture cannot join a container in network mode host"},
		"none":     {func(t *Target) { t.NetworkMode = "none" }, "capture cannot join a container in network mode none"},
		"shared":   {func(t *Target) { t.NetworkMode = "container:other" }, "capture cannot join a container in network mode container:other"},
		"sandbox":  {func(t *Target) { t.SandboxID = "" }, "the target has no network sandbox"},
		"overlay":  {func(t *Target) { t.Driver = "overlay" }, `capture requires a bridge network, not a "overlay" one`},
		"detached": {func(t *Target) { t.MAC, t.Driver = "", "" }, "no MAC"},
	} {
		target := joinable
		refusal.change(&target)
		err := Admit(target, "capture", "the target", endpoint)
		if err == nil || !strings.Contains(err.Error(), refusal.text) {
			t.Fatalf("%s: got %v, want %q", name, err, refusal.text)
		}
		if name != "detached" && !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("%s: refusal is not ceph.ErrInvalidRequest: %v", name, err)
		}
	}
}
