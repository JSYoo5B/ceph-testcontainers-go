// Package nshelper is shared by the fault and capture packages, whose helper
// containers join the network namespace of a target container: the error
// categories of their sentinels, the inspection of the target, the refusals
// of a target whose namespace a helper cannot join and the confinement of the
// helper.
package nshelper

import (
	"context"
	"fmt"
	"net/netip"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

type categoryError struct {
	text string
	kind error
}

func (e *categoryError) Error() string        { return e.text }
func (e *categoryError) Is(target error) bool { return target == e.kind }

// Category returns a sentinel with text that matches the ceph category kind
// with errors.Is.
func Category(text string, kind error) error { return &categoryError{text: text, kind: kind} }

// InvalidRequest returns a ceph.ErrInvalidRequest refusal.
func InvalidRequest(format string, args ...any) error {
	return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Err: fmt.Errorf(format, args...)}
}

// Target is what a helper needs to know about the container whose namespace
// it joins and about the container's endpoint on one network.
type Target struct {
	// Running is false for a restarting container.
	Running, Paused bool
	StartedAt       string
	NetworkMode     container.NetworkMode
	SandboxID       string
	// Driver is the driver of the network. It, the addresses and the MAC are
	// empty when the container has no endpoint on the network.
	Driver            string
	Address, Address6 netip.Addr
	MAC               string
}

// Inspector is the part of a Docker client that Inspect uses.
type Inspector interface {
	ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error)
	NetworkInspect(context.Context, string, mobycl.NetworkInspectOptions) (mobycl.NetworkInspectResult, error)
}

// Inspect reads container id and its endpoint on network. The network is
// inspected only when the container has an endpoint on it.
func Inspect(ctx context.Context, docker Inspector, id, network string) (Target, error) {
	inspection, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return Target{}, err
	}
	var target Target
	if current := inspection.Container.State; current != nil {
		target.Running, target.Paused = current.Running && !current.Restarting, current.Paused
		target.StartedAt = current.StartedAt
	}
	if inspection.Container.HostConfig != nil {
		target.NetworkMode = inspection.Container.HostConfig.NetworkMode
	}
	if settings := inspection.Container.NetworkSettings; settings != nil {
		target.SandboxID = settings.SandboxID
		if endpoint := settings.Networks[network]; endpoint != nil {
			target.Address, target.Address6, target.MAC = endpoint.IPAddress, endpoint.GlobalIPv6Address, endpoint.MacAddress.String()
			inspection, err := docker.NetworkInspect(ctx, network, mobycl.NetworkInspectOptions{})
			if err != nil {
				return Target{}, fmt.Errorf("inspect network %s: %w", network, err)
			}
			target.Driver = inspection.Network.Driver
		}
	}
	return target, nil
}

// Admit refuses, with ceph.ErrInvalidRequest, a target whose namespace a
// helper cannot join: one that is not running or is paused, one in host,
// none or another container's network mode, one without a sandbox, one whose
// endpoint the caller's endpoint check refuses, and one on a network that is
// not a bridge. op names the operation and subject the target in the
// refusal texts.
func Admit(target Target, op, subject string, endpoint func(Target) error) error {
	switch mode := target.NetworkMode; {
	case !target.Running || target.Paused:
		return InvalidRequest("%s requires a running, unpaused container", op)
	case mode.IsHost() || mode.IsNone() || mode.IsContainer():
		return InvalidRequest("%s cannot join a container in network mode %s", op, mode)
	case target.SandboxID == "":
		return InvalidRequest("%s has no network sandbox", subject)
	}
	if err := endpoint(target); err != nil {
		return err
	}
	if target.Driver != "bridge" {
		return InvalidRequest("%s requires a bridge network, not a %q one", op, target.Driver)
	}
	return nil
}

// Confine runs a helper in networkMode with capability as its only
// capability and labels it with role. The caller adds the helper's process
// and user.
func Confine(networkMode container.NetworkMode, capability, role string) []testcontainers.ContainerCustomizer {
	return []testcontainers.ContainerCustomizer{
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			host.NetworkMode = networkMode
			host.CapDrop = []string{"ALL"}
			host.CapAdd = []string{capability}
		}),
		testcontainers.WithLabels(map[string]string{"org.testcontainers.ceph.role": role}),
	}
}
