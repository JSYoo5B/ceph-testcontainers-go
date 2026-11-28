package blockio

import (
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// HelperRequest is the container request of an idle block I/O helper named
// name that runs image as root, privileged, with the engine host's /dev and
// no network. The keeper of fault/blockio and the helper of ceph/preflight
// both start from it, so the preflight checks the privileges and host
// configuration that the keeper runs with. The keeper adds its private tmpfs
// and owner script on top.
func HelperRequest(image, name string) testcontainers.ContainerRequest {
	return testcontainers.ContainerRequest{
		Image: image, Name: name, Entrypoint: []string{"sleep"}, Cmd: []string{"infinity"}, User: "0",
		HostConfigModifier: func(h *container.HostConfig) {
			h.Privileged = true
			h.Binds = append(h.Binds, "/dev:/dev")
			h.NetworkMode = "none"
		},
	}
}
