package finalize

import (
	"encoding/binary"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/testcontainers/testcontainers-go"
)

// The helpers of this file build the containers and log frames that the
// verification tests feed to their Docker fakes.

type crashTarget struct {
	testcontainers.Container
	id string
}

func (c crashTarget) GetContainerID() string { return c.id }

func crashContainer(role, name, id string) diagnostics.Target {
	return diagnostics.Target{Role: role, Name: name, Container: crashTarget{id: id}}
}

// diagnosticsFrame is one frame of Docker's multiplexed log stream.
func diagnosticsFrame(kind byte, data string) []byte {
	header := make([]byte, 8)
	header[0] = kind
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	return append(header, data...)
}
