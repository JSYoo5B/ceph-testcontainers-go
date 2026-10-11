package ceph

import "strconv"

const messengerV2SecureEnvironment = "CEPH_MSGR2_SECURE_ONLY"

// MessengerMode selects the fixture's immutable bootstrap connection policy.
type MessengerMode int

const (
	// MessengerDefault retains Ceph's own Messenger defaults and v1/v2 MON
	// addresses. It does not imply that every connection uses encryption.
	MessengerDefault MessengerMode = iota
	// MessengerV2Secure allows only encrypted Messenger v2 connections, with
	// no CRC-mode negotiation or legacy Messenger v1 listener.
	MessengerV2Secure
)

// MessengerMode returns the selected bootstrap policy. It does not inspect
// effective native settings or prove the mode negotiated by a live connection.
// A nil Container reports MessengerDefault.
func (c *Container) MessengerMode() MessengerMode {
	if c == nil {
		return MessengerDefault
	}
	return c.settings.messengerMode
}

func (o options) monitorPortCount() int {
	if o.messengerMode == MessengerV2Secure {
		return 1
	}
	return 2
}

func (o options) monitorExposedPorts() []string {
	if o.messengerMode == MessengerV2Secure {
		return []string{"3300/tcp"}
	}
	return []string{"3300/tcp", "6789/tcp"}
}

func (o options) monitorPortEnvironment(ports []int) map[string]string {
	env := map[string]string{"CEPH_MON_PORT_V2": strconv.Itoa(ports[0])}
	if o.messengerMode != MessengerV2Secure {
		env["CEPH_MON_PORT_V1"] = strconv.Itoa(ports[1])
	}
	return env
}

func messengerBootstrapSetting(name string) bool {
	switch name {
	case "ms_cluster_mode", "ms_service_mode", "ms_client_mode",
		"ms_mon_cluster_mode", "ms_mon_service_mode", "ms_mon_client_mode",
		"ms_bind_msgr1", "ms_bind_msgr2":
		return true
	}
	return false
}
