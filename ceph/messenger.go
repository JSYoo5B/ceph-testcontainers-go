package ceph

import "strconv"

const (
	messengerV2SecureEnvironment = "CEPH_MSGR2_SECURE_ONLY"
	messengerV2OnlyEnvironment   = "CEPH_MSGR2_ONLY"
)

// MessengerMode selects, at bootstrap, the Messenger protocols that every
// daemon listens on and advertises, and whether the connection modes stay
// configurable. Both are fixed for the life of the cluster.
type MessengerMode int

const (
	// MessengerDefault retains Ceph's own Messenger defaults and v1/v2 MON
	// addresses. It does not imply that every connection uses encryption.
	MessengerDefault MessengerMode = iota
	// MessengerV2Secure allows only encrypted Messenger v2 connections, with
	// no CRC-mode negotiation or legacy Messenger v1 listener.
	MessengerV2Secure
	// MessengerV2Only listens on and advertises only Messenger v2, like
	// MessengerV2Secure, but keeps the connection modes configurable: Ceph's
	// defaults, WithMessengerPolicy, OverrideMessengerPolicy and
	// WithClientMessengerPolicy select them, as with MessengerDefault.
	MessengerV2Only
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

// messengerV2Only reports a mode without Messenger v1 listeners.
func (o options) messengerV2Only() bool {
	return o.messengerMode == MessengerV2Secure || o.messengerMode == MessengerV2Only
}

// messengerEnvironment returns the bootstrap scripts' variables of the mode.
func (o options) messengerEnvironment() map[string]string {
	switch o.messengerMode {
	case MessengerV2Secure:
		return map[string]string{messengerV2SecureEnvironment: "true"}
	case MessengerV2Only:
		return map[string]string{messengerV2OnlyEnvironment: "true"}
	}
	return nil
}

func (o options) monitorPortCount() int {
	if o.messengerV2Only() {
		return 1
	}
	return 2
}

func (o options) monitorExposedPorts() []string {
	if o.messengerV2Only() {
		return []string{"3300/tcp"}
	}
	return []string{"3300/tcp", "6789/tcp"}
}

func (o options) monitorPortEnvironment(ports []int) map[string]string {
	env := map[string]string{"CEPH_MON_PORT_V2": strconv.Itoa(ports[0])}
	if !o.messengerV2Only() {
		env["CEPH_MON_PORT_V1"] = strconv.Itoa(ports[1])
	}
	return env
}

func messengerBootstrapSetting(name string) bool {
	switch name {
	case "ms_cluster_mode", "ms_service_mode", "ms_client_mode",
		"ms_mon_cluster_mode", "ms_mon_service_mode", "ms_mon_client_mode":
		return true
	}
	return messengerProtocolSetting(name)
}

// messengerProtocolSetting reports an option that selects the Messenger
// protocols that daemons listen on.
func messengerProtocolSetting(name string) bool {
	return name == "ms_bind_msgr1" || name == "ms_bind_msgr2"
}
