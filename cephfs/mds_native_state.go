package cephfs

// cephFSMDSRowStateAccepted reports whether state is a daemon state the strict
// native-map decoders accept in a daemon row: decodeColdMDSMap,
// decodeStoppedMDSMap and decodeMDSReplacementMap. It is the single copy of the
// list that each of them used to write out.
//
// The twelve names are the "up:" names of ceph_mds_state_name in the pinned
// Ceph source (src/common/ceph_strings.cc) except up:oneshot-replay. The
// "down:" names, "null" and "???" are not accepted either. The code never
// documented whether leaving up:oneshot-replay out is intentional, and this
// function preserves the refusal without deciding it: supporting that state
// needs evidence and a separate semantic decision. The row-state rules that
// differ between the decoders (which accepted states are then refused as
// "boot", "standby" or "standby-replay" for a given position) stay in each
// decoder.
func cephFSMDSRowStateAccepted(state string) bool {
	switch state {
	case "up:boot", "up:standby", "up:standby-replay", "up:creating", "up:starting",
		"up:replay", "up:resolve", "up:reconnect", "up:rejoin", "up:clientreplay",
		"up:active", "up:stopping":
		return true
	}
	return false
}
