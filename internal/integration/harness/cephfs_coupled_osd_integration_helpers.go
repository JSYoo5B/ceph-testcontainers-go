//go:build all || integration

package harness

// CephFSClientStatus is a libcephfs client's admin socket "status": the
// OSDMap epoch it holds, the epoch barrier it was given and whether it found
// itself blocklisted.
type CephFSClientStatus struct {
	ID          uint64 `json:"id"`
	Address     string `json:"addr_str"`
	OSDEpoch    uint32 `json:"osd_epoch"`
	Barrier     uint32 `json:"osd_epoch_barrier"`
	Blocklisted bool   `json:"blocklisted"`
}
