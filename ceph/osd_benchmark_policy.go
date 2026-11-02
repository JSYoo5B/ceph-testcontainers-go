package ceph

// OSDBenchmarkPolicy selects the automatic mClock startup-calibration policy
// of the cluster's owned OSDs. It does not select a scheduler or supply measured
// IOPS capacity. MemStore is already excluded from automatic calibration by
// native Ceph; BlueStore on tmpfs is not MemStore.
type OSDBenchmarkPolicy string

const (
	// OSDBenchmarkNative permits Ceph's native calibration decision. It sets
	// osd_mclock_skip_benchmark=false, not force=true: a previously configured
	// non-default capacity can still avoid another calibration. MemStore and
	// other schedulers do not enter mClock's automatic benchmark path.
	OSDBenchmarkNative OSDBenchmarkPolicy = "native"
	// OSDBenchmarkSkip bypasses automatic mClock startup calibration while
	// retaining the scheduler and its configured capacity/bandwidth assumptions.
	// Those assumptions are not a measurement of the fixture's backing device.
	OSDBenchmarkSkip OSDBenchmarkPolicy = "skip"
)

// WithStartupBenchmark chooses the startup benchmark policy of every owned
// OSD, including initial OSDs, AddOSD, same-container Stop/Start and a detached
// OSD's Reopen. Omitted, it preserves ordinary native configuration and adds
// no benchmark command-line setting. An explicit policy sets only
// osd_mclock_skip_benchmark on the daemon command line, before initialization;
// a local startup-input read checks that the binary accepts the option/value
// before store preparation. This is not a live calibration receipt. The flag
// takes precedence over local files and central configuration. Later
// override.SetConfig calls cannot change that pinned setting. Other settings,
// including scheduler, force, IOPS capacity and sequential bandwidth, are
// unchanged. Native permits the native decision and does not force calibration.
//
// Skip is fixture resource control for small functional BlueStore stores. The
// native benchmark can prefill 400 MiB of objects per OSD, additional to its
// timed writes and metadata. Sparse files may materialize physical allocation,
// and deleting benchmark objects need not release backing blocks or tmpfs
// pages. Use Native or omit the policy when native calibration or backing-device
// behavior is part of the test. Neither policy changes the automatic exclusion
// of MemStore or claims equivalent QoS behavior for every workload.
//
// The value is copied with OSDStorage. Repeated calls on one value replace the
// earlier policy; two WithOSDStorage carriers that both set it are refused,
// even when they agree. Unknown policies, including an explicit empty value,
// are refused before allocating containers.
//
//purpose:construction
func (o OSDStorage) WithStartupBenchmark(policy OSDBenchmarkPolicy) OSDStorage {
	o.set |= osdStorageBenchmark
	o.benchmark = policy
	return o
}
