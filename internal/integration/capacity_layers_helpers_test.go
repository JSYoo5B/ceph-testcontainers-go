//go:build all || integration

package integration_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// The storage layers of issue 63 beyond the underlying filesystem. Each judge
// below reads receipts that the native tests produce and decides which layer
// refused the workload. None of them observes anything itself.

// Outcomes of one BlueStore logical-capacity phase: the OSD's sparse block file
// is far shorter than the owned tmpfs, so the filesystem keeps room and only
// the policies or BlueStore's own allocator can refuse a write.
const (
	logicalPolicyBlocked    = "policy-blocked"           // OSD_FULL refused the writes; the OSD stayed up and its allocator never failed
	logicalAllocatorAborted = "allocator-aborted"        // BlueStore's allocator returned ENOSPC and the OSD aborted
	logicalFilesystemFirst  = "filesystem-not-excluded"  // the owned filesystem had too little room left to rule it out
	logicalUnreached        = "unreached"                // every write was acknowledged
	logicalOther            = "refused-without-evidence" // writes stopped but no layer's evidence is present
)

// logicalFSMargin is the room the owned filesystem must still have when a
// phase is judged: more than the largest write, so a filesystem ENOSPC cannot
// stand behind an allocator ENOSPC.
const logicalFSMargin = int64(64 << 20)

// The OSD log lines that BlueStore prints when its allocator refuses a write
// and the transaction cannot be applied.
var logicalAllocatorMarkers = []string{"_do_alloc_write failed", "No space left on device", "ENOSPC from bluestore"}

// logicalPhase is what one write phase leaves behind.
type logicalPhase struct {
	Ledger []attachMutation
	// Checks are the health check codes seen after the writes stopped.
	Checks []string
	// Running is the OSD container's state after the writes; Log is its log
	// tail at that point.
	Running bool
	Log     []string
	// FSAvail is the owned filesystem's available bytes after the writes, and
	// Block the block entry's logical size and allocated bytes.
	FSAvail        int64
	BlockSize      int64
	BlockAllocated int64
}

func allocatorRefused(log []string) bool {
	for _, marker := range logicalAllocatorMarkers {
		if !slices.ContainsFunc(log, func(line string) bool { return strings.Contains(line, marker) }) {
			return false
		}
	}
	return true
}

// judgeLogicalPhase decides which layer stopped one phase. An allocator
// refusal needs every allocator marker, an exited OSD and room left in the
// filesystem. A policy block needs OSD_FULL, a running OSD with no allocator
// marker, and writes that were held (timed out), never acknowledged after the
// first miss.
func judgeLogicalPhase(p *logicalPhase) (string, error) {
	if p == nil {
		return "", errors.New("logical phase is nil")
	}
	if len(p.Ledger) == 0 {
		return "", errors.New("the phase wrote nothing")
	}
	if p.BlockSize <= 0 || p.BlockAllocated < 0 || p.BlockAllocated > p.BlockSize {
		return "", fmt.Errorf("block allocation %d of %d is not a sparse file's", p.BlockAllocated, p.BlockSize)
	}
	first := slices.IndexFunc(p.Ledger, func(m attachMutation) bool { return m.Outcome != mutAcked })
	if first < 0 {
		return logicalUnreached, nil
	}
	if p.FSAvail < logicalFSMargin {
		return logicalFilesystemFirst, nil
	}
	allocator := allocatorRefused(p.Log)
	switch {
	case allocator && !p.Running:
		return logicalAllocatorAborted, nil
	case allocator:
		return "", errors.New("the allocator refused a write but the OSD is still running")
	case !p.Running:
		return "", errors.New("the OSD exited without an allocator refusal")
	}
	if !slices.Contains(p.Checks, "OSD_FULL") {
		return logicalOther, nil
	}
	for _, m := range p.Ledger[first:] {
		if m.Outcome == mutAcked {
			return "", fmt.Errorf("%s was acknowledged after the policy refused %s", m.Key, p.Ledger[first].Key)
		}
		if m.Outcome != mutUnknown {
			return "", fmt.Errorf("%s was refused outright; a full OSDMap holds writes", m.Key)
		}
	}
	return logicalPolicyBlocked, nil
}

// parseHealthChecks returns the codes of `ceph health detail --format json`.
func parseHealthChecks(data []byte) ([]string, error) {
	var health struct {
		Status *string                     `json:"status"`
		Checks map[string]*json.RawMessage `json:"checks"`
		Mutes  json.RawMessage             `json:"mutes"`
	}
	if err := decodeStrict(data, &health); err != nil {
		return nil, fmt.Errorf("health detail: %w", err)
	}
	if health.Status == nil || health.Checks == nil {
		return nil, errors.New("health detail: status or checks missing")
	}
	codes := make([]string, 0, len(health.Checks))
	for code := range health.Checks {
		codes = append(codes, code)
	}
	slices.Sort(codes)
	return codes, nil
}

// bluefsUsage is the part of `perf dump bluefs` that says where BlueFS keeps
// its files: the DB and WAL devices and the shared main ("slow") device.
type bluefsUsage struct {
	DBTotal, DBUsed, WALTotal, WALUsed, SlowUsed int64
}

func parseBlueFSUsage(data []byte) (bluefsUsage, error) {
	var dump struct {
		BlueFS *struct {
			DBTotal  *int64 `json:"db_total_bytes"`
			DBUsed   *int64 `json:"db_used_bytes"`
			WALTotal *int64 `json:"wal_total_bytes"`
			WALUsed  *int64 `json:"wal_used_bytes"`
			SlowUsed *int64 `json:"slow_used_bytes"`
		} `json:"bluefs"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return bluefsUsage{}, fmt.Errorf("bluefs perf dump: %w", err)
	}
	b := dump.BlueFS
	if b == nil || b.DBTotal == nil || b.DBUsed == nil || b.WALTotal == nil || b.WALUsed == nil || b.SlowUsed == nil {
		return bluefsUsage{}, errors.New("bluefs perf dump: a device counter is missing")
	}
	return bluefsUsage{DBTotal: *b.DBTotal, DBUsed: *b.DBUsed, WALTotal: *b.WALTotal, WALUsed: *b.WALUsed, SlowUsed: *b.SlowUsed}, nil
}

// Outcomes of the separate DB/WAL run.
const (
	dbSpilled     = "db-spilled-over"  // the DB device filled and BlueFS moved metadata to the main device
	dbNotFilled   = "db-not-exhausted" // the DB device kept room and nothing spilled
	dbUnexplained = "slow-use-without-full-db"
)

// dbSpillSlack is how close to its size the DB device must be before slow use
// counts as spillover: BlueFS allocates in large units and keeps a reserve.
const dbSpillSlack = int64(16 << 20)

// judgeDBCapacity compares BlueFS usage before and after the metadata
// workload. Spillover needs the DB and WAL devices that the run configured,
// slow use that grew, a DB device within dbSpillSlack of its size, and the
// BLUEFS_SPILLOVER check. Every write must have been acknowledged and the
// OSD must still run: spillover is not a refusal.
func judgeDBCapacity(dbSize, walSize int64, before, after bluefsUsage, checks []string, acked, written int, running bool) (string, error) {
	if before.DBTotal != dbSize || after.DBTotal != dbSize || before.WALTotal != walSize || after.WALTotal != walSize {
		return "", fmt.Errorf("BlueFS devices are db=%d wal=%d, want db=%d wal=%d", after.DBTotal, after.WALTotal, dbSize, walSize)
	}
	if !running || acked != written {
		return "", fmt.Errorf("%d of %d metadata writes acknowledged, OSD running=%t", acked, written, running)
	}
	grew := after.SlowUsed > before.SlowUsed
	full := after.DBTotal-after.DBUsed <= dbSpillSlack
	switch {
	case grew && full && slices.Contains(checks, "BLUEFS_SPILLOVER"):
		return dbSpilled, nil
	case grew && !full:
		return dbUnexplained, nil
	case !grew && !slices.Contains(checks, "BLUEFS_SPILLOVER"):
		return dbNotFilled, nil
	}
	return "", fmt.Errorf("spillover evidence disagrees: slow grew=%t db full=%t checks=%v", grew, full, checks)
}

// The MON store's storage domain.
const (
	monStoreEngineDisk = "engine-disk"      // the container's writable layer: shared with every other container
	monStoreOther      = "other-filesystem" // a separate mount, whose ownership this receipt does not show
)

// monStoreDomain reads the block backing receipt taken for the MON's store.db
// and data directory: both on one mount, and an overlay root is the engine's
// disk, which the fixture must never fill.
func monStoreDomain(r *blockBackingReceipt) (string, error) {
	if *r.Block.Errno != 0 || *r.Scratch.Errno != 0 {
		return "", errors.New("the MON store could not be examined")
	}
	if *r.Block.Dev != *r.Scratch.Dev || *r.Block.Mount.ID != *r.Scratch.Mount.ID {
		return "", errors.New("store.db and the MON data directory are on different filesystems")
	}
	if *r.Block.Mount.FSType == "overlay" {
		return monStoreEngineDisk, nil
	}
	return monStoreOther, nil
}
