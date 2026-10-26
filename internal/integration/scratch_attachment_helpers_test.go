//go:build all || integration

package integration_test

import (
	"errors"
	"fmt"
	"sort"
)

// Outcome of one attached-OSD capacity run on the owned tmpfs (issue 63). The
// judge below only combines receipts that earlier slices already classify; it
// observes nothing itself and has no native producer yet.
const (
	attachNotAttached      = "not-attached"                // the OSD store is not shown to be on the owned filesystem
	attachAllocUnproved    = "allocation-unproved"         // the block backing may already cover its length
	attachUnreached        = "unreached"                   // no byte-ceiling refusal was produced
	attachNotByteCeiling   = "fill-not-byte-ceiling"       // the refusal was another errno, an inode limit or unclassified
	attachRecovered        = "exhausted-recovered"         // byte ceiling, released, daemon up, acknowledged data intact
	attachNoReclaim        = "exhausted-nothing-reclaimed" // byte ceiling, but the filler owned no allocation, so no capacity was returned
	attachDaemonAborted    = "exhausted-aborted"           // byte ceiling, released, daemon exited; readback not attempted
	attachDaemonIndefinite = "exhausted-daemon-unknown"    // byte ceiling, released, daemon state not determined
)

// Daemon states observed after the filler was released.
const (
	daemonUp      = "up"
	daemonAborted = "aborted" // process exited on its own (for example a BlueFS allocation abort)
	daemonUnknown = "unknown" // killed by OOM, lost mount or no result: not a capacity outcome
)

// Mutation outcomes in the ledger. Only acknowledged ones are verified.
const (
	mutAcked   = "acked"
	mutUnknown = "unknown" // completion not observed: classified, never replayed or promoted
	mutFailed  = "failed"
)

type attachMutation struct {
	Key     string
	Outcome string
	SHA256  string // digest of the bytes the client sent
}

// attachRun gathers every input the judge needs. Empty strings, nil receipts
// and a nil Recovered map mean "not observed" and make the run inconclusive
// instead of counting as evidence.
type attachRun struct {
	Link    string    // classifyStoreLink verdict
	Backing string    // classifyBlockBacking verdict
	Policy  [2]string // canonical full-ratio rendering before the fill and when recovery is evaluated

	Fill    *scratchFillReceipt
	Release *scratchRelease
	SeedSHA string
	// WriterJoined is true only when the owned writer process was confirmed
	// exited before the identified filler was unlinked.
	WriterJoined bool

	// Keeper identity from the generation guard: the start instant recorded at
	// attachment, the one observed when recovery is evaluated, and whether the
	// keeper is running at that point. A restarted keeper empties the volume
	// and would manufacture a recovery.
	KeeperStarted [2]string
	KeeperRunning bool

	Daemon string
	Ledger []attachMutation
	// Recovered maps the key to the digest read back after release. It is only
	// consulted when the daemon is up.
	Recovered map[string]string
}

// judgeScratchAttachment returns one outcome or an error when the run cannot
// support any capacity claim. It never reads a later readback as a repair of
// an earlier failure: acknowledged data that differs is an error, an unknown
// mutation is neither required nor promoted, and a keeper restart refuses the
// run before any recovery is considered.
func judgeScratchAttachment(r *attachRun) (string, error) {
	if r == nil {
		return "", errors.New("attachment run is nil")
	}
	if r.Link == "" || r.Backing == "" {
		return "", errors.New("store link or block backing verdict is missing")
	}
	if r.Link != storeLinkSame {
		return attachNotAttached, nil
	}
	switch r.Backing {
	case blockBackingElsewhere, blockBackingNotFile:
		return attachNotAttached, nil
	case blockBackingSparse, blockBackingBelowLength:
	case blockBackingAtLeastLen:
		return attachAllocUnproved, nil
	default:
		return "", fmt.Errorf("block backing verdict %q is not recognized", r.Backing)
	}
	if r.Policy[0] == "" || r.Policy[1] == "" {
		return "", errors.New("fullness policy was not recorded at both points")
	}
	if r.Policy[0] != r.Policy[1] {
		return "", errors.New("fullness policy changed during the run; the refusal cannot be attributed to capacity")
	}
	if r.Fill == nil {
		return "", errors.New("fill receipt is missing")
	}
	fill, err := classifyScratchFill(r.Fill)
	if err != nil {
		return "", fmt.Errorf("fill: %w", err)
	}
	switch fill {
	case scratchUnreached:
		return attachUnreached, nil
	case scratchByteCeiling:
	case scratchCancelled, scratchInodeLimit, scratchUnclassifiedNS, scratchOtherErrno:
		return attachNotByteCeiling, nil
	default:
		return "", fmt.Errorf("fill verdict %q is not recognized", fill)
	}
	if !r.WriterJoined {
		return "", errors.New("the writer was not confirmed exited before the filler was unlinked")
	}
	if r.Release == nil || r.SeedSHA == "" {
		return "", errors.New("release receipt or seed digest is missing")
	}
	if err := checkScratchRelease(r.Fill, r.Release, r.SeedSHA); err != nil {
		return "", fmt.Errorf("release: %w", err)
	}
	// An already-full filesystem can refuse the first write, leaving a filler
	// with no allocation. Unlinking it frees nothing (0 >= 0 passes the release
	// check), so no recovery claim can rest on it.
	if *r.Fill.File.Allocated <= 0 || (*r.Release.VFSAfter.BAvail-*r.Release.VFSBefore.BAvail)*(*r.Release.VFSAfter.BSize) <= 0 {
		return attachNoReclaim, nil
	}
	if r.KeeperStarted[0] == "" || r.KeeperStarted[1] == "" || !r.KeeperRunning {
		return "", errors.New("keeper generation or liveness is not recorded")
	}
	if r.KeeperStarted[0] != r.KeeperStarted[1] {
		return "", errors.New("keeper restarted; a recovery would not be attributable to the release")
	}
	switch r.Daemon {
	case daemonAborted:
		return attachDaemonAborted, nil
	case daemonUnknown:
		return attachDaemonIndefinite, nil
	case daemonUp:
	default:
		return "", fmt.Errorf("daemon state %q is not recognized", r.Daemon)
	}
	var lost []string
	acked := 0
	for _, m := range r.Ledger {
		switch m.Outcome {
		case mutAcked:
			if m.Key == "" || m.SHA256 == "" {
				return "", errors.New("acknowledged mutation lacks a key or digest")
			}
			acked++
			if got, ok := r.Recovered[m.Key]; !ok || got != m.SHA256 {
				lost = append(lost, m.Key)
			}
		case mutUnknown, mutFailed:
		default:
			return "", fmt.Errorf("mutation outcome %q is not recognized", m.Outcome)
		}
	}
	if acked == 0 {
		return "", errors.New("no acknowledged mutation to verify after release")
	}
	if len(lost) > 0 {
		sort.Strings(lost)
		return "", fmt.Errorf("acknowledged mutations missing or different after release: %v", lost)
	}
	return attachRecovered, nil
}
