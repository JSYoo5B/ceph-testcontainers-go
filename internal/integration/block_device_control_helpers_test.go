//go:build all || integration

package integration_test

import "github.com/JSYoo5B/ceph-testcontainers-go/internal/blockio"

// These aliases keep the existing model and native fixtures on the production
// state machine and keeper control script. Direct I/O is a test-only extension.
const blockControlScript = blockio.TestScript

type dmControlReply = blockio.View
type dmRecord = blockio.Record
type loopIdentity = blockio.LoopIdentity

const (
	dmGone            = blockio.Gone
	dmIdentityChanged = blockio.IdentityChanged
	dmForeign         = blockio.Foreign
	dmStepUnsettled   = blockio.StepUnsettled
	dmApplied         = blockio.Applied
	dmUnchanged       = blockio.Unchanged
	dmLoaded          = blockio.Loaded
	dmSuspended       = blockio.Suspended
	dmSuspendedLoaded = blockio.SuspendedLoaded
	dmSwappedOnHold   = blockio.SwappedOnHold
	dmLoadRefused     = blockio.LoadRefused
	dmReleasable      = blockio.Releasable
	dmReleaseHeld     = blockio.ReleaseHeld
)

var (
	parseDMControlReply = blockio.Parse
	normTable           = blockio.Normalize
	classifyDM          = blockio.Classify
	dmForward           = blockio.Forward
	dmRefuses           = blockio.Refuses
	judgeDMRelease      = blockio.JudgeRelease
	judgeLoopRelease    = blockio.JudgeLoopRelease
)
