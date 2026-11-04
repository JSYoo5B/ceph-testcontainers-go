package expect

import (
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"strings"
)

// Image states what a test expects of one mirrored image in one
// mirror.ImageStatus observation.
//
// Select names the image and pins its identities, usually from an earlier
// observation of the same image. Name is required, and SourceNamespace and
// DestinationNamespace are always compared ("" is the default namespace).
// Pool and Mode are compared when set. A non-empty GlobalID, SourceImageID or
// DestinationImageID pins that native identity, so a re-created image of the
// same name does not match. Select.InstanceID is read only with NewInstance;
// the other Select fields are not read.
//
// Replaying true requires an owned receiver to replay the
// source-primary/destination-secondary pair: ReplayReady, the native state
// up+replaying, both sides' mirroring enabled, a primary source and a
// non-primary destination. Replaying false requires only that ReplayReady is
// false; a stale native up+replaying report is then allowed. Daemon, when set,
// names the owned receiver that must report the replay. NewInstance requires a
// receiving instance other than Select.InstanceID, as after a receiver restart
// or replacement.
//
// A match proves neither the arrival of a particular write nor the
// completion of a mirror snapshot: verify the destination data or the
// application's checkpoint separately.
type Image struct {
	Select      mirror.ImageStatus
	Replaying   bool
	Daemon      string
	NewInstance bool
}

// ImageMismatchReason names why an image does not satisfy an Image
// expectation.
type ImageMismatchReason string

// The reasons an ImageEvaluation reports.
const (
	// ImageMismatchScope: the observation describes another pool, namespace
	// mapping or image name. No other field is compared.
	ImageMismatchScope ImageMismatchReason = "scope"
	// ImageMismatchMode: the mirroring mode differs.
	ImageMismatchMode ImageMismatchReason = "mode"
	// ImageMismatchGlobalID: the native global mirror image ID differs from the
	// pinned one: the pair was re-created.
	ImageMismatchGlobalID ImageMismatchReason = "global-id"
	// ImageMismatchSourceImage: the source image ID differs from the pinned
	// one: the source was re-created.
	ImageMismatchSourceImage ImageMismatchReason = "source-image"
	// ImageMismatchDestinationImage: the destination image ID differs from the
	// pinned one, or no destination image is reported yet.
	ImageMismatchDestinationImage ImageMismatchReason = "destination-image"
	// ImageMismatchReplay: ReplayReady differs from Replaying.
	ImageMismatchReplay ImageMismatchReason = "replay"
	// ImageMismatchState: a Replaying expectation found a native state other
	// than up+replaying.
	ImageMismatchState ImageMismatchReason = "state"
	// ImageMismatchMirrorState: a Replaying expectation found the source's or
	// the destination's mirroring not enabled.
	ImageMismatchMirrorState ImageMismatchReason = "mirror-state"
	// ImageMismatchPrimary: a Replaying expectation found the source not
	// primary or the destination primary.
	ImageMismatchPrimary ImageMismatchReason = "primary"
	// ImageMismatchDaemon: another owned receiver, or none, reports the replay.
	ImageMismatchDaemon ImageMismatchReason = "daemon"
	// ImageMismatchInstance: a NewInstance expectation found the pinned
	// receiving instance, or none.
	ImageMismatchInstance ImageMismatchReason = "instance"
)

// ImageMismatch is one reason an image does not satisfy an expectation. Want
// and Observed are the compared values as text: the scope as
// pool/source:destination/name for ImageMismatchScope, the mirroring states as
// source/destination for ImageMismatchMirrorState and the primary flags as
// source/destination for ImageMismatchPrimary; for ImageMismatchInstance, Want
// is the instance the observation must differ from.
type ImageMismatch struct {
	Reason         ImageMismatchReason
	Want, Observed string
}

// String names the reason and the evaluated values.
func (m ImageMismatch) String() string {
	if m.Reason == ImageMismatchInstance {
		return fmt.Sprintf("instance: want other than %s, observed %s", m.Want, quoteEmpty(m.Observed))
	}
	return fmt.Sprintf("%s: want %s, observed %s", m.Reason, quoteEmpty(m.Want), quoteEmpty(m.Observed))
}

// ImageEvaluation is the result of evaluating one observation. Expectation and
// Observed are copies of what was evaluated. Mismatches are in the order of
// the reason constants; Matched means there are none. A scope mismatch is the
// only one reported, and a mode or pinned-identity mismatch suppresses the
// replay comparisons, since the observation describes another image.
type ImageEvaluation struct {
	Expectation Image
	Observed    mirror.ImageStatus
	Mismatches  []ImageMismatch
	Matched     bool
}

// String summarizes the expectation and at most eight mismatches.
func (e ImageEvaluation) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "image %s want replaying=%t", imageScope(e.Expectation.Select), e.Expectation.Replaying)
	if e.Expectation.Daemon != "" {
		fmt.Fprintf(&b, " daemon=%s", e.Expectation.Daemon)
	}
	if e.Expectation.NewInstance {
		fmt.Fprintf(&b, " new instance")
	}
	b.WriteString(": ")
	if e.Matched {
		b.WriteString("match")
		return b.String()
	}
	for i, m := range e.Mismatches {
		if i == evaluationShown {
			fmt.Fprintf(&b, "; %d more", len(e.Mismatches)-i)
			break
		}
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(m.String())
	}
	return b.String()
}

// Evaluate checks one mirror.ImageStatus observation (from ImageStatus,
// WaitReplayReady or an image watch event) against the expectation. It reads
// only its arguments: no query, wait or retry, so a test composes it with its
// own polling or assertion helpers. A mismatch is a result; an error means no
// verdict. A Select without a plain image name or with an unknown Mode, a
// Daemon or NewInstance without Replaying, and NewInstance without a
// Select.InstanceID are ceph.ErrInvalidRequest. An observation without an
// image name, or one that claims ReplayReady without the image identities and
// the receiving daemon and instance, is a malformed observation, never a
// mismatch.
//
//purpose:observation
func (e Image) Evaluate(observed mirror.ImageStatus) (ImageEvaluation, error) {
	if err := e.validate(); err != nil {
		return ImageEvaluation{}, err
	}
	if observed.Name == "" {
		return ImageEvaluation{}, fmt.Errorf("malformed image observation: no image name")
	}
	if observed.ReplayReady && (observed.GlobalID == "" || observed.SourceImageID == "" || observed.DestinationImageID == "" || observed.DaemonName == "" || observed.InstanceID == "") {
		return ImageEvaluation{}, fmt.Errorf("malformed image observation: replay-ready without image identities or a receiving instance")
	}
	result := ImageEvaluation{Expectation: e, Observed: observed}
	result.Mismatches = e.mismatches(observed)
	result.Matched = len(result.Mismatches) == 0
	return result, nil
}

func (e Image) validate() error {
	name := e.Select.Name
	if name == "" || strings.TrimSpace(name) != name || strings.HasPrefix(name, "-") || strings.ContainsAny(name, "/@\x00\t\r\n") {
		return fmt.Errorf("%w: image expectation needs an image name without pool, namespace, snapshot or option syntax", ceph.ErrInvalidRequest)
	}
	switch e.Select.Mode {
	case "", mirror.ModeSnapshot, mirror.ModeJournal:
	default:
		return fmt.Errorf("%w: image expectation selects unknown mode %q", ceph.ErrInvalidRequest, e.Select.Mode)
	}
	if (e.Daemon != "" || e.NewInstance) && !e.Replaying {
		return fmt.Errorf("%w: image expectation names a receiving daemon or instance without replay", ceph.ErrInvalidRequest)
	}
	if e.NewInstance && e.Select.InstanceID == "" {
		return fmt.Errorf("%w: image expectation wants a new instance without a pinned one", ceph.ErrInvalidRequest)
	}
	return nil
}

func (e Image) mismatches(observed mirror.ImageStatus) []ImageMismatch {
	s := e.Select
	if (s.Pool != "" && observed.Pool != s.Pool) || observed.SourceNamespace != s.SourceNamespace || observed.DestinationNamespace != s.DestinationNamespace || observed.Name != s.Name {
		want := s
		if want.Pool == "" {
			want.Pool = observed.Pool
		}
		return []ImageMismatch{{Reason: ImageMismatchScope, Want: imageScope(want), Observed: imageScope(observed)}}
	}
	var result []ImageMismatch
	pin := func(reason ImageMismatchReason, want, got string) {
		if want != "" && got != want {
			result = append(result, ImageMismatch{Reason: reason, Want: want, Observed: got})
		}
	}
	pin(ImageMismatchMode, string(s.Mode), string(observed.Mode))
	pin(ImageMismatchGlobalID, s.GlobalID, observed.GlobalID)
	pin(ImageMismatchSourceImage, s.SourceImageID, observed.SourceImageID)
	pin(ImageMismatchDestinationImage, s.DestinationImageID, observed.DestinationImageID)
	if len(result) != 0 {
		return result
	}
	if observed.ReplayReady != e.Replaying {
		result = append(result, ImageMismatch{Reason: ImageMismatchReplay, Want: fmt.Sprint(e.Replaying), Observed: fmt.Sprint(observed.ReplayReady)})
	}
	if !e.Replaying {
		return result
	}
	if observed.State != "up+replaying" {
		result = append(result, ImageMismatch{Reason: ImageMismatchState, Want: "up+replaying", Observed: observed.State})
	}
	if observed.SourceMirrorState != "enabled" || observed.DestinationMirrorState != "enabled" {
		result = append(result, ImageMismatch{Reason: ImageMismatchMirrorState, Want: "enabled/enabled", Observed: quoteEmpty(observed.SourceMirrorState) + "/" + quoteEmpty(observed.DestinationMirrorState)})
	}
	if !observed.SourcePrimary || observed.DestinationPrimary {
		result = append(result, ImageMismatch{Reason: ImageMismatchPrimary, Want: "primary/non-primary", Observed: primaryText(observed.SourcePrimary) + "/" + primaryText(observed.DestinationPrimary)})
	}
	if e.Daemon != "" && observed.DaemonName != e.Daemon {
		result = append(result, ImageMismatch{Reason: ImageMismatchDaemon, Want: e.Daemon, Observed: observed.DaemonName})
	}
	if e.NewInstance && (observed.InstanceID == "" || observed.InstanceID == s.InstanceID) {
		result = append(result, ImageMismatch{Reason: ImageMismatchInstance, Want: s.InstanceID, Observed: observed.InstanceID})
	}
	return result
}

func imageScope(s mirror.ImageStatus) string {
	pool := s.Pool
	if pool == "" {
		pool = "*"
	}
	return fmt.Sprintf("%s/%s:%s/%s", pool, s.SourceNamespace, s.DestinationNamespace, s.Name)
}

func primaryText(primary bool) string {
	if primary {
		return "primary"
	}
	return "non-primary"
}
