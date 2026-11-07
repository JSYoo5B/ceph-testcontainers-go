// Package capture records packets in one container's network namespace with
// an opt-in packet-capture helper, retrieves the classic pcap file and checks
// its structure. It never interprets the packets: a structurally valid file
// does not show that the intended traffic was captured, that nothing was
// dropped, or that any protocol message in it is valid.
//
// The helper meets contract 1 of the packet-capture extension of
// ceph-testcontainers-images. Callers explicitly select a helper image that
// meets that contract; the package never selects one by itself.
package capture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/nshelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/pcapfile"
	"github.com/containerd/errdefs"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// MaxSnapLen bounds Request.SnapLen: libpcap's largest default snaplen.
	MaxSnapLen = 262144
	// MaxFileBytes bounds the capture file that a request allows:
	// 24 + MaxPackets × (16 + SnapLen) bytes, as the helper contract derives
	// it.
	MaxFileBytes = 1 << 30
	// MaxFilterBytes bounds Request.Filter.
	MaxFilterBytes = 4096
	// DefaultReadyTimeout is the readiness wait of a zero Request.ReadyTimeout.
	DefaultReadyTimeout = 30 * time.Second
	// DefaultStopGrace is the graceful-stop wait of a zero Request.StopGrace.
	DefaultStopGrace = 10 * time.Second
	// MaxTimeout bounds Request.ReadyTimeout and Request.StopGrace.
	MaxTimeout = 5 * time.Minute
)

var (
	// ErrImageUnsupported reports a helper image that does not meet the
	// capture contract: a missing sh, sleep, tcpdump or python3, a tcpdump
	// that does not run or compile a filter, or a helper that runs as root.
	// It belongs to ceph.ErrImageCapability.
	ErrImageUnsupported error = nshelper.Category("the capture helper image does not meet the capture contract", ceph.ErrImageCapability)
	// ErrPermissionDenied reports an engine that did not let the helper's
	// tcpdump run or open its device with NET_RAW. It belongs to
	// ceph.ErrEnvironmentUnavailable.
	ErrPermissionDenied error = nshelper.Category("the capture helper was not permitted to capture packets", ceph.ErrEnvironmentUnavailable)
	// ErrFailed reports a capture that did not start, end or deliver as the
	// contract says: tcpdump exited before readiness or with a nonzero
	// status, did not end within the stop grace, the target's network
	// namespace changed, the helper's file is missing or non-regular, or it is not
	// a valid classic pcap. It belongs to ceph.ErrNativeFailure.
	ErrFailed error = nshelper.Category("the packet capture failed", ceph.ErrNativeFailure)
)

// Request selects one capture. Every field except the two timeouts is
// required: the package chooses no image, interface, filter or bound.
type Request struct {
	// HelperImage is an image that meets contract 1 of the packet-capture
	// extension.
	HelperImage string
	// Network is the user-defined bridge network through which the target's
	// endpoint is selected; bridge, host and none are refused.
	Network string
	// Interface is the target's interface on Network, inside the target's
	// network namespace, such as "eth0". Start checks that its MAC address
	// is that of the endpoint; any and lo are refused.
	Interface string
	// Filter is a pcap-filter expression that selects the traffic. It is
	// compiled before the capture starts.
	Filter string
	// SnapLen bounds the bytes kept of each packet: 1 to MaxSnapLen.
	SnapLen int
	// MaxPackets ends the capture after that many packets. With SnapLen it
	// bounds the file to 24 + MaxPackets × (16 + SnapLen) bytes, at most
	// MaxFileBytes.
	MaxPackets int
	// Output is the path of the file that Stop writes the capture to. It must
	// not exist; Start creates it, and its directory must exist, so parallel
	// or repeated tests need distinct paths.
	Output string
	// ReadyTimeout bounds the wait for readiness; zero is
	// DefaultReadyTimeout.
	ReadyTimeout time.Duration
	// StopGrace bounds the wait for tcpdump to end after SIGTERM; zero is
	// DefaultStopGrace. After it, Stop kills the helper.
	StopGrace time.Duration
}

// maxBytes is the contract's file bound for the request.
func (r Request) maxBytes() int64 {
	return 24 + int64(r.MaxPackets)*(16+int64(r.SnapLen))
}

var interfaceName = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9_.@-]{0,14}\z`)

func (r Request) validate() error {
	if strings.TrimSpace(r.HelperImage) == "" {
		return &ceph.OperationError{Kind: ceph.ErrImageCapability,
			Err: errors.New("capture needs Request.HelperImage: the capture helper is never selected implicitly")}
	}
	switch r.Network {
	case "":
		return nshelper.InvalidRequest("capture needs Request.Network")
	case "host", "none", "bridge":
		return nshelper.InvalidRequest("capture needs a user-defined bridge network, not %q", r.Network)
	}
	switch {
	case !interfaceName.MatchString(r.Interface):
		return nshelper.InvalidRequest("capture needs Request.Interface, an interface name of at most 15 characters")
	case r.Interface == "any" || r.Interface == "lo":
		return nshelper.InvalidRequest("capture needs the target's interface on %s, not %q", r.Network, r.Interface)
	case strings.TrimSpace(r.Filter) == "":
		return nshelper.InvalidRequest("capture needs an explicit Request.Filter")
	case len(r.Filter) > MaxFilterBytes:
		return nshelper.InvalidRequest("the capture filter is longer than %d bytes", MaxFilterBytes)
	case strings.ContainsRune(r.Filter, 0):
		return nshelper.InvalidRequest("the capture filter contains a NUL byte")
	case r.SnapLen < 1 || r.SnapLen > MaxSnapLen:
		return nshelper.InvalidRequest("capture SnapLen must be 1 to %d", MaxSnapLen)
	// Compared before multiplying, so that a large MaxPackets cannot wrap
	// the file bound.
	case r.MaxPackets < 1 || int64(r.MaxPackets) > (MaxFileBytes-24)/(16+int64(r.SnapLen)):
		return nshelper.InvalidRequest("capture MaxPackets must be positive, with a file bound of at most %d bytes", MaxFileBytes)
	case r.Output == "":
		return nshelper.InvalidRequest("capture needs Request.Output")
	case r.ReadyTimeout < 0 || r.ReadyTimeout > MaxTimeout:
		return nshelper.InvalidRequest("capture ReadyTimeout must be 0 to %s", MaxTimeout)
	case r.StopGrace < 0 || r.StopGrace > MaxTimeout:
		return nshelper.InvalidRequest("capture StopGrace must be 0 to %s", MaxTimeout)
	}
	return nil
}

// EndReason says how the capture process ended.
type EndReason string

const (
	// EndStopped means tcpdump ended with status 0 after Stop's SIGTERM.
	EndStopped EndReason = "stopped"
	// EndPacketLimit means tcpdump ended with status 0 before Stop, and the
	// valid file holds Request.MaxPackets packets (as does tcpdump's
	// captured counter when it printed one): later traffic is not in the
	// file.
	EndPacketLimit EndReason = "packet_limit"
	// EndPremature means tcpdump ended with status 0 before Stop, but the
	// file and its counter do not show that it reached Request.MaxPackets,
	// as after a signal from elsewhere: traffic after its end is missing.
	EndPremature EndReason = "premature"
	// EndKilled means tcpdump did not end within the stop grace and the
	// helper was killed; the file may hold only part of the capture.
	EndKilled EndReason = "killed"
	// EndFailed means tcpdump ended with a nonzero status.
	EndFailed EndReason = "failed"
	// EndUnknown means the end could not be observed, or that tcpdump ended
	// with status 0 before Stop and no valid file was retrieved to tell
	// EndPacketLimit from EndPremature.
	EndUnknown EndReason = "unknown"
)

// TargetState compares the target at Stop with the one the capture joined.
type TargetState string

const (
	// TargetUnchanged means the same container instance and network sandbox.
	TargetUnchanged TargetState = "unchanged"
	// TargetChanged means the target restarted, stopped or got another
	// sandbox: the capture covers only the namespace it joined.
	TargetChanged TargetState = "changed"
	// TargetGone means the target container no longer exists.
	TargetGone TargetState = "gone"
	// TargetUnknown means the target could not be inspected.
	TargetUnknown TargetState = "unknown"
)

// FileStatus is the structural verdict on the retrieved file.
type FileStatus string

const (
	// FileValid means every record header and its data fit the file. A
	// valid file can hold zero packets.
	FileValid FileStatus = "valid"
	// FileUnsupported means a recognized format that is not checked, such
	// as pcapng.
	FileUnsupported FileStatus = "unsupported"
	// FileMalformed means the file is not classic pcap or breaks its
	// structure, including a final record that was cut off.
	FileMalformed FileStatus = "malformed"
	// FileLimitExceeded means the helper's file is larger than the
	// request's bound; only the first MaxBytes were kept and checked.
	FileLimitExceeded FileStatus = "limit_exceeded"
)

// Statistics are tcpdump's counters from its standard error. A nil field is
// a counter that tcpdump did not print: unknown, not zero.
type Statistics struct {
	Captured           *uint64 `json:"captured,omitempty"`
	ReceivedByFilter   *uint64 `json:"received_by_filter,omitempty"`
	DroppedByKernel    *uint64 `json:"dropped_by_kernel,omitempty"`
	DroppedByInterface *uint64 `json:"dropped_by_interface,omitempty"`
}

// FileReport is the structural check of the retrieved file. Counts cover
// the records read completely before the verdict.
//
// The check reads with two bounds, the file size and one record's captured
// length. A record's declared length is compared with them before anything
// is read, and record data streams through the digest without being held,
// so a hostile length can neither allocate nor read its declared size. Read
// errors are returned as errors, never as a verdict.
type FileReport struct {
	Status FileStatus `json:"status"`
	// Reason explains any status other than FileValid.
	Reason string `json:"reason,omitempty"`
	// ByteOrder is "little" or "big"; TimestampUnit is "microsecond" or
	// "nanosecond".
	ByteOrder     string `json:"byte_order,omitempty"`
	TimestampUnit string `json:"timestamp_unit,omitempty"`
	VersionMajor  uint16 `json:"version_major,omitempty"`
	VersionMinor  uint16 `json:"version_minor,omitempty"`
	SnapLen       uint32 `json:"snaplen,omitempty"`
	LinkType      uint32 `json:"link_type,omitempty"`
	Packets       int    `json:"packets"`
	// TruncatedPackets counts packets captured shorter than their original
	// length: snaplen truncation, not a malformed file.
	TruncatedPackets int `json:"truncated_packets"`
	// BytesRead is how much of the file was checked, and SHA256 is of exactly
	// those bytes. Complete says the check reached the end of the file, so
	// that SHA256 identifies the whole file.
	BytesRead int64  `json:"bytes_read"`
	SHA256    string `json:"sha256"`
	Complete  bool   `json:"complete"`
}

// Report describes one capture: what was requested, what the helper and
// the target did, and what was delivered. Process, target, retrieval and
// file outcomes are separate: a valid file can come from a capture that
// ended early or lost its target.
//
// The report carries JSON field tags for delivery. The pcap itself holds
// payloads, addresses and identifiers that text and JSON redaction do not
// sanitize; keep capture files only where the caller deliberately selects
// them.
type Report struct {
	// ContainerID, Network, Interface and MACAddress identify the captured
	// endpoint; SandboxID, TargetStartedAt and Namespace the network
	// namespace generation that the helper joined.
	ContainerID     string `json:"container_id"`
	Network         string `json:"network"`
	Interface       string `json:"interface"`
	MACAddress      string `json:"mac_address"`
	SandboxID       string `json:"sandbox_id"`
	TargetStartedAt string `json:"target_started_at"`
	Namespace       string `json:"namespace,omitempty"`
	// HelperImage is the requested image, HelperImageID the image the helper
	// ran, HelperPlatform its os/architecture and ToolVersion what
	// tcpdump --version printed.
	HelperImage    string `json:"helper_image"`
	HelperImageID  string `json:"helper_image_id"`
	HelperPlatform string `json:"helper_platform,omitempty"`
	ToolVersion    string `json:"tool_version,omitempty"`
	// Filter, SnapLen, MaxPackets and MaxBytes are the requested selection
	// and bounds; MaxBytes is derived from SnapLen and MaxPackets.
	Filter     string `json:"filter"`
	SnapLen    int    `json:"snaplen"`
	MaxPackets int    `json:"max_packets"`
	MaxBytes   int64  `json:"max_bytes"`
	// StartedAt is when the helper was started, ReadyAt when tcpdump
	// reported that it listens and its file existed, StopRequestedAt when
	// Stop began and EndedAt when the capture process ended.
	StartedAt       time.Time `json:"started_at"`
	ReadyAt         time.Time `json:"ready_at"`
	StopRequestedAt time.Time `json:"stop_requested_at"`
	EndedAt         time.Time `json:"ended_at"`
	// End and ExitCode say how tcpdump ended.
	End      EndReason `json:"end"`
	ExitCode int       `json:"exit_code"`
	// Target compares the target at Stop with the generation joined.
	Target TargetState `json:"target"`
	// Statistics are tcpdump's counters; Stderr is its standard error, up
	// to 64 KiB.
	Statistics Statistics `json:"statistics"`
	Stderr     string     `json:"stderr,omitempty"`
	// Output is the delivered file, Retrieved whether the helper's file was
	// copied into it and RetrievedBytes how many bytes it holds.
	Output         string `json:"output"`
	Retrieved      bool   `json:"retrieved"`
	RetrievedBytes int64  `json:"retrieved_bytes"`
	// RetrievedSHA256 identifies all bytes successfully delivered to Output,
	// independently of the structural check's possibly shorter prefix.
	RetrievedSHA256 string `json:"retrieved_sha256,omitempty"`
	// File is the structural check of Output; its Status is empty when
	// nothing was retrieved.
	File FileReport `json:"file"`
	// HelperRemoved says the helper container was removed.
	HelperRemoved bool `json:"helper_removed"`
}

// Session owns one running capture helper. The caller owns the session and
// calls Stop, also after a failed workload: no cluster Terminate knows it.
// Stop is retryable.
type Session struct {
	mu         sync.Mutex
	backend    backend
	request    Request
	output     *os.File // reserved by Start and held until retrieval finishes
	outputInfo os.FileInfo
	target     nshelper.Target
	helper     string
	// probe is a probe container that could not be removed; Stop retries.
	probe     string
	selfEnded bool
	report    Report
	finished  bool
	err       error
}

// capturePath is the helper's capture file below /tmp, which the contract
// makes writable by the helper's user.
const capturePath = "/tmp/ceph-testcontainers-capture.pcap"

// stderrLimit bounds the helper's diagnostics kept in a Report.
const stderrLimit = 64 << 10

// finishTimeout bounds Stop's own Docker work beyond the stop grace, from a
// context that the caller's cancellation does not end.
const finishTimeout = 2 * time.Minute

var pollInterval = 100 * time.Millisecond

// Start starts a capture in target's network namespace, on its endpoint on
// req.Network, and returns once tcpdump listens there. It is an opt-in
// extension: req.HelperImage names the capture helper, which joins the
// target's namespace (Docker network mode container:<target>) with NET_RAW
// as its only capability and runs as the image's own non-root user. The
// target keeps its image, capabilities and networking; it may be an owned
// Ceph daemon or a caller-owned container attached to that bridge.
// Host-network, stopped or paused targets and targets that share another
// container's namespace are refused.
//
// Before the capture starts, a short-lived probe from the same image in the
// same namespace checks the contract's executables (sh, sleep, tcpdump,
// python3) and non-root user, runs tcpdump --version, compiles the filter
// with tcpdump -y EN10MB -d and reads the interface's MAC address. The
// capture then runs the image ID that the probe ran, not the tag, as
//
//	tcpdump -p -i <Interface> -s <SnapLen> -c <MaxPackets> --immediate-mode -U -w <file> -- <Filter>
//
// A missing HelperImage is
// ceph.ErrImageCapability; a bad request, filter or interface is
// ceph.ErrInvalidRequest; and the errors ErrImageUnsupported,
// ErrPermissionDenied and ErrFailed tell an unsuitable image, a denied
// capability and a capture that did not become ready apart.
//
// Readiness is tcpdump's "listening on <Interface>, link-type EN10MB" line
// together with its output file; a running helper or an existing file alone
// is not. Start the workload of interest only after Start returns. The session is bound to the target's
// container instance and sandbox at that point and never follows a restart.
// On failure Start cleans up its helper. Before removing the reserved
// output it checks that the path still identifies the file it created.
// It returns a non-nil session with an error only when it could not remove
// a probe or a helper that failed; the empty output file then stays, and Stop
// retries the removal.
//
//purpose:evidence
func Start(ctx context.Context, target testcontainers.Container, req Request) (*Session, error) {
	return start(ctx, dockerBackend{}, target, req)
}

func start(ctx context.Context, b backend, target testcontainers.Container, req Request) (*Session, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	if target == nil || target.GetContainerID() == "" {
		return nil, nshelper.InvalidRequest("capture requires a container")
	}
	id := target.GetContainerID()
	state, err := b.target(ctx, id, req.Network)
	if err != nil {
		return nil, classifyDocker(ctx, fmt.Errorf("inspect the capture target: %w", err))
	}
	if err := admitTarget(state, req.Network); err != nil {
		return nil, err
	}
	output, err := os.OpenFile(req.Output, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, nshelper.InvalidRequest("create the capture output: %w", err)
	}
	info, err := output.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("stat the reserved capture output: %w", err), output.Close())
	}
	s := &Session{backend: b, request: req, target: state, output: output, outputInfo: info}
	s.report = Report{
		ContainerID: id, Network: req.Network, Interface: req.Interface, MACAddress: state.MAC,
		SandboxID: state.SandboxID, TargetStartedAt: state.StartedAt, HelperImage: req.HelperImage,
		Filter: req.Filter, SnapLen: req.SnapLen, MaxPackets: req.MaxPackets, MaxBytes: req.maxBytes(),
		Output: req.Output, End: EndUnknown, Target: TargetUnknown,
	}
	if err := s.preflight(ctx, id); err != nil {
		if s.probe != "" {
			// The probe is still owned: Stop retries its removal.
			s.finished = true
			return s, errors.Join(err, s.closeOutput())
		}
		return nil, errors.Join(err, s.removeReservedOutput(), s.closeOutput())
	}
	if err := s.startHelper(ctx, id); err != nil {
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finishTimeout)
		defer cancel()
		removeErr := s.abort(finishCtx)
		if removeErr != nil {
			return s, errors.Join(err, removeErr, s.closeOutput())
		}
		return nil, errors.Join(err, s.removeReservedOutput(), s.closeOutput())
	}
	return s, nil
}

func (s *Session) closeOutput() error {
	err := s.output.Close()
	s.output = nil
	return err
}

// checkOutputIdentity compares the caller's path with the file reserved at Start.
// All writes use the held descriptor even if the path moves after this check.
func (s *Session) checkOutputIdentity() error {
	info, err := os.Lstat(s.request.Output)
	if err != nil {
		return err
	}
	if !os.SameFile(info, s.outputInfo) {
		return errors.New("capture output path no longer names the reserved file")
	}
	return nil
}

func (s *Session) removeReservedOutput() error {
	if err := s.checkOutputIdentity(); err != nil {
		return err
	}
	return os.Remove(s.request.Output)
}

// admitTarget refuses targets whose endpoint a helper cannot join.
func admitTarget(state nshelper.Target, network string) error {
	return nshelper.Admit(state, "capture", "the capture target", func(state nshelper.Target) error {
		if state.MAC == "" {
			return nshelper.InvalidRequest("the capture target is not attached to %s", network)
		}
		return nil
	})
}

// preflight runs the probe in the target's namespace and records the image.
// The probe is removed with a fresh bounded context; when that fails, the
// session keeps it for Stop.
func (s *Session) preflight(ctx context.Context, id string) error {
	result, probe, err := s.backend.probe(ctx, s.request.HelperImage, "container:"+id, probeArgv(s.request.Interface, s.request.Filter))
	s.probe = probe
	removeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finishTimeout)
	removeErr := s.removeProbe(removeCtx)
	cancel()
	if err != nil {
		return errors.Join(classifyStartError(ctx, "run the capture probe", err), removeErr)
	}
	if removeErr != nil {
		return removeErr
	}
	report, err := parseProbe(result)
	if err != nil {
		return err
	}
	if err := report.check(s.request, s.target.MAC); err != nil {
		return err
	}
	s.report.HelperImageID, s.report.Namespace, s.report.ToolVersion = result.imageID, report.Namespace, report.toolVersion()
	platform, err := s.backend.platform(ctx, result.imageID)
	if err != nil {
		return classifyDocker(ctx, fmt.Errorf("inspect the capture helper image: %w", err))
	}
	s.report.HelperPlatform = platform
	return nil
}

// captureArgv is the contract's capture command. "--" ends the options, so
// that the filter is never read as one.
func captureArgv(req Request) []string {
	return []string{"tcpdump", "-p", "-i", req.Interface, "-s", strconv.Itoa(req.SnapLen), "-c", strconv.Itoa(req.MaxPackets),
		"--immediate-mode", "-U", "-w", capturePath, "--", req.Filter}
}

// startHelper starts tcpdump as the helper's process and waits for
// readiness, then checks that the target is the generation it joined.
func (s *Session) startHelper(ctx context.Context, id string) error {
	readyTimeout := s.request.ReadyTimeout
	if readyTimeout == 0 {
		readyTimeout = DefaultReadyTimeout
	}
	s.report.StartedAt = time.Now().UTC()
	helper, err := s.backend.run(ctx, s.report.HelperImageID, "container:"+id, captureArgv(s.request))
	s.helper = helper
	if err != nil {
		return classifyStartError(ctx, "start the capture helper", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	prefix := "tcpdump: listening on " + s.request.Interface + ","
	for {
		state, err := s.backend.state(waitCtx, helper)
		if err != nil {
			return classifyDocker(waitCtx, fmt.Errorf("inspect the capture helper: %w", err))
		}
		_, stderr, err := s.backend.logs(waitCtx, helper, stderrLimit)
		if err != nil {
			return classifyDocker(waitCtx, fmt.Errorf("read the capture helper's output: %w", err))
		}
		if !state.running {
			return classifyCaptureExit(state.exitCode, stderr)
		}
		if line, ok := readyLine(stderr, prefix); ok {
			if !strings.Contains(line, "link-type EN10MB") {
				return fmt.Errorf("%w: the interface is not Ethernet, for which the filter was compiled: %s", ErrFailed, line)
			}
			exists, err := s.backend.exists(waitCtx, helper, capturePath)
			if err != nil {
				return classifyDocker(waitCtx, fmt.Errorf("check the capture file: %w", err))
			}
			if exists {
				break
			}
		}
		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("%w: tcpdump was not ready within %s", ErrFailed, readyTimeout)
		case <-time.After(pollInterval):
		}
	}
	s.report.ReadyAt = time.Now().UTC()
	current, err := s.backend.target(ctx, id, s.request.Network)
	if err != nil {
		return classifyDocker(ctx, fmt.Errorf("inspect the capture target: %w", err))
	}
	if !sameGeneration(s.target, current) {
		return fmt.Errorf("%w: the target's network namespace changed while the capture started", ErrFailed)
	}
	return nil
}

func readyLine(stderr []byte, prefix string) (string, bool) {
	for _, line := range strings.Split(string(stderr), "\n") {
		if strings.HasPrefix(line, prefix) {
			return line, true
		}
	}
	return "", false
}

// abort ends a helper that did not become ready and removes it. It returns
// only the removal's error; the session then keeps the helper for Stop.
func (s *Session) abort(ctx context.Context) error {
	if s.helper == "" {
		return nil
	}
	s.finished = true
	s.report.End = EndFailed
	if state, err := s.backend.state(ctx, s.helper); err == nil && !state.running {
		s.report.ExitCode = state.exitCode
	}
	if _, stderr, err := s.backend.logs(ctx, s.helper, stderrLimit); err == nil {
		s.report.Stderr = string(stderr)
	}
	return s.removeHelper(ctx)
}

func (s *Session) removeProbe(ctx context.Context) error {
	if s.probe == "" {
		return nil
	}
	if err := s.backend.remove(ctx, s.probe); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove the capture probe: %w", err)
	}
	s.probe = ""
	return nil
}

func (s *Session) removeHelper(ctx context.Context) error {
	if s.report.HelperRemoved || s.helper == "" {
		return nil
	}
	if err := s.backend.remove(ctx, s.helper); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove the capture helper: %w", err)
	}
	s.report.HelperRemoved = true
	return nil
}

// Stop ends the capture with SIGTERM and waits up to the stop grace for
// tcpdump to flush and close its file; after the grace it kills the helper
// (EndKilled). It then compares the target with the joined generation,
// copies the helper's file into the file reserved at Start, checks its structure and
// removes the helper. Its Docker work uses a fresh bounded context, so a
// canceled or expired ctx does not leave the helper behind. A replaced output
// path fails retrieval. Keep the reserved file at Request.Output until Stop returns.
//
// The report is returned also with an error. The error joins ErrFailed for
// a capture that failed, was killed, ended early on its own, lost its target
// or delivered a file that is not valid classic pcap, and a removal error.
// Docker and stream errors retain their causes; a failed retrieval alone does
// not imply ErrFailed. A capture that ended at MaxPackets is not an error; its
// End says so. After a removal error Stop can be called again; it returns the
// same report.
//
//purpose:evidence,lifetime
func (s *Session) Stop(ctx context.Context) (Report, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.stopGrace()+finishTimeout)
	defer cancel()
	if !s.finished {
		s.finished = true
		s.err = s.finish(finishCtx)
	}
	return s.report, errors.Join(s.err, s.removeProbe(finishCtx), s.removeHelper(finishCtx))
}

func (s *Session) stopGrace() time.Duration {
	if s.request.StopGrace == 0 {
		return DefaultStopGrace
	}
	return s.request.StopGrace
}

// finish ends tcpdump, collects its outcome and delivers the file. Its
// errors describe the capture, not the helper's removal.
func (s *Session) finish(ctx context.Context) error {
	s.report.StopRequestedAt = time.Now().UTC()
	var problems []error
	state, err := s.backend.state(ctx, s.helper)
	if err != nil {
		problems = append(problems, classifyDocker(ctx, fmt.Errorf("inspect the capture helper: %w", err)))
	} else if !state.running {
		s.recordEnd(state, false, false)
	} else {
		if err := s.endCapture(ctx); err != nil {
			problems = append(problems, err)
		}
	}
	switch s.report.End {
	case EndFailed:
		problems = append(problems, fmt.Errorf("%w: tcpdump exited with status %d", ErrFailed, s.report.ExitCode))
	case EndKilled:
		problems = append(problems, fmt.Errorf("%w: tcpdump did not end within %s of SIGTERM and was killed", ErrFailed, s.stopGrace()))
	}
	if _, stderr, err := s.backend.logs(ctx, s.helper, stderrLimit); err != nil {
		problems = append(problems, classifyDocker(ctx, fmt.Errorf("read the capture helper's output: %w", err)))
	} else {
		s.report.Stderr = string(stderr)
		s.report.Statistics = parseStatistics(stderr)
	}
	s.report.Target, err = s.compareTarget(ctx)
	if err != nil {
		problems = append(problems, err)
	} else if s.report.Target != TargetUnchanged {
		problems = append(problems, fmt.Errorf("%w: the target is %s since the capture became ready; the file covers only the namespace it joined", ErrFailed, s.report.Target))
	}
	if err := s.retrieve(ctx); err != nil {
		problems = append(problems, err)
	}
	if err := s.settleSelfEnd(); err != nil {
		problems = append(problems, err)
	}
	return errors.Join(problems...)
}

// endCapture sends SIGTERM, waits for the grace, then kills the helper.
func (s *Session) endCapture(ctx context.Context) error {
	// A conflict means tcpdump ended on its own before the signal.
	err := s.backend.signal(ctx, s.helper, "SIGTERM")
	if err != nil && !errdefs.IsConflict(err) {
		return classifyDocker(ctx, fmt.Errorf("signal the capture helper: %w", err))
	}
	signaled := err == nil
	if state, ok, err := s.waitEnd(ctx, s.stopGrace()); err != nil || ok {
		if ok {
			s.recordEnd(state, signaled, false)
		}
		return err
	}
	if err := s.backend.signal(ctx, s.helper, "SIGKILL"); err != nil && !errdefs.IsConflict(err) {
		return classifyDocker(ctx, fmt.Errorf("kill the capture helper: %w", err))
	}
	state, ok, err := s.waitEnd(ctx, 30*time.Second)
	if err == nil && !ok {
		err = fmt.Errorf("%w: the capture helper did not end after SIGKILL", ErrFailed)
	}
	if ok {
		s.recordEnd(state, true, true)
	}
	return err
}

func (s *Session) waitEnd(ctx context.Context, grace time.Duration) (helperState, bool, error) {
	deadline := time.Now().Add(grace)
	for {
		state, err := s.backend.state(ctx, s.helper)
		if err != nil {
			return state, false, classifyDocker(ctx, fmt.Errorf("inspect the capture helper: %w", err))
		}
		if !state.running {
			return state, true, nil
		}
		if !time.Now().Before(deadline) {
			return state, false, nil
		}
		select {
		case <-ctx.Done():
			return state, false, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func (s *Session) recordEnd(state helperState, signaled, killed bool) {
	s.report.ExitCode = state.exitCode
	s.report.EndedAt = state.finishedAt
	switch {
	case killed:
		s.report.End = EndKilled
	case state.exitCode != 0:
		s.report.End = EndFailed
	case signaled:
		s.report.End = EndStopped
	default:
		// Ended on its own: whether at MaxPackets is decided from the
		// retrieved file and the counter, in settleSelfEnd.
		s.report.End = EndUnknown
		s.selfEnded = true
	}
}

// settleSelfEnd decides how a tcpdump that ended with status 0 before Stop
// ended: at MaxPackets only when the valid file holds that many packets and
// the captured counter, when printed, agrees. Without a valid file the end
// stays EndUnknown.
func (s *Session) settleSelfEnd() error {
	if !s.selfEnded || s.report.File.Status != FileValid {
		return nil
	}
	captured := s.report.Statistics.Captured
	if s.report.File.Packets == s.request.MaxPackets && (captured == nil || *captured == uint64(s.request.MaxPackets)) {
		s.report.End = EndPacketLimit
		return nil
	}
	s.report.End = EndPremature
	return fmt.Errorf("%w: tcpdump ended before Stop with %d of %d packets", ErrFailed, s.report.File.Packets, s.request.MaxPackets)
}

func (s *Session) compareTarget(ctx context.Context) (TargetState, error) {
	current, err := s.backend.target(ctx, s.report.ContainerID, s.request.Network)
	switch {
	case err != nil && observed(ctx, err) && errdefs.IsNotFound(err):
		return TargetGone, nil
	case err != nil:
		return TargetUnknown, classifyDocker(ctx, fmt.Errorf("inspect the capture target: %w", err))
	case sameGeneration(s.target, current):
		return TargetUnchanged, nil
	}
	return TargetChanged, nil
}

// observed reports whether a failed Docker call is an engine answer about
// the resource rather than an interrupted call. Cancellation takes precedence:
// a canceled or expired ctx, or a cancellation or deadline anywhere in err,
// keeps the error out of any artifact or target verdict joined with it.
func observed(ctx context.Context, err error) bool {
	return ctx.Err() == nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// classifyRetrieval classifies a failure to open the helper's file. A missing
// or non-regular capture is an observed artifact failure; transport failures
// and cancellation retain their own category.
func classifyRetrieval(ctx context.Context, err error) error {
	if observed(ctx, err) && (errdefs.IsNotFound(err) || errors.Is(err, errCaptureNotRegular)) {
		return fmt.Errorf("%w: retrieve the capture file: %w", ErrFailed, err)
	}
	return fmt.Errorf("retrieve the capture file: %w", classifyDocker(ctx, err))
}

// copyBounded copies at most limit bytes of src to dst. When it copied limit
// bytes it reads one byte more and returns it as beyond, without writing it:
// a nonempty beyond only shows that src is longer than limit. The end of src
// is not an error; any other read or write error is returned.
func copyBounded(dst io.Writer, src io.Reader, limit int64) (written int64, beyond []byte, err error) {
	written, err = io.Copy(dst, io.LimitReader(src, limit))
	if err != nil || written != limit {
		return written, nil, err
	}
	extra := make([]byte, 1)
	n, err := io.ReadFull(src, extra)
	if err != nil && !errors.Is(err, io.EOF) {
		return written, extra[:n], err
	}
	return written, extra[:n], nil
}

// retrieve delivers the helper's file into Output and checks it.
func (s *Session) retrieve(ctx context.Context) (err error) {
	defer func() {
		if closeErr := s.closeOutput(); closeErr != nil {
			s.report.Retrieved, s.report.RetrievedSHA256 = false, ""
			err = errors.Join(err, closeErr)
		}
	}()
	content, err := s.backend.copy(ctx, s.helper, capturePath)
	if err != nil {
		return classifyRetrieval(ctx, err)
	}
	if err := s.checkOutputIdentity(); err != nil {
		return errors.Join(err, content.Close())
	}
	if err := s.output.Truncate(0); err != nil {
		return errors.Join(err, content.Close())
	}
	digest := sha256.New()
	written, beyond, copyErr := copyBounded(io.MultiWriter(s.output, digest), content, s.report.MaxBytes)
	s.report.RetrievedBytes = written
	if err := errors.Join(copyErr, content.Close(), s.checkOutputIdentity()); err != nil {
		return fmt.Errorf("retrieve the capture file: %w", err)
	}
	s.report.Retrieved = true
	s.report.RetrievedSHA256 = fmt.Sprintf("%x", digest.Sum(nil))
	// Validate the same descriptor used for delivery, including the first byte
	// beyond the configured limit when the helper exceeded it.
	delivered := io.NewSectionReader(s.output, 0, written)
	checked, err := pcapfile.Validate(io.MultiReader(delivered, bytes.NewReader(beyond)),
		pcapfile.Limits{MaxFileBytes: s.report.MaxBytes, MaxRecordBytes: uint32(s.request.SnapLen)})
	if err != nil {
		return fmt.Errorf("check the capture file: %w", err)
	}
	s.report.File = fileReport(checked)
	if s.report.File.Status != FileValid {
		return fmt.Errorf("%w: the capture file is %s: %s", ErrFailed, s.report.File.Status, s.report.File.Reason)
	}
	return nil
}

func fileReport(r pcapfile.Report) FileReport {
	return FileReport{
		Status: FileStatus(r.Status), Reason: r.Reason, ByteOrder: r.ByteOrder, TimestampUnit: r.TimestampUnit,
		VersionMajor: r.VersionMajor, VersionMinor: r.VersionMinor, SnapLen: r.SnapLen, LinkType: r.LinkType,
		Packets: r.Packets, TruncatedPackets: r.TruncatedPackets, BytesRead: r.BytesRead, SHA256: r.SHA256, Complete: r.Complete,
	}
}

var statistic = regexp.MustCompile(`(?m)^([0-9]+) packets? (captured|received by filter|dropped by kernel|dropped by interface)\s*$`)

// parseStatistics reads tcpdump's counters; a counter it did not print
// stays nil.
func parseStatistics(stderr []byte) Statistics {
	var stats Statistics
	for _, match := range statistic.FindAllStringSubmatch(string(stderr), -1) {
		value, err := strconv.ParseUint(match[1], 10, 64)
		if err != nil {
			continue
		}
		switch match[2] {
		case "captured":
			stats.Captured = &value
		case "received by filter":
			stats.ReceivedByFilter = &value
		case "dropped by kernel":
			stats.DroppedByKernel = &value
		case "dropped by interface":
			stats.DroppedByInterface = &value
		}
	}
	return stats
}

// permissionDenied matches the runtime's refusal to exec a tcpdump whose
// file capability the bounding set lacks, and tcpdump's own refusals.
func permissionDenied(text string) bool {
	lower := strings.ToLower(text)
	return strings.Contains(lower, "operation not permitted") || strings.Contains(lower, "permission denied") ||
		strings.Contains(lower, "don't have permission")
}

// classifyCaptureExit classifies tcpdump ending before readiness.
func classifyCaptureExit(code int, stderr []byte) error {
	text := strings.TrimSpace(string(stderr))
	if permissionDenied(text) {
		return fmt.Errorf("%w: tcpdump exited with status %d: %s", ErrPermissionDenied, code, text)
	}
	return fmt.Errorf("%w: tcpdump exited with status %d before it was ready: %s", ErrFailed, code, text)
}

// classifyStartError classifies a helper container that Docker could not
// start: a missing executable is the image's, an exec refusal the engine's.
func classifyStartError(ctx context.Context, label string, err error) error {
	text := strings.ToLower(err.Error())
	switch {
	case ctx.Err() != nil:
		return classifyDocker(ctx, fmt.Errorf("%s: %w", label, err))
	case strings.Contains(text, "executable file not found") || strings.Contains(text, "no such file or directory"):
		return fmt.Errorf("%s: %w: %w", label, ErrImageUnsupported, err)
	case permissionDenied(text):
		return fmt.Errorf("%s: %w: %w", label, ErrPermissionDenied, err)
	}
	return classifyDocker(ctx, fmt.Errorf("%s: %w", label, err))
}

// SettlementAllowance bounds Docker work after Stop acquires the session lock.
// Time queued behind another Stop or Settle is additional. The allowance
// is the stop grace plus two minutes (130 seconds with the default
// stop grace), whatever context Settle is given. It is what
// finalize.Finalization.Capture admits for this session. Callers that do not use
// a finalization call Stop instead of Settle.
//
//purpose:evidence
func (s *Session) SettlementAllowance() time.Duration {
	return s.stopGrace() + finishTimeout
}

// Settle is Stop for a finalize.Finalization, which registers the session
// with Finalization.Capture. It returns the Report as an any value and, so that an
// archived report names no host path, with Output reduced to its base name.
// It shares Stop's behavior: the context's cancellation does not shorten it,
// it waits for a Stop that is already running, and a Stop that already
// finished is not repeated.
//
//purpose:evidence,lifetime
func (s *Session) Settle(ctx context.Context) (any, error) {
	report, err := s.Stop(ctx)
	if report.Output != "" {
		report.Output = filepath.Base(report.Output)
	}
	return report, err
}
