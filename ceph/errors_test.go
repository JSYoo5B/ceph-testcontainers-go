package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestNativeFailureReadsTheReportedErrno(t *testing.T) {
	for name, tc := range map[string]struct {
		program        string
		code           int
		stdout, stderr string
		errno, reason  string
		is             []error
	}{
		"ceph ENOENT": {program: "ceph", code: 2, stderr: "Error ENOENT: unrecognized pool 'missing'\n",
			errno: "ENOENT", reason: "Error ENOENT: unrecognized pool 'missing'", is: []error{fs.ErrNotExist}},
		"ceph EACCES": {program: "ceph", code: 13, stderr: "Error EACCES: access denied\n",
			errno: "EACCES", reason: "Error EACCES: access denied", is: []error{fs.ErrPermission}},
		"ceph EPERM": {program: "ceph", code: 1, stderr: "Error EPERM: pool deletion is disabled; you must first set the mon_allow_pool_delete config option to true before you can destroy a pool\n",
			errno: "EPERM", reason: "Error EPERM: pool deletion is disabled; you must first set the mon_allow_pool_delete config option to true before you can destroy a pool", is: []error{fs.ErrPermission}},
		"rbd after log lines": {program: "rbd", code: 2,
			stderr: "2026-10-12T22:03:28.125+0000 ffff9d0a3020 -1 librbd::image::OpenRequest: failed to retrieve image id: (2) No such file or directory\n" +
				"rbd: error opening image missing: (2) No such file or directory\n",
			errno: "ENOENT", reason: "rbd: error opening image missing: (2) No such file or directory", is: []error{fs.ErrNotExist}},
		"rados EEXIST": {program: "rados", code: 17, stderr: "error creating pool x: (17) File exists\n", errno: "EEXIST", reason: "error creating pool x: (17) File exists", is: []error{fs.ErrExist}},
		"exit code is not an errno": {program: "radosgw-admin", code: 17, stderr: "could not create user: unable to create user, user: alice exists\n",
			reason: "could not create user: unable to create user, user: alice exists"},
		"number without its text":  {program: "rbd", code: 1, stderr: "rbd: (2) Permission denied\n", reason: "rbd: (2) Permission denied"},
		"unknown errno name":       {program: "ceph", code: 22, stderr: "Error EFOO: something\n", reason: "Error EFOO: something"},
		"standard output fallback": {program: "sh", code: 1, stdout: "first\nlast line\n", reason: "last line"},
		"errno-shaped image name": {program: "rbd", code: 2, stderr: "rbd: error opening image (13) Permission denied: (2) No such file or directory\n",
			errno: "ENOENT", reason: "rbd: error opening image (13) Permission denied: (2) No such file or directory", is: []error{fs.ErrNotExist}},
		"errno-shaped pool name": {program: "ceph", code: 2, stderr: "Error ENOENT: unrecognized pool 'Error EACCES: x'\n",
			errno: "ENOENT", reason: "Error ENOENT: unrecognized pool 'Error EACCES: x'", is: []error{fs.ErrNotExist}},
		"Error not at the start": {program: "radosgw-admin", code: 1, stderr: "failed: Error EACCES: quoted\n", reason: "failed: Error EACCES: quoted"},
		"no output":              {program: "true", code: 1},
		"cause before summary": {program: "rbd", code: 22, stderr: "start snapshot 'baseline' does not exist in the image, aborting\nrbd: import-diff failed: (22) Invalid argument\n",
			errno: "EINVAL", reason: "start snapshot 'baseline' does not exist in the image, aborting; rbd: import-diff failed: (22) Invalid argument"},
		"only log lines": {program: "rbd", code: 2, stderr: "2026-10-12T22:03:28.125+0000 ffff9d0a3020 -1 can't open ceph.conf: (2) No such file or directory\n",
			errno: "ENOENT", reason: "2026-10-12T22:03:28.125+0000 ffff9d0a3020 -1 can't open ceph.conf: (2) No such file or directory", is: []error{fs.ErrNotExist}},
	} {
		t.Run(name, func(t *testing.T) {
			failure := nativeFailure(tc.program, tc.code, []byte(tc.stdout), []byte(tc.stderr))
			if failure.Program != tc.program || failure.ExitCode != tc.code || failure.Errno != tc.errno || failure.Reason != tc.reason {
				t.Fatalf("failure = %+v", *failure)
			}
			err := fmt.Errorf("operation: %w", failure)
			if !errors.Is(err, ErrNativeFailure) || errors.Is(err, ErrInvalidRequest) || errors.Is(err, ErrUnsupported) {
				t.Fatalf("%v is not only a native failure", err)
			}
			for _, target := range []error{fs.ErrNotExist, fs.ErrPermission, fs.ErrExist} {
				want := false
				for _, expected := range tc.is {
					want = want || expected == target
				}
				if errors.Is(err, target) != want {
					t.Fatalf("errors.Is(%v, %v) = %v", err, target, !want)
				}
			}
			var found *NativeFailure
			if !errors.As(err, &found) || found != failure {
				t.Fatalf("errors.As did not return the failure of %v", err)
			}
		})
	}
}

func TestNativeFailureRedactsAndBoundsItsReason(t *testing.T) {
	const key = "AQBkjiRpAAAAABAAvMoJxkNMVvFCZQdlBfRhhw=="
	for name, tc := range map[string]struct{ stderr, leak string }{
		"keyring":       {stderr: "Error EINVAL: cannot import [client.x]\n\tkey = " + key + "\n", leak: key},
		"bare key":      {stderr: "Error EINVAL: key " + key + " was rejected\n", leak: key},
		"secret value":  {stderr: `error: {"secret_key": "s3cr3tvalue"}` + "\n", leak: "s3cr3tvalue"},
		"bearer token":  {stderr: "error: Authorization: Bearer abc.def.ghi\n", leak: "abc.def.ghi"},
		"malformed key": {stderr: "Error EINVAL: failed to decode key type=key val=AQBkjiRpAAAAABAAvMo\n", leak: "AQBkjiRpAAAAABAAvMo"},
		"truncated key": {stderr: "error: bad key AQBkjiRpAAAAABAAvMoJx in keyring\n", leak: "AQBkjiRpAAAAABAAvMoJx"},
		"control bytes": {stderr: "Error EINVAL: bad\x1b[31m value\x00\n", leak: "\x1b"},
	} {
		t.Run(name, func(t *testing.T) {
			failure := nativeFailure("ceph", 22, nil, []byte(tc.stderr))
			if strings.Contains(failure.Reason, tc.leak) || strings.Contains(failure.Error(), tc.leak) {
				t.Fatalf("reason %q leaks %q", failure.Reason, tc.leak)
			}
		})
	}
	long := nativeFailure("ceph", 22, nil, []byte("Error EINVAL: "+strings.Repeat("é", 2*MaxNativeFailureReason)))
	if len(long.Reason) > MaxNativeFailureReason || !utf8.ValidString(long.Reason) || !strings.HasSuffix(long.Reason, "...") || long.Errno != "EINVAL" {
		t.Fatalf("long reason of %d bytes: %q", len(long.Reason), long.Reason)
	}
}

func TestOperationErrorNamesItsDetails(t *testing.T) {
	cause := errors.New("cephx key type aes256k needs ceph-authtool --key-type")
	err := &OperationError{Kind: ErrUnsupported, Op: "create client", Target: "control", Version: "20.2.3", Image: "sha256:abc",
		Capability: "ceph-authtool --key-type", Err: cause}
	if got, want := err.Error(), "create client: "+cause.Error()+" (control, Ceph 20.2.3, image sha256:abc)"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(err, ErrUnsupported) || errors.Is(err, ErrInvalidRequest) || !errors.Is(err, cause) || errors.Unwrap(err) != cause {
		t.Fatalf("%v does not keep its category and cause", err)
	}
	if text := (&OperationError{Kind: ErrInvalidRequest}).Error(); text != ErrInvalidRequest.Error() {
		t.Fatalf("an operation error without a cause reads %q", text)
	}
	if errors.Is(&OperationError{Err: cause}, ErrInvalidRequest) {
		t.Fatal("an operation error without a kind matched a category")
	}
}

func TestErrorSentinelsBelongToTheirCategories(t *testing.T) {
	for _, tc := range []struct {
		err  error
		kind error
	}{
		{ErrNetemImageUnsupported, ErrImageCapability},
		{ErrNetemPermissionDenied, ErrEnvironmentUnavailable},
		{ErrNetemKernelUnsupported, ErrEnvironmentUnavailable},
		{ErrHostNetworkUnavailable, ErrEnvironmentUnavailable},
		{errClusterTerminated, ErrInvalidRequest},
	} {
		wrapped := fmt.Errorf("operation: %w: detail", tc.err)
		if !errors.Is(wrapped, tc.err) || !errors.Is(wrapped, tc.kind) {
			t.Fatalf("%v does not match %v and %v", wrapped, tc.err, tc.kind)
		}
		for _, other := range categories {
			if other != tc.kind && errors.Is(wrapped, other) {
				t.Fatalf("%v also matches %v", wrapped, other)
			}
		}
	}
	for _, err := range []error{ErrNativeCommandIncomplete, errNativeTerminated, context.Canceled, context.DeadlineExceeded,
		&NativeCommandUnstoppedError{err: context.Canceled}} {
		if classified(fmt.Errorf("operation: %w", err)) {
			t.Fatalf("%v belongs to a category", err)
		}
	}
}

// commandTarget answers one exec as Docker does: an error, or an exit code
// with multiplexed output.
type commandTarget struct {
	testcontainers.Container
	err            error
	code           int
	stdout, stderr string
	cancel         context.CancelFunc
}

func (f *commandTarget) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	if f.cancel != nil {
		f.cancel()
	}
	if f.err != nil {
		return 0, nil, f.err
	}
	var stream bytes.Buffer
	for _, frame := range []struct {
		kind stdcopy.StdType
		data string
	}{{stdcopy.Stdout, f.stdout}, {stdcopy.Stderr, f.stderr}} {
		if frame.data == "" {
			continue
		}
		header := make([]byte, 8)
		header[0] = byte(frame.kind)
		binary.BigEndian.PutUint32(header[4:], uint32(len(frame.data)))
		stream.Write(header)
		stream.WriteString(frame.data)
	}
	return f.code, &stream, nil
}

func TestCommandClassifiesHowItFailed(t *testing.T) {
	for name, tc := range map[string]struct {
		target   *commandTarget
		canceled bool
		kind     error
	}{
		"missing program": {target: &commandTarget{code: 127,
			stdout: `OCI runtime exec failed: exec failed: unable to start container process: exec: "pgrep": executable file not found in $PATH: unknown`},
			kind: ErrImageCapability},
		"program not executable": {target: &commandTarget{code: 126,
			stdout: `OCI runtime exec failed: exec failed: unable to start container process: exec: "pgrep": permission denied: unknown`},
			kind: ErrImageCapability},
		"shell without the program": {target: &commandTarget{code: 127, stderr: "sh: 1: pgrep: not found\n"}, kind: ErrNativeFailure},
		"Ceph refusal":              {target: &commandTarget{code: 2, stderr: "Error ENOENT: unrecognized pool 'x'\n"}, kind: ErrNativeFailure},
		"removed container":         {target: &commandTarget{err: errdefs.ErrNotFound.WithMessage("No such container: c")}, kind: ErrInvalidRequest},
		"stopped container":         {target: &commandTarget{err: errdefs.ErrConflict.WithMessage("container c is not running")}, kind: ErrInvalidRequest},
		"engine failure":            {target: &commandTarget{err: errors.New("Cannot connect to the Docker daemon")}, kind: ErrEnvironmentUnavailable},
		"canceled call":             {target: &commandTarget{err: errors.New("use of closed network connection")}, canceled: true},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.canceled {
				tc.target.cancel = cancel
			}
			_, err := command(ctx, tc.target, "pgrep", "-x", "ceph-osd")
			switch {
			case err == nil:
				t.Fatal("a failed command returned no error")
			case tc.kind == nil && (classified(err) || !errors.Is(err, context.Canceled)):
				t.Fatalf("a canceled call returned %v", err)
			case tc.kind != nil && !errors.Is(err, tc.kind):
				t.Fatalf("%v is not %v", err, tc.kind)
			}
			for _, other := range categories {
				if other != tc.kind && errors.Is(err, other) {
					t.Fatalf("%v also matches %v", err, other)
				}
			}
			var operation *OperationError
			if tc.kind == ErrImageCapability && (!errors.As(err, &operation) || operation.Capability != "pgrep") {
				t.Fatalf("%v does not name the missing program", err)
			}
		})
	}
	if _, err := command(t.Context(), nil, "ceph"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a missing control container returned %v", err)
	}
	if out, err := command(t.Context(), &commandTarget{stdout: "ok"}, "true"); err != nil || string(out) != "ok" {
		t.Fatalf("a successful command returned %q, %v", out, err)
	}
}

// The state model refuses what Ceph refuses, with Ceph's errno.
func TestCephReportsTheErrnoOfAStateRefusal(t *testing.T) {
	f := newCephStateFake()
	f.AddFixturePool("data", false)
	cluster := poolFixtureCluster(f, 3)
	_, err := cluster.Ceph(t.Context(), "fs", "fail", "missing")
	var failure *NativeFailure
	if !errors.Is(err, fs.ErrNotExist) || !errors.As(err, &failure) || failure.Errno != "ENOENT" || failure.ExitCode != 2 {
		t.Fatalf("failing an absent filesystem returned %v", err)
	}
	disabled := "false"
	f.AllowDelete = &disabled
	_, err = cluster.Ceph(t.Context(), "osd", "pool", "rm", "data", "data", "--yes-i-really-really-mean-it")
	if !errors.Is(err, fs.ErrPermission) || !strings.Contains(err.Error(), "mon_allow_pool_delete") || !f.HasPool("data") {
		t.Fatalf("removing a pool without permission returned %v: %s", err, f)
	}
	f.Filesystems = []FakeFilesystem{{Name: "fs", Metadata: "data"}}
	enabled := "true"
	f.AllowDelete = &enabled
	if _, err = cluster.Ceph(t.Context(), "osd", "pool", "rm", "data", "data", "--yes-i-really-really-mean-it"); !errors.As(err, &failure) || failure.Errno != "EBUSY" ||
		errors.Is(err, fs.ErrPermission) || !f.HasPool("data") {
		t.Fatalf("removing a pool in use returned %v: %s", err, f)
	}
}

// Wrappers that hide native output keep only its classification.
func TestQuietErrorsKeepTheClassificationWithoutOutput(t *testing.T) {
	native := fmt.Errorf("auth get-or-create: %w", nativeFailure("ceph", 2, nil, []byte("Error ENOENT: secret-reason\n")))
	for name, err := range map[string]error{
		"configuration": quiet(errors.New("Ceph configuration command failed; native output is redacted"), native),
		"client":        clusterutil.OperationError(t.Context(), "create client", native),
	} {
		t.Run(name, func(t *testing.T) {
			var failure *NativeFailure
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "ENOENT") || !errors.As(err, &failure) || failure.Reason != "" ||
				failure.Errno != "ENOENT" || !errors.Is(err, ErrNativeFailure) || !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("%q keeps %+v", err, failure)
			}
		})
	}
	unsupported := &OperationError{Kind: ErrUnsupported, Target: "control", Capability: "ceph-authtool --key-type", Err: errors.New("secret detail")}
	var kept *OperationError
	if err := quiet(errors.New("visible"), unsupported); err.Error() != "visible" || !errors.As(err, &kept) || kept.Kind != ErrUnsupported ||
		kept.Capability != "ceph-authtool --key-type" || kept.Err != nil {
		t.Fatalf("quiet kept %+v", kept)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := clusterutil.OperationError(canceled, "create client", context.Canceled); !errors.Is(err, context.Canceled) || classified(err) {
		t.Fatalf("a canceled operation returned %v", err)
	}
	if err := quiet(errors.New("visible"), errors.New("unclassified")); err.Error() != "visible" || classified(err) {
		t.Fatalf("an unclassified cause returned %v", err)
	}
	if err := quiet(nil, native); !errors.Is(err, fs.ErrNotExist) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("a classification alone returned %v", err)
	}
	if err := quiet(nil, errors.New("unclassified")); err != nil {
		t.Fatalf("nothing to keep returned %v", err)
	}
	// A backend's own deadline, while the caller's context is live.
	if err := quiet(errors.New("visible"), fmt.Errorf("backend: %w", context.DeadlineExceeded)); err.Error() != "visible" ||
		!errors.Is(err, context.DeadlineExceeded) || classified(err) {
		t.Fatalf("a backend deadline returned %v", err)
	}
}

func TestBootstrapRefusalKeepsTheScriptsKind(t *testing.T) {
	for name, tc := range map[string]struct {
		log     string
		kind    error
		reasons string
	}{
		"cipher": {log: "starting\ntc-ceph: unsupported: Cephx cipher aes256k needs ceph-authtool --key-type and monmaptool cipher options, as in Ceph 20.2.4 and later; this image supports only aes\n",
			kind: ErrUnsupported, reasons: "Cephx cipher aes256k needs ceph-authtool --key-type"},
		"address family": {log: "tc-ceph: environment: this container has no ipv6 address\n", kind: ErrEnvironmentUnavailable, reasons: "this container has no ipv6 address"},
		"missing tool":   {log: "tc-ceph: image: this image has no monmaptool\n", kind: ErrImageCapability, reasons: "this image has no monmaptool"},
		"untagged":       {log: "tc-ceph: something else\n", reasons: "something else"},
		"mixed kinds":    {log: "tc-ceph: environment: no address\ntc-ceph: unsupported: no cipher\n", reasons: "no address; no cipher"},
	} {
		t.Run(name, func(t *testing.T) {
			refused := parseBootstrapRefusal([]byte(tc.log))
			cause := errors.New("container exited with code 2")
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			if err := refused.with(canceled, cause); classified(err) || !strings.Contains(err.Error(), tc.reasons) {
				t.Fatalf("a start that ctx ended returned %v", err)
			}
			err := refused.with(t.Context(), cause)
			if !strings.Contains(err.Error(), tc.reasons) || !errors.Is(err, cause) || strings.Contains(err.Error(), "unsupported:") {
				t.Fatalf("refusal %q", err)
			}
			if tc.kind == nil && classified(err) || tc.kind != nil && !errors.Is(err, tc.kind) {
				t.Fatalf("refusal %v is not %v", err, tc.kind)
			}
		})
	}
	if parseBootstrapRefusal([]byte("ordinary log\n")) != nil {
		t.Fatal("a log without refusals was a refusal")
	}
}

func TestKeyTypeSupportFollowsTheControlImage(t *testing.T) {
	for _, tc := range []struct {
		version string
		option  bool
		want    []string
	}{
		// A patched image of an older release can have the option, and a
		// stripped one of a newer release can lack it.
		{"20.2.3", true, []string{"ceph-authtool", "--gen-print-key", "--key-type", "aes256k"}},
		{"20.2.4", false, nil},
		{"19.2.5", false, nil},
	} {
		c := &Container{cephVersion: tc.version, keyTypeOption: tc.option, controlImageID: "sha256:control"}
		args, err := c.generateKeyArgs(CipherAES256K)
		var unsupported *OperationError
		switch {
		case tc.want != nil && (err != nil || strings.Join(args, " ") != strings.Join(tc.want, " ")):
			t.Fatalf("Ceph %s with the option: %v %v", tc.version, args, err)
		case tc.want == nil && (!errors.As(err, &unsupported) || unsupported.Kind != ErrUnsupported || unsupported.Version != tc.version ||
			unsupported.Capability != "ceph-authtool --key-type" || unsupported.Target != "control" || unsupported.Image != "sha256:control"):
			t.Fatalf("Ceph %s without the option: %v", tc.version, err)
		}
	}
	if _, err := (&Container{}).generateKeyArgs(""); err != nil {
		t.Fatal(err)
	}
	options := &clientOptions{}
	if err := WithClientKeyType("des")(options); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("an unknown cipher returned %v", err)
	}
}
