//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_fscrypt_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func intPointer(v int) *int { return &v }

// The admission refuses each missing prerequisite of its path by name and
// admits a container that has them all.
func TestFscryptAdmissionNamesTheMissingPrerequisite(t *testing.T) {
	ready := fscryptAdmission{Kernel: "6.8.0", KernelCeph: true, KernelFuse: true, CephFuse: true, CapSysAdmin: true, Label: "unconfined"}
	for _, test := range []struct {
		name      string
		path      string
		change    func(*fscryptAdmission)
		requires  string
		admission bool
	}{
		{"kernel ready", kernelPath, func(*fscryptAdmission) {}, "", true},
		{"kernel without ceph registered", kernelPath, func(a *fscryptAdmission) { a.KernelCeph = false }, fscryptNeedsKernelFS, false},
		{"kernel without CAP_SYS_ADMIN", kernelPath, func(a *fscryptAdmission) { a.CapSysAdmin = false }, fscryptNeedsSysAdmin, false},
		{"kernel ignores FUSE", kernelPath, func(a *fscryptAdmission) { a.CephFuse, a.DeviceErrno = false, intPointer(errnoENOENT) }, "", true},
		{"fuse ready", fusePath, func(*fscryptAdmission) {}, "", true},
		{"fuse without binary", fusePath, func(a *fscryptAdmission) { a.CephFuse = false }, fscryptNeedsBinary, false},
		{"fuse without fuse registered", fusePath, func(a *fscryptAdmission) { a.KernelFuse = false }, fscryptNeedsKernelFS, false},
		{"fuse without device", fusePath, func(a *fscryptAdmission) { a.DeviceErrno = intPointer(errnoENOENT) }, fscryptNeedsDevice, false},
		{"fuse without CAP_SYS_ADMIN", fusePath, func(a *fscryptAdmission) { a.CapSysAdmin = false }, fscryptNeedsSysAdmin, false},
		{"fuse ignores kernel ceph", fusePath, func(a *fscryptAdmission) { a.KernelCeph = false }, "", true},
		{"unknown path", "nfs", func(*fscryptAdmission) {}, "client path", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			admission := ready
			test.change(&admission)
			refusal := admission.refusal(test.path)
			if test.admission != (refusal == nil) || (refusal != nil && refusal.Requirement != test.requires) {
				t.Fatalf("refusal %v, want requirement %q (admitted %t)", refusal, test.requires, test.admission)
			}
		})
	}
}

// A refused mount(2) is a missing prerequisite only for the privilege,
// security and filesystem-type errnos; every other errno is a failed
// supported operation.
func TestFscryptMountRefusalSeparatesPrerequisitesFromFailures(t *testing.T) {
	admission := fscryptAdmission{CapSysAdmin: true, Label: "docker-default (enforce)"}
	for errno, want := range map[int]string{
		errnoEPERM: fscryptNeedsSysAdmin, errnoEACCES: fscryptNeedsProfile, errnoENODEV: fscryptNeedsKernelFS,
		errnoENOENT: "", errnoEINVAL: "", 110: "", 524: "",
	} {
		refusal := mountRefusal(kernelPath, errno, admission)
		if (want == "") != (refusal == nil) || (refusal != nil && refusal.Requirement != want) {
			t.Errorf("errno %d: refusal %v, want %q", errno, refusal, want)
		}
	}
}

// The policy query on an empty directory admits ENODATA, refuses the
// answers of a client without fscrypt ioctls and classifies nothing else.
func TestFscryptPolicyQueryClassification(t *testing.T) {
	for _, test := range []struct {
		errno      int
		refused    bool
		classified bool
	}{
		{errnoENODATA, false, true},
		{errnoENOTTY, true, true},
		{errnoEOPNOTSUPP, true, true},
		{errnoEINVAL, true, true},
		{errnoENOSYS, true, true},
		{0, false, false},
		{errnoEACCES, false, false},
		{errnoENOKEY, false, false},
	} {
		refusal, classified := policyRefusal(fusePath, test.errno)
		if classified != test.classified || (refusal != nil) != test.refused || (refusal != nil && refusal.Requirement != fscryptNeedsClient) {
			t.Errorf("errno %d: refusal %v classified %t, want refused %t classified %t", test.errno, refusal, classified, test.refused, test.classified)
		}
	}
}

// The probe's ioctl request numbers are the Linux UAPI values for the
// kernel client and the fixed-size variants that ceph-fuse 21.2.0 declares.
func TestFscryptProbeRequestNumbers(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not installed")
	}
	out, err := exec.Command(python, "-c", fscryptProbe, "requests").Output()
	if err != nil {
		t.Fatal(err)
	}
	var requests map[string]map[string]uint32
	if err := json.Unmarshal(out, &requests); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	want := map[string]map[string]uint32{
		kernelPath: {"set": 0x800c6613, "get": 0xc0096616, "add": 0xc0506617, "remove": 0xc0406618, "status": 0xc080661a},
		fusePath:   {"set": 0xc0186613, "get": 0xc0206616, "add": 0xc0906617, "remove": 0xc0406618, "status": 0xc080661a},
	}
	for path, numbers := range want {
		for name, number := range numbers {
			if got := requests[path][name]; got != number {
				t.Errorf("%s %s: %#x, want %#x", path, name, got, number)
			}
		}
	}
}

// stop reads the ceph-fuse call's result only after the worker closed done.
// The worker assigns its result as soon as the call is cancelled but closes
// done only when released, so the first stop always runs out of its wait
// while the assignment may be in progress; under the race detector a read
// of the result on that path fails the test. Once the worker returned, a
// second stop reads the result and tries to stop the unstopped command,
// which a zero NativeCommandUnstoppedError refuses.
func TestFuseProcessStopReadsTheResultOnlyAfterTheWorkerReturned(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	process := &fuseProcess{cancel: cancel, done: make(chan struct{})}
	release := make(chan struct{})
	go func() {
		defer close(process.done)
		<-ctx.Done()
		process.result, process.err = ceph.NativeCommandResult{ExitCode: 1}, &ceph.NativeCommandUnstoppedError{}
		<-release
	}()
	if err := process.stop(10 * time.Millisecond); !errors.Is(err, errFuseOutstanding) {
		t.Fatalf("stop before the worker returned: %v, want errFuseOutstanding", err)
	}
	close(release)
	err := process.stop(time.Minute)
	if err == nil || errors.Is(err, errFuseOutstanding) || !strings.Contains(err.Error(), "has no invocation") {
		t.Fatalf("stop after the worker returned: %v, want the refused stop of the unstopped command", err)
	}
}

// The emergency unmount of a cleanup succeeds only when the mount table
// shows nothing below the point and umount2 either succeeded or found
// nothing mounted; every other outcome is reported with the failed step.
func TestFscryptEmergencyUnmountReportsWhatRemains(t *testing.T) {
	const point = "/mnt/cephfs"
	ok := func(output string) probeExec { return probeExec{Output: output} }
	empty := ok(`{"mounts": []}`)
	retained := ok(`{"mounts": [["/mnt/cephfs", "ceph"]]}`)
	for _, test := range []struct {
		name            string
		umount, listing probeExec
		want            string
	}{
		{"lazy unmount", ok(`{"errno": 0}`), empty, ""},
		{"lazy unmount after client log noise", ok("ceph-fuse: shutting down\n{\"errno\": 0}\n"), empty, ""},
		{"nothing was mounted", ok(`{"errno": 22}`), empty, ""},
		{"umount2 refused", ok(`{"errno": 1}`), retained, "errno 1, still mounted: [[/mnt/cephfs ceph]]"},
		{"umount2 claims success but the mount stays", ok(`{"errno": 0}`), retained, "still mounted"},
		{"umount2 refused without a mount", ok(`{"errno": 16}`), empty, "umount2 /mnt/cephfs: errno 16"},
		{"exec transport", probeExec{Err: errors.New("container is not running")}, empty, "umount2 /mnt/cephfs: exec: container is not running"},
		{"probe crashed", probeExec{Code: 1, Output: "Traceback"}, empty, "umount2 /mnt/cephfs: exit 1"},
		{"probe printed no result", ok("nothing"), empty, "umount2 /mnt/cephfs: no JSON result"},
		{"probe printed no errno", ok(`{}`), empty, "no errno"},
		{"listing transport", ok(`{"errno": 0}`), probeExec{Err: context.DeadlineExceeded}, "mount listing after umount2 /mnt/cephfs: exec: context deadline exceeded"},
		{"listing without mounts", ok(`{"errno": 0}`), ok(`{}`), "no mounts"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := emergencyUnmountError(point, test.umount, test.listing)
			switch {
			case test.want == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)):
				t.Fatalf("error %v, want one containing %q", err, test.want)
			}
		})
	}
}

func TestFscryptRetainedKeyRemovalEvidence(t *testing.T) {
	data := []byte("retained plaintext")
	busy := removalFilesBusy
	good := fscryptRetained{Before: hex.EncodeToString(data), RemoveFlags: &busy, BusyStatus: keyIncompletelyRemoved,
		Retained: hex.EncodeToString(data), OtherErrno: errnoENOKEY, FinalStatus: keyAbsent, ReopenErrno: errnoENOENT}
	if !good.matches(data) {
		t.Fatal("valid open-file removal evidence rejected")
	}
	for name, alter := range map[string]func(*fscryptRetained){
		"missing busy flag":      func(r *fscryptRetained) { r.RemoveFlags = new(int) },
		"key already absent":     func(r *fscryptRetained) { r.BusyStatus = keyAbsent },
		"open fd revoked":        func(r *fscryptRetained) { r.RetainedErrno = errnoENOKEY },
		"different bytes":        func(r *fscryptRetained) { r.Retained = "00" },
		"unrelated open failure": func(r *fscryptRetained) { r.OtherErrno = errnoEACCES },
	} {
		t.Run(name, func(t *testing.T) {
			wrong := good
			alter(&wrong)
			if wrong.matches(data) {
				t.Fatal("accepted inconsistent key removal evidence")
			}
		})
	}
}

func TestFscryptFuseQualificationMatchesTheObservedClient(t *testing.T) {
	admission := fscryptAdmission{Machine: "x86_64", Kernel: "6.8.0-117-generic"}
	// The cohort is the release that the ceph-fuse binary reports, whatever
	// image supplied the binary.
	if !admission.qualifiedView(fusePath, "21.2.0") {
		t.Fatal("established FUSE client lost its required capability check")
	}
	for _, release := range []string{"21.2.1", "20.2.4", "19.2.5", ""} {
		if admission.qualifiedView(fusePath, release) {
			t.Fatalf("ceph-fuse %q was assigned the reference FUSE behavior", release)
		}
	}
	if !admission.qualifiedView(kernelPath, "20.2.4") || admission.qualifiedView(kernelPath, "20.2.5") || admission.qualifiedView("other", "21.2.0") {
		t.Fatal("kernel client cohort changed")
	}
	for _, alter := range []func(*fscryptAdmission){
		func(a *fscryptAdmission) { a.Machine = "aarch64" }, func(a *fscryptAdmission) { a.Kernel = "6.11.0-8-generic" },
	} {
		other := admission
		alter(&other)
		if other.qualifiedView(fusePath, "21.2.0") || other.qualifiedView(kernelPath, "21.2.0") {
			t.Fatalf("unestablished platform %+v was assigned the reference behavior", other)
		}
	}
}

func TestFscryptLockedMetadataDistinguishesClientViews(t *testing.T) {
	file := fscryptEntry{Mode: 0o100600, Size: 4096, MtimeNS: 42}
	for _, alter := range []func(*fscryptEntry){
		func(e *fscryptEntry) { e.Mode++ }, func(e *fscryptEntry) { e.MtimeNS++ }, func(e *fscryptEntry) { e.Size-- },
	} {
		locked := file
		alter(&locked)
		if file.checkLockedMetadata(kernelPath, locked) == nil {
			t.Fatal("accepted changed metadata")
		}
	}
	link := fscryptEntry{Mode: 0o120777, Size: 39}
	encoded := link
	encoded.Size = 96
	if link.checkLockedMetadata(kernelPath, encoded) != nil || link.checkLockedMetadata(fusePath, link) != nil ||
		link.checkLockedMetadata(kernelPath, link) == nil || link.checkLockedMetadata(fusePath, encoded) == nil {
		t.Fatal("incorrect locked symlink size qualification")
	}
}
