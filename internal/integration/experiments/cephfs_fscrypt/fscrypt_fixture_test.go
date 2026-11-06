//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_fscrypt_test

import (
	"encoding/json"
	"os/exec"
	"testing"
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
