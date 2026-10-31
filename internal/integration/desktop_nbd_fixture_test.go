//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import "testing"

func TestJudgeDesktopNBDNeedsTwoSuccessfulCleanupObservations(t *testing.T) {
	zero, detached, attached := 0, false, true
	good := func() desktopNBDReceipt {
		return desktopNBDReceipt{Eligible: true, Device: "/dev/nbd0", DeviceReadback: true, DeviceBytes: 64 << 20, Unmapped: true,
			ListRC: &zero, KernelMapped: true, KernelAttached: &detached, LibrbdReadback: true}
	}
	if problem := judgeDesktopNBD(good(), 64<<20); problem != "" {
		t.Fatalf("a complete receipt was refused: %s", problem)
	}
	failed := 1
	for name, mutate := range map[string]func(*desktopNBDReceipt){
		"map error":                        func(r *desktopNBDReceipt) { r.MapError = "boom" },
		"device readback":                  func(r *desktopNBDReceipt) { r.DeviceReadback = false },
		"size":                             func(r *desktopNBDReceipt) { r.DeviceBytes = 1 << 20 },
		"unmap":                            func(r *desktopNBDReceipt) { r.Unmapped = false },
		"enumeration failed, empty output": func(r *desktopNBDReceipt) { r.ListRC, r.StillMapped = &failed, "" },
		"enumeration not reported":         func(r *desktopNBDReceipt) { r.ListRC = nil },
		"still listed":                     func(r *desktopNBDReceipt) { r.StillMapped = "/dev/nbd0" },
		"kernel still attached":            func(r *desktopNBDReceipt) { r.KernelAttached = &attached },
		"kernel not observed":              func(r *desktopNBDReceipt) { r.KernelAttached = nil },
		"attribute never seen":             func(r *desktopNBDReceipt) { r.KernelMapped = false },
		"librbd readback":                  func(r *desktopNBDReceipt) { r.LibrbdReadback = false },
	} {
		r := good()
		mutate(&r)
		if judgeDesktopNBD(r, 64<<20) == "" {
			t.Errorf("%s: accepted", name)
		}
	}
}
