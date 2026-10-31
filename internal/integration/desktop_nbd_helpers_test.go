//go:build all || integration

package integration_test

import "fmt"

// desktopNBDReceipt is what the rbd-nbd PoC of issue 368 reports.
type desktopNBDReceipt struct {
	Eligible       bool   `json:"eligible"`
	Device         string `json:"device"`
	MapError       string `json:"map_error"`
	DeviceReadback bool   `json:"device_readback"`
	DeviceBytes    int64  `json:"device_bytes"`
	Unmapped       bool   `json:"unmapped"`
	ListRC         *int   `json:"list_rc"`
	ListError      string `json:"list_error"`
	StillMapped    string `json:"still_mapped"`
	KernelAttached *bool  `json:"kernel_attached"`
	// KernelMapped is the same attribute while mapped: it shows that the
	// attribute is an observation of this device at all.
	KernelMapped   bool   `json:"kernel_attached_while_mapped"`
	LibrbdReadback bool   `json:"librbd_readback"`
	ExportError    string `json:"export_error"`
}

// judgeDesktopNBD returns "" when the mapping carried the pattern both ways and
// its removal is shown by two observations: rbd-nbd's enumeration, which counts
// only when it succeeded, and the kernel's device attribute. A missing or
// failed observation is not evidence that the mapping is gone.
func judgeDesktopNBD(r desktopNBDReceipt, imageBytes int64) string {
	switch {
	case r.MapError != "":
		return "rbd-nbd map: " + r.MapError
	case !r.DeviceReadback:
		return "the pattern did not read back through " + r.Device
	case r.DeviceBytes != imageBytes:
		return fmt.Sprintf("%s has %d bytes, want the image's %d", r.Device, r.DeviceBytes, imageBytes)
	case !r.Unmapped:
		return "rbd-nbd unmap failed"
	case r.ListRC == nil || *r.ListRC != 0:
		return "the mapping enumeration failed, so the unmap is not confirmed: " + r.ListError
	case r.StillMapped != "":
		return "still mapped after unmap: " + r.StillMapped
	case !r.KernelMapped:
		return "the kernel did not show " + r.Device + " connected while mapped, so its absence afterwards proves nothing"
	case r.KernelAttached == nil || *r.KernelAttached:
		return "the kernel still shows " + r.Device + " connected, or was not observed"
	case !r.LibrbdReadback:
		return "librbd did not read the pattern written through the device: " + r.ExportError
	}
	return ""
}
