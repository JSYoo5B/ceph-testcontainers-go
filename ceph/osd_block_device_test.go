package ceph

import (
	"context"
	"go/ast"
	"slices"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

func syntheticBlockDevice() osdBlockDevice {
	return osdBlockDevice{hostPath: "/dev/mapper/tc-scratch-0", major: 253, minor: 4, sizeBytes: 128 << 20}
}

func TestOSDBlockDeviceValidation(t *testing.T) {
	if err := syntheticBlockDevice().validate(); err != nil {
		t.Fatalf("valid device refused: %v", err)
	}
	for name, mutate := range map[string]func(*osdBlockDevice){
		"empty-path":       func(d *osdBlockDevice) { d.hostPath = "" },
		"raw-device":       func(d *osdBlockDevice) { d.hostPath = "/dev/sda" },
		"loop-device":      func(d *osdBlockDevice) { d.hostPath = "/dev/loop3" },
		"dm-alias":         func(d *osdBlockDevice) { d.hostPath = "/dev/dm-4" },
		"by-id-alias":      func(d *osdBlockDevice) { d.hostPath = "/dev/disk/by-id/x" },
		"mapper-control":   func(d *osdBlockDevice) { d.hostPath = "/dev/mapper/control" },
		"mapper-root":      func(d *osdBlockDevice) { d.hostPath = "/dev/mapper/" },
		"mapper-subdir":    func(d *osdBlockDevice) { d.hostPath = "/dev/mapper/a/b" },
		"traversal":        func(d *osdBlockDevice) { d.hostPath = "/dev/mapper/../sda" },
		"relative":         func(d *osdBlockDevice) { d.hostPath = "dev/mapper/x" },
		"docker-spec":      func(d *osdBlockDevice) { d.hostPath = "/dev/mapper/x:/dev/sda:rw" },
		"whitespace":       func(d *osdBlockDevice) { d.hostPath = "/dev/mapper/x y" },
		"trailing-newline": func(d *osdBlockDevice) { d.hostPath = "/dev/mapper/x\n" },
		"leading-dot":      func(d *osdBlockDevice) { d.hostPath = "/dev/mapper/.x" },
		"zero-major":       func(d *osdBlockDevice) { d.major = 0 },
		"zero-size":        func(d *osdBlockDevice) { d.sizeBytes = 0 },
		"negative-size":    func(d *osdBlockDevice) { d.sizeBytes = -512 },
		"below-floor":      func(d *osdBlockDevice) { d.sizeBytes = 64<<20 - 512 },
		"unaligned-size":   func(d *osdBlockDevice) { d.sizeBytes = 128<<20 + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			d := syntheticBlockDevice()
			mutate(&d)
			if err := d.validate(); err == nil {
				t.Fatalf("%+v admitted", d)
			}
		})
	}
}

func blockDeviceRecipe() *osdLaunchRecipe {
	return &osdLaunchRecipe{image: "synthetic-osd-image", config: "[global]\nfsid = synthetic\n", adminKeyring: "k", osdKeyring: "k",
		id: 7, uuid: "synthetic-uuid", publicNetwork: "synthetic-public", owner: "synthetic-owner"}
}

func TestOSDLaunchRecipeMapsTheBlockDeviceIntoEachRequestOnly(t *testing.T) {
	r := blockDeviceRecipe()
	var plain testcontainers.GenericContainerRequest
	if err := r.Customize(&plain); err != nil {
		t.Fatal(err)
	}
	for key := range plain.Env {
		if strings.HasPrefix(key, "CEPH_OSD_BLOCK_DEVICE") {
			t.Fatalf("a recipe without a device sets %s", key)
		}
	}
	host := &container.HostConfig{}
	if plain.HostConfigModifier != nil {
		plain.HostConfigModifier(host)
	}
	if len(host.Devices) != 0 {
		t.Fatalf("a recipe without a device maps %v", host.Devices)
	}

	device := syntheticBlockDevice()
	if err := r.attachBlockDevice(device); err != nil {
		t.Fatal(err)
	}
	device.hostPath = "/dev/mapper/changed-by-caller"
	for range 2 {
		var req testcontainers.GenericContainerRequest
		if err := r.Customize(&req); err != nil {
			t.Fatal(err)
		}
		host := &container.HostConfig{Resources: container.Resources{Devices: []container.DeviceMapping{{PathOnHost: "/dev/synthetic", PathInContainer: "/dev/synthetic", CgroupPermissions: "r"}}}}
		req.HostConfigModifier(host)
		want := []container.DeviceMapping{
			{PathOnHost: "/dev/synthetic", PathInContainer: "/dev/synthetic", CgroupPermissions: "r"},
			{PathOnHost: "/dev/mapper/tc-scratch-0", PathInContainer: "/dev/tc-osd-block", CgroupPermissions: "rw"},
		}
		if !slices.Equal(host.Devices, want) {
			t.Fatalf("devices = %+v", host.Devices)
		}
		if req.Env["CEPH_OSD_BLOCK_DEVICE"] != "/dev/tc-osd-block" || req.Env["CEPH_OSD_BLOCK_DEVICE_ID"] != "253:4" || req.Env["CEPH_OSD_BLOCK_DEVICE_BYTES"] != "134217728" {
			t.Fatalf("device environment = %v", req.Env)
		}
		if host.Privileged || len(host.CapAdd) != 0 {
			t.Fatal("the device mapping must not add privilege or capabilities")
		}
	}
}

func TestOSDLaunchRecipeKeepsHostModeModifierWithABlockDevice(t *testing.T) {
	r := blockDeviceRecipe()
	r.hostNetwork, r.publicAddress = true, "192.0.2.7"
	if err := r.attachBlockDevice(syntheticBlockDevice()); err != nil {
		t.Fatal(err)
	}
	var req testcontainers.GenericContainerRequest
	if err := r.Customize(&req); err != nil {
		t.Fatal(err)
	}
	host := &container.HostConfig{}
	req.HostConfigModifier(host)
	if host.NetworkMode != "host" || len(host.Devices) != 1 {
		t.Fatalf("host mode %q, devices %v", host.NetworkMode, host.Devices)
	}
}

func TestOSDLaunchRecipeRefusesABlockDeviceItCannotUseAndStaysUnchanged(t *testing.T) {
	for name, setup := range map[string]func(*osdLaunchRecipe){
		"memstore":       func(r *osdLaunchRecipe) { r.memstore = true },
		"memory-storage": func(r *osdLaunchRecipe) { r.memoryVolume = "synthetic-memory" },
		"volume-storage": func(r *osdLaunchRecipe) { r.dataVolume = "synthetic-data" },
		"started":        func(r *osdLaunchRecipe) { r.started.Store(true) },
	} {
		t.Run(name, func(t *testing.T) {
			r := blockDeviceRecipe()
			setup(r)
			if err := r.attachBlockDevice(syntheticBlockDevice()); err == nil {
				t.Fatal("unusable combination admitted")
			}
			if r.blockDevice.Load() != nil {
				t.Fatal("a refused attachment left a device behind")
			}
		})
	}
	t.Run("invalid", func(t *testing.T) {
		r := blockDeviceRecipe()
		if err := r.attachBlockDevice(osdBlockDevice{hostPath: "/dev/sda", major: 8, sizeBytes: 1 << 30}); err == nil || r.blockDevice.Load() != nil {
			t.Fatal("invalid device admitted")
		}
	})
	t.Run("second", func(t *testing.T) {
		r := blockDeviceRecipe()
		if err := r.attachBlockDevice(syntheticBlockDevice()); err != nil {
			t.Fatal(err)
		}
		other := syntheticBlockDevice()
		other.hostPath = "/dev/mapper/tc-scratch-1"
		if err := r.attachBlockDevice(other); err == nil || r.blockDevice.Load().hostPath != "/dev/mapper/tc-scratch-0" {
			t.Fatal("a second device replaced the first")
		}
	})
	for name, drift := range map[string]func(*osdLaunchRecipe){
		"memory-volume": func(r *osdLaunchRecipe) { r.memoryVolume = "synthetic-memory" },
		"data-volume":   func(r *osdLaunchRecipe) { r.dataVolume = "synthetic-data" },
		"memstore":      func(r *osdLaunchRecipe) { r.memstore = true },
	} {
		t.Run("customize-rechecks-"+name, func(t *testing.T) {
			// A device attached to a recipe whose storage later changes must not
			// produce a request: Customize repeats the combination check.
			r := blockDeviceRecipe()
			if err := r.attachBlockDevice(syntheticBlockDevice()); err != nil {
				t.Fatal(err)
			}
			drift(r)
			var req testcontainers.GenericContainerRequest
			if err := r.Customize(&req); err == nil {
				t.Fatal("device and incompatible storage reached one request")
			}
		})
	}
}

func TestOSDLaunchStartsOneContainerWithTheMappedBlockDevice(t *testing.T) {
	c, launch := preparedOSDLaunch(t)
	if err := launch.recipe.attachBlockDevice(syntheticBlockDevice()); err != nil {
		t.Fatal(err)
	}
	var calls int
	var host container.HostConfig
	created := &initialOSDDaemon{id: launch.osd.ID}
	c.stores.runner = func(_ context.Context, _ string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		calls++
		var req testcontainers.GenericContainerRequest
		for _, opt := range opts {
			if err := opt.Customize(&req); err != nil {
				t.Fatal(err)
			}
		}
		req.HostConfigModifier(&host)
		return created, nil
	}
	if err := c.startOSD(t.Context(), launch); err != nil || calls != 1 {
		t.Fatalf("start: %v after %d creations", err, calls)
	}
	if len(host.Devices) != 1 || host.Devices[0].PathOnHost != "/dev/mapper/tc-scratch-0" {
		t.Fatalf("started container devices = %+v", host.Devices)
	}
	if err := launch.recipe.attachBlockDevice(syntheticBlockDevice()); err == nil {
		t.Fatal("a consumed launch accepted a device")
	}
	if err := c.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// TestOSDBlockDeviceSeamIsInternalAndLockFree guards the claims written on the
// seam: the only production caller of attachBlockDevice is the lock-holding
// wiring step registerBlockDeviceOSD, which also claims the device (see
// osd_device_wiring_test.go), the file
// declares no exported identifier and no mutex, and nothing outside the seam
// and the recipe names the type.
func TestOSDBlockDeviceSeamIsInternalAndLockFree(t *testing.T) {
	var problems []string
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Name.Name == "registerBlockDeviceOSD" {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "attachBlockDevice" {
					problems = append(problems, file+": "+fn.Name.Name+" calls attachBlockDevice; only registerBlockDeviceOSD may")
				}
				return true
			})
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.SelectorExpr:
				if file == "osd_block_device.go" && (x.Sel.Name == "Mutex" || x.Sel.Name == "RWMutex") {
					problems = append(problems, file+": declares a mutex")
				}
			case *ast.Ident:
				if x.Name == "osdBlockDevice" && file != "osd_block_device.go" && file != "osd_launch.go" && file != "osd_device_claims.go" && file != "osd_device_wiring.go" {
					problems = append(problems, file+": names osdBlockDevice")
				}
			}
			return true
		})
	}
	exported := 0
	for _, decl := range parseProductionFiles(t)["osd_block_device.go"].Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.IsExported() {
			exported++
		}
		if gen, ok := decl.(*ast.GenDecl); ok {
			for _, spec := range gen.Specs {
				if ts, ok := spec.(*ast.TypeSpec); ok && ts.Name.IsExported() {
					exported++
				}
			}
		}
	}
	if exported != 0 {
		problems = append(problems, "osd_block_device.go declares exported API")
	}
	if len(problems) != 0 {
		t.Fatalf("block device seam escaped its contract: %v", problems)
	}
}
