package ceph

import (
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func storageOptions(t *testing.T, storages ...OSDStorage) (*options, error) {
	t.Helper()
	o := &options{blockSize: 1 << 30}
	for _, s := range storages {
		if err := WithOSDStorage(s)(o); err != nil {
			return o, err
		}
	}
	return o, nil
}

// Zero, one and several values.
func TestOSDStorageZeroOneAndSeveralValues(t *testing.T) {
	o, err := storageOptions(t, OSDStorage{})
	if err != nil || o.blockSize != 1<<30 || o.osdObjectStore != "" || o.osdMemorySize != 0 || o.osdVolumeStorage || o.osdStorageSet != 0 {
		t.Fatalf("the zero value changed the settings: %+v %v", o, err)
	}
	o, err = storageOptions(t, OSDStorage{}.WithBlockSize(256<<20))
	if err != nil || o.blockSize != 256<<20 || o.osdObjectStore != "" {
		t.Fatalf("one field: %+v %v", o, err)
	}
	o, err = storageOptions(t, OSDStorage{}.WithBlockSize(128<<20).WithObjectStore(OSDMemStore))
	if err != nil || o.blockSize != 128<<20 || o.osdObjectStore != OSDMemStore {
		t.Fatalf("chained fields: %+v %v", o, err)
	}
	// Different fields in different values compose in either order.
	for _, order := range [][]OSDStorage{
		{OSDStorage{}.WithBlockSize(128 << 20), OSDStorage{}.WithVolume(), OSDStorage{}},
		{OSDStorage{}, OSDStorage{}.WithVolume(), OSDStorage{}.WithBlockSize(128 << 20)},
	} {
		o, err = storageOptions(t, order...)
		if err != nil || o.blockSize != 128<<20 || !o.osdVolumeStorage {
			t.Fatalf("separate values: %+v %v", o, err)
		}
	}
	o, err = storageOptions(t, OSDStorage{}.WithInMemory(2<<30))
	if err != nil || o.osdMemorySize != 2<<30 {
		t.Fatalf("in-memory: %+v %v", o, err)
	}
}

// An explicit zero or empty setting is a refused setting, not an unspecified
// one, and unspecified keeps the default.
func TestOSDStorageExplicitZeroIsNotUnspecified(t *testing.T) {
	for name, s := range map[string]OSDStorage{
		"zero block size":     OSDStorage{}.WithBlockSize(0),
		"small block size":    OSDStorage{}.WithBlockSize(64<<20 - 1),
		"negative block size": OSDStorage{}.WithBlockSize(-1),
		"empty store":         OSDStorage{}.WithObjectStore(""),
		"unknown store":       OSDStorage{}.WithObjectStore("filestore"),
		"zero in-memory":      OSDStorage{}.WithInMemory(0),
		"negative in-memory":  OSDStorage{}.WithInMemory(-1),
		"valid then zero":     OSDStorage{}.WithBlockSize(128 << 20).WithBlockSize(0),
	} {
		if _, err := storageOptions(t, s); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if o, err := storageOptions(t, OSDStorage{}.WithObjectStore(OSDBlueStore)); err != nil || o.osdObjectStore != OSDBlueStore || o.blockSize != 1<<30 {
		t.Fatalf("explicit default store: %+v %v", o, err)
	}
	if _, err := storageOptions(t, OSDStorage{}.WithBlockSize(64<<20)); err != nil {
		t.Fatalf("the minimum block size was refused: %v", err)
	}
}

// A field set by two values is refused, also when they agree; within one value
// the later call replaces the earlier one.
func TestOSDStorageConflictsAreRefused(t *testing.T) {
	for name, values := range map[string][]OSDStorage{
		"block size twice":   {OSDStorage{}.WithBlockSize(128 << 20), OSDStorage{}.WithBlockSize(256 << 20)},
		"equal block sizes":  {OSDStorage{}.WithBlockSize(128 << 20), OSDStorage{}.WithBlockSize(128 << 20)},
		"store twice":        {OSDStorage{}.WithObjectStore(OSDMemStore), OSDStorage{}.WithObjectStore(OSDBlueStore)},
		"in-memory twice":    {OSDStorage{}.WithInMemory(1 << 30), OSDStorage{}.WithInMemory(2 << 30)},
		"volume twice":       {OSDStorage{}.WithVolume(), OSDStorage{}.WithVolume()},
		"gap between":        {OSDStorage{}.WithVolume(), OSDStorage{}, OSDStorage{}.WithVolume()},
		"overlap in a mixed": {OSDStorage{}.WithBlockSize(128 << 20).WithVolume(), OSDStorage{}.WithVolume()},
	} {
		if _, err := storageOptions(t, values...); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	o, err := storageOptions(t, OSDStorage{}.WithBlockSize(128<<20).WithBlockSize(256<<20))
	if err != nil || o.blockSize != 256<<20 {
		t.Fatalf("later call of one value did not replace the earlier: %+v %v", o, err)
	}
}

// Derived values leave their base and each other unchanged. OSDStorage holds no
// slice or map, so a struct copy is already independent; a member that holds
// one must be cloned with slices.Clone, which keeps nil and non-nil empty apart.
func TestOSDStorageDerivedValuesAreIndependent(t *testing.T) {
	base := OSDStorage{}.WithBlockSize(128 << 20)
	first, second := base.WithObjectStore(OSDMemStore), base.WithVolume()
	moved := base.WithBlockSize(512 << 20)
	if o, err := storageOptions(t, base); err != nil || o.blockSize != 128<<20 || o.osdObjectStore != "" || o.osdVolumeStorage {
		t.Fatalf("base changed by its derived values: %+v %v", o, err)
	}
	if o, err := storageOptions(t, first); err != nil || o.osdObjectStore != OSDMemStore || o.osdVolumeStorage || o.blockSize != 128<<20 {
		t.Fatalf("first: %+v %v", o, err)
	}
	if o, err := storageOptions(t, second); err != nil || !o.osdVolumeStorage || o.osdObjectStore != "" {
		t.Fatalf("second: %+v %v", o, err)
	}
	if o, err := storageOptions(t, moved); err != nil || o.blockSize != 512<<20 {
		t.Fatalf("moved: %+v %v", o, err)
	}
}

// OSDStorage is a plain value that reaches Run only through WithOSDStorage.
func TestOSDStorageIsNotACustomizer(t *testing.T) {
	if _, ok := any(OSDStorage{}).(testcontainers.ContainerCustomizer); ok {
		t.Fatal("OSDStorage is itself a customizer; it must be carried by WithOSDStorage")
	}
	var carrier testcontainers.ContainerCustomizer = WithOSDStorage(OSDStorage{}.WithVolume())
	request := &testcontainers.GenericContainerRequest{}
	if err := carrier.Customize(request); err != nil || request.Image != "" || request.Env != nil {
		t.Fatalf("the carrier changed the request: %v %+v", err, request)
	}
}

// Through Run: a refusal happens before any customizer runs and before the
// context is consulted, incompatible combinations are refused, and a valid
// configuration keeps the other settings (a service setting is never dropped).
func TestRunOSDStorageRefusesBeforeEffectsAndKeepsOtherSettings(t *testing.T) {
	calls := 0
	raw := testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { calls++; return nil })
	for name, opts := range map[string][]testcontainers.ContainerCustomizer{
		"zero block size":      {WithOSDStorage(OSDStorage{}.WithBlockSize(0)), raw},
		"conflict":             {WithOSDStorage(OSDStorage{}.WithVolume()), WithOSDStorage(OSDStorage{}.WithVolume()), raw},
		"memstore with memory": {WithOSDStorage(OSDStorage{}.WithObjectStore(OSDMemStore).WithInMemory(1 << 30)), raw},
		"volume with memory":   {WithOSDStorage(OSDStorage{}.WithVolume().WithInMemory(1 << 30)), raw},
		"volume with memstore": {WithOSDStorage(OSDStorage{}.WithVolume().WithObjectStore(OSDMemStore)), raw},
	} {
		calls = 0
		_, err := admit(t, "image", opts...)
		wantRefused(t, err)
		if calls != 0 {
			t.Errorf("%s: a customizer ran before the refusal", name)
		}
	}
	effective, err := admit(t, "image",
		WithOSDStorage(OSDStorage{}.WithBlockSize(128<<20).WithVolume()),
		(serviceHooks{}).WithServiceImage("rgw", "selected-gateway"), WithOSDCount(3), raw)
	wantAdmitted(t, err)
	if effective.blockSize != 128<<20 || !effective.osdVolumeStorage || effective.rgwOverride != "selected-gateway" || effective.osds != 3 || calls != 0 {
		t.Fatalf("a setting was dropped or reached a container: %+v calls=%d", effective, calls)
	}
}
