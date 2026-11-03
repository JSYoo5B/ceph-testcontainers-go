package ceph

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// osdKeyFileMon is the initial-OSD MON fake that also models the control
// container's file system for the registration key file: copies, removals and
// optionally failing ones.
type osdKeyFileMon struct {
	*initialOSDMon
	copied, removed []string
	copyErr         error
	rmExit          int
}

func (m *osdKeyFileMon) CopyToContainer(ctx context.Context, data []byte, path string, mode int64) error {
	m.copied = append(m.copied, path)
	if m.copyErr != nil {
		return m.copyErr
	}
	return m.initialOSDMon.CopyToContainer(ctx, data, path, mode)
}

func (m *osdKeyFileMon) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if len(args) == 3 && args[0] == "rm" && args[1] == "-f" {
		m.removed = append(m.removed, args[2])
		return m.rmExit, strings.NewReader(""), nil
	}
	return m.initialOSDMon.Exec(ctx, args, opts...)
}

func newOSDKeyFileCluster(t *testing.T) (*Container, *osdKeyFileMon) {
	t.Helper()
	c, mon, _ := newInitialOSDCluster(options{essentialImage: "synthetic-osd-image"})
	files := &osdKeyFileMon{initialOSDMon: mon}
	c.Container = files
	return c, files
}

func TestRegisterOSDRemovesItsKeyFile(t *testing.T) {
	c, files := newOSDKeyFileCluster(t)
	if _, err := c.osdFleet().registerOSD(t.Context(), OSDConfig{Root: "default", Host: "synthetic-host"}, c.settings.essentialImage, nil); err != nil {
		t.Fatal(err)
	}
	if len(files.copied) != 1 || !strings.HasPrefix(files.copied[0], "/tmp/osd-") {
		t.Fatalf("copied %q, want one registration key file", files.copied)
	}
	if !slices.Equal(files.removed, files.copied) {
		t.Fatalf("removed %q, want the key file %q", files.removed, files.copied)
	}
	if strays := c.ledger.authRecords.strayPaths(); len(strays) != 0 {
		t.Fatalf("a removed key file stays recorded: %q", strays)
	}
}

func TestRegisterOSDRecordsAKeyFileItCouldNotRemove(t *testing.T) {
	for name, fail := range map[string]func(*osdKeyFileMon){
		"failed removal": func(m *osdKeyFileMon) { m.rmExit = 1 },
		"failed copy":    func(m *osdKeyFileMon) { m.copyErr = errors.New("synthetic copy failure") },
	} {
		t.Run(name, func(t *testing.T) {
			c, files := newOSDKeyFileCluster(t)
			fail(files)
			_, err := c.osdFleet().registerOSD(t.Context(), OSDConfig{Root: "default", Host: "synthetic-host"}, c.settings.essentialImage, nil)
			if (files.copyErr != nil) != (err != nil) {
				t.Fatalf("registerOSD = %v", err)
			}
			if len(files.removed) != 1 || files.removed[0] != files.copied[0] {
				t.Fatalf("removed %q, want an attempt on %q", files.removed, files.copied)
			}
			if strays := c.ledger.authRecords.strayPaths(); !slices.Equal(strays, files.copied) {
				t.Fatalf("stray key files %q, want %q", strays, files.copied)
			}
		})
	}
}
