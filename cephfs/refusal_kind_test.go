package cephfs_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/session"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/snapschedule"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/subvolume"
)

// operationsOnFilesystem are fixture operations that admit the filesystem
// before any native command.
func operationsOnFilesystem() map[string]func(context.Context, *cephfs.Filesystem, *cephfs.MDS) error {
	return map[string]func(context.Context, *cephfs.Filesystem, *cephfs.MDS) error{
		"DataPools": func(ctx context.Context, fs *cephfs.Filesystem, _ *cephfs.MDS) error {
			_, err := fs.DataPools(ctx)
			return err
		},
		"OverrideMaxFileSize": func(ctx context.Context, fs *cephfs.Filesystem, _ *cephfs.MDS) error {
			_, err := fs.OverrideMaxFileSize(ctx, cephfs.MinMaxFileSize)
			return err
		},
		"OverrideTiming": func(ctx context.Context, fs *cephfs.Filesystem, _ *cephfs.MDS) error {
			_, err := fs.OverrideTiming(ctx, cephfs.TimingProfile{DirectoryPins: true})
			return err
		},
		"ForceReadOnly": func(ctx context.Context, _ *cephfs.Filesystem, mds *cephfs.MDS) error {
			_, err := mds.ForceReadOnly(ctx)
			return err
		},
		"subvolume.Create": func(ctx context.Context, fs *cephfs.Filesystem, _ *cephfs.MDS) error {
			_, err := subvolume.Create(ctx, fs, subvolume.Config{Name: "volume"})
			return err
		},
		"session.List": func(ctx context.Context, fs *cephfs.Filesystem, _ *cephfs.MDS) error {
			_, err := session.List(ctx, fs)
			return err
		},
		"snapschedule.Add": func(ctx context.Context, fs *cephfs.Filesystem, _ *cephfs.MDS) error {
			_, err := snapschedule.Add(ctx, fs, snapschedule.Config{Path: "/", Repeat: "1h"})
			return err
		},
		"MDSStatus": func(ctx context.Context, fs *cephfs.Filesystem, _ *cephfs.MDS) error {
			_, err := fs.MDSStatus(ctx)
			return err
		},
	}
}

// overrides are the per-filesystem MDSMap overrides.
func overrides() map[string]func(context.Context, *cephfs.Filesystem) error {
	return map[string]func(context.Context, *cephfs.Filesystem) error{
		"OverrideMaxFileSize": func(ctx context.Context, fs *cephfs.Filesystem) error {
			_, err := fs.OverrideMaxFileSize(ctx, cephfs.MinMaxFileSize)
			return err
		},
		"session.OverrideTimeouts": func(ctx context.Context, fs *cephfs.Filesystem) error {
			_, err := session.OverrideTimeouts(ctx, fs, session.Limits{Timeout: 30 * time.Second, Autoclose: 30 * time.Second})
			return err
		},
		"session.OverrideRefusal": func(ctx context.Context, fs *cephfs.Filesystem) error {
			_, err := session.OverrideRefusal(ctx, fs)
			return err
		},
		"session.OverrideRequiredFeatures": func(ctx context.Context, fs *cephfs.Filesystem) error {
			_, err := session.OverrideRequiredFeatures(ctx, fs, "metric_collect")
			return err
		},
	}
}

// A removed filesystem is one refusal kind for every operation that admits
// it, whatever package the operation is in. MDSStatus names no watch.
func TestOperationsOnARemovedFilesystemAreInvalidRequests(t *testing.T) {
	for name, operation := range operationsOnFilesystem() {
		t.Run(name, func(t *testing.T) {
			fs, mds, remove, _ := cephfs.SyntheticFilesystemForTest(t)
			remove()
			err := operation(t.Context(), fs, mds)
			if !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("%s on a removed filesystem: %v, want ceph.ErrInvalidRequest", name, err)
			}
			if strings.Contains(err.Error(), "watch MDSs") {
				t.Fatalf("%s on a removed filesystem names the MDS watch: %v", name, err)
			}
		})
	}
}

// The overrides refuse a filesystem of a terminated cluster with one kind.
func TestOverridesOnATerminatedClusterAreInvalidRequests(t *testing.T) {
	for name, override := range overrides() {
		t.Run(name, func(t *testing.T) {
			fs, _, _, terminate := cephfs.SyntheticFilesystemForTest(t)
			terminate()
			if err := override(t.Context(), fs); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("%s on a terminated cluster: %v, want ceph.ErrInvalidRequest", name, err)
			}
		})
	}
}

// Every exported Filesystem and MDS method refuses a nil receiver with
// ceph.ErrInvalidRequest instead of panicking.
func TestNilFilesystemAndMDSMethodsAreInvalidRequests(t *testing.T) {
	var fs *cephfs.Filesystem
	var mds *cephfs.MDS
	ctx := t.Context()
	methods := map[string]func() error{
		"Filesystem.DataPools": func() error { _, err := fs.DataPools(ctx); return err },
		"Filesystem.AddDataPool": func() error {
			_, err := fs.AddDataPool(ctx, "pool")
			return err
		},
		"Filesystem.ScaleMDS":  func() error { return fs.ScaleMDS(ctx, 1, 0) },
		"Filesystem.MDSStatus": func() error { _, err := fs.MDSStatus(ctx); return err },
		"Filesystem.WaitReady": func() error { return fs.WaitReady(ctx) },
		"Filesystem.Remove":    func() error { return fs.Remove(ctx) },
		"Filesystem.WatchMDSs": func() error { _, err := fs.WatchMDSs(ctx, ceph.WatchConfig{}); return err },
		"Filesystem.OverrideMaxFileSize": func() error {
			_, err := fs.OverrideMaxFileSize(ctx, cephfs.MinMaxFileSize)
			return err
		},
		"Filesystem.OverrideTiming": func() error {
			_, err := fs.OverrideTiming(ctx, cephfs.TimingProfile{DirectoryPins: true})
			return err
		},
		"MDS.AddReplacement": func() error { _, err := mds.AddReplacement(ctx); return err },
		"MDS.ForceReadOnly":  func() error { _, err := mds.ForceReadOnly(ctx); return err },
		"MDS.RemoveStopped":  func() error { return mds.RemoveStopped(ctx) },
	}
	for name, method := range methods {
		t.Run(name, func(t *testing.T) {
			if err := method(); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("%s on a nil receiver: %v, want ceph.ErrInvalidRequest", name, err)
			}
		})
	}
	if fs.MDSs() != nil {
		t.Fatal("MDSs of a nil filesystem is not nil")
	}
	// The same refusal holds for a handle that was never returned by the
	// package: a zero MDS has no creation identity.
	zero := &cephfs.MDS{}
	if _, err := zero.AddReplacement(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("AddReplacement of a zero MDS: %v", err)
	}
	if err := zero.RemoveStopped(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("RemoveStopped of a zero MDS: %v", err)
	}
}
