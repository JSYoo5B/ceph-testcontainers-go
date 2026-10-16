package cephfs

import (
	"errors"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The snap_schedule module's status of a one-minute schedule on "/" with a
// retention of three per minute, after three snapshots, as quay.io/ceph/ceph
// v20.2.4 printed it.
const nativeSnapshotScheduleStatus = `[{"fs": "tc-cephfs", "subvol": null, "group": null, "path": "/", "rel_path": "/", "schedule": "1m", "retention": {"m": 3}, "start": "2026-10-16T00:00:00", "created": "2026-10-16T22:21:31", "first": "2026-10-16T22:22:00", "last": "2026-10-16T22:24:00", "last_pruned": null, "created_count": 3, "pruned_count": 0, "active": true}]`

func TestParseSnapshotScheduleStatus(t *testing.T) {
	status, err := parseSnapshotScheduleStatus([]byte(nativeSnapshotScheduleStatus), "/", "1m")
	if err != nil {
		t.Fatal(err)
	}
	if status.Path != "/" || status.Schedule != "1m" || !status.Active || status.Created != 3 || status.Pruned != 0 ||
		status.Retention["m"] != 3 || !status.First.Equal(time.Date(2026, 10, 16, 22, 22, 0, 0, time.UTC)) ||
		!status.Last.Equal(time.Date(2026, 10, 16, 22, 24, 0, 0, time.UTC)) || len(status.Raw) == 0 {
		t.Fatalf("%+v", status)
	}
	// Before the first snapshot, first and last are null.
	fresh := strings.NewReplacer(`"first": "2026-10-16T22:22:00"`, `"first": null`, `"last": "2026-10-16T22:24:00"`, `"last": null`).Replace(nativeSnapshotScheduleStatus)
	if status, err := parseSnapshotScheduleStatus([]byte(fresh), "/", "1m"); err != nil || !status.First.IsZero() || !status.Last.IsZero() {
		t.Fatalf("fresh: %+v %v", status, err)
	}
	for name, test := range map[string]struct{ data, dir, repeat string }{
		"another directory": {nativeSnapshotScheduleStatus, "/data", "1m"},
		"another repeat":    {nativeSnapshotScheduleStatus, "/", "1h"},
		"no counts":         {strings.Replace(nativeSnapshotScheduleStatus, `"created_count": 3, `, ``, 1), "/", "1m"},
		"a bad time":        {strings.Replace(nativeSnapshotScheduleStatus, `"2026-10-16T22:24:00"`, `"yesterday"`, 1), "/", "1m"},
		"not a list":        {`{}`, "/", "1m"},
	} {
		if status, err := parseSnapshotScheduleStatus([]byte(test.data), test.dir, test.repeat); err == nil {
			t.Errorf("%s: %+v", name, status)
		}
	}
}

func TestValidateSnapshotSchedule(t *testing.T) {
	for _, valid := range []SnapshotSchedule{
		{Path: "/", Repeat: "1h"},
		{Path: "/data/dir", Repeat: "15m", Retention: map[string]int{"n": 2, "d": 7}},
		{Path: "/", Repeat: "2M", Retention: map[string]int{"y": 1}},
	} {
		if err := validateSnapshotSchedule(valid); err != nil {
			t.Errorf("%+v: %v", valid, err)
		}
	}
	for name, schedule := range map[string]SnapshotSchedule{
		"relative path":        {Path: "data", Repeat: "1h"},
		"unclean path":         {Path: "/data/", Repeat: "1h"},
		"empty repeat":         {Path: "/", Repeat: ""},
		"zero repeat":          {Path: "/", Repeat: "0h"},
		"seconds":              {Path: "/", Repeat: "30s"},
		"unknown period":       {Path: "/", Repeat: "1h", Retention: map[string]int{"x": 1}},
		"two-letter period":    {Path: "/", Repeat: "1h", Retention: map[string]int{"hh": 1}},
		"zero retention count": {Path: "/", Repeat: "1h", Retention: map[string]int{"n": 0}},
	} {
		if err := validateSnapshotSchedule(schedule); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
