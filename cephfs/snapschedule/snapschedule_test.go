package snapschedule

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
const nativeStatus = `[{"fs": "tc-cephfs", "subvol": null, "group": null, "path": "/", "rel_path": "/", "schedule": "1m", "retention": {"m": 3}, "start": "2026-10-16T00:00:00", "created": "2026-10-16T22:21:31", "first": "2026-10-16T22:22:00", "last": "2026-10-16T22:24:00", "last_pruned": null, "created_count": 3, "pruned_count": 0, "active": true}]`

func TestParseStatuses(t *testing.T) {
	statuses, err := parseStatuses([]byte(nativeStatus), "/")
	if err != nil || len(statuses) != 1 {
		t.Fatal(statuses, err)
	}
	status := statuses[0]
	if status.Path != "/" || status.Schedule != "1m" || !status.Active || status.Created != 3 || status.Pruned != 0 ||
		status.Retention["m"] != 3 || !status.First.Equal(time.Date(2026, 10, 16, 22, 22, 0, 0, time.UTC)) ||
		!status.Last.Equal(time.Date(2026, 10, 16, 22, 24, 0, 0, time.UTC)) || len(status.Raw) == 0 ||
		!status.Start.Equal(time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)) || !status.Added.Equal(time.Date(2026, 10, 16, 22, 21, 31, 0, time.UTC)) ||
		status.start != "2026-10-16T00:00:00" || status.added != "2026-10-16T22:21:31" {
		t.Fatalf("%+v", status)
	}
	// Before the first snapshot, first and last are null.
	fresh := strings.NewReplacer(`"first": "2026-10-16T22:22:00"`, `"first": null`, `"last": "2026-10-16T22:24:00"`, `"last": null`).Replace(nativeStatus)
	if statuses, err := parseStatuses([]byte(fresh), "/"); err != nil || !statuses[0].First.IsZero() || !statuses[0].Last.IsZero() {
		t.Fatalf("fresh: %+v %v", statuses, err)
	}
	// A directory without schedules prints an empty list.
	for _, empty := range []string{"\n[]\n", ""} {
		if statuses, err := parseStatuses([]byte(empty), "/"); err != nil || len(statuses) != 0 {
			t.Fatalf("empty %q: %+v %v", empty, statuses, err)
		}
	}
	if statuses, err := parseStatuses([]byte(nativeStatus), "/data"); err != nil || len(statuses) != 0 {
		t.Fatalf("another directory: %+v %v", statuses, err)
	}
	for name, data := range map[string]string{
		"no counts":   strings.Replace(nativeStatus, `"created_count": 3, `, ``, 1),
		"no start":    strings.Replace(nativeStatus, `"start": "2026-10-16T00:00:00", `, ``, 1),
		"no creation": strings.Replace(nativeStatus, `"created": "2026-10-16T22:21:31", `, ``, 1),
		"a bad time":  strings.Replace(nativeStatus, `"2026-10-16T22:24:00"`, `"yesterday"`, 1),
		"not a list":  `{}`,
	} {
		if statuses, err := parseStatuses([]byte(data), "/"); err == nil {
			t.Errorf("%s: %+v", name, statuses)
		}
	}
}

func TestValidate(t *testing.T) {
	for _, valid := range []Config{
		{Path: "/", Repeat: "1h"},
		{Path: "/data/dir", Repeat: "15m", Retention: map[string]int{"n": 2, "d": 7}},
		{Path: "/", Repeat: "2M", Retention: map[string]int{"y": 1}},
	} {
		if err := validate(valid); err != nil {
			t.Errorf("%+v: %v", valid, err)
		}
	}
	for name, schedule := range map[string]Config{
		"relative path":        {Path: "data", Repeat: "1h"},
		"unclean path":         {Path: "/data/", Repeat: "1h"},
		"empty repeat":         {Path: "/", Repeat: ""},
		"zero repeat":          {Path: "/", Repeat: "0h"},
		"seconds":              {Path: "/", Repeat: "30s"},
		"unknown period":       {Path: "/", Repeat: "1h", Retention: map[string]int{"x": 1}},
		"two-letter period":    {Path: "/", Repeat: "1h", Retention: map[string]int{"hh": 1}},
		"zero retention count": {Path: "/", Repeat: "1h", Retention: map[string]int{"n": 0}},
	} {
		if err := validate(schedule); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
