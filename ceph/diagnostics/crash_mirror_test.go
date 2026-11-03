package diagnostics

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidence"
)

// fillNonZero sets every field reachable from v, a pointer, to a non-zero
// value, with one element in each slice and map.
func fillNonZero(t *testing.T, v reflect.Value) {
	t.Helper()
	switch v.Kind() {
	case reflect.Pointer:
		fillNonZero(t, v.Elem())
	case reflect.Struct:
		if v.Type() == reflect.TypeFor[time.Time]() {
			v.Set(reflect.ValueOf(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)))
			return
		}
		for i := range v.NumField() {
			fillNonZero(t, v.Field(i))
		}
	case reflect.String:
		v.SetString("x-" + v.Type().Name())
	case reflect.Int, reflect.Int64:
		v.SetInt(7)
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		fillNonZero(t, v.Index(0))
	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
		v.SetMapIndex(reflect.ValueOf("field"), reflect.ValueOf(any("value")))
	default:
		t.Fatalf("no non-zero value for %s", v.Type())
	}
}

// The public crash types repeat the collector's: every reported field of a
// collected report, filled in full, reaches the public report with the same
// JSON, so a collector field that the public types or the conversion miss
// fails here. The public outcomes are the collector's.
func TestCrashMetadataTypesMirrorTheCollector(t *testing.T) {
	collected := &evidence.CrashReport{}
	fillNonZero(t, reflect.ValueOf(collected))
	want, err := json.Marshal(collected)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(crashMetadataReport(collected))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("public report\n%s\ncollected report\n%s", got, want)
	}
	for public, internal := range map[CrashOutcome]evidence.CrashOutcome{
		CrashRecordsFound: evidence.CrashRecordsFound, CrashEntriesRejected: evidence.CrashEntriesRejected,
		CrashDirectoryEmpty: evidence.CrashDirectoryEmpty, CrashDirectoryAbsent: evidence.CrashDirectoryAbsent,
		CrashContainerUnavailable: evidence.CrashContainerUnavailable, CrashReadFailed: evidence.CrashReadFailed,
		CrashArchiveIncomplete: evidence.CrashArchiveIncomplete,
	} {
		if string(public) != string(internal) {
			t.Errorf("outcome %q is %q in the collector", public, internal)
		}
	}
}
