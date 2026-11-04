package expect

import (
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"slices"
	"strings"
	"testing"
)

func replayingImage() mirror.ImageStatus {
	return mirror.ImageStatus{
		Pool: "volumes", SourceNamespace: "a", DestinationNamespace: "b", Name: "volume",
		GlobalID: "6c1b8f9e-0000-4000-8000-000000000001", SourceImageID: "1a2b", DestinationImageID: "3c4d",
		Mode: mirror.ModeSnapshot, SourcePrimary: true, SourceMirrorState: "enabled", DestinationMirrorState: "enabled",
		State: "up+replaying", Description: "replaying", LastUpdate: "2026-11-04 00:00:00",
		DaemonName: "a", InstanceID: "4101", ReplayReady: true,
	}
}

func TestImageMatchesPinnedReplay(t *testing.T) {
	baseline := replayingImage()
	got, err := Image{Select: baseline, Replaying: true, Daemon: "a"}.Evaluate(baseline)
	if err != nil || !got.Matched || len(got.Mismatches) != 0 || got.Observed != baseline {
		t.Fatalf("baseline does not match itself: %+v %v", got, err)
	}
	if s := got.String(); s != "image volumes/a:b/volume want replaying=true daemon=a: match" {
		t.Fatalf("summary: %q", s)
	}
}

func TestImageReportsEachReplayReasonFromTheEvaluatedObservation(t *testing.T) {
	baseline := replayingImage()
	observed := replayingImage()
	observed.ReplayReady = false
	observed.State, observed.DestinationMirrorState = "up+starting_replay", "creating"
	observed.SourcePrimary, observed.DestinationPrimary = false, true
	observed.DaemonName, observed.InstanceID = "b", "4101"
	got, err := Image{Select: baseline, Replaying: true, Daemon: "a", NewInstance: true}.Evaluate(observed)
	if err != nil || got.Matched {
		t.Fatalf("expected a valid mismatch: %+v %v", got, err)
	}
	want := []ImageMismatch{
		{Reason: ImageMismatchReplay, Want: "true", Observed: "false"},
		{Reason: ImageMismatchState, Want: "up+replaying", Observed: "up+starting_replay"},
		{Reason: ImageMismatchMirrorState, Want: "enabled/enabled", Observed: "enabled/creating"},
		{Reason: ImageMismatchPrimary, Want: "primary/non-primary", Observed: "non-primary/primary"},
		{Reason: ImageMismatchDaemon, Want: "a", Observed: "b"},
		{Reason: ImageMismatchInstance, Want: "4101", Observed: "4101"},
	}
	if !slices.Equal(got.Mismatches, want) {
		t.Fatalf("mismatches:\n got %+v\nwant %+v", got.Mismatches, want)
	}
	text := got.String()
	for _, part := range []string{"image volumes/a:b/volume want replaying=true daemon=a new instance: ", "replay: want true, observed false",
		"state: want up+replaying, observed up+starting_replay", "mirror-state: want enabled/enabled, observed enabled/creating",
		"primary: want primary/non-primary, observed non-primary/primary", "daemon: want a, observed b", "instance: want other than 4101, observed 4101"} {
		if !strings.Contains(text, part) {
			t.Fatalf("summary %q lacks %q", text, part)
		}
	}
}

func TestImagePinsIdentitiesSoAReplacementDoesNotMatch(t *testing.T) {
	baseline := replayingImage()
	replaced := replayingImage()
	replaced.GlobalID, replaced.SourceImageID, replaced.DestinationImageID, replaced.Mode = "6c1b8f9e-0000-4000-8000-000000000002", "5e6f", "", mirror.ModeJournal
	replaced.ReplayReady, replaced.State = false, "down+stopped"
	got, err := Image{Select: baseline, Replaying: true}.Evaluate(replaced)
	want := []ImageMismatch{
		{Reason: ImageMismatchMode, Want: "snapshot", Observed: "journal"},
		{Reason: ImageMismatchGlobalID, Want: baseline.GlobalID, Observed: replaced.GlobalID},
		{Reason: ImageMismatchSourceImage, Want: "1a2b", Observed: "5e6f"},
		{Reason: ImageMismatchDestinationImage, Want: "3c4d", Observed: ""},
	}
	// The replay fields of another image are not compared.
	if err != nil || !slices.Equal(got.Mismatches, want) || !strings.Contains(got.String(), `destination-image: want 3c4d, observed ""`) {
		t.Fatalf("replacement: %+v %v", got, err)
	}
	// An unpinned selection reads only the scope.
	if got, err := (Image{Select: mirror.ImageStatus{SourceNamespace: "a", DestinationNamespace: "b", Name: "volume"}}).Evaluate(replaced); err != nil || !got.Matched {
		t.Fatalf("unpinned selection: %+v %v", got, err)
	}
}

func TestImageScopeMismatchIsTheOnlyMismatch(t *testing.T) {
	observed := replayingImage()
	observed.DestinationNamespace, observed.ReplayReady, observed.State = "c", false, ""
	got, err := Image{Select: mirror.ImageStatus{SourceNamespace: "a", DestinationNamespace: "b", Name: "volume"}, Replaying: true}.Evaluate(observed)
	if err != nil || !slices.Equal(got.Mismatches, []ImageMismatch{{Reason: ImageMismatchScope, Want: "volumes/a:b/volume", Observed: "volumes/a:c/volume"}}) {
		t.Fatalf("scope: %+v %v", got, err)
	}
	got, err = Image{Select: mirror.ImageStatus{Pool: "other", SourceNamespace: "a", DestinationNamespace: "b", Name: "volume"}}.Evaluate(replayingImage())
	if err != nil || got.Matched || got.Mismatches[0].Reason != ImageMismatchScope {
		t.Fatalf("pool: %+v %v", got, err)
	}
}

func TestImageNotReplayingAllowsAStaleNativeReport(t *testing.T) {
	baseline := replayingImage()
	stale := replayingImage()
	stale.ReplayReady = false // a stopped receiver's up+replaying report outlives it
	if got, err := (Image{Select: baseline}).Evaluate(stale); err != nil || !got.Matched {
		t.Fatalf("stale report: %+v %v", got, err)
	}
	got, err := Image{Select: baseline}.Evaluate(baseline)
	if err != nil || !slices.Equal(got.Mismatches, []ImageMismatch{{Reason: ImageMismatchReplay, Want: "false", Observed: "true"}}) {
		t.Fatalf("replaying image passed as non-replaying: %+v %v", got, err)
	}
}

func TestImageNewInstanceAcceptsARestartedReceiver(t *testing.T) {
	baseline := replayingImage()
	restarted := replayingImage()
	restarted.InstanceID = "4301"
	if got, err := (Image{Select: baseline, Replaying: true, NewInstance: true}).Evaluate(restarted); err != nil || !got.Matched {
		t.Fatalf("restarted receiver: %+v %v", got, err)
	}
	// Without NewInstance, Select.InstanceID is not read.
	if got, err := (Image{Select: baseline, Replaying: true}).Evaluate(restarted); err != nil || !got.Matched {
		t.Fatalf("instance read without NewInstance: %+v %v", got, err)
	}
}

func TestImageRefusesInvalidExpectations(t *testing.T) {
	observed := replayingImage()
	pinned := replayingImage()
	pinned.InstanceID = ""
	named := func(name string) mirror.ImageStatus { s := replayingImage(); s.Name = name; return s }
	for name, e := range map[string]Image{
		"no name":                {Replaying: true},
		"pool syntax":            {Select: named("volumes/volume")},
		"snapshot syntax":        {Select: named("volume@snap")},
		"option syntax":          {Select: named("-volume")},
		"padded":                 {Select: named(" volume")},
		"unknown mode":           {Select: mirror.ImageStatus{Name: "volume", Mode: "mirror"}},
		"daemon without replay":  {Select: observed, Daemon: "a"},
		"new instance no replay": {Select: observed, NewInstance: true},
		"new instance unpinned":  {Select: pinned, Replaying: true, NewInstance: true},
	} {
		if got, err := e.Evaluate(observed); !errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || got.Observed.Name != "" {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestImageRefusesMalformedObservations(t *testing.T) {
	e := Image{Select: mirror.ImageStatus{SourceNamespace: "a", DestinationNamespace: "b", Name: "volume"}, Replaying: true}
	for name, mutate := range map[string]func(*mirror.ImageStatus){
		"no name":                 func(s *mirror.ImageStatus) { s.Name = "" },
		"ready without global ID": func(s *mirror.ImageStatus) { s.GlobalID = "" },
		"ready without source":    func(s *mirror.ImageStatus) { s.SourceImageID = "" },
		"ready without replica":   func(s *mirror.ImageStatus) { s.DestinationImageID = "" },
		"ready without daemon":    func(s *mirror.ImageStatus) { s.DaemonName = "" },
		"ready without instance":  func(s *mirror.ImageStatus) { s.InstanceID = "" },
	} {
		observed := replayingImage()
		mutate(&observed)
		got, err := e.Evaluate(observed)
		if err == nil || errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || len(got.Mismatches) != 0 {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestImageEvaluationKeepsACopyOfTheEvaluatedValues(t *testing.T) {
	selection, observed := replayingImage(), replayingImage()
	got, err := Image{Select: selection, Replaying: true}.Evaluate(observed)
	if err != nil || !got.Matched {
		t.Fatal(got, err)
	}
	selection.GlobalID, observed.State = "changed", "down+stopped"
	if got.Expectation.Select.GlobalID != replayingImage().GlobalID || got.Observed.State != "up+replaying" {
		t.Fatalf("result follows caller mutation: %+v", got)
	}
}

func TestImageEvaluationBoundsItsSummary(t *testing.T) {
	e := ImageEvaluation{Expectation: Image{Select: mirror.ImageStatus{Name: "volume"}}}
	for range 10 {
		e.Mismatches = append(e.Mismatches, ImageMismatch{Reason: ImageMismatchState, Want: "up+replaying", Observed: "down+stopped"})
	}
	text := e.String()
	if !strings.HasPrefix(text, "image */:/volume want replaying=false: ") || !strings.HasSuffix(text, "down+stopped; 2 more") || strings.Count(text, "state:") != 8 {
		t.Fatalf("unbounded summary: %q", text)
	}
}
