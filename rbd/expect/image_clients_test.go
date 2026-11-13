package expect

import (
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"slices"
	"strings"
	"testing"
)

const (
	ownerAddress  = "10.0.0.2:0/2001"
	formerAddress = "10.0.0.1:0/1001"
	otherAddress  = "10.0.0.3:0/3001"
)

// lockedImage is an image whose exclusive lock client.4102 holds while an
// unrelated client also watches it.
func lockedImage() rbd.ImageClientStatus {
	return rbd.ImageClientStatus{
		Watchers: []rbd.ImageWatcher{
			{Address: otherAddress, ClientID: 4103, Cookie: 93},
			{Address: ownerAddress, ClientID: 4102, Cookie: 92},
		},
		Locks: []rbd.ImageLock{
			{ID: "advisory", Locker: "client.4103", Address: otherAddress},
			{ID: "auto 92", Locker: "client.4102", Address: ownerAddress, Managed: true},
		},
	}
}

func TestImageClientsMatchesTheOwnerAndKeepsUnrelatedClients(t *testing.T) {
	want := ImageClients{Lock: ExclusiveLockHeld, Owner: "client.4102", OwnerWatches: true, Departed: []string{formerAddress}}
	got, err := want.Evaluate(lockedImage())
	if err != nil || !got.Matched || len(got.Mismatches) != 0 {
		t.Fatalf("owner does not match: %+v %v", got, err)
	}
	if s := got.String(); s != "image clients want lock=held owner=client.4102 owner-watches departed=["+formerAddress+"]: match" {
		t.Fatalf("summary: %q", s)
	}
	// Any holder satisfies a held lock without an owner.
	if got, err := (ImageClients{Lock: ExclusiveLockHeld, OwnerWatches: true}).Evaluate(lockedImage()); err != nil || !got.Matched {
		t.Fatalf("any holder: %+v %v", got, err)
	}
}

func TestImageClientsReportsAWrongOwnerWithoutComparingItsWatch(t *testing.T) {
	observed := lockedImage()
	observed.Watchers = observed.Watchers[:1]
	got, err := ImageClients{Lock: ExclusiveLockHeld, Owner: "client.4101", OwnerWatches: true}.Evaluate(observed)
	want := []ImageClientsMismatch{{Reason: ImageClientsMismatchOwner, Want: "client.4101", Observed: "client.4102"}}
	if err != nil || got.Matched || !slices.Equal(got.Mismatches, want) {
		t.Fatalf("wrong owner: %+v %v", got, err)
	}
	if s := got.String(); !strings.HasSuffix(s, ": owner: want client.4101, observed client.4102") {
		t.Fatalf("summary: %q", s)
	}
}

func TestImageClientsReportsAMissingOwnerWatch(t *testing.T) {
	// The lock list already names the new owner, the watcher list does not.
	observed := lockedImage()
	observed.Watchers = []rbd.ImageWatcher{{Address: otherAddress, ClientID: 4103, Cookie: 93}, {Address: formerAddress, ClientID: 4101, Cookie: 91}}
	got, err := ImageClients{Lock: ExclusiveLockHeld, Owner: "client.4102", OwnerWatches: true}.Evaluate(observed)
	want := []ImageClientsMismatch{{Reason: ImageClientsMismatchOwnerWatch, Want: ownerAddress, Observed: otherAddress + " " + formerAddress}}
	if err != nil || got.Matched || !slices.Equal(got.Mismatches, want) {
		t.Fatalf("missing owner watch: %+v %v", got, err)
	}
	if s := got.String(); !strings.HasSuffix(s, ": owner-watch: want "+ownerAddress+" among watchers ["+otherAddress+" "+formerAddress+"]") {
		t.Fatalf("summary: %q", s)
	}
}

func TestImageClientsReportsALingeringFormerWatcher(t *testing.T) {
	observed := lockedImage()
	observed.Watchers = append(observed.Watchers, rbd.ImageWatcher{Address: formerAddress, ClientID: 4101, Cookie: 91})
	got, err := ImageClients{Lock: ExclusiveLockHeld, Owner: "client.4102", OwnerWatches: true, Departed: []string{otherAddress, formerAddress}}.Evaluate(observed)
	want := []ImageClientsMismatch{
		{Reason: ImageClientsMismatchDeparted, Observed: otherAddress},
		{Reason: ImageClientsMismatchDeparted, Observed: formerAddress},
	}
	if err != nil || got.Matched || !slices.Equal(got.Mismatches, want) {
		t.Fatalf("lingering watcher: %+v %v", got, err)
	}
	if s := got.String(); !strings.Contains(s, "departed: "+otherAddress+" still watches; departed: "+formerAddress+" still watches") {
		t.Fatalf("summary: %q", s)
	}
}

func TestImageClientsExpectsAbsence(t *testing.T) {
	idle := rbd.ImageClientStatus{Watchers: []rbd.ImageWatcher{}, Locks: []rbd.ImageLock{}}
	free := ImageClients{Lock: ExclusiveLockFree, Unwatched: true}
	if got, err := free.Evaluate(idle); err != nil || !got.Matched {
		t.Fatalf("idle image: %+v %v", got, err)
	}
	// A zero observation is an idle image too.
	if got, err := free.Evaluate(rbd.ImageClientStatus{}); err != nil || !got.Matched {
		t.Fatalf("zero observation: %+v %v", got, err)
	}
	got, err := free.Evaluate(lockedImage())
	want := []ImageClientsMismatch{
		{Reason: ImageClientsMismatchLock, Want: "free", Observed: "held by client.4102"},
		{Reason: ImageClientsMismatchWatched, Observed: otherAddress + " " + ownerAddress},
	}
	if err != nil || got.Matched || !slices.Equal(got.Mismatches, want) {
		t.Fatalf("busy image: %+v %v", got, err)
	}
	if s := got.String(); s != "image clients want lock=free unwatched: lock: want free, observed held by client.4102; watched: want no watchers, observed ["+otherAddress+" "+ownerAddress+"]" {
		t.Fatalf("summary: %q", s)
	}
	// An advisory lock is not the managed lock.
	advisory := lockedImage()
	advisory.Locks = advisory.Locks[:1]
	if got, err := (ImageClients{Lock: ExclusiveLockFree}).Evaluate(advisory); err != nil || !got.Matched {
		t.Fatalf("advisory lock: %+v %v", got, err)
	}
	// A crashed holder's lock record outlives its watch; Unwatched alone does not read the lock.
	crashed := lockedImage()
	crashed.Watchers = nil
	if got, err := (ImageClients{Unwatched: true}).Evaluate(crashed); err != nil || !got.Matched {
		t.Fatalf("lock without watchers: %+v %v", got, err)
	}
	got, err = ImageClients{Lock: ExclusiveLockHeld, Owner: "client.4102", OwnerWatches: true}.Evaluate(idle)
	if err != nil || !slices.Equal(got.Mismatches, []ImageClientsMismatch{{Reason: ImageClientsMismatchLock, Want: "held", Observed: "free"}}) {
		t.Fatalf("free lock against a held expectation: %+v %v", got, err)
	}
}

func TestImageClientsRefusesInvalidExpectations(t *testing.T) {
	for name, e := range map[string]ImageClients{
		"nothing":                  {},
		"unknown lock":             {Lock: "taken"},
		"owner without held lock":  {Owner: "client.4102"},
		"owner with free lock":     {Lock: ExclusiveLockFree, Owner: "client.4102"},
		"watch without held lock":  {OwnerWatches: true},
		"owner without entity":     {Lock: ExclusiveLockHeld, Owner: "4102"},
		"owner of another type":    {Lock: ExclusiveLockHeld, Owner: "osd.0"},
		"owner without id":         {Lock: ExclusiveLockHeld, Owner: "client."},
		"padded owner":             {Lock: ExclusiveLockHeld, Owner: "client.4102 "},
		"empty departed":           {Departed: []string{""}},
		"repeated departed":        {Departed: []string{formerAddress, formerAddress}},
		"unwatched owner watch":    {Lock: ExclusiveLockHeld, OwnerWatches: true, Unwatched: true},
		"unwatched with departure": {Departed: []string{formerAddress}, Unwatched: true},
	} {
		if got, err := e.Evaluate(lockedImage()); !errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || got.Observed.Watchers != nil {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestImageClientsRefusesMalformedObservations(t *testing.T) {
	e := ImageClients{Lock: ExclusiveLockHeld, OwnerWatches: true}
	for name, mutate := range map[string]func(*rbd.ImageClientStatus){
		"watcher without address": func(s *rbd.ImageClientStatus) { s.Watchers[0].Address = "" },
		"watcher without client":  func(s *rbd.ImageClientStatus) { s.Watchers[0].ClientID = 0 },
		"lock without ID":         func(s *rbd.ImageClientStatus) { s.Locks[1].ID = "" },
		"lock without locker":     func(s *rbd.ImageClientStatus) { s.Locks[1].Locker = "" },
		"lock without address":    func(s *rbd.ImageClientStatus) { s.Locks[0].Address = "" },
		"managed advisory lock":   func(s *rbd.ImageClientStatus) { s.Locks[0].Managed = true },
		"unmanaged auto lock":     func(s *rbd.ImageClientStatus) { s.Locks[1].Managed = false },
		"two managed locks": func(s *rbd.ImageClientStatus) {
			s.Locks = append(s.Locks, rbd.ImageLock{ID: "auto 91", Locker: "client.4101", Address: formerAddress, Managed: true})
		},
	} {
		observed := lockedImage()
		mutate(&observed)
		got, err := e.Evaluate(observed)
		if err == nil || errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || len(got.Mismatches) != 0 {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
}

func TestImageClientsEvaluationKeepsACopyOfTheEvaluatedValues(t *testing.T) {
	departed := []string{formerAddress}
	observed := lockedImage()
	got, err := ImageClients{Lock: ExclusiveLockHeld, Owner: "client.4102", Departed: departed}.Evaluate(observed)
	if err != nil || !got.Matched {
		t.Fatal(got, err)
	}
	departed[0], observed.Watchers[0].Address, observed.Locks[1].Locker = "changed", "changed", "changed"
	if got.Expectation.Departed[0] != formerAddress || got.Observed.Watchers[0].Address != otherAddress || got.Observed.Locks[1].Locker != "client.4102" {
		t.Fatalf("result follows caller mutation: %+v", got)
	}
}

func TestImageClientsEvaluationBoundsItsSummary(t *testing.T) {
	e := ImageClientsEvaluation{Expectation: ImageClients{Unwatched: true}}
	for range 10 {
		e.Mismatches = append(e.Mismatches, ImageClientsMismatch{Reason: ImageClientsMismatchDeparted, Observed: formerAddress})
	}
	text := e.String()
	if !strings.HasPrefix(text, "image clients want unwatched: ") || !strings.HasSuffix(text, "still watches; 2 more") || strings.Count(text, "departed:") != 8 {
		t.Fatalf("unbounded summary: %q", text)
	}
}
