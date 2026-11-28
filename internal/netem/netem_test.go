package netem

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestArguments(t *testing.T) {
	for name, test := range map[string]struct {
		params Params
		want   string
	}{
		"delay":       {Params{Delay: 10 * time.Millisecond}, "netem delay 10000us"},
		"jitter":      {Params{Delay: 10 * time.Millisecond, Jitter: time.Millisecond}, "netem delay 10000us 1000us"},
		"loss":        {Params{Loss: 100}, "netem loss 100%"},
		"rate":        {Params{Rate: 8e6, Limit: 1000}, "netem rate 8000000bit limit 1000"},
		"faults":      {Params{Delay: time.Millisecond, Reorder: Fault{Percent: 25}, Corrupt: Fault{Percent: 0.5, Correlation: 5}}, "netem delay 1000us reorder 25% corrupt 0.5% 5%"},
		"duplicate":   {Params{Duplicate: Fault{Percent: 5}}, "netem duplicate 5%"},
		"correlation": {Params{Duplicate: Fault{Percent: 5, Correlation: 10}}, "netem delay 0us 0us 0% duplicate 5% 10%"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := strings.Join(test.params.Arguments(), " "); got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
}

func TestPreflightArgs(t *testing.T) {
	got := Preflight{Rate: true, Faults: []string{"reorder", "corrupt"}, Peers: true, Match: Match{Source: true, Setup: true}}.Args()
	if want := []string{"preflight", "rate", "reorder", "corrupt", "peers", "sport", "setup"}; !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got := (Preflight{Match: Match{Setup: true}}).Args(); !slices.Equal(got, []string{"preflight"}) {
		t.Fatalf("a match without peers adds arguments: %q", got)
	}
}

func TestVerify(t *testing.T) {
	params := Params{Delay: 10 * time.Millisecond, Loss: 1, Rate: 8e6, Limit: 1000, Reorder: Fault{Percent: 25}}
	applied := State{Applied: true, Delay: params.Delay, Loss: 1.00001, Rate: 8e6, Limit: 1000, Reorder: Fault{Percent: 25}, Gap: 1}
	if err := params.Verify(applied); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*State){
		"not applied":      func(s *State) { s.Applied = false },
		"another delay":    func(s *State) { s.Delay = time.Millisecond },
		"another limit":    func(s *State) { s.Limit = 100 },
		"a missing fault":  func(s *State) { s.Reorder = Fault{} },
		"an extra fault":   func(s *State) { s.Corrupt = Fault{Percent: 1} },
		"another gap":      func(s *State) { s.Gap = 0 },
		"another rate":     func(s *State) { s.Rate = 0 },
		"another loss":     func(s *State) { s.Loss = 2 },
		"with a mismatch":  func(s *State) { s.Applied, s.Mismatch = false, errors.New("a foreign filter") },
		"a foreign jitter": func(s *State) { s.Jitter = time.Millisecond },
	} {
		t.Run(name, func(t *testing.T) {
			state := applied
			edit(&state)
			err := params.Verify(state)
			if err == nil {
				t.Fatal("accepted")
			}
			if strings.Contains(err.Error(), "%!") {
				t.Fatalf("malformed error %q", err)
			}
			if state.Mismatch != nil && !errors.Is(err, state.Mismatch) {
				t.Fatalf("the mismatch is lost: %v", err)
			}
		})
	}
}
