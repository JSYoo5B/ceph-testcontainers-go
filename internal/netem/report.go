package netem

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

// Qdisc is one entry of tc's qdisc dump.
type Qdisc struct {
	Kind    string `json:"kind"`
	Handle  string `json:"handle"`
	Parent  string `json:"parent"`
	Root    bool   `json:"root"`
	Options struct {
		Bands   json.RawMessage `json:"bands"`
		Priomap json.RawMessage `json:"priomap"`
		Delay   struct {
			Delay  float64 `json:"delay"`
			Jitter float64 `json:"jitter"`
		} `json:"delay"`
		// Loss is a fraction; tc prints no delay member for a zero delay.
		Loss struct {
			Loss float64 `json:"loss"`
		} `json:"loss-random"`
		// Rate is netem's rate; tc prints no rate member without one.
		Rate  RateMember `json:"rate"`
		Limit int        `json:"limit"`
		// Reorder, Duplicate and Corrupt are netem's packet faults, and Gap
		// its reorder gap; tc prints no fault member without the fault.
		Reorder   FaultMember  `json:"reorder"`
		Duplicate FaultMember  `json:"duplicate"`
		Corrupt   FaultMember  `json:"corrupt"`
		Gap       NumberMember `json:"gap"`
	} `json:"options"`
	Bytes      uint64 `json:"bytes"`
	Packets    uint64 `json:"packets"`
	Drops      uint64 `json:"drops"`
	Overlimits uint64 `json:"overlimits"`
	Requeues   uint64 `json:"requeues"`
	Backlog    uint64 `json:"backlog"`
	Qlen       uint64 `json:"qlen"`
}

// RateMember is netem's rate member, an object whose rate is in bytes per
// second. Other qdiscs, such as a foreign tbf, print a plain number under
// the same key; it is not netem's rate, and reads as zero.
type RateMember struct {
	Rate uint64 `json:"rate"`
}

func (r *RateMember) UnmarshalJSON(data []byte) error {
	if trimmed := strings.TrimSpace(string(data)); !strings.HasPrefix(trimmed, "{") {
		*r = RateMember{}
		return nil
	}
	type object RateMember
	return json.Unmarshal(data, (*object)(r))
}

// FaultMember is one of netem's reorder, duplicate and corrupt members: an
// object with the probability under the member's own name and its
// correlation, both fractions. Another qdisc's member of the same name that
// is not such an object is not netem's, and reads as no fault.
type FaultMember map[string]float64

func (f *FaultMember) UnmarshalJSON(data []byte) error {
	*f = nil
	var members map[string]any
	if json.Unmarshal(data, &members) != nil {
		return nil
	}
	*f = FaultMember{}
	for key, value := range members {
		if number, ok := value.(float64); ok {
			(*f)[key] = number
		}
	}
	return nil
}

// Fault is the member in percentages; name is its probability's key.
func (f FaultMember) Fault(name string) Fault {
	return Fault{Percent: Percent(f[name]), Correlation: Percent(f["correlation"])}
}

// NumberMember is netem's gap, a whole number; another qdisc's member of the
// same name that is not one reads as zero.
type NumberMember int

func (n *NumberMember) UnmarshalJSON(data []byte) error {
	var value float64
	if json.Unmarshal(data, &value) != nil || value != math.Trunc(value) || math.Abs(value) > math.MaxInt32 {
		*n = 0
		return nil
	}
	*n = NumberMember(value)
	return nil
}

// Report is the JSON object that Script prints.
type Report struct {
	Namespace string   `json:"namespace"`
	NetAdmin  bool     `json:"net_admin"`
	Missing   []string `json:"missing"`
	Broken    []string `json:"broken"`
	Add       []any    `json:"add"`
	Shown     string   `json:"shown"`
	Delete    []any    `json:"delete"`
	// Prio, U32, U32Shown and PrioDelete are the preflight of a profile with
	// peers: a prio root and a u32 filter on the helper's loopback.
	Prio       []any  `json:"prio"`
	U32        []any  `json:"u32"`
	U32Shown   string `json:"u32_shown"`
	PrioDelete []any  `json:"prio_delete"`
	// Rate, RateShown and RateDelete are the preflight of a Rate: a netem
	// qdisc with a rate and a queue limit on the helper's loopback.
	Rate       []any  `json:"rate"`
	RateShown  string `json:"rate_shown"`
	RateDelete []any  `json:"rate_delete"`
	// Faults, FaultsShown and FaultsDelete are the preflight of a Reorder,
	// Duplicate or Corrupt: a netem qdisc with the requested faults, each
	// with a correlation, on the helper's loopback.
	Faults       []any           `json:"faults"`
	FaultsShown  string          `json:"faults_shown"`
	FaultsDelete []any           `json:"faults_delete"`
	Error        string          `json:"error"`
	Qdiscs       json.RawMessage `json:"qdiscs"`
	Filters      json.RawMessage `json:"filters"`
	Links        []struct {
		Name  string `json:"name"`
		Index int    `json:"index"`
	} `json:"interfaces"`
}

// Root returns the root qdisc of the inspected interface.
func (r Report) Root() (Qdisc, error) {
	var qdiscs []Qdisc
	if err := json.Unmarshal(r.Qdiscs, &qdiscs); err != nil {
		return Qdisc{}, fmt.Errorf("parse tc qdisc output: %w", err)
	}
	for _, qdisc := range qdiscs {
		if qdisc.Root {
			return qdisc, nil
		}
	}
	return Qdisc{}, errors.New("tc reported no root qdisc")
}
