//go:build all || integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
)

// osdIdentityView is the part of the OSD map and the CRUSH tree that an OSD
// removal, reweight, stop or identity change touches, keyed by OSD ID: the
// registration's UUID, up and in, the reweight and primary affinity from
// "osd dump", and the CRUSH weight from "osd tree". The map epoch is kept
// apart: the cluster commits new maps for reasons unrelated to the guarded
// call (PG temp mappings, clean intervals), so a raw document comparison
// cannot attribute a change.
type osdIdentityView struct {
	Epoch int
	OSDs  map[int]osdIdentityEntry
}

type osdIdentityEntry struct {
	UUID            string  `json:"uuid"`
	Up              int     `json:"up"`
	In              int     `json:"in"`
	Weight          float64 `json:"weight"`
	PrimaryAffinity float64 `json:"primary_affinity"`
	CrushWeight     float64 `json:"-"`
	InCrush         bool    `json:"-"`
}

// parseOSDIdentityView decodes "osd dump" and "osd tree" in JSON.
func parseOSDIdentityView(dump, tree []byte) (osdIdentityView, error) {
	var d struct {
		Epoch *int `json:"epoch"`
		OSDs  []struct {
			ID *int `json:"osd"`
			osdIdentityEntry
		} `json:"osds"`
	}
	if err := json.Unmarshal(dump, &d); err != nil || d.Epoch == nil {
		return osdIdentityView{}, fmt.Errorf("osd dump: %v: %.256s", err, dump)
	}
	v := osdIdentityView{Epoch: *d.Epoch, OSDs: map[int]osdIdentityEntry{}}
	for _, o := range d.OSDs {
		if o.ID == nil {
			return osdIdentityView{}, fmt.Errorf("osd dump has an OSD without an ID: %.256s", dump)
		}
		v.OSDs[*o.ID] = o.osdIdentityEntry
	}
	var tr struct {
		Nodes []struct {
			ID          int     `json:"id"`
			Type        string  `json:"type"`
			CrushWeight float64 `json:"crush_weight"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(tree, &tr); err != nil {
		return osdIdentityView{}, fmt.Errorf("osd tree: %v: %.256s", err, tree)
	}
	for _, n := range tr.Nodes {
		if n.Type != "osd" {
			continue
		}
		e := v.OSDs[n.ID]
		e.CrushWeight, e.InCrush = n.CrushWeight, true
		v.OSDs[n.ID] = e
	}
	return v, nil
}

// osdIdentityChanges lists, per OSD and field, what differs between two views.
// An empty list is the no-side-effect verdict of the OSD map and CRUSH tree; the
// epoch is not compared.
func osdIdentityChanges(before, after osdIdentityView) []string {
	var changes []string
	ids := map[int]bool{}
	for id := range before.OSDs {
		ids[id] = true
	}
	for id := range after.OSDs {
		ids[id] = true
	}
	for _, id := range slices.Sorted(maps.Keys(ids)) {
		b, inBefore := before.OSDs[id]
		a, inAfter := after.OSDs[id]
		switch {
		case !inAfter:
			changes = append(changes, fmt.Sprintf("osd.%d removed", id))
			continue
		case !inBefore:
			changes = append(changes, fmt.Sprintf("osd.%d added", id))
			continue
		}
		for _, f := range []struct {
			name          string
			before, after any
		}{
			{"uuid", b.UUID, a.UUID}, {"up", b.Up, a.Up}, {"in", b.In, a.In}, {"reweight", b.Weight, a.Weight},
			{"primary_affinity", b.PrimaryAffinity, a.PrimaryAffinity}, {"in_crush", b.InCrush, a.InCrush}, {"crush_weight", b.CrushWeight, a.CrushWeight},
		} {
			if f.before != f.after {
				changes = append(changes, fmt.Sprintf("osd.%d %s %v -> %v", id, f.name, f.before, f.after))
			}
		}
	}
	return changes
}

// osdRejectionSideEffects is every independent side effect a rejected removal
// must not have, each reported on its own.
type osdRejectionSideEffects struct {
	MapChanges   []string `json:"map_changes"`
	Mutations    int32    `json:"mutations"`
	Stops        int32    `json:"stops"`
	Terminations int32    `json:"terminations"`
	EpochBefore  int      `json:"epoch_before"`
	EpochAfter   int      `json:"epoch_after"`
}

// problems names each side effect that occurred.
func (s osdRejectionSideEffects) problems() []string {
	var out []string
	if len(s.MapChanges) != 0 {
		out = append(out, fmt.Sprintf("OSD map or CRUSH changes %v", s.MapChanges))
	}
	if s.Mutations != 0 {
		out = append(out, fmt.Sprintf("%d native mutations issued", s.Mutations))
	}
	if s.Stops != 0 {
		out = append(out, fmt.Sprintf("%d container stops", s.Stops))
	}
	if s.Terminations != 0 {
		out = append(out, fmt.Sprintf("%d container terminations", s.Terminations))
	}
	return out
}
