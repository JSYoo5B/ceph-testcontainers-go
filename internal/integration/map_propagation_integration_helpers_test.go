//go:build all || integration

package integration_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// monmapView is one MON's own view, from mon_status on its admin socket: its
// state and election epoch, and the epoch and members of the MonMap it holds.
type monmapView struct {
	Name          string   `json:"name"`
	State         string   `json:"state"`
	ElectionEpoch uint64   `json:"election_epoch"`
	Quorum        []int    `json:"quorum"`
	Epoch         uint64   `json:"monmap_epoch"`
	Members       []string `json:"members"`
}

func parseMonmapView(data []byte) (monmapView, error) {
	var status struct {
		Name          string `json:"name"`
		State         string `json:"state"`
		ElectionEpoch uint64 `json:"election_epoch"`
		Quorum        []int  `json:"quorum"`
		Monmap        *struct {
			Epoch uint64 `json:"epoch"`
			Mons  []struct {
				Name string `json:"name"`
			} `json:"mons"`
		} `json:"monmap"`
	}
	if err := json.Unmarshal(data, &status); err != nil {
		return monmapView{}, err
	}
	if status.Name == "" || status.Monmap == nil || status.Monmap.Epoch == 0 || len(status.Monmap.Mons) == 0 {
		return monmapView{}, errors.New("mon_status lacks its name or MonMap")
	}
	view := monmapView{Name: status.Name, State: status.State, ElectionEpoch: status.ElectionEpoch, Quorum: status.Quorum, Epoch: status.Monmap.Epoch}
	for _, mon := range status.Monmap.Mons {
		view.Members = append(view.Members, mon.Name)
	}
	slices.Sort(view.Members)
	return view, nil
}

// osdMapView is one OSD's own view, from status on its admin socket: the
// newest OSDMap epoch it holds.
type osdMapView struct {
	WhoAmI    int    `json:"whoami"`
	State     string `json:"state"`
	OldestMap uint64 `json:"oldest_map"`
	NewestMap uint64 `json:"newest_map"`
}

// mapEpoch is an epoch from an admin socket. OSD status prints oldest_map
// and newest_map as quoted numbers on both 19.2.5 and 20.2.4; a plain number
// is read too.
type mapEpoch uint64

func (e *mapEpoch) UnmarshalJSON(data []byte) error {
	value, err := strconv.ParseUint(strings.Trim(string(data), `"`), 10, 64)
	if err != nil {
		return fmt.Errorf("map epoch %s: %w", data, err)
	}
	*e = mapEpoch(value)
	return nil
}

func parseOSDMapView(data []byte) (osdMapView, error) {
	var status struct {
		WhoAmI    *int      `json:"whoami"`
		State     string    `json:"state"`
		OldestMap *mapEpoch `json:"oldest_map"`
		NewestMap *mapEpoch `json:"newest_map"`
	}
	if err := json.Unmarshal(data, &status); err != nil {
		return osdMapView{}, err
	}
	if status.WhoAmI == nil || status.OldestMap == nil || status.NewestMap == nil {
		return osdMapView{}, fmt.Errorf("osd status lacks whoami or its map range")
	}
	return osdMapView{WhoAmI: *status.WhoAmI, State: status.State, OldestMap: uint64(*status.OldestMap), NewestMap: uint64(*status.NewestMap)}, nil
}

// mapArrival is when one daemon first held a map epoch, in milliseconds after
// the change, with the view it showed then.
type mapArrival struct {
	Daemon string          `json:"daemon"`
	Epoch  uint64          `json:"epoch"`
	AfterM int64           `json:"after_ms"`
	View   json.RawMessage `json:"view"`
}
