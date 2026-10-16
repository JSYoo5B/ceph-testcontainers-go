//go:build all || integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// osdMapState is one OSD in "osd dump": whether it is up, the epoch it came
// up in, and the epoch it was last marked down in.
type osdMapState struct {
	OSD    int    `json:"osd"`
	UUID   string `json:"uuid"`
	Up     int    `json:"up"`
	UpFrom int    `json:"up_from"`
	DownAt int    `json:"down_at"`
}

// osdMap is the epoch and OSDs of one "osd dump".
type osdMap struct {
	Epoch int           `json:"epoch"`
	OSDs  []osdMapState `json:"osds"`
}

func parseOSDMap(data []byte) (osdMap, error) {
	var dump struct {
		Epoch *int           `json:"epoch"`
		OSDs  *[]osdMapState `json:"osds"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return osdMap{}, err
	}
	if dump.Epoch == nil || dump.OSDs == nil {
		return osdMap{}, fmt.Errorf("osd dump has no epoch or osds")
	}
	return osdMap{Epoch: *dump.Epoch, OSDs: *dump.OSDs}, nil
}

func (m osdMap) osd(id int) (osdMapState, bool) {
	for _, osd := range m.OSDs {
		if osd.OSD == id {
			return osd, true
		}
	}
	return osdMapState{}, false
}

// osdFailure is the MON's cluster log record of an OSD that its peers'
// heartbeat reports marked down.
type osdFailure struct {
	OSD       int     `json:"osd"`
	Reporters int     `json:"reporters"`
	Subtree   string  `json:"subtree"`
	After     float64 `json:"after_s"`
	Grace     float64 `json:"grace_s"`
	Line      string  `json:"line"`
}

var (
	osdFailedLine     = regexp.MustCompile(`cluster \[INF\] osd\.(\d+) failed \([^)]*\) \((\d+) reporters from different (\w+) after ([0-9.]+) >= grace ([0-9.]+)\)`)
	osdMarkedDeadLine = regexp.MustCompile(`cluster \[INF\] osd\.(\d+) marked itself dead as of e(\d+)`)
	osdStillRunning   = regexp.MustCompile(`cluster \[WRN\] Monitor daemon marked osd\.(\d+) down, but it is still running`)
)

// parseOSDFailure finds the last failure record of osd in a cluster log.
func parseOSDFailure(log string, osd int) (osdFailure, bool) {
	var found osdFailure
	ok := false
	for _, line := range strings.Split(log, "\n") {
		match := osdFailedLine.FindStringSubmatch(line)
		if match == nil || match[1] != strconv.Itoa(osd) {
			continue
		}
		reporters, _ := strconv.Atoi(match[2])
		after, err1 := strconv.ParseFloat(match[4], 64)
		grace, err2 := strconv.ParseFloat(match[5], 64)
		if err1 != nil || err2 != nil {
			continue
		}
		found, ok = osdFailure{OSD: osd, Reporters: reporters, Subtree: match[3], After: after, Grace: grace, Line: strings.TrimSpace(line)}, true
	}
	return found, ok
}

// parseOSDMarkedDead returns the epoch in the last "marked itself dead"
// record of osd, and whether the OSD said it was still running when it saw
// itself marked down.
func parseOSDMarkedDead(log string, osd int) (epoch int, stillRunning, ok bool) {
	for _, line := range strings.Split(log, "\n") {
		if match := osdStillRunning.FindStringSubmatch(line); match != nil && match[1] == strconv.Itoa(osd) {
			stillRunning = true
		}
		if match := osdMarkedDeadLine.FindStringSubmatch(line); match != nil && match[1] == strconv.Itoa(osd) {
			epoch, _ = strconv.Atoi(match[2])
			ok = true
		}
	}
	return epoch, stillRunning, ok
}
