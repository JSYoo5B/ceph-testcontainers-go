package mdsmap

import (
	"errors"
	"slices"
)

// ColdExpectation is every value the cold-start decoder compares the
// native global map with; the decoder keeps no reference to their source.
type ColdExpectation struct {
	// Filesystem is the configured name of the owned filesystem.
	Filesystem string
	// ReservedName is the name the first MDS would take, which must be absent.
	ReservedName string
	// Native is the identity captured at creation; it must not be nil.
	Native *Native
	// DataPools are the data pool IDs attached when the filesystem was created.
	DataPools []int64
}

// DecodeCold strictly decodes one `fs dump` document and checks that the
// owned filesystem still has no rank, no worker and no assigned member and that
// no standby can join it. Every other filesystem must have a consistent rank
// membership without the reserved name. It is a function of its arguments: it
// takes no lock, reads no filesystem record, roster or container and starts no
// command. It refuses up:oneshot-replay.
func DecodeCold(data []byte, want ColdExpectation) error {
	var native globalMap
	if DecodeJSON(data, &native) != nil || native.Standbys == nil || native.Filesystems == nil {
		return errors.New("decode cold MDS map")
	}
	s := newSeen()
	found := 0
	for _, entry := range *native.Filesystems {
		if !admitIdentity(s, entry) {
			return errors.New("cold filesystem map entries are incomplete or duplicated")
		}
		m := entry.Map
		if *m.Name != want.Filesystem {
			if admitRanks(s, m, want.ReservedName) != faultNone {
				return errors.New("foreign MDS map is incomplete or conflicts with initial identity")
			}
			continue
		}
		found++
		if *entry.ID != want.Native.ID || m.Metadata == nil || *m.Metadata != want.Native.MetadataPool || m.Data == nil || !slices.Equal(*m.Data, want.DataPools) || m.Max == nil || *m.Max != 1 || m.Wanted == nil || *m.Wanted != 0 || m.Flags == nil || m.Flags.Joinable == nil || !*m.Flags.Joinable || m.Flags.Replay == nil || *m.Flags.Replay || m.Flags.Refuse == nil || !*m.Flags.Refuse {
			return errors.New("original cold filesystem policy or identity changed")
		}
		if m.Up == nil || m.In == nil || m.Failed == nil || m.Damaged == nil || m.Stopped == nil || len(*m.Info) != 0 || len(*m.Up) != 0 || len(*m.In) != 0 || len(*m.Failed) != 0 || len(*m.Damaged) != 0 || len(*m.Stopped) != 0 {
			return errors.New("cold filesystem already has MDS rank or worker state")
		}
	}
	if found != 1 {
		return errors.New("original cold filesystem map is absent")
	}
	for _, row := range *native.Standbys {
		if admitRow(s, row, want.ReservedName) != faultNone || *row.Rank != -1 || *row.State != "up:standby" {
			return errors.New("native standby map is incomplete or conflicting")
		}
		if *row.Join == -1 || *row.Join == want.Native.ID || !s.fsIDs[*row.Join] {
			return errors.New("foreign standby can join the cold filesystem")
		}
	}
	return nil
}
