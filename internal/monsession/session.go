// Package monsession validates the selected fields of native MON session lists.
package monsession

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Session contains identity fields common to MON session observations.
// A MON peer legitimately has an empty entity and global ID zero.
type Session struct {
	EntityName string `json:"entity_name"`
	ConType    string `json:"con_type"`
	GlobalID   uint64 `json:"global_id"`
}

// Decode requires an actual array and explicit identity fields. It validates
// MGR IDs before callers can classify beacon and module sessions.
func Decode(data []byte) ([]Session, error) {
	var rows []struct {
		Entity *string `json:"entity_name"`
		Kind   *string `json:"con_type"`
		ID     *uint64 `json:"global_id"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, errors.New("sessions is not a complete array")
	}
	result := make([]Session, 0, len(rows))
	for i, row := range rows {
		if row.Entity == nil || row.Kind == nil || *row.Kind == "" || row.ID == nil {
			return nil, fmt.Errorf("session %d lacks identity fields", i)
		}
		if strings.HasPrefix(*row.Entity, "mgr.") && (*row.ID == 0 || len(*row.Entity) == 4 || (*row.Kind != "mgr" && *row.Kind != "client")) {
			return nil, fmt.Errorf("session %d has incomplete MGR identity", i)
		}
		result = append(result, Session{*row.Entity, *row.Kind, *row.ID})
	}
	return result, nil
}

// Nonces reads the address vectors of every non-MON connection. MON peers
// cannot be mutation CLI clients and need no client nonce. A missing vector
// on any other row cannot establish that the stopped clients are absent.
func Nonces(data []byte) ([]uint64, error) {
	sessions, err := Decode(data)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		Addrs *struct {
			Vector *[]struct {
				Nonce *uint32 `json:"nonce"`
			} `json:"addrvec"`
		} `json:"addrs"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	var result []uint64
	for i, row := range rows {
		if sessions[i].ConType == "mon" {
			continue
		}
		if row.Addrs == nil || row.Addrs.Vector == nil || len(*row.Addrs.Vector) == 0 {
			return nil, fmt.Errorf("session %d lacks its address vector", i)
		}
		for _, addr := range *row.Addrs.Vector {
			if addr.Nonce == nil {
				return nil, fmt.Errorf("session %d lacks an address nonce", i)
			}
			result = append(result, uint64(*addr.Nonce))
		}
	}
	return result, nil
}
