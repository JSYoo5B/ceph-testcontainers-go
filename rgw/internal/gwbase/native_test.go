package gwbase

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRGWPlacementRuntimeScopeRejectsAmbiguity(t *testing.T) {
	valid := `{"services":{"osd":{"daemons":{"summary":""}},"rgw":{"daemons":{"summary":"","1":{"metadata":{"hostname":"fixture","pid":"1","frontend_config#0":"beast port=7480","realm_id":"realm","realm_name":"r","zone_id":"z","zone_name":"zone","zonegroup_id":"g","zonegroup_name":"group"}}}}}}`
	scope, err := decodeRuntimeScope([]byte(valid), "fixture", "beast port=7480")
	if err != nil || scope.RealmID != "realm" {
		t.Fatal("native scope was not derived")
	}
	for _, data := range []string{strings.Replace(valid, `"zone_id":"z"`, `"zone_id":""`, 1), strings.Replace(valid, `"pid":"1"`, `"pid":"2"`, 1), `{"services":null}`, "not-json"} {
		if _, err := decodeRuntimeScope([]byte(data), "fixture", "beast port=7480"); err == nil {
			t.Fatal("missing runtime identity accepted")
		}
	}
	var duplicate map[string]any
	if err := json.Unmarshal([]byte(valid), &duplicate); err != nil {
		t.Fatal(err)
	}
	daemons := duplicate["services"].(map[string]any)["rgw"].(map[string]any)["daemons"].(map[string]any)
	daemons["2"] = map[string]any{"metadata": map[string]string{"hostname": "fixture", "pid": "1", "frontend_config#0": "beast port=7480", "realm_id": "other", "zone_id": "other-z", "zone_name": "other-zone", "zonegroup_id": "other-g", "zonegroup_name": "other-group"}}
	data, _ := json.Marshal(duplicate)
	if _, err := decodeRuntimeScope(data, "fixture", "beast port=7480"); err == nil {
		t.Fatal("two different runtime scopes for one hostname were accepted")
	}
	// Host mode shares hostname and PID 1 across gateways. The actual daemon
	// listener must select exactly the requested gateway's native scope.
	first := daemons["1"].(map[string]any)["metadata"].(map[string]any)
	first["frontend_config#0"] = "beast endpoint=127.0.0.1:42001"
	second := daemons["2"].(map[string]any)["metadata"].(map[string]string)
	second["frontend_config#0"] = "beast endpoint=127.0.0.1:42002"
	data, _ = json.Marshal(duplicate)
	scope, err = decodeRuntimeScope(data, "fixture", "beast endpoint=127.0.0.1:42001")
	if err != nil || scope.RealmID != "realm" || scope.ZoneID != "z" {
		t.Fatalf("host listener did not select first native scope: %+v error=%v", scope, err)
	}
	scope, err = decodeRuntimeScope(data, "fixture", "beast endpoint=127.0.0.1:42002")
	if err != nil || scope.RealmID != "other" || scope.ZoneID != "other-z" {
		t.Fatalf("host listener did not select second native scope: %+v error=%v", scope, err)
	}
	daemons["2"] = "malformed daemon"
	data, _ = json.Marshal(duplicate)
	if _, err := decodeRuntimeScope(data, "fixture", "beast endpoint=127.0.0.1:42001"); err == nil {
		t.Fatal("malformed native daemon accepted")
	}
}
