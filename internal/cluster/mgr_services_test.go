package cluster

import (
	"maps"
	"testing"
)

func TestManagerServicesDecodesAdvertisedURLs(t *testing.T) {
	services, err := decodeManagerServices([]byte(`{"prometheus":"http://172.19.0.3:9283/","dashboard":"https://127.0.0.1:8443/"}`))
	if err != nil || !maps.Equal(services, map[string]string{"prometheus": "http://172.19.0.3:9283/", "dashboard": "https://127.0.0.1:8443/"}) {
		t.Fatalf("unexpected services: %v %v", services, err)
	}
	if services, err := decodeManagerServices([]byte(`{}`)); err != nil || services == nil || len(services) != 0 {
		t.Fatalf("empty native map was not retained: %v %v", services, err)
	}
	for _, data := range []string{`null`, `[]`, `{"prometheus":7}`, `{"prometheus":"not a url"}`, `{"prometheus":"ftp://host/"}`, `{"":"http://host/"}`} {
		if _, err := decodeManagerServices([]byte(data)); err == nil {
			t.Fatalf("malformed native services accepted: %s", data)
		}
	}
	ctr := &poolFixtureContainer{output: map[string]string{"mgr services --format json": `{"prometheus":"http://mgr:9283/"}`}}
	services, err = poolFixtureCluster(ctr, 1).ManagerServices(t.Context())
	if err != nil || services["prometheus"] != "http://mgr:9283/" {
		t.Fatalf("native services were not read: %v %v", services, err)
	}
}
