//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// osdUpFrom returns the OSD map epoch from which the OSD's current process is
// up, or 0 while it is down.
func osdUpFrom(t *testing.T, ctx context.Context, cluster *ceph.Container, id int) int {
	t.Helper()
	out, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var dump struct {
		OSDs []struct {
			ID     int `json:"osd"`
			Up     int `json:"up"`
			UpFrom int `json:"up_from"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		t.Fatal(err)
	}
	for _, osd := range dump.OSDs {
		if osd.ID == id && osd.Up == 1 {
			return osd.UpFrom
		}
	}
	return 0
}
