package ceph

import (
	"math"
	"strings"
	"testing"
)

// Lines that the MGR prometheus module of quay.io/ceph/ceph:v20.2.4 served,
// in its order; v19.2.5 printed the same families and label sets.
const nativeMetrics = `# HELP ceph_health_status Cluster health status
# TYPE ceph_health_status untyped
ceph_health_status 1.0
# HELP ceph_mgr_status MGR status (0=standby, 1=active)
# TYPE ceph_mgr_status gauge
ceph_mgr_status{ceph_daemon="mgr.a"} 1.0
# HELP ceph_osd_metadata OSD Metadata
# TYPE ceph_osd_metadata untyped
ceph_osd_metadata{back_iface="",ceph_daemon="osd.0",cluster_addr="172.19.0.4",device_class="hdd",front_iface="",hostname="2ea8641a9d6c",objectstore="bluestore",public_addr="172.19.0.4",ceph_version="ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable - RelWithDebInfo)"} 1.0
ceph_osd_metadata{back_iface="",ceph_daemon="osd.1",cluster_addr="172.19.0.5",device_class="hdd",front_iface="",hostname="48f061bab0b9",objectstore="bluestore",public_addr="172.19.0.5",ceph_version="ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable - RelWithDebInfo)"} 1.0
# HELP ceph_osd_up OSD status up
# TYPE ceph_osd_up untyped
ceph_osd_up{ceph_daemon="osd.0"} 1.0
ceph_osd_up{ceph_daemon="osd.1"} 1.0
# HELP ceph_pool_objects DF pool objects
# TYPE ceph_pool_objects gauge
ceph_pool_objects{pool_id="1"} 0.0
# HELP ceph_pool_wr DF pool wr
# TYPE ceph_pool_wr counter
ceph_pool_wr{pool_id="1"} 0.0
`

func TestParseNativeManagerMetrics(t *testing.T) {
	metrics, err := parseMetrics(nativeMetrics)
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics.Samples) != 8 || metrics.Types["ceph_pool_wr"] != "counter" || metrics.Types["ceph_osd_up"] != "untyped" ||
		metrics.Help["ceph_mgr_status"] != "MGR status (0=standby, 1=active)" {
		t.Fatalf("%d samples, types %v, help %v", len(metrics.Samples), metrics.Types, metrics.Help)
	}
	if up, err := metrics.Value("ceph_osd_up", map[string]string{"ceph_daemon": "osd.1"}); err != nil || up != 1 {
		t.Fatalf("osd.1 up %v %v", up, err)
	}
	if health, err := metrics.Value("ceph_health_status", nil); err != nil || health != 1 {
		t.Fatalf("health %v %v", health, err)
	}
	osd0 := metrics.Select("ceph_osd_metadata", map[string]string{"ceph_daemon": "osd.0", "objectstore": "bluestore"})
	if len(osd0) != 1 || osd0[0].Labels["ceph_version"] != "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable - RelWithDebInfo)" ||
		osd0[0].Labels["back_iface"] != "" {
		t.Fatalf("osd.0 metadata %+v", osd0)
	}
	// Two samples match without a label; Value wants exactly one.
	if _, err := metrics.Value("ceph_osd_up", nil); err == nil {
		t.Fatal("Value picked one of two samples")
	}
	if _, err := metrics.Value("ceph_osd_op_w", nil); err == nil {
		t.Fatal("Value found a missing metric")
	}
}

func TestParseMetricsFormat(t *testing.T) {
	metrics, err := parseMetrics("a{l=\"x\\\\y \\\"q\\\" \\n end\",m=\"1\"} NaN\nb +Inf 1700000000000\nc{} -Inf\nd 1e3\r\n\n")
	if err != nil {
		t.Fatal(err)
	}
	a := metrics.Samples[0]
	if a.Labels["l"] != "x\\y \"q\" \n end" || a.Labels["m"] != "1" || !math.IsNaN(a.Value) {
		t.Fatalf("a %+v", a)
	}
	if b := metrics.Samples[1]; !math.IsInf(b.Value, 1) || b.Timestamp != 1700000000000 {
		t.Fatalf("b %+v", b)
	}
	if c := metrics.Samples[2]; !math.IsInf(c.Value, -1) || len(c.Labels) != 0 {
		t.Fatalf("c %+v", c)
	}
	if d := metrics.Samples[3]; d.Value != 1000 {
		t.Fatalf("d %+v", d)
	}
	for _, malformed := range []string{
		`a{l="x"} `, `a{l="x} 1`, `a{l=x} 1`, `a{l="x" m="y"} 1`, `a{l="\q"} 1`, `a{l="x",l="y"} 1`,
		`a 1 2 3`, `a one`, `a 1 later`, `{l="x"} 1`,
		"a{l=\"x\"} 1\na{l=\"x\"} 2", // the same series twice
	} {
		if metrics, err := parseMetrics(malformed); err == nil {
			t.Errorf("%q parsed: %+v", malformed, metrics.Samples)
		}
	}
	// The same series with labels in another order is a duplicate too.
	if _, err := parseMetrics("a{x=\"1\",y=\"2\"} 1\na{y=\"2\",x=\"1\"} 1"); err == nil || !strings.Contains(err.Error(), "second sample") {
		t.Errorf("reordered duplicate: %v", err)
	}
}
