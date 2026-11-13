//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"strconv"
	"strings"
	"testing"
)

// reopenOSDDump is an `osd dump --format json` with osd.0 and the given vectors,
// each a JSON value or "" to omit the field.
func reopenOSDDump(public, cluster string) []byte {
	fields := []string{`"osd": 0`}
	if public != "" {
		fields = append(fields, `"public_addrs": `+public)
	}
	if cluster != "" {
		fields = append(fields, `"cluster_addrs": `+cluster)
	}
	return []byte(`{"osds": [{"osd": 1, "public_addrs": {"addrvec": [{"type": "v2", "addr": "10.0.0.9:6800", "nonce": 7}]}}, {` + strings.Join(fields, ", ") + `}]}`)
}

func reopenAddrvec(addr string, nonce int) string {
	return `{"addrvec": [{"type": "v2", "addr": "` + addr + `", "nonce": ` + strconv.Itoa(nonce) + `}]}`
}

// The reopen test compares both vectors, so the reader refuses a dump that
// lacks one: before the fix, a reopened OSD whose cluster vector was omitted
// passed with only a fresh public address.
func TestParseOSDMessengerAddrsNeedsBothVectors(t *testing.T) {
	got, err := parseOSDMessengerAddrs(reopenOSDDump(reopenAddrvec("10.0.0.2:6800", 1), reopenAddrvec("10.0.0.2:6801", 1)), 0)
	if err != nil || !slices.Equal(got.Public, []string{"v2:10.0.0.2:6800/1"}) || !slices.Equal(got.Cluster, []string{"v2:10.0.0.2:6801/1"}) {
		t.Fatalf("both vectors: %+v, %v", got, err)
	}
	// An explicit zero is a nonce the map reported, unlike an omitted one.
	zero, err := parseOSDMessengerAddrs(reopenOSDDump(reopenAddrvec("10.0.0.2:6800", 0), reopenAddrvec("10.0.0.2:6801", 0)), 0)
	if err != nil || !slices.Equal(zero.Public, []string{"v2:10.0.0.2:6800/0"}) {
		t.Fatalf("explicit zero nonce: %+v, %v", zero, err)
	}
	for name, data := range map[string][]byte{
		"cluster vector omitted":  reopenOSDDump(reopenAddrvec("10.0.0.2:6800", 2), ""),
		"public vector omitted":   reopenOSDDump("", reopenAddrvec("10.0.0.2:6801", 2)),
		"empty cluster vector":    reopenOSDDump(reopenAddrvec("10.0.0.2:6800", 2), `{"addrvec": []}`),
		"entry without address":   reopenOSDDump(reopenAddrvec("10.0.0.2:6800", 2), `{"addrvec": [{"type": "v2", "nonce": 2}]}`),
		"entry with null address": reopenOSDDump(reopenAddrvec("10.0.0.2:6800", 2), `{"addrvec": [{"type": "v2", "addr": null, "nonce": 2}]}`),
		"entry without type":      reopenOSDDump(reopenAddrvec("10.0.0.2:6800", 2), `{"addrvec": [{"addr": "10.0.0.2:6801", "nonce": 2}]}`),
		"entry with empty type":   reopenOSDDump(reopenAddrvec("10.0.0.2:6800", 2), `{"addrvec": [{"type": "", "addr": "10.0.0.2:6801", "nonce": 2}]}`),
		"entry without nonce":     reopenOSDDump(reopenAddrvec("10.0.0.2:6800", 2), `{"addrvec": [{"type": "v2", "addr": "10.0.0.2:6801"}]}`),
		"entry with null nonce":   reopenOSDDump(reopenAddrvec("10.0.0.2:6800", 2), `{"addrvec": [{"type": "v2", "addr": "10.0.0.2:6801", "nonce": null}]}`),
		"public nonce omitted":    reopenOSDDump(`{"addrvec": [{"type": "v2", "addr": "10.0.0.2:6800"}]}`, reopenAddrvec("10.0.0.2:6801", 2)),
		"negative nonce":          reopenOSDDump(reopenAddrvec("10.0.0.2:6800", 2), `{"addrvec": [{"type": "v2", "addr": "10.0.0.2:6801", "nonce": -1}]}`),
		"OSD absent":              []byte(`{"osds": [{"osd": 1, "public_addrs": {"addrvec": [{"type": "v2", "addr": "a:1", "nonce": 1}]}, "cluster_addrs": {"addrvec": [{"type": "v2", "addr": "a:2", "nonce": 1}]}}]}`),
		"not JSON":                []byte(`osd dump failed`),
	} {
		if got, err := parseOSDMessengerAddrs(data, 0); err == nil {
			t.Errorf("%s: accepted %+v", name, got)
		}
	}
}

// A reused address on either vector is reported, nonce included.
func TestReusedOSDMessengerAddrsComparesBothVectors(t *testing.T) {
	before := osdAddrs{Public: []string{"v2:10.0.0.2:6800/1"}, Cluster: []string{"v2:10.0.0.2:6801/1"}}
	for name, c := range map[string]struct {
		after osdAddrs
		want  []string
	}{
		"fresh":                 {osdAddrs{Public: []string{"v2:10.0.0.2:6800/2"}, Cluster: []string{"v2:10.0.0.2:6801/2"}}, nil},
		"cluster vector reused": {osdAddrs{Public: []string{"v2:10.0.0.2:6800/2"}, Cluster: []string{"v2:10.0.0.2:6801/1"}}, []string{"v2:10.0.0.2:6801/1"}},
		"public vector reused":  {osdAddrs{Public: []string{"v2:10.0.0.2:6800/1"}, Cluster: []string{"v2:10.0.0.2:6801/2"}}, []string{"v2:10.0.0.2:6800/1"}},
		"moved between vectors": {osdAddrs{Public: []string{"v2:10.0.0.2:6801/1"}, Cluster: []string{"v2:10.0.0.2:6800/2"}}, []string{"v2:10.0.0.2:6801/1"}},
	} {
		if got := reusedOSDMessengerAddrs(before, c.after); !slices.Equal(got, c.want) {
			t.Errorf("%s: %v, want %v", name, got, c.want)
		}
	}
}
