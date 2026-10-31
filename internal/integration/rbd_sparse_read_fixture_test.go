//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"strings"
	"testing"
)

// The lines below are what one Squid 19.2.5 run of TestRBDSparseReadReceipt
// joined: the export client's sparse-read requests and the replies it
// received, and the OSD's replies, for image prefix rbd_data.105dfe1c3014.
var squidSparseClient = []string{
	`2026-10-31T01:07:49.406+0000 ffffa5265780  1 -- 172.19.0.6:0/3261843850 --> [v2:172.19.0.5:6800/4227786519,v1:172.19.0.5:6801/4227786519] -- osd_op(unknown.0.0:14 2.0 2:3f051c48:::rbd_data.105dfe1c3014.0000000000000000:head [sparse-read 0~4194304 [fadvise_nocache]] snapc 0=[] ondisk+read+known_if_redirected+supports_pool_eio e16) -- 0xffff80001440 con 0xaaaae2a6d9d0`,
	`2026-10-31T01:07:49.407+0000 ffffa853d780  1 -- 172.19.0.6:0/3261843850 <== osd.0 v2:172.19.0.5:6800/4227786519 14 ==== osd_op_reply(14 rbd_data.105dfe1c3014.0000000000000000 [sparse-read 0~65536 [fadvise_nocache] out=65560b] v0'0 uv11 ondisk = 0) ==== 182+0+65560 (crc 0 0 0) 0xffff9800a530 con 0xaaaae2a6d9d0`,
	`2026-10-31T01:07:49.406+0000 ffffa5a75780  1 -- 172.19.0.6:0/3261843850 --> [v2:172.19.0.5:6800/4227786519,v1:172.19.0.5:6801/4227786519] -- osd_op(unknown.0.0:12 2.0 2:7d327192:::rbd_data.105dfe1c3014.0000000000000001:head [sparse-read 0~4194304 [fadvise_nocache]] snapc 0=[] ondisk+read+known_if_redirected+supports_pool_eio e16) -- 0xffff88016890 con 0xaaaae2a6d9d0`,
	`2026-10-31T01:07:49.406+0000 ffffa853d780  1 -- 172.19.0.6:0/3261843850 <== osd.0 v2:172.19.0.5:6800/4227786519 12 ==== osd_op_reply(12 rbd_data.105dfe1c3014.0000000000000001 [sparse-read 0~4096 [fadvise_nocache] out=4120b] v0'0 uv13 ondisk = 0) ==== 182+0+4120 (crc 0 0 0) 0xffff9800a530 con 0xaaaae2a6d9d0`,
	`2026-10-31T01:07:49.406+0000 ffffa5a75780  1 -- 172.19.0.6:0/3261843850 --> [v2:172.19.0.5:6800/4227786519,v1:172.19.0.5:6801/4227786519] -- osd_op(unknown.0.0:13 2.0 2:bcef1a44:::rbd_data.105dfe1c3014.0000000000000002:head [sparse-read 0~4194304 [fadvise_nocache]] snapc 0=[] ondisk+read+known_if_redirected+supports_pool_eio e16) -- 0xffff88013260 con 0xaaaae2a6d9d0`,
	`2026-10-31T01:07:49.406+0000 ffffa853d780  1 -- 172.19.0.6:0/3261843850 <== osd.0 v2:172.19.0.5:6800/4227786519 13 ==== osd_op_reply(13 rbd_data.105dfe1c3014.0000000000000002 [sparse-read 0~4194304 [fadvise_nocache]] v0'0 uv0 ondisk = -2 ((2) No such file or directory)) ==== 182+0+0 (crc 0 0 0) 0xffff9800a530 con 0xaaaae2a6d9d0`,
	`2026-10-31T01:07:49.406+0000 ffffa5a75780  1 -- 172.19.0.6:0/3261843850 --> [v2:172.19.0.5:6800/4227786519,v1:172.19.0.5:6801/4227786519] -- osd_op(unknown.0.0:15 2.0 2:ff77f383:::rbd_data.105dfe1c3014.0000000000000003:head [sparse-read 0~4194304 [fadvise_nocache]] snapc 0=[] ondisk+read+known_if_redirected+supports_pool_eio e16) -- 0xffff88016890 con 0xaaaae2a6d9d0`,
	`2026-10-31T01:07:49.407+0000 ffffa853d780  1 -- 172.19.0.6:0/3261843850 <== osd.0 v2:172.19.0.5:6800/4227786519 15 ==== osd_op_reply(15 rbd_data.105dfe1c3014.0000000000000003 [sparse-read 0~4194304 [fadvise_nocache]] v0'0 uv0 ondisk = -2 ((2) No such file or directory)) ==== 182+0+0 (crc 0 0 0) 0xffff9800a530 con 0xaaaae2a6d9d0`,
}

var squidSparseOSD = []string{
	`2026-10-31T01:07:49.406+0000 ffff992580c0  1 -- [v2:172.19.0.5:6800/4227786519,v1:172.19.0.5:6801/4227786519] --> 172.19.0.6:0/3261843850 -- osd_op_reply(14 rbd_data.105dfe1c3014.0000000000000000 [sparse-read 0~65536 [fadvise_nocache] out=65560b] v0'0 uv11 ondisk = 0) -- 0xaaaac20f9b80 con 0xaaaac36b9c00`,
	`2026-10-31T01:07:49.406+0000 ffff97a280c0  1 -- [v2:172.19.0.5:6800/4227786519,v1:172.19.0.5:6801/4227786519] --> 172.19.0.6:0/3261843850 -- osd_op_reply(12 rbd_data.105dfe1c3014.0000000000000001 [sparse-read 0~4096 [fadvise_nocache] out=4120b] v0'0 uv13 ondisk = 0) -- 0xaaaac20a7400 con 0xaaaac36b9c00`,
	`2026-10-31T01:07:49.406+0000 ffff972180c0  1 -- [v2:172.19.0.5:6800/4227786519,v1:172.19.0.5:6801/4227786519] --> 172.19.0.6:0/3261843850 -- osd_op_reply(13 rbd_data.105dfe1c3014.0000000000000002 [sparse-read 0~4194304 [fadvise_nocache]] v0'0 uv0 ondisk = -2 ((2) No such file or directory)) -- 0xaaaac3016500 con 0xaaaac36b9c00`,
	`2026-10-31T01:07:49.406+0000 ffff98a480c0  1 -- [v2:172.19.0.5:6800/4227786519,v1:172.19.0.5:6801/4227786519] --> 172.19.0.6:0/3261843850 -- osd_op_reply(15 rbd_data.105dfe1c3014.0000000000000003 [sparse-read 0~4194304 [fadvise_nocache]] v0'0 uv0 ondisk = -2 ((2) No such file or directory)) -- 0xaaaac3016280 con 0xaaaac36b9c00`,
}

func squidSparseObjects() []sparseObject {
	object := func(n string) string { return "rbd_data.105dfe1c3014.000000000000000" + n }
	return []sparseObject{{object("0"), 64 << 10}, {object("1"), 4096}, {object("2"), 0}, {object("3"), 0}}
}

func TestJudgeSparseReadsSavedLines(t *testing.T) {
	joins, problems := judgeSparseReads(parseSparseReads(squidSparseClient), parseSparseReads(squidSparseOSD), squidSparseObjects())
	if len(problems) != 0 || len(joins) != 4 {
		t.Fatalf("joins %d, problems %v", len(joins), problems)
	}
	for _, join := range joins {
		if join.Request.Length != 4<<20 || join.Request.Offset != 0 {
			t.Errorf("%s requested %d~%d", join.Object.Object, join.Request.Offset, join.Request.Length)
		}
		if join.Object.Data > 0 && (join.Received.Out != int64(join.Object.Data)+sparseExtentMapSize || join.Received.Result != 0) {
			t.Errorf("%s: %+v", join.Object.Object, join.Received)
		}
	}
}

func TestJudgeSparseReadsRefuses(t *testing.T) {
	replace := func(lines []string, marker, old, new string) []string {
		result := slices.Clone(lines)
		index := slices.IndexFunc(result, func(l string) bool { return strings.Contains(l, marker) })
		if index < 0 || !strings.Contains(result[index], old) {
			t.Fatalf("no line with %q and %q", marker, old)
		}
		result[index] = strings.Replace(result[index], old, new, 1)
		return result
	}
	objects := squidSparseObjects()
	for name, c := range map[string]struct {
		client, osd []string
		objects     []sparseObject
		want        string
	}{
		"a plain read instead":        {replace(squidSparseClient, "0000000000000000:head", "[sparse-read ", "[read "), squidSparseOSD, objects, "sent no sparse-read of rbd_data.105dfe1c3014.0000000000000000"},
		"no OSD reply":                {squidSparseClient, slices.DeleteFunc(slices.Clone(squidSparseOSD), func(l string) bool { return strings.Contains(l, ".0000000000000001 [") }), objects, "sent by an OSD false"},
		"the full range returned":     {replace(squidSparseClient, "<== osd.0", "out=65560b", "out=4194328b"), squidSparseOSD, objects, "the OSD sent"},
		"more data than written":      {squidSparseClient, squidSparseOSD, []sparseObject{{objects[0].Object, 4096}, objects[1], objects[2], objects[3]}, "answered with result 0"},
		"an object that should exist": {squidSparseClient, squidSparseOSD, []sparseObject{objects[0], objects[1], {objects[2].Object, 4096}, objects[3]}, "answered with result -2"},
	} {
		_, problems := judgeSparseReads(parseSparseReads(c.client), parseSparseReads(c.osd), c.objects)
		if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, c.want) }) {
			t.Errorf("%s: problems %v, want %q", name, problems, c.want)
		}
	}
}
