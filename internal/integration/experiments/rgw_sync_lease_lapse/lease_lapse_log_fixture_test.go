//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_sync_lease_lapse_test

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// The lines are as a Tentacle 20.2.4 gateway logged them with debug_rgw_sync
// at 20. Shard 82 found its lease lost inside the batch it had read and went
// on from the source's newest marker without starting its entry; shard 104
// lost its lease before a read, ended, took the lease again and synced its
// entry; shard 7 was idle.
const leaseLapseLog = `2026-11-10T04:16:16.396+0000 7f337ccd0640 20 RGW-SYNC:data:sync:shard[82]: read error repo, got 0 entries
2026-11-10T04:16:16.396+0000 7f337ccd0640 20 RGW-SYNC:data:sync:shard[82]: shard_id=82 sync_marker=
2026-11-10T04:16:16.397+0000 7f337ccd0640 20 RGW-SYNC:data:sync:shard[7]: shard_id=7 sync_marker=
2026-11-10T04:16:16.440+0000 7f337ccd0640 20 RGW-SYNC:data:sync:shard[7]: shard_id=7 sync_marker= next_marker= truncated=0
2026-11-10T04:17:06.875+0000 7f337ccd0640  1 RGW-SYNC:data:sync:shard[104]: lease is lost, abort
2026-11-10T04:17:06.912+0000 7f337ccd0640 10 RGW-SYNC:data:sync:shard[104]: incremental sync failed (retcode=-125)
2026-11-10T04:17:06.913+0000 7f337ccd0640  1 RGW-SYNC:data:sync:shard[82]: lease is lost, abort
2026-11-10T04:17:06.913+0000 7f337ccd0640 20 RGW-SYNC:data:sync:shard[82]: shard_id=82 sync_marker= next_marker= truncated=0
2026-11-10T04:17:06.914+0000 7f337ccd0640  1 RGW-SYNC:meta:shard[7]: lease is lost, abort
2026-11-10T04:17:16.396+0000 7f337ccd0640 20 RGW-SYNC:data:sync:shard[82]: shard_id=82 sync_marker=00000000000000000000:00000000000000000512
2026-11-10T04:17:16.438+0000 7f337ccd0640 20 RGW-SYNC:data:sync:shard[82]: shard_id=82 sync_marker=00000000000000000000:00000000000000000512 next_marker= truncated=0
2026-11-10T04:17:22.959+0000 7f337ccd0640 10 RGW-SYNC:data:sync:shard[104]: took lease
2026-11-10T04:17:22.960+0000 7f337ccd0640 20 RGW-SYNC:data:sync:shard[104]: shard_id=104 sync_marker=
2026-11-10T04:17:22.961+0000 7f337ccd0640  1 RGW-SYNC:data:sync:shard[104]: incremental sync on tc-lease-three:cfb607dc-2ce0-490c-b69b-229425f1b36d.4199.3:0shard: 104on gen 0
2026-11-10T04:17:22.961+0000 7f337ccd0640 20 RGW-SYNC:data:sync:shard[104]: shard_id=104 log_entry: 00000000000000000000:00000000000000000656:2026-11-10T04:16:03.753565+0000:tc-lease-three:cfb607dc-2ce0-490c-b69b-229425f1b36d.4199.3:0
2026-11-10T04:17:22.962+0000 7f337ccd0640 20 RGW-SYNC:data:sync:shard[104]:entry[tc-lease-three:cfb607dc-2ce0-490c-b69b-229425f1b36d.4199.3:0[0]]: sync finished
2026-11-10T04:17:23.050+0000 7f337ccd0640 20 RGW-SYNC:data:sync:shard[104]: shard_id=104 sync_marker= next_marker= truncated=0
2026-11-10T04:17:43.050+0000 7f337ccd0640 20 RGW-SYNC:data:sync:shard[104]: shard_id=104 sync_marker=00000000000000000000:00000000000000000656
`

func leaseLapseTime(t *testing.T, clock string) time.Time {
	t.Helper()
	at, err := time.Parse("2006-01-02T15:04:05.000-0700", "2026-11-10T"+clock+"+0000")
	if err != nil {
		t.Fatal(err)
	}
	return at
}

func TestShardTracesTellALeaseLostInABatchFromOneLostBeforeARead(t *testing.T) {
	traces := readShardTraces(leaseLapseLog, leaseLapseTime(t, "04:16:00.000"))
	if len(traces) != 3 {
		t.Fatalf("shards %v", traces)
	}
	const newest82, newest104 = "00000000000000000000:00000000000000000512", "00000000000000000000:00000000000000000656"
	skipped, synced, idle := traces[82], traces[104], traces[7]
	if skipped.LostInBatch != 1 || skipped.LostAtStart != 0 || skipped.LostOther != 0 || len(skipped.Entries) != 0 || skipped.Marker != newest82 || len(skipped.Reads) != 2 {
		t.Fatalf("shard 82: %+v", skipped)
	}
	if synced.LostInBatch != 0 || synced.LostAtStart != 1 || synced.LostOther != 0 || !slices.Equal(synced.Entries, []string{newest104}) || synced.Marker != newest104 {
		t.Fatalf("shard 104: %+v", synced)
	}
	// The metadata sync shard of the same number is another log.
	if idle.LostInBatch+idle.LostAtStart+idle.LostOther != 0 || len(idle.Reads) != 1 {
		t.Fatalf("shard 7: %+v", idle)
	}
	// Shard 82's read was held from before the freeze until after it; shard
	// 7's ended before.
	from, to := leaseLapseTime(t, "04:16:28.000"), leaseLapseTime(t, "04:17:06.000")
	if !skipped.held(from, to) || idle.held(from, to) || synced.held(from, to) || (*shardTrace)(nil).held(from, to) {
		t.Fatalf("held: 82 %v, 7 %v, 104 %v", skipped.held(from, to), idle.held(from, to), synced.held(from, to))
	}

	// The signature needs every part: the lease lost inside a batch, the
	// read position at the source's newest marker, the entry never started
	// and the stored marker behind.
	if !skippedBatch(skipped, newest82, "") {
		t.Fatal("shard 82 does not show the skipped batch")
	}
	for name, got := range map[string]bool{
		"stored marker caught up":       skippedBatch(skipped, newest82, newest82),
		"source without a marker":       skippedBatch(skipped, "", ""),
		"read position behind":          skippedBatch(skipped, "00000000000000000000:00000000000000000600", ""),
		"lease lost before a read":      skippedBatch(synced, newest104, ""),
		"no lines of the shard":         skippedBatch(nil, newest82, ""),
		"entry of the marker started":   skippedBatch(&shardTrace{LostInBatch: 1, Marker: newest82, Entries: []string{newest82}}, newest82, ""),
		"lease never lost in the batch": skippedBatch(&shardTrace{Marker: newest82}, newest82, ""),
	} {
		if got {
			t.Errorf("%s: reported as a skipped batch", name)
		}
	}
}

func TestShardTracesStartAtTheGivenTime(t *testing.T) {
	traces := readShardTraces(leaseLapseLog, leaseLapseTime(t, "04:17:10.000"))
	if got := traces[82]; got == nil || got.LostInBatch != 0 || len(got.Reads) != 1 || traces[7] != nil {
		t.Fatalf("shards %+v, 82 %+v", traces, traces[82])
	}
	// A lost lease that nothing follows, and two in a row, are counted apart.
	open := "2026-11-10T04:17:06.913+0000 7f337ccd0640  1 RGW-SYNC:data:sync:shard[5]: lease is lost, abort\n"
	if got := readShardTraces(strings.Repeat(open, 2), time.Time{})[5]; got.LostOther != 2 || got.LostInBatch != 0 || got.LostAtStart != 0 {
		t.Fatalf("open losses %+v", got)
	}
	// A frame header before the time does not hide a line.
	if got := readShardTraces("\x02\x00\x00\x00\x00\x00\x00\x62"+open, time.Time{})[5]; got == nil || got.LostOther != 1 {
		t.Fatalf("framed line %+v", got)
	}
}

func TestShardLinesKeepTheLastLinesOfOneShard(t *testing.T) {
	lines := shardLines(leaseLapseLog, 104, 3)
	if len(lines) != 3 || !strings.Contains(lines[0], "sync finished") || !strings.HasSuffix(lines[2], "shard_id=104 sync_marker=00000000000000000000:00000000000000000656") {
		t.Fatalf("lines %q", lines)
	}
	// Shard 7's data sync line, not the metadata sync shard of that number.
	if lines := shardLines(leaseLapseLog, 7, 10); len(lines) != 2 || strings.Contains(strings.Join(lines, ""), "meta") {
		t.Fatalf("shard 7 lines %q", lines)
	}
}
