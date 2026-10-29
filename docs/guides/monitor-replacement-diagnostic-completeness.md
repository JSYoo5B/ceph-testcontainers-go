# Reading incomplete MON replacement evidence

The MON replacement recorder keeps diagnostic evidence after an operation
fails. Its failure bundle uses a fresh bounded context even if the operation
context expired. Improving that bundle does not classify the intermittent
warm-daemon session loss or joining-MON timeout. Those two causes remain
unclassified without an actual failing normal-replacement capture.

## Bounded traces

Each selected container trace retains at most 300 matching lines and 512
bytes per retained line. A long line keeps a UTF-8-safe prefix and tail
with ` [...omitted...] ` between them. The authentication result at the end
of a long line can remain visible, while the omission stays explicit.

Secret patterns are removed before clipping. Private-key block redaction
also spans multiple lines, including intervening lines that do not match the
trace selector. Invalid UTF-8 is repaired and counted. This is the existing
diagnostics pattern policy, not a claim that every possible application
secret syntax is recognized.

The receipt keeps these distinct observations:

- `matched` and `matched_bytes`: matching lines visited and their original
  byte lengths, before repair/redaction. They are not whole-stream totals
  when collection aborts.
- `line_bytes`: original, redacted and captured byte lengths for each line
  in `lines`; `retained_bytes` sums the captured strings, including markers.
- `clipped_lines`: all visited matching lines clipped, including lines later
  dropped by the rolling window; `dropped_lines` counts the latter separately.
- `invalid_utf8_lines`: matching lines whose original UTF-8 needed repair.
- `truncated`: clipping, rolling loss, invalid UTF-8 or a stream abort occurred.
- `stream_complete`: a clean stream end was observed inside the input/time
  limits; it does not cancel any retained-line clipping or rolling loss.
- `stream_aborted` and `error`: the stream did not reach an observed clean end.

Input has a separate 8 MiB budget and retains the existing 1 MiB scanner
line limit. An oversized line aborts with explicit partial evidence instead
of being silently skipped. Reaching the input cap also aborts; the collector
does not claim to have read the newest lines beyond it. On query expiry the
owned log stream is closed to release a blocked read. No log-reading worker
continues in the background after the close callback has returned.

An absent authentication refusal in a clipped, dropped or aborted trace is
not negative evidence. Even a complete selected trace does not establish
that every relevant event was logged at the selected debug level.

## Debug-window restoration ownership

A window records the exact prior `debug_monc`, requested level and container
start observation. An attempted apply remains recorded even when its result
is uncertain. Before restoring, the recorder requires a running owned daemon
of the recorded generation and reads its current value under a query bound.

If current equals prior, restoration is an explicit no-op. If current equals
the requested level, the recorder writes prior once and reads it back.
Any other value is a conflict: it is retained as `current`, preserved without
a write and reported as incomplete cleanup. An unreadable current value is
also incomplete and never admits a restore write.

This is a read-before-write guard, not atomic compare-and-swap. A writer that
sets the same requested value cannot be distinguished from this recorder;
a writer or restart racing after observation is not excluded by two reads.
The evidence must not be described as exclusive ownership of an uninterrupted
process or protection against concurrent racing configuration changes.

`restore_attempted` and `restore_action` distinguish a mutation from an
already-restored no-op. The existing `restored` result keeps conflicts,
failed writes, mismatched readback, missing identity and unavailable sockets
explicit. Finish reporting counts only confirmed prior values as restored.
A failed or uncertain decision is not automatically replayed by later cleanup.

`apply_stop_unconfirmed` and `restore_stop_unconfirmed` preserve the
supervisor's typed inability to confirm command termination. An apply whose
stop is unconfirmed can still write after a current-value observation, so it
admits no restore mutation and no successful restoration claim, even if the
prior value is visible. It remains incomplete until the owned fixture's
container cleanup; the recorder does not retry the configuration mutation or
claim that the outstanding command was stopped. A stopped command with a lost
acknowledgement is separate: its possible apply is handled by the value guard,
with the original error retained.

## Validation scope

The maintained recorder and fake producer are exercised directly by
`TestMonReplacementTrace*` and `TestMonReplacementDebug*`. Controls cover a
long authentication-result tail, rolling/output/input limits, oversized and
blocked streams, invalid UTF-8, redaction, outside edits, an already-restored
value, unavailable/paused reads, failed restore/readback and unconfirmed stops.
Existing expired-operation, partial joining descriptor, changed-generation and
held-topology controls remain in the same fixture suite.

```sh
CGO_ENABLED=0 go test -mod=readonly -tags=integration \
  -run '^Test(MonReplacement|MonStatus)' -count=1 -timeout=3m \
  ./internal/integration
```

These are Docker-free injected boundary tests. They do not fabricate a real
Ceph session loss, authentication refusal or join failure, and they do not
justify a membership retry or a larger native timeout. The raw `mon_status`
identity/payload retention remains a separate existing observation.
