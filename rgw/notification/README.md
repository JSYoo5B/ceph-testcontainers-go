# RGW notification queues

Import `github.com/JSYoo5B/ceph-testcontainers-go/rgw/notification` when a
test of an application that consumes S3 bucket notifications must tell a
notification still pending inside RGW apart from one that the consumer lost or
failed to process. `notification.Queue` reads the persistent queue of one
topic on an existing gateway: the topic's native identity and delivery
limits, the queue statistics and, on request, a bounded sample of the pending
entries. Topics, notification configurations and receivers belong to the SUT
and the test, which create them with their own S3/SNS client; the package
starts no container and adds no client or receiver. See
[API purpose and purpose tags](../../docs/guides/api-purpose.md).

## Order of use

1. **Start a gateway** with `rgw.Run`, `rgw.Start` or `rgw.WithGateways`, and
   hand the SUT its endpoint and a user (see [rgw](../README.md)).
2. **Let the SUT or the test create** the persistent topic
   (`persistent=true`) and the bucket notification configuration, and run the
   endpoint the topic pushes to.
3. **Observe** with `notification.Queue(ctx, gateway, topic, sample)`:
   `notification.Topic{Tenant, Name}` selects the topic, and
   `notification.Sample{MaxEntries, MaxBytes}` asks for the first
   `MaxEntries` pending entries in queue order; the zero `Sample` reads only
   the statistics.
4. **Correlate** the snapshot with what the consumer received, by the object
   keys and event names the test wrote. Poll: delivery is asynchronous.
5. **Clean up**: the topic stays the SUT's. Delete it with the client that
   created it, or terminate the cluster; this package removes nothing.

```golang
topic := notification.Topic{Name: "orders"}
pending := func(s notification.QueueSnapshot) bool {
	return s.Stats.Entries > 0 && slices.ContainsFunc(s.Pending, func(e notification.PendingEntry) bool {
		return e.Key == "orders/42" && e.EventName == "ObjectCreated:Put"
	})
}
// The consumer is down: after the SUT's write the event becomes pending in
// RGW, but not necessarily before the first read.
waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
defer cancel()
for {
	snapshot, err := notification.Queue(waitCtx, gateway, topic, notification.Sample{MaxEntries: 10})
	if err == nil && pending(snapshot) {
		break
	}
	select {
	case <-waitCtx.Done():
		t.Fatalf("event is not pending in RGW: %+v, last error %v", snapshot, err)
	case <-time.After(time.Second):
	}
}
// Start the consumer, wait until it reports orders/42, then poll Queue the
// same way until the entry has left the queue.
```

## Concepts and limits

- A snapshot is point-in-time RGW evidence, not a delivery verdict. An empty
  queue does not prove that the consumer received or processed anything, and
  a shrinking queue can mean that entries expired or ran out of retries
  (`Metadata.TimeToLive`, `MaxRetries`); the consumer's own record of what it
  received decides exactly-once or end-to-end delivery.
- `Stats.Entries` counts committed entries, `Stats.Reservations` slots of
  notifications not committed yet, and `Stats.SizeBytes` is the native queue
  size. Statistics and sample are separate native reads, not one atomic view.
  The queue is the one of the gateway's zone.
- Identity: the selection is a tenant and a name, and the topic's ARN must
  name both. `Queue` reads the topic before and after the queue and refuses a
  change of owner, ARN or queue name. Native topic metadata has no generation,
  so a topic deleted and recreated with the same name, owner and queue between
  two calls, or between the reads of one call, is not detected. Account-owned
  topics are not selected by a tenant; use `Gateway.Admin` for them.
- Invalid selection or sample arguments match `ceph.ErrInvalidRequest` before
  any command runs. If the topic read shows that the topic has no persistent
  queue, the error matches both `notification.ErrNotPersistent` and
  `ceph.ErrInvalidRequest` via `errors.Is`.
- A missing topic matches `fs.ErrNotExist` and a refused read matches
  `fs.ErrPermission`. A cluster before Ceph 19 matches `ceph.ErrUnsupported`.
  An unexpected native reply produces an unclassified error. Check the error
  before interpreting zero entries as an empty queue.
- Bounds: at most `MaxSampleEntries` entries. `topic dump` writes the whole
  queue (its `--max-entries` only sizes its pages), so `Queue` keeps the first
  `MaxBytes` bytes of the reply (default `DefaultSampleBytes`), discards the
  rest as it arrives and decodes the sample from that prefix. A sample whose
  entries do not fit is refused, not shortened. The command still reads the
  whole queue; the call is bounded by its context and the cluster's startup
  timeout.
- Redaction: `PendingEntry` keeps the event ID, name and time, the
  configuration ID, bucket, key, version, sequencer and entry creation time.
  Push endpoints and their arguments, user and request identities, source
  addresses, object metadata, tags and opaque data are never returned, and no
  error contains native output.
- `Queue` only reads (`topic get`, `topic stats`, `topic dump`): it never
  changes, purges or removes a topic, its policy or its queue, and the result
  is plain data, so observing a SUT-created topic adopts nothing.

## Reference

- API: [pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/rgw/notification)
  or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/rgw/notification`.
- [Native commands](../../docs/guides/native-commands.md#rgw-administration),
  [Errors](../../docs/guides/errors.md).
