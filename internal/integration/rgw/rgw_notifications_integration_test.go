//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_notifications))))

//ci: timeout=20m job-timeout=30 representative=flaky image-roles=essential,client,rgw

package rgw_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/notification"
	"github.com/testcontainers/testcontainers-go"
)

// RGW pushes bucket notifications to an HTTP endpoint that the test owns. The
// receiver runs in the WithClient container so it shares the cluster network,
// or the host network in host mode. While the receiver is down, the persistent
// topic's queue, read with notification.Queue, holds the known events, and the
// events leave it once the receiver has logged them.
func TestRGWBucketNotifications(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "bridge", "host") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) {
			testRGWBucketNotifications(t, host)
		})
	}
}

func testRGWBucketNotifications(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	cluster, client := harness.NewServiceCluster(t, opts...)
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "tc-notify"})
	if err != nil {
		t.Fatal(err)
	}
	access, secret, err := user.CreationKey().Credentials()
	if err != nil {
		t.Fatal(err)
	}
	s3 := harness.S3HTTPClient{Endpoint: endpoint, AccessKey: access, SecretKey: secret, Region: gateway.Region, HTTP: &http.Client{Timeout: 30 * time.Second}}

	receiverHost := harness.StaticFacts(cluster).PublicAddress
	if !host {
		receiverHost, err = client.ContainerIP(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	port := startNotificationReceiver(t, ctx, client, 0)
	pushEndpoint := "http://" + receiverHost + ":" + strconv.Itoa(port)
	t.Logf("RGW_NOTIFY receiver=%s", pushEndpoint)

	const bucket = "/tc-notify"
	harness.S3FeatureRequest(t, ctx, s3, http.MethodPut, bucket, nil, nil, http.StatusOK)
	topics := map[string]string{}
	for _, topic := range []struct {
		name       string
		attributes [][2]string
	}{
		{"tc-direct", [][2]string{{"push-endpoint", pushEndpoint}}},
		{"tc-persistent", [][2]string{{"push-endpoint", pushEndpoint}, {"persistent", "true"}, {"retry_sleep_duration", "1"}}},
		// No rule publishes to tc-unrelated: its queue must stay apart from
		// tc-persistent's.
		{"tc-unrelated", [][2]string{{"push-endpoint", pushEndpoint}, {"persistent", "true"}, {"retry_sleep_duration", "1"}}},
	} {
		topics[topic.name] = createNotificationTopic(t, ctx, s3, topic.name, topic.attributes)
	}
	configuration := `<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		notificationRule("direct", topics["tc-direct"], "in/") + notificationRule("queued", topics["tc-persistent"], "queued/") +
		`</NotificationConfiguration>`
	harness.S3FeatureRequest(t, ctx, s3, http.MethodPut, bucket+"?notification", []byte(configuration), nil, http.StatusOK)
	stored, _ := harness.S3FeatureRequest(t, ctx, s3, http.MethodGet, bucket+"?notification", nil, nil, http.StatusOK)
	for _, id := range []string{"direct", "queued"} {
		if !strings.Contains(string(stored), "<Id>"+id+"</Id>") {
			t.Fatalf("native notification configuration lost %s: %s", id, stored)
		}
	}

	t.Run("direct", func(t *testing.T) {
		harness.S3FeatureRequest(t, ctx, s3, http.MethodPut, bucket+"/in/a", []byte("notified"), nil, http.StatusOK)
		harness.S3FeatureRequest(t, ctx, s3, http.MethodPut, bucket+"/out/b", []byte("filtered"), nil, http.StatusOK)
		harness.S3FeatureRequest(t, ctx, s3, http.MethodDelete, bucket+"/in/a", nil, nil, http.StatusNoContent)
		events := waitNotificationEvents(t, ctx, client, []string{"ObjectCreated:Put in/a", "ObjectRemoved:Delete in/a"})
		for _, event := range events {
			if strings.HasSuffix(event, " out/b") {
				t.Fatal("object outside the prefix filter was notified")
			}
		}
	})

	t.Run("persistent", func(t *testing.T) {
		selected := notification.Topic{Name: "tc-persistent"}
		sample := notification.Sample{MaxEntries: 10}
		queued := []string{"queued/c", "queued/d"}
		received := []string{"ObjectCreated:Put queued/c", "ObjectCreated:Put queued/d"}
		native := map[string][]byte{}
		for _, name := range []string{"tc-persistent", "tc-unrelated", "tc-direct"} {
			native[name] = nativeTopic(t, ctx, gateway, name)
		}
		if empty := observeQueue(t, ctx, gateway, selected, sample); empty.Stats.Entries != 0 || !empty.Sampled || len(empty.Pending) != 0 || empty.Topic.ARN != topics["tc-persistent"] {
			t.Fatalf("persistent queue is not empty before its events: %+v", empty)
		}

		stopNotificationReceiver(t, ctx, client)
		started := time.Now()
		for _, key := range queued {
			harness.S3FeatureRequest(t, ctx, s3, http.MethodPut, bucket+"/"+key, []byte("queued while down"), nil, http.StatusOK)
		}
		if elapsed := time.Since(started); elapsed > 10*time.Second {
			t.Fatalf("persistent notification blocked the S3 write for %s", elapsed)
		}
		pending := waitQueue(t, ctx, gateway, selected, sample, func(s notification.QueueSnapshot) bool {
			return s.Stats.Entries >= uint64(len(queued)) && len(missingPending(s, queued)) == 0
		})
		for _, entry := range pending.Pending {
			if entry.EventName != "ObjectCreated:Put" || entry.Bucket != strings.TrimPrefix(bucket, "/") || entry.ConfigurationID != "queued" || !strings.HasPrefix(entry.Key, "queued/") {
				t.Fatalf("pending entry is not one of the selected rule's events: %+v", entry)
			}
		}
		if text := fmt.Sprintf("%+v", pending); strings.Contains(text, pushEndpoint) || strings.Contains(text, access) || strings.Contains(text, secret) {
			t.Fatal("queue snapshot exposes the push endpoint or S3 credentials")
		}
		if events, err := harness.ReadNotificationEvents(ctx, client); err != nil {
			t.Fatal(err)
		} else if len(harness.MissingNotificationEvents(received, events)) != len(received) {
			t.Fatalf("event was recorded while the receiver was stopped: %v", events)
		}
		// Observing neither drains nor delivers: the entries stay pending.
		again := observeQueue(t, ctx, gateway, selected, sample)
		if again.Stats.Entries < uint64(len(queued)) || len(missingPending(again, queued)) != 0 {
			t.Fatalf("queue observation changed the pending entries: %+v", again)
		}
		// topic dump writes every queued entry; the sample is the first one.
		if one := observeQueue(t, ctx, gateway, selected, notification.Sample{MaxEntries: 1}); len(one.Pending) != 1 || one.Pending[0] != again.Pending[0] || one.Stats.Entries < uint64(len(queued)) {
			t.Fatalf("sample is not the first entry of the queue: %+v", one)
		}
		if snapshot, err := notification.Queue(ctx, gateway, selected, notification.Sample{MaxEntries: len(queued), MaxBytes: 64}); err == nil || !strings.Contains(err.Error(), "topic dump reply reached the sample's byte limit") || snapshot.Sampled {
			t.Fatal("oversized sample did not produce the byte-limit refusal")
		} else if strings.Contains(err.Error(), pushEndpoint) {
			t.Fatal("refused sample error exposes the push endpoint")
		}
		t.Logf("RGW_NOTIFY pending stats=%+v sampled=%d", pending.Stats, len(pending.Pending))

		unrelated := observeQueue(t, ctx, gateway, notification.Topic{Name: "tc-unrelated"}, sample)
		if unrelated.Stats.Entries != 0 || len(unrelated.Pending) != 0 || unrelated.Topic.ARN != topics["tc-unrelated"] {
			t.Fatalf("unrelated topic shares the selected queue: %+v", unrelated)
		}
		// A refused or denied radosgw-admin read and a release without the
		// topic commands cannot be produced on the supported images; the unit
		// tests of rgw/notification cover them.
		for name, c := range map[string]struct {
			topic notification.Topic
			is    error
		}{
			"nonpersistent": {notification.Topic{Name: "tc-direct"}, notification.ErrNotPersistent},
			"missing":       {notification.Topic{Name: "tc-missing"}, fs.ErrNotExist},
			"other tenant":  {notification.Topic{Tenant: "tc_other", Name: "tc-persistent"}, fs.ErrNotExist},
			"invalid":       {notification.Topic{Name: "tc:persistent"}, ceph.ErrInvalidRequest},
		} {
			if snapshot, err := notification.Queue(ctx, gateway, c.topic, sample); !errors.Is(err, c.is) || snapshot.Sampled || snapshot.Stats != (notification.Stats{}) {
				t.Fatalf("%s topic observation: %+v %v", name, snapshot, err)
			} else if strings.Contains(err.Error(), pushEndpoint) {
				t.Fatalf("%s topic error exposes the push endpoint", name)
			}
		}
		canceled, cancelObservation := context.WithCancel(ctx)
		cancelObservation()
		if snapshot, err := notification.Queue(canceled, gateway, selected, sample); !errors.Is(err, context.Canceled) || snapshot.Sampled {
			t.Fatalf("canceled observation: %+v %v", snapshot, err)
		}
		for name, before := range native {
			if after := nativeTopic(t, ctx, gateway, name); !bytes.Equal(before, after) {
				t.Fatalf("queue observations changed topic %s", name)
			}
		}
		if after, _ := harness.S3FeatureRequest(t, ctx, s3, http.MethodGet, bucket+"?notification", nil, nil, http.StatusOK); !bytes.Equal(after, stored) {
			t.Fatal("queue observations changed the bucket notification configuration")
		}

		startNotificationReceiver(t, ctx, client, port)
		waitNotificationEvents(t, ctx, client, received)
		// The receiver has logged the events; only now must they leave the
		// queue. An empty queue alone would not prove the receipt.
		drained := waitQueue(t, ctx, gateway, selected, sample, func(s notification.QueueSnapshot) bool {
			return s.Stats.Entries == 0 && len(missingPending(s, queued)) == len(queued)
		})
		t.Logf("RGW_NOTIFY drained stats=%+v", drained.Stats)
	})

	harness.S3FeatureRequest(t, ctx, s3, http.MethodDelete, bucket+"?notification", nil, nil, http.StatusOK)
	if stored, _ := harness.S3FeatureRequest(t, ctx, s3, http.MethodGet, bucket+"?notification", nil, nil, http.StatusOK); strings.Contains(string(stored), "<TopicConfiguration>") {
		t.Fatalf("notification configuration remained: %s", stored)
	}
	for name, arn := range topics {
		snsRequest(t, ctx, s3, url.Values{"Action": {"DeleteTopic"}, "TopicArn": {arn}})
		t.Logf("RGW_NOTIFY deleted topic %s", name)
	}
	harness.S3FeatureRequest(t, ctx, s3, http.MethodDelete, bucket+"/out/b", nil, nil, http.StatusNoContent)
	harness.S3FeatureRequest(t, ctx, s3, http.MethodDelete, bucket+"/queued/c", nil, nil, http.StatusNoContent)
	harness.S3FeatureRequest(t, ctx, s3, http.MethodDelete, bucket+"/queued/d", nil, nil, http.StatusNoContent)
	harness.S3FeatureRequest(t, ctx, s3, http.MethodDelete, bucket, nil, nil, http.StatusNoContent)
}

// nativeTopic returns radosgw-admin topic get of name, which holds the push
// endpoint: compare it, never log it.
func nativeTopic(t *testing.T, ctx context.Context, gateway *rgw.Gateway, name string) []byte {
	t.Helper()
	data, err := gateway.Admin(ctx, "topic", "get", "--topic", name)
	if err != nil {
		t.Fatalf("native topic %s: %v", name, err)
	}
	return data
}

func observeQueue(t *testing.T, ctx context.Context, gateway *rgw.Gateway, topic notification.Topic, sample notification.Sample) notification.QueueSnapshot {
	t.Helper()
	snapshot, err := notification.Queue(ctx, gateway, topic, sample)
	if err != nil {
		t.Fatalf("queue of topic %s: %v", topic.Name, err)
	}
	return snapshot
}

// waitQueue polls the topic's queue for at most two minutes until ready
// accepts a snapshot, and returns that snapshot.
func waitQueue(t *testing.T, ctx context.Context, gateway *rgw.Gateway, topic notification.Topic, sample notification.Sample, ready func(notification.QueueSnapshot) bool) notification.QueueSnapshot {
	t.Helper()
	seen, err := harness.Observe(ctx, 2*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (notification.QueueSnapshot, bool, error) {
		snapshot, err := notification.Queue(probeCtx, gateway, topic, sample)
		return snapshot, err == nil && ready(snapshot), err
	})
	if err != nil {
		t.Fatalf("queue of topic %s not reached: %v; last %+v", topic.Name, err, seen.Value)
	}
	return seen.Value
}

// missingPending returns the keys of want for which the snapshot's sample has
// no ObjectCreated:Put entry.
func missingPending(snapshot notification.QueueSnapshot, want []string) []string {
	return slices.DeleteFunc(slices.Clone(want), func(key string) bool {
		return slices.ContainsFunc(snapshot.Pending, func(entry notification.PendingEntry) bool {
			return entry.Key == key && entry.EventName == "ObjectCreated:Put"
		})
	})
}

func notificationRule(id, topic, prefix string) string {
	return `<TopicConfiguration><Id>` + id + `</Id><Topic>` + topic + `</Topic>` +
		`<Event>s3:ObjectCreated:*</Event><Event>s3:ObjectRemoved:*</Event>` +
		`<Filter><S3Key><FilterRule><Name>prefix</Name><Value>` + prefix + `</Value></FilterRule></S3Key></Filter></TopicConfiguration>`
}

func createNotificationTopic(t *testing.T, ctx context.Context, client harness.S3HTTPClient, name string, attributes [][2]string) string {
	t.Helper()
	form := url.Values{"Action": {"CreateTopic"}, "Name": {name}}
	for i, attribute := range attributes {
		form.Set("Attributes.entry."+strconv.Itoa(i+1)+".key", attribute[0])
		form.Set("Attributes.entry."+strconv.Itoa(i+1)+".value", attribute[1])
	}
	body := snsRequest(t, ctx, client, form)
	var response struct {
		ARN string `xml:"CreateTopicResult>TopicArn"`
	}
	if err := xml.Unmarshal(body, &response); err != nil || !strings.HasSuffix(response.ARN, ":"+name) {
		t.Fatalf("native topic %s ARN unavailable: %s %v", name, body, err)
	}
	t.Logf("RGW_NOTIFY topic=%s arn=%s", name, response.ARN)
	return response.ARN
}

func snsRequest(t *testing.T, ctx context.Context, client harness.S3HTTPClient, form url.Values) []byte {
	t.Helper()
	headers := http.Header{"Content-Type": {"application/x-www-form-urlencoded"}}
	body, _ := harness.S3FeatureRequest(t, ctx, client, http.MethodPost, "/", []byte(form.Encode()), headers, http.StatusOK)
	return body
}

// The receiver appends "eventName key" lines for every pushed record. Port 0
// selects a free port; a fixed port restarts the receiver at the same URL.
const notificationReceiver = `import http.server,json,sys
class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body=self.rfile.read(int(self.headers.get('Content-Length','0')))
        with open('/tmp/tc-events.log','a') as events:
            for record in json.loads(body).get('Records',[]):
                events.write(record['eventName']+' '+record['s3']['object']['key']+'\n')
        self.send_response(200); self.end_headers()
    def log_message(self,*args): pass
server=http.server.HTTPServer(('0.0.0.0',int(sys.argv[1])),Handler)
open('/tmp/tc-receiver.port','w').write(str(server.server_address[1]))
server.serve_forever()
`

func startNotificationReceiver(t *testing.T, ctx context.Context, client testcontainers.Container, port int) int {
	t.Helper()
	if err := client.CopyToContainer(ctx, []byte(notificationReceiver), "/tmp/tc-receiver.py", 0o644); err != nil {
		t.Fatal(err)
	}
	harness.ExecOutput(t, ctx, client, "sh", "-c", `rm -f /tmp/tc-receiver.port; nohup python3 /tmp/tc-receiver.py "$0" >/tmp/tc-receiver.out 2>&1 & echo $! > /tmp/tc-receiver.pid`, strconv.Itoa(port))
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if value, err := strconv.Atoi(harness.ExecOutput(t, ctx, client, "sh", "-c", "cat /tmp/tc-receiver.port 2>/dev/null || true")); err == nil && value > 0 {
			return value
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("notification receiver did not start", harness.ExecOutput(t, ctx, client, "sh", "-c", "cat /tmp/tc-receiver.out || true"))
	return 0
}

func stopNotificationReceiver(t *testing.T, ctx context.Context, client testcontainers.Container) {
	t.Helper()
	// The client's PID 1 does not reap children, so a stopped receiver can
	// remain a zombie; it no longer holds its socket.
	harness.ExecOutput(t, ctx, client, "sh", "-c", `pid=$(cat /tmp/tc-receiver.pid); kill "$pid"; while [ -e /proc/$pid ] && [ "$(cut -d' ' -f3 /proc/$pid/stat)" != Z ]; do sleep 0.1; done`)
}

// waitNotificationEvents waits up to two minutes until the receiver in client
// has logged every event of want, and returns the logged events.
func waitNotificationEvents(t *testing.T, ctx context.Context, client testcontainers.Container, want []string) []string {
	t.Helper()
	seen, err := harness.ObserveNotificationEvents(ctx, client, want, 2*time.Minute)
	if err != nil {
		t.Fatalf("notification events %v not received: %v; last read %v", harness.MissingNotificationEvents(want, seen.Value), err, seen.Value)
	}
	data, _ := json.Marshal(seen.Value)
	t.Logf("RGW_NOTIFY events=%s after %d reads in %s", data, seen.Attempts, seen.Elapsed.Round(time.Millisecond))
	return seen.Value
}
