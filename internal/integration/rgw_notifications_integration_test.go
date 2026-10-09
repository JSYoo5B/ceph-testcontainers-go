//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_notifications))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// RGW pushes bucket notifications to an HTTP endpoint that the test owns. The
// receiver runs in the WithClient container so it shares the cluster network,
// or the host network in host mode.
func TestRGWBucketNotifications(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			testRGWBucketNotifications(t, host)
		})
	}
}

func testRGWBucketNotifications(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	cluster, client := newServiceCluster(t, opts...)
	gateway, err := cluster.StartRGWWithConfig(ctx, ceph.RGWConfig{SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	user, err := gateway.CreateUser(ctx, ceph.RGWUserConfig{ID: "tc-notify"})
	if err != nil {
		t.Fatal(err)
	}
	access, secret, err := user.Credentials()
	if err != nil {
		t.Fatal(err)
	}
	s3 := s3HTTPClient{endpoint: endpoint, accessKey: access, secretKey: secret, region: gateway.Region, http: &http.Client{Timeout: 30 * time.Second}}

	receiverHost := cluster.PublicAddress()
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
	s3FeatureRequest(t, ctx, s3, http.MethodPut, bucket, nil, nil, http.StatusOK)
	topics := map[string]string{}
	for _, topic := range []struct {
		name       string
		attributes [][2]string
	}{
		{"tc-direct", [][2]string{{"push-endpoint", pushEndpoint}}},
		{"tc-persistent", [][2]string{{"push-endpoint", pushEndpoint}, {"persistent", "true"}, {"retry_sleep_duration", "1"}}},
	} {
		topics[topic.name] = createNotificationTopic(t, ctx, s3, topic.name, topic.attributes)
	}
	configuration := `<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		notificationRule("direct", topics["tc-direct"], "in/") + notificationRule("queued", topics["tc-persistent"], "queued/") +
		`</NotificationConfiguration>`
	s3FeatureRequest(t, ctx, s3, http.MethodPut, bucket+"?notification", []byte(configuration), nil, http.StatusOK)
	stored, _ := s3FeatureRequest(t, ctx, s3, http.MethodGet, bucket+"?notification", nil, nil, http.StatusOK)
	for _, id := range []string{"direct", "queued"} {
		if !strings.Contains(string(stored), "<Id>"+id+"</Id>") {
			t.Fatalf("native notification configuration lost %s: %s", id, stored)
		}
	}

	t.Run("direct", func(t *testing.T) {
		s3FeatureRequest(t, ctx, s3, http.MethodPut, bucket+"/in/a", []byte("notified"), nil, http.StatusOK)
		s3FeatureRequest(t, ctx, s3, http.MethodPut, bucket+"/out/b", []byte("filtered"), nil, http.StatusOK)
		s3FeatureRequest(t, ctx, s3, http.MethodDelete, bucket+"/in/a", nil, nil, http.StatusNoContent)
		events := waitNotificationEvents(t, ctx, client, []string{"ObjectCreated:Put in/a", "ObjectRemoved:Delete in/a"})
		for _, event := range events {
			if strings.HasSuffix(event, " out/b") {
				t.Fatal("object outside the prefix filter was notified")
			}
		}
	})

	t.Run("persistent", func(t *testing.T) {
		stopNotificationReceiver(t, ctx, client)
		started := time.Now()
		s3FeatureRequest(t, ctx, s3, http.MethodPut, bucket+"/queued/c", []byte("queued while down"), nil, http.StatusOK)
		if elapsed := time.Since(started); elapsed > 10*time.Second {
			t.Fatalf("persistent notification blocked the S3 write for %s", elapsed)
		}
		time.Sleep(3 * time.Second)
		if events := readNotificationEvents(t, ctx, client); slices.Contains(events, "ObjectCreated:Put queued/c") {
			t.Fatal("event was recorded while the receiver was stopped")
		}
		startNotificationReceiver(t, ctx, client, port)
		waitNotificationEvents(t, ctx, client, []string{"ObjectCreated:Put queued/c"})
	})

	s3FeatureRequest(t, ctx, s3, http.MethodDelete, bucket+"?notification", nil, nil, http.StatusOK)
	if stored, _ := s3FeatureRequest(t, ctx, s3, http.MethodGet, bucket+"?notification", nil, nil, http.StatusOK); strings.Contains(string(stored), "<TopicConfiguration>") {
		t.Fatalf("notification configuration remained: %s", stored)
	}
	for name, arn := range topics {
		snsRequest(t, ctx, s3, url.Values{"Action": {"DeleteTopic"}, "TopicArn": {arn}})
		t.Logf("RGW_NOTIFY deleted topic %s", name)
	}
	s3FeatureRequest(t, ctx, s3, http.MethodDelete, bucket+"/out/b", nil, nil, http.StatusNoContent)
	s3FeatureRequest(t, ctx, s3, http.MethodDelete, bucket+"/queued/c", nil, nil, http.StatusNoContent)
	s3FeatureRequest(t, ctx, s3, http.MethodDelete, bucket, nil, nil, http.StatusNoContent)
}

func notificationRule(id, topic, prefix string) string {
	return `<TopicConfiguration><Id>` + id + `</Id><Topic>` + topic + `</Topic>` +
		`<Event>s3:ObjectCreated:*</Event><Event>s3:ObjectRemoved:*</Event>` +
		`<Filter><S3Key><FilterRule><Name>prefix</Name><Value>` + prefix + `</Value></FilterRule></S3Key></Filter></TopicConfiguration>`
}

func createNotificationTopic(t *testing.T, ctx context.Context, client s3HTTPClient, name string, attributes [][2]string) string {
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

func snsRequest(t *testing.T, ctx context.Context, client s3HTTPClient, form url.Values) []byte {
	t.Helper()
	headers := http.Header{"Content-Type": {"application/x-www-form-urlencoded"}}
	body, _ := s3FeatureRequest(t, ctx, client, http.MethodPost, "/", []byte(form.Encode()), headers, http.StatusOK)
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
	execOutput(t, ctx, client, "sh", "-c", `rm -f /tmp/tc-receiver.port; nohup python3 /tmp/tc-receiver.py "$0" >/tmp/tc-receiver.out 2>&1 & echo $! > /tmp/tc-receiver.pid`, strconv.Itoa(port))
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if value, err := strconv.Atoi(execOutput(t, ctx, client, "sh", "-c", "cat /tmp/tc-receiver.port 2>/dev/null || true")); err == nil && value > 0 {
			return value
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("notification receiver did not start", execOutput(t, ctx, client, "sh", "-c", "cat /tmp/tc-receiver.out || true"))
	return 0
}

func stopNotificationReceiver(t *testing.T, ctx context.Context, client testcontainers.Container) {
	t.Helper()
	// The client's PID 1 does not reap children, so a stopped receiver can
	// remain a zombie; it no longer holds its socket.
	execOutput(t, ctx, client, "sh", "-c", `pid=$(cat /tmp/tc-receiver.pid); kill "$pid"; while [ -e /proc/$pid ] && [ "$(cut -d' ' -f3 /proc/$pid/stat)" != Z ]; do sleep 0.1; done`)
}

func readNotificationEvents(t *testing.T, ctx context.Context, client testcontainers.Container) []string {
	t.Helper()
	output := execOutput(t, ctx, client, "sh", "-c", "cat /tmp/tc-events.log 2>/dev/null || true")
	if output == "" {
		return nil
	}
	return strings.Split(output, "\n")
}

func waitNotificationEvents(t *testing.T, ctx context.Context, client testcontainers.Container, want []string) []string {
	t.Helper()
	deadline, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		events := readNotificationEvents(t, ctx, client)
		missing := slices.DeleteFunc(slices.Clone(want), func(event string) bool { return slices.Contains(events, event) })
		if len(missing) == 0 {
			data, _ := json.Marshal(events)
			t.Logf("RGW_NOTIFY events=%s", data)
			return events
		}
		select {
		case <-deadline.Done():
			t.Fatalf("notification events %v not received; got %v", missing, events)
		case <-time.After(time.Second):
		}
	}
}
