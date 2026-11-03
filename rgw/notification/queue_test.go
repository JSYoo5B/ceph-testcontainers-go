package notification

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// The canned replies carry PRIVATE markers where native output holds
// credentials, endpoint arguments and metadata that no result or error may
// contain.
func topicDocument(tenant, name, queue string, persistent bool) string {
	owner := "tc-notify"
	if tenant != "" {
		owner = tenant + "$" + owner
	}
	return fmt.Sprintf(`{"owner":%q,"name":%q,"dest":{"push_endpoint":"amqp://PRIVATE-USER:PRIVATE-PASSWORD@receiver:5672","push_endpoint_args":"PRIVATE-ARGS","push_endpoint_topic":%q,"stored_secret":true,"persistent":%t,"persistent_queue":%q,"time_to_live":"None","max_retries":"5","retry_sleep_duration":"1"},"arn":"arn:aws:sns:default:%s:%s","opaqueData":"PRIVATE-OPAQUE","policy":"PRIVATE-POLICY"}`,
		owner, name, name, persistent, queue, tenant, name)
}

const (
	statsDocument = `{"Topic Stats":{"Reservations":1,"Size":2048,"Entries":2}}`
	pendingEntry  = `{"event":{"eventVersion":"2.2","eventSource":"ceph:s3","awsRegion":"default","eventTime":"2026-11-03T01:02:03.000000Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"PRIVATE-PRINCIPAL"},"requestParameters":{"sourceIPAddress":"PRIVATE-ADDRESS"},"responseElements":{"x-amz-request-id":"PRIVATE-REQUEST","x-amz-id-2":"PRIVATE-ID"},"s3":{"s3SchemaVersion":"1.0","configurationId":"queued","bucket":{"name":"tc-notify","ownerIdentity":{"principalId":"PRIVATE-OWNER"},"arn":"arn:aws:s3:default::tc-notify","id":"bucket-id"},"object":{"key":"queued/%s","size":17,"eTag":"etag","versionId":"","sequencer":"seq-%s","metadata":[{"key":"x-amz-meta-secret","val":"PRIVATE-METADATA"}],"tags":[{"key":"PRIVATE-TAG","val":"v"}]}},"eventId":"event-%s","opaqueData":"PRIVATE-OPAQUE"},"pushEndpoint":"amqp://PRIVATE-USER:PRIVATE-PASSWORD@receiver:5672","pushEndpointArgs":"PRIVATE-ARGS","arnTopic":"tc-persistent","creationTime":"2026-11-03T01:02:03.000000Z","TTL":0,"maxRetries":0,"retrySleepDuration":0}`
)

func entry(id string) string { return fmt.Sprintf(pendingEntry, id, id, id) }

type reply struct {
	code int
	data string
}

// topicContainer answers radosgw-admin topic get, stats and dump with the
// queued replies of each subcommand, in order, and records every command
// line. Any other command fails the call.
type topicContainer struct {
	testcontainers.Container
	calls   [][]string
	replies map[string][]reply
	onExec  func(args []string)
}

func (f *topicContainer) State(context.Context) (*container.State, error) {
	return &container.State{Running: true}, nil
}

func (f *topicContainer) GetContainerID() string { return "gateway" }

func (f *topicContainer) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if gwtest.IsCancel(args) {
		return 0, nil, nil
	}
	original := args
	args = gwtest.Argv(args)
	f.calls = append(f.calls, slices.Clone(args))
	if f.onExec != nil {
		f.onExec(args)
	}
	index := slices.Index(args, "topic")
	if args[0] != "radosgw-admin" || index < 0 || index+1 >= len(args) || len(f.replies[args[index+1]]) == 0 {
		return 1, nil, fmt.Errorf("unexpected command %v", args)
	}
	sub := args[index+1]
	answer := f.replies[sub][0]
	f.replies[sub] = f.replies[sub][1:]
	return gwtest.Reply(original, answer.code, gwtest.Stream(answer.code != 0, []byte(answer.data+"\n")))
}

func newFixture(replies map[string][]reply) (*gwtest.Gateway, *topicContainer) {
	f := &topicContainer{replies: replies}
	return gwtest.NewGateway(f, f), f
}

func persistentReplies(tenant string, dump ...string) map[string][]reply {
	doc := topicDocument(tenant, "tc-persistent", "tc-persistent", true)
	replies := map[string][]reply{"get": {{data: doc}, {data: doc}}, "stats": {{data: statsDocument}}}
	if dump != nil {
		replies["dump"] = []reply{{data: "[" + strings.Join(dump, ",") + "]"}}
	}
	return replies
}

func subcommands(calls [][]string) []string {
	var subs []string
	for _, args := range calls {
		if i := slices.Index(args, "topic"); i >= 0 && i+1 < len(args) {
			subs = append(subs, args[i+1])
		}
	}
	return subs
}

func assertNoPrivate(t *testing.T, values ...any) {
	t.Helper()
	for _, value := range values {
		for _, text := range []string{fmt.Sprintf("%#v", value), fmt.Sprintf("%+v", value), fmt.Sprint(value)} {
			if strings.Contains(text, "PRIVATE") {
				t.Fatalf("result or error exposes native secrets or metadata: %s", text)
			}
		}
	}
}

func TestQueueReportsIdentityStatsAndBoundedSample(t *testing.T) {
	for _, tenant := range []string{"", "team_a"} {
		t.Run("tenant="+tenant, func(t *testing.T) {
			g, f := newFixture(persistentReplies(tenant, entry("c"), entry("d")))
			snapshot, err := queue(t.Context(), g.Core, Topic{Tenant: tenant, Name: "tc-persistent"}, Sample{MaxEntries: 5})
			if err != nil {
				t.Fatal(err)
			}
			owner := "tc-notify"
			if tenant != "" {
				owner = tenant + "$tc-notify"
			}
			want := Metadata{Tenant: tenant, Name: "tc-persistent", Owner: owner, ARN: "arn:aws:sns:default:" + tenant + ":tc-persistent",
				Queue: "tc-persistent", TimeToLive: "None", MaxRetries: "5", RetrySleepDuration: "1"}
			if snapshot.Topic != want || snapshot.Stats != (Stats{Reservations: 1, Entries: 2, SizeBytes: 2048}) || !snapshot.Sampled {
				t.Fatalf("unexpected snapshot %+v", snapshot)
			}
			wantEntry := PendingEntry{EventID: "event-c", EventName: "ObjectCreated:Put", EventTime: "2026-11-03T01:02:03.000000Z", ConfigurationID: "queued",
				Bucket: "tc-notify", Key: "queued/c", Sequencer: "seq-c", CreationTime: "2026-11-03T01:02:03.000000Z"}
			if len(snapshot.Pending) != 2 || snapshot.Pending[0] != wantEntry || snapshot.Pending[1].Key != "queued/d" || snapshot.Pending[1].EventID != "event-d" {
				t.Fatalf("unexpected pending entries %+v", snapshot.Pending)
			}
			assertNoPrivate(t, snapshot)
			if got := subcommands(f.calls); !slices.Equal(got, []string{"get", "stats", "dump", "get"}) {
				t.Fatalf("queue observation ran %v", got)
			}
			for _, args := range f.calls {
				if !slices.Contains(args, "--format") || !slices.Contains(args, "tc-persistent") || slices.Contains(args, "--tenant") != (tenant != "") {
					t.Fatalf("topic read lost its selection: %v", args)
				}
				if tenant != "" && args[slices.Index(args, "--tenant")+1] != tenant {
					t.Fatalf("topic read selected another tenant: %v", args)
				}
				for _, mutation := range []string{"rm", "create", "set", "purge", "flush", "commit", "modify"} {
					if slices.Contains(args, mutation) {
						t.Fatalf("read-only observation invoked %q: %v", mutation, args)
					}
				}
			}
			dump := f.calls[2]
			if i := slices.Index(dump, "--max-entries"); i < 0 || dump[i+1] != "5" {
				t.Fatalf("dump is not bounded by the sample: %v", dump)
			}
		})
	}
}

func TestQueueWithoutSampleSkipsDumpAndAcceptsEmptyQueue(t *testing.T) {
	g, f := newFixture(persistentReplies(""))
	snapshot, err := queue(t.Context(), g.Core, Topic{Name: "tc-persistent"}, Sample{})
	if err != nil || snapshot.Sampled || snapshot.Pending != nil || !slices.Equal(subcommands(f.calls), []string{"get", "stats", "get"}) {
		t.Fatalf("unsampled observation: %+v %v %v", snapshot, subcommands(f.calls), err)
	}
	g, _ = newFixture(persistentReplies("", ""))
	snapshot, err = queue(t.Context(), g.Core, Topic{Name: "tc-persistent"}, Sample{MaxEntries: 1})
	if err != nil || !snapshot.Sampled || snapshot.Pending == nil || len(snapshot.Pending) != 0 {
		t.Fatalf("empty sample is not explicit: %+v %v", snapshot, err)
	}
}

func TestQueueAcceptsWrappedNativeDocuments(t *testing.T) {
	doc := `{"topic":` + topicDocument("", "tc-persistent", "q", true) + `}`
	g, _ := newFixture(map[string][]reply{"get": {{data: doc}, {data: doc}}, "stats": {{data: statsDocument}},
		"dump": {{data: `[{"entry":` + entry("c") + `}]`}}})
	snapshot, err := queue(t.Context(), g.Core, Topic{Name: "tc-persistent"}, Sample{MaxEntries: 1})
	if err != nil || snapshot.Topic.Queue != "q" || len(snapshot.Pending) != 1 || snapshot.Pending[0].Key != "queued/c" {
		t.Fatalf("wrapped native documents: %+v %v", snapshot, err)
	}
}

func TestQueueRefusesInvalidSelectionBeforeNativeCommands(t *testing.T) {
	for _, c := range []struct {
		topic  Topic
		sample Sample
	}{
		{Topic{}, Sample{}}, {Topic{Name: "a:b"}, Sample{}}, {Topic{Name: "a b"}, Sample{}}, {Topic{Name: strings.Repeat("a", 257)}, Sample{}},
		{Topic{Name: "t", Tenant: "bad-tenant"}, Sample{}}, {Topic{Name: "t", Tenant: "RGW12345678901234567"}, Sample{}},
		{Topic{Name: "t"}, Sample{MaxEntries: -1}}, {Topic{Name: "t"}, Sample{MaxEntries: MaxSampleEntries + 1}},
		{Topic{Name: "t"}, Sample{MaxEntries: 1, MaxBytes: -1}}, {Topic{Name: "t"}, Sample{MaxEntries: 1, MaxBytes: MaxSampleBytes + 1}},
	} {
		g, f := newFixture(persistentReplies(""))
		if _, err := queue(t.Context(), g.Core, c.topic, c.sample); !errors.Is(err, ceph.ErrInvalidRequest) || len(f.calls) != 0 {
			t.Fatalf("%+v %+v: %v after %d commands", c.topic, c.sample, err, len(f.calls))
		}
	}
	if _, err := Queue(t.Context(), nil, Topic{Name: "t"}, Sample{}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("nil gateway: %v", err)
	}
}

func TestQueueKeepsMissingDeniedAndFailedReadsApartFromEmptyQueues(t *testing.T) {
	for _, c := range []struct {
		name   string
		stderr string
		is     error
	}{
		{"missing", "ERROR: could not get topic PRIVATE-ARGS: (2) No such file or directory", fs.ErrNotExist},
		{"denied", "ERROR: could not get topic PRIVATE-ARGS: (13) Permission denied", fs.ErrPermission},
		{"failed", "ERROR: PRIVATE-ARGS: (5) Input/output error", ceph.ErrNativeFailure},
	} {
		t.Run(c.name, func(t *testing.T) {
			g, f := newFixture(map[string][]reply{"get": {{code: 2, data: c.stderr}}})
			snapshot, err := queue(t.Context(), g.Core, Topic{Name: "tc-persistent"}, Sample{MaxEntries: 1})
			var failure *ceph.NativeFailure
			if !errors.Is(err, c.is) || !errors.Is(err, ceph.ErrNativeFailure) || !errors.As(err, &failure) || failure.Reason != "" {
				t.Fatalf("native %s read not classified: %v", c.name, err)
			}
			if !reflectZero(snapshot) || len(f.calls) != 1 {
				t.Fatalf("refused read became a queue: %+v after %d commands", snapshot, len(f.calls))
			}
			assertNoPrivate(t, err)
		})
	}
	g, f := newFixture(map[string][]reply{"get": {{data: topicDocument("", "tc-persistent", "q", true)}}, "stats": {{code: 2, data: "(13) Permission denied PRIVATE-ARGS"}}})
	if snapshot, err := queue(t.Context(), g.Core, Topic{Name: "tc-persistent"}, Sample{}); !errors.Is(err, fs.ErrPermission) || !reflectZero(snapshot) || len(f.calls) != 2 {
		t.Fatalf("refused stats read: %+v %v", snapshot, err)
	}
}

func reflectZero(s QueueSnapshot) bool {
	return s.Topic == (Metadata{}) && s.Stats == (Stats{}) && s.Pending == nil && !s.Sampled
}

func TestQueueRefusesNonpersistentAndForeignTopics(t *testing.T) {
	g, f := newFixture(map[string][]reply{"get": {{data: topicDocument("", "tc-direct", "", false)}}})
	if _, err := queue(t.Context(), g.Core, Topic{Name: "tc-direct"}, Sample{MaxEntries: 1}); !errors.Is(err, ErrNotPersistent) || !errors.Is(err, ceph.ErrInvalidRequest) || len(f.calls) != 1 {
		t.Fatalf("nonpersistent topic: %v after %v", err, subcommands(f.calls))
	}
	for name, doc := range map[string]string{
		"other name":    topicDocument("", "tc-unrelated", "tc-unrelated", true),
		"other tenant":  topicDocument("team_b", "tc-persistent", "tc-persistent", true),
		"account owner": strings.Replace(topicDocument("", "tc-persistent", "q", true), "default::", "default:RGW12345678901234567:", 1),
		"foreign arn":   strings.Replace(topicDocument("", "tc-persistent", "q", true), "arn:aws:sns", "arn:aws:sqs", 1),
	} {
		g, f := newFixture(map[string][]reply{"get": {{data: doc}}})
		if snapshot, err := queue(t.Context(), g.Core, Topic{Name: "tc-persistent"}, Sample{}); !errors.Is(err, ceph.ErrInvalidRequest) || !reflectZero(snapshot) || len(f.calls) != 1 {
			t.Fatalf("%s: %+v %v", name, snapshot, err)
		}
	}
}

func TestQueueRefusesIdentityChangeBetweenReads(t *testing.T) {
	for name, after := range map[string]string{
		"queue": topicDocument("", "tc-persistent", "recreated", true),
		"owner": strings.Replace(topicDocument("", "tc-persistent", "tc-persistent", true), `"owner":"tc-notify"`, `"owner":"tc-other"`, 1),
	} {
		g, _ := newFixture(map[string][]reply{"get": {{data: topicDocument("", "tc-persistent", "tc-persistent", true)}, {data: after}}, "stats": {{data: statsDocument}}})
		if snapshot, err := queue(t.Context(), g.Core, Topic{Name: "tc-persistent"}, Sample{}); err == nil || !reflectZero(snapshot) {
			t.Fatalf("%s change accepted: %+v", name, snapshot)
		}
	}
	g, _ := newFixture(map[string][]reply{"get": {{data: topicDocument("", "tc-persistent", "q", true)}, {data: topicDocument("", "tc-persistent", "q", false)}}, "stats": {{data: statsDocument}}})
	if _, err := queue(t.Context(), g.Core, Topic{Name: "tc-persistent"}, Sample{}); !errors.Is(err, ErrNotPersistent) {
		t.Fatalf("topic replaced by a nonpersistent one: %v", err)
	}
}

func TestQueueRefusesMalformedNativeReplies(t *testing.T) {
	doc := topicDocument("", "tc-persistent", "q", true)
	cases := map[string]map[string][]reply{}
	for name, stats := range map[string]string{
		"stats missing": `{"Topic Stats":{"Reservations":0,"Size":0}}`, "stats negative": `{"Topic Stats":{"Reservations":0,"Size":0,"Entries":-1}}`,
		"stats string": `{"Topic Stats":{"Reservations":0,"Size":0,"Entries":"1"}}`, "stats fraction": `{"Topic Stats":{"Reservations":0,"Size":0.5,"Entries":1}}`,
		"stats duplicate": `{"Topic Stats":{"Reservations":0,"Size":0,"Entries":1,"Entries":0}}`, "stats trailing": statsDocument + `{}`,
		"stats flat": `{"Reservations":0,"Size":0,"Entries":0}`, "stats empty": ``,
	} {
		cases[name] = map[string][]reply{"get": {{data: doc}, {data: doc}}, "stats": {{data: stats}}}
	}
	for name, dump := range map[string]string{
		"dump object": `{"entries":[]}`, "dump null": `null`, "dump scalar element": `[1]`, "dump too many": "[" + entry("a") + "," + entry("b") + "]",
		"dump no key":       `[{"event":{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"b"},"object":{}}}}]`,
		"dump numeric id":   `[{"event":{"eventName":"ObjectCreated:Put","eventId":7,"s3":{"bucket":{"name":"b"},"object":{"key":"k"}}}}]`,
		"dump no event":     `[{"pushEndpoint":"PRIVATE-ARGS"}]`,
		"dump duplicate":    `[{"event":{"eventName":"a","eventName":"b","s3":{"bucket":{"name":"b"},"object":{"key":"k"}}}}]`,
		"dump null version": `[{"event":{"eventName":"a","s3":{"bucket":{"name":"b"},"object":{"key":"k","versionId":null}}}}]`,
	} {
		cases[name] = map[string][]reply{"get": {{data: doc}, {data: doc}}, "stats": {{data: statsDocument}}, "dump": {{data: dump}}}
	}
	for name, topic := range map[string]string{
		"topic persistent text": strings.Replace(doc, `"persistent":true`, `"persistent":"true"`, 1),
		"topic no dest":         `{"name":"tc-persistent","arn":"arn:aws:sns:default::tc-persistent"}`,
		"topic owner object":    strings.Replace(doc, `"owner":"tc-notify"`, `"owner":{"id":"PRIVATE-ARGS"}`, 1),
		"topic queue number":    strings.Replace(doc, `"persistent_queue":"q"`, `"persistent_queue":1`, 1),
		"topic array":           `[` + doc + `]`,
	} {
		cases[name] = map[string][]reply{"get": {{data: topic}}}
	}
	for name, replies := range cases {
		t.Run(name, func(t *testing.T) {
			g, _ := newFixture(replies)
			snapshot, err := queue(t.Context(), g.Core, Topic{Name: "tc-persistent"}, Sample{MaxEntries: 1})
			if err == nil || errors.Is(err, ceph.ErrInvalidRequest) || errors.Is(err, ceph.ErrNativeFailure) || !reflectZero(snapshot) {
				t.Fatalf("malformed reply accepted or misclassified: %+v %v", snapshot, err)
			}
			assertNoPrivate(t, err)
		})
	}
}

func TestQueueRefusesSampleReplyOverByteLimit(t *testing.T) {
	g, f := newFixture(persistentReplies("", entry("c")))
	snapshot, err := queue(t.Context(), g.Core, Topic{Name: "tc-persistent"}, Sample{MaxEntries: 1, MaxBytes: 256})
	if err == nil || !reflectZero(snapshot) || !slices.Equal(subcommands(f.calls), []string{"get", "stats", "dump"}) {
		t.Fatalf("oversized sample: %+v %v %v", snapshot, err, subcommands(f.calls))
	}
	assertNoPrivate(t, err)
}

func TestQueueRefusesClustersBeforeSquid(t *testing.T) {
	g, f := newFixture(persistentReplies(""))
	testhelper.TestSetCephVersion(g.Cluster, "18.2.4")
	var failure *ceph.OperationError
	if _, err := queue(t.Context(), g.Core, Topic{Name: "tc-persistent"}, Sample{}); !errors.Is(err, ceph.ErrUnsupported) || !errors.As(err, &failure) || failure.Version != "18.2.4" || len(f.calls) != 0 {
		t.Fatalf("pre-Squid cluster: %v after %d commands", err, len(f.calls))
	}
}

func TestQueueCancellationReturnsNoPartialSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	g, f := newFixture(persistentReplies("", entry("c")))
	if snapshot, err := queue(ctx, g.Core, Topic{Name: "tc-persistent"}, Sample{MaxEntries: 1}); !errors.Is(err, context.Canceled) || !reflectZero(snapshot) || len(f.calls) != 0 {
		t.Fatalf("canceled call: %+v %v after %d commands", snapshot, err, len(f.calls))
	}
	for _, step := range []string{"stats", "dump"} {
		t.Run(step, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			g, f := newFixture(persistentReplies("", entry("c")))
			f.onExec = func(args []string) {
				if slices.Contains(args, step) {
					cancel()
				}
			}
			snapshot, err := queue(ctx, g.Core, Topic{Name: "tc-persistent"}, Sample{MaxEntries: 1})
			if !errors.Is(err, context.Canceled) || !reflectZero(snapshot) {
				t.Fatalf("cancellation during %s: %+v %v", step, snapshot, err)
			}
			if got := subcommands(f.calls); got[len(got)-1] != step {
				t.Fatalf("canceled observation continued: %v", got)
			}
		})
	}
}
