//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// nativeInputWorker reads one record per "size:sha256" argument from its
// standard input and then expects the end of input. It publishes a ready
// receipt before it reads and one receipt per record, each with the run's
// token, and prints only counts and equality: it never echoes its input.
// Arguments: work directory, token, then the record specs.
const nativeInputWorker = `import hashlib,json,os,sys
workdir,token=sys.argv[1],sys.argv[2]
def publish(name,**fields):
 fields.update(token=token,pid=os.getpid())
 path=os.path.join(workdir,name)
 with open(path+'.tmp','w') as out: json.dump(fields,out)
 os.rename(path+'.tmp',path)
os.makedirs(workdir,exist_ok=True)
publish('ready')
matched=[]
for index,spec in enumerate(sys.argv[3:]):
 size,digest=spec.split(':')
 data=b''
 while len(data)<int(size):
  chunk=os.read(0,int(size)-len(data))
  if not chunk: break
  data+=chunk
 matched.append(len(data)==int(size) and hashlib.sha256(data).hexdigest()==digest)
 publish('record-%d'%index,matched=matched[-1])
print(json.dumps({'records':len(matched),'matched':all(matched),'eof':os.read(0,1)==b''}))
`

// nativeInputReceipt waits for the worker's named receipt in ctr and checks
// its token.
func nativeInputReceipt(t *testing.T, ctx context.Context, ctr testcontainers.Container, workdir, token, name string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		if code, out, err := harness.SessionExec(ctx, ctr, "cat", workdir+"/"+name); err == nil && code == 0 {
			var receipt map[string]any
			if err := json.Unmarshal([]byte(out), &receipt); err != nil || receipt["token"] != token {
				t.Fatalf("receipt %s of another run or malformed: %v %q", name, err, out)
			}
			return receipt
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker did not publish %s", name)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// testNativeCommandInput runs a worker under RunNativeCommand in a client
// container and sends it two binary records in separate phases: the second
// only after the worker confirmed the first. The records hold NUL and bytes
// that are not UTF-8. Only their sizes and SHA-256 sums reach the worker's
// arguments, and its output holds only counts and equality.
func testNativeCommandInput(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	image, roles := harness.IntegrationImages(t)
	cluster, client := harness.NewServiceClusterWithOptions(t, image, append(roles, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs()))...)
	first := []byte{0x00, 'f', 0xff, 0xfe, 0x00, '\n', 0x80, 0xc3, 0x28}
	second := make([]byte, 300<<10)
	if _, err := rand.Read(second); err != nil {
		t.Fatal(err)
	}
	second[0], second[len(second)-1] = 0x00, 0xff
	spec := func(record []byte) string {
		sum := sha256.Sum256(record)
		return strconv.Itoa(len(record)) + ":" + hex.EncodeToString(sum[:])
	}
	token := make([]byte, 8)
	if _, err := rand.Read(token); err != nil {
		t.Fatal(err)
	}
	tokenHex, workdir := hex.EncodeToString(token), "/tmp/native-input-worker"
	runCtx, stop := context.WithCancel(ctx)
	input := ceph.NewNativeInput()
	// The call stores its outcome and then closes finished, so the normal
	// path below and the cleanup both wait on finished.
	finished := make(chan struct{})
	var run struct {
		result ceph.NativeCommandResult
		err    error
	}
	go func() {
		defer close(finished)
		run.result, run.err = cluster.RunNativeCommand(runCtx, client, ceph.NativeCommand{
			Argv:    []string{"python3", "-c", nativeInputWorker, workdir, tokenHex, spec(first), spec(second)},
			Timeout: 2 * time.Minute, MaxStdout: 4096, MaxStderr: 64 << 10, Label: "native input worker",
			Input: input, MaxInput: len(first) + len(second),
		})
	}()
	joinNativeInputAtCleanup(t, ctx, stop, input, &run.err, finished)
	ready := nativeInputReceipt(t, ctx, client, workdir, tokenHex, "ready")
	if err := input.Write(ctx, first); err != nil {
		t.Fatal("first record:", err)
	}
	if receipt := nativeInputReceipt(t, ctx, client, workdir, tokenHex, "record-0"); receipt["matched"] != true || receipt["pid"] != ready["pid"] {
		t.Fatalf("first record receipt %v, ready %v", receipt, ready)
	}
	if err := input.Write(ctx, second); err != nil {
		t.Fatal("second record:", err)
	}
	if receipt := nativeInputReceipt(t, ctx, client, workdir, tokenHex, "record-1"); receipt["matched"] != true || receipt["pid"] != ready["pid"] {
		t.Fatalf("second record receipt %v, ready %v", receipt, ready)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	<-finished
	total := int64(len(first) + len(second))
	if run.err != nil || run.result.ExitCode != 0 || !run.result.Complete() || run.result.InputWritten != total ||
		run.result.InputDelivered != total || !run.result.InputEOF {
		t.Fatalf("worker result exit=%d complete=%t written=%d delivered=%d eof=%t err=%v stderr=%.1024s",
			run.result.ExitCode, run.result.Complete(), run.result.InputWritten, run.result.InputDelivered, run.result.InputEOF, run.err, run.result.Stderr)
	}
	if got := strings.TrimSpace(string(run.result.Stdout)); got != `{"records": 2, "matched": true, "eof": true}` {
		t.Fatalf("worker printed %q", got)
	}
	if bytes.Contains(run.result.Stdout, first) || bytes.Contains(run.result.Stderr, first) {
		t.Fatal("the worker's output holds its input")
	}
	if err := input.Write(ctx, []byte{1}); !errors.Is(err, ceph.ErrNativeInputClosed) {
		t.Fatalf("a write after the worker ended returned %v", err)
	}
	t.Logf("NATIVE_INPUT worker pid=%v took %d bytes in two phases (%d and %d) and the end of input; delivered=%d eof=%t",
		ready["pid"], total, len(first), len(second), run.result.InputDelivered, run.result.InputEOF)
	checkNoSupervisedLeftovers(t, ctx, client)
}

// testNativeCommandInputCancellation cancels a call to a command that does
// not read, once its supervisor took input. The call returns after it
// stopped the command, every later write fails as closed input, and the
// container keeps no process or marker of it. The supervisor buffers input
// up to MaxInput, so whether a write is still in progress at the
// cancellation is not observed here; the writer's log line says whether it
// reached MaxInput first. TestNativeCommandInputCancellationDuringTransfer
// covers a write interrupted on the connection.
func testNativeCommandInputCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	image, roles := harness.IntegrationImages(t)
	cluster, client := harness.NewServiceClusterWithOptions(t, image, append(roles, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs()))...)
	runCtx, stop := context.WithCancel(ctx)
	input := ceph.NewNativeInput()
	called := make(chan struct{})
	var callErr error
	go func() {
		defer close(called)
		_, callErr = cluster.RunNativeCommand(runCtx, client, ceph.NativeCommand{
			Argv:    []string{"sh", "-c", "echo $$ > /tmp/native-input-sleeper.pid; exec sleep 300"},
			Timeout: 5 * time.Minute, MaxStdout: 4096, MaxStderr: 4096, Label: "native input sleeper",
			Input: input, MaxInput: ceph.MaxNativeCommandInput,
		})
	}()
	joinNativeInputAtCleanup(t, ctx, stop, input, &callErr, called)
	var pid string
	for deadline := time.Now().Add(time.Minute); pid == ""; {
		if code, out, err := harness.SessionExec(ctx, client, "cat", "/tmp/native-input-sleeper.pid"); err == nil && code == 0 {
			pid = strings.TrimSpace(out)
		} else if time.Now().After(deadline) {
			t.Fatal("the sleeper did not start")
		} else {
			time.Sleep(200 * time.Millisecond)
		}
	}
	// The writer writes until a write fails. A write refused at MaxInput
	// leaves nothing in progress to interrupt, so the writer then waits for
	// the call to end and requires its next write to fail as closed input.
	took, wrote := make(chan struct{}), make(chan struct{})
	var writeErr error
	var limited bool
	go func() {
		defer close(wrote)
		chunk := bytes.Repeat([]byte{0x00, 0xff}, 512<<10)
		for first := true; ; first = false {
			err := input.Write(ctx, chunk)
			if err == nil {
				if first {
					close(took)
				}
				continue
			}
			if errors.Is(err, ceph.ErrInvalidRequest) {
				limited = true
				<-called
				err = input.Write(ctx, []byte{1})
			}
			writeErr = err
			return
		}
	}()
	joinNativeInputAtCleanup(t, ctx, stop, input, nil, wrote)
	select {
	case <-took:
	case <-wrote:
		t.Fatalf("the first write failed: %v", writeErr)
	case <-time.After(time.Minute):
		t.Fatal("the supervisor did not take the first write")
	}
	stop()
	select {
	case <-called:
		if !errors.Is(callErr, context.Canceled) {
			t.Fatalf("canceled call returned %v", callErr)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("the canceled call did not return")
	}
	select {
	case <-wrote:
		if !errors.Is(writeErr, ceph.ErrNativeInputClosed) || errors.Is(writeErr, ceph.ErrInvalidRequest) {
			t.Fatalf("the writer ended with %v", writeErr)
		}
	case <-time.After(time.Minute):
		t.Fatal("the writer did not return after the call was canceled")
	}
	if code, _, err := harness.SessionExec(ctx, client, "kill", "-0", pid); err != nil || code == 0 {
		t.Fatalf("the canceled command %s still runs: %d, %v", pid, code, err)
	}
	t.Logf("NATIVE_INPUT canceled sleeper pid=%s is gone and the writer returned closed input (reached MaxInput first: %t)", pid, limited)
	checkNoSupervisedLeftovers(t, ctx, client)
}

// joinNativeInputAtCleanup registers, right after workers started and so
// before the fixture cleanups registered earlier, a cleanup that cancels the
// call, closes its input and waits until every channel in finished is
// closed, also when an assertion ended the test early. With callErr, which
// the call sets before its channel closes, a command that the call could not
// confirm stopped is stopped there.
func joinNativeInputAtCleanup(t *testing.T, ctx context.Context, stop context.CancelFunc, input *ceph.NativeInput, callErr *error, finished ...<-chan struct{}) {
	t.Helper()
	t.Cleanup(func() {
		stop()
		_ = input.Close()
		timeout := time.After(2 * time.Minute)
		for _, done := range finished {
			select {
			case <-done:
			case <-timeout:
				t.Error("a native input worker did not return before the fixture cleanup")
				return
			}
		}
		var unstopped *ceph.NativeCommandUnstoppedError
		if callErr == nil || !errors.As(*callErr, &unstopped) {
			return
		}
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if err := unstopped.Stop(stopCtx); err != nil {
			t.Errorf("stop the unconfirmed native input command: %v", err)
		}
	})
}

// checkNoSupervisedLeftovers requires that no supervisor marker remains in
// ctr.
func checkNoSupervisedLeftovers(t *testing.T, ctx context.Context, ctr testcontainers.Container) {
	t.Helper()
	code, out, err := harness.SessionExec(ctx, ctr, "sh", "-c", "ls -A /tmp/tc-supervised 2>/dev/null")
	if err != nil || strings.TrimSpace(out) != "" {
		t.Fatalf("supervisor markers remain: %d %q %v", code, out, err)
	}
}
