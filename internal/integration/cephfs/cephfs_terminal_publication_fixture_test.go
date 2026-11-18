//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type terminalPublicationContainer struct {
	testcontainers.Container
	run   *cephFSActorRun
	body  string
	reads int
}

func (c *terminalPublicationContainer) Exec(_ context.Context, _ []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.reads++
	if c.reads == 1 {
		// cat has observed ENOENT; the producer publishes atomically and exits
		// before the observer can inspect Done.
		close(c.run.Done)
		return 1, strings.NewReader("missing"), nil
	}
	return 0, strings.NewReader(c.body), nil
}

func TestCephFSTerminalPublicationRace(t *testing.T) {
	for _, consumer := range []string{"receipt", "progress", "reclaim"} {
		t.Run(consumer, func(t *testing.T) {
			run := &cephFSActorRun{Done: make(chan struct{}), Token: "owned"}
			ctr := &terminalPublicationContainer{run: run, body: `{"token":"owned","phase":3,"pid":42}`}
			run.client = ctr
			switch consumer {
			case "receipt":
				run.Receipt(t, context.Background(), 3)
			case "reclaim":
				reclaimRegisteredReceipt(t, context.Background(), run, 3)
			case "progress":
				ctr.body = `{"token":"owned","pid":42,"client":1,"inode":2,"count":3}`
				var last cephFSStreamProgress
				run.progress(t, context.Background(), cephFSActorReceipt{PID: 42, Client: 1, Inode: 2}, &last, 3)
			}
			if ctr.reads != 2 {
				t.Fatalf("terminal read count=%d, want 2", ctr.reads)
			}
		})
	}
}

func TestCephFSRequestIdentityRequired(t *testing.T) {
	for _, body := range []string{`{"requests":null}`, `{"request":{}}`, `{"request":null}`, `{"requests":[{"tid":0}]}`, `{"requests":[{"op":"rename"}]}`} {
		if requests, err := parseClientMDSRequests([]byte(body)); err == nil {
			t.Errorf("unattributable requests could authorize absence: %s: %+v", body, requests)
		}
	}
}

func TestCephFSTerminalPublicationRejectsInvalid(t *testing.T) {
	if mode := os.Getenv("CTG_TERMINAL_PUBLICATION_CASE"); mode != "" {
		consumer, fault, _ := strings.Cut(mode, "/")
		run := &cephFSActorRun{Done: make(chan struct{}), Token: "owned"}
		ctr := &terminalPublicationContainer{run: run, body: `{"token":"owned","phase":3,"pid":42}`}
		run.client = ctr
		switch fault {
		case "malformed":
			ctr.body = `{`
		case "foreign":
			ctr.body = `{"token":"foreign","phase":3,"pid":42}`
		case "failed":
			run.Result.ExitCode = 1
		}
		switch consumer {
		case "receipt":
			run.Receipt(t, context.Background(), 3)
		case "reclaim":
			reclaimRegisteredReceipt(t, context.Background(), run, 3)
		case "progress":
			if fault == "foreign" {
				ctr.body = `{"token":"foreign","pid":42,"client":0,"inode":0,"count":3}`
			}
			var last cephFSStreamProgress
			run.progress(t, context.Background(), cephFSActorReceipt{PID: 42}, &last, 3)
		}
		return
	}
	for _, consumer := range []string{"receipt", "progress", "reclaim"} {
		for _, fault := range []string{"malformed", "foreign", "failed"} {
			t.Run(consumer+"/"+fault, func(t *testing.T) {
				cmd := exec.Command(os.Args[0], "-test.run=^TestCephFSTerminalPublicationRejectsInvalid$")
				cmd.Env = append(os.Environ(), "CTG_TERMINAL_PUBLICATION_CASE="+consumer+"/"+fault)
				output, err := cmd.CombinedOutput()
				if err == nil || !strings.Contains(string(output), "--- FAIL: TestCephFSTerminalPublicationRejectsInvalid") {
					t.Fatalf("invalid terminal publication did not fail its consumer: %v: %s", err, output)
				}
			})
		}
	}
}
