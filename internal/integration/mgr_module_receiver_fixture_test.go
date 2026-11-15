//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestModuleReceiverScriptsCompile(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not installed")
	}
	for name, script := range map[string]string{"smtp": smtpSinkScript, "telegraf": telegrafReceiverScript} {
		if out, err := exec.Command(python, "-I", "-c", "import sys; compile(sys.argv[1], sys.argv[2], 'exec')", moduleReceiverPrelude+script, name).CombinedOutput(); err != nil {
			t.Errorf("%s receiver: %v: %s", name, err, out)
		}
	}
}

func TestReceiverHistoryExtendsEarlierReads(t *testing.T) {
	first, err := receiverHistory(0, []byte(`{"n":1}`+"\n"+`{"n":2}`+"\n"+`{"n":`), nil, nil)
	if err != nil || !slices.Equal(first, []string{`{"n":1}`, `{"n":2}`}) {
		t.Fatalf("first read %q, %v; want two complete records", first, err)
	}
	empty, err := receiverHistory(0, nil, nil, nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty file %q, %v; want an empty history", empty, err)
	}
	grown, err := receiverHistory(0, []byte(`{"n":1}`+"\n"+`{"n":2}`+"\n"+`{"n":3}`+"\n"), nil, first)
	if err != nil || len(grown) != 3 {
		t.Fatalf("grown read %q, %v; want three records", grown, err)
	}
}

func TestReceiverHistoryRejectsFailedAndShrunkReads(t *testing.T) {
	earlier := []string{`{"n":1}`, `{"n":2}`}
	for name, tc := range map[string]struct {
		code    int
		out     string
		err     error
		earlier []string
		want    string
	}{
		"read error with no history":    {code: 1, out: "cat: /tmp/receiver/records.jsonl: Input/output error\n", want: "cat exited 1: cat: /tmp/receiver/records.jsonl: Input/output error"},
		"missing file after a history":  {code: 1, out: "cat: /tmp/receiver/records.jsonl: No such file or directory\n", earlier: earlier, want: "cat exited 1"},
		"exec failure":                  {err: errors.New("container is not running"), earlier: earlier, want: "read records: container is not running"},
		"empty read after a history":    {earlier: earlier, want: "history shrank from 2 records to 0"},
		"shorter read after a history":  {out: `{"n":1}` + "\n", earlier: earlier, want: "history shrank from 2 records to 1"},
		"rewritten record":              {out: `{"n":1}` + "\n" + `{"n":9}` + "\n", earlier: earlier, want: `record 1 changed from "{\"n\":2}" to "{\"n\":9}"`},
		"truncated record of a history": {out: `{"n":1}` + "\n" + `{"n":2`, earlier: earlier, want: "history shrank from 2 records to 1"},
	} {
		t.Run(name, func(t *testing.T) {
			lines, err := receiverHistory(tc.code, []byte(tc.out), tc.err, tc.earlier)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %q, %v; want an error containing %q", lines, err, tc.want)
			}
		})
	}
}
