//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
)

// evidenceFailure records an assertion through the same testing.TB entry point
// as the native callers, without marking this positive regression test failed.
type evidenceFailure struct{ testing.TB }

func (evidenceFailure) Helper()                           {}
func (evidenceFailure) Fatal(args ...any)                 { panic(fmt.Sprint(args...)) }
func (evidenceFailure) Fatalf(format string, args ...any) { panic(fmt.Sprintf(format, args...)) }

func evidenceRejected(run func()) (message string) {
	defer func() {
		if failure := recover(); failure != nil {
			message = fmt.Sprint(failure)
		}
	}()
	run()
	return ""
}

func TestNativeEvidenceRejectsIncompleteReaders(t *testing.T) {
	lost := errors.New("synthetic response read failed")
	for _, prefix := range []string{"", "123 ceph rotate-key\n", "--key-type", "Permission denied", "1700000000.25"} {
		t.Run(fmt.Sprintf("%q", prefix), func(t *testing.T) {
			reader := io.MultiReader(strings.NewReader(prefix), iotest.ErrReader(lost))
			if message := evidenceRejected(func() { readNativeEvidence(evidenceFailure{}, reader) }); !strings.Contains(message, lost.Error()) {
				t.Fatalf("partial observation was accepted: %q", message)
			}
			if got := readNativeEvidence(t, strings.NewReader(prefix)); string(got) != prefix {
				t.Fatalf("complete output changed: %q", got)
			}
		})
	}
	if message := evidenceRejected(func() { readNativeEvidence(evidenceFailure{}, nil) }); !strings.Contains(message, "no output reader") {
		t.Fatalf("nil reader was accepted: %q", message)
	}
}

func TestMessengerListenersRequireCompleteSocketInventory(t *testing.T) {
	const header = "sl local_address rem_address st\n"
	const listening = "0: 00000000:ABCD 00000000:0000 0A\n"
	for _, prefix := range []string{"", header, header + listening} {
		reader := io.MultiReader(strings.NewReader(prefix), iotest.ErrReader(io.ErrUnexpectedEOF))
		if message := evidenceRejected(func() { messengerListeners(evidenceFailure{}, reader) }); !strings.Contains(message, "read complete native observation") {
			t.Fatalf("partial socket inventory became evidence: %q", message)
		}
	}
	if got := messengerListeners(t, strings.NewReader(header+listening)); !slices.Equal(got, []string{"00000000:ABCD"}) {
		t.Fatalf("complete socket inventory = %q", got)
	}
	if got := messengerListeners(t, strings.NewReader(header)); len(got) != 0 {
		t.Fatalf("complete empty socket inventory = %q", got)
	}
}

func TestAuthAbsenceRejectsUnavailableObservation(t *testing.T) {
	for _, err := range []error{nil, fs.ErrPermission, errors.New("Docker unavailable"), io.ErrUnexpectedEOF} {
		if message := evidenceRejected(func() { requireAuthAbsent(evidenceFailure{}, err) }); !strings.Contains(message, "absence was not confirmed") {
			t.Fatalf("observation %v established absence", err)
		}
	}
	requireAuthAbsent(t, fmt.Errorf("native auth lookup: %w", fs.ErrNotExist))
}

func TestProcessInventoriesRejectUnreadableCandidates(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	for name, script := range map[string]string{"rotation": rotateKeyProcessScript, "diagnostics": diagnosticsProcessScript} {
		for _, test := range []struct {
			name, open string
			ok         bool
		}{
			{"empty cmdline", "return io.BytesIO(b'')", true},
			{"matching cmdline", "return io.BytesIO(b'ceph\\0rotate-key\\0')", true},
			{"permission", "raise PermissionError('synthetic denied')", false},
			{"io error", "raise OSError('synthetic read failure')", false},
		} {
			t.Run(name+"/"+test.name, func(t *testing.T) {
				setup := "import builtins,io,os\nos.listdir=lambda path: ['999999999']\ndef candidate(*args,**kwargs):\n " + test.open + "\nbuiltins.open=candidate\n"
				output, err := exec.Command(python, "-c", setup+script, "rotate-key").CombinedOutput()
				if (err == nil) != test.ok {
					t.Fatalf("inventory result: %v %s", err, output)
				}
				if test.name == "empty cmdline" && len(output) != 0 {
					t.Fatalf("empty inventory: %s", output)
				}
				if test.name == "matching cmdline" && !strings.Contains(string(output), "rotate-key") {
					t.Fatalf("missing candidate: %s", output)
				}
			})
		}
	}
}

func TestKeyTypesRequireCompleteHelp(t *testing.T) {
	for _, help := range []string{"", "usage: ceph-authtool --gen-key", "usage: ceph-authtool --key-type <type>"} {
		if got := keyTypesFromHelp(t, strings.NewReader(help)); got != strings.Contains(help, "--key-type") {
			t.Fatalf("complete help changed capability: %q", help)
		}
		reader := io.MultiReader(strings.NewReader(help), iotest.ErrReader(io.ErrUnexpectedEOF))
		if message := evidenceRejected(func() { keyTypesFromHelp(evidenceFailure{}, reader) }); !strings.Contains(message, "read complete native observation") {
			t.Fatalf("incomplete help selected a capability: %q", message)
		}
	}
}
