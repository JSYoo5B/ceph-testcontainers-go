//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"os/exec"
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
