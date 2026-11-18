//go:build all || integration

package integration_test

import (
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
)

// readNativeEvidence requires the complete response before a caller derives
// capability, absence, authentication or timing evidence from its bytes.
func readNativeEvidence(t testing.TB, reader io.Reader) []byte {
	t.Helper()
	if reader == nil {
		t.Fatal("native observation returned no output reader")
		return nil
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read complete native observation: %v", err)
		return nil
	}
	return data
}

func requireAuthAbsent(t testing.TB, err error) {
	t.Helper()
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("auth identity absence was not confirmed: %v", err)
	}
}

// ceph-authtool may return a nonzero status for --help. Its complete help
// text determines the supported option.
func keyTypesFromHelp(t testing.TB, reader io.Reader) bool {
	t.Helper()
	return strings.Contains(string(readNativeEvidence(t, reader)), "--key-type")
}
