package evidence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func logFrame(stream byte, payload string) []byte {
	frame := []byte{stream, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(frame[4:], uint32(len(payload)))
	return append(frame, payload...)
}

// A multiplexed stream goes to its two writers frame by frame, and a stream
// cut in a frame header or in a payload, also behind a header that advertises
// a huge size, is reported as one cut.
func TestDemuxLogs(t *testing.T) {
	complete := append(logFrame(1, "out\n"), logFrame(2, "err\n")...)
	var stdout, stderr bytes.Buffer
	if err := DemuxLogs(bytes.NewReader(complete), false, &stdout, &stderr); err != nil || stdout.String() != "out\n" || stderr.String() != "err\n" {
		t.Fatalf("demux: %v, stdout %q, stderr %q", err, stdout.String(), stderr.String())
	}
	huge := []byte{2, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 'x'}
	for name, stream := range map[string][]byte{
		"header":       complete[:len(complete)-len(logFrame(2, "err\n"))+5],
		"payload":      complete[:len(complete)-2],
		"huge payload": huge,
	} {
		if err := DemuxLogs(bytes.NewReader(stream), false, &bytes.Buffer{}, &bytes.Buffer{}); !errors.Is(err, ErrLogFrameCut) {
			t.Errorf("%s cut: %v", name, err)
		}
	}
	for _, header := range [][]byte{{0, 0, 0, 0, 0, 0, 0, 0}, {3, 0, 0, 0, 0, 0, 0, 0}, {1, 1, 0, 0, 0, 0, 0, 0}} {
		if err := DemuxLogs(bytes.NewReader(header), false, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || errors.Is(err, ErrLogFrameCut) {
			t.Errorf("header %v: %v", header, err)
		}
	}
	stdout.Reset()
	if err := DemuxLogs(strings.NewReader("raw tty output"), true, &stdout, nil); err != nil || stdout.String() != "raw tty output" {
		t.Fatalf("tty: %v, %q", err, stdout.String())
	}
}
