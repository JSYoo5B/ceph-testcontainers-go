package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestInvalidSettingsDoNotCreateResources(t *testing.T) {
	for _, opt := range []Option{WithOSDCount(0), WithOSDBlockSize(0), WithStartupTimeout(0)} {
		cluster, err := Run(context.Background(), DefaultImage, opt)
		if err == nil || cluster != nil {
			t.Fatalf("invalid setting allocated resources: cluster=%v error=%v", cluster, err)
		}
	}
}

func TestCLIJSONSurvivesStderrWarnings(t *testing.T) {
	output, err := command(context.Background(), &commandContainer{}, "ceph", "status", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(output) {
		t.Fatalf("stderr contaminated CLI JSON: %q", output)
	}
	_, err = command(context.Background(), &commandContainer{exitCode: 1}, "ceph", "status")
	if err == nil || !strings.Contains(err.Error(), "diagnostic on stderr") {
		t.Fatalf("missing stderr on failed command: %v", err)
	}
}

type commandContainer struct {
	testcontainers.Container
	exitCode int
}

func (c *commandContainer) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	var framed bytes.Buffer
	for _, stream := range []struct {
		kind stdcopy.StdType
		text string
	}{
		{stdcopy.Stdout, `{"fsid":"test"}`}, {stdcopy.Stderr, "diagnostic on stderr"},
	} {
		var header [8]byte
		header[0] = byte(stream.kind)
		binary.BigEndian.PutUint32(header[4:], uint32(len(stream.text)))
		framed.Write(header[:])
		framed.WriteString(stream.text)
	}
	return c.exitCode, &framed, nil
}
