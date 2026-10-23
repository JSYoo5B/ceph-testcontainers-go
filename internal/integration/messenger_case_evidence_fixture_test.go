//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

type evidenceImageContainer struct {
	testcontainers.Container
	image string
	err   error
}

func (c evidenceImageContainer) Inspect(ctx context.Context) (*container.InspectResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.err != nil {
		return nil, c.err
	}
	return &container.InspectResponse{Image: c.image}, nil
}

func TestMessengerEvidenceExecutorImages(t *testing.T) {
	essential, client, cephfs := "sha256:"+strings.Repeat("1", 64), "sha256:"+strings.Repeat("2", 64), "sha256:"+strings.Repeat("3", 64)
	for _, tc := range []struct {
		name      string
		services  []testcontainers.Container
		executors []testcontainers.Container
		want      []string
	}{
		{"split roles", []testcontainers.Container{evidenceImageContainer{image: essential}, evidenceImageContainer{image: cephfs}},
			[]testcontainers.Container{evidenceImageContainer{image: client}, evidenceImageContainer{image: client}}, []string{essential, client, cephfs}},
		{"aggregate deduplication", []testcontainers.Container{evidenceImageContainer{image: essential}, evidenceImageContainer{image: essential}},
			[]testcontainers.Container{evidenceImageContainer{image: essential}, evidenceImageContainer{image: essential}}, []string{essential}},
		{"separately owned client", []testcontainers.Container{evidenceImageContainer{image: essential}},
			[]testcontainers.Container{evidenceImageContainer{image: client}, evidenceImageContainer{image: cephfs}}, []string{essential, client, cephfs}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := messengerEvidenceImages(t.Context(), tc.services, tc.executors...)
			if err != nil || !slices.Equal(got, tc.want) {
				t.Fatalf("images %v, error %v; want %v", got, err, tc.want)
			}
		})
	}
}

func TestMessengerEvidenceIncompleteExecutorImages(t *testing.T) {
	essential := evidenceImageContainer{image: "sha256:" + strings.Repeat("1", 64)}
	inspectErr := errors.New("client inspection unavailable")
	for _, tc := range []struct {
		name     string
		executor testcontainers.Container
		wantErr  error
	}{
		{"missing", nil, nil},
		{"mutable tag", evidenceImageContainer{image: "ceph:client"}, nil},
		{"truncated ID", evidenceImageContainer{image: "sha256:1234"}, nil},
		{"inspection failed", evidenceImageContainer{err: inspectErr}, inspectErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			images, err := messengerEvidenceImages(t.Context(), []testcontainers.Container{essential}, tc.executor)
			if err == nil || images != nil || tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("incomplete executor returned images %v, error %v", images, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if images, err := messengerEvidenceImages(ctx, nil, essential); images != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled image inspection returned %v, %v", images, err)
	}
}
