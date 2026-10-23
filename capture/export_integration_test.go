//go:build all || (integration && (!ci || (ci_environment && (!ci_batch || ci_batch_packet_capture))))

package capture

import (
	"context"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// StartWithoutNetRaw starts a capture as Start does, except that the capture
// helper, not the probe, runs without NET_RAW. The public API always grants
// NET_RAW, so only this test hook can show how the package classifies an
// engine that withholds it from tcpdump.
func StartWithoutNetRaw(ctx context.Context, target testcontainers.Container, req Request) (*Session, error) {
	return start(ctx, withoutNetRaw{}, target, req)
}

// withoutNetRaw is the Docker backend whose capture helper has no
// capability at all; its probe still runs through dockerBackend.run.
type withoutNetRaw struct{ dockerBackend }

func (withoutNetRaw) run(ctx context.Context, image, networkMode string, argv []string) (string, error) {
	options := append(helperOptions(networkMode, argv), testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
		host.CapAdd = nil
	}))
	helper, err := testcontainers.Run(ctx, image, options...)
	if helper == nil {
		return "", err
	}
	return helper.GetContainerID(), err
}
