//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"time"
)

// rgwReplicaSample is one read of a replicated object: what the gateway
// answered, or why no answer arrived. A missing object (NoSuchKey) and a
// missing bucket (NoSuchBucket) are different observations.
type rgwReplicaSample struct {
	Status  int
	Code    string
	Message string
	Bytes   int
	SHA256  string
	ReadErr string
	Err     string
	// Late marks a body that was read only after the probe's own deadline;
	// it is not evidence of the probe even when its bytes match.
	Late bool
}

func (s rgwReplicaSample) String() string {
	if s.Err != "" {
		return "request error: " + s.Err
	}
	text := fmt.Sprintf("HTTP=%d code=%s message=%s bytes=%d sha256=%s", s.Status, s.Code, s.Message, s.Bytes, s.SHA256)
	if s.ReadErr != "" {
		text += " read error: " + s.ReadErr
	}
	if s.Late {
		text += " (read after the probe deadline)"
	}
	return text
}

// rgwReplicaBodyLimit bounds a body read beyond the expected payload, so an
// oversized reply is reported by its size rather than buffered whole.
const rgwReplicaBodyLimit = 64 << 10

// rgwReplicaProbe reads path once through client and reports whether it holds
// exactly payload. It is read-only for observe: a request or body error is a
// sample to wait past, and a body read after the probe's context ended is
// never accepted.
func rgwReplicaProbe(client s3HTTPClient, path string, payload []byte) func(context.Context) (rgwReplicaSample, bool, error) {
	return func(ctx context.Context) (rgwReplicaSample, bool, error) {
		var sample rgwReplicaSample
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.endpoint+path, nil)
		if err != nil {
			return sample, false, err
		}
		client.sign(request, nil, time.Now().UTC())
		response, err := client.http.Do(request)
		if err != nil {
			sample.Err = err.Error()
			return sample, false, nil
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, int64(len(payload))+rgwReplicaBodyLimit))
		_ = response.Body.Close()
		sum := sha256.Sum256(body)
		sample.Status, sample.Bytes, sample.SHA256 = response.StatusCode, len(body), hex.EncodeToString(sum[:])
		if readErr != nil {
			sample.ReadErr = readErr.Error()
		}
		if response.StatusCode != http.StatusOK {
			var failure struct{ Code, Message string }
			_ = xml.Unmarshal(body, &failure)
			sample.Code, sample.Message = failure.Code, failure.Message
		}
		if ctx.Err() != nil {
			sample.Late = true
			return sample, false, nil
		}
		return sample, readErr == nil && response.StatusCode == http.StatusOK && bytes.Equal(body, payload), nil
	}
}

// rgwReplicaBounds bound one replica convergence: the whole wait, one read
// and the pause between reads.
type rgwReplicaBounds struct {
	Wait, Read, Interval time.Duration
}

// rgwReplicaConverged waits until path holds exactly payload. Its error names
// the last sample and the expected size and digest.
func rgwReplicaConverged(ctx context.Context, client s3HTTPClient, path string, payload []byte, bounds rgwReplicaBounds) (observation[rgwReplicaSample], error) {
	seen, err := observe(ctx, bounds.Wait, bounds.Read, bounds.Interval, rgwReplicaProbe(client, path, payload))
	if err != nil {
		sum := sha256.Sum256(payload)
		return seen, fmt.Errorf("replica %s: %w; last %s; expected bytes=%d sha256=%s", path, err, seen.Value, len(payload), hex.EncodeToString(sum[:]))
	}
	return seen, nil
}
