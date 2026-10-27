package mirror

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMultiClusterWaitLegacyRetainsNativeCauseAcrossContextOnlyAttempt(t *testing.T) {
	for _, kind := range []string{"image"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			native := errors.New("private-native-query-marker")
			calls := 0
			var err error
			previous := ImageStatus{SourceImageID: "source", GlobalID: "global", State: "down+unknown"}
			var status ImageStatus
			status, err = waitRBDMirrorReplay(ctx, time.Millisecond, func(context.Context) (ImageStatus, error) {
				calls++
				if calls == 1 {
					return previous, rbdImageQueryError("read source image", native)
				}
				cancel()
				return ImageStatus{}, context.Canceled
			})
			if status.SourceImageID != previous.SourceImageID || status.State != previous.State || status.ReplayReady {
				t.Fatalf("image report changed: %+v", status)
			}

			if calls != 2 || !errors.Is(err, context.Canceled) || !errors.Is(err, native) || strings.Contains(err.Error(), native.Error()) {
				t.Fatalf("context-only attempt erased/exposed native cause: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestMultiClusterWaitLegacyUsesLatestIndependentCause(t *testing.T) {
	for _, kind := range []string{"image"} {
		for _, inner := range []bool{false, true} {
			name := kind + "/mixed-native-context"
			if inner {
				name = kind + "/inner-deadline"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				first, latest := errors.New("old-native-marker"), errors.New("latest-native-marker")
				calls := 0
				observe := func() (bool, error) {
					calls++
					if calls == 1 {
						return true, first
					}
					if inner && calls == 2 {
						if ctx.Err() != nil {
							t.Error("outer caller expired before inner attempt timeout")
						}
						return true, context.DeadlineExceeded
					}
					cancel()
					if inner {
						return false, context.Canceled
					}
					return true, errors.Join(latest, context.Canceled)
				}
				var err error
				_, err = waitRBDMirrorReplay(ctx, time.Millisecond, func(context.Context) (ImageStatus, error) {
					present, cause := observe()
					status := ImageStatus{}
					if present {
						status.SourceImageID, status.GlobalID = "source", "global"
					}
					return status, rbdImageQueryError("native image query", cause)
				})

				wantCalls := 2
				want := latest
				if inner {
					wantCalls, want = 3, context.DeadlineExceeded
				}
				if calls != wantCalls || !errors.Is(err, context.Canceled) || !errors.Is(err, want) || errors.Is(err, first) || strings.Contains(err.Error(), "native-marker") {
					t.Fatalf("inner/mixed error was mistaken for caller-only failure: calls=%d err=%v", calls, err)
				}
			})
		}
	}
}

func TestMultiClusterWaitScopedCauseRemainsCanonicalOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	private := errors.New("private-native-credential-marker")
	calls := 0
	status, err := waitRBDMirrorReplay(ctx, time.Millisecond, func(context.Context) (ImageStatus, error) {
		calls++
		if calls == 1 {
			return ImageStatus{SourceImageID: "source", GlobalID: "global"}, scopedRBDImageError(ctx, rbdImageQueryError("native read", private))
		}
		cancel()
		return ImageStatus{}, scopedRBDImageError(ctx, context.Canceled)
	})
	err = scopedRBDImageError(ctx, err)
	if calls != 2 || status.SourceImageID != "source" || status.ReplayReady || !errors.Is(err, context.Canceled) || errors.Is(err, private) || strings.Contains(err.Error(), private.Error()) {
		t.Fatalf("scoped wait exposed a private cause or lost cancellation: %+v %v", status, err)
	}
}
