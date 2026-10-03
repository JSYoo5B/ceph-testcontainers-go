//go:build integration

package integration_test

import (
	"net/http"
	"testing"
	"time"
)

func TestS3FixtureSigningUsesUTCInstantAcrossTimeZones(t *testing.T) {
	client := s3HTTPClient{accessKey: "fixture-access", secretKey: "fixture-secret", region: "us-east-1"}
	instant := time.Date(2026, 10, 3, 23, 30, 0, 0, time.UTC)
	var authorization string
	for _, offset := range []int{0, 9 * 60 * 60, -7 * 60 * 60} {
		request, err := http.NewRequest(http.MethodGet, "http://rgw.invalid/bucket/object", nil)
		if err != nil {
			t.Fatal(err)
		}
		client.sign(request, nil, instant.In(time.FixedZone("caller", offset)))
		if request.Header.Get("X-Amz-Date") != "20261003T233000Z" {
			t.Fatal("fixture signed a local wall clock as UTC")
		}
		if authorization == "" {
			authorization = request.Header.Get("Authorization")
		} else if request.Header.Get("Authorization") != authorization {
			t.Fatal("one instant produced different UTC date scopes or signatures across caller time zones")
		}
	}
}
