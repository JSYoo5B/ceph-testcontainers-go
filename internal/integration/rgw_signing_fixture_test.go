//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

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

func TestS3FixtureCanonicalURIFollowsAWS4(t *testing.T) {
	for _, test := range []struct {
		name, path, want string
	}{
		{"root", "", "/"},
		{"unreserved", "/bucket/Az09-_.~", "/bucket/Az09-_.~"},
		{"tenant separator", "/system_beta:bucket/object", "/system_beta%3Abucket/object"},
		{"reserved", "/bucket/a:@&=+$;,?", "/bucket/a%3A%40%26%3D%2B%24%3B%2C%3F"},
		{"utf8 and literal percent", "/bucket/자료 +%", "/bucket/%EC%9E%90%EB%A3%8C%20%2B%25"},
		{"s3 path is not normalized", "/bucket/a//b/./../c", "/bucket/a//b/./../c"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if actual := s3CanonicalURI(test.path); actual != test.want {
				t.Fatalf("AWS4 canonical URI=%q, want %q", actual, test.want)
			}
		})
	}
}

func TestS3FixtureSigningCanonicalizesLiteralAndEncodedTenantSeparator(t *testing.T) {
	client := s3HTTPClient{accessKey: "fixture-access", secretKey: "fixture-secret", region: "us-east-1"}
	instant := time.Date(2026, 10, 3, 23, 30, 0, 0, time.UTC)
	const canonical = "/system_beta%3Atc-system-tenant-output/system/selected"
	var authorization string
	for _, wirePath := range []string{"/system_beta:tc-system-tenant-output/system/selected", canonical} {
		request, err := http.NewRequest(http.MethodGet, "http://rgw.invalid"+wirePath, nil)
		if err != nil {
			t.Fatal(err)
		}
		if request.URL.EscapedPath() != wirePath {
			t.Fatal("regression did not preserve distinct literal and encoded wire paths")
		}
		if actual := s3CanonicalURI(request.URL.Path); actual != canonical {
			t.Fatalf("tenant canonical URI=%q, want %q; do not double-encode the separator", actual, canonical)
		}
		client.sign(request, nil, instant)
		if request.Header.Get("X-Amz-Date") != "20261003T233000Z" {
			t.Fatal("tenant signature did not use the fixed UTC instant")
		}
		if authorization == "" {
			authorization = request.Header.Get("Authorization")
		} else if request.Header.Get("Authorization") != authorization {
			t.Fatal("equivalent literal colon and %3A paths produced different AWS4 signatures")
		}
	}
}
