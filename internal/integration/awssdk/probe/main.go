// This independent consumer fixture plays the system under test with the AWS
// SDK for Go. The parent testcontainers module imports no S3 SDK; an
// integration test builds this module and runs the binary with the
// credentials it hands off.
//
// Each run is one fresh client process: it reads the endpoint, region and
// object from flags and the key pair from AWS_ACCESS_KEY_ID and
// AWS_SECRET_ACCESS_KEY, so no key appears on a command line. It performs one
// operation (create-bucket, put or get) and prints one JSON line: whether RGW
// accepted the credentials, the SHA-256 of the object it read or wrote, and
// the version of the S3 SDK module it was built with. It exits nonzero only
// when the operation failed for another reason than refused credentials or an
// untrusted certificate; its messages never include the keys.
//
// With -ca-file, an https endpoint is trusted only through the PEM
// certificates in that file, as an application configured with a private CA
// trusts it: the system roots are not used, and certificate and host name
// verification stay enabled. A certificate that the file does not verify is
// reported as the outcome untrusted, apart from a refusal by RGW.
//
// With -role-arn, the probe is an STS consumer: it first calls AssumeRole for
// that role with the key pair and reports whether STS issued temporary
// credentials and the assumed-role ARN; when it did, it performs the
// operation with the temporary credentials and their session token, which it
// never prints.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime/debug"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
)

type result struct {
	Operation string `json:"operation"`
	// Role, with -role-arn, is assumed when STS issued temporary credentials
	// for the role and refused when it refused the AssumeRole request;
	// AssumedARN is the assumed-role identity STS returned.
	Role       string `json:"role,omitempty"`
	AssumedARN string `json:"assumed_arn,omitempty"`
	// Outcome is accepted when RGW accepted the credentials, refused when it
	// refused them, and untrusted when the -ca-file certificates do not
	// verify the endpoint's certificate. It is empty when the role was
	// refused.
	Outcome string `json:"outcome,omitempty"`
	SHA256  string `json:"sha256,omitempty"`
	// SDK is the S3 SDK module and version that the probe was built with.
	SDK string `json:"sdk"`
}

// refusedCodes are the S3 error codes of a request whose credentials RGW
// does not accept.
var refusedCodes = map[string]bool{"InvalidAccessKeyId": true, "SignatureDoesNotMatch": true, "AccessDenied": true}

// assumeRefusedCodes are the STS error codes of an AssumeRole that RGW
// refuses: refused credentials, a principal the trust policy does not allow,
// or a role that does not exist.
var assumeRefusedCodes = map[string]bool{"InvalidAccessKeyId": true, "SignatureDoesNotMatch": true, "AccessDenied": true, "NoSuchEntity": true}

func main() {
	var endpoint, region, bucket, object, operation, data, roleARN, caFile string
	flag.StringVar(&endpoint, "endpoint", "", "S3 endpoint URL")
	flag.StringVar(&region, "region", "", "S3 region")
	flag.StringVar(&bucket, "bucket", "", "existing bucket")
	flag.StringVar(&object, "object", "", "object key")
	flag.StringVar(&operation, "operation", "", "create-bucket, get or put")
	flag.StringVar(&data, "data", "", "object content that put writes")
	flag.StringVar(&roleARN, "role-arn", "", "role to assume with STS before the operation")
	flag.StringVar(&caFile, "ca-file", "", "PEM certificates that alone verify an https endpoint")
	flag.Parse()
	access, secret := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	if endpoint == "" || region == "" || bucket == "" || (object == "" && operation != "create-bucket") || access == "" || secret == "" || (operation != "get" && operation != "put" && operation != "create-bucket") {
		fmt.Fprintln(os.Stderr, "s3 probe: -endpoint, -region, -bucket, -object, -operation create-bucket|get|put and the AWS key environment are required")
		os.Exit(2)
	}
	transport, err := newHTTPClient(caFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "s3 probe: %v\n", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	out := result{Operation: operation, SDK: sdkVersion()}
	token := ""
	if roleARN != "" {
		assumed, err := sts.New(sts.Options{
			BaseEndpoint:     aws.String(endpoint),
			Region:           region,
			Credentials:      credentials.NewStaticCredentialsProvider(access, secret, ""),
			RetryMaxAttempts: 1,
			HTTPClient:       transport,
		}).AssumeRole(ctx, &sts.AssumeRoleInput{RoleArn: aws.String(roleARN), RoleSessionName: aws.String("tc-probe"), DurationSeconds: aws.Int32(900)})
		var apiErr smithy.APIError
		switch {
		case err == nil && assumed.Credentials != nil && assumed.AssumedRoleUser != nil:
			out.Role, out.AssumedARN = "assumed", aws.ToString(assumed.AssumedRoleUser.Arn)
			access, secret, token = aws.ToString(assumed.Credentials.AccessKeyId), aws.ToString(assumed.Credentials.SecretAccessKey), aws.ToString(assumed.Credentials.SessionToken)
			if access == "" || secret == "" || token == "" {
				fmt.Fprintln(os.Stderr, "s3 probe: AssumeRole answered without temporary credentials")
				os.Exit(1)
			}
		case errors.As(err, &apiErr) && assumeRefusedCodes[apiErr.ErrorCode()]:
			out.Role = "refused"
			if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
				os.Exit(1)
			}
			return
		case untrusted(err):
			out.Outcome = "untrusted"
			if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
				os.Exit(1)
			}
			return
		default:
			code := "none"
			if errors.As(err, &apiErr) {
				code = apiErr.ErrorCode()
			}
			fmt.Fprintf(os.Stderr, "s3 probe: AssumeRole failed (STS error code %s)\n", code)
			os.Exit(1)
		}
	}
	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(access, secret, token),
		UsePathStyle: true,
		HTTPClient:   transport,
		// One attempt: the caller polls, and a refused request is not retried.
		RetryMaxAttempts: 1,
		// Checksums only where S3 requires them, so the probe does not depend
		// on the gateway release's flexible checksum support.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	out.Outcome = "accepted"
	var content []byte
	switch operation {
	case "create-bucket":
		_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	case "get":
		var response *s3.GetObjectOutput
		response, err = client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(object)})
		if err == nil {
			content, err = io.ReadAll(response.Body)
			_ = response.Body.Close()
		}
	case "put":
		content = []byte(data)
		_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(object), Body: bytes.NewReader(content)})
	}
	var apiErr smithy.APIError
	switch {
	case err == nil && operation == "create-bucket":
	case err == nil:
		sum := sha256.Sum256(content)
		out.SHA256 = hex.EncodeToString(sum[:])
	case errors.As(err, &apiErr) && refusedCodes[apiErr.ErrorCode()]:
		out.Outcome = "refused"
	case untrusted(err):
		out.Outcome = "untrusted"
	default:
		code := "none"
		if errors.As(err, &apiErr) {
			code = apiErr.ErrorCode()
		}
		fmt.Fprintf(os.Stderr, "s3 probe: %s failed (S3 error code %s)\n", operation, code)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		os.Exit(1)
	}
}

// httpClient is the HTTP client option of the S3 and STS clients.
type httpClient interface {
	Do(*http.Request) (*http.Response, error)
}

// newHTTPClient returns the HTTP client of the SDK clients: nil, the SDK
// default, without caFile, otherwise a client that trusts only the
// certificates in caFile.
func newHTTPClient(caFile string) (httpClient, error) {
	if caFile == "" {
		return nil, nil
	}
	data, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read -ca-file: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, errors.New("-ca-file holds no PEM certificate")
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}, nil
}

// untrusted reports a request that failed because the endpoint's certificate
// did not verify.
func untrusted(err error) bool {
	var verification *tls.CertificateVerificationError
	var authority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	return errors.As(err, &verification) || errors.As(err, &authority) || errors.As(err, &hostname)
}

// sdkVersion names the S3 SDK module and the version the binary was built
// with.
func sdkVersion() string {
	const module = "github.com/aws/aws-sdk-go-v2/service/s3"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dependency := range info.Deps {
			if dependency.Path == module {
				return module + "@" + dependency.Version
			}
		}
	}
	return module + "@unknown"
}
