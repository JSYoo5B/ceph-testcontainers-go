// This independent consumer fixture plays the system under test with the AWS
// SDK for Go. The parent testcontainers module imports no S3 SDK; an
// integration test builds this module and runs the binary with the
// credentials it hands off.
//
// Each run is one fresh client process: it reads the endpoint, region and
// object from flags and the key pair from AWS_ACCESS_KEY_ID and
// AWS_SECRET_ACCESS_KEY, so no key appears on a command line. It performs one
// operation and prints one JSON line: whether RGW accepted the credentials,
// and the SHA-256 of the object it read or wrote. It exits nonzero only when
// the operation failed for another reason than refused credentials; its
// messages never include the keys.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type result struct {
	Operation string `json:"operation"`
	// Outcome is accepted when RGW accepted the credentials and refused when
	// it refused them.
	Outcome string `json:"outcome"`
	SHA256  string `json:"sha256,omitempty"`
}

// refusedCodes are the S3 error codes of a request whose credentials RGW
// does not accept.
var refusedCodes = map[string]bool{"InvalidAccessKeyId": true, "SignatureDoesNotMatch": true, "AccessDenied": true}

func main() {
	var endpoint, region, bucket, object, operation, data string
	flag.StringVar(&endpoint, "endpoint", "", "S3 endpoint URL")
	flag.StringVar(&region, "region", "", "S3 region")
	flag.StringVar(&bucket, "bucket", "", "existing bucket")
	flag.StringVar(&object, "object", "", "object key")
	flag.StringVar(&operation, "operation", "", "get or put")
	flag.StringVar(&data, "data", "", "object content that put writes")
	flag.Parse()
	access, secret := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	if endpoint == "" || region == "" || bucket == "" || object == "" || access == "" || secret == "" || (operation != "get" && operation != "put") {
		fmt.Fprintln(os.Stderr, "s3 probe: -endpoint, -region, -bucket, -object, -operation get|put and the AWS key environment are required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(access, secret, ""),
		UsePathStyle: true,
		// One attempt: the caller polls, and a refused request is not retried.
		RetryMaxAttempts: 1,
		// Checksums only where S3 requires them, so the probe does not depend
		// on the gateway release's flexible checksum support.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	out := result{Operation: operation, Outcome: "accepted"}
	var content []byte
	var err error
	switch operation {
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
	case err == nil:
		sum := sha256.Sum256(content)
		out.SHA256 = hex.EncodeToString(sum[:])
	case errors.As(err, &apiErr) && refusedCodes[apiErr.ErrorCode()]:
		out.Outcome = "refused"
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
