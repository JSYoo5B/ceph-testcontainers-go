# RGW PUT with an empty server-side encryption header

Owner: RGW. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRGWEmptySSEHeader`, optional batch `rgw_empty_sse_header`.

## Use case and native contract

A client builds its PUT headers from configuration and sends
`x-amz-server-side-encryption` with an empty value when no algorithm is
configured. S3 treats a present header as a request for server-side
encryption, so an empty value is an invalid algorithm and the PUT is refused:

| Subtest | Header | S3 semantics |
| --- | --- | --- |
| control (before the subtests) | `AES-INVALID` | 400 `InvalidArgument`, no object |
| `omitted` | absent | 200, an unencrypted object with the exact bytes |
| `empty-new-key` | present and empty | 400 `InvalidArgument` with the control's message, no object |
| `empty-overwrite` | present and empty, on an existing key of a versioned bucket | 400 `InvalidArgument`, the seed version stays the only version with its bytes |

The defect: `rgw_s3_prepare_encrypt` in Tentacle 20.2.4 enters its
`x-amz-server-side-encryption` branch only when the header's value is not
empty, so an empty header is handled as if it were absent and the PUT writes
an unencrypted object. Tentacle 20.2.5 tests whether the header is present
(`CryptAttributes::exists`), so the empty value reaches the algorithm check
and is refused like any invalid algorithm. Umbrella 21.2.0 has the same code
as 20.2.5. This is a malformed-input difference: nothing is stored
unencrypted that the request asked to encrypt with a valid algorithm.

Upstream references:

- Release note of [Tentacle 20.2.5](https://docs.ceph.com/en/latest/releases/tentacle/)
  (empty `x-amz-server-side-encryption` header).
- `rgw_s3_prepare_encrypt` in
  [`src/rgw/rgw_crypt.cc`](https://github.com/ceph/ceph/blob/v20.2.5/src/rgw/rgw_crypt.cc);
  s3-tests `test_sse_empty_algorithm` in
  `src/test/rgw/s3-tests/s3tests/functional/test_s3.py` expects 400
  `InvalidArgument`.

## Requirements

- One cluster with one OSD and one gateway (`rgw.Start` with its generated
  user), plain HTTP: the algorithm check runs before any secure-transport
  check. Every container runs the package's `image`, by default the Tentacle
20.2.4 aggregate.
  Bridge networking. No KMS or SSE-S3 backend.
- Requests are signed by the test's own SigV4 client
  (`harness.RGWBackendSigned`), which signs every `x-amz-*` header it sends.
- The test bounds itself to 10 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Known-defect reproducer: the test passes whether or not the defect occurs and
prints the result; only a failure of its given or when steps fails it. It
stays an optional batch, so no automatic run includes it.

- Manual: `gh workflow run Experiments -f case=rgw_empty_sse_header`
  requires `REPRODUCED`; adding
  `-f image=quay.io/ceph/ceph:v20.2.5`
  requires `NOT_REPRODUCED`.
- Comment: `/ci run rgw_empty_sse_header` runs the batch and takes only its
  pass or failure.

## Running it

On the default image, Tentacle 20.2.4, which reproduces the defect:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  ./internal/integration/experiments/rgw_empty_sse_header
```

On another image, for example the fixed Tentacle 20.2.5:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_empty_sse_header_test.image=quay.io/ceph/ceph:v20.2.5" \
  ./internal/integration/experiments/rgw_empty_sse_header
```

The log names the image and Ceph version (`RGW_SSE image=`, `RGW_SSE ceph=`
and the daemon versions), every PUT's header, status, error code and message,
and what a fresh GET found.

## Observations and controls

1. Wire condition: before any gateway request, the same signer and transport
   send the empty-header PUT to a local listener, which must receive the
   header with an empty value and find it in `SignedHeaders`. A client that
   drops empty headers cannot reproduce anything. On the gateway, a refusal
   by SigV4 would be 403, not 400.
2. Condition: `GET ?encryption` answers 404
   `ServerSideEncryptionConfigurationNotFoundError` on both buckets, so no
   bucket default can explain a result.
3. Control: an invalid nonempty algorithm is refused with 400
   `InvalidArgument` and creates no object. Its message is the validation
   refusal the empty header must get too.
4. Effect: after each PUT a fresh GET reads the key. An accepted empty-header
   PUT is recorded with whether the bytes match and the encryption header
   GET returns, before the cleanup removes it. On the versioned bucket the
   seed's version ID, ETag and bytes and the version listing must be
   unchanged by a refused overwrite.

## Reading the result

The test ends with one line:
`EXPERIMENT_RESULT case=rgw_empty_sse_header result=<REPRODUCED|NOT_REPRODUCED> image=<image> symptoms=[...]`.

- **REPRODUCED:** an empty-header PUT is not refused with the control's 400
  `InvalidArgument`, or it changes what is stored: `empty-new-key` stores an
  unencrypted object, `empty-overwrite` adds a version.
- **NOT_REPRODUCED:** both PUTs get the control's refusal and nothing changes.

Native results on a Linux amd64 host, one run each:

| Image | Result | Time |
| --- | --- | --- |
| Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a…` (the default image when recorded) | REPRODUCED: both PUTs answered 200, a new object and a new version | 31 s |
| Tentacle 20.2.5, `quay.io/ceph/ceph:v20.2.5@sha256:4337ac8a…` | NOT_REPRODUCED | 30 s |

Earlier runs on split role images recorded the same behaviour on 20.2.4, and
no reproduction on Umbrella 21.2.0.

## Cleanup and limits

The cleanup is registered before the first bucket is created and receives
only the buckets whose creation answered 200. Every key a PUT may have
created is recorded before the request. The cleanup deletes every listed
version of a recorded key, then the buckets, and
reports a failure as `RGW_SSE cleanup failed` separately from the result.
The cluster is removed by its cleanup. Not run: arm64, the empty header on
`CopyObject`, `CreateMultipartUpload` and `PostObject`, an empty
`x-amz-server-side-encryption-customer-algorithm`, and a valid `AES256` or
`aws:kms` request (it needs an SSE-S3 or KMS backend).
