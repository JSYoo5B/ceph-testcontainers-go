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
  check. Image roles `essential`, `client` and `rgw`, all of one release.
  Bridge networking. No KMS or SSE-S3 backend.
- Requests are signed by the test's own SigV4 client
  (`harness.RGWBackendSigned`), which signs every `x-amz-*` header it sends.
- The test bounds itself to 10 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Deliberately failing reproducer: it fails on the published Tentacle 20.2.4
images, the pull request default. It never runs on a pull request, on `main`
or in periodic samples, and never gates anything.

- Manual: `gh workflow run "Native regressions" -f batch=rgw_empty_sse_header`.
- Comment: `/ci run rgw_empty_sse_header` or
  `/ci run rgw_empty_sse_header on umbrella`.

## Running it

```sh
CEPH_TEST_ESSENTIAL_IMAGE=<essential role@sha256:...> \
CEPH_TEST_CLIENT_IMAGE=<client role@sha256:...> \
CEPH_TEST_RGW_IMAGE=<rgw role@sha256:...> \
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRGWEmptySSEHeader$' \
  ./internal/integration/experiments/rgw_empty_sse_header
```

Resolve the role images with `run_image_matrix.py --variant official
--layout roles --release <release> --roles essential,client,rgw
--prepare-only` (see [Testing](../../../../docs/contributing/testing.md)).
Tentacle 20.2.5 has no published role images; run it with
`CEPH_TEST_IMAGE=quay.io/ceph/ceph:v20.2.5@sha256:<index digest>` instead of
the role variables. Or run the batch exactly as CI does:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch rgw_empty_sse_header \
  --package ./internal/integration/experiments/rgw_empty_sse_header \
  --directory artifacts/rgw-empty-sse-header
```

Record the image digests with the result; the log names the Ceph version
(`RGW_SSE ceph=` and the daemon versions) and every PUT's header, status,
error code and message, and what a fresh GET found.

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

- **Pass:** every step followed the table. Tentacle 20.2.5 and Umbrella
  21.2.0 pass.
- **Observed bug:** `empty-new-key` answers 200 and stores the exact bytes
  unencrypted, and `empty-overwrite` answers 200 and adds a second version.
  The control and `omitted` pass. Tentacle 20.2.4 does this.
- **Not reproduced:** a pass on a build claimed to be affected. Check that
  the wire condition was logged.
- **Unavailable prerequisite:** the cluster or the gateway does not start, or
  the invalid-algorithm control is not refused with 400 `InvalidArgument`.
  Squid 19.2.5 refuses the invalid algorithm with 400 `InvalidRequest` and no
  message, so the case stops before the subtests there.
- **Condition not established:** the client did not send and sign the empty
  header, a bucket has a default encryption configuration, or the seed write
  has no version ID. The test fails with `condition not established`.

## Cleanup and limits

Every key a PUT may have created is recorded before the request. The cleanup
deletes every listed version of a recorded key, then the buckets, and
reports a failure as `RGW_SSE cleanup failed` separately from the result.
The cluster is removed by its cleanup. Not run: arm64, the empty header on
`CopyObject`, `CreateMultipartUpload` and `PostObject`, an empty
`x-amz-server-side-encryption-customer-algorithm`, and a valid `AES256` or
`aws:kms` request (it needs an SSE-S3 or KMS backend).
