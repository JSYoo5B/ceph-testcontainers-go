# RGW SSE-C multipart completion without the customer key

Owner: RGW. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRGWSSECMultipartCompletion`, optional batch `rgw_ssec_completion`.
Pure tests of its completion classifier and verdict judgement:
`TestSSECClassifyCompletion`,
`TestSSECClassifyCompletionRejectsUnclassified200` and
`TestJudgeOmittedKeyCompletion` (batch `pure`).

## Use case and native contract

An application uploads an object with SSE-C (customer-provided keys) as a
multipart upload. S3 needs the customer key on the initiation, on every
`UploadPart` and on every later GET, but not on `CompleteMultipartUpload`,
unless the upload declared an additional checksum algorithm: then the
completion needs the key, because the gateway reads the encrypted part
checksums. A key that the completion does send is always validated.

| Subtest | Upload | Completion headers | S3 semantics |
| --- | --- | --- | --- |
| `unencrypted-control` | no SSE, no checksum | none | 200, exact bytes |
| `no-checksum-correct-key` | SSE-C, no checksum | the upload's key | 200, exact bytes with the key |
| `no-checksum-wrong-key` | SSE-C, no checksum | another valid key | 400 `InvalidArgument` (key MD5 mismatch), no object |
| `no-checksum-omitted-key` | SSE-C, no checksum | none | 200, exact bytes with the key |
| `sha256-omitted-key` | SSE-C, `SHA256` | none | 400 `InvalidArgument`, no object |
| `sha256-correct-key` | SSE-C, `SHA256` | the upload's key | 200 with the composite SHA-256, exact bytes with the key |

The defect: Tentacle 20.2.4 prepares decryption from the upload's metadata
while it parses the parameters of every `CompleteMultipartUpload` of an SSE-C
upload, so a completion without the customer-key headers is refused with 400
`InvalidArgument` ("must provide a valid encryption algorithm") whatever the
checksum algorithm. Tentacle 20.2.5 moves the check after the upload is
loaded and skips it only when the stored mode is SSE-C, the upload has no
checksum algorithm and the request sends no customer-key header; otherwise it
still validates the key. Umbrella 21.2.0 has the same code as 20.2.5.

Upstream references:

- Release note of [Tentacle 20.2.5](https://docs.ceph.com/en/latest/releases/tentacle/)
  (completion of SSE-C multipart uploads without the key).
- `RGWCompleteMultipart_ObjStore_S3::get_params` (v20.2.4) and
  `RGWCompleteMultipart_ObjStore_S3::verify_encryption` (v20.2.5) in
  [`src/rgw/rgw_rest_s3.cc`](https://github.com/ceph/ceph/blob/v20.2.5/src/rgw/rgw_rest_s3.cc);
  s3-tests in `src/test/rgw/s3-tests/s3tests/functional/test_s3.py` cover the
  ordinary and the checksummed completion.

## Requirements

- One cluster with one OSD and one gateway (`rgw.Start` with its generated
  user) with a native HTTPS listener (`rgw.Config.TLS`, a certificate of a
  test CA). Image roles `essential`, `client` and `rgw`, all of one release.
  Bridge networking.
- `rgw_crypt_require_ssl` keeps its default (true), so every request goes over
  HTTPS verified against the test CA. Requests are signed by the test's own
  SigV4 client (`harness.RGWBackendSigned`); it sends no optional checksum of
  its own, and `x-amz-content-sha256` is only the SigV4 payload hash.
- The test bounds itself to 10 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Unpatched-defect reproducer: it reports `REPRODUCED` on the
published Tentacle 20.2.4 images, the pull request default. On images its
catalogue lists the run passes only with the listed verdict, and on other
images a reproduction fails it. No automatic run includes it (the default
checks of a pull request, pushes to `main`, periodic samples), and it never
gates anything. A maintainer can run it on demand, also on a pull request's
head with the comment below.

- Manual: `gh workflow run "Native regressions" -f batch=rgw_ssec_completion -f release=20.2.4`
  for the affected build, `-f release=21.2.0` for the fixed one.
- Comment: `/ci run rgw_ssec_completion` or
  `/ci run rgw_ssec_completion on umbrella`.

## Running it

Export the images of one release first, as
[Images for the RGW S3 cases](../../../../docs/contributing/testing.md#images-for-the-rgw-s3-cases)
describes, then:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRGWSSECMultipartCompletion$' \
  ./internal/integration/experiments/rgw_ssec_multipart_completion
```

Or run the batch exactly as CI does:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch rgw_ssec_completion \
  --package ./internal/integration/experiments/rgw_ssec_multipart_completion \
  --directory "artifacts/rgw-ssec-completion-$(date -u +%Y%m%dT%H%M%SZ)"
```

The runner refuses an existing `--directory`, so the timestamp gives each
run, such as each release of a comparison, its own.

Record the image digests with the result; the log names the Ceph version
(`RGW_SSEC ceph=` and the daemon versions), every upload ID with its stored
checksum algorithm, and each completion's HTTP status, whether a 200 carried
an S3 error, and the error code and message.
Customer keys are never logged.

## Observations and controls

1. Control `unencrypted-control`: an unencrypted multipart upload completes
   without any SSE header and reads back its exact bytes.
2. Condition, for every SSE-C upload: the initiation answers with the SSE-C
   algorithm header, the part answers 200, and `ListParts` reports the
   checksum algorithm the case declared (`SHA256`, or none); a `SHA256` part
   answers with the checksum it was sent.
3. Effect: after a 200 completion, a fresh GET without the key is refused
   with 400, and a fresh GET with the key returns the exact bytes and the
   SSE-C algorithm header. The completion is classified by its XML root: a
   200 whose root is `Error`, in any namespace, is a failed completion that
   keeps its HTTP status, error code and message, and a 200 that is neither
   `CompleteMultipartUploadResult` nor `Error` is another fault, reported
   after its upload is aborted. After a refused completion, HEAD with the key answers
   404, and the upload is aborted at once, so no failed completion is reused.
4. Key validation: the wrong key is a valid 256-bit key with a correct MD5,
   and its refusal must name the MD5 mismatch, so it cannot come from
   signing, TLS or malformed XML.
5. Checksums: the `SHA256` completion with the key answers the composite
   SHA-256 of the part (`<digest>-1`).

## Verdict

The test reports one reproduction unit, `rgw_ssec_multipart_completion`,
with one planned observation, `no-checksum-omitted-key`. The other five
subtests are controls and run first: `unencrypted-control` and
`no-checksum-correct-key` show that completion works, and
`no-checksum-wrong-key`, `sha256-omitted-key` and `sha256-correct-key` show
that the gateway still validates the key where S3 needs it, so a completion
that succeeds without the key is not explained by a gateway that ignores
customer keys. `go test -v` prints one
`REPRODUCTION_VERDICT case=rgw_ssec_multipart_completion ...` line with the
verdict, the decisive observation and the resolved images.

- `REPRODUCED`: the conditions and every control hold, and the completion
  without the key is refused with 400 `InvalidArgument` whose message asks
  for a valid encryption algorithm, leaving no object
  (`judgeOmittedKeyCompletion`).
- `NOT_REPRODUCED`: the conditions and every control hold, and the
  completion without the key answers 200 and the object reads back only with
  the key.
- `INCONCLUSIVE`: a condition is not established (step 2), a control failed
  (any of the five control subtests), or another fault (the completion
  answers neither 200 nor that refusal, an embedded error, an object after
  the refusal, or a completed object that is not encrypted at rest). The line
  names the reason.

The expected verdict comes from [`reproductions.json`](reproductions.json)
beside the test. The combination whose platform and images equal the ones
the run resolves selects it, and only that verdict passes; a skip or
`INCONCLUSIVE` never does. It lists `20.2.4-roles` (`REPRODUCED`) and
`21.2.0-roles` (`NOT_REPRODUCED`), both on linux/amd64. On any other images
the run is an investigation (`expected=none combination=none`) and keeps its
pass/fail meaning: a reproduction fails `no-checksum-omitted-key`, an absent
symptom passes, and an inconclusive run fails. The command takes no
expectation; with the role images of 20.2.4 or 21.2.0 exported, it checks
the affected or the fixed build, and only the exported image variables
differ between the two runs:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch rgw_ssec_completion \
  --package ./internal/integration/experiments/rgw_ssec_multipart_completion \
  --directory "artifacts/rgw-ssec-completion-$(date -u +%Y%m%dT%H%M%SZ)"
```

The verdicts below ran on the pinned images of the shared recipe: the 20.2.4
role images (essential `3c7a6d0ae445…`, client `56b5c03d450c…`, rgw
`aac25fc48b53…`), the 20.2.5 aggregate `sha256:4337ac8ac81d…`, the 21.2.0
role images (essential `119180026952…`, client `e5f7edab88b6…`, rgw
`9ef3fb34f7c4…`) and the 19.2.5 role images (essential `43ee23624e7a…`,
client `a69e1319ce88…`, rgw `1fbe1bd4ecc0…`).

| Release | Verdict | Observation |
| --- | --- | --- |
| Tentacle 20.2.4 | `REPRODUCED` | `no-checksum-omitted-key` is refused with 400 `InvalidArgument` "Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm." and every control passes |
| Tentacle 20.2.5, Umbrella 21.2.0 | `NOT_REPRODUCED` | every subtest follows the table |
| Squid 19.2.5 | `INCONCLUSIVE` | control failed: `no-checksum-wrong-key` completes with 200 (the completion validates no key), and a `SHA256` part answers without its checksum, so the `sha256` controls stop with `condition not established` |

The Tentacle 20.2.4 and Umbrella 21.2.0 verdicts are those of the
`20.2.4-roles` and `21.2.0-roles` combinations, observed with their
expectations; the other images are no listed combination, and their verdicts
are the pass or failure they gave.

## Cleanup and limits

The cleanup is registered before the bucket is created and acts only when the
creation answered 200. Every upload ID is recorded right after the
initiation; refused uploads are aborted at once, and the cleanup aborts any
upload still recorded, deletes the completed objects and the bucket, and reports a failure as `RGW_SSEC cleanup
failed` separately from the result. The cluster is removed by its cleanup.
Not run: arm64, uploads of several parts, checksum algorithms other than
`SHA256`, the `FULL_OBJECT` checksum type, SSE-KMS and SSE-S3 uploads, and
`UploadPartCopy`.
