# RGW behaviour when its external KMS fails

Owner: RGW. Kind: cross-build characterization of native RGW behaviour.
Test: `TestRGWKMSFailureModes` (bridge), batch `recovery/rgw_kms_failure_modes`.
The recipe itself (options, per-gateway token and CA files, verified HTTPS on
both legs, a gateway with an unrelated CA) stays in `internal/integration/rgw`
as `TestRGWExternalKMS`, because the `rgw` README documents it for consumers.

## Use case and native contract

The scenario (`harness.NewRGWExternalKMS`) starts a test-owned Vault with dev
TLS, configures SSE-KMS (KV v2) and SSE-S3 (Transit) for every gateway, and
writes one SSE-KMS object. The subtests record what RGW does when the KMS
side fails:

| Subtest | Observed contract |
| --- | --- |
| `token-replacement` | After Vault revokes the token, an encrypted GET fails with 403 `AccessDenied`. Writing a new token next to the file and renaming it over the same path recovers the running gateway. |
| `kms-network-cut` | With Vault disconnected (`fault.InterruptNetwork`), an encrypted GET fails with 500 once libcurl's low-speed limit aborts the kept connection (`rgw_curl_low_speed_time` is lowered to 5 s); plain objects and listings still work. After `Restore` the object reads its exact bytes again at the same address; Vault's host port can change. |
| `sse-s3` | RGW creates one Transit key per bucket, named by the bucket ID. A bucket DELETE answers 204 whatever happens to the key: with deletion refused the key stays and the audit records no further delete attempt for five seconds after permission is restored. On Squid 19.2.5 the key stays even with permission (RGW logs `Unable to remove KEK ID ... got -22`); Tentacle 20.2.4 and Umbrella 21.2.0 delete it. |

The `sse-s3` subtest does not branch on the release: it reads the outcome from
Vault's audit log and the key state from Vault, and accepts only the two
consistent outcomes. No upstream issue is recorded for the Squid difference.

## Requirements

- One cluster with one OSD, two gateways (`kms-trusted`, `kms-wrong-ca`) with
  verified HTTPS, and Vault (`CEPH_TEST_VAULT_IMAGE`, default
  `hashicorp/vault:1.21.4`) on the cluster bridge. Image roles `essential`,
  `client` and `rgw`. Bridge networking only: Docker cannot disconnect a
  host-network endpoint.
- The test bounds itself to 20 minutes; the batch timeout is 30 minutes.

## Tier and triggers

Deterministic; the cut is bounded by the lowered low-speed limit. One run
takes about a minute on 20.2.4.

- Pull request: only when a file of this directory changes, or with the
  `ci:full` label (a required `recovery` batch).
- Comment: `/ci run rgw_kms_failure_modes [on squid|umbrella]`.
- Periodic: `rotate` can draw it on the supported releases.

## Running it

```sh
CGO_ENABLED=0 go test -tags all -count=1 -v -timeout=30m \
  -run '^TestRGWKMSFailureModes$' \
  ./internal/integration/experiments/rgw_kms_failure_modes
```

or exactly as CI runs the batch:

```sh
python3 .github/scripts/tag_scenarios.py run --category recovery \
  --batch rgw_kms_failure_modes \
  --package ./internal/integration/experiments/rgw_kms_failure_modes \
  --directory "artifacts/rgw-kms-failure-modes-$(date -u +%Y%m%dT%H%M%SZ)"
```

The runner refuses an existing `--directory`, so the timestamp gives each
run, such as each release of a comparison, its own.

The log records the Vault image, the native status codes and the audited
key outcomes.

## Observations and controls

- Token: Vault's own token lookup refuses the revoked token before the GET is
  judged, and the object's listing identity is unchanged.
- Cut: Vault is confirmed detached from the cluster bridge; plain reads on
  both gateways are the KMS-independent control; the reattached address must
  equal the original.
- SSE-S3: an unrelated control key must survive every step; the audit log
  separates a configured, a completed and a refused deletion.

## Reading the result

- **Pass:** every subtest observed one of the contracts above.
- **Observed difference:** another status, an outcome outside the two
  accepted SSE-S3 combinations, or a cut that does not fail.
- **Not reproduced:** not applicable; the Squid key outcome is accepted as
  recorded behaviour, never as success of cleanup.
- **Unavailable prerequisite:** Vault, its dev-TLS CA or a gateway does not
  start.
- **Condition not established:** the network cut does not detach Vault, or
  the token is still known to Vault.

## Cleanup and limits

The test removes orphan Transit keys as the KMS operator, its objects, bucket
and user; overrides are restored and the cluster, gateways and Vault are
terminated by their cleanups. Key rotation, an outage during a write,
ciphertext inspection in RADOS and arm64 were not run.
