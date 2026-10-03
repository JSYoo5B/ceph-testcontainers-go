# STS·Swift·외부 KMS를 위한 RGW fixture

RGW는 S3 외에도 STS·Swift endpoint와 외부 KMS 연결을 제공합니다. 이 경로의 준비는 `Run` → `TemporaryConfig` → `StartRGWWithConfig` → `CreateUser`와 scope를 보존하는 `gateway.Admin`으로 조합합니다. protocol CRUD를 새 Go wrapper로 추가하지 않습니다. [실행 가능한 조합과 실제 HTTP client probe](../internal/integration/rgw_protocol_backends_integration_test.go)의 selector는 `TestRGWProtocolBackends`입니다.

테스트는 같은 cluster/zone의 gateway 두 개를 bridge와 host networking에서 각각 실행합니다. STS session-token CryptoKey와 KMS 설정은 **gateway를 시작하기 전에** `client.admin` section에 적용합니다. 현재 bootstrap의 RGW daemon identity가 `client.admin`이기 때문입니다. `TemporaryConfig`의 exact stored-entry readback은 설정 DB의 증거이고, 아래 실제 credential·Vault 동작이 runtime 적용의 증거입니다. 설정은 각 override의 `Restore`로 복원하며, 실패한 partial fixture는 disposable cluster 종료로 정리합니다.

이 문서를 추가한 단계에서는 tag compile을 확인했습니다. 실제 Docker의 통과 여부는 [fixture 완료 기준](CLIENT_FIXTURE_COVERAGE.md)의 G09 상태로 관리합니다. backend/protocol의 모든 조합이 검증되었다는 뜻은 아닙니다.

## STS와 두 gateway의 credential 공유

`ceph-authtool --gen-print-key`로 생성한 CryptoKey를 `rgw_sts_key`에 저장하고 `rgw_s3_auth_use_sts=true`를 적용합니다. 같은 zone의 모든 gateway가 같은 키를 사용해야 다른 gateway에서도 session token을 해독할 수 있습니다. realm의 여러 zone까지 조합한다면 같은 키를 별도 cluster에도 명시적으로 준비해야 합니다. 이 테스트는 같은 cluster/zone의 두 gateway 범위입니다. [Ceph STS 설정](https://docs.ceph.com/en/tentacle/radosgw/STS/).

role은 이름이 이미 있으면 adopt하지 않습니다. `gateway.Admin("role", "list", ...)`로 확인하고 fresh `role create` 후 native ID·ARN을 캡처합니다. 이후 trust/permission 변경과 제거 전에 같은 ID인지 다시 확인합니다. API 인자는 shell interpolation 없이 각각 전달합니다.

```go
trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam:::user/ASSUMER"},"Action":"sts:AssumeRole"}]}`
permission := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::OWNED-BUCKET/allowed"}]}`
created, err := gateway.Admin(ctx, "role", "create",
    "--role-name", freshRoleName, "--path", "/fixture/",
    "--assume-role-policy-doc", trust, "--format", "json")
// Decode created's native id/arn; stop on error and retain the partial fixture.
_, err = gateway.Admin(ctx, "role-policy", "put",
    "--role-name", freshRoleName, "--policy-name", "read-owned",
    "--policy-doc", permission)
```

위 정책의 `ASSUMER`와 bucket 이름은 public `CreateUser` 및 consumer가 생성한 fresh resource의 이름으로 바꿉니다. 신뢰된 사용자만 SigV4 service `sts`로 `AssumeRole`을 호출합니다. 반환된 access key·secret·session token과 expiration을 확인하고 S3 요청에 `X-Amz-Security-Token`을 넣습니다. 모든 `x-amz-*` header를 서명합니다.

native probe는 gateway A에서 받은 credential로 A/B에서 정확한 object bytes를 읽습니다. bucket owner는 trust policy에서 제외하여 AssumeRole 거부를 확인하고, temporary credential의 write·다른 object read·session-token 없는 요청도 거부해야 합니다. 같은 role ID에 explicit deny permission을 적용해 기존 token의 read를 A/B에서 막고, trust deny로 새 AssumeRole도 막습니다. policy를 원래 값으로 복원하면 기존 token의 read와 bytes가 복구되어야 합니다. trust 변경이 발급된 token을 암호적으로 없앤다는 가정은 하지 않습니다. role과 정책의 native grammar는 [Ceph role 관리](https://docs.ceph.com/en/tentacle/radosgw/role/)를 따릅니다.

성공 후 owned object/bucket, role inline policy, role, 두 ordinary user를 제거합니다. S3 요청별로 role을 다시 읽는 동작은 [v20.2.4 STS auth source](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_rest_s3.cc)의 `STSEngine::authenticate`와 연결됩니다. OIDC/WebIdentity, 외부 Keycloak, LDAP/Keystone STSLite는 별도의 identity-provider fixture가 필요하며 이 AssumeRole probe에 포함되지 않습니다.

## Swift auth와 S3 공존

`rgw_enable_apis=s3,swift,swift_auth,sts,iam,admin`, `rgw_swift_account_in_url=true`를 gateway 시작 전에 준비합니다. `swift` prefix를 유지하므로 `/swift/v1/AUTH_<uid>`와 S3 endpoint가 공존합니다. prefix를 `/`로 바꾸는 방식은 S3와 같은 gateway에서 함께 사용할 수 없습니다. [native Swift 옵션](https://github.com/ceph/ceph/blob/v20.2.4/src/common/options/rgw.yaml.in).

ordinary user를 `CreateUser`로 만든 다음 fresh Swift subuser/key를 native CLI로 준비합니다.

```go
_, err := gateway.Admin(ctx, "subuser", "create",
    "--uid", user.ID(), "--subuser", user.ID()+":swift",
    "--access", "full", "--key-type", "swift", "--secret", freshSwiftKey)
```

`user info`에서 subuser ID·full-control permission·swift key가 정확히 일치하는지 확인합니다. `/auth`에 `X-Auth-User`/`X-Auth-Key`를 전달해 token과 서버가 반환한 `X-Storage-Url`의 account path를 얻습니다. Go orchestrator가 macOS에 있을 때 내부 bridge URL 전체를 그대로 쓰는 대신, 반환된 account path와 `S3Endpoint`가 제공한 외부 접근 주소를 조합합니다. [Swift auth v1](https://docs.ceph.com/en/tentacle/radosgw/swift/auth/).

probe는 wrong key·missing token을 거부하고, 같은 Swift token으로 두 gateway에서 object를 읽습니다. Swift PUT → S3 GET 및 S3 PUT → Swift GET이 같은 exact bytes를 반환해야 합니다. container/object를 제거하고 `subuser rm --purge-keys`로 Swift credential을 정리한 뒤 새 auth와 이미 받은 Swift token의 사용이 거부되는지 확인합니다. 원래 ordinary user의 S3 key는 계속 동작해야 합니다. 외부 Keystone catalog·auth와 Swift large-object/temp-URL의 모든 기능은 이 native auth/S3 coexistence 증거 범위 밖입니다.

## 실제 Vault KV-v2를 연결한 SSE-KMS

HTTP 모의 서버 대신 공식 `hashicorp/vault:1.21.4` image의 in-memory dev server를 별도 testcontainers container로 실행합니다. `CEPH_TEST_VAULT_IMAGE`로 호환 image/tag/digest를 주입할 수 있습니다. native client SDK나 Vault CLI를 Go host에 설치할 필요는 없습니다. 테스트가 Vault HTTP API로 fresh KV-v2 mount·두 256-bit base64 key·정확히 한 key path만 읽는 scoped token·file audit device를 준비합니다. RGW에는 root token을 전달하지 않습니다.

| `client.admin` 설정 | 값 / 준비 |
| --- | --- |
| `rgw_crypt_s3_kms_backend` | `vault` |
| `rgw_crypt_vault_auth` | `token` |
| `rgw_crypt_vault_token_file` | `/tc/vault-token`, 각 gateway에 새 reader로 복사한 mode `0600` 파일 |
| `rgw_crypt_vault_addr` | 같은 Linux Docker host/VM에서 reachable한 실제 Vault bridge IP와 8200 |
| `rgw_crypt_vault_prefix` | `/v1/tc/data` |
| `rgw_crypt_vault_secret_engine` | `kv` |
| `rgw_crypt_require_ssl` | 이 isolated HTTP recipe에서만 `false`; RGW TLS fixture는 별도로 제공 |

bridge Ceph에서는 Vault를 cluster의 public bridge에 연결합니다. host Ceph에서는 Vault가 자체 Docker bridge endpoint를 유지하고 host-network RGW가 그 IP로 접속합니다. Go orchestrator의 Vault 접근은 별도의 random mapped 8200 포트입니다. 고정 host port를 점유하거나 서로 다른 Ceph cluster에 bridge를 공유시키지 않습니다. 첫 pull에는 registry 연결이 필요합니다.

S3 PUT의 `x-amz-server-side-encryption=aws:kms`와 key ID `allowed`를 확인하고, 두 gateway에서 GET의 encryption header와 exact bytes를 검증합니다. Vault에는 존재하지만 RGW token policy가 제외한 `denied` key로 PUT하면 실패하고 object가 남지 않아야 합니다.

real Vault audit는 scoped token의 정확한 allowed/denied read path를 확인합니다. `request.id`로 request와 response를 연결하고, 서로 다른 completed response만 셉니다. allowed read는 top-level `error`가 없고 `response.data.data.key`가 있어야 합니다(HMAC 처리된 값도 유효). denied read는 permission-denied response여야 하며, audit에 `auth.policy_results.allowed`가 있으면 그 결정과도 일치해야 합니다. 요청 기록만 있거나 같은 응답을 중복한 로그는 성공 증거가 아닙니다. [Vault 1.21 audit schema](https://developer.hashicorp.com/vault/docs/v1.21.x/audit/schema).

실제 `allowed` key를 Vault에서 삭제하면 두 gateway의 object decrypt가 실패해야 합니다. 원래 key material을 새 KV version으로 복원하면 기존 object bytes가 다시 읽혀야 합니다. 그 후 object/bucket, 두 key의 모든 metadata/version, RGW의 scoped Vault token을 제거합니다. dev server/root token/audit는 owned container 종료와 함께 소멸합니다. 테스트는 Vault key·S3 key·STS/Swift token을 출력하지 않습니다.

이 경로는 production Vault HA·seal/unseal·agent renewal·Transit/KMIP/Barbican의 계약을 입증하지 않습니다. TLS/CA, transit key rotation 및 agent token refresh는 해당 backend별 추가 recipe가 필요합니다. [Ceph Vault integration](https://docs.ceph.com/en/tentacle/radosgw/vault/)과 [v20.2.4 native KMS 구현](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_kms.cc)을 따릅니다. Vault API의 준비/cleanup은 [KV-v2](https://developer.hashicorp.com/vault/api-docs/secret/kv/kv-v2), [token](https://developer.hashicorp.com/vault/api-docs/auth/token), [audit](https://developer.hashicorp.com/vault/api-docs/system/audit) 계약을 사용합니다.

## 실행

```sh
CEPH_TEST_IMAGE=ceph-testcontainers:20.2.4-control \
CEPH_TEST_OSD_IMAGE=ceph-testcontainers:20.2.4-osd \
CEPH_TEST_RGW_IMAGE=ceph-testcontainers:20.2.4-rgw \
CEPH_TEST_VAULT_IMAGE=hashicorp/vault:1.21.4 \
CGO_ENABLED=0 go test -mod=readonly -count=1 \
  -tags=integration,features ./internal/integration \
  -run '^TestRGWProtocolBackends$' -timeout 35m -v
```

Docker Desktop에서 host networking을 활성화한 경우 host subcase도 같은 명령으로 실행됩니다. Ubuntu/Debian 회사 Ceph role image도 backend/protocol runtime이 포함되어 있고 같은 release·ABI·architecture이면 image 환경 변수로 주입합니다. 이 테스트가 필요한 것은 RGW의 STS·Swift·Vault 기능과 Ceph CLI이며, 서버 library에 별도 go-ceph 의존성을 추가하는 기능은 아닙니다.
