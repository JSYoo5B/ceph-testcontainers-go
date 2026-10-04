# 시나리오 fixture CI

Go 필수 CI는 digest로 고정한 `ceph.DefaultImage`의 원본 Quay Ceph 20.2.4를 사용합니다. 기본·토폴로지 52개, 추가 서버/client fixture 47개와 Docker bridge SDK 회귀 2개를 합해 **distinct top-level test 이름 101개**를 실행합니다. Linux go-ceph 1개는 호출자가 client/runner 이미지를 준비하여 별도 실행하는 선택 target입니다. [이미지 프로젝트 CI](../../ceph-testcontainers-images/.github/workflows/test.yml)는 독립된 quick/full 검사기를 실행하며 이 Go suite나 go-ceph를 실행하지 않습니다. Helper 검사도 포함한 이름 수이며, bridge/host·phase별 subtest 또는 native I/O 수와 같지 않습니다.

분리 전에는 fixture profile 7개·새 이름 48개를 한 CI에 추가했습니다. 현재 Go 필수 CI는 6개 profile·47개이며 go-ceph 1개는 선택 실행입니다. **확대된 전체 CI는 실행했으며, source `be58018`에서 RGW sync의 실패 2개와 미실행 translation 2개가 남았습니다.** 아래 목록의 기준은 `artifacts/quay-fixture-ci-inventory-20261004/coverage-plan.json`이며, 전체 실행의 실제 terminal 결과는 다음 절에 기록합니다. 기존 완료 기록이나 다른 source의 PASS로 남은 실패를 대체하지 않습니다.

첫 확대 CI는 source `efa5ee3655173c496cc00f8c3e0f78baa7bbedf0`의 [run 37179959997](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37179959997)로 2026-10-04 05:27:55 UTC에 시작했습니다. 05:30 UTC 관측에서는 `make check` job이 SUCCESS, `quay-default`는 실행 중이며 새 fixture runtime job은 모두 대기 상태였습니다. 이 중간 관측은 terminal 성공 증거가 아닙니다.

소비자 Dockerfile의 package 복사 누락을 수정한 source `73cc4ae34ed165bd1438d88fa0e62cbbc9aef296`의 [후속 전체 CI 37180395289](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37180395289)는 2026-10-04 05:36:43 UTC에 시작했으며 첫 관측은 queued 상태였습니다. 첫 run과 별도로 추적한 당시의 중간 관측이며, 새 fixture 전체의 terminal 성공 증거가 아닙니다.

앞선 두 실행에서 `TestMGRModules`가 bridge/host 모두 실패했습니다. 원본 Quay로 재현한 오류는 module 변경 직후 `TemporaryMGRModule`의 첫 조회가 `active MGR is not available`로 실패하는 재시작 구간이었습니다. 첫 snapshot의 bounded 읽기 재시도를 수정한 `5fe327653b325c8887d721cf47bd5ec08b39e187`의 [전체 CI 37181788541](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37181788541)는 2026-10-04 06:05:34 UTC에 시작했습니다. 테스트·시간 제한·native 판정 범위는 동일하며, 이 시작 기록만으로 전체 필수 CI 성공을 판정하지 않습니다.

## 확인된 전체 CI 결과

2026-10-05 확인한 [run 37226924156](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37226924156)은 source `be58018d23efe0738c668e407075b556e4a09fd3`로 2026-10-04 19:05:23–20:32:00 UTC에 실행됐으며 terminal 결과는 **FAILURE**입니다. 원본 Quay Ceph 20.2.4의 Linux AMD64 필수 runtime job 10개 중 9개가 SUCCESS이고, 그 9개에서 기대한 parent 94개가 모두 RUN/PASS했습니다. Docker bridge SDK 2개는 이 94개에 포함됩니다.

남은 `quay-rgw-sync-fixtures` job은 기대 parent 7개 중 5개를 실행해 3개 PASS·2개 FAIL했습니다. `TestMultiClusterRGWSelectivePolicy`는 prefix를 변경한 뒤 새 객체 `/tc-policy-selected/reports/after-policy-change`가 HTTP 404/`NoSuchKey`로 남아 기다림이 종료됐습니다. `TestHostNetworkMultiClusterRGWOwnedSyncPolicy`도 prefix 변경 뒤 `/tc-owned-selected/reports/after-update`의 새 bytes가 도착하지 않아 같은 404 상태로 deadline에 도달했습니다. 이는 정책 변경 뒤 미래 객체의 실제 복제가 실패한 관측이며, 단순 CLI 성공이나 이전 checkpoint로 통과 처리하지 않습니다. 이 run은 수정 전 실패 증거이며 다음 절의 후속 focused 실행과 구분합니다.

첫 Go 명령의 실패로 Make의 두 번째 translation 명령은 실행되지 않았습니다. 따라서 `TestMultiClusterRGWSyncTranslationFiltering`와 `TestHostNetworkMultiClusterRGWSyncTranslationFiltering`의 필수 child도 이 run에서는 미실행입니다. 전체 기대 101개 중 **99개 RUN·97개 PASS·2개 FAIL·2개 미실행**이며 parent SKIP은 0개입니다. 관측한 child 117개는 모두 PASS이고 child SKIP/FAIL은 0개입니다. 이 child 성공을 실행되지 않은 translation 범위의 증거로 사용하지 않습니다.

Job ID·source head·raw log SHA-256·각 RUN/PASS/FAIL·child 및 package completion은 `artifacts/scenario-fixture-completion-20261005/previous-runtime-audit.json`에 연결했으며 raw 로그는 같은 디렉터리의 `previous-logs/`에 보관합니다. 당시 job 이름은 `quay-*`이고 아래 표는 같은 selector의 현재 `scenario-*` 이름을 사용합니다. 이름 변경이나 이후 source 변경을 이 이전 runtime의 새 PASS로 표시하지 않습니다. 대표 이미지 matrix 12개와 선택적 `rgw-native-regressions`는 위 101개 수에 합산하지 않습니다.

## RGW policy barrier 수정 후 focused 검증

`0caff38`은 정책 생성·상태·prefix 변경 후 destination의 bucket policy metadata import를 확인한 다음 미래 객체를 쓰도록 fixture를 수정했습니다. Import는 쓰기 전 준비 조건이며 실제 복제 bytes·제외·삭제 판정을 대신하지 않습니다.

2026-10-05 이 수정이 포함된 로컬 Go snapshot을 준비된 원본 Quay 이미지와 Docker Desktop Linux ARM64에서 실행했습니다. `TestMultiClusterRGWSelectivePolicy`, `TestMultiClusterRGWOwnedSyncPolicy`, `TestHostNetworkMultiClusterRGWOwnedSyncPolicy` 3개가 모두 PASS했고 FAIL/SKIP은 0개, Go test binary의 exit code는 0, 실행은 1580.965초였습니다. 종료 후 owned container와 새 network는 각각 0개이며 기존 network는 보존했습니다. 이미지 빌드·pull은 0회입니다.

`artifacts/scenario-fixture-completion-20261005/focused/summary.json`의 strict 결과는 **`source_unchanged: false`, `passed: false` 그대로 보존**합니다. 실행 중 선택되지 않은 `cephfs_multicluster_integration_test.go`의 주석 한 줄이 `// Select a compatible runtime supplying the userspace mirror daemon.`에서 `// The source control runtime supplies the userspace mirror daemon.`으로 바뀌었습니다. `comment-only-source-change.json`은 이 주석을 되돌려 재구성한 SHA-256이 시작 시 manifest와 일치함을 기록합니다. `post-runtime-audit.json`은 원본 로그·summary의 해시와 native 실행·잔존 관측 결과를 별도로 연결합니다. 이 설명으로 strict summary를 PASS로 바꾸거나 실행 당시 source와 현재 source가 동일하다고 표시하지 않습니다.

후속 source `d9115f4`의 [CI run 37240162309](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37240162309)는 기록 시점 진행 중입니다. 위 focused 3개 결과는 상세 101개, 이미지 matrix 12개 또는 필수 runtime cleanup 22개 전체의 PASS 증거가 아닙니다.

## 실행 경로와 시간 제한

현재 CI job과 Make target은 검증할 시나리오를 나타내는 `scenario-*` 이름을 사용합니다. 이전 실행의 `quay-*` job 이름과 artifact 경로는 당시 증거 그대로 보존합니다. 이름 변경은 필수 selector·timeout·이미지 선택을 변경하지 않으며, 기본 상세 시나리오는 계속 `ceph.DefaultImage`를 사용합니다.

[workflow](../.github/workflows/test.yml)는 `make check` 성공 후 `scenario-default`를 실행합니다. Go 프로젝트의 기존 topology job과 새 fixture job은 모두 `scenario-default` 성공 뒤 Ubuntu 24.04 Linux AMD64 runner에서 실행합니다. 공개 모듈·integration runner는 `CGO_ENABLED=0`이며, 실제 go-ceph probe만 호출자가 준비하는 Linux 소비자 이미지에서 cgo/native 라이브러리를 사용합니다. 역할 이미지에는 compiler나 개발 헤더를 요구하지 않습니다.

| 추가 필수 profile | 이름 수 | Go timeout | CI job timeout | `be58018` runtime 결과 |
|---|---:|---|---|---|
| `scenario-cluster-fixtures` | 8 | 40분 | 50분 | SUCCESS · 8/8 PASS · skip 0 |
| `scenario-cephfs-fixtures` | 8 | 120분 | 130분 | SUCCESS · 8/8 PASS · skip 0 |
| `scenario-rados-fixtures` | 4 | 120분 | 130분 | SUCCESS · 4/4 PASS · skip 0 |
| `scenario-rbd-fixtures` | 6 | 120분 | 130분 | SUCCESS · 6/6 PASS · skip 0 |
| `scenario-rgw-fixtures` | 14 | 120분 | 130분 | SUCCESS · 14/14 PASS · skip 0 |
| `scenario-rgw-sync-fixtures` | 7 | 각 Go 명령 60분, 두 명령 실행 | 75분 | FAILURE · RUN 5 / PASS 3 / FAIL 2 / 미실행 2 · skip 0 |
| 추가 필수 합계 | 47 | | | 43 PASS / 2 FAIL / 2 미실행 · skip 0 |
| `scenario-goceph-linux` · 선택 실행 | 1 | native integration runner 40분 | 기본 CI job 없음 | caller가 client/runner 준비 |

로컬에서는 [Makefile](../Makefile)의 같은 target을 사용합니다. Host network 경로를 container runner에서 실행하면 Docker daemon의 host 주소가 필요합니다. CI는 `TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1`을 지정합니다.

```sh
make check
make scenario-default
make scenario-topology
make scenario-multicluster-topology
make scenario-topology-extensions
make scenario-cluster-fixtures
make scenario-cephfs-fixtures
make scenario-rados-fixtures
make scenario-rbd-fixtures
make scenario-rgw-fixtures
make scenario-rgw-sync-fixtures
# 아래 두 Linux 소비자 이미지는 호출자가 로컬 엔진에 미리 준비합니다.
CEPH_TEST_GOCEPH_CLIENT_IMAGE=ceph-testcontainers-goceph:20.2.4-client \
CEPH_TEST_GOCEPH_RUNNER_IMAGE=ceph-testcontainers-goceph:20.2.4-runner \
make scenario-goceph-linux
```

Go CI의 각 새 job은 `shell: bash`의 pipefail로 `make` 실패를 유지하면서 `$RUNNER_TEMP/<profile>.log`에 출력을 저장합니다. 실패한 job만 [reporter](../.github/scripts/report_test_failures.py)를 실행하며, 완료된 Go 실패 test/subtest 이름만 annotation으로 노출합니다. Reporter에만 `continue-on-error`를 적용하므로 reporter 오류가 원래 테스트 결과를 덮지 않습니다. Consumer image 준비 실패·timeout 등으로 완료된 testcase가 없으면 원인을 추정하지 않고 미확인 notice를 남깁니다.

Runtime job은 [cleanup action](../.github/actions/runtime-cleanup/action.yml)으로 테스트 전에 `org.testcontainers=true`인 container/network ID를 기록하고, 테스트 뒤 성공·실패에 관계없이 새로 남은 소유 리소스를 조회합니다. Ryuk의 정상 종료를 최대 30초 기다린 뒤에도 새 ID가 남으면 job이 실패합니다. 기존 리소스는 baseline으로 보존하며 검사기는 삭제·stop·prune를 수행하지 않습니다. Docker 조회 오류나 엔진 변경도 빈 목록의 성공으로 처리하지 않습니다. `runtime-cleanup-scenario-*` 또는 matrix의 `runtime-cleanup-image-<variant>-<layout>-<architecture>` artifact에 실행 source와 전후 identity·잔존 결과를 보관합니다. Test PASS만으로 이 별도 정리 검사의 성공을 대신하지 않습니다.

## 이미지 호환성 matrix

위의 원본 Quay 상세 101개 경로에 더해 [workflow](../.github/workflows/test.yml)의 `image-compatibility` job이 공식·GHCR Debian·Ubuntu 이미지에 같은 대표 9개 Go 테스트를 적용합니다. 세 계열 × `all`/`roles` × Linux AMD64/ARM64, 총 12개 조합입니다. `fail-fast: false`로 한 조합의 실패가 다른 조합의 결과 수집을 취소하지 않으며 각 job의 제한은 50분입니다. AMD64는 `ubuntu-24.04`, ARM64는 `ubuntu-24.04-arm`의 native Docker 엔진에서 실행하고 에뮬레이션 성공으로 다른 architecture를 지원한다고 표시하지 않습니다.

| 계열 | `all` 방식 | `roles` 방식 | platform별 선택 수 |
|---|---|---|---:|
| `official` | `ceph.DefaultImage`의 고정 Quay digest | GHCR `official-20.2.4-{control,osd,rgw,mds}` | 9개씩 |
| `debian` | GHCR `debian-20.2.4-all` | GHCR `debian-20.2.4-{control,osd,rgw,mds}` | 9개씩 |
| `ubuntu` | GHCR `ubuntu-20.2.4-all` | GHCR `ubuntu-20.2.4-{control,osd,rgw,mds}` | 9개씩 |

GHCR repository는 `ghcr.io/jsyoo5b/ceph-testcontainers-images`입니다. `roles`는 control/OSD/RGW/MDS를 각 역할에 지정하며 mirror는 같은 control을 사용합니다. CI와 로컬 matrix runner는 준비된 이미지를 선택하고 이미지를 빌드·패키징·배포하지 않습니다. 실제 image ID·digest·platform에 연결된 실행 결과를 확인해야 하며 tag 이름이나 registry manifest 존재만으로 PASS를 표시하지 않습니다. 2026-10-05 source `be58018`의 [CI run 37226924156](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37226924156)에서 12개 조합 전체가 PASS한 기존 기록을 보존합니다.

후속 source `d9115f4`의 [CI run 37240162309](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37240162309)도 matrix 12개 각각 대표 9개 RUN/PASS·child FAIL/SKIP 0·package `ok`와 image ID·digest·native platform을 확인했습니다. Mirror 전용 role 선택을 제거하고 네 component 이미지와 source control mirror를 사용하는 경로입니다. 각 matrix cleanup 12개도 전후 동일 engine·source와 새 container/network 0개로 PASS했습니다. 현재 전체 상세 CI는 진행 중이며 이 결과를 상세 fixture 101개나 필수 cleanup 22개 전체의 성공으로 표시하지 않습니다. 같은 source manifest와 개별 artifact를 대조한 [최신 matrix 실행 증거](IMAGE_COMPATIBILITY.md#d9115f4-matrix-검증)에 기록합니다.

로컬에서는 현재 Docker 엔진의 native platform에서 한 조합을 실행합니다.

```sh
make image-matrix IMAGE_VARIANT=ubuntu IMAGE_LAYOUT=all
make image-matrix IMAGE_VARIANT=debian IMAGE_LAYOUT=roles
# On a native ARM64 Docker engine:
make image-matrix IMAGE_VARIANT=official IMAGE_LAYOUT=roles \
  IMAGE_PLATFORM=linux/arm64
```

각 조합은 cluster/MGR lifecycle, RBD, CephFS, signed RGW-S3, RBD backup와 RBD/CephFS snapshot mirroring, RGW multisite의 **대표 9개 Go 이름**을 실행합니다. 이 이름들은 기존 상세 suite와 겹칩니다. 12 × 9회 선택을 108개 새로운 distinct test로 더하거나, 이미지 프로젝트의 독립 Python full 8개·기존 Quay 상세 101개와 하나의 성공 증거로 합치지 않습니다. Linux go-ceph·cryptsetup 등 추가 소비자 도구는 별도 조건으로 유지합니다. 정확한 실행 계약은 [IMAGE_COMPATIBILITY.md](IMAGE_COMPATIBILITY.md#공식debianubuntu-이미지-matrix)를 따릅니다.

## 추가되는 named test 전체

다음 목록은 profile별로 고정합니다. Test 내부에 bridge/host child가 있는 경우 모두 유지합니다. Host 전용 wrapper와 helper 이름도 그대로 포함하며, `-list` 또는 tag compile은 runtime 통과 증거로 사용하지 않습니다.

### scenario-cluster-fixtures · 8개

```text
TestClientIdentities
TestCephFSSubvolumes
TestConfigurationOverrides
TestOSDPolicies
TestCephFSSubvolumeSnapshotsAndClones
TestRGWPlacementStorageClasses
TestHostNetworkRGWPlacementStorageClasses
TestRGWPlacementRealmStorageClasses
```

### scenario-cephfs-fixtures · 8개

```text
TestCephFSDynamicDataPools
TestCephFSCloneCancellationAndPartialCleanup
TestCephFSQuiesceCheckpoints
TestCephFSSubvolumeClientAuthorization
TestCephFSPins
TestCephFSRetainedSnapshotAndMetadataRecipe
TestCephFSAdditionalErasureCodedDataPool
TestHostNetworkCephFSFilesystem
```

### scenario-rados-fixtures · 4개

```text
TestClientFencing
TestMGRModules
TestRADOSClientFixtures
TestNativePoolReplacement
```

MGR 수정 후 Docker Desktop Linux ARM64의 focused `TestMGRModules`는 bridge/host 모두 PASS했습니다. Native membership/dependency, 실제 RBD schedule/task 완료, optional module의 이전 enabled/disabled 상태 복원, always-on 및 사용 중인 mirror policy 보호를 확인했습니다. Test는 159.29초, harness·cleanup 포함 176.592초이며 새 서버 이미지 빌드 0회와 최종 owned container/network 0개입니다. 단위·race에서도 재시작/불일치 snapshot 이후 변경 1회, 지속 실패·취소 시 변경 0회와 복원 소유권 보존을 검증했습니다. 증거는 `artifacts/quay-mgr-modules-20261004-r2/summary.json`과 `post-runtime-audit.json`에 보관하며, RADOS profile 4개 전체 또는 AMD64 CI 완료로 확대하지 않습니다.

### scenario-rbd-fixtures · 6개

```text
TestRBDClientFeatures
TestRBDAutomaticSnapshotSchedule
TestMultiClusterRBDMirrorScopeAndNamespaces
TestMultiClusterRBDFailback
TestMultiClusterRBDSplitBrainResync
TestHostNetworkRBDLifecycle
```

`TestRBDClientFeatures`의 layering/flatten, trash, migration commit/abort, group snapshot, exclusive lock, encryption format/load와 rekey 8개 phase를 bridge/host에서 모두 실행합니다. Phase를 일부 제외하여 기본 이미지의 통과를 만들지 않습니다.

2026-10-04 source `efa5ee3`의 로컬 focused 실행은 원본 Quay 서버와 같은 원본 Quay native consumer에서 `TestRBDClientFeatures` 하나를 PASS했습니다. Docker Desktop의 Linux ARM64에서 host-network Go runner로 실행했고, bridge/host 각 8개 native phase 총 16개가 skip 없이 통과했습니다. Test는 225.57초, harness·cleanup 포함 242.319초이며 이미지 빌드 0회, 최종 owned container/network 0개입니다. 증거는 `artifacts/quay-rbd-client-20261004-r1/summary.json`입니다. 이 결과는 RBD 소비자 도구와 해당 recipe의 실제 증거이며, RBD profile 6개 전체나 새 fixture 48개 전체 CI의 통과를 뜻하지 않습니다.

### scenario-rgw-fixtures · 14개

```text
TestRGWUserPlacementPolicy
TestHostNetworkRGWUserPlacementPolicy
TestRGWTenantsAndAccounts
TestHostNetworkRGWTenantsAndAccounts
TestRGWBucketMaintenance
TestRGWS3ClientFeatures
TestRGWNativeTLS
TestRGWProtocolBackends
TestRGWAdminRecordsAndRateLimit
TestHostNetworkHTTPTransportPreservesSignedRequest
TestRGWBackendSTSFormContentTypeIsSigned
TestRGWBackendRoleCleanupRefusesForeignPolicy
TestRGWBackendAuditProofRequiresCompletedVaultTransactions
TestRGWBackendStatusProbeReceivesBoundedContext
```

뒤의 transport/signing/cleanup/audit/context helper 검사는 실제 RGW·Vault client proof와 별도입니다. Helper 성공만으로 backend 동작을 완료 처리하지 않습니다.

### scenario-rgw-sync-fixtures · 7개

```text
TestMultiClusterRGWSelectivePolicy
TestMultiClusterRGWOwnedSyncPolicy
TestHostNetworkMultiClusterRGWOwnedSyncPolicy
TestMultiClusterRGWAccountRootSync
TestHostNetworkMultiClusterRGWAccountRootSync
TestMultiClusterRGWSyncTranslationFiltering
TestHostNetworkMultiClusterRGWSyncTranslationFiltering
```

마지막 두 parent에서는 `tag_owner_class`와 `tenant_system_user_isolation`만 필수 profile로 실행합니다. 독립된 이 두 child의 실제 복제 bytes·제외·checkpoint·cleanup을 확인하는 경로입니다. Parent 이름이 포함됐다는 이유로 모든 translation child를 실행한 것으로 세지 않습니다.

### scenario-goceph-linux · 준비된 소비자 이미지로 선택 실행하는 1개

```text
TestGoCephLinux
```

## 원본 서버와 소비자 도구의 경계

새 profile은 Ceph source를 컴파일하거나 MON/MGR/OSD/MDS/RGW/mirror 서버 이미지를 생성하지 않습니다. Control/OSD/RGW/MDS override 네 개를 해제하여 기존 원본 Quay 서버를 직접 소비하며 mirror는 source 클러스터의 control 이미지를 사용합니다. `ceph.Run`은 호출자가 선택한 이미지를 실행하며 이미지 builder를 호출하지 않습니다.

일반 fixture profile은 `CEPH_TEST_RBD_CLIENT_IMAGE`와 `CEPH_TEST_VAULT_IMAGE`도 해제합니다. RBD native consumer는 기본 Quay의 Python bindings·cryptsetup을 사용합니다. ARM64 원본 이미지의 사전 도구 조회에서 bindings와 cryptsetup 2.8.6이 확인됐지만, 이 관측은 cluster I/O 또는 새 AMD64 CI 통과가 아닙니다. Slim consumer에 cryptsetup이 없다면 기존 [RBD client 준비 경로](RBD_CLIENT_FIXTURES.md)를 별도로 선택합니다.

`TestRGWProtocolBackends`는 기본 `hashicorp/vault:1.21.4`의 실제 KV-v2 backend를 기동합니다. 허용·거부 audit transaction, STS/Swift 및 암호화 데이터를 확인하며 모의 backend로 성공을 대신하지 않습니다. [Backend recipe](RGW_PROTOCOL_BACKENDS.md)에 조건을 기록합니다.

2026-10-04 로컬 Docker Desktop Linux ARM64 실행에서 원본 Quay RGW의 STS/Swift/SSE-KMS가 bridge/host 각 3개, 총 6개 phase를 skip 없이 PASS했습니다. 두 gateway의 STS trust/action/resource 거부와 기존 session의 정책 복원, Swift key/token·공유 object bytes, 실제 Vault의 allowed/denied audit read와 key 삭제·복원 후 decrypt 결과를 확인했습니다. Test는 136.39초, harness·cleanup 포함 151.158초이며 새 서버 이미지 빌드 0회와 최종 owned container/network 0개입니다. `artifacts/quay-rgw-backends-20261004-r1/summary.json`과 `post-runtime-audit.json`이 증거이며, RGW profile 14개 전체나 AMD64 CI 완료를 의미하지 않습니다.

`scenario-goceph-linux`의 [Linux 실행 harness](../internal/integration/goceph/run.py)는 주어진 client/runner 이미지를 local inspect한 뒤 immutable image ID로 실행합니다. 이미지를 빌드하거나 내려받지 않습니다. 호출자는 같은 Ceph release·ABI의 Linux native 라이브러리와 `go-ceph v0.41.0` probe를 가진 client, 검증할 Go checkout의 integration binary와 native probe를 가진 runner를 준비합니다. Probe의 cgo 빌드 조건은 소비자에게 적용되며 server 역할 이미지에 개발 헤더나 compiler를 요구하지 않습니다. 이미지 프로젝트 CI는 자체 Python 검사기를 실행하고 이 SDK profile을 실행하지 않습니다. 구체적인 입력 계약은 [IMAGE_COMPATIBILITY.md](IMAGE_COMPATIBILITY.md#추가-소비자-조건)를 따릅니다. macOS native go-ceph 빌드의 지원을 뜻하지 않습니다.

새 local go-ceph 준비의 첫 실행은 runtime 이전에 실패했습니다. 소비자 Dockerfile이 module build context의 `internal/dockerbridge`를 복사하지 않아 Go runner를 컴파일할 수 없었습니다. 해당 package의 `COPY`를 추가한 `73cc4ae`의 재실행은 소비자 build와 `TestGoCephLinux`의 bridge/host runtime을 모두 PASS했습니다. 이 수정 후 결과를 `efa5ee3`의 성공 증거로 사용하지 않습니다.

2026-10-04 05:40 UTC의 로컬 실행은 Docker Desktop Linux ARM64에서 각각 두 독립 cluster의 RADOS/RBD/userspace CephFS를 검증했습니다. OSD 2 → 3 → 2 변경 전후 데이터·RBD snapshot 격리와 head 복원·fresh session·owned object/image/file 삭제를 확인한 native proof는 26개이며, 그중 Docker host namespace의 Linux native process 검증은 6개입니다. Test는 241.23초였고, 소비자 probe/runner 두 이미지만 준비했으며 새 Ceph 서버 이미지 빌드는 0회입니다. `summary.json`과 `post-runtime-audit.json`에서 실제 PASS와 최종 owned container/network 0개를 확인했습니다. 증거는 `artifacts/quay-goceph-local-20261004-r2/`에 보관합니다. 전체 Linux AMD64 필수 CI의 완료와는 별도 결과입니다.

회사 패키지나 native 패치를 포함한 이미지는 이 필수 CI와 독립적입니다. 해당 이미지의 제작은 소유자가 맡으며 준비된 결과를 [고정 이미지 요구사항](../../ceph-testcontainers-images/docs/IMAGE_REQUIREMENTS.md)에 따라 확인한 뒤 `Run`과 역할별 image option으로 지정합니다. 과거 `.deb` 입력 builder의 기록은 당시 실험 증거이며 현재 유지되는 제작 도구가 아닙니다. 일반 `integration`·`client-fixtures`의 image override 경로는 유지하고 custom image의 제작이나 통과를 기본 Quay 완료 조건에 넣지 않습니다.

## 지원되는 경로와 strict native 회귀

원본 Ceph 20.2.4의 numeric priority 선택과 ordinary-user source 권한 거부에는 확인된 native 결함이 있습니다. 필수 translation child의 성공을 이 두 기능의 지원으로 확대하지 않습니다. [G05/G07의 원본 한계와 선택적 패치 증거](RGW_SYNC_POLICY.md)를 구분합니다.

`make rgw-sync-native-regressions`는 다음 strict child를 bridge/host 모두 실행합니다.

```text
TestMultiClusterRGWSyncTranslationFiltering/priority_tags_owner_class
TestHostNetworkMultiClusterRGWSyncTranslationFiltering/priority_tags_owner_class
TestMultiClusterRGWSyncTranslationFiltering/ordinary_user_denial_grant
TestHostNetworkMultiClusterRGWSyncTranslationFiltering/ordinary_user_denial_grant
```

이 경로에는 expected-failure 변환이나 권한·데이터 판정 완화가 없습니다. CI에서는 `workflow_dispatch`의 `rgw_native_regressions`를 명시적으로 선택하며, 이미 준비된 patched RGW를 `rgw_image` 입력으로 지정할 수 있습니다. 빈 입력은 원본 Quay입니다. 이 job도 서버 이미지를 빌드하지 않습니다.

선택적 native mirror shuffle 이름 `TestMultiClusterCephFSMirrorDaemonTopology`와 `TestHostNetworkCephFSMirrorDaemonTopology`는 Go CI 101개와 선택적 go-ceph 1개에 포함하지 않습니다. 필수 topology의 daemon rebalance/HA 이름과 구분하며 기존 선택적 실행 경로를 유지합니다.

## 완료 판정과 기존 증거

서버 fixture는 [제공 기준](CLIENT_FIXTURE_COVERAGE.md)의 네 조건을 확인합니다: 실행 가능한 공개 API 조합, native 상태·identity, 실제 client 허용/거부·데이터·장애 복구, 원래 상태 복원 또는 owned cleanup입니다. Sync caught-up만으로 payload나 권한 거부를 대신하지 않습니다. Helper 검사는 각 signer/transport/ownership/deadline 계약을 확인하는 별도 증거입니다.

새 CI 완료는 해당 source·tag·selector와 실제 RUN/PASS 이름, bridge/host child 및 phase, native proof와 cleanup, 필수 job 전체의 terminal SUCCESS를 함께 확인한 뒤 기록합니다. 시작·등록·compile 또는 과거 custom-image PASS는 새 실행 성공이 아닙니다.

기존 `3f79a78cb168bbe99a78fab5450a94f2f322e9d0`의 [Linux AMD64 CI 37173593510](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37173593510)는 2026-10-04 04:42 UTC SUCCESS였습니다. 기존 필수 job 5개에서 기본 14개(11 native/runtime + 3 signer), core 8개, multi 18개, extensions 12개와 별도 SDK 2개를 확인한 기록입니다. **새 fixture 48개는 이 실행의 범위에 없습니다.** 기존 ARM64·slim·Debian 관측과 Docker Desktop 공개 포트의 한계도 [구성별 증거](CLUSTER_SCENARIOS.md)에 그대로 보존합니다.

<details>
<summary>기존 필수 integration 이름 52개</summary>

```text
TestBootstrapFailureCleanup
TestCephFSFilesystem
TestCephFSMDSScaleStandbyReplayTopology
TestCephFSMDSScaleTopology
TestCephFSMultiActiveStandbyFailoverAndFilesystems
TestCephFSStandbyReplayFailover
TestClusterLifecycle
TestErasureCodedPools
TestFiveMonitorQuorumAndNetworkRecovery
TestHostFailureDomainPool
TestHostNetworkCephFSManagerTopology
TestHostNetworkCephFSMirrorDaemonRebalanceTopology
TestHostNetworkCephFSSnapshotMirrorAndBackup
TestHostNetworkMonitorPortConflictRetry
TestHostNetworkMultiCluster
TestHostNetworkRBDMirrorDaemonTopology
TestHostNetworkRBDSnapshotMirror
TestHostNetworkRGWEndpoints
TestHostNetworkRGWInitialZonegroupsTopology
TestHostNetworkRGWMultisite
TestHostNetworkRGWThreeZoneTopology
TestHostNetworkRGWUserAdministration
TestHostNetworkRGWZonegroupsAndRemovalTopology
TestInitialClusterComposition
TestManagerLifecycle
TestMonitorManagerTopology
TestMultiClusterCephFSManagerTopology
TestMultiClusterCephFSMirrorDaemonRebalanceTopology
TestMultiClusterCephFSSnapshotMirrorAndBackup
TestMultiClusterRBDBackup
TestMultiClusterRBDJournalMirrorFailback
TestMultiClusterRBDMirrorDaemonTopology
TestMultiClusterRBDPeerLifecycle
TestMultiClusterRBDPeerNetworkInterruption
TestMultiClusterRBDSnapshotFanout
TestMultiClusterRBDSnapshotMirror
TestMultiClusterRGWInitialZonegroupsTopology
TestMultiClusterRGWMetadataMasterFailover
TestMultiClusterRGWMultisite
TestMultiClusterRGWPeerNetworkTopology
TestMultiClusterRGWThreeZoneTopology
TestMultiClusterRGWZonegroupsAndRemovalTopology
TestPoolPolicies
TestRBDLifecycle
TestRBDNamespaces
TestRGWS3
TestRGWTopology
TestRGWUserAdministration
TestS3FixtureCanonicalURIFollowsAWS4
TestS3FixtureSigningCanonicalizesLiteralAndEncodedTenantSeparator
TestS3FixtureSigningUsesUTCInstantAcrossTimeZones
TestSeparateClusterNetworksAndInterruptions
```

</details>

별도 SDK 이름은 `TestRecoverableBridgeEndpointIdentity`와 `TestRecoverableBridgePublishedPort`입니다. 이 둘의 HTTP/endpoint 복구 proof는 Ceph 서버 기능 개수에 합산하지 않습니다.
