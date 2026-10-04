# 원본 Quay fixture CI

필수 CI는 digest로 고정한 `ceph.DefaultImage`의 원본 Quay Ceph 20.2.4를 사용합니다. 기존 기본·토폴로지 52개에 서버 fixture·client recipe 48개를 추가하고, Docker bridge SDK 회귀 2개를 별도로 실행합니다. 합계는 **distinct top-level test 이름 102개**입니다. Helper 검사도 포함한 이름 수이며, bridge/host 및 phase별 subtest 수나 native I/O 시나리오 수와 같지 않습니다.

새 fixture profile 7개의 범위와 CI wiring을 추가했습니다. **새 48개를 포함한 local/CI runtime 전체 통과는 아직 확인하지 않았습니다.** 아래 목록의 기준은 `artifacts/quay-fixture-ci-inventory-20261004/coverage-plan.json`이며, 기존 완료 기록을 새 profile의 PASS로 대체하지 않습니다.

첫 확대 CI는 source `efa5ee3655173c496cc00f8c3e0f78baa7bbedf0`의 [run 37179959997](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37179959997)로 2026-10-04 05:27:55 UTC에 시작했습니다. 05:30 UTC 관측에서는 `make check` job이 SUCCESS, `quay-default`는 실행 중이며 새 fixture runtime job은 모두 대기 상태였습니다. 이 중간 관측은 terminal 성공 증거가 아닙니다.

소비자 Dockerfile의 package 복사 누락을 수정한 source `73cc4ae34ed165bd1438d88fa0e62cbbc9aef296`의 [후속 전체 CI 37180395289](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37180395289)는 2026-10-04 05:36:43 UTC에 시작했으며 첫 관측은 queued 상태였습니다. 첫 run과 별도 실행으로 추적하며 새 fixture 전체의 terminal 결과는 아직 대기 중입니다.

## 실행 경로와 시간 제한

[workflow](../.github/workflows/test.yml)는 `make check` 성공 후 `quay-default`를 실행합니다. 기존 topology job과 새 fixture job은 모두 `quay-default` 성공 뒤 Ubuntu 24.04 Linux AMD64 runner에서 실행합니다. 공개 모듈·integration runner는 `CGO_ENABLED=0`이며, 실제 go-ceph probe만 별도 Linux build stage에서 cgo를 사용합니다.

| 추가 필수 profile | 이름 수 | Go timeout | CI job timeout | 현재 runtime 상태 |
|---|---:|---|---|---|
| `quay-cluster-fixtures` | 8 | 40분 | 50분 | 검증 대기 |
| `quay-cephfs-fixtures` | 8 | 120분 | 130분 | 검증 대기 |
| `quay-rados-fixtures` | 4 | 120분 | 130분 | 검증 대기 |
| `quay-rbd-fixtures` | 6 | 120분 | 130분 | 검증 대기 |
| `quay-rgw-fixtures` | 14 | 120분 | 130분 | 검증 대기 |
| `quay-rgw-sync-fixtures` | 7 | 각 Go 명령 60분, 두 명령 실행 | 75분 | 검증 대기 |
| `quay-goceph-linux` | 1 | native integration runner 40분 | 소비자 준비 포함 75분 | 검증 대기 |
| 합계 | 48 | | | |

로컬에서는 [Makefile](../Makefile)의 같은 target을 사용합니다. Host network 경로를 container runner에서 실행하면 Docker daemon의 host 주소가 필요합니다. CI는 `TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1`을 지정합니다.

```sh
make check
make quay-default
make quay-topology
make quay-multicluster-topology
make quay-topology-extensions
make quay-cluster-fixtures
make quay-cephfs-fixtures
make quay-rados-fixtures
make quay-rbd-fixtures
make quay-rgw-fixtures
make quay-rgw-sync-fixtures
make quay-goceph-linux
```

각 새 job은 `shell: bash`의 pipefail로 `make` 실패를 유지하면서 `$RUNNER_TEMP/<profile>.log`에 출력을 저장합니다. 실패한 job만 [reporter](../.github/scripts/report_test_failures.py)를 실행하며, 완료된 Go 실패 test/subtest 이름만 annotation으로 노출합니다. Reporter에만 `continue-on-error`를 적용하므로 reporter 오류가 원래 테스트 결과를 덮지 않습니다. Consumer build 실패·timeout 등으로 완료된 testcase가 없으면 원인을 추정하지 않고 미확인 notice를 남깁니다.

## 추가되는 named test 전체

다음 목록은 profile별로 고정합니다. Test 내부에 bridge/host child가 있는 경우 모두 유지합니다. Host 전용 wrapper와 helper 이름도 그대로 포함하며, `-list` 또는 tag compile은 runtime 통과 증거로 사용하지 않습니다.

### quay-cluster-fixtures · 8개

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

### quay-cephfs-fixtures · 8개

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

### quay-rados-fixtures · 4개

```text
TestClientFencing
TestMGRModules
TestRADOSClientFixtures
TestNativePoolReplacement
```

### quay-rbd-fixtures · 6개

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

### quay-rgw-fixtures · 14개

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

### quay-rgw-sync-fixtures · 7개

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

### quay-goceph-linux · 1개

```text
TestGoCephLinux
```

## 원본 서버와 소비자 도구의 경계

새 profile은 Ceph source를 컴파일하거나 MON/MGR/OSD/MDS/RGW/mirror 서버 이미지를 생성하지 않습니다. Daemon/mirror override 다섯 개를 해제하여 기존 원본 Quay 서버를 직접 소비합니다. `ceph.Run`은 호출자가 선택한 이미지를 실행하며 이미지 builder를 호출하지 않습니다.

일반 fixture profile은 `CEPH_TEST_RBD_CLIENT_IMAGE`와 `CEPH_TEST_VAULT_IMAGE`도 해제합니다. RBD native consumer는 기본 Quay의 Python bindings·cryptsetup을 사용합니다. ARM64 원본 이미지의 사전 도구 조회에서 bindings와 cryptsetup 2.8.6이 확인됐지만, 이 관측은 cluster I/O 또는 새 AMD64 CI 통과가 아닙니다. Slim consumer에 cryptsetup이 없다면 기존 [RBD client 준비 경로](RBD_CLIENT_FIXTURES.md)를 별도로 선택합니다.

`TestRGWProtocolBackends`는 기본 `hashicorp/vault:1.21.4`의 실제 KV-v2 backend를 기동합니다. 허용·거부 audit transaction, STS/Swift 및 암호화 데이터를 확인하며 모의 backend로 성공을 대신하지 않습니다. [Backend recipe](RGW_PROTOCOL_BACKENDS.md)에 조건을 기록합니다.

`quay-goceph-linux`는 [Linux 전용 runner](../internal/integration/goceph/run.py)로 소비자 probe와 integration runner 이미지를 준비합니다. 같은 Ceph release의 공개 header와 runtime library로 별도 `go-ceph v0.41.0` probe를 cgo 빌드하고, 공개 모듈의 runner는 cgo 없이 빌드합니다. Ceph C++·서버 데몬을 다시 빌드하지 않습니다. Probe/runner 준비와 실제 RADOS/RBD/CephFS I/O 모두 성공해야 profile 성공입니다. macOS native go-ceph 빌드를 지원하는 경로로 해석하지 않습니다.

새 local go-ceph 준비의 첫 실행은 runtime 이전에 실패했습니다. 소비자 Dockerfile이 module build context의 `internal/dockerbridge`를 복사하지 않아 Go runner를 컴파일할 수 없었습니다. 해당 package의 `COPY`를 추가한 `73cc4ae`의 재실행은 소비자 build와 `TestGoCephLinux`의 bridge/host runtime을 모두 PASS했습니다. 이 수정 후 결과를 `efa5ee3`의 성공 증거로 사용하지 않습니다.

2026-10-04 05:40 UTC의 로컬 실행은 Docker Desktop Linux ARM64에서 각각 두 독립 cluster의 RADOS/RBD/userspace CephFS를 검증했습니다. OSD 2 → 3 → 2 변경 전후 데이터·RBD snapshot 격리와 head 복원·fresh session·owned object/image/file 삭제를 확인한 native proof는 26개이며, 그중 Docker host namespace의 Linux native process 검증은 6개입니다. Test는 241.23초였고, 소비자 probe/runner 두 이미지만 준비했으며 새 Ceph 서버 이미지 빌드는 0회입니다. `summary.json`과 `post-runtime-audit.json`에서 실제 PASS와 최종 owned container/network 0개를 확인했습니다. 증거는 `artifacts/quay-goceph-local-20261004-r2/`에 보관합니다. 전체 Linux AMD64 필수 CI의 완료와는 별도 결과입니다.

회사 `.deb` 또는 다른 이미지 선택은 이 필수 CI와 독립적입니다. [Debian 이미지 builder](DEBIAN_IMAGE_AUTOMATION.md)는 패키지를 받아 로컬 역할 이미지를 만들고, 그 결과를 `Run`의 image와 역할별 image option으로 명시적으로 소비합니다. 일반 `integration`·`client-fixtures` 등의 기존 image override 경로는 유지합니다. `.deb` builder 실행이나 native 패치 이미지를 기본 Quay 완료 조건에 넣지 않습니다.

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

선택적 native mirror shuffle 이름 `TestMultiClusterCephFSMirrorDaemonTopology`와 `TestHostNetworkCephFSMirrorDaemonTopology`는 위 102개에 포함하지 않습니다. 필수 topology의 daemon rebalance/HA 이름과 구분하며 기존 선택적 실행 경로를 유지합니다.

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
