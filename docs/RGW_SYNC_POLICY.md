# RGW 선택적 복제와 sync checkpoint

`multicluster.RunRGWMultisite`로 독립된 Ceph cluster를 같은 realm의 zone으로 연결한 뒤, `CreateSyncGroup`·`CreateSyncFlow`·`CreateSyncPipe`로 복제 조건을 준비합니다. S3 bucket·object·ACL·bucket policy는 소비자 client가 생성합니다. 공개 fixture는 container 내부 `radosgw-admin`을 사용하며 go-ceph 의존성을 추가하지 않습니다.

## 저장과 활성화

Group은 zonegroup 또는 기존 bucket에 저장합니다. `allowed`는 bucket이 활성화할 수 있는 상한이며, `enabled`는 해당 group의 pipe를 활성화하고 `forbidden`은 겹치는 enabled rule보다 우선합니다. Directional flow는 source/destination zone pair를, symmetrical flow는 이름이 있는 zone 집합을 선택합니다. Bucket policy는 상위 zonegroup이 허용한 범위만 좁힐 수 있습니다. [Ceph selective sync 계약](https://docs.ceph.com/en/tentacle/radosgw/multisite-sync-policy/).

```go
permission, err := fixture.CreateSyncGroup(ctx,
    multicluster.RGWSyncPolicyScope{},
    multicluster.RGWSyncGroupConfig{ID: "app-permission", Status: multicluster.RGWSyncAllowed})
if err != nil { return err }
if err := fixture.CreateSyncFlow(ctx, permission, multicluster.RGWSyncFlowConfig{
    SourceZone: "source", DestinationZone: "destination",
}); err != nil { return err }
if err := fixture.CreateSyncPipe(ctx, permission, multicluster.RGWSyncPipeConfig{
    ID: "direction", SourceZones: []string{"source"}, DestinationZones: []string{"destination"},
}); err != nil { return err }
if err := fixture.ApplySyncGroup(ctx, permission); err != nil { return err }
// Gateway reload 후 mapped endpoint를 다시 구합니다.
// S3 client로 app-data bucket을 생성한 다음 bucket policy를 준비합니다.
selected, err := fixture.CreateSyncGroup(ctx,
    multicluster.RGWSyncPolicyScope{Bucket: "app-data"},
    multicluster.RGWSyncGroupConfig{ID: "app-selection", Status: multicluster.RGWSyncEnabled})
if err != nil { return err }
if err := fixture.CreateSyncPipe(ctx, selected, multicluster.RGWSyncPipeConfig{
    ID: "published", SourceZones: []string{"source"}, DestinationZones: []string{"destination"},
    Prefix: "published/",
}); err != nil { return err }
if err := fixture.ApplySyncGroup(ctx, selected); err != nil { return err }
if _, err := fixture.WaitBucketSyncPolicyReady(ctx, selected, "destination"); err != nil {
    return err
}
// 이제 S3 client로 published/ 아래 새 object를 씁니다.
// 이후 destination bytes와 bucket checkpoint를 별도로 검증합니다.
return nil
```

Zonegroup의 `ApplySyncGroup`은 정확한 current period와 모든 stored zonegroup을 비교하고, unrelated pending/staging 변경을 덮어쓰지 않습니다. Owned 변경만 publish한 뒤 attached zone에 period를 전달하고 gateway를 reload합니다. Bucket 변경은 native metadata에 동적으로 적용하므로 period commit과 gateway 재시작이 필요 없습니다. 새 multisite bootstrap은 secondary가 commit한 최종 topology와 source의 staging을 맞춥니다. `Run`이 fresh fixture를 구성하는 동안 RGW 설정은 constructor가 독점하며 외부 CLI의 concurrent configuration을 지원하지 않습니다. 반환 후 외부 staging 변경을 일반 apply에서 지우는 경로는 제공하지 않습니다.

## 선택·변환과 소유권

`RGWSyncPipeConfig`는 prefix, OR로 결합한 exact object tag pairs, priority, 별도 source/destination bucket, destination owner/storage class 및 user mode를 제공합니다. Prefix와 tags를 함께 지정하면 두 조건을 모두 만족해야 합니다. CLI의 comma 구분자 때문에 comma를 포함하는 tag key/value는 거부합니다. Tag를 삭제하거나 ACL을 부여하는 object CRUD는 S3 client의 역할입니다.

Bucket selector는 이름뿐 아니라 native instance ID를 캡처합니다. 같은 이름을 삭제·재생성하면 stale handle의 변경·제거·checkpoint를 거부합니다. Owner/user는 fixture가 생성해 key identity와 생성 시 account 연결을 확인한 principal이며, 선택한 bucket과 tenant가 일치해야 합니다. `DestinationOwner`는 ordinary principal만 허용합니다. `User`는 ordinary principal 또는 `CreateAccountRootUser`가 생성한 root principal을 받습니다. Account root는 양쪽의 concrete bucket이 같은 생성 소유 account에 속하고 owner translation이 없는 경우만 허용합니다. IAM non-root, cross-account 및 cross-tenant principal translation은 현재 helper의 검증 범위에 포함되지 않아 거부합니다. 이를 Ceph 자체의 지원 여부에 대한 주장으로 해석하지 않습니다.

Ceph 20.2.4의 S3 `ReplicationConfiguration` 변환 경로는 account-owned 요청을 `NotImplemented`로 거부합니다. CLI의 account user mode는 별도 native permission 경로입니다. Account root helper는 위의 좁은 조건으로 구현했으며 native 대표 recipe 결과는 아래 coverage 문서에서 별도로 추적합니다. [S3 account-owner 검사](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_rest_s3.cc#L1355-L1406), [native user-mode permission 평가](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_data_sync.cc#L2649-L2742).

System mode에서 concrete source/destination bucket selector의 tenant를 서로 다르게 지정할 수 있습니다. 이때 account/user나 destination owner 변환을 함께 지정하지 않습니다. Native replication은 source object ACL을 보존하므로 destination bucket의 owner와 복사된 object를 읽을 수 있는 principal이 다를 수 있습니다. 대표 recipe는 명시적 tenant 경로와 source principal로 replica bytes를 확인합니다.

Storage class 변환은 concrete destination bucket과 각 concrete destination zone의 confirmed placement handle을 요구합니다. 양쪽 zone의 pool/class mapping을 만들고 master publish·secondary pull·reload를 먼저 수행합니다. 저장된 mapping만으로 실제 replica class를 확인하지 않으며 S3 listing과 destination RADOS pool payload를 함께 검증합니다.

동일 ID의 기존 group을 adopt하지 않습니다. Handle 복사는 lifecycle 상태를 공유합니다. Confirmed mutation의 응답이 유실되면 pending intent와 native before/after 상태를 비교해 재시도하며 unrelated 변화가 있으면 거부합니다. 초기 생성의 readback이 불확실하면 unconfirmed handle로 자동 제거하지 않으며 native 조사 또는 disposable fixture 종료가 필요합니다. Group 제거는 이미 복사된 bucket/object를 삭제하지 않습니다. 외부 CLI와 fixture mutation이 동시에 같은 policy를 변경하면 안 됩니다.

## 관측과 데이터 검증

`WaitBucketSyncPolicyReady`는 source traffic 전에 destination-local metadata import를 확인하는 읽기 전용 prerequisite입니다. 생성·확인된 bucket-scoped group과 그 pipe가 선택한 explicit attached destination zone을 받으며, master의 unchanged owned group·committed period와 destination의 같은 period, source-scope group snapshot 및 필요한 concrete source/destination bucket instance ID를 비교합니다. `RGWBucketSyncPolicyStatus`의 `PeriodImported`·`PolicyImported`·`BucketsImported`가 모두 참이면 `Imported`가 참입니다. Wildcard bucket selector는 group이 캡처한 scope instance로 해석하며, 새 instance를 같은 이름으로 adopt하지 않습니다. Pending mutation, 제거된 handle, zonegroup-scoped group 또는 선택 범위 밖의 zone은 거부합니다.

이 wait는 mutex 대기를 포함해 최대 4분, 각 native read는 최대 30초이며 더 짧은 caller context를 따릅니다. Local metadata lag는 기다리지만 master policy 변경, 다른 realm/namespace 또는 재생성된 bucket은 거부합니다. Period pull·gateway restart·replay·marker 변경은 하지 않습니다. `Imported`는 effective peer-discovery hints, checkpoint, payload 또는 authorization의 성공을 증명하지 않으므로 아래 data 관측과 client 검증이 여전히 필요합니다.

`SyncStatus`·`WaitSyncReady`는 owned zone의 committed period, metadata shard 진행률 및 요청한 source의 native remote-log 비교를 확인합니다. Source 목록을 생략하면 metadata만 기다립니다. `BucketSyncStatus`·`WaitBucketSyncReady`는 owned group/pipe가 선택한 두 native bucket instance를 고정하고 두 endpoint의 exact imported period와 per-bucket checkpoint를 확인합니다. 반환값은 identity·state·shard count를 포함하고 native marker·secret·오류 본문은 제외합니다.

Status 조회는 최대 30초, wait는 최대 4분이며 caller context가 더 짧으면 그 제한을 따릅니다. Topology mutex 대기도 제한에 포함됩니다. Disabled/absent/full-sync/lagging 상태나 native per-source 오류는 CLI exit가 0이어도 readiness를 만족하지 않습니다. Gateway 재시작 후 이전 sync lease가 자연 만료될 수 있으며 helper가 lease TTL이나 lock을 임의로 바꾸지 않습니다.

Native bucket checkpoint는 같은 source/destination tuple의 여러 pipe를 합쳐 관측할 수 있습니다. 따라서 caught-up만으로 선택한 pipe의 filter·user mode·실제 데이터 성공을 증명하지 않습니다. Test client의 writes를 동기화하고 destination의 exact bytes와 제외 대상의 absence를 별도로 확인합니다. Policy 변경이 이전 데이터의 backfill 또는 이미 복사된 object 삭제를 보장하지 않습니다.

Forbidden에서 enabled로 바꾼 뒤 새 object는 복제되어도 이전에 중단한 shard 로그가 남을 수 있습니다. 이 경우 quiet fixture에서 `BucketSyncStatus`로 같은 period·source/destination bucket instance를 확인하고, `ZoneAdmin`으로 destination의 `bucket sync run`을 명시적으로 실행한 뒤 `WaitBucketSyncReady`와 실제 bytes를 다시 확인합니다. [실행 recipe](../internal/integration/rgw_sync_policy_integration_test.go)의 `reconcileOwnedBucketCheckpoint`가 이 순서를 제공합니다. Ceph 20.2.4 bucket sync 명령에는 `--source-zone`으로 캡처한 native ID를 전달합니다. `--source-zone-id`는 sync-policy 옵션이며 bucket sync의 legacy 입력을 채우지 않습니다. [Pinned CLI 입력·실행](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/radosgw-admin/radosgw-admin.cc#L10413-L10439).

이 명령은 현재 matching pipe들의 기존 로그를 재생하므로 delete도 처리하며, forbidden 기간의 쓰기가 재활성화된 policy로 복제될 수 있습니다. `bucket sync init`, marker 편집, log trim, 강제 unlock을 사용하지 않습니다. Stopped 상태의 exit 0은 no-op일 수 있어 readiness 증거가 아닙니다. [Native replay](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_data_sync.cc#L6260-L6513), [삭제 권한·처리](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_cr_rados.cc#L912-L978). Docker Exec의 context 취소는 실행 중인 native worker 종료를 보장하지 않습니다. 불확실한 action을 자동 재시도하지 않고 disposable fixture를 정리합니다.

실행 가능한 대표 recipe는 [기본 선택·lifecycle](../internal/integration/rgw_sync_policy_integration_test.go), [tag·priority·owner/class·user mode·tenant](../internal/integration/rgw_sync_translation_integration_test.go), [account root 권한 거부·복구 및 cross-tenant system mode](../internal/integration/rgw_sync_accounts_integration_test.go)입니다. Unit/tag compile과 Docker 통과 여부는 [G05/G07 진행 상태](CLIENT_FIXTURE_COVERAGE.md)에서 구분합니다.

2026-10-03 Ceph 20.2.4 role slim 이미지의 Linux Docker 검증에서 기본 선택·lifecycle recipe는 bridge 584.99초, host 715.96초로 cleanup까지 통과했습니다. 두 모드 모두 exact 67,584 bytes와 11 shard checkpoint, forbidden 동안 제외·enabled 복구, prefix 변경·삭제 복제, group 제거 후 기존 bytes 및 재생성 bucket 보존을 확인했습니다. 정책 재개 후 이전 로그가 남는 구간은 recipe 안의 명시적 native replay로 처리했으며 외부 수동 조작은 하지 않았습니다. 증거는 `artifacts/client-quota-auth-sync-reconcile-native.log`의 두 named PASS입니다. 이 로그 전체는 수정 전 RBD RO probe 실패도 포함하므로 전체 PASS로 해석하지 않습니다.

같은 날짜의 추가 검증에서 `TestHostNetworkMultiClusterRGWAccountRootSync`는 568.68초로 cleanup까지 통과했습니다. Same-account root의 58,368 bytes, 복제 action만 거부하는 destination `s3:ReplicateObject` Deny 동안의 native checkpoint 진행과 새 key의 absence, 정책 복원 후 새 bytes를 확인했습니다. 이어서 source tenant `system_alpha`의 object를 destination tenant `system_beta`로 복제하여 source owner의 명시적 tenant 주소로 48,128 bytes와 정확한 두 bucket instance의 11 shard checkpoint를 확인했습니다. 다른 prefix의 absence와 group 제거 후 기존 replica 보존도 통과했습니다. Bridge variant는 same-account 부분 이후 cross-tenant GET의 403 AccessDenied로 실패했습니다. 403은 object 부재를 가릴 수도 있어 원인을 확정하지 않으며, 실패 전에 exact destination instance의 native object 상태와 두 principal의 S3 응답을 수집하는 진단으로 재검증합니다.

진단을 추가한 고정 소스로 fresh bridge fixture를 재실행한 `TestMultiClusterRGWAccountRootSync`는 408.22초로 전체 cleanup까지 통과했습니다. Same-account Deny 동안 checkpoint가 진행하면서 key가 제외되고, 복원 후 58,368 bytes와 checkpoint를 확인했습니다. Cross-tenant의 48,128 bytes는 15.05초, 정확한 tenant/bucket instance의 11 shard checkpoint는 1.094초에 확인했습니다. Positive 판정이나 timeout은 바꾸지 않았습니다. 이 통과는 앞선 간헐 실패의 원인을 규명한 결과는 아니며, 실패 진단은 유지합니다. 증거는 `artifacts/rgw-account-cross-tenant-diagnostic-retry.log`입니다.

`TestMultiClusterRGWSyncTranslationFiltering`과 host variant는 모두 red object의 404 NoSuchKey로 실패했습니다. Native policy에는 같은 `published/` prefix에 priority 1의 blue→STANDARD와 priority 7의 blue OR red→STANDARD_IA가 정확히 저장되어 있었지만, Ceph 20.2.4의 tagged-object 선택 함수는 matching prefix 후보 하나에서 반복을 시작하여 앞의 동일 prefix 후보를 비교하지 않습니다. Pipe ID 순 재구성으로 blue-only pipe가 마지막에 들어가는 현재 관측과 일치합니다. [Native object parameter 선택](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_bucket_sync.cc#L397-L446), [pipe 재구성](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_bucket_sync.cc#L850-L867). ID 순서 변경만으로 PASS를 만들면 numeric priority의 효과를 증명하지 못하므로 해당 기준은 열어 둡니다. 이 실패 뒤에 있던 ordinary user와 same-tenant 조건도 아직 native 통과로 표시하지 않습니다. 증거는 `artifacts/client-native-final-gates.log`의 named 결과이며, 이 batch 전체는 실패했습니다.

현재 recipe는 `priority_tags_owner_class`, `tag_owner_class`, `ordinary_user_denial_grant`, `tenant_system_user_isolation`의 네 순차 subtest로 나눴습니다. 각 subtest의 owned bucket-scoped group을 다음 subtest 전에 bounded cleanup하며 판정·pipe ID·numeric priority는 그대로 유지합니다. 특정 조건만 다시 실행하려면 `-run` 뒤에 `/ordinary_user_denial_grant` 또는 `/tenant_system_user_isolation`을 추가할 수 있습니다. 선택하지 않은 조건의 통과를 주장하지 않습니다.

분리한 user·tenant 조건의 수정 전 native 실행은 bridge와 host 모두 실패했습니다. Ordinary user는 거부 동안 bucket checkpoint가 완료되지 않았으며 host 관측은 incremental/11 shards/behind 1이었습니다. Tenant system은 selected object의 404 NoSuchKey였고, 종료 전 destination metadata는 caught-up이 아니었습니다. Source GET의 EACCES는 native object sync에서 skip될 수 있으므로 checkpoint timeout을 권한 거부 proof로 처리하지 않습니다. [Native HTTP mapping](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_http_errors.h#L18-L44), [object skip와 marker](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_data_sync.cc#L4479-L4517). 증거는 `artifacts/rgw-user-tenant-independent-native.log`이며 이 batch는 전체 실패입니다.

User-mode source replication GET는 일반 GetObject 외에 GetObjectAcl도 요구합니다. Recipe는 처음에 ACL 권한만 부여하여 ACL GET 200과 payload GET 403 AccessDenied를 확인한 뒤, payload와 ACL 권한을 함께 허용하도록 보완했습니다. Native 거부 동안 35초 absence와 checkpoint 진행 조건은 유지합니다. 실패 시 owned group을 제거하기 전에 exact pipe/bucket checkpoint를 bounded readonly 진단으로 남깁니다. [Native replication GET 권한](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_op.cc#L1166-L1208).

Native peer discovery는 destination-local source bucket policy를 사용합니다. Global data caught-up만으로 새 bucket의 정책이 local metadata에 준비됐다는 것을 증명하지 않습니다. `WaitBucketSyncPolicyReady`로 정확한 native bucket ID와 owned policy import를 쓰기 전에 확인하도록 보강했으며 unit·race·tag compile·vet 검증을 통과했습니다. Metadata 준비가 위 실패의 유일한 원인이었는지는 아직 확정하지 않습니다. [Local peer discovery](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_data_sync.cc#L5582-L5673).

2026-10-04 import wait를 적용한 fresh bridge 실행에서 `tag_owner_class`는 77.33초, `tenant_system_user_isolation`은 96.58초로 owned cleanup까지 통과했습니다. Single tagged pipe의 blue/red 각각 124,928 bytes, ordinary destination owner의 접근과 source owner의 AccessDenied, STANDARD_IA native pool payload, 11 shard checkpoint, 제외 대상의 35초 NoSuchKey 및 group 제거 후 두 replica 보존을 확인했습니다. 동일한 UID/bucket 이름을 가진 두 tenant에서도 selected alpha의 system/user 각각 54,272 bytes와 exact tenant/bucket ID checkpoint, beta 및 다른 prefix 제외·cross-tenant principal translation 거부·이전 bytes 보존을 확인했습니다. 세 group의 쓰기 전 import 관측은 exact period와 필요한 native bucket instance를 확인했습니다. 증거는 `artifacts/rgw-policy-import-native-bridge.log`이며, 이 batch 전체는 아래 user-mode 실패 때문에 FAIL입니다.

같은 실행의 `ordinary_user_denial_grant`는 source의 ACL GET 200과 일반 payload GET 403 AccessDenied를 확인했지만 destination의 `before-grant` GET이 200이어서 23.24초로 실패했습니다. 이때 exact bucket checkpoint는 incremental/11 shards/behind 0이었습니다. Caught-up이나 metadata import 성공을 권한 거부 proof로 처리하지 않으며 positive grant 단계는 실행되지 않았습니다.

동일 소스의 fresh host variant도 `tag_owner_class` 77.41초와 `tenant_system_user_isolation` 96.31초로 같은 bytes·native pool/checkpoint·제외·권한·owned cleanup 조건을 통과했습니다. `ordinary_user_denial_grant`는 같은 예상 밖 destination GET 200과 behind 0으로 23.51초에 실패했습니다. 따라서 이 batch도 전체 FAIL이며 증거는 `artifacts/rgw-policy-import-native-host.log`입니다. 두 실행 후 테스트 소유의 container 정리를 확인했습니다.

Pinned Ceph 20.2.4에서는 인증 전 `rgwx-perm-check-uid`가 system argument map에만 존재하지만 LocalEngine은 일반 `get`으로 읽습니다. 일반 signed GET의 인증이 끝난 뒤에야 SysReqApplier가 `set_system`으로 이를 노출하므로 빈 UID가 non-impersonated system/admin 경로를 선택합니다. System GET 응답은 이 경우에도 `Rgwx-Perm-Checked`를 보내 destination의 legacy ACL fallback을 생략합니다. 이 소스 경로는 일반 GET 403과 복제 GET 200 관측을 설명하며, getter를 `sys_get`으로 바꾸는 native 패치의 실제 빌드·deny/grant 재검증은 아직 미완료입니다. [인증 getter](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_rest_s3.cc#L6950-L6976), [system argument 노출 시점](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_auth_filters.h#L299-L356), [destination fallback](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_rados.cc#L3837-L3846). Priority 패치와 함께 같은 native source/ABI로 컴파일하고 기존 client 판정을 그대로 재실행하기 전에는 G05/G07을 완료로 표시하지 않습니다.

2026-10-04 두 패치를 동일 source에 적용한 Ubuntu Noble ARM64 native 빌드의 네 target과 selector gtest 9개가 통과했습니다. Build tree에 의존하지 않는 relocated binary/library의 `ldd -r`도 통과했으며, RGW process에만 같은 빌드의 private library를 연결하는 23개 `.deb`를 생성했습니다. Ordered patch manifest SHA는 `eee526e5442002aa19ed7d6a7c8eeacf00270d7e7db5dc45e87a0562a946cee8`입니다. 실제 XML과 최초 lld 실패·bfd 재개 기록은 `artifacts/rgw-native-proof-20261004/`, package provenance는 그 아래 `package/package-report.json`에 보존합니다. Role image smoke와 unchanged client 재검증이 끝나기 전에는 G05/G07을 완료로 표시하지 않습니다.

수정 RGW role image를 기존 Quay-derived control/OSD/MDS와 조합한 Linux bridge `TestMultiClusterRGWSyncTranslationFiltering`은 535.21초로 네 subtest와 owned cleanup까지 통과했습니다. 원래 pipe ID/priority 1·7을 유지한 92,160 bytes의 blue/red 및 STANDARD_IA pool, 상위 pipe 제거·import 후 77,824 bytes의 STANDARD fallback을 확인했습니다. Ordinary user는 source payload 거부 동안 35초 destination absence와 11 shard checkpoint 진행을 확인했고, grant 후 새 59,392 bytes 및 group 제거 후 replica 보존을 통과했습니다. Single tag/owner/class와 same-tenant system/user 격리도 회귀 통과했습니다. `artifacts/rgw-native-patched-translation-bridge.log`와 소유 리소스 잔존이 없는 `artifacts/rgw-native-proof-20261004/bridge-cleanup.json`에 기록합니다. Host variant 및 같은 인증 경로의 account-root 재검증은 진행 중입니다.

다섯 Noble-derived role의 첫 자동 smoke는 official `ceph-mon --version` SIGILL로 실패했습니다. MON과 전역 libceph-common은 signed package 추출본과 byte 동일이며 patched private RGW library를 사용하지 않습니다. 진단은 libgcc/gperftools stack unwind의 AUTIA1716 pointer-auth fault를 확인했지만 근본 원인은 확정하지 않습니다. 해당 run을 전체 PASS로 표시하지 않습니다. 수정 RGW role의 독립 smoke는 통과하여 위 mixed-role 검증에 사용했습니다. Image ID·23개 package SHA·ordered patch·Go snapshot의 연결은 `artifacts/rgw-native-proof-20261004/runtime-proof.json`에 보존합니다.

```sh
CGO_ENABLED=0 go test -mod=readonly -count=1 -v \
  -tags=integration,features,multicluster ./internal/integration \
  -run '^Test(HostNetwork)?MultiClusterRGW(OwnedSyncPolicy|SyncTranslationFiltering|AccountRootSync)$' \
  -timeout 60m
```

Role slim 이미지는 다른 fixture와 같은 `CEPH_TEST_IMAGE`·`CEPH_TEST_OSD_IMAGE`·`CEPH_TEST_RGW_IMAGE` 환경 변수를 사용합니다. Host variant는 Docker Linux host networking이 필요합니다.
