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
return fixture.ApplySyncGroup(ctx, selected)
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

`SyncStatus`·`WaitSyncReady`는 owned zone의 committed period, metadata shard 진행률 및 요청한 source의 native remote-log 비교를 확인합니다. Source 목록을 생략하면 metadata만 기다립니다. `BucketSyncStatus`·`WaitBucketSyncReady`는 owned group/pipe가 선택한 두 native bucket instance를 고정하고 두 endpoint의 exact imported period와 per-bucket checkpoint를 확인합니다. 반환값은 identity·state·shard count를 포함하고 native marker·secret·오류 본문은 제외합니다.

Status 조회는 최대 30초, wait는 최대 4분이며 caller context가 더 짧으면 그 제한을 따릅니다. Topology mutex 대기도 제한에 포함됩니다. Disabled/absent/full-sync/lagging 상태나 native per-source 오류는 CLI exit가 0이어도 readiness를 만족하지 않습니다. Gateway 재시작 후 이전 sync lease가 자연 만료될 수 있으며 helper가 lease TTL이나 lock을 임의로 바꾸지 않습니다.

Native bucket checkpoint는 같은 source/destination tuple의 여러 pipe를 합쳐 관측할 수 있습니다. 따라서 caught-up만으로 선택한 pipe의 filter·user mode·실제 데이터 성공을 증명하지 않습니다. Test client의 writes를 동기화하고 destination의 exact bytes와 제외 대상의 absence를 별도로 확인합니다. Policy 변경이 이전 데이터의 backfill 또는 이미 복사된 object 삭제를 보장하지 않습니다.

Forbidden에서 enabled로 바꾼 뒤 새 object는 복제되어도 이전에 중단한 shard 로그가 남을 수 있습니다. 이 경우 quiet fixture에서 `BucketSyncStatus`로 같은 period·source/destination bucket instance를 확인하고, `ZoneAdmin`으로 destination의 `bucket sync run`을 명시적으로 실행한 뒤 `WaitBucketSyncReady`와 실제 bytes를 다시 확인합니다. [실행 recipe](../internal/integration/rgw_sync_policy_integration_test.go)의 `reconcileOwnedBucketCheckpoint`가 이 순서를 제공합니다. Ceph 20.2.4 bucket sync 명령에는 `--source-zone`으로 캡처한 native ID를 전달합니다. `--source-zone-id`는 sync-policy 옵션이며 bucket sync의 legacy 입력을 채우지 않습니다. [Pinned CLI 입력·실행](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/radosgw-admin/radosgw-admin.cc#L10413-L10439).

이 명령은 현재 matching pipe들의 기존 로그를 재생하므로 delete도 처리하며, forbidden 기간의 쓰기가 재활성화된 policy로 복제될 수 있습니다. `bucket sync init`, marker 편집, log trim, 강제 unlock을 사용하지 않습니다. Stopped 상태의 exit 0은 no-op일 수 있어 readiness 증거가 아닙니다. [Native replay](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_data_sync.cc#L6260-L6513), [삭제 권한·처리](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_cr_rados.cc#L912-L978). Docker Exec의 context 취소는 실행 중인 native worker 종료를 보장하지 않습니다. 불확실한 action을 자동 재시도하지 않고 disposable fixture를 정리합니다.

실행 가능한 대표 recipe는 [기본 선택·lifecycle](../internal/integration/rgw_sync_policy_integration_test.go), [tag·priority·owner/class·user mode·tenant](../internal/integration/rgw_sync_translation_integration_test.go), [account root 권한 거부·복구 및 cross-tenant system mode](../internal/integration/rgw_sync_accounts_integration_test.go)입니다. Unit/tag compile과 Docker 통과 여부는 [G05/G07 진행 상태](CLIENT_FIXTURE_COVERAGE.md)에서 구분합니다.

```sh
CGO_ENABLED=0 go test -mod=readonly -count=1 -v \
  -tags=integration,features,multicluster ./internal/integration \
  -run '^Test(HostNetwork)?MultiClusterRGW(OwnedSyncPolicy|SyncTranslationFiltering|AccountRootSync)$' \
  -timeout 60m
```

Role slim 이미지는 다른 fixture와 같은 `CEPH_TEST_IMAGE`·`CEPH_TEST_OSD_IMAGE`·`CEPH_TEST_RGW_IMAGE` 환경 변수를 사용합니다. Host variant는 Docker Linux host networking이 필요합니다.
