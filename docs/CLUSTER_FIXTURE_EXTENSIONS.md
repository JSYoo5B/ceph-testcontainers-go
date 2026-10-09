# 클러스터 내부 fixture 확장

이 단계는 기존 cluster 안에 테스트 조건과 서버 측 리소스를 준비합니다. topology API로 MON/MGR/OSD/MDS/RGW 배치를 만든 뒤 설정·장애 조건·snapshot 복구·S3 storage class를 구성합니다. 공개 모듈은 컨테이너 내부 Ceph CLI를 사용하고 go-ceph/cgo 의존성을 추가하지 않습니다.

애플리케이션의 RADOS object/omap, RBD image/snapshot/trash, POSIX 파일, S3 object/lifecycle CRUD는 소비자 client의 역할입니다. 여기서 추가하는 CephFS snapshot/clone은 MGR volumes의 서버 provisioning API입니다. 해당 리소스를 마운트해서 데이터를 읽고 쓰는 libcephfs wrapper는 제공하지 않습니다. cluster 간 peer·mirror·realm 구성은 계속 `multicluster` 패키지가 담당합니다.

## 임시 중앙 설정

`Configuration`은 MON database에 저장된 정확한 section/mask/name entry를 조회합니다. `TemporaryConfig`는 기존 entry와 absence를 구분해서 저장하고 `ConfigOverride.Restore`로 돌려놓습니다. `global`에서 상속받은 값을 해당 daemon의 명시적 entry로 복원하지 않습니다.

```go
change, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{
    Section: "osd", Mask: "class:ssd",
    Name: "osd_scrub_min_interval", Value: "7200.0000",
})
if err != nil {
    if change != nil {
        err = errors.Join(err, change.Restore(context.WithoutCancel(ctx)))
    }
    return err
}
// 소비자 client로 테스트 조건을 확인합니다.
return change.Restore(ctx)
```

Section은 `global`, daemon 종류, `osd.0` 같은 identity입니다. Mask는 CRUSH location과 device class를 선택합니다. `mgr/volumes/max_concurrent_clones` 같은 module 경로 키도 지원합니다. native 값 정규화 후 저장값을 기록하므로 숫자 입력 `7200.0000`과 native 저장 문자열의 차이를 구분합니다.

이 API가 확인하는 것은 저장값입니다. 로컬 `ceph.conf`, argv, runtime override가 우선할 수 있고 재시작이 필요한 옵션도 있습니다. daemon 전용 section만으로 다른 section의 기존 mask보다 우선한다고 가정하지 않습니다. 실행 daemon의 적용 결과는 `ceph config show <identity> <option>` 또는 admin socket으로 별도 확인합니다. [Ceph 설정의 우선순위와 중앙 database](https://docs.ceph.com/en/tentacle/rados/configuration/ceph-conf/).

같은 key의 중복 handle은 거부합니다. 복사한 handle은 복원 상태를 공유합니다. 복원 전 현재 entry가 기록한 적용값과 다르면 외부 변경을 보존하며 실패합니다. Ceph에는 config CAS가 없으므로 외부 쓰기가 apply/readback/restore와 경쟁하면 안 됩니다. 외부에서 같은 값을 다시 쓴 경우는 구분할 수 없습니다.

명령 응답이 유실되거나 readback이 실패하면 non-nil handle과 오류를 함께 반환할 수 있습니다. handle을 버리지 말고 복원하거나 일회성 cluster를 정리합니다. 복원 응답이 유실된 경우 재시도는 native 상태를 먼저 확인합니다. apply의 정규화된 값을 확인하지 못했고 요청 문자열과 저장값도 다른 경우에는 자동 복원을 거부할 수 있으므로 native 상태를 확인한 뒤 명시적으로 처리합니다. 설정 출력에 비밀 값이 들어갈 수 있으므로 전체 dump를 그대로 로그에 남기지 않습니다.

## OSDMap 정책과 PG 복구

`OSDStates`는 fixture가 생성한 OSD의 native ID·UUID·up/in·override weight를 조회합니다. `SetOSDIn`은 등록 UUID를 확인해 native in/out을 변경합니다. out은 daemon 정지나 제거가 아니므로 같은 container와 OSD identity를 유지합니다.

```go
osd := cluster.OSDs()[0]
flag, err := cluster.TemporaryOSDFlag(ctx, "noout", true)
if err != nil {
    if flag != nil {
        err = errors.Join(err, flag.Restore(context.WithoutCancel(ctx)))
    }
    return err
}
if err := cluster.SetOSDIn(ctx, osd.ID, false); err != nil { return err }
// 소비자 client에서 replica read/write와 장애 조건을 확인합니다.
if err := cluster.SetOSDIn(ctx, osd.ID, true); err != nil { return err }
if err := flag.Restore(ctx); err != nil { return err }
return cluster.WaitForPGClean(ctx)
```

OSDMap weight는 CRUSH weight와 다릅니다. out은 map weight를 0으로 만들고 native in은 기록된 이전 weight를 복구합니다. 이 helper가 CRUSH 용량 가중치를 바꾸거나 OSD를 purge하지는 않습니다. `noout`은 MON의 자동 out을 억제하며 명시적 `SetOSDIn(false)`를 막지 않습니다. 외부 OSD 제거·동일 ID 재생성을 UUID로 구분하며, raw reweight/in/out이 이 동작과 경쟁하면 안 됩니다. [Ceph OSD 제어와 두 weight의 차이](https://docs.ceph.com/en/tentacle/rados/operations/control/).

`OSDFlags`는 정확한 global token 목록을 조회합니다. `TemporaryOSDFlag`는 `noup`, `nodown`, `noout`, `noin`, `nobackfill`, `norebalance`, `norecover`, `noscrub`, `nodeep-scrub`, `nosnaptrim`, `noautoscale`을 지원하며 기존 상태를 `OSDFlagOverride.Restore`로 복원합니다. 기존 `noout`을 원래 없었던 것으로 처리하지 않으며 다른 flag는 유지합니다. 동일 flag의 중복 handle을 거부하고 복사한 handle은 복원 상태를 공유합니다. 응답 유실 시 non-nil handle을 유지해 native 상태를 확인하고 재시도하거나 cluster를 정리합니다. 외부의 같은 값 재설정과 조회·변경 사이 경쟁은 구분할 수 없습니다.

이번 API는 global flag만 다루며 OSD/group/device-class scoped flag와 `pauserd`·`pausewr`를 함께 바꾸는 `pause`는 포함하지 않습니다. `WaitForPGClean`은 최소 한 개의 PG와 available MGR이 있을 때 모든 PG가 정확히 `active+clean`인지 기다립니다. MGR 보고값을 확인하는 방법으로, 특정 OSDMap epoch까지 모든 PG가 처리했다는 barrier는 아닙니다. out/down OSD와 topology를 검사하지 않으므로 테스트 의도상 flag로 발생한 health warning과 전체 `HEALTH_OK` 여부는 별도로 확인합니다. [native OSD 명령 계약](https://docs.ceph.com/en/tentacle/api/mon_command_api/).

## CephFS snapshot과 clone

기존 filesystem의 owned subvolume에서 snapshot을 생성합니다. `SubvolumeSnapshots`는 native 목록, `SubvolumeSnapshotInfo`는 frozen data 경로와 pending clone 정보를 조회합니다. 외부에서 생성한 snapshot을 조회하더라도 제거·clone 소유권이 생기지 않습니다.

```go
source, err := fs.CreateSubvolume(ctx, cephfs.SubvolumeConfig{
    Name: "app-data", SizeBytes: 32 << 20, NamespaceIsolated: true,
})
if err != nil { return err }
// 소비자 client의 쓰기와 checkpoint를 먼저 동기화합니다.
snapshot, err := fs.CreateSubvolumeSnapshot(ctx, source, "checkpoint")
if err != nil { return err }
clone, err := fs.CloneSubvolumeSnapshot(ctx, snapshot, cephfs.CloneConfig{
    Name: "restored-data",
})
if err != nil { return err }
restored, err := fs.WaitForSubvolumeClone(ctx, clone)
if err != nil { return err }
// restored.Path를 소비자 client에 전달합니다.
return fs.RemoveSubvolumeSnapshot(ctx, snapshot)
```

동일 이름의 기존 snapshot과 clone target을 거부합니다. `cephfs.CloneConfig.GroupName`은 기존 target group을 선택하며 비어 있으면 default group입니다. `DataPool`은 filesystem에 등록된 data pool 중에서 선택합니다. 기본 clone은 snapshot의 quota·layout·RADOS namespace를 상속합니다. pool을 명시해서 override하면 native Ceph가 상속된 namespace를 비웁니다. clone quota는 복사 완료 후 적용됩니다. [native clone 생성 구현](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/mgr/volumes/fs/operations/versions/subvolume_v2.py).

`SubvolumeCloneStatus`는 `pending`, `in-progress`, `complete`, `failed`, `canceled`를 조회합니다. wait timeout은 복사를 취소하지 않으며 같은 handle로 다시 기다릴 수 있습니다. 실패·취소 상태는 오류를 즉시 반환하고 partial data를 보존합니다. 완료한 handle은 기존 `ResizeSubvolume`·`RemoveSubvolume`에 사용할 수 있습니다. `CancelSubvolumeClone`은 owned clone만 취소하며 `RemovePartialSubvolumeClone`은 native canceled/failed 상태와 target identity를 다시 확인한 뒤 명시적으로 정리합니다. source나 성공한 clone은 삭제하지 않습니다.

`RemoveSubvolumeSnapshot`은 pending/orphan clone이 있는 source를 제거하지 않으며 `--force`를 사용하지 않습니다. snapshot 제거는 source data와 완료된 clone을 유지합니다. snapshot이 남은 source subvolume 자체의 제거도 native Ceph가 거부합니다. snapshot 복사 handle은 제거 상태를 공유하고, 불확실한 제거 응답은 목록과 identity를 다시 확인해 수렴시킵니다.

filesystem ID, subvolume UUID 경로·생성 시각, snapshot frozen 경로·생성 시각을 확인합니다. Ceph 20.2.4의 CLI는 pending/in-progress clone의 UUID를 제공하지 않고 완료 후 status에서 source 항목을 생략합니다. 첫 완료 확인 전에 외부 CLI가 동일 이름의 target을 삭제·재생성하면 안 됩니다. 완료 후에는 캡처한 target identity로 교체를 거부합니다. [native clone 상태와 접근 가능한 operation](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/mgr/volumes/fs/operations/versions/subvolume_v1.py), [volumes CLI](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/mgr/volumes/fs/volume.py).

snapshot 생성은 writer를 자동으로 quiesce하지 않습니다. 여러 client의 일관된 checkpoint가 필요하면 애플리케이션의 동기화 또는 별도의 native quiesce 절차를 먼저 적용합니다. [CephFS volumes와 snapshot/clone 계약](https://docs.ceph.com/en/tentacle/cephfs/fs-volumes/).

## RGW placement와 storage class

`CreatePlacement`는 실행 중인 gateway의 실제 realm/zonegroup/zone에 새 non-default target을 저장합니다. pool은 먼저 만들며 `STANDARD` class를 반드시 포함합니다. index와 multipart metadata pool은 replicated여야 하고 class data pool은 replicated 또는 EC를 사용할 수 있습니다. RGW의 완전한 object 쓰기용 EC data pool에는 RBD/CephFS의 EC overwrite 설정을 요구하지 않습니다.

```go
// indexPool, extraPool, standardPool, coldPool은 이미 생성한 pool입니다.
inline := false
placement, err := gateway.CreatePlacement(ctx, rgw.PlacementConfig{
    Name: "app-tiered", IndexPool: indexPool, DataExtraPool: extraPool,
    InlineData: &inline,
    StorageClasses: []rgw.StorageClassConfig{
        {Name: "STANDARD", DataPool: standardPool},
        {Name: "STANDARD_IA", DataPool: coldPool},
    },
})
if err != nil { return err }
if err := gateway.ApplyPlacement(ctx, placement); err != nil { return err }
endpoint, err := gateway.S3Endpoint(ctx)
if err != nil { return err }
// S3 CreateBucket의 LocationConstraint에 placement.LocationConstraint를,
// PutObject의 StorageClass에 STANDARD_IA를 전달합니다.
_ = endpoint
```

저장과 활성화를 분리합니다. `PlacementStatus`는 native policy를 조회하며 실행 gateway가 이를 reload했다고 주장하지 않습니다. standalone의 `ApplyPlacement`는 해당 owned gateway를 재시작하므로 endpoint를 다시 구합니다. 같은 zone의 다른 gateway도 각자 활성화해야 합니다. 시작 시 선택할 native defaults가 다른 zone으로 바뀌었으면 재시작을 거부합니다.

realm 구성에서는 현재 metadata master만 `ApplyPlacement`로 period를 publish할 수 있습니다. 생성 당시 current period와 zone/zonegroup을 다시 확인합니다. 기존 staging의 정책을 먼저 읽어 관련 없는 변경이 있으면 `period update` 전에 거부하므로 unpublished 변경도 보존합니다. 새 staging 역시 owned target 추가 외의 변경이 있으면 commit을 거부합니다. 다른 zone들의 compatible local mapping을 먼저 준비하고 master에서 publish합니다.

그 다음 `rgw.Multisite`의 `PullDestinationPeriod`로 상대 cluster에 period를 전달하고, 해당 gateway의 `ReloadPlacement`로 로컬 서비스를 재시작합니다. `ReloadPlacement`는 로컬의 committed period에 정확한 target·zonegroup·zone이 있는지, pool ID와 class mapping이 그대로인지 확인합니다. secondary zone에서도 사용할 수 있으며 publish나 peer 연결은 수행하지 않습니다. 재시작 후 `S3Endpoint`를 다시 조회합니다. 여러 destination을 구성한 경우 각 cluster에 period를 전달하고 각 gateway를 활성화합니다.

```go
// 양쪽 CreatePlacement를 완료한 뒤 metadata master에서 publish합니다.
if err := fixture.Source.ApplyPlacement(ctx, sourcePlacement); err != nil { return err }
if err := fixture.PullDestinationPeriod(ctx); err != nil { return err }
if err := fixture.Destination.ReloadPlacement(ctx, destinationPlacement); err != nil { return err }
destinationEndpoint, err := fixture.Destination.S3Endpoint(ctx)
if err != nil { return err }
_ = destinationEndpoint
```

HTTP readiness와 sync 완료는 별개입니다. gateway reload/재시작 뒤에는 기존 sync lease의 만료와 metadata/data 복구를 기다려야 할 수 있습니다. Ceph 20.2.4의 기본 lease는 120초이며, sync coroutine의 abort 경로는 unlock을 건너뛸 수 있습니다. 복제 검증은 native lease·poll 범위를 고려한 deadline 안에서 실제 destination bytes를 확인합니다. [기본 sync 설정](https://github.com/ceph/ceph/blob/v20.2.4/src/common/options/rgw.yaml.in), [native lease 종료 경로](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_cr_rados.cc).

placement 생성·활성화는 기존 target, zonegroup default, 사용자 default, 기존 bucket placement를 변경하지 않습니다. 사용자 default 변경은 아래 `SetUserPlacement`의 명시적인 요청으로 수행합니다. pool ID와 실제 runtime scope를 확인하며 sequential command의 partial 실패를 자동 rollback하지 않습니다. `PlacementStatus`는 incomplete mapping도 오류로 표시하므로 partial 생성의 자세한 조사는 `Admin`으로 수행합니다. confirmed creation만 활성화할 수 있습니다. restart/period commit 응답이 불확실하면 native 상태를 확인하고 처리해야 하며 policy/pool/bucket/data를 자동 삭제하지 않습니다.

`LocationConstraint`는 zonegroup API name과 target을 결합한 native 값입니다. bucket placement는 생성 이후 바꿀 수 없습니다. 사용자별 target 권한은 `SetUserPlacement`, S3 lifecycle transition, object CRUD와 SDK storage class 호환성은 소비자 client에서 설정·검증합니다. [RGW placement/storage class와 활성화 절차](https://docs.ceph.com/en/tentacle/radosgw/placement/).

## 정확한 client session 차단

native librados의 연결 후 `GetAddrs`에 해당하는 session 주소를 `TemporaryBlocklist(ctx, address, duration)`에 전달합니다. `IP:port/nonzero-nonce`를 요구하므로 같은 host의 다른 client를 함께 차단하지 않습니다. 동일 session을 나타내는 v1/v2 vector도 허용하며 bare IP·CIDR·nonce zero·이미 존재하는 entry는 쓰기 전에 거부합니다.

```go
fence, err := cluster.TemporaryBlocklist(ctx, nativeClientAddress, time.Minute)
if err != nil {
    if fence != nil { err = errors.Join(err, fence.Restore(context.WithoutCancel(ctx))) }
    return err
}
// 해당 session의 native I/O 실패와 다른 session의 정상 I/O를 확인합니다.
return fence.Restore(ctx)
```

`BlocklistEntries`는 외부 exact entry와 CIDR range까지 조회하지만 제거 소유권을 부여하지 않습니다. handle은 native expiration을 캡처하며 외부 TTL 갱신을 덮어쓰지 않습니다. unknown readback의 present entry는 자동 제거를 거부하고 natural expiry는 absence로 수렴시킵니다. 복사한 handle도 제거 상태를 공유합니다. blocklist 해제 후 세션 복구 방식은 client library에 따릅니다. Ceph 20.2.4 librados에서는 기존 세션의 write/read 복구와 새 nonce 연결 모두 실제 검증했습니다.

## MGR module 제어와 readiness

`MGRModules`는 enabled/always-on/force-disabled membership과 active MGR의 available/can-run/dependency 진단을 함께 조회합니다. `TemporaryMGRModule(ctx, name, enabled)`의 `Restore`는 이전 membership을 복원하며 readback 과도기를 기다립니다. always-on disable, unavailable dependency enable, overlapping handle, 사용 중인 filesystem의 volumes 또는 native mirror policy가 있는 mirroring disable은 거부합니다.

Module 변경은 MON의 응답 뒤 active MGR 재시작을 일으킬 수 있습니다. 변경·복원 API는 기존 context/startup timeout 안에서 첫 native snapshot도 재조회하며, 취소 이후 도착한 snapshot으로 변경을 허용하지 않습니다. 재시도 대상은 읽기이며 변경 명령을 반복하지 않습니다. `MGRModules` 자체는 단발 snapshot이므로 readiness가 필요한 경우 `WaitMGRModuleReady`를 사용합니다.

Ceph 20.2.4의 `rbd_support`와 `volumes`는 always-on입니다. enable true의 no-op lease는 허용하지만 임의 force-disable로 조건을 가장하지 않습니다. `WaitMGRModuleReady(ctx, "rbd_support")`는 실제 `ceph rbd task list`와 snapshot schedule list를, `volumes`는 `fs volume ls`를 probe합니다. 다른 module은 intended command를 caller가 확인합니다. configured membership과 작업 완료는 별도입니다. [MGR 관리 계약](https://docs.ceph.com/en/tentacle/mgr/administrator/).

## 실행 중 CephFS data pool 추가

```go
attachment, err := fs.AddDataPool(ctx, "app-ec-data")
if err != nil { return err }
subvolume, err := fs.CreateSubvolume(ctx, cephfs.SubvolumeConfig{
    Name: "dynamic-data", DataPool: attachment.Name, NamespaceIsolated: true,
})
if err != nil { return err }
_ = subvolume
pools, err := fs.DataPools(ctx)
_ = pools
return err
```

pool은 미리 만들며 EC에는 `AllowOverwrites`가 필요합니다. `DataPools`는 native FSMap의 현재 membership을 조회합니다. subvolume/group/clone의 pool 검증도 live membership을 사용하며 초기 옵션의 whitelist에 제한되지 않습니다. filesystem 생성 때 native FSID·metadata/default pool ID를 캡처하고 모든 이후 provisioning에서 재생성을 거부합니다.

`RemoveUnusedDataPool`은 이번 helper가 추가한 attachment 중 provisioning에 사용한 적 없고 native group/subvolume/clone 참조와 모든 RADOS namespace의 데이터가 없는 것만 해제합니다. default pool을 제거하지 않습니다. raw POSIX layout reference를 native MON이 전부 검사하는 것은 아니므로 외부 writer/layout 변경은 caller가 중지해야 합니다. native application tag는 남으며 pool/data를 삭제하지 않습니다. 사용한 pool은 subvolume가 제거된 뒤에도 이 좁은 API로 제거하지 않습니다.

## RGW user placement 정책

새 placement의 `rgw.PlacementConfig.Tags`에 required tags를 지정하고 활성화한 뒤, owned 사용자에 기본 target/class와 허용 tags를 지정합니다.

```go
err := gateway.SetUserPlacement(ctx, user, placement, rgw.UserPlacementConfig{
    StorageClass: "STANDARD_IA", Tags: []string{"app-tier"},
})
```

nil Tags는 기존 tags를 유지합니다. nonempty 목록은 정확히 교체합니다. native CLI의 빈 tags 값은 clearing이 아니므로 empty non-nil 목록은 거부합니다. target 접근을 취소하려면 matching tags를 nonmatching tags로 교체합니다. 이 정책은 새 bucket placement 권한이며 기존 bucket의 object 권한·placement를 취소하는 API가 아닙니다. 기본 class는 class header가 없는 새 object의 data pool 선택에 영향을 줍니다. 관련 없는 user caps/quota/flags/keys는 native readback으로 유지 여부를 확인합니다.

native CLI는 기본 `STANDARD` class를 빈 문자열로 정규화합니다. `UserInfo`는 실제 문자열을 반환하고 policy readback 비교는 두 값을 같은 기본 class로 취급합니다. tenant 사용자 CLI의 `user_id`는 `tenant$uid` 전체이며 REST AdminOps serializer의 별도 tenant/local user ID 형식과 다릅니다. user handle은 canonical identity로 모든 이후 native 작업을 수행합니다.

User handle은 생성한 key와 원래 native user type/account 연결을 함께 고정합니다. 외부 CLI가 key를 유지한 채 ordinary user를 account root로 이동하거나 account 연결을 바꾸면 조회·suspend·quota·placement·제거를 거부합니다. `CreateAccountRootUser`의 handle은 생성 당시 owned account의 metadata lifetime tag와 공유 lifecycle도 확인합니다. Account를 같은 ID로 재생성하거나 제거한 뒤 이전 root handle로 새 account를 adopt하지 않습니다. 불확실한 생성 응답에서 ownership을 확인하지 못한 handle도 이후 coherent 응답만으로 소유권을 얻지 않습니다.

## CephFS quiesce checkpoint

```go
pause, err := fs.QuiesceSubvolumes(ctx, []*cephfs.Subvolume{first, second},
    cephfs.QuiesceConfig{Timeout: 20*time.Second, Expiration: time.Minute})
if err != nil { return err }
// 여러 client의 durable writes가 멈춘 동안 snapshot을 만듭니다.
snapshot, err := fs.CreateSubvolumeSnapshot(ctx, first, "checkpoint")
if err != nil { return err } // native expiration이 I/O를 자동 복구합니다.
_ = snapshot
return pause.Release(ctx)
```

`QuiesceSubvolumes`는 confirmed owned subvolume들의 FSID·pool ID·UUID path와 생성 시간을 다시 확인하고 고유 set을 `--if-version=0`으로 생성합니다. finite timeout/expiration은 필수입니다. `Status`는 readonly query로 TTL을 연장하지 않습니다. `Release`는 캡처한 QUIESCED version에 native optimistic concurrency를 적용하고 복사한 handle과 완료 상태를 공유합니다. 외부 변경·EXPIRED·TIMEDOUT은 consistent checkpoint로 간주하지 않습니다. uncertain create의 non-nil handle을 유지하거나 native TTL 복구 후 cluster를 정리합니다. 다른 overlapping set을 취소하지 않습니다. [native quiesce 계약](https://docs.ceph.com/en/tentacle/cephfs/fs-volumes/#subvolume-quiesce).

## 실행

```sh
make cluster-feature-extensions
```

대표 Docker 시나리오를 순차 실행합니다. 기존 role 이미지에 맞춰 `CEPH_TEST_IMAGE`, `CEPH_TEST_OSD_IMAGE`, `CEPH_TEST_MDS_IMAGE`, `CEPH_TEST_RGW_IMAGE`를 지정할 수 있습니다. host 모드는 Docker engine의 host networking과 native advertised endpoint에 접근 가능한 Linux client 환경을 요구합니다. macOS에서 라이브러리를 사용하는 경우 native client 검증은 Docker Linux 안에서 실행합니다. 실제 적용 결과와 데이터 경로는 별도의 PoC 결과로 기록합니다.

## 실제 검증 기록

2026-10-03, Ceph 20.2.4 role slim 이미지와 Docker Desktop Linux ARM64 engine 29.8.1에서 검증했습니다. 리소스는 4 CPU·3916 MiB였으며 Docker 시나리오는 순차 실행했습니다. 아래 시간은 cleanup을 포함합니다.

| 시나리오 | bridge | host network | 확인한 실제 동작 |
| --- | --- | --- | --- |
| 중앙 config | PASS, 65.39s | PASS, 67.28s | absence·상속·mask·기존 entry 복원, native 숫자 정규화, 외부 변경 보존, runtime 조회와 RADOS I/O |
| OSD 정책 | PASS, 117.99s | PASS, 111.44s | explicit out/in 후 PG 복구, 같은 stopped OSD의 noout 유지와 flag 복원 후 자동 out, 재시작 후 동일 UUID·데이터 유지 |
| pool replica/quota 정책 | PASS, 74.47s | PASS, 74.30s | 독립 OSD map과 양수 pool ID 교차 확인, replicas 2→3→2·quota 거부/복구 후 원래 ID·bytes·CRUSH와 unrelated pool 보존 |
| CephFS subvolume/group quota | PASS, 78.89s | PASS, 78.17s | 각각 native EDQUOT(122) 쓰기 거부, 기존 256 KiB·unlimited neighbor 보존, quota 확장 후 fresh session의 2 MiB 쓰기 복구·owned cleanup |
| CephFS snapshot/clone | PASS, 133.49s | PASS, 132.58s | frozen bytes, source·clone 독립 쓰기, pending source 보호, wait timeout 재시도, quota·namespace 상속과 data pool override |
| RGW standalone placement | PASS, 87.71s | PASS, 87.37s | named bucket, STANDARD replicated·STANDARD_IA EC pool, S3 class 목록과 실제 RADOS payload, 기존/default bucket 정책 유지 |
| RGW realm placement | PASS, 279.51s | 이번 단계 미실행 | zone-local mapping, master publish·destination pull·reload, secondary publish 거부, unrelated staging 보존, 복제 class·bytes·실제 destination pool |
| client nonce fencing | PASS, 47.66s | PASS, 47.05s | 두 session 중 한 nonce만 ESHUTDOWN, 기존·새 session 복구, TTL 만료, unrelated exact/range 보존 |
| MGR module | PASS, 73.92s | PASS, 74.12s | 실제 task 완료, module readiness·membership 복원, always-on/사용 중 mirror·volumes 보호 |
| native pool 재생성 guard | PASS, 35.39s | PASS, 35.32s | 같은 이름의 pool ID 변경 검출, stale namespace handle 거부, 새 namespace 보존 |
| RADOS client recipe | PASS, 27.36s | PASS, 27.47s | compound atomicity, xattr/omap, cls_hello 실행, watch/notify, snapshot, 3-object striper payload |
| CephFS 동적 data pool | PASS, 108.91s | PASS, 103.16s | live replicated/EC 등록·2 MiB 데이터/clone, unused detach, native pool 7→8 재생성 시 stale add/remove identity 거부·replacement sentinel와 전체 FSMap IDs·기존 bytes 보존 |
| CephFS canceled/failed clone | PASS, 204.58s | PASS, 209.80s | 실제 FAILED/EISDIR 주입과 cancel, source 보호 해제, partial 명시적 정리, 같은 이름 재생성 보존, frozen bytes와 독립 clone I/O |
| CephFS quiesce | PASS, 77.45s | PASS, 77.35s | 두 client 쓰기 정지·snapshot/head·release·EXPIRED 복구, exact PID SIGSTOP의 acquisition TIMEDOUT·SIGCONT 후 held/fresh durable I/O 복구, neighbor·외부 version 보존·owned cleanup |
| RGW user placement | PASS, 81.47s | PASS, 81.65s | required tags 거부·허용·취소, header 없는 class 선택과 실제 pool bytes, 기존 bucket/key/policy 유지 |
| CephFS metadata/retained snapshot | PASS, 64.54s | PASS, 72.35s | metadata 덮어쓰기·제거, source 삭제 후 snapshot 보존·복구, frozen bytes와 stale handle 거부 |
| CephFS export/distributed/random pin | PASS, 191.55s | PASS, 171.57s | export rank 선택·두 rank의 실제 group dirfrag/child subtree 분산, 16 file bytes 유지·원래 정책 복원 |
| CephFS subvolume authorization | PASS, 114.56s | PASS, 112.73s | native RO/RW·path/namespace 격리, revoke 후 strict permission denial·neighbor bytes/동일 key 보존, held session eviction |
| RBD mirror scope/namespace mapping | PASS, 357.32s | PASS, 353.97s | pool/image scope 및 default/named namespace 각 5조합, 신규 journal image 자동 편입·실제 bytes, sibling namespace 보존 |
| RBD automatic mirror snapshot schedule | PASS, 126.05s | PASS, 119.25s | MGR schedule만으로 source snapshot 증가·변경 destination bytes, 좁은 schedule 삭제와 기존 policy/head 보존 |
| RBD native client features | PASS, 119.24s | PASS, 118.69s | native layering/trash/migration/group/lock·LUKS1/2 format/load/rekey, exact bytes·IDs·strict key denial·namespace cleanup |
| RBD namespace/Cephx RO | PASS, 63.30s | PASS, 63.32s | 독립 namespace bytes·native image ID, RO open 읽기와 writable open/create/직접 RADOS write/foreign RO read 거부, RW client 유지·nonempty 보호·owned cleanup |
| RGW tenant/account quota | PASS, 66.28s | PASS, 68.28s | 같은 uid/bucket의 tenant 격리, account root 간 aggregate quota 실제 거부, nonpurge 보호·명시적 cleanup |
| RGW ordinary user quota/AdminOps | PASS, 52.31s | PASS, 53.31s | 개별 bucket quota가 꺼진 두 bucket의 aggregate 403 QuotaExceeded, sibling principal 쓰기 유지, 원래 quota·keys·bucket ID 복원과 동일 bytes 복구·명시적 cleanup |
| RGW bucket quota/reshard | PASS, 46.47s | PASS, 46.37s | 개별 quota 거부·해제 후 복구, queue 처리·11→17 idle shard, 네 payload와 sibling 정책 보존 |
| RGW S3 client features | PASS, 51.35s | PASS, 50.37s | frozen versions·multipart bytes·ACL/policy grant/revoke, scoped lifecycle, retention/hold 거부·명시적 bypass cleanup |
| RGW native TLS | PASS, 44.79s | PASS, 44.69s | Beast의 분리된 HTTP/HTTPS, 실제 CA/SAN/TLS12 검증·unknown CA 거부, 동일 S3 bytes·cleanup |
| RGW usage/rate-limit | PASS, 51.70s | PASS, 51.41s | 실제 usage category·bytes 기록, AdminOps caps 거부·UID trim 격리, bucket별 503 SlowDown·정책 복원·bytes 복구 |
| RGW STS/Swift/Vault KMS | PASS, 68.33s | PASS, 64.64s | 두 gateway의 임시 credentials·정책 revoke/restore, Swift/S3 동일 bytes와 key revoke, 실제 KMS key 삭제·복원·completed audit·owned cleanup |
| RGW owned selective sync 기본 | PASS, 584.99s | PASS, 715.96s | exact 67,584 bytes·11 shard checkpoint, bucket/prefix/reverse 제외, forbidden→enabled·prefix 변경 후 native replay, 삭제 복제·기존 bytes 보존·stale bucket guard·owned cleanup |

CephFS 데이터 검증은 Linux client container의 libcephfs로 수행했습니다. pending clone은 native clone delay로 유지해 helper와 native CLI 양쪽의 source 보호를 확인했습니다. RGW는 S3 GET만으로 판단하지 않고 각 지정 data pool의 native object에서 96 KiB payload를 읽어 SHA256까지 비교했습니다. RGW shadow object의 이름은 S3 key를 그대로 포함하지 않을 수 있습니다.

realm 복제는 native 설정 `lease=120`, metadata/data `poll=20`초를 유지한 채 100.452초 후 118,784 bytes가 일치했습니다. destination의 S3 listing에서 `STANDARD_IA`를 확인하고 실제 `tc-realm-ia` data pool의 payload SHA256까지 비교했습니다. 재시작 후 읽기 전용 native probe에서는 이전 locker `client.4248`의 `sync_lock`이 남아 있었고 새 RGW service ID는 `4425`였습니다. 기존 lock 만료 시각 `06:49:28.591 UTC` 뒤에 복제된 데이터가 조회되었습니다. HTTP readiness 직후 1분짜리 대기로 실패했던 경우와 구분해, 최종 테스트는 4분 안에 실제 복구를 확인합니다.

결과 로그는 `artifacts/cluster-config-cephfs-verified.log`, `artifacts/cluster-osd-policies.log`, `artifacts/cluster-rgw-placement-payload-final.log`, `artifacts/cluster-rgw-placement-realm-lease-final.log`에 남깁니다. native lock 관측은 `artifacts/rgw-placement-restart-lease-proof.json`에 기록합니다. artifacts는 Git에 포함하지 않습니다. 공개 모듈의 `CGO_ENABLED=0 go test ./...`, `go test -race ./...`, integration·features·auth·hostnetwork·topology·multicluster·goceph 전체 tag 컴파일과 vet도 통과했습니다. tag 컴파일은 기존 모든 Docker 시나리오의 재실행을 의미하지 않습니다.

client fixture 확장 로그는 `artifacts/client-fencing-final.log`, `artifacts/mgr-rados-fixtures-final.log`, `artifacts/client-dynamic-fs-rgw-rados-final.log`, `artifacts/cephfs-client-fixtures-final.log`, `artifacts/cephfs-pins-auth-rbd-schedule-final.log`, `artifacts/cephfs-quiesce-fixtures-final.log`, `artifacts/rgw-tenant-placement-fixtures-final.log`, `artifacts/rbd-scope-schedule-fixtures-final.log`에 있습니다. 일부 batch는 다른 미완료 시나리오의 실패도 포함하며 위 표는 각 이름의 개별 PASS 결과를 기록합니다. 전체 제공 기준은 [제공 기준 matrix](CLIENT_FIXTURE_COVERAGE.md)에서 별도로 관리합니다.

pool replica/quota, CephFS subvolume/group quota 및 ordinary RGW user quota의 보강 검증은 `artifacts/client-quota-auth-sync-reconcile-native.log`의 개별 PASS 결과입니다. 같은 batch의 RBD RO probe는 writable open에 필요한 watch 권한을 잘못 가정해 실패했으며 수정 후 별도 재검증한 PASS 결과는 아래에 기록합니다. 이 batch 전체를 PASS로 표시하지 않습니다.

같은 로그의 `TestMultiClusterRGWOwnedSyncPolicy`와 host variant도 각각 PASS입니다. 외부 수동 개입 없이 recipe 자체의 `bucket sync run`이 enabled 상태에서 남은 로그를 처리한 뒤 strict checkpoint를 확인했습니다. tag/priority/owner/class·user/account·cross-tenant 확장 검증은 이 기본 시나리오의 결과에 포함하지 않습니다.

CephFS 동적 data pool의 보강 결과는 `artifacts/client-native-final-gates.log`의 `TestCephFSDynamicDataPools/bridge`와 `/host` named PASS입니다. 원래 세 data pool의 이름·양수 ID·default flag를 재생성 전후 정확히 비교했고, 이전 handle의 거부가 단순 detach/nonempty 오류가 아니라 native identity 변경 때문인지 확인했습니다. 다른 후속 시나리오의 결과는 이 두 PASS와 구분합니다.

같은 로그의 `TestCephFSQuiesceCheckpoints`도 bridge/host 각각 PASS입니다. held libcephfs client의 정확한 PID·start ticks·argv를 확인한 뒤 SIGSTOP하여 timeout=3초, expiration=20초인 native set의 acquisition `TIMEDOUT`을 확인했습니다. 해당 상태는 consistent checkpoint로 release되지 않으며, neighbor I/O를 보존한 채 같은 process를 SIGCONT한 뒤 held session과 fresh session 모두 durable 쓰기/읽기를 복구했습니다. 이 proof는 정상 `QUIESCED`의 자연 `EXPIRED` TTL 복구와 별도로 실행합니다.

수정한 `TestRBDNamespaces`도 같은 로그에서 bridge/host 모두 PASS입니다. Cephx RO principal은 기본 writable open의 native watch 등록을 거부하므로 positive read는 read-only open으로 확인합니다. 이 handle의 local EROFS를 서버 권한 증거로 사용하지 않고, 기본 writable open·native image create·직접 RADOS write·다른 namespace의 read-only open에서 정확한 native EPERM/EACCES를 확인합니다. 기존 image ID·bytes와 RW principal 동작을 보존한 뒤 owned namespace/principal을 제거했습니다.

위 완료 시나리오의 종료 시점에는 작업 소유 running/stopped Docker container와 전용 network가 정리됐습니다. 기존 `kind` network는 유지했습니다. 후속 RGW native 패치 검증의 bridge/host·account-root 조건도 owned cleanup까지 통과했으며 최종 결과는 [선택적 복제 실행 기록](RGW_SYNC_POLICY.md)과 [제공 기준 matrix](CLIENT_FIXTURE_COVERAGE.md)에 연결합니다.
