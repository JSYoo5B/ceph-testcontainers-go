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
source, err := fs.CreateSubvolume(ctx, ceph.CephFSSubvolumeConfig{
    Name: "app-data", SizeBytes: 32 << 20, NamespaceIsolated: true,
})
if err != nil { return err }
// 소비자 client의 쓰기와 checkpoint를 먼저 동기화합니다.
snapshot, err := fs.CreateSubvolumeSnapshot(ctx, source, "checkpoint")
if err != nil { return err }
clone, err := fs.CloneSubvolumeSnapshot(ctx, snapshot, ceph.CephFSCloneConfig{
    Name: "restored-data",
})
if err != nil { return err }
restored, err := fs.WaitForSubvolumeClone(ctx, clone)
if err != nil { return err }
// restored.Path를 소비자 client에 전달합니다.
return fs.RemoveSubvolumeSnapshot(ctx, snapshot)
```

동일 이름의 기존 snapshot과 clone target을 거부합니다. `CephFSCloneConfig.GroupName`은 기존 target group을 선택하며 비어 있으면 default group입니다. `DataPool`은 filesystem에 등록된 data pool 중에서 선택합니다. 기본 clone은 snapshot의 quota·layout·RADOS namespace를 상속합니다. pool을 명시해서 override하면 native Ceph가 상속된 namespace를 비웁니다. clone quota는 복사 완료 후 적용됩니다. [native clone 생성 구현](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/mgr/volumes/fs/operations/versions/subvolume_v2.py).

`SubvolumeCloneStatus`는 `pending`, `in-progress`, `complete`, `failed`, `canceled`를 조회합니다. wait timeout은 복사를 취소하지 않으며 같은 handle로 다시 기다릴 수 있습니다. 실패·취소 상태는 오류를 즉시 반환하고 partial data를 보존합니다. 완료한 handle은 기존 `ResizeSubvolume`·`RemoveSubvolume`에 사용할 수 있습니다. 명시적 clone cancel과 실패 target의 `--force` 삭제는 `Ceph` escape hatch 또는 cluster 정리로 처리합니다.

`RemoveSubvolumeSnapshot`은 pending/orphan clone이 있는 source를 제거하지 않으며 `--force`를 사용하지 않습니다. snapshot 제거는 source data와 완료된 clone을 유지합니다. snapshot이 남은 source subvolume 자체의 제거도 native Ceph가 거부합니다. snapshot 복사 handle은 제거 상태를 공유하고, 불확실한 제거 응답은 목록과 identity를 다시 확인해 수렴시킵니다.

filesystem ID, subvolume UUID 경로·생성 시각, snapshot frozen 경로·생성 시각을 확인합니다. Ceph 20.2.4의 CLI는 pending/in-progress clone의 UUID를 제공하지 않고 완료 후 status에서 source 항목을 생략합니다. 첫 완료 확인 전에 외부 CLI가 동일 이름의 target을 삭제·재생성하면 안 됩니다. 완료 후에는 캡처한 target identity로 교체를 거부합니다. [native clone 상태와 접근 가능한 operation](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/mgr/volumes/fs/operations/versions/subvolume_v1.py), [volumes CLI](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/mgr/volumes/fs/volume.py).

snapshot 생성은 writer를 자동으로 quiesce하지 않습니다. 여러 client의 일관된 checkpoint가 필요하면 애플리케이션의 동기화 또는 별도의 native quiesce 절차를 먼저 적용합니다. [CephFS volumes와 snapshot/clone 계약](https://docs.ceph.com/en/tentacle/cephfs/fs-volumes/).

## RGW placement와 storage class

`CreatePlacement`는 실행 중인 gateway의 실제 realm/zonegroup/zone에 새 non-default target을 저장합니다. pool은 먼저 만들며 `STANDARD` class를 반드시 포함합니다. index와 multipart metadata pool은 replicated여야 하고 class data pool은 replicated 또는 EC를 사용할 수 있습니다. RGW의 완전한 object 쓰기용 EC data pool에는 RBD/CephFS의 EC overwrite 설정을 요구하지 않습니다.

```go
// indexPool, extraPool, standardPool, coldPool은 이미 생성한 pool입니다.
inline := false
placement, err := gateway.CreatePlacement(ctx, ceph.RGWPlacementConfig{
    Name: "app-tiered", IndexPool: indexPool, DataExtraPool: extraPool,
    InlineData: &inline,
    StorageClasses: []ceph.RGWStorageClassConfig{
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

그 다음 `multicluster` fixture의 `PullDestinationPeriod`로 상대 cluster에 period를 전달하고, 해당 gateway의 `ReloadPlacement`로 로컬 서비스를 재시작합니다. `ReloadPlacement`는 로컬의 committed period에 정확한 target·zonegroup·zone이 있는지, pool ID와 class mapping이 그대로인지 확인합니다. secondary zone에서도 사용할 수 있으며 publish나 peer 연결은 수행하지 않습니다. 재시작 후 `S3Endpoint`를 다시 조회합니다. 여러 destination을 구성한 경우 각 cluster에 period를 전달하고 각 gateway를 활성화합니다.

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

기존 target, zonegroup default, 사용자 default, 기존 bucket placement를 변경하지 않습니다. pool ID와 실제 runtime scope를 확인하며 sequential command의 partial 실패를 자동 rollback하지 않습니다. `PlacementStatus`는 incomplete mapping도 오류로 표시하므로 partial 생성의 자세한 조사는 `Admin`으로 수행합니다. confirmed creation만 활성화할 수 있습니다. restart/period commit 응답이 불확실하면 native 상태를 확인하고 처리해야 하며 policy/pool/bucket/data를 자동 삭제하지 않습니다.

`LocationConstraint`는 zonegroup API name과 target을 결합한 native 값입니다. bucket placement는 생성 이후 바꿀 수 없습니다. 사용자별 target 권한, S3 lifecycle transition, object CRUD와 SDK storage class 호환성은 소비자 client에서 설정·검증합니다. [RGW placement/storage class와 활성화 절차](https://docs.ceph.com/en/tentacle/radosgw/placement/).

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
| CephFS snapshot/clone | PASS, 133.49s | PASS, 132.58s | frozen bytes, source·clone 독립 쓰기, pending source 보호, wait timeout 재시도, quota·namespace 상속과 data pool override |
| RGW standalone placement | PASS, 87.71s | PASS, 87.37s | named bucket, STANDARD replicated·STANDARD_IA EC pool, S3 class 목록과 실제 RADOS payload, 기존/default bucket 정책 유지 |
| RGW realm placement | PASS, 279.51s | 이번 단계 미실행 | zone-local mapping, master publish·destination pull·reload, secondary publish 거부, unrelated staging 보존, 복제 class·bytes·실제 destination pool |

CephFS 데이터 검증은 Linux client container의 libcephfs로 수행했습니다. pending clone은 native clone delay로 유지해 helper와 native CLI 양쪽의 source 보호를 확인했습니다. RGW는 S3 GET만으로 판단하지 않고 각 지정 data pool의 native object에서 96 KiB payload를 읽어 SHA256까지 비교했습니다. RGW shadow object의 이름은 S3 key를 그대로 포함하지 않을 수 있습니다.

realm 복제는 native 설정 `lease=120`, metadata/data `poll=20`초를 유지한 채 100.452초 후 118,784 bytes가 일치했습니다. destination의 S3 listing에서 `STANDARD_IA`를 확인하고 실제 `tc-realm-ia` data pool의 payload SHA256까지 비교했습니다. 재시작 후 읽기 전용 native probe에서는 이전 locker `client.4248`의 `sync_lock`이 남아 있었고 새 RGW service ID는 `4425`였습니다. 기존 lock 만료 시각 `06:49:28.591 UTC` 뒤에 복제된 데이터가 조회되었습니다. HTTP readiness 직후 1분짜리 대기로 실패했던 경우와 구분해, 최종 테스트는 4분 안에 실제 복구를 확인합니다.

결과 로그는 `artifacts/cluster-config-cephfs-verified.log`, `artifacts/cluster-osd-policies.log`, `artifacts/cluster-rgw-placement-payload-final.log`, `artifacts/cluster-rgw-placement-realm-lease-final.log`에 남깁니다. native lock 관측은 `artifacts/rgw-placement-restart-lease-proof.json`에 기록합니다. artifacts는 Git에 포함하지 않습니다. 공개 모듈의 `CGO_ENABLED=0 go test ./...`, `go test -race ./...`, integration·features·auth·hostnetwork·topology·multicluster·goceph 전체 tag 컴파일과 vet도 통과했습니다. tag 컴파일은 기존 모든 Docker 시나리오의 재실행을 의미하지 않습니다.

최종 실행 후 running/stopped Docker container는 모두 정리됐고 전용 network도 남지 않았습니다. 기존 `kind` network는 유지했습니다.
