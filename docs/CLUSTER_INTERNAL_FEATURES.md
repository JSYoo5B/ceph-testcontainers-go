# 클러스터 내부 설정 API

토폴로지 단계 이후의 목표는 애플리케이션 테스트에 필요한 서버 측 리소스와 정책을 준비하는 것입니다. 첫 내부 설정 단계는 아래 5개 영역으로 구성합니다. 공개 모듈은 컨테이너 안의 Ceph CLI로 이를 제어하며 go-ceph/cgo 의존성을 추가하지 않습니다. 실제 객체·image·파일·S3 데이터 조작은 소비자 클라이언트가 담당합니다.

## 제공 범위

| 영역 | 공개 API | 역할 |
|---|---|---|
| Pool | `Pools`, `PoolStatus`, `SetPoolReplication`, `SetPoolQuota` | native pool ID·현재 정책 조회, replicated size/min-size 변경, logical byte/object quota 변경·해제 |
| Cephx | `ClientCapabilities`, `UpdateClientCaps` | 생성한 identity의 키를 유지하며 전체 caps 조회·교체 |
| RBD | `rbd.InitPool`, `rbd.CreateNamespace`, `rbd.ListNamespaces`, `rbd.RemoveNamespace` | replicated metadata pool 초기화, namespace 분리와 비어 있는 owned namespace 제거 |
| CephFS | `CreateSubvolumeGroup`, `CreateSubvolume`, 조회·목록·resize·remove | 기존 filesystem의 volumes 모듈, data pool/layout·namespace 선택, directory quota |
| RGW | `Admin`, `CreateUser`, `UserInfo`, `SetUserQuota`, `SetBucketQuota`, `SuspendUser`, `RemoveUser` | gateway 범위의 CLI, 일반 S3 계정, 명시적 Admin Ops caps와 quota·정지·복구 |

이미 제공하는 `CreatePool`/`WithPools`의 replicated/EC 설정과 CRUSH 배치, `CreateClient`/`WithClientIdentity`/`DeleteClient`를 함께 사용합니다. 이번 추가 기능은 cluster 간 연결을 만들지 않으며 `multicluster`의 peer·mirror·zone API는 별도로 유지합니다.

## Pool과 제한된 identity

```go
pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{
    Name: "app-data", Replicas: 2, MinSize: 1, Application: "rados",
})
if err != nil { return err }
if err := cluster.SetPoolReplication(ctx, pool.Name, 3, 2); err != nil { return err }
if err := cluster.WaitForClean(ctx); err != nil { return err }
if err := cluster.SetPoolQuota(ctx, pool.Name, ceph.PoolQuota{
    MaxBytes: 64 << 20, MaxObjects: 1000,
}); err != nil { return err }

client, err := cluster.CreateClient(ctx, "app", ceph.ClientCaps{
    Mon: "allow r", OSD: "allow rw pool=app-data namespace=tenant-a",
})
if err != nil { return err }
return cluster.UpdateClientCaps(ctx, client, ceph.ClientCaps{
    Mon: "allow r", OSD: "allow r pool=app-data namespace=tenant-a",
})
```

replication 변경은 현재 native CRUSH rule에서 충분한 owned osd/host/rack 도메인을 요구합니다. fixture가 이해하는 단순 take/choose/emit rule에 한정하며 EC shard 수를 변경하지 않습니다. 기존 pool descriptor는 생성 시점 값이므로 변경 후에는 `PoolStatus`를 읽습니다. 여러 설정 명령 중 일부만 성공할 수 있으며 자동 rollback·데이터 삭제는 하지 않습니다. 외부 CLI로 pool/CRUSH/auth를 동시에 바꾸지 않아야 합니다.

Pool quota의 0은 해당 제한 해제입니다. PG 통계 보고와 full flag 반영은 비동기이므로 엄격한 write별 byte accounting 경계로 사용하지 않습니다. Cephx caps 변경은 생략한 service 권한도 제거합니다. 이미 발급된 ticket에 이전 권한이 남을 수 있으므로 새 native connection으로 효과를 확인합니다.

## RBD namespace

```go
if err := rbd.InitPool(ctx, cluster, "rbd-metadata"); err != nil { return err }
namespace, err := rbd.CreateNamespace(ctx, cluster, "rbd-metadata", "tenant-a")
if err != nil { return err }
// librbd에서 pool=rbd-metadata, namespace=namespace.Name()를 사용합니다.
// image/snapshot/trash를 소비자 client로 정리한 후:
return rbd.RemoveNamespace(ctx, cluster, namespace)
```

pool은 미리 생성해야 합니다. EC pool은 image data에 사용할 수 있지만 RBD metadata pool로 초기화할 수 없습니다. CLI exit code뿐 아니라 native application과 초기화 object를 확인합니다. 제거는 owned descriptor와 native pool ID를 확인하고, Ceph가 nonempty namespace를 거부하도록 합니다. namespace에는 generation ID가 없으므로 활성 descriptor를 사용하면서 외부에서 동일 이름을 삭제·재생성하면 안 됩니다.

## CephFS subvolume

```go
group, err := fs.CreateSubvolumeGroup(ctx, cephfs.SubvolumeGroupConfig{
    Name: "app", SizeBytes: 128 << 20,
})
if err != nil { return err }
volume, err := fs.CreateSubvolume(ctx, cephfs.SubvolumeConfig{
    Name: "tenant-a", GroupName: group.Name, SizeBytes: 16 << 20,
    NamespaceIsolated: true,
})
if err != nil { return err }
// volume.Path가 native client의 실제 mount 경로입니다.
return fs.ResizeSubvolume(ctx, volume, 32 << 20)
```

기존 filesystem/MDS/pool을 사용하며 새 filesystem이나 orchestrator service를 만들지 않습니다. `DataPool`은 해당 filesystem에 등록된 data pool 중에서 선택합니다. namespace isolation만으로 접근 권한이 생기거나 제한되는 것은 아니므로 Cephx caps도 함께 구성해야 합니다. quota resize의 0은 unlimited이고, `--no_shrink`로 현재 사용량 아래의 축소를 거부합니다. group은 비어 있어야 제거되며, subvolume 제거는 그 directory tree와 내용을 삭제합니다. snapshots가 있으면 native 제거가 거부되고 trash purge는 비동기입니다. snapshot/clone 관리 API는 [fixture 확장](CLUSTER_FIXTURE_EXTENSIONS.md)에 정리합니다.

## RGW 사용자 정책

```go
user, err := gateway.CreateUser(ctx, rgw.UserConfig{
    ID: "app-user", AdminCaps: "users=read;usage=read",
})
if err != nil { return err }
accessKey, secretKey, err := user.Credentials()
if err != nil { return err }
// S3 client에 gateway.S3Endpoint(ctx)와 자격 증명을 전달합니다.
_, _ = accessKey, secretKey
if err := gateway.SetUserQuota(ctx, user, rgw.Quota{
    Enabled: true, MaxSizeBytes: 64 << 20, MaxObjects: 1000,
}); err != nil { return err }
return gateway.SuspendUser(ctx, user, true)
```

기본 사용자는 일반 S3 계정이며 system/global admin flag를 부여하지 않습니다. Admin Ops capability는 명시적으로 요청합니다. CLI는 RGW 역할 이미지 안에서 실행하므로 해당 gateway가 실행 중이어야 합니다. 같은 cluster와 realm/zonegroup/zone의 다른 gateway로 owned 사용자 관리를 계속할 수 있습니다. raw `Admin` 성공 출력에는 key가 포함될 수 있으므로 로그에 그대로 남기지 않아야 합니다. typed 조회와 실패 오류는 비밀 값을 제외합니다.

생성 응답에서 credentials를 확보한 뒤 caps 추가가 실패한 경우에는 owned handle로 상태 조회·삭제가 가능합니다. 생성 응답 자체가 유실되거나 JSON/key를 확인하지 못한 경우에는 handle을 임의로 채택하거나 삭제하지 않습니다. `Admin`으로 상태를 확인하거나 일회성 cluster를 정리해야 합니다.

RGW quota의 -1은 해당 제한 해제이고 0은 실제 0 제한입니다. `Enabled`를 별도로 설정하며 gateway cache와 비동기 accounting 때문에 즉시 초과 거부를 보장하지 않습니다. `SetBucketQuota`는 그 사용자가 소유하는 각 bucket에 적용되는 정책입니다. 사용자가 bucket을 소유하면 `RemoveUser`가 거부되며 bucket/object purge 옵션은 사용하지 않습니다. suspension의 HTTP 효과도 새 요청으로 polling합니다.

## 실행과 근거

`make cluster-features`는 내부 설정의 대표 Docker 시나리오를 순차 실행합니다. `CEPH_TEST_IMAGE`, `CEPH_TEST_OSD_IMAGE`, `CEPH_TEST_RGW_IMAGE`, `CEPH_TEST_MDS_IMAGE`로 기존 역할 이미지를 지정할 수 있습니다. native 검증은 Docker Linux 안의 librados/librbd/libcephfs Python client와 실제 S3 HTTP를 사용합니다. 공개 Go 코드에 native linking을 추가하지 않습니다.

2026-10-03, Ceph 20.2.4 역할별 slim 이미지와 Docker Desktop Linux ARM64 엔진에서 다음 대표 시나리오가 통과했습니다. bridge/host 각각의 결과이며 서로 다른 실행을 하나의 전체 suite 통과로 합치지 않습니다.

| 시나리오 | Bridge / Host 결과 | 확인한 효과 |
|---|---|---|
| Cephx caps | 46.00초 / 45.24초 PASS | writer→reader→writer fresh connection, 동일 key, omitted MGR 권한 제거, pool/namespace 경계, 잘못된 key와 revoke 거부 |
| Pool policy 최종 | 75.99초 / 85.16초 PASS | replica 2→3→2 후 데이터·pool ID·CRUSH 유지, native weight=0 배치의 불가능한 replica 거부, full quota의 쓰기 차단·해제 후 재개, 다른 pool 보존 |
| RBD namespace | 50.08초 / 50.56초 PASS | 동일 image 이름의 다른 bytes, namespace-scoped native 읽기·쓰기와 교차 접근 거부, image/trash 보존·restore, empty namespace 제거 |
| CephFS subvolume | 68.62초 / 67.99초 PASS | 기존 FS/MDS/pool 유지, group/default group 분리, 추가 pool·실제 RADOS namespace, quota 1→8 MiB와 기존 bytes·추가 2 MiB 파일 보존, unlimited/no-shrink, owned 삭제 |
| RGW user admin | 57.96초 / 58.76초 PASS | S3 사용자 격리, 일반 사용자의 Admin Ops 거부와 명시적 users=read 허용, user/bucket quota 저장, 정지·복구, nonempty 삭제 거부·데이터 보존 |

로컬 실행 증거는 `artifacts/cluster-internal-core.log`(auth와 첫 pool 검증, 241.164초 PASS), `artifacts/cluster-internal-services.log`(CephFS/RBD, 237.585초 PASS), `artifacts/cluster-internal-policy-rgw-final.log`(native CRUSH 보강 후 pool/RGW, 278.219초 PASS)에 있습니다. RGW quota의 즉시 초과 거부와 CephFS quota의 모든 overshoot 조건을 검증한 것은 아닙니다. 정책 저장·native layout·resize 이후 데이터와 접근 제어 효과를 위 표의 범위로 확인했습니다.

최종 Go 단위 테스트는 `CGO_ENABLED=0`으로 통과했고, 전체 단위 테스트 `-race`, 모든 integration/features/auth/hostnetwork/topology/multicluster tag의 컴파일과 `go vet`가 통과했습니다. CephFS MDS 설정 교체와 subvolume preflight의 동시 접근은 별도 race 테스트로 확인했습니다. 실패·응답 유실·복사한 handle·교체된 identity·secret redaction은 단위 테스트에서 확인하며, 실제 Docker의 실패 주입을 전수 수행한 것은 아닙니다.

마지막 실행 뒤 Docker의 running/stopped container 목록은 비어 있었으며 임시 network가 남지 않았습니다. 기존 `kind` network는 유지했습니다. snapshot/clone 관리, RGW placement/storage class, 임시 daemon config·OSD flag 제어는 다음 [fixture 확장](CLUSTER_FIXTURE_EXTENSIONS.md) 단계로 구분합니다.

공식 계약: [pool 설정·quota](https://docs.ceph.com/en/tentacle/rados/operations/pools/), [Cephx capability 교체](https://docs.ceph.com/en/tentacle/rados/operations/user-management/), [RBD namespace](https://docs.ceph.com/en/tentacle/man/8/rbd/), [CephFS volumes](https://docs.ceph.com/en/tentacle/cephfs/fs-volumes/), [RGW 관리](https://docs.ceph.com/en/tentacle/radosgw/admin/).
