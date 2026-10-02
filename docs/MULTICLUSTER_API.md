# 단일 클러스터와 다중 클러스터 시나리오 API

`ceph` 패키지는 단일 일회성 클러스터, `multicluster` 패키지는 기존 클러스터 사이의 multisite 구성·정책, mirroring과 백업·복원을 다룹니다. Go module은 하나를 유지하며 의존 방향은 `multicluster → ceph`입니다. `ceph.Run`에는 다른 클러스터와의 관계를 섞지 않습니다. 일반 RGW/RBD/CephFS 테스트에서는 복제용 컨테이너를 실행하지 않습니다.

## 책임과 수명

| 계층 | API | 소유 리소스 |
| --- | --- | --- |
| 단일 클러스터 | `ceph.Run`, `AddOSD`, `StartRGW`, `StartCephFS` | MON, MGR, OSD, 필요한 RGW/MDS, 자체 Ceph network |
| RGW multisite | `multicluster.RunRGWMultisite` | 기존 두 클러스터 위의 gateway 2개, 관리 client 2개, HTTP bridge |
| RBD snapshot mirror | `multicluster.RunRBDMirror` | 관리 client 2개, destination의 `rbd-mirror` daemon |
| CephFS snapshot mirror | `multicluster.RunCephFSMirror` | source의 `cephfs-mirror` daemon, source MGR에 추가한 remote network 연결 |
| RBD 백업·복원 | `ExportRBDBackup`, `ExportRBDIncremental`, `RestoreRBDBackup`, `RestoreRBDIncremental` | CLI archive의 byte 전달과 임시 파일 정리. 보관처·pool·client는 호출자 소유 |
| CephFS archive PoC | 테스트의 JSON/base64 fixture helper | 파일 tree를 별도 경로에 복원. 범용 backup API로 제공하지 않음 |

각 연결은 기존 `*ceph.Container`를 참조합니다. FSID, OSD, 키, 클러스터 네트워크를 공유하지 않습니다. RBD pool과 CephFS/MDS는 호출자가 먼저 만듭니다. RGW multisite는 realm/zone을 기동 전에 구성해야 하므로 standalone `StartRGW`를 실행하지 않은 새 클러스터를 사용합니다.

클러스터 cleanup을 먼저 등록하고 연결 cleanup을 나중에 등록합니다. LIFO로 연결 → 클러스터 순서로 종료합니다. 오류와 함께 non-nil 연결이 반환돼도 cleanup이 필요합니다. Mirror는 `testcontainers.Container`를 embed하여 `Stop`/`Start`로 장애를 주입할 수 있습니다. `Terminate`는 연결이 소유한 추가 컨테이너와 네트워크 연결을 제거하고 클러스터나 데이터를 삭제하지 않습니다. Ceph에 쓴 realm, peer, auth, directory policy는 일회성 클러스터에 남습니다. CephFS의 `RemoveDirectory`/`RemovePeer`와 RBD의 peer 제어는 호출자가 명시적으로 선택하는 구성 변경이며 `Terminate`가 자동으로 수행하지 않습니다. 기존 클러스터 전체의 원래 상태 복원도 계약에 포함하지 않습니다.

## 사용 예

```go
source, err := ceph.Run(ctx, controlImage, ceph.WithOSDImage(osdImage))
if source != nil { testcontainers.CleanupContainer(t, source) }
if err != nil { t.Fatal(err) }

destination, err := ceph.Run(ctx, controlImage, ceph.WithOSDImage(osdImage))
if destination != nil { testcontainers.CleanupContainer(t, destination) }
if err != nil { t.Fatal(err) }

multisite, err := multicluster.RunRGWMultisite(ctx, rgwImage,
    multicluster.RGWMultisiteConfig{
        Source: source, Destination: destination, ControlImage: controlImage,
    })
if multisite != nil {
    t.Cleanup(func() {
        cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
        defer cancel()
        if err := multisite.Terminate(cleanupCtx); err != nil { t.Error(err) }
    })
}
if err != nil { t.Fatal(err) }
endpoint, err := multisite.Destination.S3Endpoint(ctx)
if err != nil { t.Fatal(err) }
// endpoint, AccessKey, SecretKey, Region을 path-style S3 client에 전달합니다.
```

RBD pool을 초기화한 뒤에는 다음과 같이 연결합니다. 대상 image마다 `rbd mirror image enable POOL/IMAGE snapshot`과 `rbd mirror image snapshot POOL/IMAGE`을 따로 실행합니다. 연결 API의 성공이 데이터 동기화 완료나 자동 failover를 의미하지는 않습니다.

```go
mirror, err := multicluster.RunRBDMirror(ctx, controlImage,
    multicluster.RBDMirrorConfig{Source: source, Destination: destination, Pool: "images"})
if mirror != nil { testcontainers.CleanupContainer(t, mirror) }
if err != nil { t.Fatal(err) }
```

CephFS는 양쪽에서 `StartCephFS`를 호출한 뒤 연결합니다. 지정한 directory의 snapshot을 복제합니다.

```go
mirror, err := multicluster.RunCephFSMirror(ctx, controlImage,
    multicluster.CephFSMirrorConfig{
        Source: source, Destination: destination,
        SourceFilesystem: sourceFS.FilesystemName,
        DestinationFilesystem: destinationFS.FilesystemName,
        Directories: []string{"/application"},
    })
if mirror != nil { testcontainers.CleanupContainer(t, mirror) }
if err != nil { t.Fatal(err) }
```

## 백업·복원과 관계 변경

RBD archive는 mirroring 연결 없이 전달할 수 있습니다. Native format-2 full export와 snapshot baseline 기반 incremental export를 사용하며, 실제 보관처는 호출자가 선택합니다.

```go
var full bytes.Buffer
if err := multicluster.ExportRBDBackup(ctx, sourceClient, "images/application", &full); err != nil {
    t.Fatal(err)
}
if err := multicluster.RestoreRBDBackup(ctx, destinationClient, "images/restored", bytes.NewReader(full.Bytes())); err != nil {
    t.Fatal(err)
}
```

`ExportRBDIncremental`은 추가로 baseline snapshot 이름을 받고 `RestoreRBDIncremental`은 대상 image에 그 baseline이 있는지 Ceph CLI가 검증합니다. Reader/Writer를 helper가 닫지 않으며, container archive와 restore용 private host spool은 정리합니다. Host에서는 byte 전달만 수행하고 Ceph client/native linking은 사용하지 않습니다. 이미지 삭제나 pool 생성은 helper가 자동으로 수행하지 않습니다.

Archive 전송은 read 사이에서 context 취소를 확인합니다. 호출자가 제공한 Reader/Writer의 blocking I/O 자체를 중단해야 한다면 해당 구현도 취소를 지원해야 합니다. Incremental restore는 baseline의 존재를 검사하지만, 같은 이름의 snapshot 내용까지 인증하는 backup manifest를 제공하지는 않습니다.

RBD의 `SourceRBD`/`DestinationRBD`는 연결이 소유한 관리 client에서 명령을 실행하며 `Rebootstrap`은 제거한 receiving peer를 다시 등록합니다. 왕복 전환에는 `SourceSite`/`DestinationSite`를 각 클러스터의 고정 이름으로 지정하고 반대 방향 연결에서도 그 이름을 유지합니다. 한 방향 연결의 daemon은 destination에서 실행됩니다. 반대 방향 연결을 추가하면 daemon도 반대 클러스터에 하나 더 필요합니다. Primary demote/promote, writer fencing, 복제 완료 대기, split-brain의 authoritative image 선택과 `resync`는 시나리오가 명시적으로 수행합니다.

CephFS의 `AddDirectory`/`RemoveDirectory`는 복제 directory 정책을, `PeerIDs`/`RemovePeer`/`RebootstrapPeer`는 동일한 source/destination 사이의 연결을 제어합니다. 제거한 정책이 daemon에 반영될 때까지 확인하고 다음 snapshot을 생성해야 합니다. API 성공과 실제 데이터 동기화 완료는 별도로 관측합니다. Destination에 이미 복제된 데이터나 snapshot을 peer 제거 시 삭제하지 않습니다.

RGW의 `SourceAdmin`/`DestinationAdmin`은 realm/zone별 제어 명령을 실행합니다. `PullSourcePeriod`/`PullDestinationPeriod`는 native `realm pull`로 상대 site의 realm과 current committed period를 가져오고 활성 period 포인터·realm epoch·local zonegroup을 갱신합니다. Native `period pull`만으로는 활성 realm이나 local 구성이 바뀌지 않으므로 이 복귀 절차에 충분하지 않습니다. 정책 변경과 master 승격에는 실제 Ceph CLI와 gateway 재시작을 사용합니다. 새 master로 전환하기 전 metadata 동기화와 이전 writer fencing은 시나리오가 명시적으로 수행합니다. Realm/current period 반영은 해당 버전의 [rgw_zone.cc](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_zone.cc)를 기준으로 구성했습니다.

Ceph 20.2.4의 승격 PoC는 `period update`와 `period commit`을 나눠 실행합니다. 합친 명령에서는 이미 바뀐 local zonegroup으로 driver를 초기화하여 metadata master의 빈 sync 상태를 승격 검사에 전달할 수 있습니다. 별도 commit은 이전 current period를 기준으로 검사를 수행합니다. 승격 전 native metadata status의 period·realm epoch·incremental shard와 master 비교 결과를 확인하며 `--yes-i-really-mean-it`으로 검사를 우회하지 않습니다. 근거: 해당 버전의 [radosgw-admin](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/radosgw-admin/radosgw-admin.cc), [metadata sync](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_sync.cc), [period 승격 검사](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_period.cc).

각 데이터 경로와 서비스별 함수는 같은 Go package에 두되 [rgw.go](../multicluster/rgw.go), [rbd.go](../multicluster/rbd.go), [cephfs.go](../multicluster/cephfs.go), [rbd_backup.go](../multicluster/rbd_backup.go)로 구현 책임을 구분합니다. 세부 시나리오와 관측 결과는 [PoC 보고서](MULTICLUSTER_POC.md)에 기록합니다.

## Testcontainers와 이미지의 경계

단일 기술 module의 진입점은 기존 `ceph.Run(ctx, image, opts...)`입니다. 다중 클러스터 연결 API는 그 위의 구성 helper로 두며 image를 명시하고, `ContainerCustomizer`를 daemon에 마지막으로 적용하고, 부분 생성 결과를 반환하고, 소유한 리소스를 cleanup합니다. 별개의 공식 Testcontainers module로 등록한 것은 아닙니다. [Testcontainers Go module 지침](https://golang.testcontainers.org/modules/)의 Run·composition·customizer·SDK dependency 원칙을 기반으로 복수 리소스의 수명을 명시한 추가 API입니다.

이미지는 기존 다섯 역할을 유지합니다. `control`은 MON/MGR/CLI에 더해 `rbd-mirror`와 `cephfs-mirror`를 제공하고 `all`도 포함합니다. `osd`/`rgw`/`mds`에는 mirror daemon을 넣지 않습니다. 이미지에 포함해도 일반 클러스터에서 자동으로 실행하지 않으며 각각의 연결 API에서만 실행합니다.

`multicluster`는 복수 클러스터를 사용하는 시나리오의 상위 패키지입니다. 공통 Ceph federation control plane을 뜻하지 않습니다. RGW는 realm/zone의 HTTP multisite, RBD는 destination이 source MON/OSD에서 읽는 snapshot mirror, CephFS는 source daemon이 destination에 쓰는 snapshot mirror입니다. RBD의 새 receiving peer는 rx-only로 등록하고, 반대 방향 연결에서 재사용하는 tx-only peer는 기존 송신을 유지하도록 rx-tx로 확장합니다. CephFS bootstrap에서는 source MGR의 원격 filesystem 검사도 필요하므로 추가 네트워크 연결을 API가 관리합니다. [RGW](https://docs.ceph.com/en/tentacle/radosgw/multisite/), [RBD](https://docs.ceph.com/en/tentacle/rbd/rbd-mirroring/), [CephFS](https://docs.ceph.com/en/tentacle/cephfs/cephfs-mirroring/)의 기능과 제약을 각각 적용합니다.
