# 토폴로지 확장 목표와 검증

이 단계의 목표는 RGW 여러 zonegroup과 zone 탈퇴, mirror daemon 증감·HA, public/cluster 네트워크 분리, 선택적 연결 단절·복구를 구성 API로 제공하고 실제 Ceph map과 통신으로 확인하는 것입니다. 요청한 확장 범위의 구현과 대표 구성 검증을 완료했습니다. CRUSH·EC·클라이언트 기능 확장은 포함하지 않습니다. 아래 결과는 실행한 대표 구성의 근거이며 숫자·정책 조합 전체의 전수 검증을 뜻하지 않습니다.

## 검증 결과

2026-10-03, Ceph 20.2.4 역할별 slim 이미지와 Docker Desktop Linux ARM64 엔진에서 검증했습니다. Docker PoC는 4 GiB VM에서 순차 실행했습니다. 공개 Go 모듈은 cgo-free로 유지하며 native I/O는 Linux client container에서 확인했습니다.

| 토폴로지 | 구성 API | 현재 검증 |
|---|---|---|
| 5 MON quorum | `WithMonitorCount(5)`, `InterruptNetwork` | PASS. 두 실행 중 MON endpoint 격리, 3/5 quorum과 native I/O 유지, 원래 IP 복구 뒤 5/5 재가입 |
| 별도의 public/cluster network | `WithSeparateClusterNetwork`, `ClusterNetworkName` | PASS. 실제 OSD map의 두 subnet, public-only client/MON/MGR, OSD 증감과 데이터 유지 |
| 선택적 daemon/client endpoint 단절 | `Container.InterruptNetwork`, `NetworkInterruption.Restore` | PASS. client 인증 실패·복구, OSD backend만 제거하며 public listener/process 유지, daemon 재시작 없이 I/O 복구 |
| RBD mirror daemon 증감·HA | `DaemonCount`, `Daemons`, `AddDaemon`, `RemoveDaemon`, daemon `Status` | bridge/host PASS. native leader·instance membership, leader 중단/교체, 2→0→1, peer/image identity와 8 MiB replica 유지 |
| CephFS mirror daemon 증감·HA | count/list/add/remove, `RebalanceDirectories` | bridge/host 명시적 경로 PASS: native owner 장애 승계, 증설 재분배, 2→0→1과 backlog·snapshot 유지. 20.2.4 자동 증설 재분배 오류는 별도 기록 |
| RGW 여러 zonegroup·zone 탈퇴 | `Zonegroups`, `MasterZonegroup`, `AddZonegroup`, `AddZone`, `RemoveZone` | bridge/host PASS. 초기 2 group과 동적 group 추가, 지역별 복제·redirect, non-master 탈퇴, 실제 period와 저장 데이터 보존 |
| 클러스터 간 peer endpoint 단절 | RBD/CephFS `InterruptPeerLink`, RGW `InterruptZoneLink` | 세 서비스 bridge PASS. local 서비스·native identity·peer 유지, 원래 endpoint 복구 후 catch-up. RGW의 양방향 전달과 unchanged period 확인 |

네트워크 PoC 로그는 `artifacts/topology-network-extensions.log`이며 두 케이스 178.178초 PASS입니다. RBD daemon 로그는 `artifacts/topology-rbd-daemons.log`이며 host 120.34초, bridge 116.47초, 명령 전체 237.164초 PASS입니다. 서로 다른 실행을 하나의 전체 suite 통과로 합치지 않습니다.

RGW 초기 두 zonegroup은 `artifacts/topology-rgw-initial-zonegroups.log`에서 host 263.02초, bridge 108.41초, 명령 전체 371.785초 PASS입니다. 입력에서 뒤에 둔 group을 realm master로 지정하고 각 group의 local master, 실제 zone ID·endpoint와 양쪽 period를 확인했습니다.

RGW 동적 group 추가·zone 탈퇴는 `artifacts/topology-rgw-zonegroups-removal.log`에서 host 599.29초, bridge 286.83초, 명령 전체 886.291초 PASS입니다. 서로 다른 세 cluster의 한 realm/two zonegroups에서 local object 복제·다른 region redirect와 탈퇴 후 원래 RADOS object의 byte 보존을 확인했습니다.

CephFS 명시적 경로는 `artifacts/topology-cephfs-daemons-rebalance.log`에서 bridge 257.02초, host 236.40초, 명령 전체 493.837초 PASS입니다. 초기 분담과 dead-owner 승계는 native 경로로 유지하고 증설 시에만 명시적 재분배를 호출합니다. 동일 peer와 원래 snapshot 데이터를 유지하는 2→1→2→1→0→1, bridge peer 단절 중 PID·source endpoint·배치 보존과 복구 후 snapshot catch-up을 확인했습니다.

RBD/RGW peer 단절은 `artifacts/topology-peer-network-links.log`에서 RBD 132.71초, RGW 141.33초, 명령 전체 274.375초 PASS입니다. RBD는 양쪽 cluster의 분리 public/backend 구성과 동일 receiver instance·peer를 유지했습니다. RGW는 같은 PID·local S3 endpoint·period를 유지하며 12초간 양방향 미전달, 원래 HTTP endpoint 복구 후 양방향 catch-up을 확인했습니다.

`CGO_ENABLED=0 go test -count=1 ./...`, 관련 tag 조합의 전체 컴파일, `go vet`, bootstrap shell 문법과 diff 검사가 통과했습니다. 재실행 대상은 `make topology-extensions`이며 역할별 이미지는 기존 `CEPH_TEST_*_IMAGE` 환경 변수로 지정합니다. 이 target은 CephFS 명시적 재분배 경로를 선택합니다.

최종 실행 후 running/stopped Docker container는 0개이며 testcontainers 임시 network도 남지 않았습니다. 기본 `bridge`/`host`/`none`과 기존 `kind` network는 유지했습니다. 부분 생성·불확실한 endpoint 연결 응답·client close·daemon 종료·directory release 실패에서 cleanup/재시도 상태를 유지하는 것은 단위 테스트에서 별도로 확인했습니다.

## 네트워크 구성 계약

`WithSeparateClusterNetwork()`는 Docker가 선택한 서로 다른 IPv4 subnet의 bridge 두 개를 생성합니다. `NetworkName()`은 public bridge이며 `ClusterNetworkName()`은 OSD backend bridge입니다. OSD만 양쪽에 연결됩니다. `WithClient`, MON/MGR/MDS/RGW와 mirror의 native client는 public bridge를 사용합니다. config의 `public network`/`cluster network`로 Ceph가 실제 주소를 선택합니다. 기본 단일 bridge와 host mode의 동작은 기존과 같습니다. 분리 옵션과 host mode를 함께 요청하면 생성 전에 거부합니다.

```go
cluster, err := ceph.Run(ctx, image, ceph.WithSeparateClusterNetwork())
if cluster != nil {
    defer cluster.Terminate(context.Background())
}
if err != nil {
    return err
}
cut, err := cluster.InterruptNetwork(ctx, cluster.OSDs()[0], ceph.ClusterNetworkPlane)
if cut != nil {
    defer cut.Restore(context.Background())
}
if err != nil {
    return err
}
// 이 구간에서 backend 단절 중 토폴로지를 확인합니다.
if err := cut.Restore(ctx); err != nil {
    return err
}
```

단절은 선택한 container의 해당 bridge endpoint 전체를 끊는 방식입니다. 개별 source/destination 주소 쌍의 packet filtering, 지연·손실 주입, host namespace의 firewall 조작은 제공 범위에 포함하지 않습니다. 복구 시 광고된 원래 IP·alias와 gateway priority를 유지하고 native daemon은 재시작하지 않습니다. fixture는 자신이 만든 interruption의 복구를 cleanup에 등록하며 실패한 복구는 재시도할 수 있습니다. caller-owned client를 복구하지 못한 network는 제거를 보류합니다.

RBD peer 단절은 receiver의 source public network, CephFS는 daemon의 destination public network, RGW는 gateway의 multisite HTTP bridge를 선택합니다. local cluster endpoint와 peer 정책을 유지하며, 복구 뒤 native catch-up을 검증합니다. host mode에서는 공유 namespace endpoint를 Docker로 끊을 수 없어 해당 API를 거부합니다.

## Daemon와 zone lifecycle 계약

RBD/CephFS `DaemonCount`의 0은 기본 1개를 의미합니다. 초기 이름은 a/b 순서이고 동적 추가는 이름을 명시합니다. 실행 중 모든 daemon을 제거하는 outage와 이후 재추가가 가능합니다. `Daemons`로 현재 구성원을 조회하며 초기 embedded `Container`는 첫 daemon의 호환 handle입니다. 첫 daemon 제거 후 이 handle은 비워지며 다른 daemon으로 재할당되지 않습니다. daemon 증감은 peer·filesystem·pool·replica 데이터를 제거하지 않습니다.

RGW는 한 realm 안에서 zonegroup마다 local master를 구성하고 realm의 metadata master group을 지정합니다. metadata는 realm 단위로 공유하지만 object 복제는 같은 zonegroup 안에서 수행합니다. 다른 group의 bucket 요청은 해당 지역으로 redirect됩니다. `RemoveZone`은 non-master zone의 membership과 owned runtime을 제거하되 storage의 local zone 설정과 pool 데이터를 유지합니다. realm/group master 탈퇴는 먼저 명시적인 master 전환이 필요합니다. 유지된 storage는 fresh-cluster bootstrap에 다시 사용할 수 있는 것으로 취급하지 않습니다.

CephFS mirroring의 filesystem당 single peer 제한은 그대로 적용합니다. 다중 mirror daemon은 동일 peer를 담당하며 A→B/C fanout을 뜻하지 않습니다. Tentacle 문서는 복수 daemon의 분담·HA를 설명하면서 해당 배치를 untested로 표시합니다. 버전별 native 동적 배치 문제와 fixture의 명시적 재조정 경로는 실제 PoC 결과에 맞춰 기록합니다.

현재 20.2.4 이미지에서는 초기 2개 daemon의 2/2 directory 배치와 장애 후 4개 directory의 자동 승계·새 snapshot 전달을 확인했습니다. 교체 daemon을 추가하면 native MGR가 `callback exception: 'DirectoryState' object is not subscriptable`을 기록하고 4/0 배치에 머뭅니다. 실제 이미지의 `can_shuffle_dir`는 `DirectoryState`를 `dir_state['mapped_time']`으로 접근합니다. [해당 버전 정책 코드](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/mgr/mirroring/fs/dir_map/policy.py#L54-L62)와 로컬 `artifacts/topology-cephfs-native-shuffle-evidence.log`에 근거를 남겼습니다. 기본 PoC는 native 장애 승계를 유지하면서 증설 뒤 `RebalanceDirectories`를 명시적으로 호출합니다. 자동 shuffle 경로는 `CEPH_TEST_CEPHFS_NATIVE_SHUFFLE=1`로 별도 검증하며 현재 이미지에서 실패한 범위를 PASS로 처리하지 않습니다.

재분배는 fixture의 소유 directory 정책만 release·재등록하며 파일·snapshot·peer는 유지합니다. 현재 native watcher와 policy 집합이 owned daemon과 정확히 일치해야 시작합니다. source 권한은 해당 filesystem으로 제한하고 외부 mirror watcher가 있으면 준비 조건에서 거부합니다. 이 작업 중 복제가 잠시 멈출 수 있습니다.

자동 경로의 세 번째 실행은 `artifacts/topology-cephfs-daemons-native-third.log`에서 317.419초 FAIL입니다. 앞의 두 실행은 준비 코드의 JSON parser 문제로 실패했으며, 각각의 수정 후 단위 테스트에 실제 JSON 형태를 추가했습니다. 최종 자동 경로 실패는 준비 단계 이후 증설 시 native 배치 오류로 구분합니다.

근거: [Ceph network 구성](https://docs.ceph.com/en/tentacle/rados/configuration/network-config-ref/), [RBD mirror daemon 구성](https://docs.ceph.com/en/tentacle/rbd/rbd-mirroring/), [CephFS mirror 모듈](https://docs.ceph.com/en/tentacle/cephfs/cephfs-mirroring/), [RGW multisite](https://docs.ceph.com/en/tentacle/radosgw/multisite/).
