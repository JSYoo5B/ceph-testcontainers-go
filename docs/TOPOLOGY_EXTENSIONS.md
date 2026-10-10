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

## OSD 삭제의 소유권과 재시도

`RemoveOSD`는 native OSD map의 등록 UUID를 생성 당시 값과 비교하고 reweight/out, safe-to-destroy, stop/down, purge 단계 사이에서도 다시 확인합니다. 같은 숫자 ID에 다른 UUID가 등록됐거나 map을 해석할 수 없으면 다음 변경을 거부합니다. Ceph purge에는 UUID compare-and-swap이 없으므로 외부 등록·교체를 삭제와 동시에 실행하지 않아야 합니다.

Purge를 요청한 뒤 응답이 유실되면 descriptor를 유지합니다. 새 context의 재시도에서 엄격하게 읽은 native map에 해당 ID가 없으면 자신의 이전 요청을 완료된 것으로 정리합니다. 같은 ID의 다른 UUID가 있으면 거부하며, 이전 purge 요청 없이 사라진 ID를 성공으로 간주하지 않습니다. 완료된 purge 뒤 Docker cleanup이 실패하면 native mutation이나 stop을 반복하지 않고 cleanup만 재시도합니다. 완료되지 않은 purge/cleanup handle이 있으면 `AddOSD`를 막아 ID 재사용에 따른 handle 유실을 방지합니다. 마지막 OSD 판단은 cleanup handle 개수 대신 실제 등록된 소유 UUID를 사용합니다.

2026-10-07 Docker Desktop Linux ARM64에서 고정 원본 `ceph.DefaultImage`로 `TestOSDRemovalLifecycle/bridge`, `/host`를 실행했습니다. 실제 purge 이후 응답만 유실시켜 재시도 중 native 변경·stop이 늘지 않고 cleanup만 수행됨을 확인했습니다. OSD 추가의 ID 재사용과 새로운 UUID, 외부 UUID 등록 뒤 native mutation·stop·terminate 거부, 복제 수 2인 pool의 8개 object 각각 32 KiB 원문 유지까지 검사했습니다. 최종 로그 `artifacts/followups-20261007/osd-runtime-final.log`는 127.678초 PASS이며 `osd-final-cleanup/after.json`은 새 container/network 0개로 PASS입니다. Unit·race·전체 tag compile/vet도 통과했습니다. 이 focused 실행은 기존 전체 CI 또는 다른 이미지 계열의 새 검증으로 표시하지 않습니다.

## MON 교체 후 bootstrap 설정

`AddMonitor`와 `RemoveMonitor`는 현재 quorum의 monmap으로 control·MON·MGR·OSD·MDS·RGW의 `/etc/ceph/ceph.conf`에 있는 global `mon host`를 갱신합니다. 실행 중인 native session이 새 monmap을 배운 것과 다음 재시작의 bootstrap 설정은 다르므로 양쪽을 유지합니다. Docker archive API를 사용해 중지된 소유 컨테이너도 처리하며 그 외 section·node별 설정·keyring과 process 상태를 유지합니다. Control의 private 설정은 새 client용 template에 복사하지 않습니다.

파일 복사가 일부 실패하면 native MON 변경과 이미 성공한 파일 갱신은 유지됩니다. `RefreshMonitorConfig(ctx)`로 새 context에서 현재 quorum을 다시 읽고 재시도할 수 있습니다. 이미 최신인 파일은 쓰지 않습니다. Add의 partial handle에 다시 같은 이름을 추가하거나 새 데몬을 만드는 동작으로 재시도를 대신하지 않습니다. Remove의 partial ownership도 완료 전까지 유지됩니다. 이 작업은 process를 시작·재시작하지 않습니다.

호출자가 `WithClient`로 만든 client와 별도 `multicluster` 연결은 각자의 config snapshot을 소유합니다. `RefreshClientMonitorConfig`와 link의 `RefreshMonitorConfig`로 stopped/running 파일을 명시적으로 갱신할 수 있습니다. Remote peer 주소는 RBD `Rebootstrap` 또는 CephFS `RefreshPeerMonitorConfig`로 갱신한 뒤 daemon을 cold-start합니다. [MON bootstrap 재연결 계약](MON_BOOTSTRAP_REFRESH.md)을 따르며 cluster가 소유한 daemon의 자동 갱신과 구분합니다.

2026-10-07의 수정 전 원본 Quay host probe는 a,b,c → d,e,f 교체와 3-member quorum을 유지했지만 기존 MGR/RGW/OSD의 파일에 옛 주소가 남았고 RGW cold start는 timeout으로 실패했습니다. 로그 `artifacts/followups-20261007/mon-before.log`와 별도 `mon-before-cleanup/after.json`의 새 리소스 0개를 보존합니다.

수정 후 `TestMonitorRollingReplacement`는 같은 원본 Quay Linux ARM64의 bridge 144.00초·host 156.59초, 명령 전체 301.046초 PASS입니다. 최종 MON 세 개의 실제 quorum, running/stopped 소유 daemon의 개별 설정·role keyring, 중지된 MDS의 명시적 설정 복구, 원래 MGR/MDS/RGW container ID와 재등록된 새 MGR/MDS GID, RADOS/S3의 정확한 64 KiB를 확인했습니다. Caller-owned client snapshot은 유지했습니다. `mon-source.json`의 실행 전후 source 해시는 동일하며 `mon-runtime.log`, `mon-check.log`, 새 리소스 0개인 `mon-cleanup/after.json`에 runtime·전체 unit/race/vet/tag compile·cleanup 증거를 연결합니다. 기존 전체 CI와 다른 이미지 계열의 새 전체 검증으로 합산하지 않습니다.

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

단절은 선택한 container의 해당 bridge endpoint 전체를 끊는 방식입니다. 개별 source/destination 주소 쌍의 packet filtering, packet 손실 주입, host namespace의 firewall 조작은 제공 범위에 포함하지 않습니다. 공식 이미지에 `tc`·`iptables`가 없기 때문입니다. 특정 daemon이 client 메시지를 버리거나 늦게 처리하는 상황은 Ceph messenger 옵션으로 만들 수 있으며 [messenger 지연·차단 recipe](MESSENGER_FAULTS.md)를 따릅니다. 복구 시 광고된 원래 IP·alias와 gateway priority를 유지하고 native daemon은 재시작하지 않습니다. fixture는 자신이 만든 interruption의 복구를 cleanup에 등록하며 실패한 복구는 재시도할 수 있습니다. caller-owned client를 복구하지 못한 network는 제거를 보류합니다.

RBD peer 단절은 receiver의 source public network, CephFS는 daemon의 destination public network, RGW는 gateway의 multisite HTTP bridge를 선택합니다. local cluster endpoint와 peer 정책을 유지하며, 복구 뒤 native catch-up을 검증합니다. host mode에서는 공유 namespace endpoint를 Docker로 끊을 수 없어 해당 API를 거부합니다.

## Daemon와 zone lifecycle 계약

RBD/CephFS `DaemonCount`의 0은 기본 1개를 의미합니다. 초기 이름은 a/b 순서이고 동적 추가는 이름을 명시합니다. 실행 중 모든 daemon을 제거하는 outage와 이후 재추가가 가능합니다. `Daemons`로 현재 구성원을 조회하며 초기 embedded `Container`는 첫 daemon의 호환 handle입니다. 첫 daemon 제거 후 이 handle은 비워지며 다른 daemon으로 재할당되지 않습니다. daemon 증감은 peer·filesystem·pool·replica 데이터를 제거하지 않습니다.

RGW는 한 realm 안에서 zonegroup마다 local master를 구성하고 realm의 metadata master group을 지정합니다. metadata는 realm 단위로 공유하지만 object 복제는 같은 zonegroup 안에서 수행합니다. 다른 group의 bucket 요청은 해당 지역으로 redirect됩니다. `RemoveZone`은 non-master zone의 membership과 owned runtime을 제거하되 storage의 local zone 설정과 pool 데이터를 유지합니다. realm/group master 탈퇴는 먼저 명시적인 master 전환이 필요합니다. 유지된 storage는 fresh-cluster bootstrap에 다시 사용할 수 있는 것으로 취급하지 않습니다.

CephFS mirroring의 filesystem당 single peer 제한은 그대로 적용합니다. 다중 mirror daemon은 동일 peer를 담당하며 A→B/C fanout을 뜻하지 않습니다. Tentacle 문서는 복수 daemon의 분담·HA를 설명하면서 해당 배치를 untested로 표시합니다. 버전별 native 동적 배치 문제와 fixture의 명시적 재조정 경로는 실제 PoC 결과에 맞춰 기록합니다.

현재 20.2.4 이미지에서는 초기 2개 daemon의 2/2 directory 배치와 장애 후 4개 directory의 자동 승계·새 snapshot 전달을 확인했습니다. 교체 daemon을 추가하면 native MGR가 `callback exception: 'DirectoryState' object is not subscriptable`을 기록하고 4/0 배치에 머뭅니다. 실제 이미지의 `can_shuffle_dir`는 `DirectoryState`를 `dir_state['mapped_time']`으로 접근합니다. [해당 버전 정책 코드](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/mgr/mirroring/fs/dir_map/policy.py#L54-L62)와 로컬 `artifacts/topology-cephfs-native-shuffle-evidence.log`에 근거를 남겼습니다. 기본 PoC는 native 장애 승계를 유지하면서 증설 뒤 `RebalanceDirectories`를 명시적으로 호출합니다. 자동 shuffle 경로는 `native_regression` build tag가 있는 optional `native_shuffle` batch로 별도 검증합니다. 환경 변수와 `t.Skip`으로 선택하지 않으며 `all`만으로는 포함되지 않습니다. [태그 기반 실행](TEST_TAGS.md#명시적으로-선택하는-nativesdk-검사)을 따르고, 아래 고정 이미지에서 실패한 범위를 PASS로 처리하지 않습니다.

재분배는 fixture의 소유 directory 정책만 release·재등록하며 파일·snapshot·peer는 유지합니다. 현재 native watcher와 policy 집합이 owned daemon과 정확히 일치해야 시작합니다. source 권한은 해당 filesystem으로 제한하고 외부 mirror watcher가 있으면 준비 조건에서 거부합니다. 이 작업 중 복제가 잠시 멈출 수 있습니다.

자동 경로의 세 번째 실행은 `artifacts/topology-cephfs-daemons-native-third.log`에서 317.419초 FAIL입니다. 앞의 두 실행은 준비 코드의 JSON parser 문제로 실패했으며, 각각의 수정 후 단위 테스트에 실제 JSON 형태를 추가했습니다. 최종 자동 경로 실패는 준비 단계 이후 증설 시 native 배치 오류로 구분합니다.

## 후속 runtime 우선순위

2026-10-07 코드 검토에서 OSD 삭제 재시도, 소유 daemon MON bootstrap 갱신, RBD/CephFS 관측 다음의 공백을 아래 순서로 확인했습니다. 아래 항목은 후속 진행 상태를 구분합니다.

| 순서 | 남은 공백 | 다음 검증 기준 |
| --- | --- | --- |
| 완료 | 전체 MON rolling 이후 caller client와 multicluster bootstrap | 명시적 local/remote 갱신 API 및 원본 Quay bridge/host PASS. 양쪽 MON 전체 교체·동일 link cold restart·원문 유지; [계약과 결과](MON_BOOTSTRAP_REFRESH.md) |
| 완료 | 기존 topology·lifecycle 직렬화 gate의 plain mutex 대기 | 42개 진입 경로와 nested cleanup/network lock에 caller context 적용. Busy fixture의 native 변경 0회·새 context 재시도, 원본 Quay 대표 MON/OSD/RBD/CephFS/RGW 5개 parent PASS·strict cleanup 확인; [적용 범위와 결과](TOPOLOGY_CONTEXT.md) |
| 완료 | constructor·관측 경로의 context-free configuration/manager/gateway/control snapshot | additive context getter와 consumer preflight. Busy source/destination 8개 실제 constructor의 deadline cause·후속 native 호출 0회, MON bridge/host 및 RGW 회귀 PASS·strict cleanup; [계약과 결과](TOPOLOGY_CONTEXT.md) |
| 완료 | CephFS peer 제거 후 원래 worker의 종료 관측 | live cohort의 original FS/peer/session을 보존한 `BeginPeerRemoval`, `Status`, `WaitDrained`. 실제 삭제 응답 유실·동일 handle 재시도·새 peer/checkpoint·원문·strict cleanup bridge/host PASS; [계약과 증거](CEPHFS_PEER_REMOVAL.md) |
| 완료 | host RGW allocator 생성과 lease 기록의 순서 | owner를 allocation 전에 얻고 nonnil lease를 같은 critical section에 기록. Busy/closed/canceled·partial error·동시 termination unit, native TLS·gateway 증감 bridge/host PASS; [계약과 증거](TOPOLOGY_CONTEXT.md#host-rgw-포트-allocator의-소유권) |
| 완료 | CephFS setup/scale·subvolume/data-pool·pin/clone의 혼합 잠금 | 생성 전 setup→owner→control 대기는 caller context, 생성 후 identity/descriptor 기록 유지. 실제 held-gate·fresh retry unit/race 및 원본 Quay MDS scale/replay·data-pool·pin·clone 5개 parent PASS·strict cleanup; [계약과 결과](TOPOLOGY_CONTEXT.md#cephfs-setupscaleprovisioning의-context-admission) |
| 완료 | CephFS directory 제거 후 원래 sync cycle 해제 관측 | `BeginDirectoryRemoval`, `Status`, `WaitReleased`; 원래 path/peer/session과 모든 owned live replayer에서 exact path stats 부재. 응답 유실·재등록·원문 및 strict cleanup bridge/host PASS; [계약과 증거](CEPHFS_DIRECTORY_REMOVAL.md) |
| 완료 | 제거 receipt의 원래 process·watcher 종료 관측 | 선택적 raw Docker observer와 `ProcessQuiescence`; 원래 engine/CID·StartedAt/GID·policy/generation. Peer/directory × bridge/host 4개 pair·12개 관측·bytes·strict cleanup PASS; [관측 계약](CEPHFS_PROCESS_QUIESCENCE.md) |
| 완료 | 원래 daemon 제거 뒤 중단된 제거의 명시적 승인과 fixture 재사용 | `AcknowledgeProcessQuiescence`의 fresh removed-CID/watcher proof와 별도 terminal 상태. 응답 유실·삭제 재전송 없음·peer/directory × bridge/host 새 daemon/peer/path·backlog와 새 checkpoint bytes·strict cleanup PASS; [승인 계약과 증거](CEPHFS_PROCESS_ACKNOWLEDGMENT.md). Drained·Released·원격 unlock은 별도 |
| 완료 | Cephx·CephFS grant/eviction의 owner/control admission | caller context cause·secret-safe 오류·부분 client/grant 보존. 원본 Quay RADOS/CephFS 인증 bridge/host PASS·strict cleanup; [범위와 결과](TOPOLOGY_CONTEXT.md#cephxsubvolume-authorization의-context-admission). Context 없는 customizer와 post-native publication은 별도 |
| 완료 | mirror directory 추가의 응답 유실·등록 intent | `BeginDirectoryAddition`·read-only `Status`, zero inventory·실제 응답 유실·미적용 요청·typed 제거 후 재등록. 원본 Quay bridge/host·56 byte/hash·12 checkpoint·독립 verifier·strict cleanup PASS; [등록 계약과 증거](CEPHFS_DIRECTORY_ADDITION.md) |
| 완료 | RBD receiver의 pool/namespace topology readiness | 이미지 없이 exact owned cohort·namespace/election 관측. 원본 Quay bridge/host 10 scope·70 readiness·10 byte marker·8 checkpoint, 기존 A→B→A 회귀와 source·cleanup 검증 완료; [계약·범위](RBD_RECEIVER_READINESS.md) |
| 완료 | RBD·CephFS 최초 mirror daemon 없는 정책 구성 | additive `NoInitialDaemons`; 기존 zero default 보존, 원본 Quay bridge/host 여섯 fresh pair의 first Add·partial cleanup·factory 지연·raw resource·실제 bytes·source/cleanup·독립 증거 검증; [계약·범위](NO_INITIAL_MIRROR_DAEMONS.md) |

Bootstrap 갱신은 cluster가 소유한 daemon을 갱신하는 현재 계약의 실패가 아니라, 별도 연결과 caller 소유 config의 경계입니다. Removal drain도 현재 observer의 명시적인 current-owned-policy 범위와 구분합니다. Peer map에서 UUID가 없어지는 것과 in-flight replayer shutdown 완료는 서로 다른 관측입니다. [관측 계약과 검증](MIRROR_OBSERVABILITY.md)을 따릅니다.

근거: [Ceph network 구성](https://docs.ceph.com/en/tentacle/rados/configuration/network-config-ref/), [RBD mirror daemon 구성](https://docs.ceph.com/en/tentacle/rbd/rbd-mirroring/), [CephFS mirror 모듈](https://docs.ceph.com/en/tentacle/cephfs/cephfs-mirroring/), [RGW multisite](https://docs.ceph.com/en/tentacle/radosgw/multisite/).
