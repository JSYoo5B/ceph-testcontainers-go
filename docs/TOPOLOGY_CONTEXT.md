# Topology 호출의 잠금 대기와 caller context

긴 native 작업을 직렬화하는 기존 owner mutex의 진입 대기는 caller의 deadline·cancel을 따릅니다. 다른 호출이 잠금을 계속 보유하고 있어도 취소된 호출은 `errors.Is(err, context.DeadlineExceeded)` 또는 `context.Canceled`로 반환합니다. 잠금을 얻기 전 native mutation·container stop/terminate·ownership 변경을 하지 않습니다. 잠금을 얻은 직후에도 context를 확인합니다.

| Owner | 적용한 진입 경로 |
| --- | --- |
| Ceph cluster | MON/MGR 추가·제거, OSD 추가·제거, owned service 생성, RGW 제거, cluster 종료, 선택적 network 단절 |
| Docker handle | host port lease 반환, network endpoint 복구·기존 interruption 완료 상태 조회 |
| RBD link | daemon 추가·제거·종료, link 종료, peer rebootstrap, image enable, policy 조회, peer network 단절 |
| CephFS link | daemon 추가·제거·종료, link 종료, MGR peer 연결, directory 추가·제거·rebalance, peer 조회·제거·재등록, peer network 단절 |
| RGW multisite | zone/zonegroup 추가·제거, zone admin, link 종료, sync group 생성·변경·제거·적용, zone network 단절 |
| Owned cleanup | 개별 daemon 종료 및 resource cleanup action 처리 |

부분 생성·변경 뒤 cleanup handle과 retry ownership을 유지합니다. 취소 때문에 이미 생성한 resource의 bookkeeping을 건너뛰지 않습니다. Owner→daemon/resources/interruption의 기존 잠금 순서도 유지합니다. 기다리는 goroutine을 남기지 않고 `TryLock`과 bounded poll로 대기합니다.

호출이 잠금을 얻은 뒤에는 기존 native API와 Docker operation의 context 처리·부분 실패 계약을 따릅니다. 위 표는 해당 직렬화 gate의 진입 대기 범위입니다. Constructor와 내부 getter의 `ConnectionConfig`·`Managers`·`Gateways` snapshot, control container 조회, CephFS setup/scale·subvolume/data-pool의 여러 단계 잠금, host RGW allocator의 resource publication은 후속 context·ownership 검토가 필요합니다. 모든 public API의 전체 latency가 caller deadline 안에 끝난다는 계약으로 넓히지 않습니다.

Native 변경 뒤 resource reconciliation을 위한 짧은 잠금은 소유권 기록을 완료합니다. 그 지점에서 cancellation을 이유로 handle 추적을 버리면 subsequent cleanup과 retry가 깨집니다. 새 context에서 같은 handle을 다시 호출해 확인·정리를 이어갑니다.

## 검증

실제 mutex를 20ms caller deadline 이후까지 계속 잡은 fixture에서 native `Exec`·`Stop`·`Terminate` 0회와 inventory 불변을 확인합니다. 같은 fixture를 새 context로 재시도해 OSD UUID guard·purge, mirror daemon cleanup, port lease 반환과 endpoint 복구가 수행되는지 별도로 확인합니다. Nested cleanup/network mutex도 같은 방법으로 검사합니다. 대기 테스트에는 500ms watchdog을 두며 already-canceled context가 비어 있는 gate도 획득하지 않는지 확인합니다.

Unit·race·vet와 `integration,auth,features,multicluster,topology,hostnetwork,goceph,diagnostics` 전체 tag compile이 통과했습니다. `artifacts/context-gates-20261007/context-check-validation.json`은 실제 tool 응답의 command·exit code·출력을 보존한 기록입니다. 원래 tee log가 있는 것으로 표시하지 않습니다. `covered-methods.txt`·`.json`은 42개 기존 직렬화 진입 경로와 추가 nested endpoint 상태 조회, 유지한 짧은 publication 잠금을 구분합니다.

2026-10-07 고정 원본 Quay Ceph 20.2.4를 Docker Desktop Linux ARM64 engine에서 순차 실행합니다. 기존 시나리오는 native topology와 client 데이터의 회귀를 확인하며, 실제 busy mutex의 deadline 계약은 위 unit·race 검증으로 확인합니다.

| Native 시나리오 | 결과 | 확인한 구성·데이터 |
| --- | --- | --- |
| `TestMultiClusterCephFSMirrorDaemonRebalanceTopology` | PASS 369.63s | 4 directory의 2→1→2→1→0→1 daemon, 장애 승계·명시적 재분배·peer 연결 단절, source ID 2~21의 20회 exact checkpoint와 원문 |
| `TestMonitorRollingReplacement` | PASS 334.88s | bridge 181.48s·host 153.39s. a,b,c 전체 교체·owned daemon cold restart·RADOS/S3 원문 및 private 설정·keyring 보존 |
| `TestOSDRemovalLifecycle` | PASS 122.18s | bridge 59.70s·host 62.47s. Purge 응답 유실 재시도·foreign UUID 거부·retained RADOS 객체 |
| `TestMultiClusterRBDMirrorDaemonTopology` | PASS 128.56s | leader stop/failover·교체·0개 이후 재개, 원래 receiving peer·image identity와 ranged write 뒤 8 MiB 원문 |
| `TestMultiClusterRGWZonegroupsAndRemovalTopology` | PASS 290.80s | 3개 독립 cluster·2개 zonegroup의 동적 추가·zone 탈퇴, exact period 전파·S3 복제/redirect·native object 원문 |

`artifacts/context-gates-20261007/native-runtime.log`의 다섯 parent 및 MON/OSD bridge·host child가 모두 통과했고 package terminal은 PASS 1,246.672초입니다. `native-cleanup/after.json`은 같은 engine에서 신규 container/network/Ryuk 잔여 0개를 확인합니다. `source-before.json`·`source-after.json`의 관련 production/test/script/module source 208개 SHA256이 실행 전후 동일하며 `verification.json`은 terminal·named 결과·20개 source checkpoint·cleanup을 원본에서 대조합니다. 고정 image 정책 SHA256도 `4682819b3174a632b9da32eb955f9c52593a24112f780f64280380f0e6ccee62`로 유지됐습니다. 별도 admin socket read-only schema probe는 다음 CephFS drain 설계 자료이며 이번 drain 기능의 완료 증거로 표시하지 않습니다.

이 focused 실행을 전체 CI나 다른 image matrix의 새 검증으로 합산하지 않습니다. [MON bootstrap 재연결](MON_BOOTSTRAP_REFRESH.md)과 [topology 변경 계약](TOPOLOGY_EXTENSIONS.md)의 소유권 경계를 유지합니다.
