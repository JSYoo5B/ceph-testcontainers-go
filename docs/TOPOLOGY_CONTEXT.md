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

호출이 잠금을 얻은 뒤에는 기존 native API와 Docker operation의 context 처리·부분 실패 계약을 따릅니다. 위 표는 해당 직렬화 gate의 진입 대기 범위입니다. 기존 context-free getter 자체의 대기, Ceph 내부의 직접 control 조회, CephFS setup/scale·subvolume/data-pool의 여러 단계 잠금과 post-native descriptor publication은 별도 범위입니다. Host RGW allocator의 admission·publication은 아래 추가 계약을 따릅니다. 모든 public API의 전체 latency가 caller deadline 안에 끝난다는 계약으로 넓히지 않습니다.

Native 변경 뒤 resource reconciliation을 위한 짧은 잠금은 소유권 기록을 완료합니다. 그 지점에서 cancellation을 이유로 handle 추적을 버리면 subsequent cleanup과 retry가 깨집니다. 새 context에서 같은 handle을 다시 호출해 확인·정리를 이어갑니다.

## Context를 받는 설정·구성 조회

기존 context-free getter는 유지합니다. 긴 topology 작업이나 control 종료와 함께 사용하는 호출자는 아래 variant로 잠금 대기에 caller context를 적용할 수 있습니다.

| 공개 API | 반환·소유권 |
| --- | --- |
| `ConnectionConfigContext(ctx)` | 독립된 ceph.conf·admin keyring byte 복사본. 종료·불완전한 bootstrap을 거부하고 owner 및 config cache 잠금 대기를 제한 |
| `ManagersContext(ctx)` | daemon 이름순 독립 slice. Descriptor와 container는 fixture 소유이며 종료 후 남은 descriptor 조회도 허용 |
| `GatewaysContext(ctx)` | gateway 이름순 독립 slice. Partial startup descriptor와 fixture 소유권 유지 |
| `ControlContainerContext(ctx)` | 기존 안정적인 control CLI handle 또는 기본 MON. Control handle 잠금만 사용하며 반환 이후 process 수명을 보장하지 않음 |

`Ceph(ctx)`는 context를 받는 control snapshot을 사용합니다. Cluster owner mutex를 다시 얻지 않으므로 이미 owner를 보유한 MON/MGR/OSD 작업에서도 사용할 수 있습니다. Native query 동안 snapshot의 read lock을 유지하지 않습니다. Snapshot과 process 종료 사이의 기존 lifetime race는 native 오류로 보고합니다.

Multicluster constructor와 RGW zone/zonegroup 사전 검증은 source/destination의 context-aware control·bootstrap snapshot을 사용하고 오류 cause를 `%w`로 보존합니다. CephFS MGR attachment는 manager snapshot 오류 뒤 native manager query를 실행하지 않습니다. 새 RGW cluster 판단도 gateway snapshot 오류 뒤 pool query를 실행하지 않습니다. CephFS watcher·peer 설정 갱신 및 RBD 설정 갱신의 control 조회도 같은 context를 사용합니다. Ceph 내부의 다른 직접 `cliContainer` 경로와 CephFS setup/scale의 혼합 잠금은 여전히 별도 범위입니다.

새 getter의 unit test는 실제 owner/config/control writer 잠금을 caller deadline 이후까지 유지합니다. 이미 owner를 보유한 `Ceph(ctx)`가 control snapshot만 얻어 native query에 도달하는 경로도 검사합니다. Configuration byte copy와 정렬된 descriptor slice, 기존 closed-cluster inspection 계약을 유지합니다.

`TestMultiClusterTopologySnapshotsHonorBusyOwners`는 독립된 실제 source/destination cluster를 각각 busy 상태로 만들고 `RunRBDMirror`, `RunCephFSMirror`, `RunRGWMultisite`, `RunRGWTopology` 8개 호출을 검증합니다. 50ms deadline과 400ms watchdog 안에 context cause를 보존하며 후속 Exec/file copy/cleanup 0회, nil partial fixture를 확인합니다. 잠금을 해제한 뒤 같은 cluster의 bootstrap/keyring·MGR/RGW descriptor·native OSD ID와 RADOS 원문이 그대로인지 확인합니다. 잠금 획득은 실제 `AddOSD` 진입을 이용하며 mutation 직전의 caller-owned container wrapper에서 중단합니다.

추가 변경의 unit·race·vet와 전체 tag compile은 PASS이며 실제 tee 기록은 `artifacts/context-snapshots-20261007/check.log`입니다. 같은 원본 Quay 20.2.4 Linux ARM64에서 아래 focused runtime을 실행했습니다.

| 추가 검증 | 결과 |
| --- | --- |
| 실제 source/destination busy owner와 네 종류 constructor | 8개 child PASS, parent 56.53초 |
| 양쪽 MON 전체 교체·동일 client/mirror cold restart·local/remote bootstrap 갱신 | bridge 357.92초, host 370.31초 PASS |
| RGW 3개 cluster·2개 zonegroup 구성과 zone 제거 | 291.29초 PASS |

Package terminal은 PASS 1,076.494초입니다. MON 경로의 각 mode에서 CephFS source snapshot ID/name `2/backup-1`(초기·cold restart), `3/backup-2`, `4/backup-3`(새 peer)와 destination 원문을 확인했습니다. 같은 engine의 신규 container/network/Ryuk 잔여는 0개이고 관련 source와 Makefile 212개 SHA256은 실행 전후 동일합니다. `verification.json`은 실제 3개 parent·MON의 두 mode·constructor 8개 child·8개 checkpoint와 cleanup을 원본에서 대조합니다. Selector inventory는 이 실행 당시 105개 required test 이름을 확인한 목록이며 이 focused run을 전체 matrix 재실행으로 표시하지 않습니다.

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

## Host RGW 포트 allocator의 소유권

Host RGW는 native port allocator를 만들기 전에 caller context로 cluster owner에 진입하고 closed 상태를 확인합니다. Allocation과 nonnil lease 기록을 같은 critical section에서 처리하므로 동시 `Terminate`가 inventory를 먼저 정리한 뒤 늦게 lease가 추가되는 순서를 막습니다. 기존 코드도 nonnil lease를 오류 분기 전에 기록했으며, 이번 변경은 admission과 publication 순서를 조정합니다.

Allocation 뒤 native 오류나 caller cancellation이 있어도 nonnil lease를 기록하고 원래 오류/context cause와 함께 반환합니다. 이후 새 context의 `Terminate`가 같은 allocator cleanup을 재시도합니다. 성공한 lease 반환은 반복하지 않습니다. Generic service·gateway descriptor의 post-native publication과 초기 MON reservation은 이 helper의 변경 범위가 아닙니다.

6개 unit parent가 busy/closed/canceled admission의 allocation 0회, partial lease identity·오류 cause 유지, cleanup 실패/재시도/idempotence, 동시 termination과 inventory 순서를 검증합니다. 원본 Quay 20.2.4 Linux ARM64의 `TestRGWNativeTLS`는 bridge 46.41초·host 45.71초 PASS로 HTTP/TLS 별도 포트, 실제 인증서 검증과 S3 bytes를 확인했습니다. `TestRGWTopology`는 bridge 74.61초·host 74.52초 PASS로 gateway 증감·재생성과 기존 원문을 확인했습니다.

Unit·race·vet·전체 tag compile 및 위 native 결과는 `artifacts/peer-drain-host-ports-20261007/`에 보존합니다. 같은 suite의 CephFS peer drain을 포함한 terminal은 PASS 710.650초이며 strict cleanup은 신규 container/network/Ryuk 0개, 관련 source·Makefile 217개는 실행 전후 동일합니다. 동시 termination 순서의 회귀는 unit fixture에서 검사한 것이며 native suite에서 해당 concurrency fault를 별도 주입했다는 뜻은 아닙니다.

이 focused 실행을 전체 CI나 다른 image matrix의 새 검증으로 합산하지 않습니다. [MON bootstrap 재연결](MON_BOOTSTRAP_REFRESH.md)과 [topology 변경 계약](TOPOLOGY_EXTENSIONS.md)의 소유권 경계를 유지합니다.
