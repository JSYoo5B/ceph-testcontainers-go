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

호출이 잠금을 얻은 뒤에는 기존 native API와 Docker operation의 context 처리·부분 실패 계약을 따릅니다. 위 표는 해당 직렬화 gate의 진입 대기 범위입니다. 기존 context 없는 getter, 남은 직접 control 조회·context 없는 WithClient customizer cache와 post-native descriptor publication은 별도 범위입니다. Cephx/authorization의 진입 대기는 아래 추가 계약을 따릅니다. Host RGW allocator와 CephFS setup/scale family의 admission은 아래 추가 계약을 따릅니다. 모든 public API의 전체 latency가 caller deadline 안에 끝난다는 계약으로 넓히지 않습니다.

Native 변경 뒤 resource reconciliation을 위한 짧은 잠금은 소유권 기록을 완료합니다. 그 지점에서 cancellation을 이유로 handle 추적을 버리면 subsequent cleanup과 retry가 깨집니다. 새 context에서 같은 handle을 다시 호출해 확인·정리를 이어갑니다.

## Context를 받는 설정·구성 조회

기존 context-free getter는 유지합니다. 긴 topology 작업이나 control 종료와 함께 사용하는 호출자는 아래 variant로 잠금 대기에 caller context를 적용할 수 있습니다.

| 공개 API | 반환·소유권 |
| --- | --- |
| `ConnectionConfigContext(ctx)` | 독립된 ceph.conf·admin keyring byte 복사본. 종료·불완전한 bootstrap을 거부하고 owner 및 config cache 잠금 대기를 제한 |
| `ManagersContext(ctx)` | daemon 이름순 독립 slice. Descriptor와 container는 fixture 소유이며 종료 후 남은 descriptor 조회도 허용 |
| `rgw.GatewaysContext(ctx, cluster)` | gateway 이름순 독립 slice. Partial startup descriptor와 fixture 소유권 유지 |
| `ControlContainerContext(ctx)` | 기존 안정적인 control CLI handle 또는 기본 MON. Control handle 잠금만 사용하며 반환 이후 process 수명을 보장하지 않음 |

`Ceph(ctx)`는 context를 받는 control snapshot을 사용합니다. Cluster owner mutex를 다시 얻지 않으므로 이미 owner를 보유한 MON/MGR/OSD 작업에서도 사용할 수 있습니다. Native query 동안 snapshot의 read lock을 유지하지 않습니다. Snapshot과 process 종료 사이의 기존 lifetime race는 native 오류로 보고합니다.

Multicluster constructor와 RGW zone/zonegroup 사전 검증은 source/destination의 context-aware control·bootstrap snapshot을 사용하고 오류 cause를 `%w`로 보존합니다. CephFS MGR attachment는 manager snapshot 오류 뒤 native manager query를 실행하지 않습니다. 새 RGW cluster 판단도 gateway snapshot 오류 뒤 pool query를 실행하지 않습니다. CephFS watcher·peer 설정 갱신 및 RBD 설정 갱신의 control 조회도 같은 context를 사용합니다. Ceph 내부의 나머지 직접 `cliContainer` 경로·WithClient customizer cache는 아래 추가 CephFS 및 인증 범위와 구분합니다.

새 getter의 unit test는 실제 owner/config/control writer 잠금을 caller deadline 이후까지 유지합니다. 이미 owner를 보유한 `Ceph(ctx)`가 control snapshot만 얻어 native query에 도달하는 경로도 검사합니다. Configuration byte copy와 정렬된 descriptor slice, 기존 closed-cluster inspection 계약을 유지합니다.

`TestMultiClusterTopologySnapshotsHonorBusyOwners`는 독립된 실제 source/destination cluster를 각각 busy 상태로 만들고 `rbd.RunMirror`, `cephfs.RunMirror`, `rgw.RunMultisite`, `rgw.RunTopology` 8개 호출을 검증합니다. 50ms deadline과 400ms watchdog 안에 context cause를 보존하며 후속 Exec/file copy/cleanup 0회, nil partial fixture를 확인합니다. 잠금을 해제한 뒤 같은 cluster의 bootstrap/keyring·MGR/RGW descriptor·native OSD ID와 RADOS 원문이 그대로인지 확인합니다. 잠금 획득은 실제 `AddOSD` 진입을 이용하며 mutation 직전의 caller-owned container wrapper에서 중단합니다.

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

## CephFS setup·scale·provisioning의 context admission

Filesystem setup과 MDS scale, subvolume/group·snapshot/clone·data-pool·pin의 기존 operation gate는 setup → owner → control 순서로 caller context를 적용합니다. Inner owner/control에 진입하지 못하면 이미 얻은 setup 잠금을 반환하고 native 조회·변경 전에 원래 context cause로 반환합니다. Configured startup timeout이 admission 뒤 시작되는 기존 timing 계약을 유지하며, caller의 더 짧은 deadline은 admission부터 적용됩니다.

`cephfs.MDSStatus`, scale의 loop/poll과 retirement preflight는 context 없는 `MDSs()` 대신 private context snapshot을 사용합니다. Snapshot 실패를 daemon 0개로 처리하지 않습니다. Native query 동안 owner 잠금을 유지하지 않으며 descriptor membership과 native FSMap을 하나의 atomic view로 보장하지 않습니다. 기존 `MDSs()`의 context 없는 inspection과 descriptor 생성 순서·identity·closed-fixture 조회 계약을 유지합니다.

Inherited `CreatePool` 진입과 pin/clone/data-pool의 직접 control snapshot에도 context를 적용합니다. Clone wait/cancel이 immutable startup timeout을 읽기 위해 owner를 얻던 잠금은 제거했습니다. Scale의 daemon inspect 오류는 `%w`로 원래 cause를 유지하고, desired count publication 전에 closed/원래 filesystem ownership을 다시 확인합니다. Restored pin의 fast-path도 setup 진입에 context를 적용하므로 already-canceled 호출은 cause를 반환하고 새 context에서의 idempotence는 유지합니다.

Native 성공 뒤 원래 FS/pool identity 기록, nonnil partial MDS 등록, 확인된 retirement inventory 정리는 caller cancellation과 별개로 보존합니다. 그 post-native publication 잠금과 당시 미변경 authorization·WithClient customizer cache는 이 setup/scale 변경의 전체 latency 보장에 포함되지 않습니다. 이후 인증 admission은 아래 별도 계약을 따릅니다. 외부 native 정책 변경과 owner snapshot도 atomic으로 묶지 않습니다.

영구 unit regression은 실제 setup/owner/control 잠금을 deadline 이후까지 보유합니다. 8개 public operation × 세 gate, setup/pin·MDS snapshot/status, 관측된 native 경계 이후 registration/desired config/WaitReady 대기, inherited CreatePool·직접 control 조회를 검사합니다. Causal deadline/cancel, 후속 native query/mutation 0회, descriptor/count/identity·recovery handle 보존과 같은 fixture의 새 context 재시도를 확인합니다. Successful identity readback 직후 취소해도 원래 identity가 기록되는 것을 별도 확인합니다. 기존 canceled-data-pool 테스트도 setup을 반환하기 전에 취소 결과를 받도록 강화했고 failure cleanup은 한 경로에서 잠금 반환과 worker join을 수행합니다.

추가 변경의 unit·race·vet·전체 tag compile은 PASS입니다. `artifacts/cephfs-context-admission-20261007/check.log`은 실제 전체 check 기록이며 최종 cancellation-test cleanup 수정의 unit·race는 별도 로그로 보존합니다. 원본 pinned Quay 20.2.4 Linux ARM64에서 다음 focused native 회귀를 통과했습니다.

| Parent | 실행 결과 | 실제 확인 범위 |
| --- | --- | --- |
| `TestCephFSMDSScaleTopology` | bridge PASS 108.73s | 같은 FSID·pool에서 1 active → 2 active + 1 standby → 1 active + 1 standby → 1 active, retired container 제거·다른 filesystem 유지·새 client session 원문 |
| `TestCephFSMDSScaleStandbyReplayTopology` | bridge PASS 106.80s | 초기 replay follower 및 같은 scale 전환, filesystem·pool·별도 filesystem identity와 원문 유지 |
| `TestCephFSDynamicDataPools` | bridge 118.77s / host 118.56s PASS | replicated/EC attachment·native pool ID·fresh libcephfs bytes·clone placement, managed-use/all-namespace object guard 및 unused registration 제거 |
| `TestCephFSPins` | bridge 252.05s / host 238.10s PASS | group export rank1·가까운 subvolume rank0, distributed/random의 두 active rank 배치·각 16개 파일·원래 pin 값 복원 |
| `TestCephFSCloneCancellationAndPartialCleanup` | bridge 226.47s / host 219.69s PASS | canceled/failed clone·source protection 해제·명시적 partial cleanup·same-name replacement 보존·frozen snapshot bytes와 독립 clone I/O |

다섯 parent와 예상 child 10개는 FAIL/SKIP 없이 package terminal PASS 1,389.544초입니다. MDS 두 구성의 fresh native session 16회에서 각 38,912 bytes와 SHA256을 확인하고 별도 filesystem은 scale 세 단계마다 동일 원문을 유지했습니다. `native-cleanup/after.json`은 같은 engine에서 새 container/network/Ryuk 잔여 0개를 확인합니다. `source-before.json`·`source-after.json`의 관련 source 219개(Go·script·module·Makefile)는 실행 전후 동일하며 고정 image 정책 해시도 유지됐습니다. `verification.json`은 원본 named 결과·native byte proof·cleanup·각 실제 check log 해시를 대조합니다. 이번 실행은 현재 selector 106개 전체 CI나 공식·Debian·Ubuntu matrix 재실행의 완료를 의미하지 않습니다.

이 focused 실행을 전체 CI나 다른 image matrix의 새 검증으로 합산하지 않습니다. [MON bootstrap 재연결](MON_BOOTSTRAP_REFRESH.md)과 [topology 변경 계약](TOPOLOGY_EXTENSIONS.md)의 소유권 경계를 유지합니다.

## Cephx·subvolume authorization의 context admission

`CreateClient`, `DeleteClient`, `ClientCapabilities`, `UpdateClientCaps`와 CephFS authorize/deauthorize/evict의 기존 owner 진입은 caller deadline·cancel을 따릅니다. CephFS의 setup → owner → control 순서를 유지하며, volume identity를 먼저 읽은 뒤 나중에 얻는 owner에도 같은 context를 적용합니다. `CreateClient`의 첫 bootstrap 확인과 key 생성 직전 control snapshot도 context를 사용합니다. 대기 중 취소되면 아직 시작하지 않은 native 인증 변경을 실행하지 않습니다.

Native key 생성·keyring copy·auth 오류는 고정 operation 메시지와 `context.Canceled`·`DeadlineExceeded` 원인만 보존합니다. 원래 native 오류를 연결하지 않으므로 그 오류의 key·credential 문자열을 노출하지 않습니다. `errors.Is`로 caller 또는 native의 canonical context cause를 확인할 수 있습니다. Sanitizer는 caller context를 한 번만 읽어 그 사이의 취소 전환으로 native cause를 잃지 않습니다.

이미 생성한 client와 성공한 native readback의 created/ready/revoked·caps·grant 기록은 취소 후에도 보존합니다. Fresh client 생성 뒤 authorization 진입에서 취소되면 nonnil grant에 ready client를 반환할 수 있지만 grant 자체는 아직 authorize를 시도하지 않았습니다. 이를 native 권한으로 adopt하거나 자동으로 principal을 삭제하지 않습니다. 필요하면 원래 `grant.Client`로 명시적인 `DeleteClient`를 수행하고 새 principal로 다시 구성합니다. Attempted grant와 revoke는 원래 private scope로 재시도하며 copied public descriptor의 변경으로 다른 리소스를 채택하지 않습니다.

Keyring 임시 파일 정리는 기존처럼 caller 취소와 분리된 최대 10초 context를 사용합니다. Native 실행 시간, 이 정리와 이미 확인한 결과의 bookkeeping publication까지 caller deadline 안에 끝난다는 계약은 아닙니다. Context 없는 `WithClient`·`WithClientIdentity`도 유지합니다. 내부 daemon startup은 cluster owner를 가진 상태에서 config cache를 읽으므로 runtime config writer와 경합하지 않으며, 외부 customizer의 짧은 byte-copy cache lock은 이 인증 변경의 범위 밖입니다.

15개 새 unit parent가 실제 owner/control 및 setup → owner → control 대기, 관측된 native 경계 뒤 late admission, 잠금 반환, 동일 fixture의 새 context 재시도, partial client/grant, 성공 readback 직후 취소, applied mutation 이후 불확실한 readback, 원래 identity replacement 거부를 검증합니다. Secret masking 도중의 취소 전환도 deterministic regression으로 확인했으며 두 번 context를 읽는 잘못된 sanitizer의 negative control은 실패하고 최종 구현은 통과했습니다. Native 실행 후 late-control watchdog의 failure cleanup을 launch 전에 등록하고 late callback publication을 막는 테스트 정리만 보완했으며, 최종 focused unit·race(count=3)가 각각 1.097초·3.244초 PASS입니다. Production은 변경하지 않았습니다.

전체 `make check`의 unit·race·vet·tag compile 및 독립 production 검토가 통과했습니다. 고정 원본 Quay Ceph 20.2.4 Linux ARM64에서 다음 focused 회귀를 실행했습니다.

| Parent | bridge / host | 실제 확인 |
| --- | --- | --- |
| `TestClientIdentities` | PASS 48.86s / 48.72s | 65,536-byte RADOS 원문, RO/RW·다른 pool/namespace 거부, wrong key·revoke의 fresh 연결 거부, writer→reader→writer의 동일 key와 omitted MGR cap 제거 |
| `TestCephFSSubvolumeClientAuthorization` | PASS 128.84s / 133.62s | 524,288-byte CephFS 원문·RADOS namespace 접근, 16개 fresh probe, RO/RW·neighbor 격리, scoped revoke 후 native EPERM·동시 admin 연결, 다른 권한/key 보존 및 established-session eviction |

`artifacts/auth-context-admission-20261007/`의 `check.log`, `native-runtime.log`, `changed-test-unit.log`, `changed-test-race.log`, `independent-review.md`, `verification.json`에 기록했습니다. Package terminal은 PASS 360.498초이고 같은 engine의 신규 container/network/Ryuk는 0개입니다. Native 실행 중 source·Makefile 224개의 해시가 동일하며 실행 후 위 watchdog failure 정리 테스트 한 파일만 변경된 사실과 별도 검증을 기록했습니다. 고정 이미지 정책 해시도 유지했습니다. 현재 selector 107개는 유지하며 이 두 기존 parent의 focused 실행을 전체 CI·다른 이미지 계열·새 native cancellation fault의 증거로 표시하지 않습니다.
