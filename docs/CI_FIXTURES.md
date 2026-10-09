# 시나리오 fixture CI

## 현재 필수 CI: 공식 roles 시나리오

Go 모듈 CI는 준비된 `official-20.2.4-{control,osd,rgw,mds}`로
Linux AMD64에서 전체 필수 시나리오를 실행한다. 이미지 계열·`all`/역할
조합·AMD64/ARM64의 quick/full 검증과 배포는
[이미지 프로젝트 CI](../../ceph-testcontainers-images/.github/workflows/test.yml)가
담당하며, 이 저장소의 자동 `image-compatibility` matrix 12개 job은 제거했다.
이미지 검사기의 성공을 모든 Go 조합의 성공으로 표시하지 않는다.
다른 이미지나 platform의 Go 연결 검사가 필요하면
[수동 호환성 target](IMAGE_COMPATIBILITY.md#공식debianubuntu-이미지-matrix)을 사용한다.

| 필수 범위 | Matrix 포함 `8ef88e7` | Matrix 제거 `27d8008` | 현재 관련 시나리오 묶음 |
| --- | ---: | ---: | ---: |
| Job | 79 | 67 | 58 |
| 자체 resource cleanup | 78쌍 | 66쌍 | 57쌍 |
| 주요 roles 이미지 준비 | 66 | 66 | 57 |
| Distinct test parent | 120 | 120 | 121 |
| 주요 시나리오 parent 실행 instance | 127 | 127 | 128 |
| 자동 이미지 조합 반복 | 12개 × 대표 9개 | 0 | 0 |

이 수는 현재 workflow·compiled selector의 범위다. 제거한 대표 9개 이름은
모두 남은 필수 시나리오에서 검증한다. 각 runner의 이미지 ID 기록·native
assertion·완료 검사·항상 실행하는 cleanup을 유지하며 테스트 내부의 phase나
negative window를 줄이지 않는다. `scenario-default`도 필수 검사다.
Compiled selector 기준 Ceph integration parent 119개·126회 실행에
Docker bridge SDK parent 2개·2회 실행을 더한 수다.
선택적 `rgw-native-regressions`도 기본적으로 같은 roles를 준비하고,
비어 있지 않은 `workflow_dispatch.rgw_image`는 RGW 역할만 덮어쓴다.
그 명시 입력은 기본 이미지 준비 receipt와 구분한다. Vault와 Linux go-ceph
소비자 container는 별도 입력이며 Ceph 역할 이미지의 의존성을 바꾸지 않는다.

## 메모리 기반 OSD fixture

`TestOSDInMemoryStorageTopology`는 `WithOSDInMemoryStorage(2 << 30)`을
명시한 bridge cluster 하나로 검사한다. 초기 OSD가 없으면 volume과 keeper도
생기지 않으며, 첫 `AddOSD` 때 cluster 전체가 공유하는 최대 2 GiB의 tmpfs
named volume과 control-image `sleep` keeper를 만든다. 기본 sparse-file
OSD 저장소와 다른 시나리오의 설정은 바꾸지 않는다. 제한은 저장소의 상한이며
RAM을 미리 예약하거나 daemon 메모리까지 제한하는 설정은 아니다.

초기 1 GiB 상한 실험에서는 128 MiB replica 데이터 읽기까지 통과했지만
OSD 재시작의 mClock 자체 benchmark 중 shared volume의 남은 byte가 0이 됐다.
Ceph의 64 KiB AIO가 일부 byte만 기록해 native abort한 로그와 keeper의
`statvfs` 결과를 보존했다. 이에 fixture 상한을 2 GiB로 잡으며 scheduler나
benchmark를 끄지 않는다. 이 크기는 모든 토폴로지의 최소 요구량을 뜻하지 않는다.

두 OSD에 replica 2의 결정적 128 MiB 데이터를 쓰고 전체 reader/hash로 확인한다.
두 OSD를 모두 중지한 동안 원래 keeper가 계속 실행하는지 확인한 뒤 동일
CID·native UUID·FSID·pool ID로 다시 시작해 데이터를 읽는다. 세 번째 OSD의
데이터가 있는 clean PG 배치, 원래 OSD drain·directory 제거, 새 CID/UUID의
replacement와 데이터 보존을 검사한다. Ceph가 numeric ID를 재사용했는지도
기록한다. Keeper를 중지한 뒤에는 `AddOSD`가 등록 전에 거부되고 원래 native
ID/UUID와 stopped keeper를 바꾸지 않는지 검사하며, 마지막 `Terminate` 뒤
owned container·bridge·named volume의 부재를 Docker에서 직접 확인한다.

개별 실행은 `make scenario-osd-memory`의 Go 20분 제한을 사용한다. 필수 CI는
기존 `scenario-empty-bootstrap`에 이 whole parent를 더해 58개 job과 57쌍의
cleanup·roles 준비를 유지한다. 묶음의 기존 80분 process 제한은 공유하며
아래의 11분 55초는 메모리 fixture 추가 전 세 구성원의 기록이다. Phase 시간과
Docker cgroup usage의 phase snapshot 최대 합을 로그에 남기지만 disk 대비
성능 향상이나 실제 순간 peak를 측정한 benchmark로 표시하지 않는다. 새로운
native 실행과 전체 CI 결과는 별도 실행 receipt로 확인해야 한다.

## 짧고 관련 있는 시나리오 묶음

Source `27d8008`의 [run 37870121307](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37870121307)에서
완료된 짧은 job을 기준으로 다음 여섯 묶음을 선택했다. 아래 시간은 각
구성원의 기존 **job 전체 시간의 합**이며 새 묶음의 측정값이 아니다.
선택에 사용한 중간 snapshot은 48개 SUCCESS·19개 실행 중이었다.
해당 snapshot이나 선택 검사만으로 전체 CI 성공을 판정하지 않는다.
후속 GitHub API 조회에서 이 실행은 2026-10-09 02:10:38 UTC에
SUCCESS로 완료됐으며 필수 job 67개 SUCCESS·선택적 regression 1개 SKIP,
생성부터 완료까지 39분 58초였다. 이는 묶음 변경 전 source의 결과다.

| CI target / case | 함께 실행하는 기존 case | 기존 job 시간 합 |
| --- | --- | ---: |
| `scenario-empty-bootstrap` | 최초 OSD·MGR·MDS 없는 구성 | 11분 55초 |
| `scenario-mds-replacements` | 중지 MDS 교체·마지막 MDS 교체 | 9분 9초 |
| `scenario-topology-extensions` / `network-recovery` | cluster network interruption·5 MON quorum recovery | 5분 3초 |
| `scenario-topology-extensions` / `rbd-daemons` | bridge·host mirror daemon·RBD peer network interruption | 10분 3초 |
| `scenario-rbd-fixtures` / `client-setup` | RBD client features·host lifecycle | 6분 30초 |
| `scenario-cephfs-fixtures` / `data-layout` | dynamic data pools·EC data pool·host filesystem | 9분 23초 |

기존 15개 job을 6개로 묶어 필수 job 9개를 줄인다. 각 묶음은 한 Go
process에서 whole parent를 순차 실행하며 각 parent는 원래대로 새 cluster와
client를 만들고 정리한다. Cluster나 native I/O 결과를 다음 parent와
공유하지 않는다. 각 묶음은 최초 resource baseline과 역할 이미지 준비를
한 번 수행하고 마지막에 `always()` cleanup 검사를 한다. 중간에 baseline을
다시 기록하지 않으므로 앞선 parent의 누수도 마지막 검사에 포함된다.
Go 로그의 각 parent RUN/PASS/FAIL과 실패 요약·artifact를 보존한다.
SDK endpoint recovery 2개는 `network-recovery`에서 한 번만 실행한다.
SDK 실패는 job 실패로 유지한다. 최초 baseline과 이미지 준비가 성공하고
취소되지 않았다면 SDK 실패 뒤에도 묶음의 Ceph parent를 시도한다.

새 bootstrap 묶음의 Go process 제한은 80분, job 제한은 90분이며 MDS
교체 묶음은 각각 50분·60분이다. 묶음의 process 전체가 공유하는 제한으로,
각 parent에 그 시간을 별도로 배정하는 설정은 아니다. 두 새 target은
`-failfast` 없이 실행해 일반적인 parent 실패 뒤에도 다음 parent를 시도한다.
Package timeout이나 process 중단 뒤의 실행까지 보장하지는 않는다.
기존 개별 Make target·case·aggregate와 시간 제한·`-failfast` 설정은
유지하며 테스트 내부의 operation context와 failed-child guard도 바꾸지 않는다.
기존 fixture·topology target에 추가한 묶음은 해당 target의 제한을 공유한다.

Original-process quiescence·recovery의 네 leaf씩과 receiver의 bridge·host
job 및 각각의 다섯 scope는 그대로 독립 실행한다. 긴 mirror scope·schedule·
failback·split-brain·CephFS pins도 이번 짧은 시나리오 묶음에 넣지 않았다.
Job 시작·이미지 준비·외부 cleanup의 반복을 줄이는 변경이며 native Ceph
동작 자체의 시간이 줄었다고 주장하지 않는다. 현재 변경은 호스트 검사
40개(이미지 선택 17개·compiled selector 23개)가 통과한 상태다. 새 묶음의
native 실행과 전체 CI 시간은 사용자가 push한 뒤 별도로 확인해야 한다.
실행 원본·선택 근거는 `artifacts/related-scenario-bundles-20261009/`에 보존한다.

## Matrix 제거 전 작업량으로 계산한 예상

아래 성공 source `8ef88e7`의 자동 matrix가 사용한 runner 시간은
14,383초(약 240분)였다. 이를 제외한 같은 시나리오 작업량은
34,841초(약 581분)이다. 기존 20개 동시 실행과 109초의 선행 호스트 검사를
가정하면 이상적 하한은 **30분 46초**다. 고정된 기존 job 시간으로 배정
지연을 0으로 놓은 계산은 관측 순서에서 35분 34초, 긴 job 우선에서
31분 30초였다. 후자는 GitHub가 해당 순서로 배정한다는 보장이 아니다.
새 CI의 실제 완료를 관측하기 전에는 30분 달성이나 성공으로 표시하지 않는다.
범위 비교·원본·계산은 `artifacts/module-role-ci-20261009/`에 보존한다.

## 전체 workflow 시간과 runner 대기

Source `8ef88e7`의 [run 37849665913](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37849665913)은
GitHub API 기준 필수 79개 job이 SUCCESS이고 선택적 native regression
1개만 SKIP이다. 생성부터 마지막 job 완료까지 **54분 15초**였으며,
첫 job 시작부터 완료까지는 54분 12초였다. 앞선 `4da2744`의 생성부터
완료까지 59분 39초와 구분한다. 최장 job은 분리 전 RBD fixture의
35분 7초에서 현재 cluster fixture의 **25분 10초**로 바뀌었다.
최장 job만으로 전체 workflow 시간을 설명할 수 없다.

마지막 `multicluster / rgw-multisite-host` job의 실제 경로는 다음과 같다.
Job API의 `created_at`부터 `started_at`까지를 실행 대기로 계산했다.

| 구간 | 시간 |
| --- | ---: |
| 호스트 `test` job | 1분 49초 |
| 다음 job 배정 | 2초 |
| 필수 `scenario-default` job | 16분 44초 |
| 마지막 RGW job의 생성 후 runner 대기 | 24분 3초 |
| 마지막 RGW job 실행 | 11분 34초 |
| 첫 job 시작부터 전체 완료 | 54분 12초 |

당시 독립 시나리오 job 65개는 `scenario-default` 완료 시점에 한꺼번에
생성됐으며 대기는 중앙값 11분 36초, 최대 25분이었다. 이 job들은 이전
runner의 cluster·workspace·artifact를 사용하지 않는다. 현재 workflow는
모든 주요 시나리오를 호스트 `test` 성공 뒤 허용하며 `scenario-default`도
필수 runtime job으로 병행한다. 각 job의 독립 checkout·역할 이미지 준비·
자체 baseline·항상 실행하는 cleanup, 필수 선택과 기존 시간 제한은 유지한다.

관측한 동시 실행 최대치는 20개이며 성공 job의 runner 점유 합은
820.4분이었다. 작업량과 동시 용량을 그대로 유지하면 `820.4 / 20`인
**41분 1초**가 작업량만 고려한 하한이다. 호스트 검사를 먼저 완료하는
조건까지 포함한 이상적 하한은 **42분 45초**이다. 이 관측만으로 계정의
요금제나 공식 동시 실행 quota를 판정하지 않는다.

같은 job 시간을 고정하고 배정 지연을 0으로 가정한 계산에서, 직렬
의존성 제거와 기존 관측 순서를 사용하면 47분 42초, 긴 job 우선 배정이면
44분 4초다. 30개 slot의 긴 job 우선 계산은 30분 25초, 32개는 28분 53초다.
이 값들은 실제 GitHub 실행 결과가 아니며 workflow 선언 순서가 실행
우선순위를 보장한다고 가정하지 않는다. 같은 호스트 검사 조건과 작업량으로
30분에 도달하려면 적어도 30개의 동시 slot이 필요하고, 20개를 유지하려면
총 runner 작업량을 최소 약 31.1% 줄여야 한다. 이 값도 충분조건은 아니다.
[GitHub 공식 한도](https://docs.github.com/en/actions/reference/limits#job-concurrency-limits-for-github-hosted-runners)는
요금제별 차이와 지원팀을 통한 한도 증가 요청을 설명한다. 계정이나 runner
용량은 이번 변경으로 수정하지 않는다.

현재 결과는 기존 성공 실행의 API timing 분석과 workflow 의존성 변경의
호스트 검사이다. 변경 후 실제 전체 시간은 새 CI가 완료돼야 확정한다.
API 원본·계산·가정과 source별 receipt는
`artifacts/ci-wall-time-20261009/`에 보존한다.

## 긴 시나리오 분리

2026-10-09 KST에 확인한 source `4da2744`의
[성공 run 37804783231](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37804783231)에서
가장 긴 job은 RBD fixture 묶음이었다. Job/step 시간은 GitHub API의
시작·종료 시각, Go package 시간은 각 job의 원본 로그에서 읽었다.

| Job | 전체 | Make/test step | 이미지 준비 | 별도 cleanup 검사 |
| --- | ---: | ---: | ---: | ---: |
| RBD fixtures | 35분 7초 | 34분 27초 | 11초 | 12초 |
| Cluster fixtures | 27분 8초 | 26분 31초 | 10초 | 11초 |
| CephFS process recovery | 25분 13초 | 24분 23초 | 23초 | 12초 |

Test step은 bootstrap·native 동작·CLI polling·각 fixture cleanup을
포함한다. 별도 cleanup 검사는 테스트 안에서 이미 소비한 종료 시간을
대신하지 않는다. 최장 단일 Go parent는 CephFS process recovery
1,455.33초이며 독립된 네 leaf가 각각 355.06–372.80초를 소비했다.
RBD 묶음의 여섯 parent 중 mirror scope가 983.54초, automatic schedule이
393.24초였다. Schedule의 1분 주기와 각 leaf의 필수 negative window는
실제 검사 조건이므로 그대로 유지한다.

Source `8ef88e7`에서는 RBD의 여섯 parent와 CephFS recovery의
`bridge|host × peer|directory` 네 leaf를 독립 runner로 나눴다.
각 leaf 내부의 원래 process·정책·checkpoint·bytes 검사는 함께 실행한다.
Local `scenario-rbd-fixtures`와 `process-recovery` aggregate는 유지하며,
선택 실행은 `SCENARIO_RBD_FIXTURE_CASE`와
`SCENARIO_CEPHFS_REMOVAL_CASE=process-recovery-{network}-{kind}`를 사용한다.
기존 Go·job timeout, role 이미지 준비와 cleanup 조건을 유지한다.
당시 source `8ef88e7`의 compiled selector와 workflow 검사는 distinct
parent 120개와 primary instance 127개를 선택했다. 필수 job 79개,
cleanup 78쌍, 주요 role 이미지 준비 66개와 12×9 이미지 matrix를 사용했다.
Source `27d8008`의 matrix 제거 구성은 67개 필수 job이며 현재 짧은
시나리오 묶음 구성은 58개다. 각 source의 완료 증거를 구분한다.

분리는 가용 runner가 있을 때 wall time을 줄일 수 있다. 준비를 반복하므로
runner 점유 합과 이미지 준비 비용이 줄었다고 주장하지 않는다.
이 분리 구성의 새 성공 실행과 전체 대기는 위 절에 구분해 기록한다.
Timing 원본과 source별 hash receipt는
`artifacts/slow-scenario-wpq-20261009/`에 보존한다.

## 앞선 전체 CI 완료

앞선 CephFS original-process-quiescence 분리 구성은 **120개 distinct
parent, primary 실행 instance 124개**를 선택합니다. 필수 job 71개,
cleanup 70쌍, 주요 역할 이미지 준비 58개와 기존 12×9 matrix를 유지합니다.
Source `3355e8acf822f5bd9b3e64549e24b8f7d46a89b4`의
[run 37787341008](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37787341008)은
2026-10-08 14:50:18 UTC에 SUCCESS로 완료됐습니다. 원본 job 로그,
자체 artifact API·ZIP·upload 연결, source archive와 이미지 publication을
대조하고 필수 71개 job·120개 parent·124회 실행 및 cleanup·준비 수를 확인했습니다.
Required test skip은 0개이며 선택적 `rgw-native-regressions` job만 실행하지 않았습니다.

Receiver는 두 network의 10 scope, quiescence는 네 leaf가 각각 RUN/PASS
1회씩 완료됐습니다. Quiescence leaf마다 원래 세 process phase와 byte 8개,
absence 3개, original-process 3개 증거를 유지했습니다. PoolPGs native 비교는
13 contexts × 4 phases = 52개와 별도 cold/EC/4KiB boundary를 검증했습니다.
Health native marker는 실제 75개이며 TTL expiry polling 때문에 실행마다
관측 수가 달라질 수 있습니다. Public PG snapshot JSON은 보존했고 중간 native
CLI JSON 전체를 보존했다는 주장은 하지 않습니다.

기준 `b7daea9` 설정은 68 job·67 cleanup·55 prep·121 parent instance입니다.
Quiescence parent의 `bridge|host × peer|directory` 네 leaf를 각각 독립
runner로 옮겨 job·cleanup·prep·parent instance가 3개씩 늘었습니다.
Distinct parent는 기존 완료 source `3ac07fe`의 119개에
`TestPoolPGReportedBoundaries`를 추가한 120개 그대로입니다. Compiled
`internal/integration` 120개 중 기존 local-only native shuffle 2개만 제외한
118개와 별도 Docker bridge SDK parent 2개를 선택합니다. Receiver parent는
기존 bridge/host 두 job과 네트워크별 5 scope를 그대로 유지합니다.

최신 완료 증거는 `artifacts/heavy-scenario-split-20261008/ci-3355e8a/`와
별도 원본 보존 receipt·독립 감사 결과에 있습니다. 아래의 `3ac07fe` 67-job
결과는 해당 source의 과거 결과로 보존하며 최신 실행을 대신하지 않습니다.

Source `b7daea9`의 run 37780571140에서는 12×9 이미지 native 테스트와
52개 PG native 비교가 통과했지만, 새 receiver 로그 검증기의 PASS 순서
오류로 bridge/host 두 job이 실패했습니다. 두 network의 원래 parent·5 scope와
Go package는 각각 904.857초·1055.246초에 PASS했고 자체 cleanup도 통과했습니다.
실패는 native 실행 뒤 coverage 단계에 한정됐으며 이미지 역할·의존성 문제는
발견되지 않았습니다. 이 결과를 전체 CI 성공으로 합산하지 않습니다.

`09efe52`는 실제 Go의 parent→network→leaf PASS 순서를 따르도록 수정하고,
모든 하위 PASS가 package 완료보다 앞서야 한다는 조건도 보존했습니다.
실제 Go `testing.T` 출력의 Docker 없는 회귀와 6개 조기 package 완료 위치의
부정 대조, 관련 helper 103개가 통과했습니다. 현재 분리 구성은 이 수정을
포함하며 위의 `3355e8a` 전체 CI에서 새 own-job 로그로 검증했습니다.
최초 b7 실행은 최종 63 success·2 failure·3 cancelled·1 optional skipped로
끝났습니다. 그 원본은 `artifacts/heavy-scenario-split-20261008/ci-b7daea9/`에
별도로 보존하며 최신 전체 성공으로 합산하지 않습니다.

이전 O source는 digest로 고정한 `ceph.DefaultImage`의 원본 Quay Ceph 20.2.4를 사용했습니다. 기본·토폴로지 55개, 별도 CephFS 제거·재등록 복구 5개, RBD receiver 1개, 최초 daemon 없는 mirror 1개, 공유 RBD namespace 1개, scoped RBD image 관측 1개, 최초 OSD 없는 bootstrap 1개, 최초 MGR 없는 bootstrap 1개, 서버/client fixture 48개와 Docker bridge SDK 회귀 2개를 합해 새 profile를 포함한 **distinct top-level test 이름 116개**를 선택하도록 구성합니다. 이전 N source의 실제 선택 115개에서 `TestMultiClusterRBDNamespaceImageObservation` 한 parent만 추가한 실제 compiled 목록을 확인했습니다. 기본 14개와 SDK 전체 11개 중 선택 2개는 유지했습니다. N115·M114와 과거 전체 CI101 결과는 각 source의 증거로 보존합니다. 이 선택 결과 자체는 O source의 116개 전체 CI의 새 runtime 성공을 뜻하지 않습니다. Scoped image 관측의 별도 원본 Quay Linux ARM64 bridge/host 실행은 아래 전용 profile의 실제 범위로 기록합니다. `scenario-multicluster-topology` 20개와 `scenario-cephfs-removal` 5개는 겹치지 않습니다. 2026-10-07의 추가 이름은 `TestOSDRemovalLifecycle`, `TestMonitorRollingReplacement`, `TestMultiClusterMonitorBootstrapRefresh`, `TestMultiClusterTopologySnapshotsHonorBusyOwners`, `TestMultiClusterCephFSPeerRemovalDrain`, `TestMultiClusterCephFSDirectoryRemovalRelease`, `TestMultiClusterCephFSOriginalProcessQuiescence`, `TestMultiClusterCephFSOriginalProcessQuiescenceRecovery`, `TestMultiClusterCephFSDirectoryAdditionIntent`, `TestMultiClusterRBDReceiverReadiness`, `TestMultiClusterNoInitialMirrorDaemons`, `TestMultiClusterRBDNamespaceBinding`, `TestNoInitialOSDTopology`, `TestNoInitialManagerTopology`, `TestMultiClusterRBDNamespaceImageObservation`이며 아래 전체 CI 101개 성공과 별도로 추적합니다. Linux go-ceph 1개는 호출자가 client/runner 이미지를 준비하여 별도 실행하는 선택 target입니다. [이미지 프로젝트 CI](../../ceph-testcontainers-images/.github/workflows/test.yml)는 독립된 quick/full 검사기를 실행하며 이 Go suite나 go-ceph를 실행하지 않습니다. Helper 검사도 포함한 이름 수이며, bridge/host·phase별 subtest 또는 native I/O 수와 같지 않습니다.

분리 전에는 fixture profile 7개·새 이름 48개를 한 CI에 추가했습니다. 현재 Go 필수 CI는 6개 fixture profile·48개이며 go-ceph 1개는 선택 실행입니다. **Source `d9115f4`의 전체 CI는 terminal SUCCESS이며 상세 101개·matrix 12개 조합·필수 cleanup 22개를 모두 확인했습니다.** 아래 목록의 기준은 `artifacts/quay-fixture-ci-inventory-20261004/coverage-plan.json`이며, 이전 실패와 후속 전체 성공은 source별로 다음 절에 기록합니다.

첫 확대 CI는 source `efa5ee3655173c496cc00f8c3e0f78baa7bbedf0`의 [run 37179959997](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37179959997)로 2026-10-04 05:27:55 UTC에 시작했습니다. 05:30 UTC 관측에서는 `make check` job이 SUCCESS, `quay-default`는 실행 중이며 새 fixture runtime job은 모두 대기 상태였습니다. 이 중간 관측은 terminal 성공 증거가 아닙니다.

소비자 Dockerfile의 package 복사 누락을 수정한 source `73cc4ae34ed165bd1438d88fa0e62cbbc9aef296`의 [후속 전체 CI 37180395289](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37180395289)는 2026-10-04 05:36:43 UTC에 시작했으며 첫 관측은 queued 상태였습니다. 첫 run과 별도로 추적한 당시의 중간 관측이며, 새 fixture 전체의 terminal 성공 증거가 아닙니다.

앞선 두 실행에서 `TestMGRModules`가 bridge/host 모두 실패했습니다. 원본 Quay로 재현한 오류는 module 변경 직후 `TemporaryMGRModule`의 첫 조회가 `active MGR is not available`로 실패하는 재시작 구간이었습니다. 첫 snapshot의 bounded 읽기 재시도를 수정한 `5fe327653b325c8887d721cf47bd5ec08b39e187`의 [전체 CI 37181788541](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37181788541)는 2026-10-04 06:05:34 UTC에 시작했습니다. 테스트·시간 제한·native 판정 범위는 동일하며, 이 시작 기록만으로 전체 필수 CI 성공을 판정하지 않습니다.

현재 주요 CI는 공식 역할 이미지 네 개를 검증해 immutable ID로 선택합니다.
모듈의 기본 `ceph.DefaultImage`와 원본 Quay all의 수동 호환성 경로는 유지합니다.
이미지 준비 artifact는 native 테스트 성공을 뜻하지 않으며 각 job의 실제
Go 결과·이미지 identity·자체 cleanup을 함께 확인합니다. 선택 방식은
[이미지 호환성 문서](IMAGE_COMPATIBILITY.md#주요-시나리오의-역할-이미지-선택)를
따릅니다. Native go-ceph 소비자 probe는 이 필수 선택에 포함하지 않습니다.

## 새 역할 이미지의 전체 CI 확인

2026-10-08 source `639f226`의 [run 37743484010](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37743484010)은
terminal SUCCESS입니다. 필수 job 42개에서 distinct parent 119개가 모두
RUN/PASS했고, 필수 parent/child FAIL·SKIP은 0개입니다. 공식·Debian·Ubuntu
× all/roles × Linux AMD64/ARM64의 matrix 12개도 각각 대표 9개를
RUN/PASS했습니다. 자체 cleanup 41쌍은 동일 engine/source에서 새
container/network 0개를 확인했고, 주요 job의 이미지 준비 29개와 실제
native 실행을 이미지 release `37735853373-1`의 immutable identity에
대조했습니다. 원본 Quay all의 matrix 두 cell은 기존 고정 digest를 유지합니다.

기존 선택적 `rgw-native-regressions` job의 SKIP은 필수 범위에 합산하지
않습니다. 이 결과는 아래 parent별 분할 이전 source의 전체 검증입니다.
분할 설정의 실행 시간이나 성공 결과로 대신 사용하지 않습니다.
원본 로그·artifact와 strict 판정은
`artifacts/image-contract-resume-20261008/ci-639f226/`에 보존합니다.

## 독립 parent 분할의 전체 CI 확인

2026-10-08 source `a388e6d`의 [run 37751704257](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37751704257)은
terminal SUCCESS입니다. 필수 job 65개에서 distinct parent 119개와 선택된
필수 child가 모두 RUN/PASS했습니다. 대표 이미지 matrix 12개×9개,
각 job의 자체 cleanup 64쌍과 주요 이미지 준비 52개도 검증했습니다.
필수 FAIL·SKIP은 0개이고, translation의 필수 child 4개와 한 case에서만
실행하는 Docker bridge SDK parent 2개를 유지했습니다. 선택적 native
regression의 SKIP은 완료 범위에서 제외합니다.

공식·Debian·Ubuntu 이미지의 실제 native 실행과 cleanup·원본 job upload를
source 및 immutable release `37735853373-1`에 대조했습니다. Matrix의
기본 summary에는 Docker engine ID가 없으므로 그 필드의 일치를 주장하지
않습니다. 같은 job의 원본 전후 cleanup에서 engine identity와 새 자원
0개를 검증하며, summary의 native platform과 실행 로그를 함께 대조합니다.
원본 API 2페이지의 artifact 165개와 세 verifier의 재실행 결과는
`artifacts/heavy-scenario-split-20261008/ci-a388e6d/`에 보존합니다.

이전 `639f226`과 이번 실행의 측정값은 다음과 같습니다. 그룹 시간은
`scenario-default` 완료부터 해당 그룹의 마지막 job 완료까지로, runner
대기를 포함합니다. 각 case의 최장 실행 시간과 같은 수치가 아닙니다.

| 완료 범위 | 이전 묶음 실행 | 독립 parent 실행 |
|---|---:|---:|
| CephFS fixtures 8개 | 46분 41초 | 32분 37초 |
| topology extensions 12개 | 57분 14초 | 30분 10초 |
| RGW multicluster 6개 | 44분 55초 | 23분 22초 |
| workflow 생성부터 마지막 필수 job 완료 | 72분 04초 | 66분 54초 |

26개 case의 native Ceph 실험 시간 합은 약 1.2% 감소했지만 runner 점유
시간 합은 약 13.6% 증가했습니다. 전체 필수 job의 runner 점유 합은
약 1.6% 증가했습니다. 병렬 완료와 중복된 준비·정리 비용을 구분해야 하며,
CPU 사용량이나 이후 실행의 성능 보장으로 해석하지 않습니다. 각 source의
한 번씩 완료된 CI 비교이고, 실제 관측된 동시 runner 최대치는 20개입니다.

이 `a388e6d` 실행의 마지막 job은 `rgw sync / policy`로, 준비조건 충족 후 runner 대기가
23분 23초이고 job 자체는 28분 35초였습니다. 세 parent는 각각 새
클러스터 쌍을 만들므로 parent별 분할로 추가 bootstrap 없이 독립 실행할
수 있습니다. 아래 새 policy 선택은 이 critical path를 대상으로 하지만,
새 설정의 전체 CI 성공과 관측 시간은 다음 절에 별도로 기록합니다.

## RGW policy 분리와 HealthDetails의 전체 CI 확인

2026-10-08 source `3ac07fe9525e5442fb646fdd67a92d321e0bc89d`의
[run 37766891332](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37766891332)은
terminal SUCCESS입니다. 필수 job 67개에서 기존 distinct parent 119개,
matrix 12×9개, 자체 cleanup 66쌍, 주요 역할 이미지 준비 54개를
검증했습니다. 필수 FAIL·SKIP은 0개이고 translation child 4개와 Docker
bridge SDK parent 2개는 유지했습니다. 선택적 native regression SKIP은
필수 완료 범위에 포함하지 않습니다.

각 policy parent를 단일 job으로 옮겨 추가 Ceph bootstrap 없이 세 개를
병행했습니다. Job별 native PASS는 selective 366.91초, owned bridge
564.12초, owned host 696.41초입니다. 원래 job들의 겹친 runner 실행
구간은 1715→820초였으며 전체 workflow 생성부터 마지막 필수 job
완료까지는 66분 54초→62분 08초였습니다. 반면 필수 runner 점유 합은
48,135→48,406초, 주요 이미지 준비 시간 합은 760.363→875.587초로
증가했습니다. Source와 runner가 다른 한 번씩의 실제 관측이며 분할의
순수 성능 효과나 이후 실행 시간으로 일반화하지 않습니다. 새 마지막
job은 `cephfs removal / process-quiescence`였습니다.

같은 source의 `HealthDetails`는 기본 lifecycle과 matrix 12개에서 native
전체 field를 비교했고, cold MGR의 bridge/host 변형에서 TTL 만료·sticky·
원래 상태 복원까지 검증했습니다. 전체 raw 로그의 비교 기록은 72개입니다.
중간 TTL의 raw JSON 파일 자체를 저장했다는 뜻은 아니며 실제 실행한
독립 oracle의 비교와 predicate를 원본 Go log에서 검증합니다.

원본 source archive, API artifact 171개와 ZIP·upload identity, 세 verifier의
재실행, 독립 완료 검토와 별도 시간 비교는
`artifacts/heavy-scenario-split-20261008/ci-3ac07fe/`에 보존합니다.
아래 receiver 분리와 새 PoolPGs source의 성공 증거로 대신 사용하지 않습니다.

## 이전 전체 CI 결과

2026-10-05 확인한 [run 37226924156](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37226924156)은 source `be58018d23efe0738c668e407075b556e4a09fd3`로 2026-10-04 19:05:23–20:32:00 UTC에 실행됐으며 terminal 결과는 **FAILURE**입니다. 원본 Quay Ceph 20.2.4의 Linux AMD64 필수 runtime job 10개 중 9개가 SUCCESS이고, 그 9개에서 기대한 parent 94개가 모두 RUN/PASS했습니다. Docker bridge SDK 2개는 이 94개에 포함됩니다.

남은 `quay-rgw-sync-fixtures` job은 기대 parent 7개 중 5개를 실행해 3개 PASS·2개 FAIL했습니다. `TestMultiClusterRGWSelectivePolicy`는 prefix를 변경한 뒤 새 객체 `/tc-policy-selected/reports/after-policy-change`가 HTTP 404/`NoSuchKey`로 남아 기다림이 종료됐습니다. `TestHostNetworkMultiClusterRGWOwnedSyncPolicy`도 prefix 변경 뒤 `/tc-owned-selected/reports/after-update`의 새 bytes가 도착하지 않아 같은 404 상태로 deadline에 도달했습니다. 이는 정책 변경 뒤 미래 객체의 실제 복제가 실패한 관측이며, 단순 CLI 성공이나 이전 checkpoint로 통과 처리하지 않습니다. 이 run은 수정 전 실패 증거이며 후속 focused 실행과 구분합니다.

첫 Go 명령의 실패로 Make의 두 번째 translation 명령은 실행되지 않았습니다. 따라서 `TestMultiClusterRGWSyncTranslationFiltering`와 `TestHostNetworkMultiClusterRGWSyncTranslationFiltering`의 필수 child도 이 run에서는 미실행입니다. 전체 기대 101개 중 **99개 RUN·97개 PASS·2개 FAIL·2개 미실행**이며 parent SKIP은 0개입니다. 관측한 child 117개는 모두 PASS이고 child SKIP/FAIL은 0개입니다. 이 child 성공을 실행되지 않은 translation 범위의 증거로 사용하지 않습니다.

Job ID·source head·raw log SHA-256·각 RUN/PASS/FAIL·child 및 package completion은 `artifacts/scenario-fixture-completion-20261005/previous-runtime-audit.json`에 연결했으며 raw 로그는 같은 디렉터리의 `previous-logs/`에 보관합니다. 당시 job 이름은 `quay-*`이고 아래 표는 같은 selector의 현재 `scenario-*` 이름을 사용합니다. 이름 변경이나 이후 source 변경을 이 이전 runtime의 새 PASS로 표시하지 않습니다. 대표 이미지 matrix 12개와 선택적 `rgw-native-regressions`는 위 101개 수에 합산하지 않습니다.

## d9115f4 전체 CI 완료

2026-10-05 확인한 [run 37240162309](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37240162309)는 source `d9115f4d05fda4da6c8d8975bc1b3204fb09ba15`로 2026-10-04 22:28:44–23:54:23 UTC에 실행됐으며 terminal 결과는 **SUCCESS**입니다. 원본 Quay Ceph 20.2.4의 Linux AMD64 상세 runtime job 10개에서 기대 parent 101개와 선택된 child 121개가 모두 RUN/PASS했고 parent/child FAIL·SKIP은 0개, 모든 package 결과는 `ok`였습니다. 앞서 미실행된 translation parent 2개의 필수 `tag_owner_class`·`tenant_system_user_isolation` child도 포함합니다.

101개는 Ceph runtime 90개·bootstrap 실패 cleanup 1개·Docker bridge SDK 2개·helper 검사 8개입니다. Child 121개나 matrix의 대표 9개 반복 실행을 distinct named test 또는 개별 native I/O 수로 더하지 않습니다. 알려진 native 한계의 선택적 `rgw-native-regressions` job은 push에서 SKIPPED였으며 원래 inventory와 완료 조건에서 제외된 경로입니다. G05/G07의 numeric priority·ordinary-user source 권한 거부를 해결하거나 지원으로 표시한 결과가 아닙니다.

공식·Debian·Ubuntu × all/roles × Linux AMD64/ARM64 matrix 12개도 각각 대표 9개 RUN/PASS·child FAIL/SKIP 0·package `ok`를 확인했습니다. Source manifest `1486b355cef20286a528ef7ea1e25a6b2fef1954a8eb64c9a18b14953e52baf6`, 네 component 환경 변수와 source control mirror, image ID·digest·native platform 및 이미지 빌드 0회가 각 artifact에 연결됩니다. 상세 10개와 matrix 12개의 cleanup artifact 총 22개도 전후 동일 engine·source와 새 container/network 0개로 모두 PASS했습니다.

Strict audit는 `artifacts/scenario-fixture-completion-20261005/cleanup-ci-snapshot-20261004T235822-469b79e6/audit.json`에서 `passed: true`와 collection error 0개를 기록합니다. 같은 snapshot의 `run.json`·`jobs.json`·`artifacts-index.json`에 terminal source와 job/artifact identity를 보관합니다. 각 raw log는 `artifacts/scenario-fixture-completion-20261005/cleanup-ci-cache-d9115f4/logs/`, image summary·cleanup before/after 원본은 같은 cache의 `artifacts/`에 보관합니다. 이전 `be58018` 실패와 아래 focused summary의 source 불일치는 각각 원문 그대로 유지합니다.

## RGW policy barrier 수정 후 focused 검증

`0caff38`은 정책 생성·상태·prefix 변경 후 destination의 bucket policy metadata import를 확인한 다음 미래 객체를 쓰도록 fixture를 수정했습니다. Import는 쓰기 전 준비 조건이며 실제 복제 bytes·제외·삭제 판정을 대신하지 않습니다.

2026-10-05 이 수정이 포함된 로컬 Go snapshot을 준비된 원본 Quay 이미지와 Docker Desktop Linux ARM64에서 실행했습니다. `TestMultiClusterRGWSelectivePolicy`, `TestMultiClusterRGWOwnedSyncPolicy`, `TestHostNetworkMultiClusterRGWOwnedSyncPolicy` 3개가 모두 PASS했고 FAIL/SKIP은 0개, Go test binary의 exit code는 0, 실행은 1580.965초였습니다. 종료 후 owned container와 새 network는 각각 0개이며 기존 network는 보존했습니다. 이미지 빌드·pull은 0회입니다.

`artifacts/scenario-fixture-completion-20261005/focused/summary.json`의 strict 결과는 **`source_unchanged: false`, `passed: false` 그대로 보존**합니다. 실행 중 선택되지 않은 `cephfs_multicluster_integration_test.go`의 주석 한 줄이 `// Select a compatible runtime supplying the userspace mirror daemon.`에서 `// The source control runtime supplies the userspace mirror daemon.`으로 바뀌었습니다. `comment-only-source-change.json`은 이 주석을 되돌려 재구성한 SHA-256이 시작 시 manifest와 일치함을 기록합니다. `post-runtime-audit.json`은 원본 로그·summary의 해시와 native 실행·잔존 관측 결과를 별도로 연결합니다. 이 설명으로 strict summary를 PASS로 바꾸거나 실행 당시 source와 현재 source가 동일하다고 표시하지 않습니다.

후속 source `d9115f4`의 전체 성공은 [별도 CI 완료 증거](#d9115f4-전체-ci-완료)에 기록합니다. 위 focused 3개 결과와 source 검증 제한을 그 전체 실행의 증거로 대체하지 않습니다.

## 실행 경로와 시간 제한

현재 CI job과 Make target은 검증할 시나리오를 나타내는 `scenario-*` 이름을 사용합니다. 이전 실행의 `quay-*` job 이름과 artifact 경로는 당시 증거 그대로 보존합니다. 로컬 Make 기본 실행은 `ceph.DefaultImage`를 사용하고, 주요 CI는 준비된 공식 역할 이미지 네 개를 명시적으로 선택합니다. 이 선택은 기존 이미지 runtime 계약과 필수 selector·판정을 유지하며, 이전 Quay 실행의 결과를 새 역할 이미지 결과로 표시하지 않습니다.

[workflow](../.github/workflows/test.yml)는 호스트 `test`의 `make check`와 helper 검사 성공 뒤 모든 독립 시나리오를 허용합니다. 기본·topology·fixture job은 각각 Ubuntu 24.04 Linux AMD64 runner에서 실행하며 `scenario-default`도 필수 검사로 병행합니다. 공개 모듈·integration runner는 `CGO_ENABLED=0`이며, 실제 go-ceph probe만 호출자가 준비하는 Linux 소비자 이미지에서 cgo/native 라이브러리를 사용합니다. 역할 이미지에는 compiler나 개발 헤더를 요구하지 않습니다.

현재 CI 설정은 `scenario-cephfs-fixtures`의 8개 parent를 6개 case job,
`scenario-topology-extensions`의 12개 parent를 9개 case job으로
선택합니다. 두 profile의 case별 Go 제한은 40분이고, runner 제한은
cleanup을 포함해 50분입니다. 같은 case에 묶인 parent는 이 process 제한을
공유합니다. Extensions의 Docker bridge SDK runtime 2개는
`network-recovery` case에서만 한 번 실행합니다.

`scenario-multicluster-topology`의 20개 parent는 `infra` 4개, `rbd` 6개,
`cephfs` 4개를 기존 group으로 유지하고, RGW 6개는 parent별 독립 job으로
선택합니다. `scenario-cephfs-removal`의 5개 parent도 각각 독립 job입니다.
이 두 profile의 job별 Go 제한은 기존 60분, runner 제한은 cleanup을
포함해 70분입니다. 모든 선택에서 기존 bridge/host 하위 케이스를 유지하며
`fail-fast: false`로 다른 case의 결과도 수집합니다. 컴파일된 실제 이름을
이용하는 selector 검사는 분할의 중복·누락·빈 선택과 잘못된 case/group
인자를 거부합니다.

분할 이전 source `22d9371`의 [run 37729295117](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37729295117)에서
CephFS fixtures, topology extensions, multicluster RGW의 Go package는
각각 2206.970초, 2565.836초, 2415.653초였고 해당 job들은 성공했습니다.
그 run 전체는 RBD client encryption rekey의 이미지 의존성 누락으로
FAILURE였으며, 위 시간은 새 분할 설정의 측정값이 아닙니다.

각 parent는 기존에도 자체 cluster를 구성하고 cleanup했습니다. 따라서
parent별 CI 분할은 같은 테스트에 필요한 Ceph bootstrap 횟수를 늘리지
않고, 다른 시나리오가 공유하던 package/job 시간 예산을 분리합니다.
독립 runner의 checkout·Go 준비·이미지 pull은 늘며, runner 대기 때문에
전체 wall time이 줄어든다고 보장하지 않습니다. 같은 cluster·identity를
유지해야 하는 RBD lifecycle phase와 비교적 짧은 RGW protocol backend의
공유 fixture는 유지합니다.

부모별 1차 분할 `a388e6d`의 필수 job은 **65개**, 자체 cleanup은
**64쌍**, 주요 시나리오 이미지 준비 artifact는 **52개**입니다. Distinct parent 119개와 이미지
matrix 12개×대표 parent 9개는 유지합니다. 각 case의 이미지 역할·immutable
identity·source·native 판정·항상 실행하는 cleanup과 독립 artifact를 함께
검사합니다. 이 수는 `a388e6d`에서 완료한 분할의 범위입니다. 위 전체 CI 증거와
측정값을 따르며, 이후 policy 분할 설정의 완료로 합산하지 않습니다.

Source `3ac07fe`의 RGW sync CI 선택은 `policy-selective`, `policy-owned-bridge`,
`policy-owned-host` 각 1개, `account` 2개, `translation` 2개의
독립 job입니다. Policy parent는 각자 기존 fresh cluster 쌍을 사용합니다.
당시 설정의 필수 job은 **67개**, 자체 cleanup은 **66쌍**, 주요 이미지
준비는 **54개**이며 distinct parent 119개와 matrix 12개×9개를 유지합니다.
이 수는 설정의 완료 조건으로, 위 `a388e6d`의 65-job 성공을 새 설정의
native 성공으로 표시하지 않습니다.

각 Go 실행은 기존 40분, cleanup을 포함한 job은 50분 제한을 유지합니다.
Translation의 `tag_owner_class`·`tenant_system_user_isolation` child는
그대로 실행하고 선택적 native regression은 기존처럼 별도입니다.
로컬 `SCENARIO_RGW_SYNC_GROUP=policy`는 기존 세 parent의 한 Go 명령,
`all`은 기존 두 Go 명령과 기본 60분 제한을 유지합니다.

분할 근거인 `e31968e` RGW sync job은 전체 68분 14초였으며, 두 Go
package가 각각 2882.202초·1175.122초를 사용했습니다. 기존 75분 job
여유는 6분 46초였습니다. 같은 native 검증을 group별로 나누면 앞선
시나리오의 시간이 뒤 시나리오의 실행·cleanup 시간을 소진하지 않습니다.
이 RGW sync group 분할 시점의 설정은 필수 job 42개, 자체 cleanup
41쌍이었습니다. Distinct parent 119개와 이미지 matrix 12개×9개는
현재 parent별 추가 분할에서도 유지합니다.

이 분할은 source `27e9338`의 [run 37672475341](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37672475341)에서
확인한 누적 시간 제한에 대응합니다. Multicluster는 앞선 18개 PASS가
86.24분을 사용한 뒤 다음 parent 실행 중 90분 package 제한에 도달했고,
CephFS removal은 앞선 4개 PASS가 77.05분을 사용한 뒤 마지막 parent
실행 중 같은 제한에 도달했습니다. 이 실패를 개별 assertion 실패로
표시하거나 분할 후 성공한 것으로 간주하지 않습니다. 해당 실행의 공식·
Debian·Ubuntu 이미지 matrix 12개는 모두 성공했습니다.

분리 커밋 `e31968e`의 [run 37704685360](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37704685360)은
2026-10-08에 전체 SUCCESS로 끝났습니다. 필수 job 40개, 실제 RUN/PASS
parent 119개, 이미지 matrix 12개×대표 parent 9개와 자체 cleanup 39쌍을
원본 source·run·job·artifact 및 Docker engine/ID로 대조했습니다.
필수 119개는 `internal/integration` 117개와 `internal/dockerbridge`의
runtime parent 2개입니다. 별도 선택 실행인 `goceph-linux` 소비자 probe의
성공을 포함하지 않습니다. 이후 Check 추가 커밋 `457473e`의
[run 37708580179](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37708580179)은
MON rolling replacement의 bridge `RemoveMonitor` 호출에서 실패했으므로
앞선 전체 성공을 최신 main의 결과로 합산하지 않습니다. 해당 native
조회·제거 응답의 처리 계약은 [MON 재연결 문서](MON_BOOTSTRAP_REFRESH.md)를 따릅니다.
이 실행의 terminal 결과는 FAILURE이며 필수 job은 39개 SUCCESS·1개
FAILURE입니다. 실제 parent 119개가 실행돼 118개 PASS·1개 FAIL이고,
RGW sync는 64분 3초로 SUCCESS였습니다. RGW sync의 추가 분할은
이 실행에서 관측되지 않은 timeout 실패를 주장하는 변경이 아니라,
두 Go 실행이 공유하던 job 예산을 분리하는 변경입니다.

로컬 aggregate target과 기본 90분 제한은 유지합니다. 일부 시나리오만
실행하려면 다음과 같이 선택합니다. 테스트 간 병렬 실행은 독립 Docker
엔진을 사용하는 CI job에서 수행합니다.

```sh
make scenario-multicluster-topology SCENARIO_MULTICLUSTER_GROUP=rgw
make scenario-multicluster-topology SCENARIO_MULTICLUSTER_GROUP=rgw-master-failover
make scenario-cephfs-removal SCENARIO_CEPHFS_REMOVAL_CASE=process-recovery
make scenario-cephfs-fixtures SCENARIO_CEPHFS_FIXTURE_CASE=pins
make scenario-topology-extensions SCENARIO_TOPOLOGY_EXTENSION_CASE=cephfs-mirror-host
make scenario-rgw-sync-fixtures SCENARIO_RGW_SYNC_GROUP=policy-owned-host
make scenario-rgw-sync-fixtures SCENARIO_RGW_SYNC_GROUP=translation
```

각 shard는 자체 resource baseline·cleanup·테스트 로그 artifact를 남깁니다.
Baseline 준비가 실패한 경우도 별도 `*-baseline` artifact를 보존하며,
성공한 baseline 없이 후행 cleanup을 PASS로 표시하지 않습니다.
Docker 준비 조회는 최대 60초 안에서 명령당 최대 10초를 사용합니다.
Timeout과 로컬 daemon socket의 연결 실패만 재시도하고 권한 오류·잘못된
응답·지원하지 않는 platform은 즉시 실패합니다. 준비가 끝난 뒤 원래의
engine·resource ID·동일 engine baseline 검사를 별도로 수행합니다.
준비 중 각 명령과 오류를 보존하며 후행 cleanup의 30초 grace는 유지합니다.

아래 이름 수와 runtime 결과는 당시 source의 범위이며, timeout 열은 현재
실행 설정입니다. 현재 `scenario-cluster-fixtures`에는 OSD removal parent가
추가되어 9개를 선택합니다.

| 추가 필수 profile | 당시 이름 수 | Go timeout | CI job timeout | `be58018` runtime 결과 | `d9115f4` runtime 결과 |
|---|---:|---|---|---|---|
| `scenario-cluster-fixtures` | 8 | 40분 | 50분 | SUCCESS · 8/8 PASS · skip 0 | SUCCESS · 8/8 PASS · fail/skip 0 |
| `scenario-cephfs-fixtures` | 8 | CI case별 40분, 로컬 all 120분 | case별 50분 | SUCCESS · 8/8 PASS · skip 0 | SUCCESS · 8/8 PASS · fail/skip 0 |
| `scenario-rados-fixtures` | 4 | 120분 | 130분 | SUCCESS · 4/4 PASS · skip 0 | SUCCESS · 4/4 PASS · fail/skip 0 |
| `scenario-rbd-fixtures` | 6 | 120분 | 130분 | SUCCESS · 6/6 PASS · skip 0 | SUCCESS · 6/6 PASS · fail/skip 0 |
| `scenario-rgw-fixtures` | 14 | 120분 | 130분 | SUCCESS · 14/14 PASS · skip 0 | SUCCESS · 14/14 PASS · fail/skip 0 |
| `scenario-rgw-sync-fixtures` | 7 | CI group별 40분, 로컬 all 각 명령 60분 | group별 50분 | FAILURE · RUN 5 / PASS 3 / FAIL 2 / 미실행 2 · skip 0 | SUCCESS · 7/7 PASS · fail/skip 0 |
| 추가 필수 합계 | 47 | | | 43 PASS / 2 FAIL / 2 미실행 · skip 0 | 47/47 PASS · fail/skip 0 |
| `scenario-goceph-linux` · 선택 실행 | 1 | native integration runner 40분 | 기본 CI job 없음 | caller가 client/runner 준비 | 필수 CI 완료 범위 밖 |

로컬에서는 [Makefile](../Makefile)의 같은 target을 사용합니다. Host network 경로를 container runner에서 실행하면 Docker daemon의 host 주소가 필요합니다. CI는 `TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1`을 지정합니다.

```sh
make check
make scenario-default
make scenario-topology
make scenario-multicluster-topology
make scenario-cephfs-removal
make scenario-rbd-receivers
make scenario-mirror-initial-daemons
make scenario-rbd-namespaces
make scenario-rbd-namespace-observation
make scenario-storage-bootstrap
make scenario-manager-bootstrap
make scenario-mds-bootstrap
make scenario-mds-replacement
make scenario-topology-extensions
make scenario-cluster-fixtures
make scenario-cephfs-fixtures
make scenario-rados-fixtures
make scenario-rbd-fixtures
make scenario-rgw-fixtures
make scenario-rgw-sync-fixtures
# 아래 두 Linux 소비자 이미지는 호출자가 로컬 엔진에 미리 준비합니다.
CEPH_TEST_GOCEPH_CLIENT_IMAGE=ceph-testcontainers-goceph:20.2.4-client \
CEPH_TEST_GOCEPH_RUNNER_IMAGE=ceph-testcontainers-goceph:20.2.4-runner \
make scenario-goceph-linux
```

Go CI의 6개 fixture profile과 각 case job은 `shell: bash`의 pipefail로
`make` 실패를 유지하면서 독립 로그를 저장합니다. Case별 artifact 이름에도
선택한 case를 포함해 같은 profile의 결과가 덮이지 않게 합니다. 실패한
job만 [reporter](../.github/scripts/report_test_failures.py)를 실행하며,
완료된 Go 실패 test/subtest 이름만 annotation으로 노출합니다. Reporter에만
`continue-on-error`를 적용하므로 reporter 오류가 원래 테스트 결과를 덮지
않습니다. Consumer image 준비 실패·timeout 등으로 완료된 testcase가 없으면
원인을 추정하지 않고 미확인 notice를 남깁니다.

Runtime job은 [cleanup action](../.github/actions/runtime-cleanup/action.yml)으로 테스트 전에 `org.testcontainers=true`인 container/network ID와 volume 이름을 기록하고, 테스트 뒤 성공·실패에 관계없이 새로 남은 소유 리소스를 조회합니다. Schema 2 baseline이 필요하며 schema 1이나 volume 목록이 없는 baseline은 Docker 조회 전에 거부합니다. Ryuk의 정상 종료를 최대 30초 기다린 뒤에도 새 소유 리소스가 남으면 job이 실패합니다. 기존 리소스는 baseline으로 보존하며 검사기는 삭제·stop·prune를 수행하지 않습니다. Docker 조회 오류나 엔진 변경도 빈 목록의 성공으로 처리하지 않습니다. 현재 `runtime-cleanup-scenario-*` artifact에 실행 source와 전후 identity·잔존 결과를 보관합니다. Test PASS만으로 이 별도 정리 검사의 성공을 대신하지 않습니다.

## 이미지 호환성 matrix

이 조합 목록은 수동 Go 호환성 검사 입력과 과거 자동 matrix 결과입니다.
현재 [workflow](../.github/workflows/test.yml)는 `image-compatibility` job을
자동 실행하지 않고 공식 roles의 전체 필수 시나리오를 검사합니다. 이전 자동
matrix는 세 계열 × `all`/`roles` × Linux AMD64/ARM64의 12개 조합에서
같은 대표 9개를 반복했습니다. 다른 조합은 아래 수동 target에서 native
Docker 엔진을 사용하며 에뮬레이션 결과로 대신하지 않습니다.

| 계열 | `all` 방식 | `roles` 방식 | platform별 선택 수 |
|---|---|---|---:|
| `official` | `ceph.DefaultImage`의 고정 Quay digest | GHCR `official-20.2.4-{control,osd,rgw,mds}` | 9개씩 |
| `debian` | GHCR `debian-20.2.4-all` | GHCR `debian-20.2.4-{control,osd,rgw,mds}` | 9개씩 |
| `ubuntu` | GHCR `ubuntu-20.2.4-all` | GHCR `ubuntu-20.2.4-{control,osd,rgw,mds}` | 9개씩 |

GHCR repository는 `ghcr.io/jsyoo5b/ceph-testcontainers-images`입니다. `roles`는 control/OSD/RGW/MDS를 각 역할에 지정하며 mirror는 같은 control을 사용합니다. 현재 역할 이미지 준비와 로컬 matrix runner는 준비된 이미지를 선택하고 이미지를 빌드·패키징·배포하지 않습니다. 실제 image ID·digest·platform에 연결된 실행 결과를 확인해야 하며 tag 이름이나 registry manifest 존재만으로 PASS를 표시하지 않습니다. 2026-10-05 source `be58018`의 [CI run 37226924156](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37226924156)에서 12개 조합 전체가 PASS한 기존 기록을 보존합니다.

후속 source `d9115f4`의 [CI run 37240162309](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37240162309)도 matrix 12개 각각 대표 9개 RUN/PASS·child FAIL/SKIP 0·package `ok`와 image ID·digest·native platform을 확인했습니다. Mirror 전용 role 선택을 제거하고 네 component 이미지와 source control mirror를 사용하는 경로입니다. 각 matrix cleanup 12개도 전후 동일 engine·source와 새 container/network 0개로 PASS했습니다. 같은 source manifest와 개별 artifact를 대조한 [최신 matrix 실행 증거](IMAGE_COMPATIBILITY.md#d9115f4-matrix-검증)에 기록합니다. 같은 run의 상세 101개·전체 cleanup 22개 성공은 [전체 CI 완료 증거](#d9115f4-전체-ci-완료)로 별도 확인합니다.

로컬에서는 현재 Docker 엔진의 native platform에서 한 조합을 실행합니다.

```sh
make image-matrix IMAGE_VARIANT=ubuntu IMAGE_LAYOUT=all
make image-matrix IMAGE_VARIANT=debian IMAGE_LAYOUT=roles
# On a native ARM64 Docker engine:
make image-matrix IMAGE_VARIANT=official IMAGE_LAYOUT=roles \
  IMAGE_PLATFORM=linux/arm64
```

각 조합은 cluster/MGR lifecycle, RBD, CephFS, signed RGW-S3, RBD backup와 RBD/CephFS snapshot mirroring, RGW multisite의 **대표 9개 Go 이름**을 실행합니다. 이 이름들은 기존 상세 suite와 겹칩니다. 12 × 9회 선택을 108개 새로운 distinct test로 더하거나, 이미지 프로젝트의 독립 Python full 11개·기존 Quay 상세 101개·당시 필수 상세 119개와 하나의 성공 증거로 합치지 않습니다. Cryptsetup·암호화·striper는 control/all의 필수 runtime 계약이고 hello·lock class는 osd/all 계약입니다. Linux go-ceph 프로그램·개발 헤더와 Vault 같은 외부 backend는 별도 조건으로 유지합니다. 정확한 실행 계약은 [IMAGE_COMPATIBILITY.md](IMAGE_COMPATIBILITY.md#공식debianubuntu-이미지-matrix)를 따릅니다.

## 추가되는 named test 전체

현재 긴 CephFS 제거·재등록 복구 시나리오는 별도 필수 profile에서 실행합니다. 각 parent의 bridge/host child 전체를 유지하며, 선택한 기존 이미지·각 runner 내부 순차 실행·자체 cleanup 조건을 따릅니다. 이 profile은 호스트 `test` 성공 뒤 독립 runner에서 실행합니다.

| 현재 필수 profile | 이름 수 | 로컬 aggregate Go timeout | CI job timeout | cleanup artifact |
| --- | ---: | --- | --- | --- |
| `scenario-multicluster-topology` | 20 | 90분 | group별 70분 | `runtime-cleanup-multicluster-<group>` |
| `scenario-cephfs-removal` | 5 distinct parent · 8 CI job | 90분 | case별 70분 | `runtime-cleanup-cephfs-removal-<case>` |
| `scenario-topology-extensions` | 12 + SDK 2 | 90분 | case별 50분 | `runtime-cleanup-scenario-topology-extensions-<case>` |
| `scenario-rbd-receivers` | 1 distinct parent · bridge/host job별 5 scope | 90분 | 각 100분 | `runtime-cleanup-scenario-rbd-receivers-<case>` |
| `scenario-mirror-initial-daemons` | 1 | 150분 | 160분 | `runtime-cleanup-scenario-mirror-initial-daemons` |
| `scenario-rbd-namespaces` | 1 | 90분 | 100분 | `runtime-cleanup-scenario-rbd-namespaces` |
| `scenario-rbd-namespace-observation` | 1 | 90분 | 100분 | `runtime-cleanup-scenario-rbd-namespace-observation` |
| `scenario-storage-bootstrap` | 1 | 80분 | 90분 | `runtime-cleanup-scenario-storage-bootstrap` |
| `scenario-manager-bootstrap` | 1 | 80분 | 90분 | `runtime-cleanup-scenario-manager-bootstrap` |
| `scenario-mds-bootstrap` | 1 | 50분 | 60분 | `runtime-cleanup-scenario-mds-bootstrap` |
| `scenario-mds-replacement` | 1 | 50분 | 60분 | `runtime-cleanup-scenario-mds-replacement` |

Source `d9115f4`의 multicluster Make step은 68분 37초, 전체 job은 69분 1초였습니다. 추가된 긴 제거 관측·복구 parent의 시간 예산을 분리합니다. 이 분리와 선택 목록은 새 전체 CI 성공 증거가 아닙니다.

등록 intent의 focused parent는 803.78초 PASS입니다. 기존 네 parent의 별도 focused 시간 합계 3436.52초와 합하면 약 70분 40초입니다. 서로 다른 실행의 합계이며 새 합동 CI의 완료 시간이나 PASS를 보장하지 않습니다. [등록 검증과 계약](CEPHFS_DIRECTORY_ADDITION.md)을 따릅니다.

### `scenario-cephfs-removal` · 5 parent, 후보 8 CI job

```text
TestMultiClusterCephFSPeerRemovalDrain
TestMultiClusterCephFSDirectoryRemovalRelease
TestMultiClusterCephFSOriginalProcessQuiescence
TestMultiClusterCephFSOriginalProcessQuiescenceRecovery
TestMultiClusterCephFSDirectoryAdditionIntent
```

Original-process-quiescence의 `process-quiescence-bridge-peer`,
`process-quiescence-bridge-directory`, `process-quiescence-host-peer`,
`process-quiescence-host-directory`를 별도 runner에서 실행합니다. 각 selector는
`^TestMultiClusterCephFSOriginalProcessQuiescence$/^<network>$/^<kind>$`로
세 component를 모두 anchor합니다. 나머지 peer-drain·directory-release·
process-recovery·directory-intent parent는 기존 job 단위를 유지합니다.

각 leaf는 이미 별도 클러스터 쌍·mirror daemon·receipt를 만들므로 기존 네
pair의 bootstrap 횟수는 그대로입니다. 같은 leaf 안의 pre-Begin restart와
`exited → later-run → container-removed` 이력, 원래 engine/full CID/
StartedAt/GID·FSID·filesystem/pool/peer identity, pending gate와 native 호출
검사, 원래 bytes 및 단계마다 10초 이상 destination 부재 확인은 모두 유지합니다.
Native Go 본문·15분 operation context·내부 wait/cleanup cap은 변경하지 않았습니다.
CI Go 60분/job 70분과 로컬 aggregate Go 90분도 유지합니다.

각 shard는 고유 이미지 준비·cleanup·test.log·coverage.json artifact를 남깁니다.
`check_scenario_quiescence.py`는 선택된 parent/network/leaf의 정확한 RUN/PASS와
Go package 완료, 8개 positive byte/hash·3개 timed absence·3개 동일 original
process identity 기록을 검사합니다. 실제 Go verbose PASS는 parent부터
표시되므로 보존한 native producer의 순서를 기준으로 확인합니다. 네 leaf의
전체 union을 compiled coverage에서 검사하며, 이 parent 4회와 기존 receiver
2회만 정확히 중복 실행을 허용합니다. 빠진 leaf를 parent PASS로 대신하지 않습니다.

이 로그 기록만으로 full native 상태를 독자적으로 재구성하는 것은 아닙니다.
원래 Go assertion과 그 파일의 유지가 native FSID/pool/peer·raw Docker/watcher·
pending gate 판정을 담당합니다. Docker 없는 긍정 대조는 source `3ac07fe`
run 37766891332의 실제 출력에서 원문 순서와 byte를 보존해 추린 fixture이며,
새 shard의 실행 성공이나 시간을 뜻하지 않습니다. 이전 그 parent는 1198.47초,
네 leaf는 305.15/297.98/308.27/287.06초, 이미지 준비는 22.709초였습니다.
이전 job의 시작 대기도 1506초였으므로 이를 새 CI 시간 단축 보장으로 사용하지 않습니다.

기존 로컬 전체 및 parent aggregate는 그대로 실행할 수 있습니다.

```sh
make scenario-cephfs-removal
make scenario-cephfs-removal SCENARIO_CEPHFS_REMOVAL_CASE=process-quiescence
make scenario-cephfs-removal SCENARIO_CEPHFS_REMOVAL_CASE=process-quiescence-bridge-peer
```

### `scenario-rbd-receivers` · 1 parent, 2 CI job

```text
TestMultiClusterRBDReceiverReadiness
```

Image 없는 receiver의 default/named namespace 매핑 네 종류와 named pool journal 모드를 검증합니다. Leader·membership readiness, explicit snapshot checkpoint, 실제 destination bytes를 별도로 확인합니다. CI는 `SCENARIO_RBD_RECEIVERS_CASE=bridge|host`로 서로 독립적인 두 runner에서 실행합니다. 네트워크별 `scope-0`부터 `scope-4`까지는 같은 원래 클러스터 쌍을 공유하며 원래 phase history·election·CID/UUID·bytes·negative window를 유지합니다. 추가 클러스터 bootstrap은 없습니다. 로컬 기본값과 `CASE=all`은 기존 bridge/host 순차 실행을 유지합니다.

전용 Go 90분·job별 100분을 유지하며 호스트 `test` 성공 뒤 실행합니다. Job별 log/prep/cleanup artifact는 case 이름을 포함합니다. [실행 검사기](../.github/scripts/check_scenario_receivers.py)는 선택한 parent·network·5 scope가 정확히 한 번씩 RUN/PASS했는지 확인하며 다른 network·누락·중복·FAIL·SKIP·비어 있는 filtered 실행을 거부합니다. 같은 parent를 두 job에서 선택하므로 distinct 이름 수와 실행 instance 수를 구분합니다.

첫 로컬 Linux ARM64 묶음 실행은 package 1705.518초로 PASS했습니다. 이는 분리한 새 job의 실행 시간이나 성공 증거가 아닙니다. 항상 실행하는 자체 cleanup 검사를 유지하며 [RBD receiver 계약](RBD_RECEIVER_READINESS.md)을 따릅니다.

### `scenario-mirror-initial-daemons` · 1개

```text
TestMultiClusterNoInitialMirrorDaemons
```

Bridge/host 각각 RBD 정상 첫 추가·journal partial 첫 추가·CephFS 정상 첫 추가의 fresh pair를 순차 실행합니다. 최초 zero 설정·명시적 Add·실제 데이터와 resource oracle을 검사합니다. 전용 Go 150분·job 160분은 여섯 child의 caller budget 및 cleanup을 고려한 예산입니다. 첫 로컬 Linux ARM64 실행은 package 931.698초로 PASS했으며 이 측정은 다른 runner의 시간 보장이 아닙니다. 호스트 `test` 성공 뒤 별도 runner에서 자체 baseline·always cleanup을 유지합니다. [구성 계약](NO_INITIAL_MIRROR_DAEMONS.md)을 따릅니다.

다음 목록은 profile별로 고정합니다. Test 내부에 bridge/host child가 있는 경우 모두 유지합니다. Host 전용 wrapper와 helper 이름도 그대로 포함하며, `-list` 또는 tag compile은 runtime 통과 증거로 사용하지 않습니다.

### `scenario-rbd-namespaces` · 1개

```text
TestMultiClusterRBDNamespaceBinding
```

한 pair·한 pool·한 owner가 두 snapshot mapping과 한 journal mapping을 공유합니다. Bridge/host에서 최초 daemon 없는 Bind의 리소스 수, 각 scope의 독립 native 정책·election·image identity·replica bytes, 공유 Stop/Start·제거·교체를 검사합니다. 전용 Go 90분·job 100분과 자체 baseline·always cleanup을 사용합니다. 전체 체크와 실제 selector 목록을 통과했고, 원본 Quay Linux ARM64 bridge/host focused native 실행은 package 696.407초로 통과했습니다. 현재 전체 CI나 다른 이미지 계열의 새 전체 PASS로 합산하지 않습니다. [읽기 전용 namespace view 계약](RBD_NAMESPACE_BINDING.md)을 따릅니다.

### `scenario-rbd-namespace-observation` · 1개

```text
TestMultiClusterRBDNamespaceImageObservation
```

같은 shared pool·owner·cohort의 두 image-snapshot mapping과 한 pool-journal mapping을 retained view의 `ImageStatus`·`WaitReplayReady`로 관측합니다. Bridge/host 각각 두 OSD·pool replicas2/min1로 strict HEALTH_OK를 유지하고, zero-daemon source 관측·명시적 Add·원래 same-name image IDs·Stop/Start·Remove/Add replacement·live attribution과 실제 nonce bytes·source checkpoint ID를 구분합니다. Receiver election 준비는 별도 API의 계약이며 image replay 관측에 추가 조건으로 만들지 않습니다.

`integration,multicluster`의 정확한 한 parent를 `-count=1 -failfast`로 선택하며 Go90분·job100분, 호스트 `test` dependency, 기존 네 역할 override 해제, 자체 baseline·always cleanup과 별도 artifact를 사용합니다. Network별30분 context가 bootstrap/client 생성부터 operation까지 덮고 cleanup은 별도 bounded Background context를 유지합니다. 실패한 child는 다음 network를 시작하지 않습니다. 첫 native35557은 size1/one-OSD의 POOL_NO_REDUNDANCY로 package156.187초 FAIL했고 host는 시작하지 않았으며 자체 cleanup을 확인했습니다. 두 OSD·size2로 바꾼 Final3는 원본 Quay Linux ARM64 bridge/host에서 실제3개 RUN/PASS·package886.778초(parent886.38초,bridge450.08초/host436.31초), FAIL/SKIP0개로 종료했습니다. 18 READY·18 BYTES·12 source CHECKPOINT·18 PENDING·8 rawHEALTH_OK/checks{}·8 MODULES·2 ownerCLEANUP·2 COMPLETE를 확인했고 별도 outer cleanup의 새 container/network는0개였습니다. 같은253개 source input과 고정 policy를 유지했으며 [실제 종료·marker 요약](../artifacts/rbd-namespace-image-observation-20261007/native-summary.json)과 [원문·체크 기록](../artifacts/rbd-namespace-image-observation-20261007/checks-provenance.json), [별도 cleanup](../artifacts/rbd-namespace-image-observation-20261007/runtime-cleanup-final3/after.json)을 보관합니다. 경고를 성공으로 허용하거나 image/module 요구사항을 완화하지 않았습니다. 이 measured focused 결과는116개 전체 CI나 다른 image/platform 조합의 새 성공을 뜻하지 않습니다. [관측 계약·보존된 실패와 검증 상태](RBD_NAMESPACE_IMAGE_OBSERVATION.md)를 따릅니다.

### `scenario-storage-bootstrap` · 1개

```text
TestNoInitialOSDTopology
```

`ceph.WithNoInitialOSDs()`로 MON/MGR만 먼저 구성한 뒤 명시적 OSD 추가·pool 생성·native client I/O를 검사합니다. Bridge/host 각각 정상 첫 추가와 등록 후 partial 첫 추가의 fresh fixture를 순차 실행합니다. 기본 OSD 수와 마지막 owned OSD 제거 보호를 바꾸지 않습니다. `integration,topology`의 정확한 한 parent만 선택하고 Go 80분·job 90분, 호스트 `test` dependency, 기존 역할 override 해제, 자체 baseline·always cleanup을 유지합니다. 새 profile의 `-failfast`와 parent의 failed-leaf 중단은 실패 뒤 후속 fixture를 시작하지 않도록 합니다. 실제 실패를 skip이나 PASS로 변환하지 않습니다.

storage bootstrap 추가 시점의 실제 compiled 선택 114개는 공유 namespace 관측 시점의 113개에 이 parent 한 개만 더한 목록이며, `d9115f4`의 전체 CI 101개 성공과 별도입니다. 첫 native 실행의 FAIL과 별도 config 진단을 보존했고, 실제 MON admin socket을 검증하는 수정 시나리오는 원본 Quay Linux ARM64 bridge/host에서 7개 RUN/PASS·89.354초·자체 cleanup의 새 리소스 0개를 확인했습니다. 기존 양수 OSD·pool·CephFS 초기 구성도 별도 실행과 cleanup을 통과했습니다. 고정 이미지 요구사항·역할 payload는 완화하지 않고, 확인된 이미지 요구사항 충돌이 있으면 후속 작업을 즉시 중지하고 증거를 기록합니다. [구성·검증 계약](NO_INITIAL_OSDS.md)을 따릅니다.

### `scenario-manager-bootstrap` · 1개

```text
TestNoInitialManagerTopology
```

`ceph.WithNoInitialManagers()`로 최초 MGR identity/process 없이 MON/OSD부터 구성합니다. Bridge/host 각각 초기 pool·양수 OSD의 관리 이전 data path와 `WithNoInitialOSDs()`를 함께 쓰는 MON-only 조합을 독립 fixture에서 순차 검사합니다. 원래 FSID·pool·OSD identity, 명시적 첫 `AddManager`, native active GID·module command·clean/health closure와 실제 client bytes를 구분합니다. 기본 MGR 1개·양수 count validation·마지막 MGR 제거 보호는 유지합니다.

`integration,topology`의 정확한 한 parent, Go 80분·job 90분, 호스트 `test` dependency, 기존 네 역할 override 해제, 자체 baseline·always cleanup과 별도 artifact를 사용합니다. Operational context는 network별 15분 cold-positive·10분 MON-only이며 별도 cleanup이 있습니다. 새 profile의 `-failfast`와 parent의 failed-leaf 중단은 후속 fixture를 시작하지 않도록 합니다. 이 시간 예산은 runtime PASS 약속이 아닙니다. 실제 compiled 필수 선택은 M의 114개에 이 parent만 추가한 115개이며 기본 14개와 SDK 전체 11개 중 선택 2개는 유지했습니다. 원본 Quay Linux ARM64 bridge/host의 독립 네 fixture는 7개 RUN/PASS·package 141.986초·자체 cleanup의 새 container/network 0개를 확인했습니다. 같은 250개 source input과 고정 image/policy를 유지했으며, [원문·종료 기록](../artifacts/no-initial-managers-20261007/checks-provenance.json)과 [실제 selector](../artifacts/no-initial-managers-20261007/compiled-selection.json)를 validating checkout에 보관합니다. 현재 source의 기존 storage bootstrap도 별도 원본 Quay bridge/host 실행에서 7개 RUN/PASS·89.109초·자체 cleanup의 새 리소스 0개를 확인했으며, [자체 종료 증거](../artifacts/no-initial-managers-20261007/storage-bootstrap-regression/provenance.json)를 따로 기록합니다. 이 focused 결과를 전체 CI 115개나 다른 image/platform 조합의 성공으로 합산하지 않습니다. 고정 이미지 요구사항은 그대로 유지하고, 증명된 이미지 충돌은 후속 작업을 즉시 중지하여 기록합니다. [구성 계약과 실행 범위](NO_INITIAL_MANAGERS.md)를 따릅니다.

### scenario-cluster-fixtures · 현재 9개

```text
TestClientIdentities
TestCephFSSubvolumes
TestConfigurationOverrides
TestOSDPolicies
TestOSDRemovalLifecycle
TestCephFSSubvolumeSnapshotsAndClones
TestRGWPlacementStorageClasses
TestHostNetworkRGWPlacementStorageClasses
TestRGWPlacementRealmStorageClasses
```

2026-10-07에 `TestOSDRemovalLifecycle`을 추가했습니다. 해당 시점의 fixture selector는 총 48개이며 기본·토폴로지·SDK 58개와 합해 106개였습니다. `scenario-topology`에는 `TestMonitorRollingReplacement`도 추가했습니다. 위 표의 8/47개 및 전체 101개는 해당 source의 역사적 CI 결과로 유지합니다. 새 이름은 원본 Quay Linux ARM64 bridge/host focused 실행과 별도 cleanup 검사를 통과했으며 [OSD lifecycle 증거](TOPOLOGY_EXTENSIONS.md#osd-삭제의-소유권과-재시도)를 따릅니다. `scenario-multicluster-topology`에는 `TestMultiClusterMonitorBootstrapRefresh`도 추가했습니다. [양쪽 MON 교체 후 bootstrap 검증](MON_BOOTSTRAP_REFRESH.md)을 따르며 storage bootstrap 시점 selector 114개의 전체 CI를 새로 통과했다고 표시하지 않습니다. `TestMultiClusterTopologySnapshotsHonorBusyOwners`도 필수 multicluster selector에 추가했으며 [constructor와 snapshot context 계약](TOPOLOGY_CONTEXT.md)을 따릅니다. `TestMultiClusterCephFSPeerRemovalDrain`도 같은 selector에 추가했으며 [bridge/host의 public 제거 handle 검증](CEPHFS_PEER_REMOVAL.md)을 따릅니다. `TestMultiClusterCephFSDirectoryRemovalRelease`도 같은 selector에 추가했으며 [원래 두 daemon의 directory cycle 해제·재등록 검증](CEPHFS_DIRECTORY_REMOVAL.md)을 따릅니다. `TestMultiClusterCephFSOriginalProcessQuiescence`도 추가했으며 [원래 process 관측 계약](CEPHFS_PROCESS_QUIESCENCE.md)을 따릅니다. Peer drain·directory release·원래 process 관측은 이제 별도 `scenario-cephfs-removal`로 이동하고, `TestMultiClusterCephFSOriginalProcessQuiescenceRecovery`를 같은 profile에 추가했습니다. [명시적 복구 승인 계약](CEPHFS_PROCESS_ACKNOWLEDGMENT.md)을 따릅니다. `TestMultiClusterCephFSDirectoryAdditionIntent`도 같은 profile에 추가했으며 [응답 유실을 보존하는 등록 계약](CEPHFS_DIRECTORY_ADDITION.md)을 따릅니다. `TestMultiClusterRBDReceiverReadiness`는 전용 `scenario-rbd-receivers`에 추가했습니다. `TestMultiClusterNoInitialMirrorDaemons`는 별도 `scenario-mirror-initial-daemons`에 추가했습니다. `TestMultiClusterRBDNamespaceBinding`은 별도 `scenario-rbd-namespaces`에 추가했습니다. `TestNoInitialOSDTopology`은 별도 `scenario-storage-bootstrap`에 추가했습니다. 공유 namespace 관측 시점의 실제 선택 113개와 storage bootstrap 시점의 114개 inventory는 각각의 source 선택 목록이며 과거 전체 CI 101개와 별도로 기록합니다. `TestNoInitialManagerTopology`은 별도 `scenario-manager-bootstrap`에 추가했으며 실제 compiled 선택 115개와 원본 Quay bridge/host focused 7개 RUN/PASS를 확인했습니다. 기존 114개 inventory와 과거 전체 CI 101개를 이 새 focused runtime의 성공으로 바꾸지 않습니다.

### scenario-cephfs-fixtures · 8개

```text
TestCephFSDynamicDataPools
TestCephFSCloneCancellationAndPartialCleanup
TestCephFSQuiesceCheckpoints
TestCephFSSubvolumeClientAuthorization
TestCephFSPins
TestCephFSRetainedSnapshotAndMetadataRecipe
TestCephFSAdditionalErasureCodedDataPool
TestHostNetworkCephFSFilesystem
```

아래 `SCENARIO_CEPHFS_FIXTURE_CASE`는 개별 parent를 선택하는 수동 case입니다.
CI는 `data-pools`·`ec-data-pool`·`host-filesystem`을 `data-layout` case에서
함께 선택하고 나머지는 개별 case로 실행합니다. 기본 `all`은 로컬
aggregate 선택 8개와 120분 제한을 유지하며,
각 parent 안의 bridge/host·client·phase는 분리하거나 생략하지 않습니다.

| case | 선택한 parent |
| --- | --- |
| `data-pools` | `TestCephFSDynamicDataPools` |
| `clone-cancellation` | `TestCephFSCloneCancellationAndPartialCleanup` |
| `quiesce` | `TestCephFSQuiesceCheckpoints` |
| `authorization` | `TestCephFSSubvolumeClientAuthorization` |
| `pins` | `TestCephFSPins` |
| `retained-snapshot` | `TestCephFSRetainedSnapshotAndMetadataRecipe` |
| `ec-data-pool` | `TestCephFSAdditionalErasureCodedDataPool` |
| `host-filesystem` | `TestHostNetworkCephFSFilesystem` |

### scenario-topology-extensions · 12개와 SDK 2개

아래 `SCENARIO_TOPOLOGY_EXTENSION_CASE`는 개별 parent를 선택하는 수동
case입니다. CI는 `network-interruption`·`five-monitors`를 `network-recovery`,
`rbd-mirror-bridge`·`rbd-mirror-host`·`rbd-peer-network`를 `rbd-daemons`로
묶으며 나머지는 개별 case로 실행합니다. 기존 Docker bridge SDK runtime
2개는 CI의 `network-recovery`에서 한 번 실행합니다. 기본 `all`은 기존
로컬 aggregate 선택과 Go 90분 제한을 유지합니다.

| case | 선택한 parent |
| --- | --- |
| `network-interruption` | `TestSeparateClusterNetworksAndInterruptions` |
| `five-monitors` | `TestFiveMonitorQuorumAndNetworkRecovery` |
| `rbd-mirror-bridge` | `TestMultiClusterRBDMirrorDaemonTopology` |
| `rbd-mirror-host` | `TestHostNetworkRBDMirrorDaemonTopology` |
| `cephfs-mirror-bridge` | `TestMultiClusterCephFSMirrorDaemonRebalanceTopology` |
| `cephfs-mirror-host` | `TestHostNetworkCephFSMirrorDaemonRebalanceTopology` |
| `rgw-initial-bridge` | `TestMultiClusterRGWInitialZonegroupsTopology` |
| `rgw-initial-host` | `TestHostNetworkRGWInitialZonegroupsTopology` |
| `rgw-removal-bridge` | `TestMultiClusterRGWZonegroupsAndRemovalTopology` |
| `rgw-removal-host` | `TestHostNetworkRGWZonegroupsAndRemovalTopology` |
| `rbd-peer-network` | `TestMultiClusterRBDPeerNetworkInterruption` |
| `rgw-peer-network` | `TestMultiClusterRGWPeerNetworkTopology` |

### scenario-multicluster-topology · 20개

CI의 `SCENARIO_MULTICLUSTER_GROUP`은 `infra` 4개, `rbd` 6개,
`cephfs` 4개와 아래 RGW parent별 group을 선택합니다. 로컬 기본 `all`의
20개와 `rgw` aggregate의 6개는 그대로 선택할 수 있습니다. 아래 group의
Go 60분/job 70분 제한은 기존 multicluster CI 예산을 유지합니다.

| group | 선택한 parent |
| --- | --- |
| `rgw-endpoints-host` | `TestHostNetworkRGWEndpoints` |
| `rgw-multisite-bridge` | `TestMultiClusterRGWMultisite` |
| `rgw-multisite-host` | `TestHostNetworkRGWMultisite` |
| `rgw-three-zone-bridge` | `TestMultiClusterRGWThreeZoneTopology` |
| `rgw-three-zone-host` | `TestHostNetworkRGWThreeZoneTopology` |
| `rgw-master-failover` | `TestMultiClusterRGWMetadataMasterFailover` |

### scenario-rados-fixtures · 4개

```text
TestClientFencing
TestMGRModules
TestRADOSClientFixtures
TestNativePoolReplacement
```

MGR 수정 후 Docker Desktop Linux ARM64의 focused `TestMGRModules`는 bridge/host 모두 PASS했습니다. Native membership/dependency, 실제 RBD schedule/task 완료, optional module의 이전 enabled/disabled 상태 복원, always-on 및 사용 중인 mirror policy 보호를 확인했습니다. Test는 159.29초, harness·cleanup 포함 176.592초이며 새 서버 이미지 빌드 0회와 최종 owned container/network 0개입니다. 단위·race에서도 재시작/불일치 snapshot 이후 변경 1회, 지속 실패·취소 시 변경 0회와 복원 소유권 보존을 검증했습니다. 증거는 `artifacts/quay-mgr-modules-20261004-r2/summary.json`과 `post-runtime-audit.json`에 보관하며, RADOS profile 4개 전체 또는 AMD64 CI 완료로 확대하지 않습니다.

### scenario-rbd-fixtures · 6개

```text
TestRBDClientFeatures
TestRBDAutomaticSnapshotSchedule
TestMultiClusterRBDMirrorScopeAndNamespaces
TestMultiClusterRBDFailback
TestMultiClusterRBDSplitBrainResync
TestHostNetworkRBDLifecycle
```

CI는 `SCENARIO_RBD_FIXTURE_CASE=client-setup`에서 client features와
host lifecycle을 함께 실행하며 나머지 네 parent는 개별 case로 실행합니다.
수동 `client-features`·`host-lifecycle` case와 기본 `all`은 그대로 유지합니다.

`TestRBDClientFeatures`의 layering/flatten, trash, migration commit/abort, group snapshot, exclusive lock, encryption format/load와 rekey 8개 phase를 bridge/host에서 모두 실행합니다. Phase를 일부 제외하여 기본 이미지의 통과를 만들지 않습니다.

2026-10-04 source `efa5ee3`의 로컬 focused 실행은 원본 Quay 서버와 같은 원본 Quay native consumer에서 `TestRBDClientFeatures` 하나를 PASS했습니다. Docker Desktop의 Linux ARM64에서 host-network Go runner로 실행했고, bridge/host 각 8개 native phase 총 16개가 skip 없이 통과했습니다. Test는 225.57초, harness·cleanup 포함 242.319초이며 이미지 빌드 0회, 최종 owned container/network 0개입니다. 증거는 `artifacts/quay-rbd-client-20261004-r1/summary.json`입니다. 이 결과는 RBD 소비자 도구와 해당 recipe의 실제 증거이며, RBD profile 6개 전체나 새 fixture 48개 전체 CI의 통과를 뜻하지 않습니다.

### scenario-rgw-fixtures · 14개

```text
TestRGWUserPlacementPolicy
TestHostNetworkRGWUserPlacementPolicy
TestRGWTenantsAndAccounts
TestHostNetworkRGWTenantsAndAccounts
TestRGWBucketMaintenance
TestRGWS3ClientFeatures
TestRGWNativeTLS
TestRGWProtocolBackends
TestRGWAdminRecordsAndRateLimit
TestHostNetworkHTTPTransportPreservesSignedRequest
TestRGWBackendSTSFormContentTypeIsSigned
TestRGWBackendRoleCleanupRefusesForeignPolicy
TestRGWBackendAuditProofRequiresCompletedVaultTransactions
TestRGWBackendStatusProbeReceivesBoundedContext
```

뒤의 transport/signing/cleanup/audit/context helper 검사는 실제 RGW·Vault client proof와 별도입니다. Helper 성공만으로 backend 동작을 완료 처리하지 않습니다.

### scenario-rgw-sync-fixtures · 7개

```text
TestMultiClusterRGWSelectivePolicy
TestMultiClusterRGWOwnedSyncPolicy
TestHostNetworkMultiClusterRGWOwnedSyncPolicy
TestMultiClusterRGWAccountRootSync
TestHostNetworkMultiClusterRGWAccountRootSync
TestMultiClusterRGWSyncTranslationFiltering
TestHostNetworkMultiClusterRGWSyncTranslationFiltering
```

마지막 두 parent에서는 `tag_owner_class`와 `tenant_system_user_isolation`만 필수 profile로 실행합니다. 독립된 이 두 child의 실제 복제 bytes·제외·checkpoint·cleanup을 확인하는 경로입니다. Parent 이름이 포함됐다는 이유로 모든 translation child를 실행한 것으로 세지 않습니다.

### scenario-goceph-linux · 준비된 소비자 이미지로 선택 실행하는 1개

```text
TestGoCephLinux
```

## 원본 서버와 소비자 도구의 경계

필수 profile은 Ceph source를 컴파일하거나 MON/MGR/OSD/MDS/RGW/mirror 서버 이미지를 생성하지 않습니다. 기본 `SCENARIO_IMAGE_LAYOUT=all`은 control/OSD/RGW/MDS override 네 개를 해제하여 원본 Quay를 직접 사용합니다. 명시적인 `roles`는 준비된 역할 이미지 네 개를 유지하며 현재 주요 CI는 이 경로를 사용합니다. Mirror는 source 클러스터의 control 이미지를 사용합니다. `ceph.Run`은 호출자가 선택한 이미지를 실행하며 이미지 builder를 호출하지 않습니다.

일반 fixture profile은 `CEPH_TEST_RBD_CLIENT_IMAGE`를 해제합니다. RBD native consumer는 선택한 control/all의 Python bindings·librbd 암호화·libcryptsetup·cryptsetup을 사용하며 이 구성요소는 현재 고정 이미지 계약의 필수 조건입니다. 별도 consumer로 필수 역할 검증을 우회하지 않습니다. 일반 client recipe의 선택적 override는 [RBD client 준비 경로](RBD_CLIENT_FIXTURES.md)를 따릅니다. 과거 ARM64 원본 이미지의 사전 도구 조회에서 bindings와 cryptsetup 2.8.6이 확인됐지만, 이 조회를 cluster I/O·새 배포 이미지 payload·새 AMD64 CI 통과로 확대하지 않습니다.

`TestRGWProtocolBackends`는 RGW SSE-KMS 구성을 위해 `testcontainers-go/modules/vault`의 `Run`·`WithToken`으로 기본 `hashicorp/vault:1.21.4`의 실제 KV-v2 backend를 기동합니다. `CEPH_TEST_VAULT_IMAGE`를 지정하면 `scenario-rgw-fixtures`에서도 그 값을 유지하므로 사내 registry의 호환 Vault 이미지나 이미 로컬에 준비한 이미지를 사용할 수 있습니다. Vault는 외부 KMS 테스트 서비스이며 Ceph 역할 이미지 요구사항에 포함하지 않습니다. 허용·거부 audit transaction, STS/Swift 및 암호화 데이터를 확인하며 모의 backend로 성공을 대신하지 않습니다. [Backend recipe](RGW_PROTOCOL_BACKENDS.md)에 조건을 기록합니다.

2026-10-04 로컬 Docker Desktop Linux ARM64 실행에서 원본 Quay RGW의 STS/Swift/SSE-KMS가 bridge/host 각 3개, 총 6개 phase를 skip 없이 PASS했습니다. 두 gateway의 STS trust/action/resource 거부와 기존 session의 정책 복원, Swift key/token·공유 object bytes, 실제 Vault의 allowed/denied audit read와 key 삭제·복원 후 decrypt 결과를 확인했습니다. Test는 136.39초, harness·cleanup 포함 151.158초이며 새 서버 이미지 빌드 0회와 최종 owned container/network 0개입니다. `artifacts/quay-rgw-backends-20261004-r1/summary.json`과 `post-runtime-audit.json`이 증거이며, RGW profile 14개 전체나 AMD64 CI 완료를 의미하지 않습니다.

`scenario-goceph-linux`의 [Linux 실행 harness](../internal/integration/goceph/run.py)는 주어진 client/runner 이미지를 local inspect한 뒤 immutable image ID로 실행합니다. 이미지를 빌드하거나 내려받지 않습니다. 호출자는 같은 Ceph release·ABI의 Linux native 라이브러리와 `go-ceph v0.41.0` probe를 가진 client, 검증할 Go checkout의 integration binary와 native probe를 가진 runner를 준비합니다. Probe의 cgo 빌드 조건은 소비자에게 적용되며 server 역할 이미지에 개발 헤더나 compiler를 요구하지 않습니다. 이미지 프로젝트 CI는 자체 Python 검사기를 실행하고 이 SDK profile을 실행하지 않습니다. 구체적인 입력 계약은 [IMAGE_COMPATIBILITY.md](IMAGE_COMPATIBILITY.md#추가-소비자-조건)를 따릅니다. macOS native go-ceph 빌드의 지원을 뜻하지 않습니다.

새 local go-ceph 준비의 첫 실행은 runtime 이전에 실패했습니다. 소비자 Dockerfile이 module build context의 `internal/dockerbridge`를 복사하지 않아 Go runner를 컴파일할 수 없었습니다. 해당 package의 `COPY`를 추가한 `73cc4ae`의 재실행은 소비자 build와 `TestGoCephLinux`의 bridge/host runtime을 모두 PASS했습니다. 이 수정 후 결과를 `efa5ee3`의 성공 증거로 사용하지 않습니다.

2026-10-04 05:40 UTC의 로컬 실행은 Docker Desktop Linux ARM64에서 각각 두 독립 cluster의 RADOS/RBD/userspace CephFS를 검증했습니다. OSD 2 → 3 → 2 변경 전후 데이터·RBD snapshot 격리와 head 복원·fresh session·owned object/image/file 삭제를 확인한 native proof는 26개이며, 그중 Docker host namespace의 Linux native process 검증은 6개입니다. Test는 241.23초였고, 소비자 probe/runner 두 이미지만 준비했으며 새 Ceph 서버 이미지 빌드는 0회입니다. `summary.json`과 `post-runtime-audit.json`에서 실제 PASS와 최종 owned container/network 0개를 확인했습니다. 증거는 `artifacts/quay-goceph-local-20261004-r2/`에 보관합니다. 전체 Linux AMD64 필수 CI의 완료와는 별도 결과입니다.

회사 패키지나 native 패치를 포함한 이미지는 이 필수 CI와 독립적입니다. 해당 이미지의 제작은 소유자가 맡으며 준비된 결과를 [고정 이미지 요구사항](../../ceph-testcontainers-images/docs/IMAGE_REQUIREMENTS.md)에 따라 확인한 뒤 `Run`과 역할별 image option으로 지정합니다. 과거 `.deb` 입력 builder의 기록은 당시 실험 증거이며 현재 유지되는 제작 도구가 아닙니다. 일반 `integration`·`client-fixtures`의 image override 경로는 유지하고 custom image의 제작이나 통과를 기본 Quay 완료 조건에 넣지 않습니다.

## 지원되는 경로와 strict native 회귀

원본 Ceph 20.2.4의 numeric priority 선택과 ordinary-user source 권한 거부에는 확인된 native 결함이 있습니다. 필수 translation child의 성공을 이 두 기능의 지원으로 확대하지 않습니다. [G05/G07의 원본 한계와 선택적 패치 증거](RGW_SYNC_POLICY.md)를 구분합니다.

`make rgw-sync-native-regressions`는 다음 strict child를 bridge/host 모두 실행합니다.

```text
TestMultiClusterRGWSyncTranslationFiltering/priority_tags_owner_class
TestHostNetworkMultiClusterRGWSyncTranslationFiltering/priority_tags_owner_class
TestMultiClusterRGWSyncTranslationFiltering/ordinary_user_denial_grant
TestHostNetworkMultiClusterRGWSyncTranslationFiltering/ordinary_user_denial_grant
```

이 경로에는 expected-failure 변환이나 권한·데이터 판정 완화가 없습니다. CI에서는 `workflow_dispatch`의 `rgw_native_regressions`를 명시적으로 선택하며, 이미 준비된 patched RGW를 `rgw_image` 입력으로 지정할 수 있습니다. 빈 입력은 원본 Quay입니다. 이 job도 서버 이미지를 빌드하지 않습니다.

선택적 native mirror shuffle 이름 `TestMultiClusterCephFSMirrorDaemonTopology`와 `TestHostNetworkCephFSMirrorDaemonTopology`는 Go CI 101개와 선택적 go-ceph 1개에 포함하지 않습니다. 필수 topology의 daemon rebalance/HA 이름과 구분하며 기존 선택적 실행 경로를 유지합니다.

## 완료 판정과 기존 증거

서버 fixture는 [제공 기준](CLIENT_FIXTURE_COVERAGE.md)의 네 조건을 확인합니다: 실행 가능한 공개 API 조합, native 상태·identity, 실제 client 허용/거부·데이터·장애 복구, 원래 상태 복원 또는 owned cleanup입니다. Sync caught-up만으로 payload나 권한 거부를 대신하지 않습니다. Helper 검사는 각 signer/transport/ownership/deadline 계약을 확인하는 별도 증거입니다.

새 CI 완료는 해당 source·tag·selector와 실제 RUN/PASS 이름, bridge/host child 및 phase, native proof와 cleanup, 필수 job 전체의 terminal SUCCESS를 함께 확인한 뒤 기록합니다. 시작·등록·compile 또는 과거 custom-image PASS는 새 실행 성공이 아닙니다.

기존 `3f79a78cb168bbe99a78fab5450a94f2f322e9d0`의 [Linux AMD64 CI 37173593510](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37173593510)는 2026-10-04 04:42 UTC SUCCESS였습니다. 기존 필수 job 5개에서 기본 14개(11 native/runtime + 3 signer), core 8개, multi 18개, extensions 12개와 별도 SDK 2개를 확인한 기록입니다. **새 fixture 48개는 이 실행의 범위에 없습니다.** 기존 ARM64·slim·Debian 관측과 Docker Desktop 공개 포트의 한계도 [구성별 증거](CLUSTER_SCENARIOS.md)에 그대로 보존합니다.

<details>
<summary>기존 필수 integration 이름 52개</summary>

```text
TestBootstrapFailureCleanup
TestCephFSFilesystem
TestCephFSMDSScaleStandbyReplayTopology
TestCephFSMDSScaleTopology
TestCephFSMultiActiveStandbyFailoverAndFilesystems
TestCephFSStandbyReplayFailover
TestClusterLifecycle
TestErasureCodedPools
TestFiveMonitorQuorumAndNetworkRecovery
TestHostFailureDomainPool
TestHostNetworkCephFSManagerTopology
TestHostNetworkCephFSMirrorDaemonRebalanceTopology
TestHostNetworkCephFSSnapshotMirrorAndBackup
TestHostNetworkMonitorPortConflictRetry
TestHostNetworkMultiCluster
TestHostNetworkRBDMirrorDaemonTopology
TestHostNetworkRBDSnapshotMirror
TestHostNetworkRGWEndpoints
TestHostNetworkRGWInitialZonegroupsTopology
TestHostNetworkRGWMultisite
TestHostNetworkRGWThreeZoneTopology
TestHostNetworkRGWUserAdministration
TestHostNetworkRGWZonegroupsAndRemovalTopology
TestInitialClusterComposition
TestManagerLifecycle
TestMonitorManagerTopology
TestMultiClusterCephFSManagerTopology
TestMultiClusterCephFSMirrorDaemonRebalanceTopology
TestMultiClusterCephFSSnapshotMirrorAndBackup
TestMultiClusterRBDBackup
TestMultiClusterRBDJournalMirrorFailback
TestMultiClusterRBDMirrorDaemonTopology
TestMultiClusterRBDPeerLifecycle
TestMultiClusterRBDPeerNetworkInterruption
TestMultiClusterRBDSnapshotFanout
TestMultiClusterRBDSnapshotMirror
TestMultiClusterRGWInitialZonegroupsTopology
TestMultiClusterRGWMetadataMasterFailover
TestMultiClusterRGWMultisite
TestMultiClusterRGWPeerNetworkTopology
TestMultiClusterRGWThreeZoneTopology
TestMultiClusterRGWZonegroupsAndRemovalTopology
TestPoolPolicies
TestRBDLifecycle
TestRBDNamespaces
TestRGWS3
TestRGWTopology
TestRGWUserAdministration
TestS3FixtureCanonicalURIFollowsAWS4
TestS3FixtureSigningCanonicalizesLiteralAndEncodedTenantSeparator
TestS3FixtureSigningUsesUTCInstantAcrossTimeZones
TestSeparateClusterNetworksAndInterruptions
```

</details>

별도 SDK 이름은 `TestRecoverableBridgeEndpointIdentity`와 `TestRecoverableBridgePublishedPort`입니다. 이 둘의 HTTP/endpoint 복구 proof는 Ceph 서버 기능 개수에 합산하지 않습니다.

### `scenario-mds-bootstrap` · 1개

```text
TestNoInitialMDSTopology
```

`CephFSConfig.NoInitialMDS`로 원래 FS와 metadata/default/additional pool을
생성하고 MDS auth·container·customizer 없이 유지합니다. `WaitReady`와 fresh
libcephfs mount의 non-ready/가용성 관측 뒤 같은 descriptor의 첫
`ScaleMDS(ctx, 1, 0)`으로 기동합니다. 한 fresh cluster의 ready sibling을
독립 identity·live task·nonce control로 유지하며 bridge/host를 순차 검사합니다.

정확한 한 parent와 최소 `integration,topology` 태그, `-mod=readonly`,
`-count=1`, `-failfast`, Go 50분·job 60분, 호스트 `test` dependency,
기존 네 역할 override 해제와 자체 baseline·always cleanup·별도 artifact를
사용합니다. 각 network의 setup을 포함하는 operation context는 15분이며
독립 cleanup과 실패 로그 상한을 둡니다. Pool은 두 OSD·두 replica로 구성하고
cold target absence 외의 warning을 허용하거나 mute하지 않습니다.

O source의 actual 116개에 이 parent만 추가한 required 117개를 실제
compiled 목록에서 확인했습니다. 기본 14개와 SDK 전체 11개 중 선택 2개,
기존 named selector는 유지했습니다. 원본 Quay Linux ARM64의 Final2
focused 실행은 3개 RUN/PASS·package 203.141초로 완료됐습니다. Parent 202.77초,
bridge 102.22초/host 100.55초이며 FAIL/SKIP은 0개입니다. 과거 O116/N115/M114/전체 CI101 성공을 새 117개 전체 runtime
결과로 채택하지 않습니다. [공개 계약과 focused 증거](NO_INITIAL_MDS.md)를
따릅니다.

첫 native session 20463은 package 103.921초 FAIL로 보존합니다. Bridge의
original first-rank·target/sibling I/O·strict final health/module·raw cleanup 및
COMPLETE 관측 뒤 이미 성공한 client 제거를 fallback이 반복해 NoSuchContainer를
오류로 기록했습니다. Host는 시작하지 않았습니다. 별도 outer cleanup은 새
container/network 0개였으며 [실패 분류](../artifacts/no-initial-mds-20261008/native-final1-rejected/classification.json)와
[원문](../artifacts/no-initial-mds-20261008/native-final1-rejected/native.log)을 보존합니다.
Explicit 제거의 실제 성공 receipt만 fallback에서 건너뛰는 native-only
수정이며, 실패/partial cleanup은 유지하고 일반 404 오류를 숨기지 않습니다.
Final2 session 59387은 actual EXIT 0이며, 256개 runtime source의 전후 SHA
`2f4c6974015a64bdd6200ff24c783db68eff55d31cad4a11e60a05951045fb09`와 고정
policy가 유지됐습니다. 두 cold mount는 native errno 110·return_code 0·not killed였고
first Scale 뒤 raw final health는 모두 HEALTH_OK/checks{}였습니다. 10개
128 KiB byte 기록은 독립 nonce dataset 4개와 fresh 검증을 합한 관측입니다.
두 core raw cleanup 외의 별도 outer cleanup도 새 container/network 0개·같은
engine으로 actual EXIT 0입니다. [원문](../artifacts/no-initial-mds-20261008/native.log),
[종료·compile 기록](../artifacts/no-initial-mds-20261008/checks-provenance.json),
[별도 cleanup](../artifacts/no-initial-mds-20261008/runtime-cleanup-final2/after.json)을
확인했습니다. 같은 source의 기존 bridge-only ordinary MDS scale/standby-replay
회귀 session 5038은 별도 2개 RUN/PASS·package 202.702초(ordinary 96.07초,
replay 106.07초)와 자체 cleanup 0개를 확인했습니다. Source 256과 policy는
유지됐으며 [원문](../artifacts/no-initial-mds-20261008/scale-regression.log)과
[자체 cleanup](../artifacts/no-initial-mds-20261008/scale-regression-cleanup/after.json)은
cold first-start와 별도 evidence입니다. 두 focused 실행을 전체 117개 CI,
모든 MDS 조합 또는 native partial-start 복구로 확장하지 않습니다.

### `scenario-mds-replacement` · 1개

```text
TestStoppedMDSRetirementTopology
```

정확한 `integration,topology` parent·`-count=1`·`-failfast`, Go 50분/job 60분,
호스트 `test` dependency, 기존 네 역할 override 해제, 자체 baseline·always
cleanup·별도 artifact를 사용합니다. Setup부터 network별 15분 context와 별도
cleanup/실패 로그 상한을 적용하며 두 OSD·두 replica의 ordinary target/sibling을
구성합니다. [공개 계약](CEPHFS_STOPPED_MDS.md)에 따라 API는 native fail·auth
삭제 없이 stopped original CID만 retire하고 desired capacity를 유지합니다.

P117에 이 parent만 추가한 [실제 required 118개](../artifacts/stopped-mds-retirement-20261008/compiled-selection.json)와
root `make check` session 2950은 actual EXIT 0입니다. Original Quay Linux ARM64
primary 48592는 3개 RUN/PASS·package 207.328초(parent 206.97초, bridge 103.81초,
host 103.16초), 같은 source의 별도 cold regression 58408은 3개 RUN/PASS·201.502초
(parent 200.89초, bridge 102.33초, host 98.56초)로 각각 actual EXIT 0였습니다.
259개 runtime source/policy를 유지했고 각각 자체 outer cleanup의 새 리소스는
0개였습니다. [원문·종료·cleanup과 범위](CEPHFS_STOPPED_MDS.md)를 따르며
기존 selector/P117·과거 focused 결과·전체 CI101을 새 전체 118개 runtime 성공으로
바꾸지 않습니다. Replay/foreign standby 변형과 다른 image/platform도 범위 밖입니다.

`scenario-last-mds-replacement`는 minimal `integration,topology`와 exact
`TestLastMDSReplacementTopology`, `-count=1 -failfast`, Go 50분/job 60분을 사용합니다.
기존 22개 job/selector/check recipe는 유지하고 default dependency, 네 role env 제거,
자체 resource baseline과 always cleanup의 별도 artifact를 따릅니다. 두 OSD·replicas 2/min 1,
ordinary 1 active / 0 standby target과 active sibling의 stopped registration 거부,
caller GID fail·엄격한 failed-rank health·fresh native mount availability·새 indexed
worker·copied retry·Q retire·coexisting nonce bytes를 순차 bridge/host에서 검사합니다.
Actual session 25476은 EXIT 0·package 223.931초(parent 223.62초, bridge 113.12초/host 110.49초)입니다.
24개 128 KiB reader 기록은 독립 nonce dataset 8개와 retained verify를 합한 관측입니다.
두 completed mount는 native errno 110/not killed였고, exact failed-target health 2개·
final/initial strict HEALTH_OK 4개·required module closure 6개와 자체 outer cleanup
0개를 확인했습니다. 262개 runtime source와 고정 image policy를 유지했으며
[actual 종료](../artifacts/last-mds-replacement-20261008/native-terminal.json),
[원문](../artifacts/last-mds-replacement-20261008/native.log),
[자체 cleanup](../artifacts/last-mds-replacement-20261008/runtime-cleanup/after.json)과
[last-MDS 계약](CEPHFS_LAST_MDS_REPLACEMENT.md)에 한정합니다. Runtime registration은 PASS 약속이나 image waiver가 아닙니다.

현재 262개 source의 기존 Q `scenario-mds-replacement` 회귀 71135는 별도
3개 RUN/PASS·package 212.955초(parent 212.59초, bridge 105.00초/host 107.59초),
P `scenario-mds-bootstrap` 회귀 33769는 별도 3개 RUN/PASS·197.593초
(parent 197.31초, bridge 98.97초/host 98.34초)로 실제 EXIT 0였습니다.
각 실행은 fresh own baseline과 별도 cleanup 새 리소스 0개·source/policy 유지로
확인했습니다. [Q 회귀 종료](../artifacts/last-mds-replacement-20261008/stopped-regression-terminal.json)와
[P 회귀 종료](../artifacts/last-mds-replacement-20261008/cold-regression-terminal.json)를
primary와 구분하며 이전 Q source259/P source256의 측정값은 보존합니다.
세 focused 실행을 전체 119개 CI나 다른 platform/image의 성공으로 합산하지 않습니다.
