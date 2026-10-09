# 테스트 fixture의 OSD scheduler 검토

2026-10-08의 첫 3쌍 반복 실험에서는 기본 설정의 WPQ가 기본 mClock보다 느렸다.
별도 WPQ 튜닝 비교에서는 HDD recovery sleep 두 값을 0으로 설정했을 때
backfill과 recovery 구간이 빨라졌다. 첫 비교로 튜닝한 WPQ까지 불리하다고
판단할 수 없다. 공통 suite의 기본 scheduler는 유지하며 선택한 fixture에서
설정의 효과를 검증한다.

Ceph의 mClock은 client I/O와 recovery 등 작업 종류별 자원을 조절한다.
`osd_op_queue`를 WPQ로 바꾸는 것은 다른 queue와 관련 설정을 선택하는 변경이며
실행 중인 OSD에는 재기동이 필요하다. mClock의 recovery 설정·sleep 처리도
함께 달라지므로 단순 queue 자료구조만 비교하는 실험으로 해석하지 않는다.
[Tentacle mClock 문서](https://docs.ceph.com/en/tentacle/rados/configuration/mclock-config-ref/)와
[실행한 v20.2.4 OSD 설정](https://github.com/ceph/ceph/blob/v20.2.4/src/common/options/osd.yaml.in)을 따른다.

## 실제 비교 조건

실행 라이브러리는 `a388e6d1ffc07e27c8867a9a4f91a47a3d707003`의 고정 복사본이다.
동일한 CGO 없는 binary를 mClock→WPQ, WPQ→mClock, mClock→WPQ 순서로
실행했다. 각 실행은 새 클러스터를 만들고 정리하며, 다른 Docker 작업을 병행하지 않았다.

- Docker Desktop Linux/arm64 엔진, 4 CPU·약 4 GiB RAM, Ceph 20.2.4.
- 캐시된 공식 role 이미지의 control·OSD를 사용했다. 원본 Quay all 이미지의
  별도 실행이나 Debian/Ubuntu 이미지의 scheduler 비교는 아니다.
- control: `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:42d753062656366b0383e402920721c683618b7981e71b42732f5a5e5bfad022`.
- OSD: `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:7bf9001b79e141a66b5fd916b03db7700852cf8613e964c3131627b3c2a8ec84`.
- OSD마다 BlueStore 1 GiB sparse file을 사용하고 native device class는 HDD였다.
- PG 8개, replicas 2·min_size 1·autoscale off, 결정적 512 KiB 객체 256개로
  128 MiB를 기록하고 전체 hash를 검증했다.
- MON/MGR-only bootstrap에서 queue를 설정한 뒤 OSD 2개를 추가했다.
  세 번째 OSD로 실제 데이터를 이동시킨 뒤 데이터가 있는 원래 OSD 0을 제거했다.

## 실행별 중앙값

| 구간 | mClock | WPQ |
| --- | ---: | ---: |
| MON/MGR bootstrap | 3.105초 | 3.097초 |
| 최초 OSD 0 추가 | 3.566초 | 3.555초 |
| 최초 OSD 1 추가 | 3.568초 | 3.561초 |
| 128 MiB bulk write | 0.506초 | 0.761초 |
| 128 MiB bulk read·hash 확인 | 0.224초 | 0.225초 |
| OSD 증설·실제 backfill·clean 확인 | 13.024초 | 28.559초 |
| 원래 OSD 제거·실제 recovery·clean 확인 | 7.373초 | 19.671초 |
| 전체 native 실행 | 59.644초 | 87.883초 |

각 반복쌍에서 WPQ 전체 시간은 mClock의 1.406·1.604·1.469배였다.
전체 시간에는 phase 밖의 CLI·config·identity·perf 조회와 cleanup도 포함되므로
개별 phase 합과 다르다. 짧은 bulk I/O는 공유 page cache를 포함하며 디스크 처리량
벤치마크로 사용하지 않는다.

18개 OSD 모두 direct daemon `tell ... config get osd_op_queue`와
`config show`에서 요청한 값을 확인했다. 여섯 실행에서 원래 FSID·pool ID와
살아남은 OSD UUID/CID, 전체 payload hash를 보존했다. 증설 후 세 번째 OSD가
실제 객체를 가진 PG의 acting에 들어왔고, 제거 전 원래 OSD에도 객체·bytes가 있었다.
제거 후 모든 PG가 `{1,2}`의 active+clean 상태였다. 같은 OSD 2 process의
recovery counter도 각각 97–98 ops·50,855,936 bytes 증가했다. 이는 물리 디스크
bytes 측정이 아니다. 실행별 cleanup 6쌍과 전체 최초/최종 검사에서 새 컨테이너·
네트워크는 0개였으며 이미지 pull/build 없이 Docker 컨테이너 0→0을 확인했다.

Fresh mClock OSD 9/9에서 자동 IOPS benchmark 로그가 있었고 WPQ 0/9에는
없었지만 OSD 준비 시간은 거의 같았다. 파일 기반 HDD 측정은 native 허용 범위를
벗어나 capacity 315 IOPS가 유지됐다. 재기동 실험은 수행하지 않았다.
Recovery sleep 값을 직접 수집하지 않아 관측한 차이의 원인을 sleep으로 단정하지 않는다.
큰 EC 작업·client/recovery 경합·QoS·실제 디스크·다른 플랫폼은 추가 비교가 필요하다.

RGW policy의 의도적인 미복제 확인 시간과 RBD receiver의 election·identity
관측 시간은 별도다. 이번 OSD 결과로 그 대기를 줄일 수 있다고 주장하지 않는다.
독립 fixture를 runner별로 분리하는 [CI 구성](CI_FIXTURES.md)을 계속 사용한다.

## WPQ recovery sleep 튜닝 비교

두 arm 모두 queue는 WPQ다. 동일한 binary와 앞서 고정한 라이브러리·이미지,
128 MiB 데이터·OSD 2→3→2 조건으로 기본→튜닝, 튜닝→기본, 기본→튜닝
세 쌍을 실행했다. 변경한 값은 다음 두 개뿐이다.

| 설정 | 기본 WPQ | 튜닝 WPQ |
| --- | ---: | ---: |
| `osd_recovery_sleep_hdd` | 0.1초 | 0초 |
| `osd_recovery_sleep_degraded_hdd` | 0.1초 | 0초 |

Ceph v20.2.4의 [원본 설정](https://github.com/ceph/ceph/blob/v20.2.4/src/common/options/osd.yaml.in)을
고정하고 모든 실행에서 override 전 기본값을 확인했다. Generic recovery sleep은
0, `osd_recovery_max_active`는 0, HDD active는 3, `osd_max_backfills`는 1을
유지했다. 18개 OSD의 HDD·rotational·1 GiB BlueStore metadata와 원래 5개
identity phase의 총 60개 관측에서 direct daemon·central config의 8개 값을
대조했다. 추가 설정 조회는 backfill·제거의 기존 측정 구간 밖에 있다.

| 구간 중앙값 | 기본 WPQ | sleep=0 WPQ |
| --- | ---: | ---: |
| OSD 증설·backfill·clean 확인 | 26.609초 | 12.592초 |
| 원래 OSD 제거·recovery·clean 확인 | 18.192초 | 8.812초 |
| 전체 native 실행 | 102.806초 | 77.130초 |

세 쌍 모두 두 migration 구간은 튜닝한 쪽이 빨랐다. 전체 시간 비율은
0.741·1.070·0.720으로, 두 번째 쌍에서는 튜닝한 전체 실행이 약 7% 느렸다.
전체 시간에는 설정·identity·PG 보고·perf 조회 및 cleanup도 들어간다.
새 FSID마다 실제 배치가 달랐고 증설 후 OSD 2의 logical object 수는
159–190개였다. 제거 전 원래 OSD의 logical objects는 모든 실행에서 193개였고,
같은 OSD 2 process의 제거 recovery counter는 각각 97 ops·50,855,936 bytes
증가했다. 이 수치는 물리 디스크 throughput 측정이 아니다.

6회 모두 원래 FSID·pool·OSD identity, 실제 데이터 이동과 전체 payload hash를
검증했다. 각 실행의 cleanup이 통과했고 최초·최종 전체 컨테이너 수는 0개,
이미지는 62개였다. 같은 엔진·4 CPU·4,107,141,120 bytes RAM을 확인했다.
배타적 lock은 협조하는 실험을 직렬화하며, 전체 엔진의 연속 isolation·RSS를
측정했다는 주장은 하지 않는다. Native batch는 648.537초에 EXIT0로 끝났다.

이 결과는 작은 HDD file fixture에서 WPQ 튜닝으로 시간이 줄어들 가능성을
실제로 확인한 것이다. 새 비교에는 mClock arm이 없고 설정 조회도 늘었으므로
이전 mClock 전체 시간과 직접 비교하지 않는다. 동시 recovery·backfill 수를
늘리는 튜닝, foreground I/O 경합, EC 및 나머지 CI 시나리오의 효과는 별도로
검증해야 한다. 기존 timeout·assertion과 공통 scheduler는 바꾸지 않았다.

새 binary SHA256은
`f8b39e3ce2ad0f806a0de8115629c989f1422b31b4b388fa011d30af77a1d8ed`다.
원본·실행별 CLI·daemon·payload·cleanup·검증 receipt는
`artifacts/wpq-tuning-review-20261009/`에 보존한다.

## 최장 CI 시나리오에서의 비교

2026-10-09 KST에 확인한 [source 4da2744의 성공 CI](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37804783231)에서
최장 job은 RBD fixtures 2,107초였다. 최장 단일 Go parent는
CephFS original-process recovery 1,455.33초이며 독립된 네 leaf의 합이다.
여기서 recovery는 mirror process·정책·watcher·checkpoint를 복구하는
동작이다. Site마다 OSD 한 개를 유지하며 OSD 증설·제거에 따른 PG migration을
검사하지 않는다. RBD scope도 OSD membership을 유지한다.
[Job·step 분석과 CI 분리](CI_FIXTURES.md)를 따른다.

원래 CephFS `bridge/peer` leaf를 기본 mClock→WPQ sleep=0 순으로
각각 새 pair에서 한 번 실행했다. 앞의 OSD 실험과 같은 엔진·자원,
고정된 official control·OSD 이미지와 기존 MDS 역할 이미지를 사용했다.
MDS digest는 `sha256:76885d63b94ca90bb6988df49f135a5a567b4fb29c2991ce26fdf3ee487206b1`이다.
Library·원래 native 테스트는 source `4da2744`에 고정했다. 동일한 binary와
MON `PostReadies` hook을 사용해 첫 OSD 전에 queue를 설정했다.
원래 OSD 수·pool defaults·assertion·15분 context·Go timeout은 유지했다.
Overlay는 설정 검증·이미지 identity·구간별 clock 기록만 추가했다.

두 arm의 실제 daemon·central 조회에서 HDD/generic recovery sleep 네 값은
모두 0이었다. 기본 mClock은 sleep을 이미 비활성화하며, WPQ arm은 두 HDD
값을 명시적으로 0으로 적용했다. Queue 외의 recovery active 0/HDD 3과
backfills 1은 같았다. 두 site의 OSD는 BlueStore·HDD·rotational·1 GiB였다.
이 비교를 앞의 기본 WPQ 0.1초→0초 실험과 혼동하지 않는다.

| 관측 구간 | 기본 mClock | WPQ sleep=0 |
| --- | ---: | ---: |
| Bootstrap·설정 검증 | 20.554초 | 21.233초 |
| Mirror 원래 process 재시작·baseline | 39.424초 | 39.114초 |
| 제거·outage·필수 negative 증거 | 97.255초 | 95.650초 |
| Acknowledgment·replacement·backlog | 37.929초 | 38.085초 |
| 테스트 본문 합계 | 270.771초 | 268.326초 |
| Fixture cleanup | 30.415초 | 34.397초 |
| Go leaf 전체 | 301.19초 | 302.73초 |

본문은 WPQ가 약 2.45초 짧았지만 전체는 약 1.54초 길었다. 한 쌍의 탐색
관측에서 전체 개선은 없었으며 작은 차이의 원인을 scheduler로 단정하지 않는다.
구간은 CLI·native 수렴·관측 조건을 포함하며 순수 디스크 실행 시간이 아니다.
Linux ARM64 로컬 결과를 Linux AMD64 CI 시간과 직접 비교하지 않는다.
큰 데이터·foreground 경합·여러 OSD의 migration 결과로도 일반화하지 않는다.

각 arm은 원래 native assertion, byte proof 14개, 10초 이상의 absence window
3개, acknowledgment 5개와 replacement identity 검사를 통과했다.
Actual role image·full CID·engine·platform을 client와 두 mirror process까지
대조했고 두 실행 모두 자체 cleanup이 통과했다. 최초·각 실행 후 전체 container는
0개, 이미지 수는 62개였다. 연속 engine isolation이나 RSS를 측정한 것은 아니다.

첫 native 실행은 통과했으나 초기 검증기는 `go test -json`의 postorder PASS를
plain `-v`의 preorder와 같다고 가정해 실패했다. 원본 실패 receipt를 보존하고
별도 JSON 모드로 실제 구조화된 RUN/PASS·package 완료와 전체 native 증거를
다시 검증했다. CI plain 모드와 원래 assertion은 유지했고 첫 native 실행을
재실행하거나 성공 로그로 교체하지 않았다. 마지막 검증기는 두 arm 모두 통과했다.

이 시나리오의 시간을 줄이는 우선 조치는 독립된 fixture의 runner 분리다.
공통 scheduler는 유지한다. 추가 WPQ 튜닝은 실제 OSD out/add/remove를
검증하는 `OSDPolicies`·`OSDRemovalLifecycle` 등에서 구간을 측정해 판단한다.
이번에는 recovery 동시성·backfill 수를 늘리지 않았다.

Binary SHA256은 `d3f467a0603e2bbfe11ddf493a5ebec76672ee77559a184115d3680432d160f4`다.
Duration 원본, 273개 source pin, overlay 복원 검증, 실제 JSON·설정·이미지·
cleanup·실패 및 수정 gate receipt는
`artifacts/slow-scenario-wpq-20261009/`에 보존한다.

## 필요한 경우 선택하는 방법

기존 API로 최초 OSD 기동 전에 queue를 선택할 수 있다.

1. `ceph.Run(ctx, image, ceph.WithNoInitialOSDs())`로 MON/MGR를 구성한다.
2. `TemporaryConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: "osd_op_queue", Value: "wpq"})`
   로 central config를 설정하고 오류와 partial override를 처리한다.
3. `AddOSD(ctx)`로 원하는 OSD를 추가하고 각 daemon의 실제 queue를 확인한다.

위의 sleep 튜닝을 재현하려면 첫 OSD 전에 두 HDD sleep 설정도 각각
`TemporaryConfig`로 0을 적용한다. 각 호출의 오류와 partial override를 처리하고
daemon의 실제 값을 확인한다. Recovery/backfill 동시성은 별도 비교를 위해
기본값을 유지한다.

Central config의 readback만으로 local config·argv·runtime override를 이겼다고
판단하지 않는다. 실행 중 OSD에 설정하거나 override를 Restore하는 것만으로
queue가 즉시 바뀌지는 않는다. Cluster 종료는 fixture의 config DB도 폐기한다.
[ConfigSetting 계약](../internal/cluster/config.go)과 Ceph의 restart 조건을 따른다.

## 증거

고정 실행 binary SHA256:
`0f8a4ad7f84701344189a4c5538b2d3f3ee5ef682e6f4b518181e56f4071ae76`.
검증 JSON SHA256:
`b7e2c5a41bab72fac54501af94ca7d66c656971d8f165021024a143d68851f01`.
로컬 `artifacts/osd-scheduler-review-20261008/`에는 harness, 실행별 raw CLI·
daemon 로그·PG·perf·payload·cleanup, 검증기와 원본 보존 receipt가 있다.
초기 config-show readback 지연과 이전 PG 보고를 잘못 최종으로 취급한 준비 실패는
별도로 보존했으며 유효 여섯 실행의 성능 결과에 포함하지 않는다.
