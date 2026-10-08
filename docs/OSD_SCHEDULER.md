# 테스트 fixture의 OSD scheduler 검토

2026-10-08의 동일 환경 3쌍 반복 실험에서는 WPQ 전환으로 시간을 줄인다는
근거를 얻지 못했다. 현재 기본 scheduler를 유지하며, 모든 시나리오에 WPQ를
강제하거나 기존 timeout·assertion을 바꾸지 않는다.

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

## 필요한 경우 선택하는 방법

기존 API로 최초 OSD 기동 전에 queue를 선택할 수 있다.

1. `ceph.Run(ctx, image, ceph.WithNoInitialOSDs())`로 MON/MGR를 구성한다.
2. `TemporaryConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: "osd_op_queue", Value: "wpq"})`
   로 central config를 설정하고 오류와 partial override를 처리한다.
3. `AddOSD(ctx)`로 원하는 OSD를 추가하고 각 daemon의 실제 queue를 확인한다.

Central config의 readback만으로 local config·argv·runtime override를 이겼다고
판단하지 않는다. 실행 중 OSD에 설정하거나 override를 Restore하는 것만으로
queue가 즉시 바뀌지는 않는다. Cluster 종료는 fixture의 config DB도 폐기한다.
[ConfigSetting 계약](../ceph/config.go)과 Ceph의 restart 조건을 따른다.

## 증거

고정 실행 binary SHA256:
`0f8a4ad7f84701344189a4c5538b2d3f3ee5ef682e6f4b518181e56f4071ae76`.
검증 JSON SHA256:
`b7e2c5a41bab72fac54501af94ca7d66c656971d8f165021024a143d68851f01`.
로컬 `artifacts/osd-scheduler-review-20261008/`에는 harness, 실행별 raw CLI·
daemon 로그·PG·perf·payload·cleanup, 검증기와 원본 보존 receipt가 있다.
초기 config-show readback 지연과 이전 PG 보고를 잘못 최종으로 취급한 준비 실패는
별도로 보존했으며 유효 여섯 실행의 성능 결과에 포함하지 않는다.
