# Pool별 PG 보고 상태

`cluster.PoolPGs(ctx, poolName)`은 control container의 Ceph CLI로 선택한
pool의 PG 보고를 읽는다. Host의 librados, go-ceph, cgo는 필요하지 않다.
OSD 장애·증감·EC 부족 조건에서 assertion에 사용할 실제 native 상태를
제공하며, 새로운 운영 제어나 recovery 완료 판정은 추가하지 않는다.

```go
snapshot, err := cluster.PoolPGs(ctx, "app-data")
if err != nil {
    t.Fatal(err)
}
for _, pg := range snapshot.PGs {
    t.Log(pg.PGID, pg.State, pg.Up, pg.Acting, pg.Stats.Objects)
}
```

## 반환값

`PoolPGSnapshot`은 원래 `FSID`, 조회를 둘러싼 `PoolBefore`·`PoolAfter`
(`PoolState`), `OSDMapEpochBefore`·`OSDMapEpochAfter`, native `PGReady`와
`PGs`를 보존한다. 두 pool 정책은 각각 관측한 값이다. 같은 ID의 정책이나
OSDMap epoch가 달라질 수 있으며 이를 하나의 원자적 snapshot으로 합치지 않는다.
과거에 저장한 pool ID와 비교하려면 호출자가 그 ID를 따로 보존한다.

각 `PGState`는 다음을 제공한다.

- `PGID`, native 문자열 `State`.
- `Up`·`Acting`의 원래 순서와 별도 `UpPrimary`·`ActingPrimary`.
- `ReportedEpoch`·`ReportedSequence`, `MappingEpoch`, `LastEpochClean`.
- `StatsInvalid`와 `PGStats`의 signed 64-bit counters:
  `Objects`, `Bytes`, `ObjectCopies`, `ObjectsDegraded`,
  `ObjectsMisplaced`, `ObjectsUnfound`.

보고된 vector는 현재 OSDMap의 매핑과 다를 수 있다. 삭제된 OSD가 이전
보고에 남거나, EC vector에 `2147483647` (`CRUSH_ITEM_NONE`)이 반복될 수 있다.
배열을 정렬하거나 NONE을 제거하지 않으며 primary를 첫 원소에서 추론하지 않는다.
PG마다 `pg map`을 호출하는 추가 fanout도 수행하지 않는다.

Field의 숫자 폭과 PG ID 표현은
[Ceph v20.2.4의 native 타입](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/osd_types.h#L1478-L1488)을
따른다. 반환 pool 정책의 CRUSH rule은 native unsigned 8-bit 값이다.

## assertion의 의미

`PGReady`는 native envelope의 `pg_ready` 그대로다. `true`가 모든 PG의
보고 도착·active·clean·실제 데이터 가시성을 뜻하지 않는다. `unknown`,
빈 vector, primary `-1`, epoch/sequence `0`, 빈 row 목록도 유효한
관측이다. `StatsInvalid=false`여도 통계가 이전 값을 보고할 수 있다.
PG 수와 pool의 PGNum이 다르다는 이유로 유효한 보고를 거부하지 않는다.

빈 집합에서는 `pg_stats` 키 자체가 생략될 수 있다. 성공한 `pg_ready`
envelope만 있는 응답도 빈 `PGs`로 반환한다. 명시적인 `pg_stats:null`은
거부한다. 이는 [PGMap의 빈 집합 처리](https://github.com/ceph/ceph/blob/v20.2.4/src/mon/PGMap.cc#L3702-L3712)와
[MGR의 envelope 생성](https://github.com/ceph/ceph/blob/v20.2.4/src/mgr/DaemonServer.cc#L3036-L3057)에
따른다.

클라이언트가 방금 쓴 bytes나 특정 snapshot의 존재는 그 클라이언트의 실제
readback으로 확인한다. 특정 fixture의 PG 상태를 기다릴 때에는 호출자
context로 반복 조회하고 해당 fixture가 요구하는 predicate를 명시한다.
`WaitForClean`의 기존 aggregate 준비 계약은 바꾸지 않는다.

## 조회·실패 계약

조회는 다음 다섯 CLI 호출로 한정한다.

1. `ceph --connect-timeout 5 fsid`.
2. `ceph --connect-timeout 5 osd dump --format json`.
3. `ceph --connect-timeout 5 pg ls-by-pool <name> --format json`.
4. 같은 `osd dump`.
5. 같은 `fsid`.

원래 bootstrap FSID와 앞뒤 native FSID, 선택 pool의 정확한 이름과 ID를
검증한다. Pool 교체, 누락·중복 identity, 잘못된 알려진 JSON field,
known-field null·case alias·duplicate·trailing JSON, overflow, context
취소 및 CLI 실패는 error와 zero snapshot을 반환한다. Native 출력과
command error 내용은 반환 error에서 제거한다. 각 JSON은 4 MiB와 nesting
depth 256으로 제한한다. 반환 상태·pool 이름은
그 자체로 사용자가 준비한 환경 정보를 포함할 수 있다.

토폴로지 소유권과 config 접근도 context에 따라 대기한다. 모든 query에서
취소를 확인하며 내부 재시도·background worker는 없다. 유효한 unknown
JSON field는 허용한다. Strict JSON 처리의 Unicode 정규화 제한은
[HealthDetails](HEALTH_DETAILS.md)의 공통 decoder 범위를 따른다.
CLI의 여러 읽기는 원자적이지 않으며 외부 pool/config 변경을 동시에
실행하지 않는다.

## 검증 경로

[단위 테스트](../ceph/pool_pgs_test.go)는 identity·JSON·숫자 범위·context·
error redaction과 실제 native 경계 값의 허용을 검증한다.
[통합 테스트](../internal/integration/pool_pgs_integration_test.go)는 production
decoder를 호출하지 않는 raw CLI oracle로 모든 공개 field를 비교한다.
주변 raw 보고가 일치하는 관측에서만 API 값을 비교하며 API 실패를
통계 지연으로 재시도하지 않는다.

`TestClusterLifecycle`은 원래 pool ID와 retained object bytes를 보존하면서
초기 상태·OSD 증설·원래 OSD 제거·마지막 lifecycle 상태에서 비교한다.
`TestPoolPGReportedBoundaries`는 OSD 없이 만든 replica/EC pool의
unknown/empty 보고, OSD 하나만 추가한 EC incomplete/NONE vector와
replicated pool의 실제 4 KiB readback을 별도로 검증한다. 부족한
placement domain을 허용하기 위해 테스트가 raw CLI로 pool을 준비하며
기존 `CreatePool`의 domain guard는 유지한다.

2026-10-08 Docker Desktop Linux ARM64에서 같은 CGO 없는 binary로 위 두
parent를 공식·Debian·Ubuntu의 고정 role 이미지에 각각 실행했다.

| role 이미지 계열 | 두 parent의 native 시간 | 전체 field 비교 |
| --- | ---: | ---: |
| official | 123.913초 | 8회 |
| Debian | 122.510초 | 9회 |
| Ubuntu | 123.385초 | 9회 |

세 실행 모두 두 parent RUN/PASS, FAIL·SKIP 0, 실제 4 KiB readback과
원래 FSID·pool ID를 확인했다. 원래 client의 lifecycle object bytes도
유지했다. 사용한 이미지 inspect bytes와 runtime source가 같았고,
각 실행의 별도 cleanup에서 새 소유 container/network는 0개였다.
Debian·Ubuntu의 추가 비교 각 1회는 replicated PG의 active 보고를
기다리는 실제 관측이며 별도 fixture나 named test를 늘린 것이 아니다.
중간 raw CLI JSON 파일 자체는 보관하지 않았고, 실행한 독립 oracle과
공개 snapshot JSON·SHA는 로그에 남겼다.

Runtime source manifest는 275개 파일의
`f47cf2c119b39aa86b53c945ea999d02f1316f8da7b7fe30de5d5089c3c0bcb3`,
binary SHA256은
`0d09d84b049e5331f76c0436bb812c78926b5f6d0427b6d2a7d609bb8a3d0255`다.
고정 image ID, source manifest, Go 로그·snapshot과 전후 cleanup은
`artifacts/pool-pgs-20261008/roles-v1/`에 보존한다.

이는 focused ARM64 role 검증이며 새 source의 전체 CI 성공이나
다른 platform/all 이미지의 완료로 합산하지 않는다. Source별 전체
완료 결과는 [CI 기록](CI_FIXTURES.md)에 별도로 추가한다. 이미지 runtime
계약과 역할은 변경하지 않는다.
