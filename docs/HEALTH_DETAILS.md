# MON health details Check

`cluster.HealthDetails(ctx)`는 MON이 보고한 health code, 원인과 mute를 읽어
테스트 assertion의 근거를 제공한다. 공개 Go 패키지에 go-ceph, librados나 cgo를
추가하지 않고, 주어진 이미지의 컨테이너 내부 `ceph` CLI를 사용한다.
MGR·OSD·pool이 아직 없는 MON-only 단계에서도 조회할 수 있다.
MON에 연결 가능한 상태와 확인된 원래 bootstrap 설정은 필요하다.

구현은 [health_details.go](../internal/cluster/health_details.go), result와 경계 검사는
[health_details_test.go](../internal/cluster/health_details_test.go)에 있다. 공식 role 이미지의
Linux ARM64 focused native 실행은 통과했다. 새 source 전체 CI와 다른
image/platform의 확인은 별도 범위이며, 과거 raw CLI 결과를 새 API의 성공으로 합산하지 않는다.

## 조회와 원래 identity

성공 경로는 한 번 캡처한 동일 control container에서 다음 세 읽기를 수행한다.

```text
ceph --connect-timeout 5 fsid
ceph --connect-timeout 5 health detail --format json
ceph --connect-timeout 5 fsid
```

첫 번째와 마지막 native FSID는 확인된 bootstrap 설정의 canonical nonzero FSID와
정확히 같아야 한다. 반환 `HealthSnapshot.FSID`도 그 원래 값이다. 조회 도중 control
handle을 다시 선택하지 않는다. 닫힌 fixture, 불완전 bootstrap, 다른 FSID,
native 명령·decode 실패는 `HealthSnapshot{}`과 error를 반환한다.

호출자 context는 topology/control/config 접근 대기와 native 조회를 제한한다.
유효한 응답을 받았더라도 마지막 FSID 확인 후 취소되면 zero snapshot을 반환한다.
취소·deadline의 canonical 원인은 `errors.Is`로 확인할 수 있다. Error에는 native
stdout/stderr, parser 입력과 원래 command error를 넣지 않는다. Error와 함께 받은
snapshot을 성공한 관측으로 사용하지 않는다.

이 세 읽기는 비원자적 snapshot이다. 모듈의 owned topology 변경은 같은 owner
gate로 직렬화하지만, 외부 raw CLI/config writer는 이 조회와 동시에 실행하지
않아야 한다. 조회 전후 FSID 확인으로 health 상태 전체의 원자성까지 보장하지
않는다. API 자체는 native retry, HEALTH_OK 대기나 mutation을 수행하지 않는다.
상태 전이를 assertion하려면 [Go 대기 사용법](GO_WAITS.md)처럼 deadline을 둔
호출자 polling을 사용한다.

```go
health, err := cluster.HealthDetails(ctx)
if err != nil {
    t.Fatal(err)
}
check, present := health.Checks["MGR_DOWN"]
if !present || check.Muted {
    t.Fatal("expected an unmuted MGR_DOWN check")
}
```

위 예는 `MGR_DOWN`을 실제로 준비한 fixture를 관측한다. MGR가 없다는 사실만으로
이 code가 즉시 발생한다고 가정하지 않는다. 새 MON의 grace period와 OSD 유무도
조건에 포함된다. [v20.2.4의 MGR 경고 조건](https://github.com/ceph/ceph/blob/v20.2.4/src/mon/MgrMonitor.cc#L284-L299)을 따른다.
전체 HEALTH_OK, PG clean,
module 준비와 객체·파일 I/O 성공은 각 Check 또는 소비자 클라이언트로 확인한다.

## 반환 값

| 값 | 의미 |
| --- | --- |
| `HealthSnapshot.Status` | `HEALTH_OK`, `HEALTH_WARN`, `HEALTH_ERR` 중 mute되지 않은 check의 가장 심각한 상태 |
| `HealthSnapshot.Checks` | native code를 대소문자 구분 map key로 보존; 알려지지 않은 code도 반환 |
| `HealthCheck.Severity` | 해당 check 자체의 native severity; muted check에서도 원래 severity 유지 |
| `HealthCheck.Summary`, `Details` | native summary와 순서대로 보고된 detail message |
| `HealthCheck.Count` | native signed `int64`; detail 개수와 같을 필요가 없음 |
| `HealthCheck.Muted` | native 저장 mute의 code 집합에 현재 check가 포함되는지 여부 |
| `HealthSnapshot.Mutes` | 현재 check 유무와 별개로 MON에 저장된 mute 목록 |
| `HealthMute.Count`, `Summary`, `Sticky` | mute가 보존한 native 값; 현재 check의 count/summary와 같다고 가정하지 않음 |
| `HealthMute.ExpiresAt` | native `ttl` string; TTL이 없으면 빈 문자열 |

`HEALTH_OK`여도 muted warning/error가 `Checks`에 남을 수 있다. 장애 code가
사라졌다는 assertion에는 `Status`와 함께 정확한 code와 mute 여부를 확인한다.
Sticky mute는 해당 check가 해결되어도 남는다. Nonsticky mute도 MON의 다음
tick 전에는 check가 사라진 상태나 TTL이 지난 상태로 보일 수 있다.
이 관계는 [Ceph v20.2.4 health formatter와 mute 처리](https://github.com/ceph/ceph/blob/v20.2.4/src/mon/HealthMonitor.cc#L387-L484)를 따른다.

`ExpiresAt`는 시간을 caller timezone으로 바꾸거나 caller clock으로 만료 판정한
값이 아니다. `2026-10-08T18:00:00.123456+0900`처럼 microsecond 여섯 자리와
숫자 offset `+HHMM`/`-HHMM`을 보존한다. Epoch가 315360000초 미만이면
`1.000000` 같은 seconds.microseconds 형식도 허용한다. TTL이 없으면 native가
field를 생략한다. 근거는 [native mute schema](https://github.com/ceph/ceph/blob/v20.2.4/src/mon/health_check.h#L85-L110)와
[native 시간 문자열](https://github.com/ceph/ceph/blob/v20.2.4/src/include/utime.h#L314-L347)이다.

필수 native field의 누락·null, 중복 field/code, 잘못된 타입, signed count 범위
초과와 trailing JSON은 성공한 관측으로 해석하지 않는다. 필수 field의 대소문자나
유효한 Unicode case-folding 별칭은 중첩 check map 값 안에서도 거부한다.
결과에 영향을 주지 않는 추가 field는 허용한다. 입력은 최대 4MiB와 nesting
제한을 적용한다. Invalid UTF-8와 unpaired UTF-16 escape는 Go `encoding/json`의
U+FFFD replacement 동작을 따른다. 유효한 native 문자열 보존과 malformed
Unicode의 원본 byte 보존은 다른 범위다.

반환 `Summary`·`Details`·mute summary에는 애플리케이션이 사용한 경로, 이름이나
비밀 정보가 포함될 수 있다. Error redaction은 이 성공 결과의 message까지 제거하는
기능이 아니다. Caller가 로그·실패 메시지에 노출할 내용을 선택한다.

## 검증 상태와 계획

2026-10-08 기준 API와 fake-container unit 경계 검사를 작성했고, 루트의
`CGO_ENABLED=0 go test -mod=readonly ./...`, race, vet와 모든 integration tag의
compile 검사가 통과했다. Race에는 FSID 거부와 동시 취소 경계도 포함한다.
시나리오 helper 검사 89개도 통과했다. 공식 pinned role 이미지의 focused
native 두 parent와 MGR의 bridge/host 각 두 leaf를 모두 통과했고, 실행
271.643초 뒤 자체 cleanup의 새 자원은 0개였다. 과거 65-job/119-parent CI나
scheduler 실험을 이 API의 PASS로 합산하지 않는다. 앞선 cache tag 준비
artifact는 `runtime: not_run`이며 이 native 실행의 이미지나 성공 증거가 아니다.

| 범위 | 검증 대상 | 현재 구분 |
| --- | --- | --- |
| Unit 경계 | 정확한 세 읽기, 한 control handle, original FSID, zero-on-failure, 취소·redaction, strict schema·signed count·mutes·expiry | 구현 옆 fake-container 테스트; native 증거와 별개 |
| 정상 storage와 OSD lifecycle | `TestClusterLifecycle`의 초기 pool-clean과 OSD 추가·제거 후 public 결과를 독립 raw CLI와 비교 | 공식 roles·Linux ARM64 native PASS |
| 첫 MGR 없는 상태 | `TestNoInitialManagerTopology`의 bridge/host 각 storage-first와 MON-only 조합에서 cold 상태와 복구 상태 비교 | 네 leaf 모두 native PASS |
| TTL·sticky mute | owned `noout`의 `OSDMAP_FLAGS` 경고와 TTL 저장·만료, flag 복원 뒤 sticky 유지 및 명시적 unmute | storage-first bridge/host에서 native PASS |

독립 원본 검증은 [integrationNativeHealth](../internal/integration/health_details_integration_test.go)의
raw `ceph health detail --format json`을 별도로 decode한다. Public API 호출 전후
원본 결과가 안정된 구간에서 status/code/severity/message/count/mute를 비교한다.
두 원본 관측 사이에 상태가 바뀌면 caller deadline 안에서 다시 비교한다.
API 결과 자체를 원본 oracle로 쓰지 않고, 기존 unexpected-code/module failure
거부 및 FSID·pool/data·owned cleanup 검사를 유지한다.

MGR fixture는 기존 `TemporaryOSDFlag(ctx, "noout", true)`로 즉시 확인할 경고를
준비한다. Global flag의 code는
[`OSDMAP_FLAGS`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSDMap.cc#L7696-L7721)이며,
per-OSD/CRUSH flag의 `OSD_FLAGS`와 구분한다. Raw CLI로 `OSDMAP_FLAGS`에
20초 TTL mute를 준비하고, 같은 check의
`Muted`와 native expiry string, TTL 이후 mute 제거를 확인한다. 이어서 sticky
mute를 준비하고 첫 MGR 구성과 owned flag 복원 뒤 check가 없어져도 mute가 남는지 비교한다.
명시적 unmute와 실패 경로의 flag·mute 복원 후 원래 unmuted HEALTH_OK·빈 checks/mutes
closure와 기존 데이터 검증을 수행했다. Raw mute/unmute는 테스트 환경
준비·복원의 내부 동작이며, 공개 health mute·운영 복구 mutation API를 추가하지 않는다.

첫 focused 실행은 MGR 경고가 즉시 발생한다는 잘못된 준비 가정으로 실패했다.
새 MON의 기본 `mon_mgr_mkfs_grace`는 2분이며, 이 대기를 늘려 통과시키는 대신
명시적 OSD flag 경고를 준비하도록 수정했다. 첫 실행의 원본·성공한 초기 lifecycle·
실패한 cold fixture와 새 자원 0개의 cleanup은 별도로 보존한다.
두 번째 실행은 실제 global code를 per-OSD code로 잘못 적은 fixture allowlist에서
실패했다. 원본의 `OSDMAP_FLAGS` 관측과 cleanup 성공을 보존하고 정확한 code로
수정했다. 이 두 준비 오류는 API decode 실패나 이미지 의존성 부족이 아니다.

Focused 실행은 source manifest 272파일·SHA256
`0a4a46f5893ea8fcda826448fef8156beb77056ab6b42094da9e9650b798ca6f`와
실행 binary SHA256
`dad2de919c909d8fc6f89df9f2cb65ea77a36e92b032d3dc68fa34d79fcf341d`에 연결한다.
Control은 `sha256:42d753062656366b0383e402920721c683618b7981e71b42732f5a5e5bfad022`,
OSD는 `sha256:7bf9001b79e141a66b5fd916b03db7700852cf8613e964c3131627b3c2a8ec84`였다.
Go host의 cgo·go-ceph 연동은 실행하지 않았다. 원본 로그·source·native image inspect·
실행 receipt와 cleanup은 `artifacts/health-details-20261008/official-pinned-roles-v3/`에 있다.
Native TTL string은 실행 중 raw/API 전체 값 비교와 predicate로 검증했지만,
중간 원문 TTL JSON을 별도 artifact로 저장한 범위까지는 주장하지 않는다.

기존 cold OSD/MDS, 마지막 MDS 교체와 RBD namespace의 raw health oracle은
토폴로지에 맞는 장애 code·이미지 의존성 문제를 구분하는 참고 근거다. 그 과거
성공은 새 HealthDetails 호출, 실제 TTL/sticky decode 또는 전체 API surface의
검증을 대신하지 않는다. 실행 선택과 증거 범위는 [CI fixture 계약](CI_FIXTURES.md)을 따른다.
