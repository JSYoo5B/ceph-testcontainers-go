# OSDMap full 조건 fixture

`Container.TemporaryFullRatios`는 disposable cluster의 nearfull·backfillfull·full
임계값을 변경하고 `FullRatiosOverride.Restore`로 원래 값을 복원한다.
`Container.FullRatios`는 native OSDMap의 FSID·epoch·세 임계값을 읽는 Check다.
Central config나 pool quota와 별개인 OSDMap 정책이며, 기존 `TemporaryConfig`의
대체 대상이 아니다. Ceph native Go 라이브러리나 추가 이미지 도구는 필요 없다.

```go
before, err := cluster.FullRatios(ctx)
if err != nil {
    return err
}
change, err := cluster.TemporaryFullRatios(ctx, ceph.FullRatios{
    NearFull: .0001, BackfillFull: .0002, Full: .0003,
})
// 여러 native 명령 중 일부가 성공한 뒤 오류가 날 수 있다.
// 오류와 함께 받은 non-nil handle도 복원하거나 cluster를 종료해야 한다.
if change != nil {
    defer change.Restore(cleanupCtx)
}
if err != nil {
    return err
}
// HealthDetails로 OSD_FULL을 기다리고 실제 client 오류를 검사한다.
// before.Ratios는 복원 후 비교할 값이다.
```

## 입력과 관측 계약

입력은 유한한 capacity fraction이며 native float32 정규화 후
`0 < NearFull < BackfillFull < Full <= 1`이어야 한다. 원래 map도 이 순서를
만족해야 임시 변경을 시작한다. 정밀도 손실로 두 임계값이 같아지는 입력은
거부한다. 반환 snapshot은 caller의 float64 리터럴 대신 native JSON 값을
보존하므로 `.95`와 단순 float64 equality를 비교하지 않는다.

Check는 native에 있는 0·동일값·잘못된 순서도 그대로 보여준다. 임시 fixture의
admission과 native 관측의 허용 범위는 다르다. Epoch는 전체 OSDMap 버전이며
ratio만의 generation이 아니다. Identity 확인과 map query는 독립 명령이므로
원자적 snapshot이 아니고, 외부 writer가 검사와 변경 사이에 개입하면 안 된다.

Control handle은 호출마다 한 번 캡처한다. Map query 전후의 native FSID와 map
안의 FSID를 bootstrap identity와 비교한다. Restore는 생성 당시 identity도
보존한다. Context 대기·query·decode·최종 identity 검사에서 실패하면 Check는
zero result와 error를 반환하며 raw CLI 오류 본문은 공개 오류에 넣지 않는다.

## 변경·복원과 부분 실패

Ceph CLI는 세 값을 한 번에 바꾸지 못한다. 감소는 nearfull→backfillfull→full,
증가는 full→backfillfull→nearfull 순서로 진행해 중간 값의 순서를 보존한다.
최대 세 명령이며 native readback까지 성공해야 apply 성공을 반환한다.

한 cluster에는 하나의 override만 허용한다. 값이 같은 no-op lease도 중첩할 수
없으며 handle 복사본은 같은 복원 상태를 공유한다. Restore 성공은 멱등적이다.
마지막 확인 tuple과 아직 응답이 확정되지 않은 명령의 before/after tuple만
기록한다. 부분 성공·응답 유실 후 Restore는 이 값만 채택하고 다른 tuple이나
epoch rollback은 거부한다. 과거 중간 tuple도 새 외부 변경을 덮어쓸 근거가
되지 않는다. 실패 후 자동 rollback이나 background retry는 없다.

Native ratio CAS가 없어 외부 writer가 같은 값을 다시 쓴 경우는 구별할 수 없다.
Apply·Restore에 외부 ratio writer를 동시에 사용하지 않는다. Unrelated flag,
pool policy, central config는 이 fixture가 변경하지 않는다.

Exec가 취소되거나 응답을 잃어도 native CLI process 종료를 증명하지는 못한다.
아직 실행 중인 명령은 readback 뒤에 commit할 수도 있다. 이 경우 먼저 native
operation의 종료를 확인하거나 disposable cluster를 종료한다. Journal은 관측한
부분 상태의 복원을 제공하며 취소된 process의 drain이나 transaction을 보장하지 않는다.

## 실제 full 상태와 client 검증

Snapshot은 map 정책이다. 실제 fullness 보고는 OSD 통계 전파 후 바뀌므로
`HealthDetails.Checks`의 `OSD_NEARFULL`, `OSD_BACKFILLFULL`, `OSD_FULL`을
별도로 기다린다. 낮은 ratio는 소량의 기존 사용량으로 이 상태를 재현하며,
실제 디바이스를 가득 채우거나 filesystem ENOSPC를 만드는 기능은 아니다.
OSD별 failsafe와 기타 설정도 실제 동작에 영향을 준다.

Native RADOS는 full map에서 일반 write를 대기시킬 수 있다. `FULL_TRY`는 이
client 대기를 통과해 OSD의 `ENOSPC`/`EDQUOT`를 관측하는 별도 operation flag다.
`FULL_FORCE`와 혼동하지 않는다. 해당 client flag를 적용하는 것은 테스트할
client의 책임이며 Go 모듈은 client SDK를 추가하지 않는다.

[통합 시나리오](../internal/integration/osd_full_ratios_integration_test.go)는
bridge·host network, 역할별 control/client·OSD 이미지, 512 MiB sparse OSD 두
개로 다음 조건을 검증한다. 각 단계는 native JSON oracle과 비교하고, 기존
pool ID·설정을 보존하며 원래 ratio와 I/O를 복원한다.

- nearfull 보고와 일반 RADOS write 성공
- backfillfull 보고와 일반 RADOS write 성공
- full 보고와 `FULL_TRY` write의 errno 28, 거부 객체 부재와 기존 read 성공
- 각 단계 복원 후 기존 bytes와 새 write 성공, copied handle 반복 복원

테스트는 startup benchmark를 명시적으로 끈 뒤 OSD를 추가한다. 모듈 기본값을
바꾸지는 않는다. 이 결과는 RGW HTTP 응답이나 CephFS client ENOSPC, 실제
디바이스 고갈, 자동 ratio 튜닝까지 검증한 것으로 해석하지 않는다.

```sh
python3 .github/scripts/tag_scenarios.py run \
  --category short --batch osd_full_ratios --package ./internal/integration \
  --directory artifacts/local-full-ratios
```

## Native 근거

- [MON ratio 명령](https://github.com/ceph/ceph/blob/v20.2.4/src/mon/MonCommands.h#L854-L865)
- [OSDMap float32 보관](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSDMap.h#L412-L414)
- [Native fullness health](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSDMap.cc#L7224-L7298)
- [OSD effective threshold 계산](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc#L750-L798)
- [librados FULL_TRY](https://github.com/ceph/ceph/blob/v20.2.4/src/include/rados/librados.h#L103-L125)
- [초기 config와 OSDMap의 구분](https://docs.ceph.com/en/tentacle/rados/configuration/mon-config-ref/)

## 검증 기록

현재 단위 테스트는 mixed transition 순서, native float32, strict JSON, identity
변경·cancel·lock 대기, 부분 apply/restore와 응답 유실, outside tuple 거부,
copied handle과 no-op lease를 검사한다. Native 실행 결과는 아래에 별도로
기록한다. 최초 실행의 초기 OSD count helper 충돌도 실패 기록으로 보존한다.

2026-10-09의 공식 역할 이미지 Linux ARM64 실행은 bridge·host 각각 세 fault
단계와 parent를 포함한 9개 RUN/PASS, parent 182.29초로 통과했다. Check와
독립 OSDMap/health JSON이 일치했고 각 단계에서 원래 ratio·pool identity·bytes와
일반 write를 복원했다. Native `FULL_TRY`의 errno 28과 거부 객체 부재는 Python
client assertion으로 검사했다. 외부 cleanup checker도 container/network/volume
잔여 없이 PASS했다. 이 focused 결과를 전체 CI나 다른 플랫폼의 완료로 합산하지
않는다.

로컬 기록은 `artifacts/full-ratios-images/summary.json`의 역할별 resolved
image IDs, `artifacts/full-ratios-native-v3/report.json`과 `native.log`,
`artifacts/full-ratios-cleanup-v3`에 보존했다. 첫 helper 충돌 native FAIL과 다음
미사용 import compile FAIL은 각각 `artifacts/full-ratios-native`와
`artifacts/full-ratios-native-v2-driver.log`에 보존한다. 이 artifacts는 Git의
배포 문서가 아니며 checkout에서 별도로 실행한 결과다.

같은 source의 host unit·race·vet, 수정 후 `make tag-compile`, 전체 compiled tag
inventory 검증도 PASS했다. Planner는 새 batch를 자동 발견했으며 workflow
YAML에 테스트 이름을 추가하지 않았다.
