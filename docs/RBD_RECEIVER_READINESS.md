# RBD receiver 구성 준비 관측

`RBDMirror.ReceiverStatus`와 `WaitReceiverReady`는 link의 configured
Pool/SourceNamespace/DestinationNamespace를 발견한 receiver들의 native
leader/member 합의를 관측합니다. Mirrored image가 없어도 사용할 수 있습니다.
`ImageStatus`와 `WaitReplayReady`의 image replay 의미는 그대로 유지합니다.

```go
link, err := multicluster.RunRBDMirror(ctx, image, multicluster.RBDMirrorConfig{
    Source: source, Destination: destination, Pool: "rbd",
    SourceNamespace: "app", DestinationNamespace: "standby", DaemonCount: 2,
})
if link != nil {
    defer func() {
        cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
        defer cancel()
        if cleanupErr := link.Terminate(cleanupCtx); cleanupErr != nil {
            log.Printf("terminate RBD mirror: %v", cleanupErr)
        }
    }()
}
if err != nil {
    return err
}
status, err := link.WaitReceiverReady(ctx)
if err != nil {
    return err
}
// 이제 빈 namespace에 테스트용 이미지를 생성하고 기존 image API를 사용할 수 있다.
_ = status
```

이 예시의 source/destination에는 replicated RBD pool과 필요한 namespace가
이미 준비되어 있어야 합니다. 이 API는 기존 pool policy, peer, namespace를
생성하거나 복구하지 않습니다. Run과 AddDaemon의 기존 성공 조건을 readiness
완료로 확대하지 않습니다.

## Ready의 범위

첫 계약의 Ready는 선택 scope의 발견과 **exact owned cohort의 election 합의**입니다.
기대 receiver 모두가 normal running 상태이며 original pool/peer와 정확한
namespace 매핑을 발견하고, 고유한 positive native instance를 제공해야 합니다.
Owned leader는 한 명이고 자신을 leader로 보고하며, leader의 member 집합은
기대 instance 집합과 같고 모든 member의 leader ID가 일치해야 합니다.

Native namespace 상태에는 별도의 ready boolean이 없습니다. Destination의
local namespace는 DestinationNamespace, remote namespace는 SourceNamespace와
같아야 합니다. 빈 문자열은 실제 default namespace입니다. 필드 누락/null은
빈 문자열과 구별하고, 빈 image_replayers 배열은 정상 발견으로 인정합니다.
Default→default, named→named, named→default, default→named 모두 같은 규칙입니다.
Image scope와 자동 journal enrollment인 pool scope 모두 관측할 수 있습니다.

이 Ready는 개별 image replay, 특정 write/checkpoint 전달, 백로그 소진, 과거
process의 종료 증거를 뜻하지 않습니다. 데이터 검증은 기존 image API와 실제
클라이언트 I/O로 별도 진행합니다.

이름을 생략하면 현재 owned inventory 전체가 기대 집합입니다. Stopped 또는
partial-start daemon을 자동으로 제외하지 않습니다. HA survivor를 기다릴 때는
owned 이름을 명시합니다.

```go
status, err := link.WaitReceiverReady(ctx, survivor.DaemonName)
```

다른 member가 아직 native election에 남아 있으면 convergence를 기다립니다.
같은 receiving pool의 foreign member는 ForeignInstances에 기록하고 exact
cohort Ready를 false로 둡니다. 이 진단은 다른 fixture나 cluster가 unhealthy라는
판정이 아닙니다. Shared pool 전체의 aggregate readiness는 별도 후속 계약입니다.
Zero inventory는 성공한 non-ready 관측이며 Wait은 caller deadline까지 기다립니다.
DaemonCount=0의 기존 one-daemon 기본값은 유지하고, zero는 RemoveDaemon으로 구성합니다.

## Original identity와 수명

성공한 Run은 양쪽 원래 bootstrap config의 positive FSID, fresh CLI FSID,
원래 숫자 pool ID와 replicated type, default/selected namespace policy
UUID/mode/site/remote mapping, strict receiving peer tuple을 저장합니다.
Receiving tuple은 peer UUID/site/source mirror UUID/bootstrap client입니다.
각 AddDaemon의 실제 owned handle/full CID/client/name과 socket startup 성공도
private capture로 남깁니다. Partial Run에는 readiness capability가 없습니다.
Partial AddDaemon은 inventory에 남아도 준비된 member로 인정하지 않습니다.

Observer는 앞뒤 fresh CLI로 identity를 확인합니다. Peer remove/recreate,
client/site/source mirror UUID 변경, pool recreate, public Container/ClientName/
DaemonName substitution을 채택하지 않습니다. 정상 rx-only→rx-tx receiving
upgrade는 허용합니다. Native mirror_uuid의 명시적인 빈 문자열은 비동기 학습
pending으로 허용합니다. 이 학습 필드가 아직 빈 상태라도 exact original identity와
namespace/election 조건을 모두 만족하면 Ready=true일 수 있습니다. 따라서 Ready는
remote UUID 학습 완료를 뜻하지 않습니다. 누락/null은 invalid schema이며 nonempty conflicting
UUID는 identity drift입니다.

명시적 Rebootstrap은 owner/context/preflight admission과 original FSID guard
성공 후 첫 native mutation attempt 직전에 old capability를 invalidation합니다.
그 경계는 source의 bootstrap create 호출입니다. 고정 MirrorPool.cc의 create
경로는 --site-name에 대해 set_site_name을 호출한 뒤 native token create를
호출하므로 destination import보다 앞서 source mutation이 가능합니다.
Create-call 오류/응답 유실/호출 중 취소도 old capability를 복구하지 않습니다.
Busy/canceled/preflight rejection으로 native mutation을 시도하지 않았다면 이전
capture를 보존합니다. Mutation attempt 뒤 오류/응답 유실/cleanup 실패는
unconfirmed로 남깁니다. Strict final readback, token cleanup, caller context
확인까지 모두 성공했을 때에만 새 peer generation을 commit합니다.

Wait은 첫 admitted observation의 handles/CIDs/owned names/pool/policy/FSID/peer
capture와 scope를 고정합니다. Poll 중 remove/replacement/새 Rebootstrap을
채택하지 않습니다. 명시적인 topology 변경 뒤에는 새 Wait을 시작합니다.
같은 CID를 Stop/Start하여 현재 native instance가 바뀌는 정상 restart는 허용합니다.
Process state와 raw socket을 두 차례 확인하여 같은 관측 안의 변화를 non-ready로 둡니다.

Fresh ReceiverStatus는 현재 inventory를 새로 관측합니다. Before/after reads는
snapshot 관측이며 외부 CLI의 동시 policy/process mutation을 원자적으로 막지
않습니다. Owned mutex 밖의 native writer와 경쟁시키지 않는 fixture 사용이 필요합니다.

## Context와 오류

ReceiverStatus는 owner/member admission을 포함하여 최대 30초, WaitReceiverReady는
최대 4분이며 더 짧은 caller deadline을 따릅니다. Poll마다 fixture lock을 해제합니다.
Transport/native query 오류는 sanitized error로 재시도하고, 성공한 read의
schema/identity ambiguity는 permanent 오류로 즉시 반환합니다. Arbitrary native
stderr와 credential/token을 public report나 error chain에 보존하지 않습니다.
Canonical context.Canceled/DeadlineExceeded 원인은 errors.Is로 확인할 수 있습니다.
마지막 caller context 검사 뒤에만 Ready=true를 반환합니다.

TC Exec/State와 반환 reader가 context를 준수하는 기존 transport 계약에 의존합니다.
임의의 custom callback이나 reader가 context를 무시할 때의 강제 종료를 보장하지 않습니다.

## 검증 범위

Temp 후보의 multicluster 전체 unit, receiver unit race, vet는 통과했습니다.
현재 저장소에 합친 `make check`의 전체 unit·race·vet·tag compile과 전용 target의 `integration,multicluster` compile도 통과했습니다. 필수 CI 선택 목록은 111개로 확인했습니다. 이 이름 inventory는 현재 111개 전체의 새로운 CI runtime 성공을 뜻하지 않습니다.
별도 native acceptance는 original Quay image에서 bridge/host를 순차 실행하여
네 namespace 매핑의 image snapshot과 named 자동 pool journal enrollment를 검사합니다.
Empty two→explicit survivor→same-CID restart→leader removal→replacement→partial
startup rejection→zero→new receiver resume를 관측하고, 원래 peer/pool/FSID와
raw admin-socket election을 독립 대조합니다. Source/destination의 원래 base와
selected namespace policy tuple 및 receiving remote UUID도 별도로 capture하여
각 positive phase에서 재검사합니다. Zero 동안 만든 2MiB backlog의
실제 destination bytes/global image ID는 readiness와 별도로 검증합니다.
Snapshot case는 initial enrollment 뒤 payload를 쓰고 explicit mirror snapshot을
생성하여 반환 snapshot ID를 source status와 독립 대조하고 증거에 기록합니다.
Pool journal case는 explicit image enrollment나 mirror snapshot을 사용하지 않습니다.
2026-10-07 Docker Desktop Linux ARM64, 4 GiB VM에서 고정 원본
`ceph.DefaultImage`로 실행했습니다. Bridge 880.77초, host 824.54초, parent
1705.32초, package 1705.518초로 모두 PASS했습니다. Network마다 한 fresh
cluster pair 안에 pool/link 다섯 개를 순차 구성했습니다. Parent·network·scope의
13개 RUN/PASS, positive readiness 70개, 2MiB 데이터 marker 10개, 실제
destination reader 10개, snapshot checkpoint 8개를 독립 검사했습니다.
Journal 두 case의 checkpoint marker는 0개입니다.

`artifacts/rbd-receiver-readiness-20261007/native-runtime.log`와
`native-evidence.json`에 terminal exit 0, 원래 image·policy, lifecycle 순서와
raw reader 증거를 연결합니다. 실행 전후 238개 source 해시가 같고
`native-cleanup/after.json`의 새 running/stopped container·network는 0개입니다.
독립 verifier의 16개 synthetic 거부·허용 case도 통과했습니다. 다른 이미지
계열·플랫폼·전체 CI를 새로 검증한 결과와 합산하지 않습니다.

별도 기존 `TestMultiClusterRBDFailback` 회귀도 같은 source에서 parent
183.27초, package 183.721초 PASS입니다. A→B→A의 B-only write 반환,
non-forced A 승격, 이후 복제 재개와 양쪽 user snapshot의 전체 8MiB를
확인했습니다. 예상 최종 byte digest도 독립 계산과 일치했습니다.
`failback/evidence.json`과 별도 before/after cleanup의 새 리소스 0개에 연결하며
이 결과를 receiver suite의 70/10/8 관측 수에 더하지 않습니다.

Native caller budget은 network당 35분, case당 8분이고 전용 Go 90분·CI job
100분을 사용합니다. 위 측정값은 이 로컬 실행의 결과이며 다른 환경의
실행 시간 보장은 아닙니다.

Native schema 근거는 고정 Ceph v20.2.4 source입니다:
[PoolReplayer](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/rbd_mirror/PoolReplayer.cc#L765-L831),
[NamespaceReplayer](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/rbd_mirror/NamespaceReplayer.cc#L118-L137),
[Types](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/rbd_mirror/Types.cc#L25-L28),
[MirrorPool](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/rbd/action/MirrorPool.cc#L256-L330),
[Bootstrap create](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/rbd/action/MirrorPool.cc#L840-L879),
[Mirror checkpoint ID/status](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/rbd/action/MirrorImage.cc#L485-L526).
