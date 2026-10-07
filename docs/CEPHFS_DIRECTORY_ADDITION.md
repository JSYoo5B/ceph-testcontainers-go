# CephFS mirror 디렉터리 추가 intent

`BeginDirectoryAddition(ctx, path)`는 native 추가 전에 원래 cluster·filesystem·metadata pool·peer·경로 generation과 추가 intent를 보존합니다. 응답 유실 뒤 같은 경로로 명시적으로 재시도하면 새 정책을 확인해 소유권을 확정할 수 있습니다. Receipt의 `Status(ctx)`는 조회만 수행합니다.

## 사용 방법

실제 CephFS directory와 데이터는 client에서 먼저 준비합니다. 경로는 filesystem root의 absolute path이며 container mount path가 아닙니다. 기존 peer가 준비된 mirror에서 사용합니다.

```go
receipt, err := mirror.BeginDirectoryAddition(ctx, "/test-data")
if receipt == nil {
    return err
}
// 오류와 함께 받은 nonnil receipt도 보존한다.
if err != nil {
    // 호출자가 선택한 새 context에서 같은 intent를 명시적으로 재확인한다.
    same, retryErr := mirror.BeginDirectoryAddition(retryCtx, "/test-data")
    if same != receipt {
        return errors.New("directory addition intent changed")
    }
    if retryErr != nil {
        return retryErr
    }
}
status, err := receipt.Status(observeCtx)
if err != nil {
    return err
}
if !status.Registered {
    return errors.New("directory addition is still pending")
}
// 필요한 AddDaemon 또는 WaitSnapshotSynced를 별도로 실행한다.
```

Begin은 최대 2분, Status는 최대 30초이며 더 짧은 caller deadline을 따릅니다. Native ACK 뒤 정책이 아직 보이지 않으면 receipt와 오류를 반환합니다. 같은 receipt의 fresh Begin으로 convergence를 확인하며 ACK된 추가를 다시 보내지 않습니다. Status는 path/peer·filesystem 이름과 ID, `PolicyPresent`·`Registered`를 반환합니다. 두 flag는 daemon assignment나 snapshot 복제 완료를 뜻하지 않습니다.

## 등록과 재시도의 경계

새 intent는 경로가 privately unowned이고 원래 MON/MGR peer tuple·양쪽 cluster FSID·filesystem·metadata pool identity가 유지되며, strict native `mirror ls`에서 경로가 없다는 반복 관측 뒤에만 생성합니다. Nil/malformed/duplicate/noncanonical 경로 목록과 겹치는 native 경로는 거부합니다. Native mutation 전에 receipt와 attempted intent를 기록합니다.

- 응답이 불확실하고 exact 경로가 보이면 같은 retained intent의 fresh Begin만 소유권을 확정합니다. Status는 `PolicyPresent=true, Registered=false`를 반환할 수 있습니다.
- 응답이 불확실하고 원래 scope에서 경로 부재를 확인하면 같은 intent의 추가 요청을 다시 시도할 수 있습니다. CLI exactly-once를 보장하지 않습니다.
- Native ACK를 받은 뒤 경로가 아직 없으면 조회를 재시도하고 추가 요청을 재전송하지 않습니다.
- 등록 완료 뒤의 Begin은 최신 원래 정책을 확인하고 같은 receipt를 유지합니다. 경로가 사라졌다고 이전 receipt로 자동 재등록하지 않습니다.

마지막 원래 scope·경로 정책·local owner·generation·context 확인까지 fixture 잠금을 유지합니다. 소유 path를 한 번만 기록하고 generation을 한 번만 증가시킵니다. 후속 조회 실패는 과거 등록 결정을 취소하지 않으며, 이번 결과의 flag는 새 관측에 성공한 경우에만 반환합니다.

이전 `AddDirectory` 응답 유실이나 외부에서 먼저 등록한 경로는 새 typed intent로 채택하지 않습니다. Native path 정책에는 등록 UUID나 compare-and-swap identity가 없으므로 경합하는 외부 policy writer는 지원 조건 밖입니다. 이 API가 생성한 attempted intent와 최신 원래 scope를 신뢰하는 일회성 fixture를 전제로 합니다.

## 수명과 구성 변경

한 fixture는 pending addition intent 하나를 유지합니다. 같은 경로의 Begin은 원래 receipt를 재시도하며 다른 경로로 미확정 intent를 덮어쓰지 않습니다. Pending 동안 daemon 추가·legacy add·rebalance·peer remove/rebootstrap·directory remove 및 기존 제거 receipt의 retry shortcut을 거부합니다. `RemoveDaemon`과 `Terminate`는 cleanup에 사용할 수 있습니다. Receipt 자체는 컨테이너·데이터·cleanup hook을 소유하지 않습니다.

이미 초기화한 fixture의 owned daemon이 0개여도 정책을 등록·조회할 수 있습니다. Constructor의 `DaemonCount=0` 기본값은 계속 1이므로, 실제 zero inventory는 `RemoveDaemon`으로 구성합니다. 새 daemon 이후 backlog와 특정 snapshot의 복제 완료는 별도 관측과 client I/O로 확인합니다.

같은 path의 제거·재등록·rebalance 시도는 native 응답이 실패해도 기존 완료 addition receipt를 먼저 무효화합니다. 원래 owned peer의 삭제 시도도 peer-scoped 완료 receipt를 먼저 무효화합니다. Stale peer 제거 receipt의 조회·재시도는 더 새로운 addition generation을 무효화하지 않습니다.

최소 retained registry는 완료 receipt 하나를 보관합니다. 다른 경로의 새 typed intent를 시작하면 이전 완료 addition receipt는 supersede됩니다. 이미 소유한 이전 경로의 등록·데이터는 유지합니다. 다른 경로의 legacy `AddDirectory`는 기존 선택 receipt를 무효화하지 않습니다. 같은 경로를 재등록할 때 이전 제거 receipt도 새 native 추가 시도 전에 supersede하며, 기존 `Released`·process acknowledgment 결과를 새 generation으로 옮기지 않습니다. [제거 계약](CEPHFS_DIRECTORY_REMOVAL.md)과 [원래 process 승인](CEPHFS_PROCESS_ACKNOWLEDGMENT.md)을 따릅니다.

## 검증

적용한 runtime 후보의 독립 source 검토와 임시 module의 multicluster unit·focused race는 통과했습니다. 적용한 저장소의 `make check`도 unit·race·vet·전체 tag compile까지 통과했습니다. 현재 실제 Go 목록에서 제거·재등록 복구 profile 5개, 필수 union 110개의 disjoint 선택을 확인했습니다. 2026-10-07 Docker Desktop Linux ARM64에서 원본 pinned `ceph.DefaultImage`로 `TestMultiClusterCephFSDirectoryAdditionIntent`를 순차 실행했습니다.

| 실행 | 시간 | 결과 |
| --- | ---: | --- |
| bridge | 404.82초 | PASS |
| host network | 398.96초 | PASS |
| parent / Go package | 803.78 / 804.090초 | PASS |

각 fresh pair에서 원래 daemon을 모두 제거한 뒤 실제 native 추가의 응답만 한 번 유실했습니다. 정책 존재·미확정 소유권을 read-only로 확인한 뒤 fresh Begin에서 추가 재전송 없이 등록을 확정했습니다. 새 receiver의 backlog·새 checkpoint, typed 제거의 실제 응답 유실·동일 receipt 재확인·release·같은 경로의 새 등록 cycle을 검사했습니다. 별도 경로는 추가 전 오류로 native dispatch 0회를 유지하고 정책 부재 관측 뒤 fresh Begin에서 한 번 적용했습니다. Preexisting foreign 정책을 privately 채택하지 않았습니다.

원문 검증은 source/destination 56개 byte/hash 기록이며 반복한 frozen snapshot·seed·foreign control도 포함합니다. 공개 exact source checkpoint 관측은 pair마다 6개, 총 12개입니다. 부재 관측 구간 6개와 정책·등록·native 요청 counter의 상태 기록 10개를 확인했습니다. 원래 FSID·filesystem·metadata pool·peer, pending mutation의 CLI 0회·customizer 호출·stale receipt 거부는 실행한 source assertion으로 대조합니다. Private generation이나 별도 raw CID marker가 출력됐다고 해석하지 않습니다.

`artifacts/cephfs-directory-addition-20261007/native-runtime.log`와 `source-before.json`·`source-after.json`의 runtime source 234개 해시가 같으며 image 정책도 유지됐습니다. Same-engine strict cleanup에서 새 container/network 0개를 확인했습니다. 독립 verifier도 runtime·source·정책·cleanup·선택 목록·원문 및 관측 순서를 대조해 PASS했으며 최종 `verification.json`을 같은 evidence 디렉터리에 보존합니다. 이 focused 실행은 전체 CI나 공식·Debian·Ubuntu image matrix의 새 PASS가 아닙니다.
