# Mirror 상태 관측과 준비 대기

## RBD image replay

`RBDMirror.ImageStatus(ctx, name)`은 연결이 선택한 pool·source/destination namespace 안의 image를 읽습니다. 이름에 pool/namespace/snapshot 또는 option 문법을 넣지 않습니다. `RBDMirrorImageStatus`는 양쪽 local image ID와 global mirror ID, primary 방향, mirror mode/state, destination local replay 상태와 실제 소유 receiver의 daemon/instance를 제공합니다. Source primary에는 local replay report가 없을 수 있으므로 destination report를 사용합니다.

`ReplayReady`는 source primary·destination secondary, 같은 global ID, destination의 `up+replaying`, 현재 실행 중인 소유 Docker receiver와 일치하는 native pool instance를 함께 확인합니다. 중지·pause·restart 중인 process나 다른 daemon을 readiness 근거로 쓰지 않습니다. 이 값은 특정 write나 snapshot의 복제 완료를 뜻하지 않습니다. Application bytes 또는 별도의 checkpoint를 계속 확인합니다. Native `last_update`도 freshness token으로 해석하지 않습니다.

```go
status, err := link.WaitReplayReady(ctx, "volume")
if err != nil {
    // deadline/cancel에서도 마지막 관측을 검사할 수 있습니다.
    return err
}
if !status.ReplayReady {
    return fmt.Errorf("receiver is not replaying")
}
// 이후 client로 필요한 checkpoint와 실제 destination bytes를 확인합니다.
```

`ImageStatus`는 caller context와 최대 30초, `WaitReplayReady`는 caller context와 최대 4분으로 제한합니다. Wait는 poll 사이의 mutex를 놓고 최초로 관측한 source local/global ID와 나타난 destination local ID를 고정합니다. 기다리는 중 같은 이름의 새 image를 받아들이지 않습니다. Native 조회가 일시적으로 실패하면 재시도하며 마지막 관측과 query cause를 유지해 `errors.Is(err, context.DeadlineExceeded)` 등을 사용할 수 있습니다. 성공한 native 조회가 pool/namespace UUID·scope·mapping·site 또는 image identity 변경을 증명하거나 schema가 잘못되면 즉시 실패합니다.

아직 destination image나 local site report가 없는 초기 전환은 대기할 수 있습니다. Creating/disabling은 관측 가능하지만 ready가 아닙니다. 하나의 관측에서도 status 앞뒤의 양쪽 image info와 원래 pool/policy를 다시 확인합니다. 외부 변경을 이 조회들과 원자적으로 묶는 계약은 아닙니다. 관측·대기는 image enable, checkpoint 생성, promotion/resync 또는 process restart를 수행하지 않습니다.

Native `daemon_service.daemon_id`는 fixture가 생성한 인증 이름 `client.rbd-mirror.<id>`의 `<id>`입니다. 전체 고정 prefix를 제거한 exact 값과 실제 admin-socket instance를 비교하며 suffix 추측을 사용하지 않습니다. Schema는 고정 [v20.2.4 image status](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/rbd/action/MirrorImage.cc)와 [daemon service 출력](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/rbd/MirrorDaemonServiceInfo.cc), [Tentacle mirroring 계약](https://docs.ceph.com/en/tentacle/rbd/rbd-mirroring/)을 기준으로 확인합니다.

공개 Go module은 cgo/go-ceph 의존성을 추가하지 않습니다. Setup client와 daemon은 기존 control/all image를 사용하며 별도 mirror role을 요구하지 않습니다.

## RBD 검증

2026-10-07 원본 `quay.io/ceph/ceph:v20.2.4`의 고정 digest `sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`, Linux ARM64 Docker engine에서 bridge/host를 순서대로 실행했습니다. `make check`의 cgo-free unit·vet·전체 tag compile과 race 검사가 통과했고, 아래 runtime 실행 전후 관련 source 38개 파일의 SHA256은 같았습니다.

| 선택한 시나리오 | bridge | host | 별도 client 증거 |
| --- | --- | --- | --- |
| `TestMultiClusterRBDJournalMirrorFailback` | PASS 242.03s | PASS 265.23s | receiver Stop 후 non-ready·3초 deadline·동일 container 재시작의 새 native instance, 원래 image IDs 유지, A→B→A non-forced failback과 8MiB 원문 |
| `TestMultiClusterRBDMirrorScopeAndNamespaces/pool-named` | PASS 72.52s | PASS 93.31s | journal 자동 편입 image와 selected image의 2MiB 초기·변경 원문, ns-a→ns-b mapping, 다른 namespace/기본 namespace 원문·plain image 제외 |
| `TestMultiClusterRBDMirrorScopeAndNamespaces/image-snapshot-named` | PASS 62.85s | PASS 66.02s | snapshot checkpoint의 2MiB 초기·변경 원문, 동일 namespace 격리·제외 |

각 network 실행의 package terminal은 PASS 426.415s / 474.152s이며 parent·child 시간을 distinct test로 더하지 않습니다. `artifacts/followups-20261007/rbd-bridge-runtime-final.log`, `rbd-host-runtime.log`, `rbd-check-final.log`, `rbd-source.json`에 로컬 증거를 보관합니다. `rbd-bridge-final-cleanup/after.json`과 `rbd-host-cleanup/after.json`은 같은 engine에서 baseline 대비 container/network/Ryuk 신규 잔여 0개, strict PASS입니다.

초기 bridge probe는 native `daemon_id` prefix 불일치를 확인한 뒤 해당 실행을 SIGTERM으로 중단했습니다. `rbd-bridge-runtime.log`의 signal-terminated FAIL과 `rbd-bridge-cleanup/after.json`의 cleanup PASS/잔여 0개를 보존하며, 수정한 source의 위 최종 실행과 구분합니다. 기본 namespace의 독립 `TestMultiClusterRBDSnapshotMirror`도 공개 wait를 사용하도록 변경하고 compile했지만 이번 focused 실행에 선택하지 않았습니다. Namespace 나머지 세 조합의 새 wait나 103개 전체 CI·Debian/Ubuntu image matrix를 이번 실행으로 새로 통과했다고 주장하지 않습니다. 기존 전체 검증은 당시 source 증거를 따릅니다.
