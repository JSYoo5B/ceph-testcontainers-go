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
| `TestMultiClusterRBDJournalMirrorFailback/<network>` | PASS 242.03s | PASS 265.23s | receiver Stop 후 non-ready·3초 deadline·동일 container 재시작의 새 native instance, 원래 image IDs 유지, A→B→A non-forced failback과 8MiB 원문 |
| `TestMultiClusterRBDMirrorScopeAndNamespaces/<network>/pool-named` | PASS 72.52s | PASS 93.31s | journal 자동 편입 image와 selected image의 2MiB 초기·변경 원문, ns-a→ns-b mapping, 다른 namespace/기본 namespace 원문·plain image 제외 |
| `TestMultiClusterRBDMirrorScopeAndNamespaces/<network>/image-snapshot-named` | PASS 62.85s | PASS 66.02s | snapshot checkpoint의 2MiB 초기·변경 원문, 동일 namespace 격리·제외 |

`<network>`는 각 column의 `bridge` 또는 `host` child 이름입니다.

각 network 실행의 package terminal은 PASS 426.415s / 474.152s이며 parent·child 시간을 distinct test로 더하지 않습니다. `artifacts/followups-20261007/rbd-bridge-runtime-final.log`, `rbd-host-runtime.log`, `rbd-check-final.log`, `rbd-source.json`에 로컬 증거를 보관합니다. `rbd-bridge-final-cleanup/after.json`과 `rbd-host-cleanup/after.json`은 같은 engine에서 baseline 대비 container/network/Ryuk 신규 잔여 0개, strict PASS입니다.

초기 bridge probe는 native `daemon_id` prefix 불일치를 확인한 뒤 해당 실행을 SIGTERM으로 중단했습니다. `rbd-bridge-runtime.log`의 signal-terminated FAIL과 `rbd-bridge-cleanup/after.json`의 cleanup PASS/잔여 0개를 보존하며, 수정한 source의 위 최종 실행과 구분합니다. 기본 namespace의 독립 `TestMultiClusterRBDSnapshotMirror`도 공개 wait를 사용하도록 변경하고 compile했지만 이번 focused 실행에 선택하지 않았습니다. Namespace 나머지 세 조합의 새 wait나 103개 전체 CI·Debian/Ubuntu image matrix를 이번 실행으로 새로 통과했다고 주장하지 않습니다. 기존 전체 검증은 당시 source 증거를 따릅니다.

## CephFS directory와 source checkpoint

`CephFSMirror.DirectoryStatus(ctx, directory)`는 현재 연결이 소유한 canonical directory 정책을 읽습니다. `CephFSMirrorDirectoryStatus`는 양쪽 filesystem 이름·원래 ID, 현재 peer UUID, MGR mapping 상태, native filesystem watcher instance, 실제 소유 daemon, replay 상태·실패 이유, current/last source snapshot 및 세 native counter를 제공합니다. Counter는 process 재시작이나 directory 재할당 때 0부터 시작할 수 있습니다.

`Ready`는 해당 directory가 현재 실행 중인 소유 daemon과 정확한 destination peer에 연결되어 `idle`/`syncing`인지를 나타냅니다. 멈춘 다른 구성원은 `DaemonProblems`에 남고 `DirectoryStatus`는 partial 오류도 반환합니다. `WaitDirectoryReady`와 `WaitSnapshotSynced`는 선택된 owner가 ready일 때에만 이 다른 구성원의 partial 오류를 허용하며, 성공한 결과에도 문제를 보존합니다. 선택된 owner의 조회 오류, identity/schema 오류, cancellation은 이 예외에 포함되지 않습니다. 전체 fixture의 건강이나 payload 일치를 보장하는 값은 아닙니다.

```go
checkpoint := multicluster.CephFSMirrorSnapshot{
    ID: sourceSnapshotID, // source native client에서 독립적으로 읽은 ID
    Name: "checkpoint-1",
}
if _, err := link.WaitSnapshotSynced(ctx, "/application", checkpoint); err != nil {
    return err
}
// 이후 destination의 해당 snapshot과 실제 bytes를 별도로 확인합니다.
```

`WaitSnapshotSynced`는 `last_synced_snap`의 source ID와 이름이 **둘 다 정확히 같을 때** 완료됩니다. 더 큰 ID나 counter 증가로 대신하지 않습니다. Expected ID는 source native client의 `LibCephFS.snap_info("/application/.snap/checkpoint-1")["id"]` 등에서 읽습니다. Destination의 local snapshot ID는 다른 값이며, observer의 현재 `LastSyncedSnapshot`을 그대로 expected 값으로 삼으면 의도한 checkpoint의 검증이 되지 않습니다. Native Go binding 없이 source Linux client container에서 이를 읽을 수 있습니다.

`DirectoryStatus`는 최대 30초, 두 wait는 최대 4분과 caller context를 따릅니다. Wait는 poll 사이 mutex를 놓고 첫 관측의 양쪽 filesystem ID·peer UUID·canonical path를 고정합니다. Peer를 정당하게 다시 만들었으면 새 wait를 시작합니다. 원래 양쪽 filesystem/metadata pool과 peer destination identity를 조회 앞뒤에서 확인하며, 현재 MGR assignment를 다시 읽습니다. 일시적 CLI 조회 오류는 재시도하고 마지막 관측과 원래 error chain을 보존합니다. 성공한 native 응답의 잘못된 schema나 identity 변경은 즉시 실패합니다. 관측은 rebalance·policy·snapshot 생성·process 상태를 변경하지 않습니다.

Directory의 identity는 현재 owned policy path이며 inode generation의 계약은 아닙니다. 여러 CLI 조회를 외부 변경과 원자적으로 묶지 않습니다. Exact snapshot wait도 한 시점의 native 완료 report이므로 이후 삭제·같은 path 재생성·재할당의 retention까지 보장하지 않습니다. 더 새 checkpoint가 먼저 `last_synced_snap`을 차지하면 conservative wait는 timeout할 수 있습니다. 기다리는 동안 checkpoint 생성을 잠시 멈추고 destination snapshot의 원문을 따로 확인합니다. 제거한 directory의 원래 sync cycle 해제는 별도 [retained directory receipt](CEPHFS_DIRECTORY_REMOVAL.md), peer worker 종료는 [peer receipt](CEPHFS_PEER_REMOVAL.md)로 관측합니다. 이 현재 상태 API가 제거 완료를 대신하지 않습니다.

Native schema와 snapshot ID 의미는 고정 [MGR dirmap](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/mgr/mirroring/fs/dir_map/policy.py#L344), [filesystem session](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/cephfs_mirror/FSMirror.cc#L462), [peer replay status](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/cephfs_mirror/PeerReplayer.cc#L2109), [source snap_info](https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/cephfs/cephfs.pyx#L1209-L1232), [restart 후 last_synced 복원](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/cephfs_mirror/PeerReplayer.cc#L1971)을 따릅니다. Admin JSON의 첫 object section은 [Formatter](https://github.com/ceph/ceph/blob/v20.2.4/src/common/Formatter.cc#L194)와 [admin socket setup](https://github.com/ceph/ceph/blob/v20.2.4/src/common/admin_socket.cc#L535)에 따라 별도 `stats` wrapper 없이 directory map입니다. 실행 이미지는 기존 control/all을 그대로 사용합니다.

## CephFS 검증

2026-10-07 같은 고정 원본 Quay와 Linux ARM64 engine에서 다음 snapshot·backup·HA 실행을 완료했습니다. Expected checkpoint는 source Linux client의 `snap_info`에서 읽고 wait 뒤 source에서 다시 읽어 같은 이름의 재생성을 거부합니다. Observer report 자체로 expected ID를 만들지 않습니다.

| 시나리오 | terminal | 실제 확인 범위 |
| --- | --- | --- |
| `TestMultiClusterCephFSSnapshotMirrorAndBackup` | PASS 234.132s | bridge: source snapshot ID 2/3/5/6와 backup-1~4 이름, 최초 watcher 4297→restart 4422, stopped receiver non-ready·2초 deadline·상태 유지, directory/peer 제거의 10초 미도착·재등록 catch-up, 새 peer UUID와 원문 |
| `TestHostNetworkCephFSSnapshotMirrorAndBackup` | PASS 243.527s | host: 같은 source ID·이름, watcher 4299→4424, 동일 중지·재등록 경로 및 원문 |
| `TestMultiClusterCephFSMirrorDaemonRebalanceTopology` | PASS 365.435s | bridge HA: 4 directory의 2→1→2→1→0→1 receiver 구성, source ID 2~21의 20회 exact checkpoint, initial·owner-stopped·replacement·all-removed·peer-isolated 각 4개 원문 |

두 실행 모두 기존 snapshot 삭제 전파·frozen bytes 유지, binary/empty/nested files·상대 symlink·mode·owner의 별도 archive 복원, destination OSD 2→3→2 교체 후 source MON/MDS/OSD 정지 상태의 fresh destination 읽기를 유지했습니다. Native mirror의 기존 user xattr 차이는 raw log에서 계속 별도 보고하며, 완전한 metadata 복제로 표시하지 않습니다.

HA에서는 watcher 4282/4288의 2/2 배치에서 `a` 중지 후 `b`/4288이 네 directory를 승계했습니다. Direct status의 partial error와 `DaemonProblems["a"]`를 유지하며 공개 wait는 선택된 `b`의 exact checkpoint에서 성공했습니다. 교체 `a`/4745의 명시적 rebalance, 모든 receiver 제거 중 10초 미도착과 새 `c`/5038의 backlog catch-up도 통과했습니다. Peer bridge 단절·복구 동안 같은 process/PID·source assignment·peer UUID와 endpoint IP·alias·priority를 유지하고 마지막 네 snapshot의 원문을 확인했습니다. 알려진 자동 native shuffle diagnostic의 실패/skip을 이 명시적 rebalance 성공으로 바꾸지 않습니다.

`artifacts/followups-20261007/cephfs-bridge-runtime.log`, `cephfs-host-runtime.log`와 각각의 `cephfs-bridge-cleanup/after.json`, `cephfs-host-cleanup/after.json`에 terminal PASS 및 같은 engine의 baseline 대비 container/network/Ryuk 신규 잔여 0개인 strict PASS를 보관합니다. `cephfs-check.log`의 `make check`는 cgo-free unit·vet·전체 integration tag compile과 별도 race 검사 PASS입니다. 공개 Go module의 native dependency와 고정 이미지 정책은 변경하지 않았습니다.

HA의 terminal/cleanup은 `cephfs-ha-runtime.log`와 `cephfs-ha-cleanup/after.json`의 PASS/신규 잔여 0개입니다. `cephfs-verification.json`은 세 terminal·4/4/20 checkpoint 관측·phase별 4개·partial member와 strict cleanup을 raw 원본에서 대조한 로컬 요약이며 관측을 distinct test 수로 더하지 않습니다. `cephfs-source.json`의 관련 Go/script/module 196개 파일 SHA256은 세 실행 전후 동일했고, 이미지 정책 SHA256도 기존 `4682819b3174a632b9da32eb955f9c52593a24112f780f64280380f0e6ccee62`로 유지됐습니다. 새 observer의 HA host 경로는 compile했지만 이번 focused native 실행에서는 선택하지 않았습니다. 103개 전체 CI·Debian/Ubuntu image matrix의 새 전체 성공으로 합산하지 않습니다.
