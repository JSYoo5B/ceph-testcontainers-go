# Messenger v2 secure 전용 구성

`WithMessengerMode(ceph.MessengerV2Secure)`는 `Run`의 초기 설정에서 Messenger v2 secure만
허용하는 disposable cluster를 만든다. ceph-msgr-go 같은 독립 client 구현이
CephX와 secure framing을 실제 Ceph 서버에 연결해 테스트할 수 있는 구성이다.
공개 Go 모듈에 go-ceph·native linking·cgo 의존성을 추가하지 않는다.

```go
cluster, err := ceph.Run(ctx, image,
    ceph.WithMessengerMode(ceph.MessengerV2Secure),
    ceph.WithOSDImage(osdImage),
)
if cluster != nil {
    defer cluster.Terminate(cleanupCtx)
}
if err != nil {
    return err
}

// 별도 client 구현에 전달할 bootstrap 설정과 ephemeral CephX keyring.
config, keyring, err := cluster.ConnectionConfig()
if err != nil {
    return err
}
// Container 소비자는 cluster.WithClient() / WithClientIdentity(...)를 사용한다.
```

`MessengerMode`는 확장 가능한 typed policy다. 현재 `MessengerDefault`와
`MessengerV2Secure`를 지원하며 unknown 값은 allocation 전에 거부한다.
`Container.MessengerMode()`는 선택한 immutable bootstrap descriptor를 반환한다.
실제 effective 설정이나 현재 connection negotiation의 Check는 아니다.

## Bootstrap 계약

다음 여섯 설정을 global `ceph.conf`에 singleton `secure`로 넣는다. 이 설정은
daemon 시작 시 적용되므로 running cluster의 임시 central config 변경으로
제공하지 않는다. MON·MGR 정책과 OSD 등 다른 daemon 정책을 모두 지정한다.

| 설정 | 값 |
| --- | --- |
| `ms_cluster_mode`, `ms_service_mode`, `ms_client_mode` | `secure` |
| `ms_mon_cluster_mode`, `ms_mon_service_mode`, `ms_mon_client_mode` | `secure` |
| `ms_bind_msgr1` | `false` |
| `ms_bind_msgr2` | `true` |

초기 MON과 `AddMonitor`의 monmap·`mon_host`는 v2 주소만 포함한다. Bridge에서는
MON의 3300 포트만 publish하고 host network에서는 MON마다 하나의 빈 포트를
선택한다. Legacy 주소를 생성해 놓고 bind flag만 끄는 방식은 사용하지 않는다.
이 옵션에서는 `MappedPort(ctx, "6789/tcp")`가 실패한다. Bridge의 MON publish
정보는 `3300/tcp`로 조회하고, host 구성은 선택된 포트를 포함한 exported
bootstrap 주소를 사용한다. 기존 CephX 요구사항은 그대로 유지한다. CephX 인증과
Messenger secure 연결 모드는 서로 다른 설정이다.

옵션을 사용하지 않은 기존 구성은 dual v1/v2 주소와 native 기본 connection
mode를 유지한다. 별도 CRC-only preset이나 running daemon mode 변경 API는
이번 범위에 포함하지 않는다.

`WithClient`·`WithClientIdentity`와 신규 daemon은 같은 global config를 복사한다.
MON refresh는 `mon_host`만 바꾸므로 secure 설정을 보존한다. Secure 구성의
`TemporaryConfig`는 위 여섯 mode와 두 bind 설정을 모든 section/mask에서
CLI 실행과 handle publication 전에 거부한다. 파일 값이 central DB보다
우선하므로 성공했지만 효과 없는 override를 만들지 않는다. 관련 없는
debug/config는 계속 사용할 수 있으며 `MessengerDefault`에는 이 예약을
적용하지 않는다. Caller customizer나
외부 config writer가 이후 모드를 바꾸면 초기 옵션만으로 secure를 보장할 수
없다. 실제 상태는 native effective config와 연결 진단으로 검사한다.

## Client 위치와 이미지

Container client는 기존 `WithClient` network 연결을 사용한다. Docker host에서
직접 MON/MGR/OSD 주소를 소비하는 client는 `WithHostNetwork`와 그 실행 조건을
함께 사용한다. Bridge의 published MON 포트 하나가 daemon의 advertised 주소
전체를 재작성하지는 않는다. `ConnectionConfig`의 주소와 실제 client 위치가
일치해야 한다. [Host network 구성](HOST_NETWORK_POC.md)을 참고한다.

AES-GCM은 Ceph Messenger의 built-in OpenSSL EVP 경로다. 모든 역할 이미지의
기존 executable/runtime dependency closure에 필요한 crypto library가 포함돼야
한다. 별도 `openssl` CLI·TLS 인증서·NET_ADMIN·Go crypto binding을 요구하지
않는다. 이미지 제작이나 package 설치는 이 Go 프로젝트가 수행하지 않는다.
실제 crypto library/provider 문제가 나오면 해당 이미지와 native 오류를 근거로
보고한다.

## 검증 기준

[Native 시나리오](../internal/integration/messenger_secure_integration_test.go)는
공식 역할별 이미지를 받아 bridge의 public/cluster network 및 host network에서
다음을 검사한다.

- 모든 owned MON·MGR·OSD의 effective 여섯 mode와 두 bind flag
- 현재 MON bootstrap과 OSD public/cluster address의 v2-only 구성
- 실제 READY 원격 연결의 송수신 `AES-128-GCM`, CRC/PLAIN 거부
- 독립 Linux native client의 MON·OSD secure 연결과 binary RADOS write/read
- MON·MGR·OSD 추가와 client bootstrap refresh 뒤 기존 데이터 및 새 I/O
- 같은 key/config를 쓰는 CRC-only client의 native mode 거부와 fresh secure 성공
- owned cluster/client 종료와 외부 cleanup checker

`messenger dump`는 먼저 messenger 이름을 발견한 다음 각 이름을 조회한다.
`connections`와 `anon_conns`의 connected, non-loopback, v2 READY만 관측한다.
Local·accepting·deleted 항목, v2 banner, effective config만으로 encryption 성공을
표시하지 않는다. Client 측 admin socket은 살아있는 librados process에서 같은
MON/OSD 연결을 관측한다. OSD role에 ceph CLI를 추가하지 않고 control의 `ceph
tell`로 daemon admin-socket 명령을 전달한다.

Ceph 20.2.4에서 노드 추가 뒤 일부 READY 연결은 `con_mode=unknown`과
송수신 `AES-128-GCM`을 함께 보고했다. Native dump는 `auth_meta`의 mode와
현재 session handler의 cipher를 별도로 읽는다. Connection reuse의 비동기
security reset 경로가 이 조합을 설명할 수 있지만, 정확한 reset 순서는 native
trace로 확정하지 않았다. 검사는 raw mode를 그대로 기록하며 `secure` 또는
`unknown`만 허용하고, 두 방향의 실제 cipher는 반드시 AES-GCM이어야 한다.
명시적 `crc`, PLAIN 또는 crypto handler 부재는 실패한다. 새 client 연결의
실제 I/O와 CRC-only 거부도 별도로 확인한다.

CRC 거부는 단순 timeout을 성공으로 취급하지 않는다. 동일 credentials의
positive→negative→positive 연결을 실행하고, incompatible mode의
`AUTH_BAD_METHOD`와 server allowed mode `[2]`를 확인한다. Python rados의
`connect(timeout=...)`는 watchdog이 아니므로 container-side subprocess 제한과
native timeout 설정을 함께 사용한다.

```sh
python3 .github/scripts/tag_scenarios.py run \
  --category topology --batch messenger_secure --package ./internal/integration \
  --directory artifacts/local-messenger-secure
```

이 fixture의 성공은 별도 pure-Go client 구현의 성공까지 뜻하지 않는다. 해당
client의 framing·CephX·reconnect·data path 검사는 소비자 프로젝트에서 실행한다.
MDS/RGW는 별도 cold fixture에서 CephFS file I/O와 S3 object I/O를 실행한다.
MDS의 native 연결과 CephFS client의 MON/MDS/OSD 연결을 직접 조회한다.
RGW는 native ServiceMap의 hostname/listener/GID를 소유 gateway와 매칭한 뒤
해당 GID의 OSD-side READY connection을 조회한다.

기본 cluster는 CRC-only가 아니므로 secure/default 조합은 협상이 가능하다고
예상할 수 있지만, 두 방향의 실제 mirror 연결/I/O 결과가 필요하다. RBD와
CephFS mirror의 mixed-mode 시나리오는 별도 batch에서 실행하고 아래 기록에서
프로토콜·방향·network별 검증 여부를 구분한다. 실행하지 않은 조합은 미검증이다.
Mirror의 각 cluster에서는 MON `sessions`로 정확한 CephX principal의
open/authenticated session과 global ID를 얻고, owned mirror container의 주소와
함께 MON/OSD/MDS `messenger dump`에 대조한다. MON messenger의
`peer.entity_name`은 해당 client 인증 경로에서 비어 있을 수 있어 principal
oracle로 쓰지 않는다. Mirror의 main admin socket만으로 별도 librados/libcephfs
context를 관측했다고 표시하지 않는다. Default-side CRC는 허용하되 secure-side
원격 연결은 v2 READY와 양방향 AES-GCM을 반드시 확인한다.

## Native 근거

- [Tentacle Messenger v2 설정](https://docs.ceph.com/en/tentacle/rados/configuration/msgr2/)
- [Startup mode 옵션](https://github.com/ceph/ceph/blob/v20.2.4/src/common/options/global.yaml.in#L955-L1038)
- [MON/MGR와 다른 daemon의 정책 선택](https://github.com/ceph/ceph/blob/v20.2.4/src/auth/AuthRegistry.cc#L193-L237)
- [READY 연결의 con_mode와 cipher 진단](https://github.com/ceph/ceph/blob/v20.2.4/src/msg/async/ProtocolV2.cc#L730-L759)
- [Messenger 진단 명령](https://docs.ceph.com/en/tentacle/rados/operations/monitoring/#messenger-status)
- [MON session의 인증 identity](https://github.com/ceph/ceph/blob/v20.2.4/src/mon/Session.h#L129-L143)
- [Dump의 mode/handler 구분](https://github.com/ceph/ceph/blob/v20.2.4/src/msg/async/ProtocolV2.cc#L730-L747)
- [Connection reuse와 security reset](https://github.com/ceph/ceph/blob/v20.2.4/src/msg/async/ProtocolV2.cc#L2619-L2700)
- [Native crypto 구현](https://github.com/ceph/ceph/blob/v20.2.4/src/msg/async/crypto_onwire.cc)

## 검증 기록

2026-10-09의 공개 core source는 `46c2aed`이다. `make check`의 unit·race·vet와
`all` / `all,goceph,native_regression` 컴파일은 통과했다. 자동 CI selector는
63개 required profile, distinct parent 129개 / 실행 선택 136개를 실제 compiled
목록과 대조했다. 이는 전체 native CI 136개 통과 결과가 아니다.

공식 `official-20.2.4-{control,osd,rgw,mds}` 이미지의 Linux ARM64에서 실행했다.
Docker Desktop VM은 4 CPU·약 4 GiB이고 host networking을 사용 가능하게 설정했다.
이미지를 새로 build하거나 package를 설치하지 않았으며 동일 실행의 immutable
image ID를 사용했다. 원본 Quay all, Debian/Ubuntu role, amd64의 새 실행은
이 기록에 포함하지 않는다.

| Role | Immutable image ID |
| --- | --- |
| control | `sha256:5569ed5ec54b85c5f00ff47bea86e5ee6950957424dd7c8e3e857220df90e01a` |
| osd | `sha256:6b9fed2be10a71d89fbbf19af5d68722b9f965c86a10287b3e4ac4e385444bea` |
| rgw | `sha256:a30a48981b857ad2241522aee7a0fba30ed8cbdc7bfdc311c9a6a8c2029f09b7` |
| mds | `sha256:76885d63b94ca90bb6988df49f135a5a567b4fb29c2991ce26fdf3ee487206b1` |

`messenger_secure`는 2 parent + 4 child의 정확한 6 RUN/PASS,
Go package 294.462초로 통과했다. Topology bridge fixture는 separate
public/cluster network, MDS/RGW fixture는 기본 bridge를 사용했고 host는
선택된 MON 포트를 사용했다. MON 1→3, MGR 1→2, OSD 2→3의
구성 변경·v2-only 주소·effective config와 RADOS 64 KiB retained/fresh bytes를
확인했다. CRC-only CLI는 두 network 모두 timeout 없이 native exit 13과
server allowed mode `[2]`로 거부됐고 fresh secure client가 다시 성공했다.
별도 fixture의 CephFS 106496-byte 파일과 S3 94208-byte object도 bridge/host에서
동일 bytes를 반환했다. MDS의 native 연결, CephFS client의 MON/MDS/OSD와
GID로 특정한 RGW→OSD 연결은 송수신 AES-GCM이었다. 외부 cleanup checker는
새 owned container/network/volume이 남지 않았음을 확인했다.

실행 receipt는 `artifacts/messenger-secure-native-v3/{report.json,native.log}`,
`artifacts/messenger-secure-cleanup-v3/{before.json,after.json}`에 보존했다.
첫 실행의 string boolean/anon_conns schema 실패와 두 번째 실행의
`unknown` mode 진단 실패도 `messenger-secure-native`와 `-v2` receipt에 보존했다.
첫 두 실행은 native 전체 성공으로 집계하지 않는다.

`messenger_secure_mix`도 2 parent + 4 child의 정확한 6 RUN/PASS,
Go package 441.829초로 통과했다. 각 case는 독립 FSID·CephX keyring·IPv4
bridge network의 cold pair를 만들며 cluster당 512 MiB OSD 하나와 replica/min_size
1을 사용한다. RBD 1 MiB와 CephFS 8 KiB의 initial/changed 두 checkpoint가
실제로 destination에 전달됐다. 인증 session ID로 특정한 전체 daemon 관측
40회 중 secure-side 관측 20회에서 MON/OSD 및 CephFS MDS가 양방향
AES-GCM임을 확인했고, default-side 관측 20회는 아래 표로 구분했다.
외부 cleanup checker도 새 owned resource가 남지 않았음을 확인했다.

| Mirror / source→destination | Secure cluster 연결 | Default cluster에서 관측한 연결 |
| --- | --- | --- |
| RBD / secure→default | MON·OSD secure/AES-GCM | MON secure, OSD CRC/PLAIN |
| RBD / default→secure | MON·OSD secure/AES-GCM | MON secure, OSD CRC/PLAIN |
| CephFS / secure→default | MON·OSD·MDS secure/AES-GCM | MON·OSD·MDS secure/AES-GCM |
| CephFS / default→secure | MON·OSD·MDS secure/AES-GCM | MON secure, OSD·MDS CRC/PLAIN |

Mixed 구성의 복제 성공을 모든 연결의 암호화로 해석하지 않는다. 특히 RBD의
remote librados context와 CephFS의 remote context는 서로 다른 설정 경로를
사용하며, default 서비스가 CRC를 허용하면 위와 같이 협상될 수 있다.
Mixed host network, secure↔secure pair와 IPv6의 새 실행은 미검증이다.

Receipt는 `artifacts/messenger-secure-mix-native-v2/{report.json,native.log}`,
`artifacts/messenger-secure-mix-cleanup-v2/{before.json,after.json}`에 보존했다.
첫 mixed 실행은 복제 bytes 확인 뒤 잘못된 `peer.entity_name` 필터로 실패했다.
이 결과와 native sessions/dump 비교는 `messenger-secure-mix-native` 및
`messenger-secure-mix-live-{identities,sessions}.json`에 보존하고 PASS로 합산하지 않는다.


Main push concurrency 변경은 기존 Python tooling 180개를 통과했다. PR은 번호,
main push는 ref, 수동 실행은 run ID로 구분한다. 새 main push가 같은 workflow의
이전 main 실행을 취소하며, 다른 PR이나 수동 실행을 취소하지 않는다.
[CI 선택과 실행 정책](TEST_TAGS.md)을 따른다.
