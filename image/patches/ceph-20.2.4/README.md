# Ceph 20.2.4 RGW native 검증 패치

Ceph client fixture의 unchanged native 검증에서 발견한 두 실패를 재현하고 수정 이미지를 검증하기 위한 패치입니다. 공개 Go module에는 C++ 또는 cgo 의존성을 추가하지 않습니다. 패키지로 빌드한 결과는 기존 [Debian 이미지 빌더](../../../docs/DEBIAN_IMAGE_AUTOMATION.md)에 입력할 수 있습니다.

패치 적용과 native/image 빌드는 선택적 실험입니다. 원본 pinned Quay가 기본 지원·필수 검증 기준이며, 아래 custom-image PASS를 기본 이미지 성공으로 표시하지 않습니다.

기준은 Ceph `v20.2.4`, commit `7f793731f1b39eb4f465e960113d2363c311b964`입니다. `source-manifest.json`은 원본 파일의 SHA256과 primary source URL, `patches.json`은 적용 순서와 각 패치의 SHA256을 고정합니다. 다른 버전에는 그대로 적용된다고 가정하지 않습니다.

1. `ceph-20.2.4-rgw-sync-pipe-selection.patch`: matching prefix 후보를 모두 검사해 numeric priority를 비교합니다. 동일 prefix·nested prefix·tag 선택·fallback·user/owner/class tuple을 실제 Ceph 타입으로 확인하는 9개 native gtest와 `unittest_rgw_bucket_sync_pipe_rules` target을 포함합니다.
2. `ceph-20.2.4-rgw-sync-user-impersonation.patch`: 인증 전에 system map에 보관된 `rgwx-perm-check-uid`를 `sys_get`으로 읽습니다. 기존 signature 및 system-user guard를 유지합니다.

Owned Go fixture에서 tag/owner/class와 same-tenant system/user의 import·bytes·checkpoint·제외·cleanup은 unpatched image의 bridge/host에서 통과했습니다. Numeric priority와 ordinary user의 source payload 거부는 실패했으며, 이를 우회하는 pipe ID 변경이나 느슨한 판정은 사용하지 않습니다. [실행 기록과 native 근거](../../../docs/RGW_SYNC_POLICY.md)를 참고합니다.

2026-10-04 실제 Ubuntu Noble ARM64 빌드에서 네 target과 selector gtest 9개가 통과했습니다. 같은 빌드의 RGW binary와 private library는 relocated DT_RPATH 및 `ldd -r` 검사를 통과했으며, 전역 Ceph library를 교체하지 않는 23개 Debian package를 생성했습니다. 최초 lld 실패와 GNU linker 재개 이력은 보존합니다. 수정 RGW role image의 smoke와 기존 Quay control/OSD/MDS를 조합한 네 translation subtest 및 account-root/cross-tenant 회귀가 bridge/host 모두 통과했습니다. 실제 bytes·native bucket identity/checkpoint·제외·deny/grant·owned cleanup을 확인한 선택적 G05/G07 실험 증거입니다. 원본 Quay의 두 실패를 수정하지 않은 채 통과했다고 주장하지 않습니다. 새 Noble-derived 다섯 role은 명시적 `TCMALLOC_STACKTRACE_METHOD=generic_fp` 환경으로 smoke를 통과했으며 역할별 cluster baseline도 통과했습니다. [재현 workflow와 검증 경계](../../native/README.md)를 확인합니다.

실제 검증에 사용한 순서는 다음과 같습니다.

1. 원본 파일 hash와 `patches.json`을 확인하고 순서대로 `patch --batch --forward --fuzz=0 -p1`을 적용합니다.
2. 같은 소스로 `radosgw`, `radosgw-admin`, 관련 private Ceph library와 native test를 빌드합니다. 현재 PoC에서 `lld`는 librados의 empty symbol-version 표현을 거부하여 GNU linker를 사용합니다.
3. 9개 selector gtest를 실제 실행하고 빌드한 binary/library의 동일 ABI 및 relocated loader closure를 확인합니다.
4. MON·OSD·MDS의 기존 library를 보존하면서 patched RGW process의 private library를 격리한 Debian package/image를 만듭니다.
5. [translation recipe](../../../internal/integration/rgw_sync_translation_integration_test.go)의 네 subtest를 bridge/host에서 그대로 실행합니다. Source 권한 거부 동안 destination absence와 checkpoint 진행, grant 후 새 exact bytes, priority winner/fallback, owned cleanup을 모두 확인합니다. 같은 인증 경로의 [account-root/cross-tenant recipe](../../../internal/integration/rgw_sync_accounts_integration_test.go)도 두 network variant에서 확인합니다.

Native `--version`은 원래 release commit을 표시합니다. 수정된 package payload SHA와 ordered patch manifest로 custom build를 식별해야 합니다. 원본 Ceph 코드의 라이선스 및 출처는 [pinned upstream COPYING](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/COPYING)에 있습니다.
