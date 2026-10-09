# RGW bucket notification recipe

Bucket notification을 쓰는 S3 client는 SNS 호환 topic API와 S3 notification
설정 API를 직접 호출한다. 이 fixture가 준비할 서버 조건은 실행 중인 gateway,
fresh user와 event를 받을 HTTP endpoint뿐이다. Ceph 20.2.4 기본 설정으로 충분하므로
별도 공개 API를 두지 않고, 기존 공개 API 조합과 실제 전달 결과로 검증한다.

## 구성 순서

1. `Run`과 `rgw.Start`로 gateway를 시작하고 `CreateUser`로 user를 만든다.
   S3 요청은 `S3Endpoint`와 `rgw.User.Credentials`로 서명한다.
2. Event를 받을 HTTP 서버를 `WithClient`로 연결한 컨테이너에서 실행한다.
   Bridge 모드에서는 그 컨테이너의 cluster network IP가, host 모드에서는
   `PublicAddress()`가 RGW에서 닿는 주소다.
3. SNS `CreateTopic`을 gateway endpoint에 `POST /`로 보낸다. 본문은
   `application/x-www-form-urlencoded`이고 `Attributes.entry.N.key/value`로
   `push-endpoint`를 지정한다. RGW는 SigV4 service 이름 `s3`로 서명한 요청을
   받는다. 응답의 `TopicArn`은 `arn:aws:sns:<zonegroup>::<name>` 형식이다.
4. Bucket에 `PUT ?notification`으로 `TopicConfiguration`을 저장한다. Event
   종류와 `Filter/S3Key/FilterRule` prefix 조건을 함께 지정할 수 있다.
5. 정리할 때는 `DELETE ?notification`(RGW 확장, 200 응답)과 SNS
   `DeleteTopic`을 호출한 뒤 object와 bucket을 지운다.

Topic 속성에 `persistent=true`를 주면 RGW가 event를 queue에 저장하고 비동기로
전달한다. `retry_sleep_duration`으로 재시도 간격(초)을 줄일 수 있다.

## 확인한 동작

[TestRGWBucketNotifications](../internal/integration/rgw_notifications_integration_test.go)는
bridge와 host network 각각 다음을 확인한다.

| 단계 | 조작 | 확인 |
| --- | --- | --- |
| 구성 | topic 두 개 생성, prefix가 다른 rule 두 개 저장 | native ARN, `GET ?notification`에 두 rule ID |
| direct | `in/a` 쓰기, `out/b` 쓰기, `in/a` 삭제 | 수신기가 `ObjectCreated:Put in/a`와 `ObjectRemoved:Delete in/a`를 받고 filter 밖의 `out/b`는 받지 않음 |
| persistent | 수신기 중지 후 `queued/c` 쓰기, 같은 포트로 수신기 재시작 | 쓰기가 10초 안에 성공, 중지 중에는 기록 없음, 재시작 뒤 `ObjectCreated:Put queued/c` 수신 |
| 정리 | `DELETE ?notification`, `DeleteTopic`, object·bucket 삭제 | rule 제거 확인, 모든 요청 성공 |

수신기를 `WithClient` 컨테이너에서 띄울 때는 그 컨테이너의 PID 1이 종료된 자식
프로세스를 회수하는지 확인한다. Init 없이 `sleep infinity`를 PID 1로 쓰면 중지한
수신기가 zombie로 남아 `kill -0`이 계속 성공한다. `ceph.WithIdleEntrypoint()`는
Docker init이 자식을 회수한다. 테스트는 두 경우 모두를 위해 종료 대기를
`/proc/<pid>/stat`의 상태로 판정한다. Zombie는 socket을 잡고 있지 않아 같은 포트로
다시 띄울 수 있다.

2026-10-09 macOS ARM64 Docker Desktop(Linux ARM64 VM, 메모리 4 GiB)에서 기본
digest 고정 Quay Ceph 20.2.4 이미지로 실행한 결과는 bridge/host 각 2개 단계
subtest 모두 PASS, 전체 138.84초였다. 공식 역할 이미지 조합과 CI 실행은 이
기록에 포함하지 않는다. CI에서는 `Ceph short`의 `rgw_notifications` batch로
실행한다.

```sh
CGO_ENABLED=0 go test -tags=integration,features -count=1 -v -timeout=30m -run '^TestRGWBucketNotifications$' ./internal/integration
```

[RGW bucket notifications](https://docs.ceph.com/en/tentacle/radosgw/notifications/)를
참고한다.
