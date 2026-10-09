# Native RGW TLS fixture

`rgw.Config.TLS`는 native Beast의 HTTPS listener를 HTTP와 함께 시작합니다. certificate chain과 matching private key를 PEM으로 전달합니다. 공개 module은 Go 표준 TLS validation과 container 파일·CLI만 사용합니다.

```go
gateway, err := rgw.Start(ctx, cluster, rgw.Config{
    Name: "secure",
    TLS: &rgw.TLSConfig{
        CertificatePEM: certificateChain,
        PrivateKeyPEM: privateKey,
    },
})
if err != nil { return err }
endpoint, err := gateway.S3SecureEndpoint(ctx)
if err != nil { return err }
_ = endpoint
```

bridge 모드는 container의 HTTP 7480·HTTPS 7481을 각각 Docker의 임의 host port로 매핑합니다. host 모드는 두 빈 port를 함께 예약한 뒤 native bind로 인계하고, 시작 충돌 시 같은 cluster의 기존 gateway를 건드리지 않고 재시도합니다. 여러 cluster·gateway가 같은 host를 사용해도 고정 공개 port를 요구하지 않습니다.

caller가 제공한 bytes는 복사하며 certificate와 key는 owned gateway의 `/tc/rgw-tls.pem`에 0600으로 전달합니다. descriptor formatting은 secret 내용을 표시하지 않습니다. 시작 검사는 native daemon이 socket을 소유하는지, 실제 listener가 설정한 certificate fingerprint를 반환하는지, HTTPS 응답이 준비됐는지 확인합니다.

consumer는 평소와 동일하게 CA와 DNS/IP SAN을 검증해야 합니다. `S3SecureEndpoint`는 검증을 끄거나 client trust store를 바꾸지 않습니다. Docker에서 전달하는 host 이름 또는 `WithHostNetwork`의 public address를 certificate SAN에 포함해야 합니다. TLS가 없는 gateway에서 이 메서드를 부르면 오류입니다.

`TestRGWNativeTLS`는 bridge·host에서 CA 검증을 수행하는 S3 client로 durable bytes를 읽고 쓰며, CA가 없는 client의 거부와 기존 HTTP endpoint의 동일 데이터, 서로 다른 port, owned resource 정리를 확인합니다. Ceph 20.2.4 Docker Linux ARM64에서 bridge 44.79초·host 44.69초로 모두 통과했습니다. native 로그는 ignored `artifacts/rgw-client-fixtures-and-cephfs-auth-final.log`에 있습니다.

[Ceph Beast frontend TLS options](https://docs.ceph.com/en/tentacle/radosgw/frontends/#beast).
