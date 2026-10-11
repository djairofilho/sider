//! Security component integration using independently encoded policies and TLS peers.

use std::sync::Arc;

use argon2::{
    Algorithm, Argon2, Params, Version,
    password_hash::{PasswordHasher, SaltString},
};
use bytes::Bytes;
use serde_json::{Value, json};
use sider::{
    command::Command,
    security::{
        Authentication, ClientPolicy, ClientTransport, InternalPolicy, InternalTransport, Role,
    },
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};
use tokio_rustls::{
    TlsConnector,
    rustls::{self, RootCertStore, pki_types::ServerName},
};

// Independent test servers otherwise multiply the 64 MiB Argon2 pools and make
// valid logins compete with fixture hashing for the fixed five-second deadline.
// Serialize only the credential-heavy cases; production limits remain unchanged.
static CREDENTIAL_WORK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn reply(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    args: &[&[u8]],
) -> sider::resp::Frame {
    let mut request = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        request.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        request.extend_from_slice(arg);
        request.extend_from_slice(b"\r\n");
    }
    stream.write_all(&request).await.unwrap();
    stream.flush().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut decoder = sider::resp::Decoder::new(sider::resp::RespLimits::default()).unwrap();
        let mut input = bytes::BytesMut::new();
        loop {
            if let Some(frame) = decoder.decode(&mut input).unwrap() {
                return frame;
            }
            assert!(input.len() < 16 * 1024, "bounded test reply");
            input.extend_from_slice(&[stream.read_u8().await.unwrap()]);
        }
    })
    .await
    .unwrap()
}

fn assert_hello(frame: sider::resp::Frame, protocol: u8) {
    use sider::resp::Frame;
    let fields = match frame {
        Frame::Map(fields) if protocol == 3 => fields,
        Frame::Array(Some(fields)) if protocol == 2 => fields,
        _ => panic!("HELLO reply has the negotiated aggregate type"),
    };
    assert_eq!(fields.len(), 14);
    assert!(fields.chunks_exact(2).any(|pair| pair
        == [
            Frame::Bulk(Some(Bytes::from_static(b"proto"))),
            Frame::Integer(i64::from(protocol))
        ]));
}

fn connector(certificates: &[&rcgen::Certificate]) -> TlsConnector {
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots.add(certificate.der().clone()).unwrap();
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

async fn tls_peer(
    connector: &TlsConnector,
    address: std::net::SocketAddr,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    connector
        .connect(
            ServerName::IpAddress(address.ip().into()),
            TcpStream::connect(address).await.unwrap(),
        )
        .await
        .unwrap()
}

async fn assert_closed(stream: &mut (impl AsyncRead + Unpin)) {
    let mut byte = [0];
    let result = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
        .await
        .expect("revoked stream closed before activation success");
    assert!(
        matches!(result, Ok(0) | Err(_)),
        "revoked stream cannot emit additional application bytes"
    );
}

#[tokio::test]
async fn security_reload_revalidates_queue_subscriptions_and_closes_removed_credentials_and_certificates()
 {
    let _credential_work = CREDENTIAL_WORK.lock().await;
    use sider::security::{ClientRuntime, Generation};
    let password = b"generation-valid-password";
    let replacement_password = b"generation-rotated-password";
    let mut value = policy(&verifier(password));
    value["users"][0]["commands"]
        .as_array_mut()
        .unwrap()
        .extend([json!("MULTI"), json!("EXEC"), json!("WATCH")]);
    value["users"][0]["read_keys"]["all"] = json!(true);
    value["users"][0]["publish_channels"]["all"] = json!(true);
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let generation = |number, value: &Value| {
        Generation::client_files(
            number,
            &serde_json::to_vec(value).unwrap(),
            certified.cert.pem().as_bytes(),
            certified.key_pair.serialize_pem().as_bytes(),
        )
        .unwrap()
    };
    let runtime = ClientRuntime::new(generation(1, &value)).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, shutdown) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(sider::server::serve_secure(
        listener,
        sider::ServerConfig::default(),
        async {
            let _ = shutdown.await;
        },
        runtime.client().await,
    ));
    let connector = connector(&[&certified.cert]);
    let mut queue = tls_peer(&connector, address).await;
    command(&mut queue, &[b"AUTH", b"Reader", password], b"+OK\r\n").await;
    let mut subscriber = tls_peer(&connector, address).await;
    assert_hello(
        reply(
            &mut subscriber,
            &[b"HELLO", b"3", b"AUTH", b"Reader", password],
        )
        .await,
        3,
    );
    command(&mut queue, &[b"SET", b"write", b"original"], b"+OK\r\n").await;
    command(
        &mut queue,
        &[b"INFO", b"unknown-selector-canary"],
        b"-ERR syntax error\r\n",
    )
    .await;
    assert!(matches!(
        reply(&mut subscriber, &[b"SUBSCRIBE", b"in"]).await,
        sider::resp::Frame::Push(_)
    ));
    command(&mut queue, &[b"WATCH", b"write"], b"+OK\r\n").await;
    command(&mut queue, &[b"MULTI"], b"+OK\r\n").await;
    command(
        &mut queue,
        &[b"SET", b"write", b"must-not-apply"],
        b"+QUEUED\r\n",
    )
    .await;
    let mut reduced = value.clone();
    reduced["users"][0]["write_keys"] = json!({"exact":[],"prefix":[]});
    reduced["users"][0]["subscribe_channels"] = json!({"exact":[],"prefix":[]});
    reduced["users"][0]["commands"]
        .as_array_mut()
        .unwrap()
        .retain(|command| command != "WATCH");
    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            runtime.activate(generation(2, &reduced))
        )
        .await
        .unwrap()
        .unwrap()
    );
    command(&mut queue, &[b"EXEC"], b"-NOPERM Permission denied\r\n").await;
    command(&mut queue, &[b"GET", b"write"], b"$8\r\noriginal\r\n").await;
    command(
        &mut queue,
        &[b"PUBLISH", b"in", b"must-not-deliver"],
        b":0\r\n",
    )
    .await;
    command(&mut subscriber, &[b"PING"], b"+PONG\r\n").await;
    assert!(!runtime.activate(generation(2, &reduced)).await.unwrap());
    assert!(runtime.activate(generation(2, &value)).await.is_err());
    command(
        &mut queue,
        &[b"SET", b"write", b"must-not-apply"],
        b"-NOPERM Permission denied\r\n",
    )
    .await;
    value["users"][0]["credentials"][0]["verifier"] = json!(verifier(replacement_password));
    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            runtime.activate(generation(3, &value))
        )
        .await
        .unwrap()
        .unwrap()
    );
    assert_closed(&mut queue).await;
    assert_closed(&mut subscriber).await;
    let mut fresh = tls_peer(&connector, address).await;
    command(
        &mut fresh,
        &[b"AUTH", b"Reader", password],
        b"-WRONGPASS Authentication failed\r\n",
    )
    .await;
    // Each new connection must complete its first successful authentication in
    // five seconds. Debug Argon2 verification of two sequential attempts can
    // exceed that deadline, so the valid replacement is tested on a fresh socket.
    drop(fresh);
    let mut fresh = tls_peer(&connector, address).await;
    command(
        &mut fresh,
        &[b"AUTH", b"Reader", replacement_password],
        b"+OK\r\n",
    )
    .await;
    let new_certificate = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let rotated = Generation::client_files(
        4,
        &serde_json::to_vec(&value).unwrap(),
        new_certificate.cert.pem().as_bytes(),
        new_certificate.key_pair.serialize_pem().as_bytes(),
    )
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), runtime.activate(rotated))
            .await
            .unwrap()
            .unwrap()
    );
    assert_closed(&mut fresh).await;
    assert!(
        connector
            .connect(
                ServerName::IpAddress(address.ip().into()),
                TcpStream::connect(address).await.unwrap()
            )
            .await
            .is_err()
    );
    let new_connector = self::connector(&[&new_certificate.cert]);
    let mut fresh = tls_peer(&new_connector, address).await;
    command(
        &mut fresh,
        &[b"AUTH", b"Reader", replacement_password],
        b"+OK\r\n",
    )
    .await;
    command(&mut fresh, &[b"GET", b"write"], b"$8\r\noriginal\r\n").await;
    stop.send(()).unwrap();
    drop(fresh);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn security_hello_auth_is_atomic_preserves_failed_identity_and_resets_subscriptions() {
    let _credential_work = CREDENTIAL_WORK.lock().await;
    use sider::resp::Frame;
    let password = b"hello-auth-valid-password";
    let mut value = policy(&verifier(password));
    value["users"][0]["commands"]
        .as_array_mut()
        .unwrap()
        .extend([json!("MULTI"), json!("EXEC")]);
    value["users"][0]["publish_channels"]["all"] = json!(true);
    let mut other = value["users"][0].clone();
    other["name"] = json!("Other");
    other["commands"] = json!(["PING"]);
    value["users"].as_array_mut().unwrap().push(other);
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let security = sider::security::ClientSecurity::new(
        parse(value).unwrap(),
        ClientTransport::new(
            certified.cert.pem().as_bytes(),
            certified.key_pair.serialize_pem().as_bytes(),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, shutdown) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(sider::server::serve_secure(
        listener,
        sider::ServerConfig::default(),
        async {
            let _ = shutdown.await;
        },
        security,
    ));
    let mut roots = RootCertStore::empty();
    roots.add(certified.cert.der().clone()).unwrap();
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let mut stream = TlsConnector::from(Arc::new(config))
        .connect(
            ServerName::IpAddress(std::net::IpAddr::from([127, 0, 0, 1]).into()),
            TcpStream::connect(address).await.unwrap(),
        )
        .await
        .unwrap();
    command(
        &mut stream,
        &[b"HELLO", b"3"],
        b"-NOAUTH Authentication required\r\n",
    )
    .await;
    // Syntax and unsupported negotiation do not spend the three-failure budget.
    for _ in 0..3 {
        command(
            &mut stream,
            &[b"HELLO", b"3", b"AUTH", b"Reader"],
            b"-ERR syntax error\r\n",
        )
        .await;
        command(
            &mut stream,
            &[b"HELLO", b"4", b"AUTH", b"Reader", password],
            b"-NOPROTO unsupported protocol version\r\n",
        )
        .await;
    }
    assert_hello(
        reply(&mut stream, &[b"HELLO", b"3", b"AUTH", b"Reader", password]).await,
        3,
    );
    assert_eq!(
        reply(&mut stream, &[b"GET", b"read:missing"]).await,
        Frame::Null
    );
    command(
        &mut stream,
        &[b"HELLO", b"2", b"AUTH", b"Other", b"wrong-password-canary"],
        b"-WRONGPASS Authentication failed\r\n",
    )
    .await;
    // Both the prior RESP3 protocol and Reader's authority survive the failed replacement.
    assert_eq!(
        reply(&mut stream, &[b"GET", b"read:missing"]).await,
        Frame::Null
    );
    assert!(matches!(
        reply(&mut stream, &[b"SUBSCRIBE", b"in"]).await,
        Frame::Push(_)
    ));
    assert_hello(
        reply(&mut stream, &[b"HELLO", b"3", b"AUTH", b"Reader", password]).await,
        3,
    );
    command(
        &mut stream,
        &[b"PUBLISH", b"in", b"must-not-deliver"],
        b":0\r\n",
    )
    .await;
    command(&mut stream, &[b"MULTI"], b"+OK\r\n").await;
    command(
        &mut stream,
        &[b"HELLO", b"2", b"AUTH", b"Other", password],
        b"-ERR authentication/protocol control inside MULTI is not allowed\r\n",
    )
    .await;
    command(
        &mut stream,
        &[b"EXEC"],
        b"-EXECABORT Transaction discarded because of previous errors.\r\n",
    )
    .await;
    assert_eq!(
        reply(&mut stream, &[b"GET", b"read:missing"]).await,
        Frame::Null
    );
    assert_hello(
        reply(&mut stream, &[b"HELLO", b"2", b"AUTH", b"Other", password]).await,
        2,
    );
    command(
        &mut stream,
        &[b"GET", b"read:missing"],
        b"-NOPERM Permission denied\r\n",
    )
    .await;
    assert_hello(reply(&mut stream, &[b"HELLO", b"3"]).await, 3);
    command(&mut stream, &[b"PING"], b"+PONG\r\n").await;
    stop.send(()).unwrap();
    drop(stream);
    server.await.unwrap().unwrap();
}

fn verifier(password: &[u8]) -> String {
    let salt = SaltString::encode_b64(b"test-salt-16byte").unwrap();
    Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(65536, 3, 1, Some(32)).unwrap(),
    )
    .hash_password(password, &salt)
    .unwrap()
    .to_string()
}

async fn command(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    args: &[&[u8]],
    expected: &[u8],
) {
    let mut request = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        request.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        request.extend_from_slice(arg);
        request.extend_from_slice(b"\r\n");
    }
    stream.write_all(&request).await.unwrap();
    stream.flush().await.unwrap();
    let mut response = vec![0; expected.len()];
    tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response, expected);
}

#[tokio::test]
async fn security_server_auth_acl_and_failure_limits_precede_dataset_and_subscription_changes() {
    let _credential_work = CREDENTIAL_WORK.lock().await;
    let password = b"security-server-password";
    let mut value = policy(&verifier(password));
    value["users"][0]["commands"]
        .as_array_mut()
        .unwrap()
        .extend([json!("MULTI"), json!("EXEC")]);
    value["users"][0]["read_keys"]["exact"]
        .as_array_mut()
        .unwrap()
        .push(json!("d3JpdGU="));
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let transport = ClientTransport::new(
        certified.cert.pem().as_bytes(),
        certified.key_pair.serialize_pem().as_bytes(),
    )
    .unwrap();
    let security = sider::security::ClientSecurity::new(parse(value).unwrap(), transport);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, shutdown) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(sider::server::serve_secure(
        listener,
        sider::ServerConfig::default(),
        async {
            let _ = shutdown.await;
        },
        security,
    ));
    let mut roots = RootCertStore::empty();
    roots.add(certified.cert.der().clone()).unwrap();
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let name = || ServerName::IpAddress(std::net::IpAddr::from([127, 0, 0, 1]).into());
    let mut stream = connector
        .connect(name(), TcpStream::connect(address).await.unwrap())
        .await
        .unwrap();
    command(
        &mut stream,
        &[b"PING"],
        b"-NOAUTH Authentication required\r\n",
    )
    .await;
    command(&mut stream, &[b"AUTH", b"Reader", password], b"+OK\r\n").await;
    command(&mut stream, &[b"SET", b"write", b"original"], b"+OK\r\n").await;
    command(
        &mut stream,
        &[b"MSET", b"write", b"changed", b"denied", b"secret"],
        b"-NOPERM Permission denied\r\n",
    )
    .await;
    command(&mut stream, &[b"GET", b"write"], b"$8\r\noriginal\r\n").await;
    command(
        &mut stream,
        &[b"SUBSCRIBE", b"in", b"out"],
        b"-NOPERM Permission denied\r\n",
    )
    .await;
    command(&mut stream, &[b"PUBLISH", b"out", b"secret"], b":0\r\n").await;
    command(
        &mut stream,
        &[b"INFO", b"config"],
        b"-NOPERM Permission denied\r\n",
    )
    .await;
    command(&mut stream, &[b"MULTI"], b"+OK\r\n").await;
    command(&mut stream, &[b"SET", b"write", b"queued"], b"+QUEUED\r\n").await;
    command(
        &mut stream,
        &[b"SET", b"denied", b"secret"],
        b"-NOPERM Permission denied\r\n",
    )
    .await;
    command(
        &mut stream,
        &[b"EXEC"],
        b"-EXECABORT Transaction discarded because of previous errors.\r\n",
    )
    .await;
    command(&mut stream, &[b"GET", b"write"], b"$8\r\noriginal\r\n").await;
    let mut failed = connector
        .connect(name(), TcpStream::connect(address).await.unwrap())
        .await
        .unwrap();
    for _ in 0..3 {
        command(
            &mut failed,
            &[b"AUTH", b"Reader", b"short"],
            b"-WRONGPASS Authentication failed\r\n",
        )
        .await;
    }
    let mut byte = [0];
    // TLS close without a close_notify may surface UnexpectedEof; no further RESP is returned.
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), failed.read(&mut byte)).await,
        Ok(Ok(0)) | Ok(Err(_))
    ));
    stop.send(()).unwrap();
    drop(stream);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn security_internal_role_denial_happens_before_payload_read() {
    use sider::replication::protocol::{self, Message};
    let frame = protocol::encode(&Message::Promote, protocol::Limits::default()).unwrap();
    let (mut sender, mut receiver) = tokio::io::duplex(64);
    sender
        .write_all(&frame[..protocol::HEADER_BYTES])
        .await
        .unwrap();
    let result = protocol::read_authorized(
        &mut receiver,
        protocol::Limits::default(),
        Duration::from_secs(1),
        |_| false,
    )
    .await;
    assert!(matches!(
        result,
        Err(protocol::Error::Invalid("permission denied"))
    ));
    // The peer intentionally withholds payload. Denial cannot await it or consume it.
}
fn policy(verifier: &str) -> Value {
    json!({"version":1,"users":[{
        "name":"Reader", "credentials":[{"verifier":verifier,"not_before":"2026-01-01T00:00:00Z","expires_at":"2099-01-01T00:00:00Z"}],
        "commands":["GET","MGET","MSET","WATCH","SET","SUBSCRIBE","PUBLISH","INFO","PING"],
        "read_keys":{"exact":["AP8="],"prefix":["cmVhZDo="]},
        "write_keys":{"exact":["d3JpdGU="],"prefix":[]},
        "publish_channels":{"exact":["b3V0"],"prefix":[]},
        "subscribe_channels":{"exact":["aW4="],"prefix":[]},
        "info_sections":["server"]
    }]})
}
fn parse(value: Value) -> Result<ClientPolicy, sider::security::Error> {
    ClientPolicy::parse(&serde_json::to_vec(&value).unwrap())
}

#[test]
fn security_policy_rejects_ambiguous_or_noncanonical_configuration() {
    // No Argon2 calculation is needed to validate the PHC format.
    let valid = "$argon2id$v=19$m=65536,t=3,p=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let original = policy(valid);
    assert!(parse(original.clone()).is_ok());
    for mutation in [
        ("unknown", json!(true)),
        ("name", json!("bad name")),
        ("commands", json!(["get"])),
        ("commands", json!(["AUTH"])),
        ("read_keys", json!({"exact":[],"prefix":[""]})),
        ("read_keys", json!({"exact":["AP8"],"prefix":[]})),
        ("info_sections", json!(["secrets"])),
    ] {
        let mut value = original.clone();
        value["users"][0][mutation.0] = mutation.1;
        assert!(parse(value).is_err(), "{}", mutation.0);
    }
    let mut duplicate = original.clone();
    duplicate["users"]
        .as_array_mut()
        .unwrap()
        .push(original["users"][0].clone());
    assert!(parse(duplicate).is_err());
    let mut overlap = original.clone();
    overlap["users"][0]["credentials"]
        .as_array_mut()
        .unwrap()
        .push(original["users"][0]["credentials"][0].clone());
    assert!(parse(overlap).is_err());
    let mut invalid = original;
    invalid["users"][0]["credentials"][0]["verifier"] =
        json!(valid.replace("m=65536,t=3,p=1", "t=3,m=65536,p=1"));
    assert!(parse(invalid).is_err());
    assert!(ClientPolicy::parse(br#"{"version":1,"version":1,"users":[]}"#).is_err());
}

#[test]
fn security_acl_checks_all_binary_keys_and_channels_and_info_sections() {
    let policy = parse(policy("$argon2id$v=19$m=65536,t=3,p=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")).unwrap();
    assert!(
        policy
            .authorize(
                "Reader",
                b"get",
                &Command::Get {
                    key: Bytes::from_static(b"\0\xff")
                }
            )
            .is_ok()
    );
    assert!(
        policy
            .authorize(
                "Reader",
                b"MGET",
                &Command::MGet {
                    keys: vec![
                        Bytes::from_static(b"read:one"),
                        Bytes::from_static(b"denied")
                    ]
                }
            )
            .is_err()
    );
    assert!(
        policy
            .authorize(
                "Reader",
                b"MSET",
                &Command::MSet {
                    entries: vec![(Bytes::from_static(b"write"), Bytes::from_static(b"denied"))]
                }
            )
            .is_ok()
    );
    assert!(
        policy
            .authorize(
                "Reader",
                b"SET",
                &Command::Set {
                    key: Bytes::from_static(b"read:one"),
                    value: Bytes::new()
                }
            )
            .is_err()
    );
    assert!(
        policy
            .authorize(
                "Reader",
                b"SUBSCRIBE",
                &Command::Subscribe {
                    channels: vec![Bytes::from_static(b"in"), Bytes::from_static(b"out")]
                }
            )
            .is_err()
    );
    assert!(
        policy
            .authorize(
                "Reader",
                b"PUBLISH",
                &Command::Publish {
                    channel: Bytes::from_static(b"out"),
                    message: Bytes::new()
                }
            )
            .is_ok()
    );
    assert!(
        policy
            .authorize(
                "Reader",
                b"INFO",
                &Command::Info(sider::command::InfoSections::ALL)
            )
            .is_err()
    );
    assert!(
        policy
            .authorize(
                "Reader",
                b"INFO",
                &Command::Info(sider::command::InfoSections::from_names([
                    b"server".as_slice()
                ]))
            )
            .is_ok()
    );
    assert!(
        policy
            .authorize("reader", b"PING", &Command::Ping(None))
            .is_err()
    );
}

#[test]
fn security_internal_roles_admit_distinct_holder_instance_and_exact_roles() {
    let value = json!({"version":1,"instance_id":"primary-a","principals":[
        {"uri_san":"spiffe://sider/replica-b/sync","roles":["replica"],"upstream_instance":"primary-a"},
        {"uri_san":"spiffe://sider/backup-c/export","roles":["backup"]}
    ]});
    let policy = InternalPolicy::parse(&serde_json::to_vec(&value).unwrap(), false).unwrap();
    assert!(
        policy
            .authorize("spiffe://sider/replica-b/sync", Role::Replica)
            .is_ok()
    );
    assert!(
        policy
            .authorize("spiffe://sider/replica-b/sync", Role::Promotion)
            .is_err()
    );
    assert!(
        policy
            .authorize("spiffe://sider/backup-c/export", Role::Backup)
            .is_ok()
    );
    assert!(
        policy
            .authorize("spiffe://sider/backup-c/export", Role::Monitor)
            .is_err()
    );
    assert!(policy.own_identity("spiffe://sider/primary-a/server"));
    assert!(!policy.own_identity("spiffe://sider/primary-a/server/extra"));
    let mut wrong = value;
    wrong["principals"][0]["upstream_instance"] = json!("replica-b");
    assert!(InternalPolicy::parse(&serde_json::to_vec(&wrong).unwrap(), false).is_err());
}

#[tokio::test]
async fn security_authentication_proof_is_exact_case_sensitive_and_revocable() {
    let _credential_work = CREDENTIAL_WORK.lock().await;
    let password = b"security-canary-password";
    let hash = verifier(password);
    let value = policy(&hash);
    let original = Arc::new(parse(value.clone()).unwrap());
    let authentication = Authentication::default();
    let proof = authentication
        .verify(
            original.clone(),
            b"Reader",
            password,
            Instant::now() + Duration::from_secs(30),
        )
        .await
        .unwrap();
    assert_eq!(proof.username(), "Reader");
    assert!(proof.valid(&original, time::OffsetDateTime::now_utc()));
    assert!(
        authentication
            .verify(
                original.clone(),
                b"reader",
                password,
                Instant::now() + Duration::from_secs(30)
            )
            .await
            .is_err()
    );
    assert!(
        authentication
            .verify(
                original.clone(),
                b"Reader",
                b"wrong-password-canary",
                Instant::now() + Duration::from_secs(30)
            )
            .await
            .is_err()
    );
    assert!(
        authentication
            .verify(original, b"Reader", password, Instant::now())
            .await
            .is_err()
    );
    let mut expired = value;
    expired["users"][0]["credentials"][0]["expires_at"] = json!("2026-01-02T00:00:00Z");
    assert!(!proof.valid(&parse(expired).unwrap(), time::OffsetDateTime::now_utc()));
}

#[tokio::test]
async fn security_client_tls_encrypts_bytes_and_rejects_plaintext_untrusted_and_wrong_ip() {
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let transport = ClientTransport::new(
        certified.cert.pem().as_bytes(),
        certified.key_pair.serialize_pem().as_bytes(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for valid in [false, false, false, true] {
            let (socket, _) = listener.accept().await.unwrap();
            let result = transport.accept(socket).await;
            if valid {
                let mut stream = result.unwrap();
                let mut bytes = [0; 4];
                stream.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"PING");
                stream.write_all(b"PONG").await.unwrap();
                stream.flush().await.unwrap();
            } else {
                assert!(result.is_err());
            }
        }
    });
    let mut plaintext = TcpStream::connect(address).await.unwrap();
    plaintext.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    plaintext.shutdown().await.unwrap();
    let make_connector = |trust: bool| {
        let mut roots = RootCertStore::empty();
        if trust {
            roots.add(certified.cert.der().clone()).unwrap();
        }
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        TlsConnector::from(Arc::new(config))
    };
    let ip = |ip| ServerName::IpAddress(std::net::IpAddr::from(ip).into());
    assert!(
        make_connector(false)
            .connect(
                ip([127, 0, 0, 1]),
                TcpStream::connect(address).await.unwrap()
            )
            .await
            .is_err()
    );
    assert!(
        make_connector(true)
            .connect(
                ip([127, 0, 0, 2]),
                TcpStream::connect(address).await.unwrap()
            )
            .await
            .is_err()
    );
    let mut socket = make_connector(true)
        .connect(
            ip([127, 0, 0, 1]),
            TcpStream::connect(address).await.unwrap(),
        )
        .await
        .unwrap();
    socket.write_all(b"PING").await.unwrap();
    socket.flush().await.unwrap();
    let mut response = [0; 4];
    socket.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"PONG");
    server.await.unwrap();
}

struct InternalCertificates {
    issuer: rcgen::Certificate,
    issuer_key: rcgen::KeyPair,
}
impl InternalCertificates {
    fn new() -> Self {
        let issuer_key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let issuer = params.self_signed(&issuer_key).unwrap();
        Self { issuer, issuer_key }
    }
    fn leaf(&self, uri: &str, serial: u64, client_only: bool) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
        params.serial_number = Some(rcgen::SerialNumber::from(serial));
        params
            .subject_alt_names
            .push(rcgen::SanType::URI(uri.try_into().unwrap()));
        params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        if !client_only {
            params
                .extended_key_usages
                .push(rcgen::ExtendedKeyUsagePurpose::ServerAuth);
        }
        let cert = params
            .signed_by(&key, &self.issuer, &self.issuer_key)
            .unwrap();
        (cert.pem(), key.serialize_pem())
    }
    fn crl(&self, revoked: Option<u64>, expired: bool) -> String {
        let now = time::OffsetDateTime::now_utc();
        rcgen::CertificateRevocationListParams {
            this_update: now - time::Duration::hours(2),
            next_update: now + time::Duration::hours(if expired { -1 } else { 24 }),
            crl_number: rcgen::SerialNumber::from(1_u64),
            issuing_distribution_point: None,
            revoked_certs: revoked
                .into_iter()
                .map(|serial| rcgen::RevokedCertParams {
                    serial_number: rcgen::SerialNumber::from(serial),
                    revocation_time: now - time::Duration::hours(1),
                    reason_code: Some(rcgen::RevocationReason::KeyCompromise),
                    invalidity_date: None,
                })
                .collect(),
            key_identifier_method: rcgen::KeyIdMethod::Sha256,
        }
        .signed_by(&self.issuer, &self.issuer_key)
        .unwrap()
        .pem()
        .unwrap()
    }
    fn transport(
        &self,
        leaf: &(String, String),
        crl: &str,
        tool: bool,
        expected: &str,
    ) -> Result<InternalTransport, sider::security::Error> {
        let value = if tool {
            json!({"version":1,"instance_id":"tool-b","principals":[],"upstream_principal":expected})
        } else {
            json!({"version":1,"instance_id":"primary-a","principals":[{"uri_san":"spiffe://sider/tool-b/monitor","roles":["monitor"]}]})
        };
        let policy =
            Arc::new(InternalPolicy::parse(&serde_json::to_vec(&value).unwrap(), tool).unwrap());
        let build = if tool {
            InternalTransport::tool
        } else {
            InternalTransport::new
        };
        build(
            leaf.0.as_bytes(),
            leaf.1.as_bytes(),
            self.issuer.pem().as_bytes(),
            crl.as_bytes(),
            policy,
        )
    }
}

type InternalStream = Box<dyn sider::security::NetworkStream>;
type OutgoingInternal = Result<InternalStream, sider::security::Error>;
type IncomingInternal = Result<(InternalStream, String), sider::security::Error>;

async fn internal_pair(
    server: InternalTransport,
    client: &InternalTransport,
) -> (OutgoingInternal, IncomingInternal) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        server.accept(socket).await
    });
    let outgoing = client.connect(address).await;
    let incoming = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    (outgoing, incoming)
}

#[tokio::test]
async fn security_internal_mtls_accepts_distinct_holders_and_rejects_wrong_exact_uri_and_revocation()
 {
    let certificates = InternalCertificates::new();
    let primary = certificates.leaf("spiffe://sider/primary-a/server", 1, false);
    // A CLI certificate requires clientAuth only; remote holder differs from receiver.
    let monitor = certificates.leaf("spiffe://sider/tool-b/monitor", 2, true);
    let crl = certificates.crl(None, false);
    let server = || certificates.transport(&primary, &crl, false, "").unwrap();
    let client = certificates
        .transport(&monitor, &crl, true, "spiffe://sider/primary-a/server")
        .unwrap();
    let (outgoing, incoming) = internal_pair(server(), &client).await;
    let mut outgoing = outgoing.unwrap();
    let (mut incoming, identity) = incoming.unwrap();
    assert_eq!(identity, "spiffe://sider/tool-b/monitor");
    outgoing.write_all(b"PING").await.unwrap();
    outgoing.flush().await.unwrap();
    let mut bytes = [0; 4];
    incoming.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"PING");
    drop(outgoing);
    drop(incoming);

    let wrong_expected = certificates
        .transport(&monitor, &crl, true, "spiffe://sider/primary-a/other")
        .unwrap();
    let (outgoing, incoming) = internal_pair(server(), &wrong_expected).await;
    assert!(outgoing.is_err());
    drop(incoming);
    let unlisted = certificates.leaf("spiffe://sider/tool-b/other", 3, true);
    let unlisted = certificates
        .transport(&unlisted, &crl, true, "spiffe://sider/primary-a/server")
        .unwrap();
    let (outgoing, incoming) = internal_pair(server(), &unlisted).await;
    assert!(incoming.is_err());
    drop(outgoing);

    let revoked = certificates.crl(Some(2), false);
    let revoked_server = certificates
        .transport(&primary, &revoked, false, "")
        .unwrap();
    let (outgoing, incoming) = internal_pair(revoked_server, &client).await;
    assert!(incoming.is_err());
    drop(outgoing);
    assert!(
        certificates
            .transport(&primary, &certificates.crl(None, true), false, "")
            .is_err()
    );
    assert!(certificates.transport(&primary, "", false, "").is_err());
}
