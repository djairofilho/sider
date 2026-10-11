//! Real mTLS boundaries for internal request roles, directions, and expiry.

#[path = "common/process.rs"]
mod startup_process;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use serde_json::{Value, json};
use sider::{
    replication::{
        Cursor,
        protocol::{self, Hello, Message},
    },
    security::{
        InternalPeer, InternalPhase, InternalPolicy, InternalTransport, NetworkStream,
        authorize_internal_request,
    },
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};

const DEADLINE: Duration = Duration::from_secs(5);
const SERVER: &str = "spiffe://sider/primary-a/server";
const PEER: &str = "spiffe://sider/holder-b/operator";

struct Certificates {
    issuer: rcgen::Certificate,
    key: rcgen::KeyPair,
}
impl Certificates {
    fn new() -> Self {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let issuer = params.self_signed(&key).unwrap();
        Self { issuer, key }
    }
    fn leaf(
        &self,
        uri: &str,
        serial: u64,
        expiry: Option<time::OffsetDateTime>,
    ) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
        params.serial_number = Some(serial.into());
        params
            .subject_alt_names
            .push(rcgen::SanType::URI(uri.try_into().unwrap()));
        params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        ];
        if let Some(expiry) = expiry {
            params.not_after = expiry;
        }
        let cert = params.signed_by(&key, &self.issuer, &self.key).unwrap();
        (cert.pem(), key.serialize_pem())
    }
    fn crl(&self, until: time::OffsetDateTime, revoked: Option<u64>) -> String {
        signed_crl(&self.issuer, &self.key, until, revoked)
    }
    fn transport(
        &self,
        leaf: &(String, String),
        crl: &str,
        policy: Arc<InternalPolicy>,
        tool: bool,
    ) -> InternalTransport {
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
        .unwrap()
    }
}
fn signed_crl(
    issuer: &rcgen::Certificate,
    key: &rcgen::KeyPair,
    until: time::OffsetDateTime,
    revoked: Option<u64>,
) -> String {
    let now = time::OffsetDateTime::now_utc();
    rcgen::CertificateRevocationListParams {
        this_update: now - time::Duration::hours(1),
        next_update: until,
        crl_number: 1_u64.into(),
        issuing_distribution_point: None,
        revoked_certs: revoked
            .into_iter()
            .map(|serial| rcgen::RevokedCertParams {
                serial_number: serial.into(),
                revocation_time: now - time::Duration::minutes(1),
                reason_code: Some(rcgen::RevocationReason::KeyCompromise),
                invalidity_date: None,
            })
            .collect(),
        key_identifier_method: rcgen::KeyIdMethod::Sha256,
    }
    .signed_by(issuer, key)
    .unwrap()
    .pem()
    .unwrap()
}
fn policy(roles: &[&str]) -> Arc<InternalPolicy> {
    let mut principal = json!({"uri_san":PEER,"roles":roles});
    if roles.contains(&"replica") {
        principal["upstream_instance"] = json!("primary-a");
    }
    parse(
        json!({"version":1,"instance_id":"primary-a","principals":[principal]}),
        false,
    )
}
fn parse(value: Value, outgoing: bool) -> Arc<InternalPolicy> {
    Arc::new(InternalPolicy::parse(&serde_json::to_vec(&value).unwrap(), outgoing).unwrap())
}
fn tool_policy(expected: &str) -> Arc<InternalPolicy> {
    parse(
        json!({"version":1,"instance_id":"holder-b","principals":[],"upstream_principal":expected}),
        true,
    )
}
type Stream = Box<dyn NetworkStream>;
async fn pair(
    server: InternalTransport,
    client: &InternalTransport,
) -> ((Stream, InternalPeer), (Stream, InternalPeer), SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        server
            .accept_with_peer(listener.accept().await.unwrap().0)
            .await
            .unwrap()
    });
    let outgoing = client.connect_with_peer(address).await.unwrap();
    let incoming = timeout(DEADLINE, task).await.unwrap().unwrap();
    (outgoing, incoming, address)
}

#[tokio::test]
async fn security_internal_intermediate_chain_requires_each_issuer_crl() {
    async fn admitted(
        server_leaf: &(String, String),
        client_leaf: &(String, String),
        root: &str,
        crls: &str,
    ) -> bool {
        let Ok(server) = InternalTransport::new(
            server_leaf.0.as_bytes(),
            server_leaf.1.as_bytes(),
            root.as_bytes(),
            crls.as_bytes(),
            policy(&["monitor"]),
        ) else {
            return false;
        };
        let Ok(client) = InternalTransport::tool(
            client_leaf.0.as_bytes(),
            client_leaf.1.as_bytes(),
            root.as_bytes(),
            crls.as_bytes(),
            tool_policy(SERVER),
        ) else {
            return false;
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = async move {
            let (socket, _) = listener.accept().await.unwrap();
            server.accept_with_peer(socket).await
        };
        let (incoming, outgoing) = tokio::join!(
            timeout(DEADLINE, incoming),
            timeout(DEADLINE, client.connect_with_peer(address))
        );
        matches!(incoming, Ok(Ok(_))) && matches!(outgoing, Ok(Ok(_)))
    }

    let root_key = rcgen::KeyPair::generate().unwrap();
    let mut root_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    root_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    root_params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::CrlSign,
    ];
    root_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "sider test root");
    let root = root_params.self_signed(&root_key).unwrap();

    let intermediate_key = rcgen::KeyPair::generate().unwrap();
    let mut intermediate_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    intermediate_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(0));
    intermediate_params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::CrlSign,
    ];
    intermediate_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "sider test intermediate");
    intermediate_params.serial_number = Some(3_u64.into());
    let intermediate = intermediate_params
        .signed_by(&intermediate_key, &root, &root_key)
        .unwrap();

    let leaf = |uri: &str, serial: u64| {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
        params.serial_number = Some(serial.into());
        params
            .subject_alt_names
            .push(rcgen::SanType::URI(uri.try_into().unwrap()));
        params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let certificate = params
            .signed_by(&key, &intermediate, &intermediate_key)
            .unwrap();
        (
            format!("{}{}", certificate.pem(), intermediate.pem()),
            key.serialize_pem(),
        )
    };
    let server_leaf = leaf(SERVER, 1);
    let client_leaf = leaf(PEER, 2);
    let until = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
    let root_crl = signed_crl(&root, &root_key, until, None);
    let intermediate_crl = signed_crl(&intermediate, &intermediate_key, until, None);
    let full = format!("{root_crl}{intermediate_crl}");
    assert!(admitted(&server_leaf, &client_leaf, &root.pem(), &full).await);
    assert!(!admitted(&server_leaf, &client_leaf, &root.pem(), &root_crl).await);
    assert!(!admitted(&server_leaf, &client_leaf, &root.pem(), &intermediate_crl).await);

    let revoked_leaf = signed_crl(&intermediate, &intermediate_key, until, Some(2));
    assert!(
        !admitted(
            &server_leaf,
            &client_leaf,
            &root.pem(),
            &format!("{root_crl}{revoked_leaf}")
        )
        .await
    );
    let revoked_intermediate = signed_crl(&root, &root_key, until, Some(3));
    assert!(
        !admitted(
            &server_leaf,
            &client_leaf,
            &root.pem(),
            &format!("{revoked_intermediate}{intermediate_crl}")
        )
        .await
    );
    let expired_intermediate = signed_crl(
        &intermediate,
        &intermediate_key,
        time::OffsetDateTime::now_utc() - time::Duration::minutes(1),
        None,
    );
    assert!(
        !admitted(
            &server_leaf,
            &client_leaf,
            &root.pem(),
            &format!("{root_crl}{expired_intermediate}")
        )
        .await
    );
}

fn runtime_generation(
    number: u64,
    certs: &Certificates,
    leaf: &(String, String),
    crl: &str,
    policy: Value,
    outgoing: bool,
) -> sider::security::Generation {
    sider::security::Generation::server_files(
        number,
        [
            br#"{"version":1,"users":[]}"#,
            leaf.0.as_bytes(),
            leaf.1.as_bytes(),
        ],
        [
            serde_json::to_vec(&policy).unwrap().as_slice(),
            leaf.0.as_bytes(),
            leaf.1.as_bytes(),
            certs.issuer.pem().as_bytes(),
            crl.as_bytes(),
        ],
        outgoing,
    )
    .unwrap()
}
fn runtime_policy(roles: &[&str]) -> Value {
    let mut principal = json!({"uri_san":PEER,"roles":roles});
    if roles.contains(&"replica") {
        principal["upstream_instance"] = json!("primary-a");
    }
    json!({"version":1,"instance_id":"primary-a", "principals":[principal]})
}
fn raw_connector(
    certs: &Certificates,
    identity: Option<&(String, String)>,
) -> tokio_rustls::TlsConnector {
    use tokio_rustls::rustls;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certs.issuer.der().clone()).unwrap();
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots);
    let config = if let Some(leaf) = identity {
        let chain = rustls_pemfile::certs(&mut std::io::Cursor::new(leaf.0.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(leaf.1.as_bytes()))
            .unwrap()
            .unwrap();
        builder.with_client_auth_cert(chain, key).unwrap()
    } else {
        builder.with_no_client_auth()
    };
    tokio_rustls::TlsConnector::from(Arc::new(config))
}
async fn raw_connect(connector: &tokio_rustls::TlsConnector, address: SocketAddr) -> Stream {
    Box::new(
        connector
            .connect(
                tokio_rustls::rustls::pki_types::ServerName::IpAddress(address.ip().into()),
                tokio::net::TcpStream::connect(address).await.unwrap(),
            )
            .await
            .unwrap(),
    )
}
async fn registered_pair(
    server: sider::security::InternalRuntime,
    client: &InternalTransport,
) -> ((Stream, InternalPeer), (Stream, InternalPeer)) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        server
            .accept_with_peer(listener.accept().await.unwrap().0)
            .await
            .unwrap()
    });
    let outgoing = client.connect_with_peer(address).await.unwrap();
    (outgoing, timeout(DEADLINE, task).await.unwrap().unwrap())
}
async fn physically_closed(stream: &mut Stream) {
    let result = timeout(Duration::from_secs(2), stream.read_u8())
        .await
        .expect("registry closed the TLS/TCP socket before activation completed");
    assert!(result.is_err(), "no post-revocation application byte");
}

#[tokio::test]
async fn security_runtime_internal_close_ack_waits_short_lease_and_revokes_retained_proof() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let generation = |number, roles: &[&str]| {
        runtime_generation(number, &certs, &primary, &crl, runtime_policy(roles), false)
    };
    let runtime =
        sider::security::GenerationRuntime::new(generation(1, &["monitor", "backup"])).unwrap();
    let internal = runtime.internal().await.unwrap();
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let ((mut outgoing, _), (mut incoming, peer)) =
        registered_pair(internal.clone(), &client).await;
    let lease = internal.lease().await;
    lease.authorize_request(&peer, 11).unwrap();
    let activation = runtime.clone();
    let replacement = generation(2, &["monitor"]);
    let mut activation = tokio::spawn(async move { activation.activate(replacement).await });
    assert!(
        timeout(Duration::from_millis(50), &mut activation)
            .await
            .is_err()
    );
    assert!(peer.current());
    drop(lease);
    assert!(
        timeout(DEADLINE, activation)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    );
    assert_eq!(runtime.number().await, 2);
    // Removing any original role closes the session, even if its current request role remains.
    assert!(!peer.current());
    physically_closed(&mut outgoing).await;
    assert_eq!(
        incoming.read_u8().await.unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    runtime
        .activate(generation(3, &["monitor", "backup"]))
        .await
        .unwrap();
    assert!(internal.lease().await.authorize_request(&peer, 11).is_err());
    // The retained admission handle resolves the active generation on a new handshake.
    let ((mut outgoing, _), (mut incoming, peer)) = registered_pair(internal, &client).await;
    assert!(peer.current());
    outgoing.write_u8(42).await.unwrap();
    outgoing.flush().await.unwrap();
    assert_eq!(
        timeout(DEADLINE, incoming.read_u8())
            .await
            .unwrap()
            .unwrap(),
        42
    );
}

#[tokio::test]
async fn security_runtime_internal_registry_closes_rotated_pair_crl_and_trust_without_consumer_poll()
 {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let rotated = certs.leaf(SERVER, 3, None);
    let other = Certificates::new();
    let other_primary = other.leaf(SERVER, 4, None);
    let until = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
    let crl = certs.crl(until, None);
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    for replacement in [
        runtime_generation(
            2,
            &certs,
            &rotated,
            &crl,
            runtime_policy(&["monitor"]),
            false,
        ),
        runtime_generation(
            2,
            &certs,
            &primary,
            &certs.crl(until, Some(2)),
            runtime_policy(&["monitor"]),
            false,
        ),
        runtime_generation(
            2,
            &other,
            &other_primary,
            &other.crl(until, None),
            runtime_policy(&["monitor"]),
            false,
        ),
    ] {
        let runtime = sider::security::GenerationRuntime::new(runtime_generation(
            1,
            &certs,
            &primary,
            &crl,
            runtime_policy(&["monitor"]),
            false,
        ))
        .unwrap();
        let ((mut outgoing, _), (_retained_socket, peer)) =
            registered_pair(runtime.internal().await.unwrap(), &client).await;
        runtime.activate(replacement).await.unwrap();
        assert!(!peer.current());
        physically_closed(&mut outgoing).await;
    }
}

#[tokio::test]
async fn security_runtime_internal_surviving_session_and_invalid_generation_retain_active_state() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let generation = |number, roles: &[&str]| {
        runtime_generation(number, &certs, &primary, &crl, runtime_policy(roles), false)
    };
    let runtime = sider::security::GenerationRuntime::new(generation(1, &["monitor"])).unwrap();
    let internal = runtime.internal().await.unwrap();
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let ((mut outgoing, _), (mut incoming, peer)) =
        registered_pair(internal.clone(), &client).await;
    assert!(runtime.activate(generation(1, &["backup"])).await.is_err());
    assert_eq!(runtime.number().await, 1);
    assert!(peer.current());
    runtime
        .activate(generation(2, &["monitor", "backup"]))
        .await
        .unwrap();
    let missing_lane = sider::security::Generation::client_files(
        3,
        br#"{"version":1,"users":[]}"#,
        primary.0.as_bytes(),
        primary.1.as_bytes(),
    )
    .unwrap();
    assert!(runtime.activate(missing_lane).await.is_err());
    assert_eq!(runtime.number().await, 2);
    internal.lease().await.authorize_request(&peer, 10).unwrap();
    outgoing.write_u8(73).await.unwrap();
    outgoing.flush().await.unwrap();
    assert_eq!(
        timeout(DEADLINE, incoming.read_u8())
            .await
            .unwrap()
            .unwrap(),
        73
    );
}

#[tokio::test]
async fn security_runtime_internal_idle_expiry_drops_socket_and_closes_retained_proof() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::seconds(4),
        None,
    );
    let runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["monitor"]),
        false,
    ))
    .unwrap();
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let internal = runtime.internal().await.unwrap();
    let incoming = tokio::spawn(async move {
        internal
            .accept_with_peer(listener.accept().await.unwrap().0)
            .await
            .unwrap()
    });
    // Unguarded test connector ensures observed EOF is physical socket close,
    // rather than the client's own expiry wrapper refusing a read.
    let mut outgoing = raw_connect(&raw_connector(&certs, Some(&remote)), address).await;
    let (retained_socket, peer) = timeout(DEADLINE, incoming).await.unwrap().unwrap();
    // No I/O is polled on the registered side; the registry timer still closes it.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(!peer.current());
    physically_closed(&mut outgoing).await;
    assert_eq!(runtime.number().await, 1);
    drop(retained_socket);
    assert!(
        client
            .connect_with_peer(
                TcpListener::bind("127.0.0.1:0")
                    .await
                    .unwrap()
                    .local_addr()
                    .unwrap()
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn security_runtime_combined_client_internal_rotation_shares_one_barrier() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let rotated = certs.leaf(SERVER, 3, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["monitor"]),
        false,
    ))
    .unwrap();
    let internal = runtime.internal().await.unwrap();
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let ((mut outgoing, _), (_retained_socket, peer)) =
        registered_pair(internal.clone(), &client).await;
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
    let mut resp = raw_connect(&raw_connector(&certs, None), address).await;
    resp.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    resp.flush().await.unwrap();
    let expected = b"-NOAUTH Authentication required\r\n";
    let mut reply = vec![0; expected.len()];
    timeout(DEADLINE, resp.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, expected);
    let lease = internal.lease().await;
    let replacement = runtime_generation(
        2,
        &certs,
        &rotated,
        &crl,
        runtime_policy(&["monitor"]),
        false,
    );
    let reload = runtime.clone();
    let mut activation = tokio::spawn(async move { reload.activate(replacement).await });
    assert!(
        timeout(Duration::from_millis(50), &mut activation)
            .await
            .is_err()
    );
    assert!(peer.current());
    drop(lease);
    timeout(DEADLINE, activation)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!peer.current());
    physically_closed(&mut outgoing).await;
    physically_closed(&mut resp).await;
    stop.send(()).unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn security_runtime_internal_activation_survives_caller_cancellation() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["monitor", "backup"]),
        false,
    ))
    .unwrap();
    let internal = runtime.internal().await.unwrap();
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let ((mut outgoing, _), (_retained_socket, peer)) =
        registered_pair(internal.clone(), &client).await;
    let lease = internal.lease().await;
    let replacement = runtime_generation(
        2,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["monitor"]),
        false,
    );
    let reload = runtime.clone();
    let mut caller = tokio::spawn(async move { reload.activate(replacement).await });
    assert!(
        timeout(Duration::from_millis(50), &mut caller)
            .await
            .is_err()
    );
    caller.abort();
    let _ = caller.await;
    drop(lease);
    timeout(DEADLINE, async {
        while runtime.number().await != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!peer.current());
    physically_closed(&mut outgoing).await;
}

#[tokio::test]
async fn security_runtime_internal_outgoing_reconnect_uses_active_certificate_pair() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let rotated = certs.leaf(PEER, 3, None);
    let until = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
    let crl = certs.crl(until, None);
    let outgoing_policy =
        json!({"version":1,"instance_id":"holder-b","principals":[],"upstream_principal":SERVER});
    let runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1,
        &certs,
        &remote,
        &crl,
        outgoing_policy.clone(),
        true,
    ))
    .unwrap();
    let internal = runtime.internal().await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let old_server = certs.transport(&primary, &crl, policy(&["monitor"]), false);
    // The second handshake rejects the old client certificate, independently
    // proving that a retained runtime handle resolves the replacement key pair.
    let new_server = certs.transport(
        &primary,
        &certs.crl(until, Some(2)),
        policy(&["monitor"]),
        false,
    );
    let (first_send, first_receive) = tokio::sync::oneshot::channel();
    let accept = tokio::spawn(async move {
        first_send
            .send(
                old_server
                    .accept_with_peer(listener.accept().await.unwrap().0)
                    .await
                    .unwrap(),
            )
            .ok()
            .unwrap();
        new_server
            .accept_with_peer(listener.accept().await.unwrap().0)
            .await
            .unwrap()
    });
    let (retained_socket, peer) = internal.connect_with_peer(address).await.unwrap();
    let (mut first_server_socket, _) = timeout(DEADLINE, first_receive).await.unwrap().unwrap();
    runtime
        .activate(runtime_generation(
            2,
            &certs,
            &rotated,
            &crl,
            outgoing_policy,
            true,
        ))
        .await
        .unwrap();
    assert!(!peer.current());
    physically_closed(&mut first_server_socket).await;
    let (mut outgoing, proof) = internal.connect_with_peer(address).await.unwrap();
    let (mut incoming, _) = timeout(DEADLINE, accept).await.unwrap().unwrap();
    assert!(proof.current());
    outgoing.write_u8(91).await.unwrap();
    outgoing.flush().await.unwrap();
    assert_eq!(
        timeout(DEADLINE, incoming.read_u8())
            .await
            .unwrap()
            .unwrap(),
        91
    );
    drop(retained_socket);
}

#[tokio::test]
async fn security_runtime_internal_revocation_wakes_pending_reader_after_socket_close() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["monitor", "backup"]),
        false,
    ))
    .unwrap();
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let ((mut outgoing, _), (mut incoming, peer)) =
        registered_pair(runtime.internal().await.unwrap(), &client).await;
    let (pending_send, pending_receive) = tokio::sync::oneshot::channel();
    let reader = tokio::spawn(async move {
        let mut notice = Some(pending_send);
        let mut byte = [0];
        let mut buffer = tokio::io::ReadBuf::new(&mut byte);
        std::future::poll_fn(|cx| {
            let result = std::pin::Pin::new(&mut incoming).poll_read(cx, &mut buffer);
            if result.is_pending()
                && let Some(notice) = notice.take()
            {
                notice.send(()).unwrap();
            }
            result
        })
        .await
    });
    timeout(DEADLINE, pending_receive).await.unwrap().unwrap();
    runtime
        .activate(runtime_generation(
            2,
            &certs,
            &primary,
            &crl,
            runtime_policy(&["monitor"]),
            false,
        ))
        .await
        .unwrap();
    assert!(!peer.current());
    let error = timeout(DEADLINE, reader)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    physically_closed(&mut outgoing).await;
}

#[tokio::test]
async fn security_runtime_internal_replacement_expired_while_waiting_retains_generation_and_sessions()
 {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let now = time::OffsetDateTime::now_utc();
    let crl = certs.crl(now + time::Duration::hours(1), None);
    let runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["monitor", "backup"]),
        false,
    ))
    .unwrap();
    let internal = runtime.internal().await.unwrap();
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let ((mut outgoing, _), (mut incoming, peer)) =
        registered_pair(internal.clone(), &client).await;
    let lease = internal.lease().await;
    let replacement = runtime_generation(
        2,
        &certs,
        &primary,
        &certs.crl(now + time::Duration::seconds(3), None),
        runtime_policy(&["monitor"]),
        false,
    );
    let reload = runtime.clone();
    let mut activation = tokio::spawn(async move { reload.activate(replacement).await });
    assert!(
        timeout(Duration::from_millis(50), &mut activation)
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_secs(4)).await;
    drop(lease);
    assert!(matches!(
        timeout(DEADLINE, activation).await.unwrap().unwrap(),
        Err(sider::security::Error::Transport)
    ));
    assert_eq!(runtime.number().await, 1);
    assert!(peer.current());
    internal.lease().await.authorize_request(&peer, 10).unwrap();
    outgoing.write_u8(84).await.unwrap();
    outgoing.flush().await.unwrap();
    assert_eq!(
        timeout(DEADLINE, incoming.read_u8())
            .await
            .unwrap()
            .unwrap(),
        84
    );
}
fn hello() -> Message {
    Message::Hello(Hello {
        sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
        record_version: 1,
        shard_count: 1,
        routing_version: 1,
        max_record_bytes: 1024,
        max_mutations: 64,
        max_snapshot_bytes: 65536,
        cursor: None,
    })
}
fn cursor() -> Cursor {
    Cursor {
        epoch: [1; 16],
        sequence: 1,
    }
}

#[tokio::test]
async fn security_internal_real_tls_role_matrix_denies_before_withheld_payload() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let requests = [
        hello(),
        Message::Export {
            sider_version: Bytes::from_static(b"1.0.1"),
            max_record_bytes: 1024,
            max_snapshot_bytes: 65536,
        },
        Message::StatusRequest,
        Message::Promote,
        Message::Ack(cursor()),
    ];
    for role in ["replica", "backup", "monitor", "promotion"] {
        let policy = policy(&[role]);
        for (index, request) in requests.iter().enumerate() {
            let allowed = matches!(
                (role, index),
                ("replica", 0) | ("backup", 1) | ("monitor", 2) | ("promotion", 2 | 3)
            );
            let server = certs.transport(&primary, &crl, policy.clone(), false);
            let ((mut outgoing, _), (mut incoming, peer), _) = pair(server, &client).await;
            let policy = policy.clone();
            let read = tokio::spawn(async move {
                protocol::read_authorized(
                    &mut incoming,
                    protocol::Limits::default(),
                    DEADLINE,
                    |kind| peer.current() && authorize_internal_request(&policy, peer.uri(), kind),
                )
                .await
            });
            let frame = protocol::encode(request, protocol::Limits::default()).unwrap();
            outgoing
                .write_all(if allowed {
                    &frame
                } else {
                    &frame[..protocol::HEADER_BYTES]
                })
                .await
                .unwrap();
            outgoing.flush().await.unwrap();
            let result = timeout(Duration::from_secs(1), read)
                .await
                .expect("header denial cannot await payload")
                .unwrap();
            if allowed {
                assert_eq!(result.unwrap().1, *request);
            } else {
                assert!(matches!(
                    result,
                    Err(protocol::Error::Invalid("permission denied"))
                ));
            }
        }
    }
}

#[tokio::test]
async fn security_internal_real_tls_direction_denies_large_header_before_payload() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let server = certs.transport(&primary, &crl, policy(&["monitor"]), false);
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    for phase in [
        InternalPhase::Hello,
        InternalPhase::ReplicaStart,
        InternalPhase::BackupStart,
        InternalPhase::SnapshotEntry,
        InternalPhase::SnapshotEnd,
        InternalPhase::ReplicaStream,
        InternalPhase::ReplicaAck,
        InternalPhase::Status,
        InternalPhase::Promoted,
    ] {
        let ((mut outgoing, _), (mut incoming, _), _) = pair(server.clone(), &client).await;
        let read = tokio::spawn(async move {
            protocol::read_in_phase(&mut outgoing, protocol::Limits::default(), DEADLINE, phase)
                .await
        });
        let mut header = protocol::encode(&Message::Ack(cursor()), protocol::Limits::default())
            .unwrap()[..protocol::HEADER_BYTES]
            .to_vec();
        header[10] = if matches!(phase, InternalPhase::SnapshotEntry) {
            6
        } else {
            4
        };
        // Valid record-kind header with 64 KiB body withheld.
        header[12..16].copy_from_slice(&65536_u32.to_le_bytes());
        header[16..20].copy_from_slice(&(!65536_u32).to_le_bytes());
        incoming.write_all(&header).await.unwrap();
        incoming.flush().await.unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(1), read)
                .await
                .expect("direction rejected before body read")
                .unwrap(),
            Err(protocol::Error::Invalid("permission denied"))
        ));
    }
}

#[tokio::test]
async fn security_internal_established_proof_revalidates_roles_trust_crl_and_own_pair() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let until = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
    let crl = certs.crl(until, None);
    let server = certs.transport(&primary, &crl, policy(&["monitor", "backup"]), false);
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let ((outgoing, outgoing_peer), (incoming, incoming_peer), _) =
        pair(server.clone(), &client).await;
    assert!(server.revalidate_peer(&incoming_peer).is_ok());
    assert!(client.revalidate_peer(&outgoing_peer).is_ok());
    assert!(
        certs
            .transport(&primary, &crl, policy(&["monitor"]), false)
            .revalidate_peer(&incoming_peer)
            .is_err()
    );
    let revoked = certs.crl(until, Some(2));
    assert!(
        certs
            .transport(&primary, &revoked, policy(&["monitor", "backup"]), false)
            .revalidate_peer(&incoming_peer)
            .is_err()
    );
    let other = Certificates::new();
    let other_crl = other.crl(until, None);
    assert!(
        other
            .transport(&primary, &other_crl, policy(&["monitor", "backup"]), false)
            .revalidate_peer(&incoming_peer)
            .is_err()
    );
    let replacement = certs.leaf(SERVER, 3, None);
    assert!(
        certs
            .transport(&replacement, &crl, policy(&["monitor", "backup"]), false)
            .revalidate_peer(&incoming_peer)
            .is_err()
    );
    assert!(
        certs
            .transport(
                &remote,
                &crl,
                tool_policy("spiffe://sider/primary-a/other"),
                true
            )
            .revalidate_peer(&outgoing_peer)
            .is_err()
    );
    let revoked_server = certs.crl(until, Some(1));
    assert!(
        certs
            .transport(&remote, &revoked_server, tool_policy(SERVER), true)
            .revalidate_peer(&outgoing_peer)
            .is_err()
    );
    drop(outgoing);
    drop(incoming);
}

async fn prove_idle_expiry(short_crl: bool) {
    let certs = Certificates::new();
    let until = time::OffsetDateTime::now_utc() + time::Duration::seconds(5);
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, (!short_crl).then_some(until));
    let crl = certs.crl(
        if short_crl {
            until
        } else {
            until + time::Duration::hours(1)
        },
        None,
    );
    let server = certs.transport(&primary, &crl, policy(&["monitor"]), false);
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let ((mut outgoing, outgoing_peer), (mut incoming, incoming_peer), address) =
        pair(server, &client).await;
    let read = tokio::spawn(async move {
        let mut byte = [0];
        incoming.read(&mut byte).await
    });
    let result = timeout(Duration::from_secs(8), read)
        .await
        .expect("expiry wakes an idle TLS read")
        .unwrap();
    assert!(matches!(result, Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied));
    assert!(!incoming_peer.current());
    assert!(!outgoing_peer.current());
    let mut byte = [0];
    assert!(
        timeout(Duration::from_secs(1), outgoing.read(&mut byte))
            .await
            .unwrap()
            .is_err()
    );
    assert!(
        timeout(Duration::from_millis(100), client.connect(address))
            .await
            .unwrap()
            .is_err()
    );
}
#[tokio::test]
async fn security_internal_crl_expiry_wakes_idle_session_and_refuses_new_admission() {
    prove_idle_expiry(true).await;
}
#[tokio::test]
async fn security_internal_peer_certificate_expiry_wakes_idle_session() {
    prove_idle_expiry(false).await;
}

#[tokio::test]
async fn security_internal_backup_rejects_wrong_direction_before_creating_artifact() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let server = certs.transport(&primary, &crl, policy(&["backup"]), false);
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut stream, _) = server
            .accept_with_peer(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert!(matches!(
            protocol::read(&mut stream, protocol::Limits::default(), DEADLINE)
                .await
                .unwrap(),
            Message::Export { .. }
        ));
        let mut header = protocol::encode(&Message::Ack(cursor()), protocol::Limits::default())
            .unwrap()[..protocol::HEADER_BYTES]
            .to_vec();
        header[10] = 4;
        header[12..16].copy_from_slice(&65536_u32.to_le_bytes());
        header[16..20].copy_from_slice(&(!65536_u32).to_le_bytes());
        stream.write_all(&header).await.unwrap();
        stream.flush().await.unwrap();
        // Keep the TLS session open without supplying the declared payload.
        let mut byte = [0];
        let _ = timeout(DEADLINE, stream.read(&mut byte)).await;
    });
    let destination = std::env::temp_dir().join(format!(
        "sider-security-internal-backup-{}-{}",
        std::process::id(),
        time::OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    let options = sider::persistence::backup::ExportOptions {
        source: address,
        destination: destination.clone(),
        source_sha: "1".repeat(40),
        limits: sider::persistence::backup::Limits::default(),
    };
    let result = timeout(
        Duration::from_secs(1),
        sider::persistence::backup::export_secure(
            options,
            Arc::new(sider::storage::SystemClock),
            &client,
        ),
    )
    .await
    .expect("backup does not wait for unauthorized payload");
    assert!(result.is_err());
    assert!(!destination.exists());
    timeout(DEADLINE, task).await.unwrap().unwrap();
}

// Actual consumer paths: workers and durable state run behind the live registry.
struct Consumer {
    database: sider::storage::worker::DbHandle,
    context: sider::storage::replication::Context,
    stop: tokio::sync::watch::Sender<bool>,
    workers: tokio::task::JoinSet<()>,
    writer: tokio::task::JoinHandle<Result<(), sider::persistence::AofError>>,
    directory: std::path::PathBuf,
}
impl Consumer {
    async fn new(role: sider::persistence::Role) -> Self {
        Self::with_faults(role, Arc::new(sider::persistence::NoFaults)).await
    }
    async fn with_faults(
        role: sider::persistence::Role,
        faults: Arc<dyn sider::persistence::FaultInjector>,
    ) -> Self {
        use sider::{
            persistence::{self, AofConfig, ReplicationMetadata},
            replication::{journal, state::Runtime},
            storage::{Store, StoreConfig, worker},
        };
        let directory = std::env::temp_dir().join(format!(
            "sider-live-consumer-{}-{}",
            std::process::id(),
            time::OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        let mut config = AofConfig::new(directory.clone());
        config.compact_after_bytes = 0;
        config.layout.shard_count = 1;
        let layout = config.layout;
        let aof_limits = config.limits;
        let clock = Arc::new(sider::storage::SystemClock);
        let store_config = StoreConfig {
            max_dataset_bytes: 262144,
        };
        let recovered =
            persistence::recover_with_faults(config, store_config, clock.clone(), faults).unwrap();
        let (_, aof, writer) = recovered.start();
        let cursor = Cursor {
            epoch: [0; 16],
            sequence: 0,
        };
        aof.install_snapshot(
            Vec::new(),
            0,
            ReplicationMetadata {
                role: persistence::Role::Replica,
                epoch: cursor.epoch,
            },
        )
        .await
        .unwrap();
        let runtime = Runtime::new(role, cursor);
        let (stop, shutdown) = tokio::sync::watch::channel(false);
        let (database, owners) = worker::channel_with_stores(
            8,
            DEADLINE,
            shutdown,
            vec![Store::with_config(store_config, clock.clone()).unwrap()],
        )
        .unwrap();
        let database = database.with_replication(runtime.clone());
        let mut workers = tokio::task::JoinSet::new();
        for owner in owners {
            workers.spawn(
                owner
                    .with_aof(aof.clone(), 0)
                    .with_replication(runtime.clone())
                    .run(),
            );
        }
        let context = sider::storage::replication::Context {
            aof,
            runtime,
            layout,
            store_config,
            clock,
            journal_limits: journal::Limits {
                max_bytes: 32768,
                max_batches: 128,
                max_frame_bytes: 32768,
            },
            aof_limits,
        };
        if role == persistence::Role::Primary {
            database
                .promote_replica(context.clone(), [1; 16])
                .await
                .unwrap();
        }
        Self {
            database,
            context,
            stop,
            workers,
            writer,
            directory,
        }
    }
    async fn listen(
        &self,
        runtime: &sider::security::GenerationRuntime,
    ) -> (
        SocketAddr,
        tokio::task::JoinHandle<Result<(), sider::replication::Error>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let database = self.database.clone();
        let context = self.context.clone();
        let stop = self.stop.subscribe();
        let internal = runtime.internal().await.unwrap();
        let config = sider::replication::config::Config {
            frame_timeout: Duration::from_millis(400),
            sync_timeout: DEADLINE,
            ..Default::default()
        };
        let server = tokio::spawn(sider::replication::session::serve_live(
            listener, database, context, config, stop, internal,
        ));
        (address, server)
    }
    async fn finish(
        mut self,
        server: tokio::task::JoinHandle<Result<(), sider::replication::Error>>,
    ) {
        self.stop.send_replace(true);
        timeout(DEADLINE, server).await.unwrap().unwrap().unwrap();
        while let Some(result) = self.workers.join_next().await {
            result.unwrap();
        }
        drop(self.database);
        drop(self.context);
        timeout(DEADLINE, self.writer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        std::fs::remove_dir_all(self.directory).unwrap();
    }
}

#[tokio::test]
async fn security_live_consumer_status_and_role_revocation_close_idle_before_header() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["monitor"]),
        false,
    ))
    .unwrap();
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let consumer = Consumer::new(sider::persistence::Role::Replica).await;
    let (address, server) = consumer.listen(&runtime).await;
    let mut stream = client.connect(address).await.unwrap();
    protocol::write(
        &mut stream,
        &Message::StatusRequest,
        protocol::Limits::default(),
        DEADLINE,
    )
    .await
    .unwrap();
    assert!(matches!(
        protocol::read(&mut stream, protocol::Limits::default(), DEADLINE)
            .await
            .unwrap(),
        Message::Status { readonly: true, .. }
    ));
    let mut idle = client.connect(address).await.unwrap();
    timeout(
        Duration::from_millis(250),
        runtime.activate(runtime_generation(
            2,
            &certs,
            &primary,
            &crl,
            runtime_policy(&["backup"]),
            false,
        )),
    )
    .await
    .expect("idle header wait owns no generation lease")
    .unwrap();
    physically_closed(&mut idle).await;
    assert!(consumer.context.runtime.readonly());
    consumer.finish(server).await;
}

#[tokio::test]
async fn security_live_consumer_denies_promotion_before_payload_or_durable_effect() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["monitor"]),
        false,
    ))
    .unwrap();
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let consumer = Consumer::new(sider::persistence::Role::Replica).await;
    let (address, server) = consumer.listen(&runtime).await;
    let mut stream = client.connect(address).await.unwrap();
    let before = consumer.context.runtime.status().applied;
    let mut header = protocol::encode(&Message::Promote, protocol::Limits::default()).unwrap()
        [..protocol::HEADER_BYTES]
        .to_vec();
    header[12..16].copy_from_slice(&65536_u32.to_le_bytes());
    header[16..20].copy_from_slice(&(!65536_u32).to_le_bytes());
    stream.write_all(&header).await.unwrap();
    stream.flush().await.unwrap();
    physically_closed(&mut stream).await;
    assert!(consumer.context.runtime.readonly());
    assert_eq!(consumer.context.runtime.status().applied, before);
    consumer.finish(server).await;
}

#[tokio::test]
async fn security_live_consumer_promotion_persists_and_returns_authorized_reply() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["promotion"]),
        false,
    ))
    .unwrap();
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let consumer = Consumer::new(sider::persistence::Role::Replica).await;
    let (address, server) = consumer.listen(&runtime).await;
    let mut stream = client.connect(address).await.unwrap();
    protocol::write(
        &mut stream,
        &Message::Promote,
        protocol::Limits::default(),
        DEADLINE,
    )
    .await
    .unwrap();
    let Message::Promoted(cursor) =
        protocol::read(&mut stream, protocol::Limits::default(), DEADLINE)
            .await
            .unwrap()
    else {
        panic!("expected promotion receipt")
    };
    assert!(!consumer.context.runtime.readonly());
    assert_eq!(consumer.context.runtime.status().applied, cursor);
    assert_ne!(cursor.epoch, [0; 16]);
    consumer.finish(server).await;
}

#[tokio::test]
async fn security_live_consumer_partial_authorized_body_bounds_reload_lease() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["backup"]),
        false,
    ))
    .unwrap();
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let consumer = Consumer::new(sider::persistence::Role::Primary).await;
    let (address, server) = consumer.listen(&runtime).await;
    let mut stream = client.connect(address).await.unwrap();
    let frame = protocol::encode(
        &Message::Export {
            sider_version: env!("CARGO_PKG_VERSION").into(),
            max_record_bytes: u32::MAX,
            max_snapshot_bytes: u64::MAX,
        },
        protocol::Limits::default(),
    )
    .unwrap();
    stream
        .write_all(&frame[..protocol::HEADER_BYTES])
        .await
        .unwrap();
    stream.flush().await.unwrap();
    timeout(
        Duration::from_secs(2),
        runtime.activate(runtime_generation(
            2,
            &certs,
            &primary,
            &crl,
            runtime_policy(&["backup"]),
            false,
        )),
    )
    .await
    .expect("partial payload cannot hold the generation indefinitely")
    .unwrap();
    physically_closed(&mut stream).await;
    consumer.finish(server).await;
}

struct ConsumerGate {
    armed: std::sync::atomic::AtomicBool,
    entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    released: std::sync::Mutex<bool>,
    wake: std::sync::Condvar,
}
impl ConsumerGate {
    fn new() -> (Arc<Self>, tokio::sync::oneshot::Receiver<()>) {
        let (entered, response) = tokio::sync::oneshot::channel();
        (
            Arc::new(Self {
                armed: false.into(),
                entered: std::sync::Mutex::new(Some(entered)),
                released: std::sync::Mutex::new(false),
                wake: std::sync::Condvar::new(),
            }),
            response,
        )
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}
impl sider::persistence::FaultInjector for ConsumerGate {
    fn hit(&self, point: &'static str) -> std::io::Result<()> {
        if point == "replication_before_publish"
            && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            if let Some(entered) = self.entered.lock().unwrap().take() {
                let _ = entered.send(());
            }
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.wake.wait(released).unwrap();
            }
        }
        Ok(())
    }
}
struct ConsumerRelease(Arc<ConsumerGate>);
impl Drop for ConsumerRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test]
async fn security_live_consumer_expiry_after_blocked_barrier_refuses_promotion() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(
        PEER,
        2,
        Some(time::OffsetDateTime::now_utc() + time::Duration::seconds(2)),
    );
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["promotion"]),
        false,
    ))
    .unwrap();
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let (gate, entered) = ConsumerGate::new();
    let _release = ConsumerRelease(gate.clone());
    let consumer = Consumer::with_faults(sider::persistence::Role::Replica, gate.clone()).await;
    let generation = consumer.context.runtime.begin_session();
    let cursor = Cursor {
        epoch: [9; 16],
        sequence: 0,
    };
    gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    let database = consumer.database.clone();
    let context = consumer.context.clone();
    let installing = tokio::spawn(async move {
        database
            .install_replica(context, generation, cursor, Vec::new())
            .await
    });
    timeout(DEADLINE, entered).await.unwrap().unwrap();
    let (address, server) = consumer.listen(&runtime).await;
    let mut stream = client.connect(address).await.unwrap();
    protocol::write(
        &mut stream,
        &Message::Promote,
        protocol::Limits::default(),
        DEADLINE,
    )
    .await
    .unwrap();
    let mut byte = [0];
    assert!(
        timeout(DEADLINE, stream.read(&mut byte))
            .await
            .unwrap()
            .is_err()
    );
    gate.release();
    timeout(DEADLINE, installing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    timeout(
        DEADLINE,
        runtime.activate(runtime_generation(
            2,
            &certs,
            &primary,
            &crl,
            runtime_policy(&["monitor"]),
            false,
        )),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        consumer.context.runtime.readonly(),
        "expired waiting promotion was never accepted"
    );
    assert_eq!(consumer.context.runtime.status().applied, cursor);
    consumer.finish(server).await;
}

#[tokio::test]
async fn security_live_consumer_cancelled_promotion_retains_lease_through_publication() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["promotion"]),
        false,
    ))
    .unwrap();
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let (gate, entered) = ConsumerGate::new();
    let _release = ConsumerRelease(gate.clone());
    let consumer = Consumer::with_faults(sider::persistence::Role::Replica, gate.clone()).await;
    let (address, server) = consumer.listen(&runtime).await;
    gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    let mut stream = client.connect(address).await.unwrap();
    protocol::write(
        &mut stream,
        &Message::Promote,
        protocol::Limits::default(),
        DEADLINE,
    )
    .await
    .unwrap();
    timeout(DEADLINE, entered).await.unwrap().unwrap();
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    drop(stream);
    let reload = runtime.clone();
    let replacement = runtime_generation(
        2,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["monitor"]),
        false,
    );
    let mut activation = tokio::spawn(async move { reload.activate(replacement).await });
    assert!(
        timeout(Duration::from_millis(80), &mut activation)
            .await
            .is_err(),
        "owned accepted promotion pins old policy until durable publication"
    );
    gate.release();
    timeout(DEADLINE, activation)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!consumer.context.runtime.readonly());
    assert_ne!(consumer.context.runtime.status().applied.epoch, [0; 16]);
    // Listener was explicitly cancelled above; finish only the database owners.
    let finished = tokio::spawn(async { Ok(()) });
    consumer.finish(finished).await;
}

#[tokio::test]
async fn security_live_consumer_backup_tool_exports_actual_worker_snapshot() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["backup"]),
        false,
    ))
    .unwrap();
    let client = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let consumer = Consumer::new(sider::persistence::Role::Primary).await;
    consumer
        .database
        .execute(sider::command::Command::Set {
            key: Bytes::from_static(b"backup-key"),
            value: Bytes::from_static(b"snapshot-value"),
        })
        .await
        .unwrap();
    let (address, server) = consumer.listen(&runtime).await;
    let destination = consumer.directory.join("export");
    let options = sider::persistence::backup::ExportOptions {
        source: address,
        destination: destination.clone(),
        source_sha: "1".repeat(40),
        limits: sider::persistence::backup::Limits::default(),
    };
    timeout(
        DEADLINE,
        sider::persistence::backup::export_secure(options, consumer.context.clock.clone(), &client),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        destination.is_dir(),
        "completed backup is published atomically"
    );
    consumer.finish(server).await;
}

#[tokio::test]
async fn security_live_consumer_replica_installs_streams_and_stops_on_role_revocation() {
    use sider::command::{Command, Reply};
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let primary_runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1,
        &certs,
        &primary,
        &crl,
        runtime_policy(&["replica"]),
        false,
    ))
    .unwrap();
    let outgoing =
        json!({"version":1,"instance_id":"holder-b","principals":[],"upstream_principal":SERVER});
    let replica_runtime = sider::security::GenerationRuntime::new(runtime_generation(
        1, &certs, &remote, &crl, outgoing, true,
    ))
    .unwrap();
    let source = Consumer::new(sider::persistence::Role::Primary).await;
    let replica = Consumer::new(sider::persistence::Role::Replica).await;
    let key = Bytes::from_static(b"replicated-key");
    source
        .database
        .execute(Command::Set {
            key: key.clone(),
            value: Bytes::from_static(b"snapshot"),
        })
        .await
        .unwrap();
    let (address, server) = source.listen(&primary_runtime).await;
    let config = sider::replication::config::Config {
        upstream: Some(address),
        frame_timeout: Duration::from_secs(2),
        sync_timeout: DEADLINE,
        reconnect_min: Duration::from_millis(20),
        reconnect_max: Duration::from_millis(50),
        ..Default::default()
    };
    let follower = tokio::spawn(sider::replication::session::follow_live(
        replica.database.clone(),
        replica.context.clone(),
        config,
        replica.stop.subscribe(),
        replica_runtime.internal().await.unwrap(),
    ));
    timeout(DEADLINE, async {
        loop {
            if replica
                .database
                .execute(Command::Get { key: key.clone() })
                .await
                .unwrap()
                == Reply::Bulk(Some(Bytes::from_static(b"snapshot")))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("full snapshot is installed under the live upstream lease");
    source
        .database
        .execute(Command::Set {
            key: key.clone(),
            value: Bytes::from_static(b"incremental"),
        })
        .await
        .unwrap();
    timeout(DEADLINE, async {
        loop {
            if replica
                .database
                .execute(Command::Get { key: key.clone() })
                .await
                .unwrap()
                == Reply::Bulk(Some(Bytes::from_static(b"incremental")))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("worker applies an incremental accepted batch");
    primary_runtime
        .activate(runtime_generation(
            2,
            &certs,
            &primary,
            &crl,
            runtime_policy(&["monitor"]),
            false,
        ))
        .await
        .unwrap();
    let stopped_at = replica.context.runtime.status().applied;
    source
        .database
        .execute(Command::Set {
            key: key.clone(),
            value: Bytes::from_static(b"denied"),
        })
        .await
        .unwrap();
    replica.stop.send_replace(true);
    timeout(DEADLINE, follower).await.unwrap().unwrap().unwrap();
    assert_eq!(replica.context.runtime.status().applied, stopped_at);
    assert_eq!(
        replica.database.execute(Command::Get { key }).await,
        Err(sider::storage::worker::DbError::ShuttingDown)
    );
    let finished = tokio::spawn(async { Ok(()) });
    replica.finish(finished).await;
    source.finish(server).await;
}

struct StartupDirectory(std::path::PathBuf);
impl StartupDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sider-security-startup-{}-{}",
            std::process::id(),
            time::OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn config(&self) -> sider::ServerConfig {
        let mut aof = sider::persistence::AofConfig::new(self.0.join("aof"));
        aof.compact_after_bytes = 0;
        sider::ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            aof: Some(aof),
            ready_file: Some(self.0.join("client.ready")),
            replication: Some(sider::replication::config::Config {
                ready_file: Some(self.0.join("internal.ready")),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}
impl Drop for StartupDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
async fn startup_ready(path: &std::path::Path) -> SocketAddr {
    timeout(DEADLINE, async {
        loop {
            if let Ok(bytes) = std::fs::read(path) {
                let value: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(value["transport_mode"], "tls");
                assert_eq!(value["authentication_required"], true);
                return format!(
                    "{}:{}",
                    value["host"].as_str().unwrap(),
                    value["port"].as_u64().unwrap()
                )
                .parse()
                .unwrap();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server publishes complete TLS readiness")
}

#[tokio::test]
async fn security_memory_live_export_requires_current_backup_role() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let directory = StartupDirectory::new();
    let mut config = directory.config();
    config.aof =
        Some(sider::persistence::AofConfig::new(directory.0.join("aof")).with_memory_profile());
    let preflight = sider::security::ServerPreflight::new(
        &config,
        runtime_generation(
            1,
            &certs,
            &primary,
            &crl,
            runtime_policy(&["backup"]),
            false,
        ),
    )
    .unwrap();
    let runtime = preflight.runtime();
    let (stop, shutdown) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(sider::server::serve_live(
        config.clone(),
        async {
            let _ = shutdown.await;
        },
        preflight,
    ));
    let address = startup_ready(
        config
            .replication
            .as_ref()
            .unwrap()
            .ready_file
            .as_deref()
            .unwrap(),
    )
    .await;
    let tool = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let options = |destination| sider::persistence::backup::ExportOptions {
        source: address,
        destination,
        source_sha: "a".repeat(40),
        limits: sider::persistence::backup::Limits::default(),
    };
    let accepted = directory.0.join("memory-accepted");
    let manifest = sider::persistence::backup::export_memory_secure(
        options(accepted.clone()),
        Arc::new(sider::storage::SystemClock),
        &tool,
    )
    .await
    .unwrap();
    assert_eq!(
        manifest.profile,
        sider::persistence::backup::Profile::Memory
    );
    assert_eq!(manifest.entries, 0);
    sider::persistence::backup::verify_memory(
        &accepted,
        config.aof.as_ref().unwrap().layout,
        sider::persistence::backup::Limits::default(),
        Arc::new(sider::storage::SystemClock),
    )
    .unwrap();
    runtime
        .activate(runtime_generation(
            2,
            &certs,
            &primary,
            &crl,
            runtime_policy(&["monitor"]),
            false,
        ))
        .await
        .unwrap();
    let denied = directory.0.join("memory-denied");
    assert!(
        sider::persistence::backup::export_memory_secure(
            options(denied.clone()),
            Arc::new(sider::storage::SystemClock),
            &tool,
        )
        .await
        .is_err()
    );
    assert!(!denied.exists());
    stop.send(()).unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn security_startup_live_server_uses_shared_runtime_for_client_and_internal() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let rotated = certs.leaf(SERVER, 3, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let directory = StartupDirectory::new();
    let config = directory.config();
    let preflight = sider::security::ServerPreflight::new(
        &config,
        runtime_generation(
            1,
            &certs,
            &primary,
            &crl,
            runtime_policy(&["monitor"]),
            false,
        ),
    )
    .unwrap();
    let runtime = preflight.runtime();
    let (stop, shutdown) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(sider::server::serve_live(
        config.clone(),
        async {
            let _ = shutdown.await;
        },
        preflight,
    ));
    let client_address = startup_ready(config.ready_file.as_deref().unwrap()).await;
    let internal_address = startup_ready(
        config
            .replication
            .as_ref()
            .unwrap()
            .ready_file
            .as_deref()
            .unwrap(),
    )
    .await;
    let mut client = raw_connect(&raw_connector(&certs, None), client_address).await;
    client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    client.flush().await.unwrap();
    let mut reply = [0; 33];
    let expected = b"-NOAUTH Authentication required\r\n";
    assert_eq!(expected.len(), reply.len());
    timeout(DEADLINE, client.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&reply, expected);
    let tool = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let mut status = tool.connect(internal_address).await.unwrap();
    protocol::write(
        &mut status,
        &Message::StatusRequest,
        protocol::Limits::default(),
        DEADLINE,
    )
    .await
    .unwrap();
    assert!(matches!(
        protocol::read(&mut status, protocol::Limits::default(), DEADLINE)
            .await
            .unwrap(),
        Message::Status {
            readonly: false,
            ..
        }
    ));
    let mut idle = tool.connect(internal_address).await.unwrap();
    runtime
        .activate(runtime_generation(
            2,
            &certs,
            &rotated,
            &crl,
            runtime_policy(&["monitor"]),
            false,
        ))
        .await
        .unwrap();
    physically_closed(&mut client).await;
    physically_closed(&mut idle).await;
    let mut fresh = tool.connect(internal_address).await.unwrap();
    protocol::write(
        &mut fresh,
        &Message::StatusRequest,
        protocol::Limits::default(),
        DEADLINE,
    )
    .await
    .unwrap();
    assert!(matches!(
        protocol::read(&mut fresh, protocol::Limits::default(), DEADLINE)
            .await
            .unwrap(),
        Message::Status { .. }
    ));
    stop.send(()).unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap().unwrap();
    assert!(!config.ready_file.unwrap().exists());
    assert!(!config.replication.unwrap().ready_file.unwrap().exists());
}

#[tokio::test]
async fn security_startup_changed_configuration_rejects_before_aof_and_listener_binds() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let directory = StartupDirectory::new();
    let mut config = directory.config();
    let occupied_client = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let occupied_internal = TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.bind_addr = occupied_client.local_addr().unwrap();
    config.replication.as_mut().unwrap().listen = occupied_internal.local_addr().unwrap();
    let preflight = sider::security::ServerPreflight::new(
        &config,
        runtime_generation(
            1,
            &certs,
            &primary,
            &crl,
            runtime_policy(&["monitor"]),
            false,
        ),
    )
    .unwrap();
    config.max_connections += 1;
    let result = sider::server::serve_live(config, std::future::pending(), preflight).await;
    assert!(
        matches!(result, Err(sider::server::ServerError::Security(_))),
        "security check must precede occupied bind errors"
    );
    assert!(!directory.0.join("aof").exists());
    assert!(!directory.0.join("client.ready").exists());
    assert!(!directory.0.join("internal.ready").exists());
}

#[test]
fn security_startup_preflight_requires_exact_lanes_and_outgoing_policy() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let directory = StartupDirectory::new();
    let config = directory.config();
    let client_only = sider::security::Generation::client_files(
        1,
        br#"{"version":1,"users":[]}"#,
        primary.0.as_bytes(),
        primary.1.as_bytes(),
    )
    .unwrap();
    assert!(sider::security::ServerPreflight::new(&config, client_only).is_err());
    let no_internal = sider::ServerConfig::default();
    assert!(
        sider::security::ServerPreflight::new(
            &no_internal,
            runtime_generation(
                1,
                &certs,
                &primary,
                &crl,
                runtime_policy(&["monitor"]),
                false
            )
        )
        .is_err()
    );
    let outgoing =
        json!({"version":1,"instance_id":"primary-a","principals":[],"upstream_principal":PEER});
    assert!(
        sider::security::ServerPreflight::new(
            &config,
            runtime_generation(1, &certs, &primary, &crl, outgoing, true)
        )
        .is_err()
    );
    let mut replica = config;
    replica.replication.as_mut().unwrap().upstream = Some("127.0.0.1:12345".parse().unwrap());
    assert!(
        sider::security::ServerPreflight::new(
            &replica,
            runtime_generation(
                1,
                &certs,
                &primary,
                &crl,
                runtime_policy(&["monitor"]),
                false
            )
        )
        .is_err()
    );
    assert!(!directory.0.join("aof").exists());
}

#[test]
fn security_startup_executable_rejects_missing_material_before_recovery() {
    let directory = StartupDirectory::new();
    let profile = sider::security::StartupSecurity::from_lookup(|name| {
        (name == "SIDER_SECURITY_DIR").then(|| directory.0.clone().into_os_string())
    })
    .unwrap();
    #[cfg(windows)]
    assert!(matches!(
        profile.preflight(&directory.config()),
        Err(sider::security::Error::PlatformValidation)
    ));
    #[cfg(unix)]
    assert!(matches!(
        profile.preflight(&directory.config()),
        Err(sider::security::Error::Configuration)
    ));
    #[cfg(windows)]
    assert!(matches!(
        profile.preflight(&sider::ServerConfig::default()),
        Err(sider::security::Error::PlatformValidation)
    ));
    assert!(!directory.0.join("aof").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn security_posix_complete_file_generation_serves_tls_and_enforces_backup_role() {
    use sha2::Digest;
    use std::{fs, os::unix::fs::PermissionsExt};

    struct SecurityDirectory(std::path::PathBuf, std::path::PathBuf);
    impl SecurityDirectory {
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for SecurityDirectory {
        fn drop(&mut self) {
            let resolved = fs::canonicalize(&self.0).ok();
            let home = fs::canonicalize(&self.1).ok();
            if home.is_some()
                && resolved.as_ref().and_then(|path| path.parent()) == home.as_deref()
                && self.0.file_name().is_some_and(|name| {
                    name.to_string_lossy()
                        .starts_with("sider-complete-security-")
                })
            {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
    }
    fn write_generation(directory: &std::path::Path, files: &[(&str, Vec<u8>)], number: u64) {
        let mut digests = std::collections::BTreeMap::new();
        for (name, bytes) in files {
            let digest = sha2::Sha256::digest(bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            digests.insert(*name, digest);
            let path = directory.join(name);
            fs::write(&path, bytes).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let manifest = directory.join("security-manifest.json");
        fs::write(
            &manifest,
            serde_json::to_vec(&json!({"version":1,"generation":number,"files":digests})).unwrap(),
        )
        .unwrap();
        fs::set_permissions(manifest, fs::Permissions::from_mode(0o600)).unwrap();
    }

    let home = fs::canonicalize(std::env::var_os("HOME").expect("service home")).unwrap();
    let security_directory = SecurityDirectory(
        home.join(format!(
            "sider-complete-security-{}-{}",
            std::process::id(),
            time::OffsetDateTime::now_utc().unix_timestamp_nanos()
        )),
        home.clone(),
    );
    fs::create_dir(security_directory.path()).unwrap();
    fs::set_permissions(security_directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let mut files = [
        (
            "client-policy.json",
            br#"{"version":1,"users":[]}"#.to_vec(),
        ),
        ("client-cert.pem", primary.0.as_bytes().to_vec()),
        ("client-key.pem", primary.1.as_bytes().to_vec()),
        (
            "internal-policy.json",
            serde_json::to_vec(&runtime_policy(&["monitor", "backup"])).unwrap(),
        ),
        ("internal-cert.pem", primary.0.as_bytes().to_vec()),
        ("internal-key.pem", primary.1.as_bytes().to_vec()),
        ("internal-ca.pem", certs.issuer.pem().into_bytes()),
        ("internal-crl.pem", crl.as_bytes().to_vec()),
    ];
    write_generation(security_directory.path(), &files, 1);
    let key = security_directory.path().join("internal-key.pem");
    fs::set_permissions(&key, fs::Permissions::from_mode(0o640)).unwrap();
    assert!(sider::security::Generation::server(security_directory.path(), true, false).is_err());
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&key, b"invalid replacement").unwrap();
    assert!(sider::security::Generation::server(security_directory.path(), true, false).is_err());
    fs::write(&key, primary.1.as_bytes()).unwrap();

    let directory = StartupDirectory::new();
    let config = directory.config();
    let profile = sider::security::StartupSecurity::from_lookup(|name| {
        (name == "SIDER_SECURITY_DIR").then(|| security_directory.path().as_os_str().to_owned())
    })
    .unwrap();
    let preflight = profile.preflight(&config).unwrap().unwrap();
    drop(preflight);
    assert!(!directory.0.join("aof").exists());
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_sider"));
    for (name, _) in std::env::vars_os() {
        if name.to_str().is_some_and(|name| name.starts_with("SIDER_")) {
            command.env_remove(name);
        }
    }
    command
        .env("SIDER_SECURITY_PROFILE", "protected")
        .env("SIDER_SECURITY_DIR", security_directory.path())
        .env("SIDER_ADDR", "127.0.0.1:0")
        .env("SIDER_REPLICATION_ADDR", "127.0.0.1:0")
        .env("SIDER_AOF_DIR", directory.0.join("aof"))
        .env("SIDER_READY_FILE", directory.0.join("client.ready"))
        .env(
            "SIDER_REPLICATION_READY_FILE",
            directory.0.join("internal.ready"),
        );
    let mut child = startup_process::OwnedChild::spawn(&mut command).unwrap();
    let client_address = startup_ready(config.ready_file.as_deref().unwrap()).await;
    let internal_address = startup_ready(
        config
            .replication
            .as_ref()
            .unwrap()
            .ready_file
            .as_deref()
            .unwrap(),
    )
    .await;
    child.assert_alive().unwrap();
    let mut client = raw_connect(&raw_connector(&certs, None), client_address).await;
    client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    client.flush().await.unwrap();
    let mut reply = [0; 33];
    timeout(DEADLINE, client.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&reply, b"-NOAUTH Authentication required\r\n");
    let tool = certs.transport(&remote, &crl, tool_policy(SERVER), true);
    let mut stream = tool.connect(internal_address).await.unwrap();
    protocol::write(
        &mut stream,
        &Message::StatusRequest,
        protocol::Limits::default(),
        DEADLINE,
    )
    .await
    .unwrap();
    assert!(matches!(
        protocol::read(&mut stream, protocol::Limits::default(), DEADLINE)
            .await
            .unwrap(),
        Message::Status { .. }
    ));

    let tool_directory = SecurityDirectory(
        home.join(format!(
            "sider-complete-security-tool-{}-{}",
            std::process::id(),
            time::OffsetDateTime::now_utc().unix_timestamp_nanos()
        )),
        home,
    );
    fs::create_dir(tool_directory.path()).unwrap();
    fs::set_permissions(tool_directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let tool_files = [
        (
            "internal-policy.json",
            serde_json::to_vec(&json!({"version":1,"instance_id":"holder-b","principals":[],"upstream_principal":SERVER})).unwrap(),
        ),
        ("internal-cert.pem", remote.0.as_bytes().to_vec()),
        ("internal-key.pem", remote.1.as_bytes().to_vec()),
        ("internal-ca.pem", certs.issuer.pem().into_bytes()),
        ("internal-crl.pem", crl.as_bytes().to_vec()),
    ];
    write_generation(tool_directory.path(), &tool_files, 1);
    let tool_generation = sider::security::Generation::tool(tool_directory.path()).unwrap();
    let tool_transport = tool_generation.internal.unwrap().transport;
    let options = |source, destination| sider::persistence::backup::ExportOptions {
        source,
        destination,
        source_sha: "a".repeat(40),
        limits: sider::persistence::backup::Limits::default(),
    };
    let accepted = directory.0.join("backup-accepted");
    sider::persistence::backup::export_secure(
        options(internal_address, accepted.clone()),
        Arc::new(sider::storage::SystemClock),
        &tool_transport,
    )
    .await
    .unwrap();
    assert!(accepted.join("backup-manifest.json").exists());

    drop(client);
    drop(stream);
    child.terminate(DEADLINE).unwrap();
    fs::remove_file(config.ready_file.as_ref().unwrap()).unwrap();
    fs::remove_file(
        config
            .replication
            .as_ref()
            .unwrap()
            .ready_file
            .as_ref()
            .unwrap(),
    )
    .unwrap();
    files[3].1 = serde_json::to_vec(&runtime_policy(&["monitor"])).unwrap();
    write_generation(security_directory.path(), &files, 2);
    let mut child = startup_process::OwnedChild::spawn(&mut command).unwrap();
    let denied_source = startup_ready(
        config
            .replication
            .as_ref()
            .unwrap()
            .ready_file
            .as_deref()
            .unwrap(),
    )
    .await;
    let denied = directory.0.join("backup-denied");
    assert!(
        sider::persistence::backup::export_secure(
            options(denied_source, denied.clone()),
            Arc::new(sider::storage::SystemClock),
            &tool_transport,
        )
        .await
        .is_err()
    );
    assert!(!denied.exists());
    child.terminate(DEADLINE).unwrap();
}

#[tokio::test]
async fn security_startup_live_server_replica_reconnects_with_rotated_runtime_identity() {
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let remote = certs.leaf(PEER, 2, None);
    let rotated = certs.leaf(PEER, 4, None);
    let until = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
    let crl = certs.crl(until, None);
    let source_directory = StartupDirectory::new();
    let source_config = source_directory.config();
    let source_preflight = sider::security::ServerPreflight::new(
        &source_config,
        runtime_generation(
            1,
            &certs,
            &primary,
            &crl,
            runtime_policy(&["replica", "monitor"]),
            false,
        ),
    )
    .unwrap();
    let source_runtime = source_preflight.runtime();
    let (source_stop, source_shutdown) = tokio::sync::oneshot::channel();
    let source = tokio::spawn(sider::server::serve_live(
        source_config.clone(),
        async {
            let _ = source_shutdown.await;
        },
        source_preflight,
    ));
    let source_address = startup_ready(
        source_config
            .replication
            .as_ref()
            .unwrap()
            .ready_file
            .as_deref()
            .unwrap(),
    )
    .await;
    let replica_directory = StartupDirectory::new();
    let mut replica_config = replica_directory.config();
    let config = replica_config.replication.as_mut().unwrap();
    config.upstream = Some(source_address);
    config.reconnect_min = Duration::from_millis(20);
    config.reconnect_max = Duration::from_millis(50);
    let outgoing = json!({"version":1,"instance_id":"holder-b","principals":[{"uri_san":SERVER,"roles":["monitor"]}],"upstream_principal":SERVER});
    let replica_preflight = sider::security::ServerPreflight::new(
        &replica_config,
        runtime_generation(1, &certs, &remote, &crl, outgoing.clone(), true),
    )
    .unwrap();
    let replica_runtime = replica_preflight.runtime();
    let (replica_stop, replica_shutdown) = tokio::sync::oneshot::channel();
    let replica = tokio::spawn(sider::server::serve_live(
        replica_config.clone(),
        async {
            let _ = replica_shutdown.await;
        },
        replica_preflight,
    ));
    let replica_address = startup_ready(
        replica_config
            .replication
            .as_ref()
            .unwrap()
            .ready_file
            .as_deref()
            .unwrap(),
    )
    .await;
    let monitor_policy = parse(
        json!({"version":1,"instance_id":"primary-a","principals":[],"upstream_principal":PEER}),
        true,
    );
    let monitor = certs.transport(&primary, &crl, monitor_policy, true);
    async fn status(tool: &InternalTransport, address: SocketAddr) -> Message {
        let mut stream = tool.connect(address).await.unwrap();
        protocol::write(
            &mut stream,
            &Message::StatusRequest,
            protocol::Limits::default(),
            DEADLINE,
        )
        .await
        .unwrap();
        protocol::read(&mut stream, protocol::Limits::default(), DEADLINE)
            .await
            .unwrap()
    }
    timeout(DEADLINE, async {
        loop {
            if matches!(
                status(&monitor, replica_address).await,
                Message::Status {
                    readonly: true,
                    connected: true,
                    full_syncs: 1,
                    ..
                }
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server dispatches live full replication");
    source_runtime
        .activate(runtime_generation(
            2,
            &certs,
            &primary,
            &certs.crl(until, Some(2)),
            runtime_policy(&["replica", "monitor"]),
            false,
        ))
        .await
        .unwrap();
    replica_runtime
        .activate(runtime_generation(
            2, &certs, &rotated, &crl, outgoing, true,
        ))
        .await
        .unwrap();
    timeout(DEADLINE, async {
        loop {
            if matches!(
                status(&monitor, replica_address).await,
                Message::Status {
                    connected: true,
                    partial_syncs: 1..,
                    ..
                }
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reconnect requires new certificate because primary revoked old serial");
    replica_stop.send(()).unwrap();
    timeout(DEADLINE, replica).await.unwrap().unwrap().unwrap();
    source_stop.send(()).unwrap();
    timeout(DEADLINE, source).await.unwrap().unwrap().unwrap();
}

#[test]
fn security_startup_executable_invalid_material_precedes_recovery_bind_and_readiness() {
    let directory = StartupDirectory::new();
    let occupied_client = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let occupied_internal = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_sider"));
    for (name, _) in std::env::vars_os() {
        if name.to_str().is_some_and(|name| name.starts_with("SIDER_")) {
            command.env_remove(name);
        }
    }
    command
        .env("SIDER_SECURITY_PROFILE", "protected")
        .env("SIDER_SECURITY_DIR", &directory.0)
        .env("SIDER_AOF_DIR", directory.0.join("aof"))
        .env(
            "SIDER_ADDR",
            occupied_client.local_addr().unwrap().to_string(),
        )
        .env(
            "SIDER_REPLICATION_ADDR",
            occupied_internal.local_addr().unwrap().to_string(),
        )
        .env("SIDER_READY_FILE", directory.0.join("client.ready"))
        .env(
            "SIDER_REPLICATION_READY_FILE",
            directory.0.join("internal.ready"),
        );
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut child = startup_process::OwnedChild::spawn(&mut command).unwrap();
    let output = child.wait(DEADLINE).unwrap();
    assert!(!output.status.success());
    #[cfg(windows)]
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("protected Windows profile unavailable")
    );
    #[cfg(unix)]
    assert!(String::from_utf8_lossy(&output.stderr).contains("security configuration unavailable"));
    assert!(!directory.0.join("aof").exists());
    assert!(!directory.0.join("client.ready").exists());
    assert!(!directory.0.join("internal.ready").exists());
}

#[tokio::test]
async fn security_startup_expired_preflight_rejects_before_recovery_or_binds() {
    let certs = Certificates::new();
    let expires = time::OffsetDateTime::now_utc() + time::Duration::seconds(2);
    let primary = certs.leaf(SERVER, 1, Some(expires));
    let crl = certs.crl(
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        None,
    );
    let directory = StartupDirectory::new();
    let config = directory.config();
    let preflight = sider::security::ServerPreflight::new(
        &config,
        runtime_generation(
            1,
            &certs,
            &primary,
            &crl,
            runtime_policy(&["monitor"]),
            false,
        ),
    )
    .unwrap();
    timeout(DEADLINE, async {
        while time::OffsetDateTime::now_utc().unix_timestamp() < expires.unix_timestamp() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        sider::server::serve_live(config, std::future::pending(), preflight).await,
        Err(sider::server::ServerError::Security(_))
    ));
    assert!(!directory.0.join("aof").exists());
    assert!(!directory.0.join("client.ready").exists());
    assert!(!directory.0.join("internal.ready").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn security_startup_posix_executable_validates_restricted_generation_and_serves_tls() {
    use sha2::{Digest, Sha256};
    use std::{fs, os::unix::fs::PermissionsExt};
    let path = fs::canonicalize(std::env::var_os("HOME").expect("service home"))
        .unwrap()
        .join(format!(
            ".sider-protected-startup-{}-{}",
            std::process::id(),
            time::OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
    fs::create_dir(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    let directory = StartupDirectory(path);
    let certs = Certificates::new();
    let primary = certs.leaf(SERVER, 1, None);
    let mut hashes = serde_json::Map::new();
    for (name, bytes) in [
        (
            "client-policy.json",
            br#"{"version":1,"users":[]}"#.as_slice(),
        ),
        ("client-cert.pem", primary.0.as_bytes()),
        ("client-key.pem", primary.1.as_bytes()),
    ] {
        let path = directory.0.join(name);
        fs::write(&path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        hashes.insert(
            name.into(),
            json!(
                Sha256::digest(bytes)
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            ),
        );
    }
    let manifest = directory.0.join("security-manifest.json");
    fs::write(
        &manifest,
        serde_json::to_vec(&json!({"version":1,"generation":1,"files":hashes})).unwrap(),
    )
    .unwrap();
    fs::set_permissions(manifest, fs::Permissions::from_mode(0o600)).unwrap();
    let ready = directory.0.join("ready.json");
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_sider"));
    for (name, _) in std::env::vars_os() {
        if name.to_str().is_some_and(|name| name.starts_with("SIDER_")) {
            command.env_remove(name);
        }
    }
    command
        .env("SIDER_SECURITY_DIR", &directory.0)
        .env("SIDER_ADDR", "127.0.0.1:0")
        .env("SIDER_READY_FILE", &ready);
    let mut child = startup_process::OwnedChild::spawn(&mut command).unwrap();
    let address = startup_ready(&ready).await;
    child.assert_alive().unwrap();
    let mut client = raw_connect(&raw_connector(&certs, None), address).await;
    client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    client.flush().await.unwrap();
    let mut reply = [0; 33];
    timeout(DEADLINE, client.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&reply, b"-NOAUTH Authentication required\r\n");
    child.terminate(DEADLINE).unwrap();
}
