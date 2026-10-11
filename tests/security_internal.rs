//! Real mTLS boundaries for internal request roles, directions, and expiry.

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
    io::{AsyncReadExt, AsyncWriteExt},
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
        .signed_by(&self.issuer, &self.key)
        .unwrap()
        .pem()
        .unwrap()
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
