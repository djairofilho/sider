//! Frozen RESP2 memory lifecycle exercised through a real TCP listener.
#![forbid(unsafe_code)]

#[path = "common/wire.rs"]
mod wire;

use sider::ServerConfig;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;
use wire::Response;

async fn tcp(config: ServerConfig, scenario: impl FnOnce(SocketAddr) + Send + 'static) {
    config.validate().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(sider::server::serve(listener, config, async {
        let _ = stopped.await;
    }));
    tokio::task::spawn_blocking(move || scenario(address))
        .await
        .unwrap();
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

fn connect(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
}
fn command(stream: &mut TcpStream, args: &[&[u8]]) -> wire::Observed {
    stream
        .write_all(&wire::request(
            &args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>(),
        ))
        .unwrap();
    wire::read_response(stream).unwrap()
}

fn owned_command(stream: &mut TcpStream, args: &[Vec<u8>]) -> wire::Observed {
    stream.write_all(&wire::request(args)).unwrap();
    wire::read_response(stream).unwrap()
}
fn bulk(value: &[u8]) -> Response {
    Response::Bulk(Some(value.to_vec()))
}
fn array(values: Vec<Response>) -> Response {
    Response::Array(Some(values))
}
fn vector() -> Vec<u8> {
    [(-0.0f32).to_le_bytes(), f32::from_bits(1).to_le_bytes()].concat()
}
fn put(key: &[u8], id: &[u8]) -> Vec<Vec<u8>> {
    [
        b"MEM.PUT".as_slice(),
        key,
        id,
        b"\0\xff",
        b"content\0\r\n\xff",
        b"p",
        b"m",
        b"r",
        b"t",
        &vector(),
        b"-1",
        b"UPSERT",
        b"0",
        b"0",
    ]
    .into_iter()
    .map(<[u8]>::to_vec)
    .collect()
}
fn create(client: &mut TcpStream, key: &[u8]) {
    assert_eq!(
        command(
            client,
            &[
                b"MEM.CREATE",
                key,
                b"1",
                b"2",
                b"L2SQ",
                b"p",
                b"m",
                b"r",
                b"t"
            ]
        )
        .bytes,
        b"+OK\r\n"
    );
}

#[tokio::test]
async fn memory_tcp_search_filters_before_top_k_and_returns_binary_distances() {
    tcp(ServerConfig::default(), |address| {
        let mut client = connect(address);
        create(&mut client, b"c");
        let mut nearest_ineligible = put(b"c", b"ineligible");
        nearest_ineligible[3] = b"other".to_vec();
        assert_eq!(
            owned_command(&mut client, &nearest_ineligible).bytes,
            b":1\r\n"
        );
        let mut eligible = put(b"c", b"a\0");
        eligible[9] = [1.0_f32.to_le_bytes(), f32::from_bits(1).to_le_bytes()].concat();
        eligible[12] = b"1".to_vec();
        eligible.splice(13..13, [b"rank".to_vec(), b"I64".to_vec(), b"-7".to_vec()]);
        assert_eq!(owned_command(&mut client, &eligible).bytes, b":1\r\n");
        let mut args = vec![
            b"MEM.SEARCH".to_vec(),
            b"c".to_vec(),
            b"p".to_vec(),
            b"m".to_vec(),
            b"r".to_vec(),
            b"t".to_vec(),
            vector(),
            b"1".to_vec(),
            b"\0\xff".to_vec(),
            b"1".to_vec(),
            b"rank".to_vec(),
            b"I64".to_vec(),
            b"-7".to_vec(),
        ];
        assert_eq!(
            owned_command(&mut client, &args).value,
            array(vec![array(vec![
                bulk(b"a\0"),
                bulk(&1.0_f64.to_le_bytes())
            ])])
        );
        args[11] = b"RANGE".to_vec();
        assert_eq!(
            owned_command(&mut client, &args).bytes,
            b"-ERR unsupported memory filter\r\n"
        );
        args[11] = b"I64".to_vec();
        args[1] = b"missing".to_vec();
        assert_eq!(owned_command(&mut client, &args).bytes, b"*0\r\n");
        args[1] = b"c".to_vec();
        args[7] = b"0".to_vec();
        assert_eq!(
            owned_command(&mut client, &args).bytes,
            b"-ERR memory k is out of range\r\n"
        );
        args[7] = b"1".to_vec();
        args[6] = [f32::NAN.to_le_bytes(), 0.0_f32.to_le_bytes()].concat();
        assert_eq!(
            owned_command(&mut client, &args).bytes,
            b"-ERR memory vector contains non-finite value\r\n"
        );
        args[6] = vector();
        args[2] = b"other-provider".to_vec();
        assert_eq!(
            owned_command(&mut client, &args).bytes,
            b"-ERR memory model identity mismatch\r\n"
        );
        args[2] = b"p".to_vec();
        args[9] = b"2".to_vec();
        args.extend([b"rank".to_vec(), b"I64".to_vec(), b"-7".to_vec()]);
        assert_eq!(
            owned_command(&mut client, &args).bytes,
            b"-ERR invalid memory argument\r\n"
        );
        let stats = command(&mut client, &[b"INFO", b"stats"]).bytes;
        for line in [
            b"memory_searches_total:2\r\n".as_slice(),
            b"memory_search_record_visits_total:2\r\n",
            b"memory_search_components_total:2\r\n",
            b"memory_rejections_total:5\r\n",
        ] {
            assert!(stats.windows(line.len()).any(|window| window == line));
        }
        let config = command(&mut client, &[b"INFO", b"config"]).bytes;
        assert!(
            config
                .windows(b"memory_contract_version:1\r\n".len())
                .any(|window| window == b"memory_contract_version:1\r\n")
        );
        assert!(
            config
                .windows(b"memory_max_search_components:1048576\r\n".len())
                .any(|window| window == b"memory_max_search_components:1048576\r\n")
        );
        assert_eq!(command(&mut client, &[b"PING"]).bytes, b"+PONG\r\n");
    })
    .await;
}

#[tokio::test]
async fn memory_tcp_binary_record_fields_identity_replacement_and_absence() {
    tcp(
        ServerConfig {
            shards: 2,
            ..ServerConfig::default()
        },
        |address| {
            let mut client = connect(address);
            let mut other = connect(address);
            let key = b"memory:{same}:\0\xff";
            let id = b"{different}:\xff\0";
            assert_eq!(command(&mut client, &[b"MEM.INFO", key]).bytes, b"$-1\r\n");
            assert_eq!(
                command(&mut client, &[b"MEM.GET", key, id]).bytes,
                b"$-1\r\n"
            );
            assert_eq!(
                command(&mut client, &[b"MEM.DEL", key, id]).bytes,
                b":0\r\n"
            );
            assert_eq!(
                command(&mut client, &[b"MEM.PTTL", key, id]).bytes,
                b":-2\r\n"
            );
            let missing = owned_command(&mut client, &put(key, id));
            assert_eq!(missing.bytes, b"-ERR memory collection does not exist\r\n");
            create(&mut client, key);
            assert_eq!(
                command(&mut other, &[b"mem.info", key]).value,
                array(vec![
                    Response::Integer(1),
                    Response::Integer(2),
                    bulk(b"L2SQ"),
                    bulk(b"p"),
                    bulk(b"m"),
                    bulk(b"r"),
                    bulk(b"t")
                ])
            );
            let mut args = put(key, id);
            args.truncate(12);
            args.extend(
                [
                    b"3".as_slice(),
                    b"\xff",
                    b"bool",
                    b"1",
                    b"z",
                    b"i64",
                    b"-9223372036854775808",
                    b"a\0",
                    b"bytes",
                    b"\0\r\n\xff",
                    b"2",
                    b"z",
                    b"last",
                    b"a",
                    b"first\xff",
                ]
                .into_iter()
                .map(<[u8]>::to_vec),
            );
            assert_eq!(owned_command(&mut client, &args).bytes, b":1\r\n");
            assert_eq!(
                command(&mut other, &[b"MEM.GET", key, id]).value,
                array(vec![
                    bulk(b"\0\xff"),
                    bulk(b"content\0\r\n\xff"),
                    bulk(&vector()),
                    Response::Integer(-1),
                    array(vec![
                        array(vec![bulk(b"a\0"), bulk(b"BYTES"), bulk(b"\0\r\n\xff")]),
                        array(vec![
                            bulk(b"z"),
                            bulk(b"I64"),
                            bulk(b"-9223372036854775808")
                        ]),
                        array(vec![bulk(b"\xff"), bulk(b"BOOL"), bulk(b"1")])
                    ]),
                    array(vec![
                        array(vec![bulk(b"a"), bulk(b"first\xff")]),
                        array(vec![bulk(b"z"), bulk(b"last")])
                    ])
                ])
            );
            assert_eq!(
                command(&mut client, &[b"MEM.PTTL", key, id]).bytes,
                b":-1\r\n"
            );
            create(&mut client, key);
            assert_eq!(
                command(&mut client, &[b"MEM.GET", key, id]).value,
                array(vec![
                    bulk(b"\0\xff"),
                    bulk(b"content\0\r\n\xff"),
                    bulk(&vector()),
                    Response::Integer(-1),
                    array(vec![
                        array(vec![bulk(b"a\0"), bulk(b"BYTES"), bulk(b"\0\r\n\xff")]),
                        array(vec![
                            bulk(b"z"),
                            bulk(b"I64"),
                            bulk(b"-9223372036854775808")
                        ]),
                        array(vec![bulk(b"\xff"), bulk(b"BOOL"), bulk(b"1")])
                    ]),
                    array(vec![
                        array(vec![bulk(b"a"), bulk(b"first\xff")]),
                        array(vec![bulk(b"z"), bulk(b"last")])
                    ])
                ])
            );
            let mut replacement = put(key, id);
            replacement[3] = b"other".to_vec();
            replacement[4] = vec![];
            assert_eq!(owned_command(&mut client, &replacement).bytes, b":1\r\n");
            assert_eq!(
                command(&mut other, &[b"MEM.GET", key, id]).value,
                array(vec![
                    bulk(b"other"),
                    bulk(b""),
                    bulk(&vector()),
                    Response::Integer(-1),
                    array(vec![]),
                    array(vec![])
                ])
            );
            assert_eq!(
                command(&mut client, &[b"MEM.DEL", key, id]).bytes,
                b":1\r\n"
            );
            assert_eq!(command(&mut client, &[b"EXISTS", key]).bytes, b":1\r\n");
            assert_eq!(
                command(&mut client, &[b"MEM.GET", key, id]).bytes,
                b"$-1\r\n"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn memory_tcp_invalid_requests_and_conditions_are_atomic_and_data_free() {
    tcp(ServerConfig::default(),|address| {
        let mut client=connect(address); create(&mut client,b"c");
        assert_eq!(owned_command(&mut client,&put(b"c",b"id")).bytes,b":1\r\n");
        let before=command(&mut client,&[b"MEM.GET",b"c",b"id"]).bytes;
        let cases:[(usize,Vec<u8>,&[u8]);9]=[
            (9,vec![0;4],b"ERR memory vector length mismatch"),
            (9,f32::INFINITY.to_le_bytes().repeat(2),b"ERR memory vector contains non-finite value"),
            (9,vec![0;7],b"ERR memory vector length mismatch"),
            (6,b"secret-identity".to_vec(),b"ERR memory model identity mismatch"),
            (10,b"0".to_vec(),b"ERR invalid memory ttl"),
            (10,b"-0".to_vec(),b"ERR invalid memory ttl"),
            (11,b"MAYBE".to_vec(),b"ERR invalid memory argument"),
            (12,b"01".to_vec(),b"ERR invalid memory argument"),
            (4,vec![0xff;65_537],b"ERR memory limit exceeded"),
        ];
        for (index,value,message) in cases {
            let mut args=put(b"c",b"id"); args[11]=b"NX".to_vec(); args[index]=value;
            assert_eq!(owned_command(&mut client,&args).value,Response::Error(message.to_vec()));
            assert_eq!(command(&mut client,&[b"MEM.GET",b"c",b"id"]).bytes,before);
        }
        for fields in [vec![b"2".as_slice(),b"f",b"BYTES",b"1",b"f",b"BOOL",b"1",b"0"],vec![b"1",b"f",b"I64",b"+1",b"0"],vec![b"1",b"f",b"BOOL",b"true",b"0"],vec![b"1",b"f",b"JSON",b"{}",b"0"],vec![b"0",b"2",b"f",b"a",b"f",b"b"]] {
            let mut args=put(b"c",b"id"); args.truncate(12); args.extend(fields.into_iter().map(<[u8]>::to_vec));
            assert_eq!(owned_command(&mut client,&args).value,Response::Error(b"ERR invalid memory argument".to_vec()));
        }
        for (arguments,expected) in [
            (vec![b"MEM.CREATE".as_slice(),b"c",b"2",b"2",b"L2SQ",b"p",b"m",b"r",b"t"],b"ERR unsupported memory schema version".as_slice()),
            (vec![b"MEM.CREATE",b"c",b"1",b"02",b"L2SQ",b"p",b"m",b"r",b"t"],b"ERR invalid memory argument"),
            (vec![b"MEM.CREATE",b"c",b"1",b"2",b"COSINE",b"p",b"m",b"r",b"t"],b"ERR unsupported memory metric"),
            (vec![b"MEM.CREATE",b"c",b"1",b"1",b"L2SQ",b"p",b"m",b"r",b"t"],b"ERR memory schema mismatch"),
            (vec![b"MEM.GET",b"c"],b"ERR wrong number of arguments for 'mem.get' command"),
            (vec![b"MEM.PUT",b"c"],b"ERR wrong number of arguments for 'mem.put' command"),
            (vec![b"MEM.EXPIRE",b"c",b"id",b"+1"],b"ERR invalid memory ttl"),
        ] {assert_eq!(command(&mut client,&arguments).value,Response::Error(expected.to_vec()));}
        let mut nx=put(b"c",b"id"); nx[11]=b"NX".to_vec(); assert_eq!(owned_command(&mut client,&nx).bytes,b":0\r\n");
        let mut xx=put(b"c",b"missing"); xx[11]=b"XX".to_vec(); assert_eq!(owned_command(&mut client,&xx).bytes,b":0\r\n");
        assert_eq!(command(&mut client,&[b"SET",b"wrong",b"value"]).bytes,b"+OK\r\n");
        let mut wrong=put(b"wrong",b"id"); wrong[9]=vec![0;4];
        assert!(matches!(owned_command(&mut client,&wrong).value,Response::Error(message) if message.starts_with(b"WRONGTYPE")));
        assert_eq!(command(&mut client,&[b"MEM.GET",b"c",b"id"]).bytes,before);
        assert_eq!(command(&mut client,&[b"PING"]).bytes,b"+PONG\r\n");
    }).await;
}

#[tokio::test]
async fn memory_tcp_nonqueueable_multi_and_watch_write_semantics() {
    tcp(ServerConfig::default(),|address| {
        let mut client=connect(address); let mut writer=connect(address); create(&mut client,b"c");
        let args=put(b"c",b"id"); assert_eq!(owned_command(&mut writer,&args).bytes,b":1\r\n");
        for invalidates in [false,true] {
            assert_eq!(command(&mut client,&[b"WATCH",b"c"]).bytes,b"+OK\r\n");
            if invalidates {assert_eq!(owned_command(&mut writer,&args).bytes,b":1\r\n");} else {create(&mut writer,b"c"); let mut nx=args.clone(); nx[11]=b"NX".to_vec(); assert_eq!(owned_command(&mut writer,&nx).bytes,b":0\r\n");}
            assert_eq!(command(&mut client,&[b"MULTI"]).bytes,b"+OK\r\n");
            assert_eq!(command(&mut client,&[b"EXEC"]).bytes,if invalidates {b"*-1\r\n".as_slice()} else {b"*0\r\n"});
        }
        for memory in [vec![b"MEM.INFO".as_slice(),b"c"],vec![b"MEM.GET",b"c",b"id"],vec![b"MEM.DEL",b"c",b"id"]] {
            assert_eq!(command(&mut client,&[b"MULTI"]).bytes,b"+OK\r\n");
            assert_eq!(command(&mut client,&[b"SET",b"never",b"written"]).bytes,b"+QUEUED\r\n");
            assert_eq!(command(&mut client,&memory).bytes,b"-ERR memory commands are not supported in MULTI\r\n");
            assert!(matches!(command(&mut client,&[b"EXEC"]).value,Response::Error(message) if message.starts_with(b"EXECABORT")));
            assert_eq!(command(&mut client,&[b"GET",b"never"]).bytes,b"$-1\r\n");
        }
        assert_eq!(command(&mut client,&[b"MEM.PTTL",b"c",b"id"]).bytes,b":-1\r\n");
    }).await;
}

#[tokio::test]
async fn memory_tcp_effective_reply_budget_returns_whole_error_and_keeps_connection() {
    for depth in [false, true] {
        let mut config = ServerConfig::default();
        if depth {
            config.resp_limits.max_depth = 1;
        } else {
            config.max_response_bytes = 140;
            config.resp_limits.max_bulk_bytes = 128;
        }
        tcp(config, move |address| {
            let mut client = connect(address);
            create(&mut client, b"c");
            let mut args = put(b"c", b"id");
            args[4] = vec![0xff; if depth { 256 } else { 128 }];
            assert_eq!(owned_command(&mut client, &args).bytes, b":1\r\n");
            assert_eq!(
                command(&mut client, &[b"MEM.GET", b"c", b"id"]).bytes,
                b"-ERR memory reply limit exceeded\r\n"
            );
            assert_eq!(
                command(&mut client, &[b"MEM.INFO", b"c"]).value,
                array(vec![
                    Response::Integer(1),
                    Response::Integer(2),
                    bulk(b"L2SQ"),
                    bulk(b"p"),
                    bulk(b"m"),
                    bulk(b"r"),
                    bulk(b"t")
                ])
            );
            assert_eq!(command(&mut client, &[b"PING"]).bytes, b"+PONG\r\n");
        })
        .await;
    }
}

#[tokio::test]
async fn memory_tcp_ttl_keep_persist_nonpositive_expire_and_collection_deadline() {
    tcp(ServerConfig::default(),|address| {
        let mut client=connect(address); create(&mut client,b"c");
        let mut args=put(b"c",b"id"); args[10]=b"60000".to_vec(); assert_eq!(owned_command(&mut client,&args).bytes,b":1\r\n");
        let before=command(&mut client,&[b"MEM.GET",b"c",b"id"]).value;
        args[10]=b"kEeP".to_vec(); assert_eq!(owned_command(&mut client,&args).bytes,b":1\r\n");
        assert_eq!(command(&mut client,&[b"MEM.GET",b"c",b"id"]).value,before);
        assert!(matches!(command(&mut client,&[b"MEM.PTTL",b"c",b"id"]).value,Response::Integer(value) if (1..=60000).contains(&value)));
        assert_eq!(command(&mut client,&[b"MEM.PERSIST",b"c",b"id"]).bytes,b":1\r\n");
        assert_eq!(command(&mut client,&[b"MEM.PERSIST",b"c",b"id"]).bytes,b":0\r\n");
        assert_eq!(command(&mut client,&[b"MEM.EXPIRE",b"c",b"id",b"60000"]).bytes,b":1\r\n");
        assert!(matches!(command(&mut client,&[b"MEM.GET",b"c",b"id"]).value,Response::Array(Some(values)) if matches!(values[3],Response::Integer(deadline) if deadline>0)));
        assert_eq!(command(&mut client,&[b"MEM.PERSIST",b"c",b"id"]).bytes,b":1\r\n");
        assert_eq!(command(&mut client,&[b"PEXPIRE",b"c",b"30000"]).bytes,b":1\r\n");
        assert!(matches!(command(&mut client,&[b"MEM.PTTL",b"c",b"id"]).value,Response::Integer(value) if (1..=30000).contains(&value)));
        assert_eq!(command(&mut client,&[b"MEM.EXPIRE",b"c",b"id",b"-1"]).bytes,b":1\r\n");
        assert_eq!(command(&mut client,&[b"MEM.GET",b"c",b"id"]).bytes,b"$-1\r\n");
        assert_eq!(command(&mut client,&[b"MEM.EXPIRE",b"c",b"missing",b"0"]).bytes,b":0\r\n");
        args[2]=b"new".to_vec(); assert_eq!(owned_command(&mut client,&args).bytes,b":1\r\n");
        assert!(matches!(command(&mut client,&[b"MEM.GET",b"c",b"new"]).value,Response::Array(Some(values)) if values[3]==Response::Integer(-1)));
        assert_eq!(command(&mut client,&[b"MEM.EXPIRE",b"c",b"new",b"0"]).bytes,b":1\r\n");
        assert_eq!(command(&mut client,&[b"EXISTS",b"c"]).bytes,b":1\r\n");
    }).await;
}

#[tokio::test]
async fn memory_tcp_request_contract_limit_is_recoverable_and_decoder_limit_closes() {
    tcp(ServerConfig::default(), |address| {
        let mut client = connect(address);
        assert_eq!(
            owned_command(&mut client, &[b"MEM.INFO".to_vec(), vec![0xff; 262_144]]).bytes,
            b"-ERR memory limit exceeded\r\n"
        );
        assert_eq!(command(&mut client, &[b"PING"]).bytes, b"+PONG\r\n");
    })
    .await;
    let mut config = ServerConfig::default();
    config.resp_limits.max_bulk_bytes = 128;
    tcp(config, |address| {
        use std::io::Read;
        let mut client = connect(address);
        client
            .write_all(&wire::request(&[b"MEM.INFO".to_vec(), vec![0xff; 129]]))
            .unwrap();
        assert_eq!(
            wire::read_response(&mut client).unwrap().bytes,
            b"-ERR invalid protocol\r\n"
        );
        let mut byte = [0];
        assert!(matches!(client.read(&mut byte), Ok(0) | Err(_)));
    })
    .await;
}

#[tokio::test]
async fn memory_tcp_create_uses_frozen_resp2_grammar() {
    tcp(ServerConfig::default(), |address| {
        let mut client = connect(address);
        let observed = command(
            &mut client,
            &[
                b"MEM.CREATE",
                b"c\xff",
                b"1",
                b"2",
                b"L2SQ",
                b"p",
                b"m",
                b"r",
                b"t",
            ],
        );
        assert_eq!(observed.bytes, b"+OK\r\n");
        assert_eq!(observed.value, Response::Simple(b"OK".to_vec()));
    })
    .await;
}

#[tokio::test]
async fn memory_tcp_default_record1_aof_refuses_memory_without_dataset_or_durable_change() {
    let directory = std::env::temp_dir().join(format!(
        "sider-memory-tcp-aof-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let config = ServerConfig {
        aof: Some(sider::persistence::AofConfig::new(directory.clone())),
        ..ServerConfig::default()
    };
    tcp(config, |address| {
        use std::io::Read;
        let mut client = connect(address);
        client
            .write_all(&wire::request(&[
                b"MEM.CREATE".to_vec(),
                b"never".to_vec(),
                b"1".to_vec(),
                b"2".to_vec(),
                b"L2SQ".to_vec(),
                b"p".to_vec(),
                b"m".to_vec(),
                b"r".to_vec(),
                b"t".to_vec(),
            ]))
            .unwrap();
        let mut byte = [0];
        assert_eq!(
            client.read(&mut byte).unwrap(),
            0,
            "unsupported durable profile must not acknowledge a memory write"
        );
        let mut other = connect(address);
        assert_eq!(command(&mut other, &[b"EXISTS", b"never"]).bytes, b":0\r\n");
        assert_eq!(command(&mut other, &[b"GET", b"never"]).bytes, b"$-1\r\n");
        assert_eq!(command(&mut other, &[b"PING"]).bytes, b"+PONG\r\n");
    })
    .await;
    let recovered = sider::persistence::recover(
        sider::persistence::AofConfig::new(directory.clone()),
        sider::storage::StoreConfig::default(),
        std::sync::Arc::new(sider::storage::SystemClock),
    )
    .unwrap();
    assert_eq!(recovered.metadata.format_version, 2);
    assert_eq!(recovered.metadata.sequence, 0);
    assert!(recovered.store.is_empty());
    drop(recovered);
    assert_eq!(
        directory.canonicalize().unwrap().parent(),
        Some(std::env::temp_dir().canonicalize().unwrap().as_path())
    );
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn memory_tcp_explicit_record2_survives_restart_and_refuses_legacy_reader() {
    let directory = std::env::temp_dir().join(format!(
        "sider-memory-tcp-record2-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let config = ServerConfig::from_lookup(|name| match name {
        "SIDER_AOF_DIR" => Some(directory.as_os_str().to_owned()),
        "SIDER_AOF_RECORD_VERSION" => Some("2".into()),
        _ => None,
    })
    .unwrap();
    tcp(config.clone(), |address| {
        let mut client = connect(address);
        create(&mut client, b"c");
        assert_eq!(
            owned_command(&mut client, &put(b"c", b"\xff\0")).bytes,
            b":1\r\n"
        );
        assert!(matches!(
            command(&mut client, &[b"MEM.GET", b"c", b"\xff\0"]).value,
            Response::Array(Some(_))
        ));
    })
    .await;
    let generation = directory.join("generation-00000000000000000000.aof");
    let before = std::fs::read(&generation).unwrap();
    assert_eq!(&before[..8], b"SIDERAOF");
    assert_eq!(u32::from_le_bytes(before[8..12].try_into().unwrap()), 4);
    assert!(
        sider::persistence::recover(
            sider::persistence::AofConfig::new(directory.clone()),
            sider::storage::StoreConfig::default(),
            std::sync::Arc::new(sider::storage::SystemClock),
        )
        .is_err()
    );
    assert_eq!(std::fs::read(&generation).unwrap(), before);
    tcp(config.clone(), |address| {
        let mut client = connect(address);
        let recovered = command(&mut client, &[b"MEM.GET", b"c", b"\xff\0"]);
        assert!(matches!(
            recovered.value,
            Response::Array(Some(ref values))
                if values[0] == bulk(b"\0\xff")
                    && values[1] == bulk(b"content\0\r\n\xff")
                    && values[2] == bulk(&vector())
        ));
        assert_eq!(
            command(&mut client, &[b"MEM.DEL", b"c", b"\xff\0"]).bytes,
            b":1\r\n"
        );
    })
    .await;
    tcp(config, |address| {
        let mut client = connect(address);
        assert_eq!(
            command(&mut client, &[b"MEM.GET", b"c", b"\xff\0"]).bytes,
            b"$-1\r\n"
        );
    })
    .await;
    assert_eq!(
        directory.canonicalize().unwrap().parent(),
        Some(std::env::temp_dir().canonicalize().unwrap().as_path())
    );
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn memory_tcp_typed_command_name_and_routing_visit_only_collection_key() {
    use bytes::Bytes;
    use sider::command::Command;
    use sider::resp::Frame;
    let command = sider::command::parse(Frame::Array(Some(
        put(b"collection:{owner}", b"member:{other}")
            .into_iter()
            .map(|value| Frame::Bulk(Some(Bytes::from(value))))
            .collect(),
    )))
    .unwrap();
    let mut keys = Vec::new();
    command.visit_keys(|key| keys.push(key.clone()));
    assert_eq!(keys, vec![Bytes::from_static(b"collection:{owner}")]);
    assert!(command.writes_dataset());
    let Command::Memory { operation, .. } = command else {
        panic!("typed memory command")
    };
    assert_eq!(operation.command_name(), "MEM.PUT");
    assert_eq!(sider::command::memory::MAX_REQUEST_BYTES, 262_144);
    assert_eq!(sider::command::memory::MAX_REPLY_BYTES, 131_072);
}

fn parsed(args: Vec<Vec<u8>>) -> sider::command::Command {
    use bytes::Bytes;
    use sider::resp::Frame;
    sider::command::parse(Frame::Array(Some(
        args.into_iter()
            .map(|value| Frame::Bulk(Some(Bytes::from(value))))
            .collect(),
    )))
    .unwrap()
}

fn native_create() -> sider::command::Command {
    parsed(
        [
            b"MEM.CREATE".as_slice(),
            b"c",
            b"1",
            b"2",
            b"L2SQ",
            b"p",
            b"m",
            b"r",
            b"t",
        ]
        .into_iter()
        .map(<[u8]>::to_vec)
        .collect(),
    )
}

#[tokio::test(start_paused = true)]
async fn memory_tcp_native_dispatch_preserves_monotonic_member_deadlines() {
    use sider::command::Reply;
    struct FixedWallClock;
    impl sider::storage::Clock for FixedWallClock {
        fn now(&self) -> tokio::time::Instant {
            tokio::time::Instant::now()
        }
        fn unix_millis(&self) -> i64 {
            1000
        }
    }
    let mut store = sider::storage::Store::with_clock(std::sync::Arc::new(FixedWallClock));
    assert_eq!(store.execute(native_create()), Reply::Ok);
    let mut args = put(b"c", b"i");
    args[10] = b"10".to_vec();
    assert_eq!(store.execute(parsed(args)), Reply::Integer(1));
    tokio::time::advance(Duration::from_millis(10)).await;
    assert_eq!(
        store.execute(parsed(vec![
            b"MEM.GET".to_vec(),
            b"c".to_vec(),
            b"i".to_vec()
        ])),
        Reply::Bulk(None),
        "wall-clock rollback must not resurrect an elapsed monotonic member"
    );
    assert_eq!(store.used_bytes(), 389);
}

#[test]
fn memory_tcp_native_generic_preparation_and_mixed_batches_fail_closed() {
    use bytes::Bytes;
    use sider::command::{Command, ExecutionError, Reply};
    let mut store = sider::storage::Store::new();
    let prepared = store.prepare(native_create());
    assert_eq!(
        prepared.reply,
        Reply::Error(ExecutionError::Memory(
            "ERR memory commands require native preparation"
        ))
    );
    assert!(prepared.batch.mutations.is_empty());
    store.apply(prepared);
    assert!(store.is_empty());
    let prepared = store.prepare_batch(vec![
        Command::Set {
            key: Bytes::from_static(b"first"),
            value: Bytes::from_static(b"v"),
        },
        native_create(),
    ]);
    assert_eq!(
        prepared.reply,
        Reply::Error(ExecutionError::Memory(
            "ERR memory commands are not supported in MULTI"
        ))
    );
    assert!(prepared.batch.mutations.is_empty());
    store.apply(prepared);
    assert!(store.is_empty());
}

#[tokio::test]
async fn memory_tcp_native_worker_batch_refuses_before_queueing_or_effects() {
    use bytes::Bytes;
    use sider::command::{Command, ExecutionError, Reply};
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let (db, worker) = sider::storage::worker::channel(1, Duration::from_secs(1), stopped).unwrap();
    // No worker task exists yet: refusal must happen before queue acceptance.
    assert_eq!(
        db.execute_batch(
            vec![
                Command::Set {
                    key: Bytes::from_static(b"first"),
                    value: Bytes::from_static(b"v")
                },
                native_create(),
            ],
            vec![]
        )
        .await
        .unwrap(),
        Reply::Error(ExecutionError::Memory(
            "ERR memory commands are not supported in MULTI"
        ))
    );
    let task = tokio::spawn(worker.run());
    assert_eq!(
        db.execute(Command::Get {
            key: Bytes::from_static(b"first")
        })
        .await
        .unwrap(),
        Reply::Bulk(None)
    );
    assert_eq!(
        db.execute(parsed(vec![b"MEM.INFO".to_vec(), b"c".to_vec()]))
            .await
            .unwrap(),
        Reply::Bulk(None)
    );
    stop.send(true).unwrap();
    task.await.unwrap();
}
