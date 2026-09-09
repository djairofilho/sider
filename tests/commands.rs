//! 0.1 semantics without sockets or a runtime, using the verified literal oracle.

#![forbid(unsafe_code)]

#[path = "common/resp_fixtures.rs"]
mod resp_fixtures;

use bytes::{Bytes, BytesMut};
use sider::command::{Command, Reply, RequestError, parse};
use sider::resp::{Decoder, Frame, RespLimits, encode};
use sider::storage::Store;

fn request(arguments: &[&[u8]]) -> Frame {
    Frame::Array(Some(
        arguments
            .iter()
            .map(|value| Frame::Bulk(Some(Bytes::copy_from_slice(value))))
            .collect(),
    ))
}

fn get(store: &mut Store, key: &'static [u8]) -> Reply {
    store.execute(Command::Get {
        key: Bytes::from_static(key),
    })
}

fn execute(store: &mut Store, frame: Frame) -> Frame {
    match parse(frame) {
        Ok(command) => store.execute(command).into(),
        Err(error) => error.into_frame(),
    }
}

#[test]
fn five_commands_match_all_verified_redis_fixtures_without_network() {
    for case in resp_fixtures::CASES {
        let mut store = Store::new();
        // The cases also verify that their cleanup allows the sequence to be repeated.
        for _ in 0..2 {
            for (index, (request, expected)) in case.exchanges.iter().enumerate() {
                let mut decoder = Decoder::new(RespLimits::default()).unwrap();
                let mut input = BytesMut::from(*request);
                let frame = decoder.decode(&mut input).unwrap().unwrap();
                assert!(input.is_empty());
                let response = execute(&mut store, frame);
                let mut actual = BytesMut::new();
                encode(&response, &mut actual, RespLimits::default()).unwrap();
                assert_eq!(actual.as_ref(), *expected, "{}[{index}]", case.name);
            }
        }
    }
}

#[test]
fn every_rejected_set_option_preserves_existing_and_absent_keys() {
    let mut store = Store::new();
    assert_eq!(
        execute(&mut store, request(&[b"SET", b"k", b"original"])),
        Frame::from(Reply::Ok)
    );
    for key in [b"k".as_slice(), b"absent"] {
        for option in [
            vec![b"NX".as_slice(), b"XX"],
            vec![b"EX".as_slice()],
            vec![b"EX".as_slice(), b"10", b"PX", b"1000"],
            vec![b"PX".as_slice()],
            vec![b"GET".as_slice(), b"invalid"],
            vec![b"KEEPTTL".as_slice(), b"EX", b"10"],
            vec![b"invalid".as_slice()],
        ] {
            let mut args = vec![b"SET".as_slice(), key, b"replacement"];
            args.extend(option);
            assert_eq!(parse(request(&args)), Err(RequestError::Syntax));
            assert_eq!(
                execute(&mut store, request(&args)),
                RequestError::Syntax.into_frame()
            );
            assert_eq!(
                get(&mut store, b"k"),
                Reply::Bulk(Some(Bytes::from_static(b"original")))
            );
            assert_eq!(get(&mut store, b"absent"), Reply::Bulk(None));
        }
    }
}

#[test]
fn malformed_and_unknown_requests_do_not_reach_the_store() {
    let mut store = Store::new();
    store.execute(Command::Set {
        key: Bytes::from_static(b"k"),
        value: Bytes::from_static(b"original"),
    });
    let malformed = Frame::Array(Some(vec![
        Frame::Bulk(Some(Bytes::from_static(b"SET"))),
        Frame::Bulk(Some(Bytes::from_static(b"k"))),
        Frame::Bulk(None),
    ]));
    assert_eq!(parse(malformed.clone()), Err(RequestError::InvalidFormat));
    assert_eq!(
        execute(&mut store, malformed),
        RequestError::InvalidFormat.into_frame()
    );
    let unknown = request(&[b"unknown\xff", b"private-value"]);
    assert_eq!(
        execute(&mut store, unknown),
        Frame::Error(Bytes::from_static(b"ERR unknown command"))
    );
    assert_eq!(
        get(&mut store, b"k"),
        Reply::Bulk(Some(Bytes::from_static(b"original")))
    );
    assert_eq!(
        execute(&mut store, request(&[b"PING"])),
        Frame::from(Reply::Pong)
    );
}

#[test]
fn arity_errors_are_canonical_and_recoverable() {
    let cases: &[(&[&[u8]], &str)] = &[
        (&[b"pInG", b"a", b"b"], "ping"),
        (&[b"ECHO"], "echo"),
        (&[b"echo", b"a", b"b"], "echo"),
        (&[b"GET"], "get"),
        (&[b"GET", b"a", b"b"], "get"),
        (&[b"SET"], "set"),
        (&[b"SeT", b"key"], "set"),
        (&[b"DEL"], "del"),
    ];
    for (args, name) in cases {
        let error = parse(request(args)).unwrap_err();
        assert!(!error.is_fatal());
        assert_eq!(
            error.into_frame(),
            Frame::Error(Bytes::from(format!(
                "ERR wrong number of arguments for '{name}' command"
            )))
        );
    }
}

#[test]
fn protocol_commands_only_move_owned_payloads_into_storage() {
    let payload = Bytes::from(vec![0xff; 32_768]);
    let address = payload.as_ptr();
    let frame = Frame::Array(Some(vec![
        Frame::Bulk(Some(Bytes::from_static(b"SET"))),
        Frame::Bulk(Some(Bytes::from_static(b"key"))),
        Frame::Bulk(Some(payload)),
    ]));
    let mut store = Store::new();
    assert_eq!(execute(&mut store, frame), Frame::from(Reply::Ok));
    let Reply::Bulk(Some(value)) = get(&mut store, b"key") else {
        panic!("expected value")
    };
    assert_eq!(value.as_ptr(), address);
    assert_eq!(value.len(), 32_768);
}
