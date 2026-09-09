//! Additional string contracts, with real parsing and RESP responses.

#![forbid(unsafe_code)]

use bytes::{Bytes, BytesMut};
use sider::command::{Command, ExecutionError, Reply, RequestError, parse};
use sider::resp::{Frame, RespLimits, encode};
use sider::storage::Store;

fn request(args: &[&[u8]]) -> Frame {
    Frame::Array(Some(
        args.iter()
            .map(|arg| Frame::Bulk(Some(Bytes::copy_from_slice(arg))))
            .collect(),
    ))
}

fn execute(store: &mut Store, args: &[&[u8]]) -> Reply {
    store.execute(parse(request(args)).unwrap())
}

#[test]
fn mset_mget_exists_preserve_binary_order_duplicates_and_empty_values() {
    let mut store = Store::new();
    assert_eq!(
        execute(
            &mut store,
            &[b"mSeT", b"\0\xff", b"first", b"", b"", b"\0\xff", b"last"]
        ),
        Reply::Ok
    );
    assert_eq!(
        execute(
            &mut store,
            &[b"EXISTS", b"\0\xff", b"absent", b"", b"\0\xff"]
        ),
        Reply::Integer(3)
    );
    let reply = execute(&mut store, &[b"MGET", b"\0\xff", b"absent", b"", b"\0\xff"]);
    let mut output = BytesMut::new();
    encode(&reply.into(), &mut output, RespLimits::default()).unwrap();
    assert_eq!(
        output.as_ref(),
        b"*4\r\n$4\r\nlast\r\n$-1\r\n$0\r\n\r\n$4\r\nlast\r\n"
    );
    assert_eq!(store.execute(Command::MSet { entries: vec![] }), Reply::Ok);
}

#[test]
fn increments_reject_noncanonical_and_overflow_without_mutation() {
    let mut store = Store::new();
    assert_eq!(
        execute(&mut store, &[b"INCR", b"missing"]),
        Reply::Integer(1)
    );
    assert_eq!(
        execute(&mut store, &[b"DECR", b"negative"]),
        Reply::Integer(-1)
    );
    for invalid in [
        b"".as_slice(),
        b"+1",
        b"-0",
        b"00",
        b"01",
        b"-01",
        b" 1",
        b"1 ",
        b"1\0",
        b"\xff",
        b"9223372036854775808",
        b"-9223372036854775809",
    ] {
        execute(&mut store, &[b"SET", b"n", invalid]);
        for command in [b"INCR".as_slice(), b"DECR"] {
            assert_eq!(
                execute(&mut store, &[command, b"n"]),
                Reply::Error(ExecutionError::InvalidInteger)
            );
            assert_eq!(
                execute(&mut store, &[b"GET", b"n"]),
                Reply::Bulk(Some(Bytes::copy_from_slice(invalid)))
            );
        }
    }
    for (command, boundary) in [
        (b"INCR".as_slice(), b"9223372036854775807".as_slice()),
        (b"DECR", b"-9223372036854775808"),
    ] {
        execute(&mut store, &[b"SET", b"n", boundary]);
        assert_eq!(
            execute(&mut store, &[command, b"n"]),
            Reply::Error(ExecutionError::IntegerOverflow)
        );
        assert_eq!(
            execute(&mut store, &[b"GET", b"n"]),
            Reply::Bulk(Some(Bytes::copy_from_slice(boundary)))
        );
    }
}

#[test]
fn arity_and_full_format_are_checked_before_mset_mutation() {
    let cases: &[(&[&[u8]], &str)] = &[
        (&[b"EXISTS"], "exists"),
        (&[b"MGET"], "mget"),
        (&[b"INCR"], "incr"),
        (&[b"INCR", b"a", b"b"], "incr"),
        (&[b"DECR"], "decr"),
        (&[b"MSET"], "mset"),
        (&[b"MSET", b"a"], "mset"),
        (&[b"MSET", b"a", b"v", b"b"], "mset"),
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
    let mut frame = request(&[b"MSET", b"a", b"value", b"b", b"value"]);
    if let Frame::Array(Some(args)) = &mut frame {
        args[4] = Frame::Bulk(None);
    }
    assert_eq!(parse(frame), Err(RequestError::InvalidFormat));
}
