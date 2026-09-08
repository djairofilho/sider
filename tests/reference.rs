//! Referência externa opt-in. Não declara suporte no Sider antes de R01-05.

#![forbid(unsafe_code)]

#[path = "common/redis_reference.rs"]
mod redis_reference;
#[path = "common/resp_fixtures.rs"]
mod resp_fixtures;

use std::collections::HashSet;
use std::io::{BufRead, Cursor, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::process::Command;
use std::time::Duration;

use redis_reference::RedisReference;
use resp_fixtures::CASES;

// Validação independente e deliberadamente restrita aos requests literais.
// Não é o decoder do produto, nem gera os bytes das respostas esperadas.
fn literal_arguments(wire: &[u8]) -> Vec<Vec<u8>> {
    fn length(cursor: &mut Cursor<&[u8]>, marker: u8) -> usize {
        let mut line = Vec::new();
        cursor.read_until(b'\n', &mut line).unwrap();
        assert_eq!(line.first(), Some(&marker));
        assert!(line.ends_with(b"\r\n"));
        let value = std::str::from_utf8(&line[1..line.len() - 2])
            .unwrap()
            .parse::<usize>()
            .unwrap();
        assert!(value <= cursor.get_ref().len());
        value
    }

    let mut cursor = Cursor::new(wire);
    let count = length(&mut cursor, b'*');
    assert!(count > 0);
    let mut args = Vec::new();
    for _ in 0..count {
        let mut argument = vec![0; length(&mut cursor, b'$')];
        cursor.read_exact(&mut argument).unwrap();
        let mut terminator = [0; 2];
        cursor.read_exact(&mut terminator).unwrap();
        assert_eq!(&terminator, b"\r\n");
        args.push(argument);
    }
    assert_eq!(cursor.position(), wire.len() as u64);
    args
}

#[test]
fn fixtures_have_unique_names_and_exact_request_lengths() {
    let mut names = HashSet::new();
    let mut commands = HashSet::new();
    for case in CASES {
        assert!(names.insert(case.name));
        assert!(!case.exchanges.is_empty());
        for (request, response) in case.exchanges {
            let args = literal_arguments(request);
            commands.insert(args[0].to_ascii_uppercase());
            assert!(response.ends_with(b"\r\n"));
            assert!(matches!(response[0], b'+' | b'-' | b':' | b'$'));
        }
    }
    let expected: HashSet<_> = ["PING", "ECHO", "GET", "SET", "DEL"]
        .map(|command| command.as_bytes().to_vec())
        .into_iter()
        .collect();
    assert_eq!(commands, expected);
}

#[test]
fn fixtures_distinguish_null_empty_and_binary_payloads() {
    let responses: Vec<_> = CASES
        .iter()
        .flat_map(|case| case.exchanges.iter().map(|(_, response)| *response))
        .collect();
    for expected in [
        &b"+PONG\r\n"[..],
        &b"$-1\r\n"[..],
        &b"$0\r\n\r\n"[..],
        &b"$5\r\n\x00\r\n\xffA\r\n"[..],
        &b":0\r\n"[..],
        &b":1\r\n"[..],
        &b":2\r\n"[..],
    ] {
        assert!(responses.contains(&expected));
    }
    let args = literal_arguments(b"*2\r\n$4\r\nECHO\r\n$5\r\n\x00\r\n\xffA\r\n");
    assert_eq!(args[1], b"\x00\r\n\xffA");
    assert!(std::str::from_utf8(&args[1]).is_err());
}

#[test]
fn reference_fails_explicitly_when_docker_is_unavailable() {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "redis_8_10_1_matches_literal_fixtures",
            "--ignored",
            "--nocapture",
        ])
        .env("PATH", "")
        .output()
        .expect("executar teste filho sem Docker no PATH");
    assert!(
        !output.status.success(),
        "referência ausente não pode passar"
    );
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("Docker indisponível"), "{error}");
}

fn exchange(stream: &mut TcpStream, request: &[u8], expected: &[u8], context: &str) {
    stream.write_all(request).expect("escrita da fixture");
    let mut actual = vec![0; expected.len()];
    stream.read_exact(&mut actual).expect(context);
    assert_eq!(actual, expected, "{context}");
}

#[test]
#[ignore = "requer Docker ativo e pull da imagem fixada; execute com --ignored --nocapture"]
fn redis_8_10_1_matches_literal_fixtures() {
    let reference = RedisReference::start();
    let mut stream = TcpStream::connect_timeout(&reference.address(), Duration::from_secs(5))
        .expect("conexão com Redis descartável");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.set_nodelay(true).unwrap();

    let mut exchanges = 0;
    for case in CASES {
        for (index, (request, response)) in case.exchanges.iter().enumerate() {
            exchange(
                &mut stream,
                request,
                response,
                &format!("{}[{index}]", case.name),
            );
            exchanges += 1;
        }
        // Repete o caso como pipeline sem usar encoder ou decoder do Sider.
        let request: Vec<u8> = case
            .exchanges
            .iter()
            .flat_map(|(wire, _)| *wire)
            .copied()
            .collect();
        let expected: Vec<u8> = case
            .exchanges
            .iter()
            .flat_map(|(_, wire)| *wire)
            .copied()
            .collect();
        exchange(&mut stream, &request, &expected, case.name);
        exchange(
            &mut stream,
            b"*1\r\n$4\r\nPING\r\n",
            b"+PONG\r\n",
            "sentinela após pipeline",
        );
    }

    // Evidência separada da CLI. A saída textual não é o oráculo binário.
    assert_eq!(reference.cli(&["PING"]), b"PONG\n");
    assert_eq!(reference.cli(&["ECHO", "hello"]), b"hello\n");
    assert_eq!(reference.cli(&["SET", "cli-key", "value"]), b"OK\n");
    assert_eq!(reference.cli(&["GET", "cli-key"]), b"value\n");
    assert_eq!(reference.cli(&["DEL", "cli-key"]), b"1\n");
    stream.shutdown(Shutdown::Write).unwrap();
    let mut trailing = [0; 1];
    assert_eq!(
        stream.read(&mut trailing).expect("EOF após half-close"),
        0,
        "bytes excedentes após a última resposta: {trailing:?}"
    );
    drop(stream);
    reference.finish();
    println!(
        "Referência aprovada: {} casos, {exchanges} trocas sequenciais, {} pipelines, cinco comandos via CLI. Sider ainda não comparado.",
        CASES.len(),
        CASES.len()
    );
}
