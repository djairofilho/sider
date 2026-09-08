//! Varredura incremental com metadados limitados e materialização tardia.

use bytes::{Buf, Bytes, BytesMut};

use super::{Frame, ProtocolError, RespLimits};

#[derive(Clone, Copy, Debug)]
struct Payload {
    start: usize,
    end: usize,
}

#[derive(Debug)]
enum Node {
    Simple(Payload),
    Error(Payload),
    Integer(i64),
    Bulk(Option<Payload>),
    Array(Option<usize>),
}

#[derive(Clone, Copy, Debug)]
struct Line {
    kind: u8,
    start: usize,
    payload_start: usize,
    saw_cr: bool,
}

#[derive(Clone, Copy, Debug)]
enum State {
    Prefix,
    Line(Line),
    Bulk { payload: Payload, saw_cr: bool },
}

/// Decoder RESP2 pertencente a uma única conexão.
///
/// Enquanto [`Self::decode`] retornar `Ok(None)`, o chamador só pode acrescentar
/// bytes ao final do mesmo conteúdo. Não remova nem modifique o prefixo já
/// fornecido. Realocações de `BytesMut` são permitidas; uma redução de comprimento
/// é detectada, mas modificações dos bytes antigos não são verificadas novamente.
///
/// O scanner preserva cursor, pilha e metadados entre chamadas. Nenhum payload é
/// copiado antes da validação completa. Em erro, o buffer permanece intacto e o
/// decoder fica permanentemente encerrado; descarte também a conexão.
#[derive(Debug)]
pub struct Decoder {
    limits: RespLimits,
    cursor: usize,
    previous_len: usize,
    state: State,
    nodes: Vec<Node>,
    arrays: Vec<usize>,
    nodes_started: usize,
    pending_nodes: usize,
    poisoned: bool,
    #[cfg(test)]
    work: usize,
}

impl Decoder {
    /// Cria um scanner vazio após validar todos os limites.
    pub fn new(limits: RespLimits) -> Result<Self, crate::ConfigError> {
        limits.validate()?;
        Ok(Self {
            limits,
            cursor: 0,
            previous_len: 0,
            state: State::Prefix,
            nodes: Vec::new(),
            arrays: Vec::new(),
            nodes_started: 0,
            pending_nodes: 1,
            poisoned: false,
            #[cfg(test)]
            work: 0,
        })
    }

    /// Consome exatamente um frame completo, preservando o sufixo concatenado.
    ///
    /// `Ok(None)` e `Err` nunca alteram `src`. Uma falha é terminal, inclusive
    /// quando causada pela redução indevida de um buffer incompleto.
    pub fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, ProtocolError> {
        if self.poisoned {
            return Err(ProtocolError::Poisoned);
        }
        if src.len() < self.previous_len {
            return self.fail(ProtocolError::BufferChanged);
        }
        self.previous_len = src.len();
        match self.scan(src) {
            Ok(false) => Ok(None),
            Ok(true) => {
                let frame = match self.materialize(src) {
                    Ok(frame) => frame,
                    Err(error) => return self.fail(error),
                };
                src.advance(self.cursor);
                self.cursor = 0;
                self.previous_len = 0;
                self.state = State::Prefix;
                self.nodes.clear();
                self.arrays.clear();
                self.nodes_started = 0;
                self.pending_nodes = 1;
                Ok(Some(frame))
            }
            Err(error) => self.fail(error),
        }
    }

    fn fail<T>(&mut self, error: ProtocolError) -> Result<T, ProtocolError> {
        self.poisoned = true;
        self.nodes.clear();
        self.arrays.clear();
        Err(error)
    }

    fn scan(&mut self, src: &[u8]) -> Result<bool, ProtocolError> {
        loop {
            #[cfg(test)]
            {
                self.work += 1;
            }
            match self.state {
                State::Prefix => {
                    if self.nodes_started > 0 {
                        self.require_frame_end(checked_add(self.cursor, 3)?)?;
                    }
                    let Some(&kind) = src.get(self.cursor) else {
                        return Ok(false);
                    };
                    if !matches!(kind, b'+' | b'-' | b':' | b'$' | b'*') {
                        return Err(ProtocolError::Malformed("prefixo desconhecido"));
                    }
                    if kind == b'*' && self.arrays.len() >= self.limits.max_depth {
                        return Err(ProtocolError::LimitExceeded("max_depth"));
                    }
                    self.nodes_started = self
                        .nodes_started
                        .checked_add(1)
                        .filter(|count| *count <= self.limits.max_nodes)
                        .ok_or(ProtocolError::LimitExceeded("max_nodes"))?;
                    self.pending_nodes = self
                        .pending_nodes
                        .checked_sub(1)
                        .ok_or(ProtocolError::Malformed("nó sem pai"))?;
                    let start = self.cursor;
                    self.cursor = checked_add(self.cursor, 1)?;
                    self.state = State::Line(Line {
                        kind,
                        start,
                        payload_start: self.cursor,
                        saw_cr: false,
                    });
                }
                State::Line(mut line) => {
                    // Mesmo sem bytes disponíveis, uma linha que já não comporta
                    // seu terminador não pode vir a ser um frame válido.
                    let minimum_end = checked_add(self.cursor, if line.saw_cr { 1 } else { 2 })?;
                    self.require_frame_end(minimum_end)?;
                    let line_len = minimum_end
                        .checked_sub(line.start)
                        .ok_or(ProtocolError::Malformed("offset de linha inválido"))?;
                    if line_len > self.limits.max_line_bytes {
                        return Err(ProtocolError::LimitExceeded("max_line_bytes"));
                    }
                    let Some(&byte) = src.get(self.cursor) else {
                        return Ok(false);
                    };
                    self.cursor = checked_add(self.cursor, 1)?;
                    if line.saw_cr {
                        if byte != b'\n' {
                            return Err(ProtocolError::Malformed("CR sem LF"));
                        }
                        let payload = Payload {
                            start: line.payload_start,
                            end: self
                                .cursor
                                .checked_sub(2)
                                .ok_or(ProtocolError::Malformed("offset de linha inválido"))?,
                        };
                        if self.finish_line(src, line.kind, payload)? {
                            return Ok(true);
                        }
                    } else {
                        match byte {
                            b'\n' => return Err(ProtocolError::Malformed("LF sem CR")),
                            b'\r' => line.saw_cr = true,
                            _ => {}
                        }
                        self.state = State::Line(line);
                    }
                }
                State::Bulk {
                    payload,
                    mut saw_cr,
                } => {
                    // O payload é binário: basta avançar sobre os novos bytes,
                    // sem inspecionar seu conteúdo nem recopiá-lo por fragmento.
                    if self.cursor < payload.end {
                        self.cursor = src.len().min(payload.end);
                        if self.cursor < payload.end {
                            return Ok(false);
                        }
                    }
                    let Some(&byte) = src.get(self.cursor) else {
                        return Ok(false);
                    };
                    self.cursor = checked_add(self.cursor, 1)?;
                    if saw_cr {
                        if byte != b'\n' {
                            return Err(ProtocolError::Malformed("terminador bulk inválido"));
                        }
                        self.nodes.push(Node::Bulk(Some(payload)));
                        if self.finish_node()? {
                            return Ok(true);
                        }
                    } else {
                        if byte != b'\r' {
                            return Err(ProtocolError::Malformed("terminador bulk inválido"));
                        }
                        saw_cr = true;
                        self.state = State::Bulk { payload, saw_cr };
                    }
                }
            }
        }
    }

    fn finish_line(
        &mut self,
        src: &[u8],
        kind: u8,
        payload: Payload,
    ) -> Result<bool, ProtocolError> {
        match kind {
            b'+' => self.nodes.push(Node::Simple(payload)),
            b'-' => self.nodes.push(Node::Error(payload)),
            b':' => {
                let bytes = payload_slice(src, payload)?;
                #[cfg(test)]
                {
                    self.work += bytes.len();
                }
                self.nodes.push(Node::Integer(parse_integer(bytes)?));
            }
            b'$' | b'*' => {
                let bytes = payload_slice(src, payload)?;
                #[cfg(test)]
                {
                    self.work += bytes.len();
                }
                let length = parse_length(bytes)?;
                if kind == b'$' {
                    if let Some(length) = length {
                        if length > self.limits.max_bulk_bytes {
                            return Err(ProtocolError::LimitExceeded("max_bulk_bytes"));
                        }
                        let end = checked_add(self.cursor, length)?;
                        self.require_frame_end(checked_add(end, 2)?)?;
                        self.state = State::Bulk {
                            payload: Payload {
                                start: self.cursor,
                                end,
                            },
                            saw_cr: false,
                        };
                        return Ok(false);
                    }
                    self.nodes.push(Node::Bulk(None));
                } else {
                    if let Some(length) = length {
                        self.pending_nodes = self
                            .pending_nodes
                            .checked_add(length)
                            .ok_or(ProtocolError::LimitExceeded("max_nodes"))?;
                        let declared_nodes = self
                            .nodes_started
                            .checked_add(self.pending_nodes)
                            .ok_or(ProtocolError::LimitExceeded("max_nodes"))?;
                        if declared_nodes > self.limits.max_nodes {
                            return Err(ProtocolError::LimitExceeded("max_nodes"));
                        }
                        // Cada nó futuro precisa de ao menos "+\r\n". Esta
                        // verificação não reserva memória pelo tamanho declarado.
                        let minimum_bytes = self
                            .pending_nodes
                            .checked_mul(3)
                            .ok_or(ProtocolError::LimitExceeded("max_frame_bytes"))?;
                        self.require_frame_end(checked_add(self.cursor, minimum_bytes)?)?;
                    }
                    self.nodes.push(Node::Array(length));
                    if let Some(length) = length.filter(|length| *length > 0) {
                        self.arrays.push(length);
                        self.state = State::Prefix;
                        return Ok(false);
                    }
                }
            }
            _ => return Err(ProtocolError::Malformed("prefixo desconhecido")),
        }
        self.finish_node()
    }

    fn finish_node(&mut self) -> Result<bool, ProtocolError> {
        self.state = State::Prefix;
        while let Some(remaining) = self.arrays.last_mut() {
            #[cfg(test)]
            {
                self.work += 1;
            }
            *remaining = remaining
                .checked_sub(1)
                .ok_or(ProtocolError::Malformed("array já concluído"))?;
            if *remaining > 0 {
                return Ok(false);
            }
            self.arrays.pop();
        }
        Ok(true)
    }

    fn require_frame_end(&self, end: usize) -> Result<(), ProtocolError> {
        if end > self.limits.max_frame_bytes {
            Err(ProtocolError::LimitExceeded("max_frame_bytes"))
        } else {
            Ok(())
        }
    }

    fn materialize(&mut self, src: &[u8]) -> Result<Frame, ProtocolError> {
        // A ordem inversa permite montar a árvore sem recursão. Neste ponto,
        // todos os nós e payloads já passaram pelos limites e pela sintaxe.
        let mut frames = Vec::new();
        for node in self.nodes.iter().rev() {
            #[cfg(test)]
            {
                self.work += 1;
            }
            let frame = match *node {
                Node::Simple(payload) => Frame::Simple(copy_payload(src, payload)?),
                Node::Error(payload) => Frame::Error(copy_payload(src, payload)?),
                Node::Integer(value) => Frame::Integer(value),
                Node::Bulk(None) => Frame::Bulk(None),
                Node::Bulk(Some(payload)) => Frame::Bulk(Some(copy_payload(src, payload)?)),
                Node::Array(None) => Frame::Array(None),
                Node::Array(Some(length)) => {
                    let mut children = Vec::with_capacity(length);
                    for _ in 0..length {
                        children.push(
                            frames
                                .pop()
                                .ok_or(ProtocolError::Malformed("array incompleto"))?,
                        );
                    }
                    Frame::Array(Some(children))
                }
            };
            frames.push(frame);
        }
        let result = frames
            .pop()
            .ok_or(ProtocolError::Malformed("frame sem raiz"))?;
        if !frames.is_empty() {
            return Err(ProtocolError::Malformed("frame com múltiplas raízes"));
        }
        Ok(result)
    }
}

fn checked_add(left: usize, right: usize) -> Result<usize, ProtocolError> {
    left.checked_add(right)
        .ok_or(ProtocolError::LimitExceeded("max_frame_bytes"))
}

fn payload_slice(src: &[u8], payload: Payload) -> Result<&[u8], ProtocolError> {
    src.get(payload.start..payload.end)
        .ok_or(ProtocolError::BufferChanged)
}

fn copy_payload(src: &[u8], payload: Payload) -> Result<Bytes, ProtocolError> {
    Ok(Bytes::copy_from_slice(payload_slice(src, payload)?))
}

fn parse_integer(bytes: &[u8]) -> Result<i64, ProtocolError> {
    let (negative, digits) = match bytes.first() {
        Some(b'-') => (true, &bytes[1..]),
        Some(b'+') => (false, &bytes[1..]),
        _ => (false, bytes),
    };
    if digits.is_empty() {
        return Err(ProtocolError::Malformed("inteiro sem dígitos"));
    }
    // Acumular na faixa negativa admite MIN sem precisar negar seu módulo.
    let mut value = 0_i64;
    for &digit in digits {
        if !digit.is_ascii_digit() {
            return Err(ProtocolError::Malformed("inteiro inválido"));
        }
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_sub(i64::from(digit - b'0')))
            .ok_or(ProtocolError::Malformed("inteiro fora do intervalo"))?;
    }
    if negative {
        Ok(value)
    } else {
        value
            .checked_neg()
            .ok_or(ProtocolError::Malformed("inteiro fora do intervalo"))
    }
}

fn parse_length(bytes: &[u8]) -> Result<Option<usize>, ProtocolError> {
    if bytes == b"-1" {
        return Ok(None);
    }
    if bytes.is_empty() {
        return Err(ProtocolError::Malformed("comprimento sem dígitos"));
    }
    let mut value = 0_usize;
    for &digit in bytes {
        if !digit.is_ascii_digit() {
            return Err(ProtocolError::Malformed("comprimento inválido"));
        }
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(usize::from(digit - b'0')))
            .ok_or(ProtocolError::Malformed("comprimento fora do intervalo"))?;
    }
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> RespLimits {
        RespLimits {
            max_frame_bytes: 4096,
            max_bulk_bytes: 1024,
            max_line_bytes: 128,
            max_nodes: 64,
            max_depth: 4,
        }
    }

    fn decode_all(wire: &[u8]) -> Frame {
        let mut decoder = Decoder::new(limits()).unwrap();
        let mut src = BytesMut::from(wire);
        let frame = decoder.decode(&mut src).unwrap().unwrap();
        assert!(src.is_empty());
        frame
    }

    fn assert_error(wire: &[u8], limits: RespLimits, expected: ProtocolError) {
        let mut decoder = Decoder::new(limits).unwrap();
        let mut src = BytesMut::from(wire);
        assert_eq!(decoder.decode(&mut src), Err(expected));
        assert_eq!(src.as_ref(), wire);
        assert_eq!(decoder.decode(&mut src), Err(ProtocolError::Poisoned));
        assert_eq!(src.as_ref(), wire);
        assert!(decoder.nodes.is_empty());
        assert!(decoder.arrays.is_empty());
    }

    #[test]
    fn decodes_all_types_and_preserves_binary_payloads() {
        assert_eq!(
            decode_all(b"+\xff\0\r\n"),
            Frame::Simple(Bytes::from_static(b"\xff\0"))
        );
        assert_eq!(
            decode_all(b"-ERR\r\n"),
            Frame::Error(Bytes::from_static(b"ERR"))
        );
        assert_eq!(decode_all(b":-42\r\n"), Frame::Integer(-42));
        assert_eq!(
            decode_all(b"$4\r\n\0\r\n\xff\r\n"),
            Frame::Bulk(Some(Bytes::from_static(b"\0\r\n\xff")))
        );
        assert_eq!(
            decode_all(b"*2\r\n:1\r\n*2\r\n+ok\r\n$-1\r\n"),
            Frame::Array(Some(vec![
                Frame::Integer(1),
                Frame::Array(Some(vec![
                    Frame::Simple(Bytes::from_static(b"ok")),
                    Frame::Bulk(None)
                ]))
            ]))
        );
    }

    #[test]
    fn distinguishes_null_and_empty_values() {
        assert_eq!(decode_all(b"$-1\r\n"), Frame::Bulk(None));
        assert_eq!(decode_all(b"$0\r\n\r\n"), Frame::Bulk(Some(Bytes::new())));
        assert_eq!(decode_all(b"*-1\r\n"), Frame::Array(None));
        assert_eq!(decode_all(b"*0\r\n"), Frame::Array(Some(vec![])));
        assert_eq!(decode_all(b"+\r\n"), Frame::Simple(Bytes::new()));
        assert_eq!(decode_all(b"-\r\n"), Frame::Error(Bytes::new()));
    }

    #[test]
    fn parses_integer_extremes_and_normalizable_forms() {
        for (wire, value) in [
            (b":9223372036854775807\r\n".as_slice(), i64::MAX),
            (b":-9223372036854775808\r\n".as_slice(), i64::MIN),
            (b":+00042\r\n".as_slice(), 42),
            (b":-0\r\n".as_slice(), 0),
        ] {
            assert_eq!(decode_all(wire), Frame::Integer(value));
        }
        assert_eq!(
            decode_all(b"$0001\r\nx\r\n"),
            Frame::Bulk(Some(Bytes::from_static(b"x")))
        );
        assert_eq!(decode_all(b"*0000\r\n"), Frame::Array(Some(vec![])));
    }

    #[test]
    fn preserves_every_incomplete_prefix_and_concatenated_suffix() {
        let wire = b"*3\r\n:7\r\n*2\r\n$4\r\n\0\r\n\xff\r\n$-1\r\n+tail\r\n";
        let expected = decode_all(wire);
        for split in 0..wire.len() {
            let mut decoder = Decoder::new(limits()).unwrap();
            let mut src = BytesMut::from(&wire[..split]);
            assert_eq!(decoder.decode(&mut src), Ok(None), "split {split}");
            assert_eq!(src.as_ref(), &wire[..split]);
            src.extend_from_slice(&wire[split..]);
            src.extend_from_slice(b":8\r\n");
            assert_eq!(decoder.decode(&mut src), Ok(Some(expected.clone())));
            assert_eq!(src.as_ref(), b":8\r\n");
            assert_eq!(decoder.decode(&mut src), Ok(Some(Frame::Integer(8))));
            assert!(src.is_empty());
            assert_eq!(decoder.decode(&mut src), Ok(None));
        }
    }

    #[test]
    fn handles_byte_by_byte_input_and_no_new_bytes() {
        let wire = b"*2\r\n$4\r\n\0\r\n\xff\r\n+ok\r\n";
        let expected = decode_all(wire);
        let mut decoder = Decoder::new(limits()).unwrap();
        let mut src = BytesMut::new();
        for (index, byte) in wire.iter().enumerate() {
            src.extend_from_slice(&[*byte]);
            if index + 1 < wire.len() {
                assert_eq!(decoder.decode(&mut src), Ok(None));
                assert_eq!(src.as_ref(), &wire[..=index]);
                assert_eq!(decoder.decode(&mut src), Ok(None));
            } else {
                assert_eq!(decoder.decode(&mut src), Ok(Some(expected.clone())));
            }
        }
        assert!(src.is_empty());
    }

    #[test]
    fn rejects_malformed_lines_lengths_and_terminators_without_consumption() {
        for wire in [
            b"?foo\r\n".as_slice(),
            b"+foo\n",
            b"+foo\rx",
            b":\r\n",
            b":+\r\n",
            b":--1\r\n",
            b": 1\r\n",
            b":1 \r\n",
            b":\xff\r\n",
            b":9223372036854775808\r\n",
            b":-9223372036854775809\r\n",
            b"$\r\n",
            b"$+1\r\n",
            b"$-0\r\n",
            b"$-01\r\n",
            b"$-2\r\n",
            b"*+1\r\n",
            b"*-0\r\n",
            b"*-01\r\n",
            b"*-2\r\n",
            b"$999999999999999999999999999999999999999999999\r\n",
            b"*999999999999999999999999999999999999999999999\r\n",
            b"$1\r\nx\n\n",
            b"$1\r\nx\rx",
            b"*2\r\n+ok\r\n?bad\r\n",
        ] {
            let mut decoder = Decoder::new(limits()).unwrap();
            let mut src = BytesMut::from(wire);
            assert!(
                matches!(decoder.decode(&mut src), Err(ProtocolError::Malformed(_))),
                "{wire:?}"
            );
            assert_eq!(src.as_ref(), wire);
            assert_eq!(decoder.decode(&mut src), Err(ProtocolError::Poisoned));
        }
    }

    #[test]
    fn enforces_exact_bulk_and_frame_budgets_including_framing() {
        let exact = RespLimits {
            max_frame_bytes: 9,
            max_bulk_bytes: 3,
            max_line_bytes: 9,
            ..limits()
        };
        let mut decoder = Decoder::new(exact).unwrap();
        let mut src = BytesMut::from(b"$3\r\nabc\r\n+suffix\r\n".as_slice());
        assert_eq!(
            decoder.decode(&mut src),
            Ok(Some(Frame::Bulk(Some(Bytes::from_static(b"abc")))))
        );
        assert_eq!(src.as_ref(), b"+suffix\r\n");
        assert_error(
            b"$4\r\n",
            exact,
            ProtocolError::LimitExceeded("max_bulk_bytes"),
        );
        assert_error(
            b"$3\r\n",
            RespLimits {
                max_frame_bytes: 8,
                max_line_bytes: 8,
                ..exact
            },
            ProtocolError::LimitExceeded("max_frame_bytes"),
        );
        assert_error(
            b"+1234567\r\n",
            exact,
            ProtocolError::LimitExceeded("max_frame_bytes"),
        );
    }

    #[test]
    fn line_limit_counts_prefix_and_crlf() {
        let exact = RespLimits {
            max_line_bytes: 5,
            ..limits()
        };
        let mut decoder = Decoder::new(exact).unwrap();
        let mut src = BytesMut::from(b"+ab\r\n".as_slice());
        assert_eq!(
            decoder.decode(&mut src),
            Ok(Some(Frame::Simple(Bytes::from_static(b"ab"))))
        );
        for wire in [b"+abc\r\n".as_slice(), b"+abc", b"$000\r\n", b"*000\r\n"] {
            assert_error(wire, exact, ProtocolError::LimitExceeded("max_line_bytes"));
        }
    }

    #[test]
    fn declared_nodes_include_root_and_unparsed_siblings() {
        let exact = RespLimits {
            max_nodes: 4,
            ..limits()
        };
        let mut decoder = Decoder::new(exact).unwrap();
        let mut src = BytesMut::from(b"*2\r\n*1\r\n:1\r\n:2\r\n".as_slice());
        assert!(decoder.decode(&mut src).unwrap().is_some());
        assert_error(b"*4\r\n", exact, ProtocolError::LimitExceeded("max_nodes"));
        assert_error(
            b"*2\r\n*2\r\n",
            exact,
            ProtocolError::LimitExceeded("max_nodes"),
        );
        let mut decoder = Decoder::new(RespLimits {
            max_nodes: 1000,
            ..limits()
        })
        .unwrap();
        let mut src = BytesMut::from(b"*999\r\n".as_slice());
        assert_eq!(decoder.decode(&mut src), Ok(None));
        assert_eq!(decoder.nodes.len(), 1);
        assert_eq!(decoder.arrays.len(), 1);
        assert!(decoder.nodes.capacity() < 999);
        assert!(decoder.arrays.capacity() < 999);
    }

    #[test]
    fn rejects_arrays_that_cannot_fit_the_frame_even_before_children_arrive() {
        assert_error(
            b"*3\r\n",
            RespLimits {
                max_frame_bytes: 12,
                max_bulk_bytes: 8,
                max_line_bytes: 8,
                ..limits()
            },
            ProtocolError::LimitExceeded("max_frame_bytes"),
        );
    }

    #[test]
    fn depth_includes_empty_and_null_arrays_but_not_scalars() {
        let exact = RespLimits {
            max_depth: 2,
            ..limits()
        };
        for wire in [
            b"*1\r\n*1\r\n:0\r\n".as_slice(),
            b"*1\r\n*0\r\n",
            b"*1\r\n*-1\r\n",
        ] {
            let mut decoder = Decoder::new(exact).unwrap();
            assert!(decoder.decode(&mut BytesMut::from(wire)).unwrap().is_some());
        }
        for wire in [b"*1\r\n*1\r\n*0\r\n".as_slice(), b"*1\r\n*1\r\n*-1\r\n"] {
            assert_error(wire, exact, ProtocolError::LimitExceeded("max_depth"));
        }
    }

    #[test]
    fn detects_shrinking_incomplete_buffers_and_stays_closed() {
        let mut decoder = Decoder::new(limits()).unwrap();
        let mut src = BytesMut::from(b"$5\r\nabc".as_slice());
        assert_eq!(decoder.decode(&mut src), Ok(None));
        src.truncate(6);
        let snapshot = src.clone();
        assert_eq!(decoder.decode(&mut src), Err(ProtocolError::BufferChanged));
        assert_eq!(src, snapshot);
        src.extend_from_slice(b"de\r\n");
        assert_eq!(decoder.decode(&mut src), Err(ProtocolError::Poisoned));
    }

    #[test]
    fn incomplete_frames_store_metadata_without_materializing_payloads() {
        let mut decoder = Decoder::new(limits()).unwrap();
        let mut src = BytesMut::from(b"*2\r\n$3\r\nabc\r\n$2\r\nx".as_slice());
        assert_eq!(decoder.decode(&mut src), Ok(None));
        assert_eq!(decoder.nodes.len(), 2);
        assert!(matches!(
            decoder.nodes[1],
            Node::Bulk(Some(Payload { start: 8, end: 11 }))
        ));
        src.extend_from_slice(b"y\r\n");
        assert_eq!(
            decoder.decode(&mut src),
            Ok(Some(Frame::Array(Some(vec![
                Frame::Bulk(Some(Bytes::from_static(b"abc"))),
                Frame::Bulk(Some(Bytes::from_static(b"xy")))
            ]))))
        );
    }

    #[test]
    fn completed_payloads_do_not_retain_the_network_allocation() {
        for wire in [b"+small\r\n".as_slice(), b"-small\r\n", b"$5\r\nsmall\r\n"] {
            let mut decoder = Decoder::new(limits()).unwrap();
            let mut src = BytesMut::with_capacity(1024 * 1024);
            src.extend_from_slice(wire);
            let start = src.as_ptr() as usize;
            let end = start + src.capacity();
            let frame = decoder.decode(&mut src).unwrap().unwrap();
            let payload = match frame {
                Frame::Simple(payload) | Frame::Error(payload) | Frame::Bulk(Some(payload)) => {
                    payload
                }
                _ => panic!("tipo diferente do fixture"),
            };
            let address = payload.as_ptr() as usize;
            assert!(address < start || address >= end);
            drop(src);
            assert_eq!(payload.as_ref(), b"small");
        }
    }

    #[test]
    fn offsets_and_declared_node_arithmetic_cannot_overflow() {
        let largest = RespLimits {
            max_frame_bytes: usize::MAX,
            max_bulk_bytes: usize::MAX,
            max_nodes: usize::MAX,
            ..limits()
        };
        assert_error(
            format!("${}\r\n", usize::MAX).as_bytes(),
            largest,
            ProtocolError::LimitExceeded("max_frame_bytes"),
        );
        assert_error(
            format!("*{}\r\n", usize::MAX).as_bytes(),
            largest,
            ProtocolError::LimitExceeded("max_nodes"),
        );
        assert_error(
            format!("*{}\r\n", usize::MAX / 2).as_bytes(),
            largest,
            ProtocolError::LimitExceeded("max_frame_bytes"),
        );
    }

    #[test]
    fn maximum_supported_depth_materializes_without_recursive_parsing() {
        let mut wire = b"*1\r\n".repeat(128);
        wire.extend_from_slice(b":0\r\n");
        let mut decoder = Decoder::new(RespLimits {
            max_depth: 128,
            max_nodes: 129,
            ..limits()
        })
        .unwrap();
        let mut src = BytesMut::from(wire.as_slice());
        let mut frame = decoder.decode(&mut src).unwrap().unwrap();
        for _ in 0..128 {
            let Frame::Array(Some(mut children)) = frame else {
                panic!("array esperado");
            };
            assert_eq!(children.len(), 1);
            frame = children.pop().unwrap();
        }
        assert_eq!(frame, Frame::Integer(0));
        assert!(src.is_empty());
    }

    fn fragmented_work(wire: &[u8], limits: RespLimits) -> usize {
        let mut decoder = Decoder::new(limits).unwrap();
        let mut src = BytesMut::new();
        for (index, byte) in wire.iter().enumerate() {
            src.extend_from_slice(&[*byte]);
            let result = decoder.decode(&mut src).unwrap();
            assert_eq!(result.is_some(), index + 1 == wire.len());
        }
        assert!(src.is_empty());
        decoder.work
    }

    #[test]
    fn byte_fragmented_long_headers_have_linear_work() {
        let mut previous = 0;
        for length in [256, 512, 1024, 2048] {
            let mut wire = Vec::from(b"$".as_slice());
            wire.extend(std::iter::repeat_n(b'0', length));
            wire.extend_from_slice(b"1\r\nx\r\n");
            let work = fragmented_work(
                &wire,
                RespLimits {
                    max_line_bytes: 4096,
                    ..limits()
                },
            );
            assert!(work <= wire.len() * 4, "{length}: {work}");
            if previous > 0 {
                assert!(work <= previous * 2 + 16);
            }
            previous = work;
        }
    }

    #[test]
    fn byte_fragmented_flat_arrays_have_linear_work() {
        let mut previous = 0;
        for count in [128, 256, 512, 1024] {
            let mut wire = format!("*{count}\r\n").into_bytes();
            for _ in 0..count {
                wire.extend_from_slice(b":0\r\n");
            }
            let work = fragmented_work(
                &wire,
                RespLimits {
                    max_frame_bytes: 8192,
                    max_nodes: 2048,
                    ..limits()
                },
            );
            assert!(work <= wire.len() * 5, "{count}: {work}");
            if previous > 0 {
                assert!(work <= previous * 2 + 32);
            }
            previous = work;
        }
    }

    #[test]
    fn validates_configuration_before_accepting_input() {
        assert!(
            Decoder::new(RespLimits {
                max_nodes: 0,
                ..limits()
            })
            .is_err()
        );
    }
}
