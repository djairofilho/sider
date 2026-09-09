//! Frames binários limitados; o payload de dados conserva o codec AOF tipado.

use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::Cursor;
use crate::persistence::format::{self, Next, Record};
use crate::storage::Mutation;

pub const VERSION: u16 = 1;
pub const MAGIC: &[u8; 8] = b"SIDERREP";
pub const HEADER_BYTES: usize = 24;
pub const MAX_FRAME_BYTES: usize = format::MAX_RECORD_BYTES + 128;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Inclui cabeçalho de transporte, framing AOF e payload.
    pub max_frame_bytes: usize,
    pub record: format::Limits,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_FRAME_BYTES,
            record: format::Limits::default(),
        }
    }
}

impl Limits {
    fn validate(self) -> Result<(), Error> {
        if !(HEADER_BYTES + 1..=MAX_FRAME_BYTES).contains(&self.max_frame_bytes)
            || self.record.max_record_bytes == 0
            || self.record.max_record_bytes > format::MAX_RECORD_BYTES
            || self.record.max_mutations == 0
        {
            return Err(Error::Limit);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hello {
    pub sider_version: Bytes,
    pub record_version: u32,
    pub shard_count: u32,
    pub routing_version: u32,
    pub max_record_bytes: u32,
    pub max_mutations: u32,
    pub max_snapshot_bytes: u64,
    pub cursor: Option<Cursor>,
}

impl Hello {
    /// Capacidades do receptor devem comportar o contrato anunciado pelo emissor.
    pub fn accepts(&self, source: &Self) -> Result<(), Reject> {
        if self.sider_version != source.sider_version
            || self.record_version != source.record_version
        {
            return Err(Reject::IncompatibleVersion);
        }
        if self.shard_count != source.shard_count || self.routing_version != source.routing_version
        {
            return Err(Reject::IncompatibleLayout);
        }
        if self.max_record_bytes < source.max_record_bytes
            || self.max_mutations < source.max_mutations
            || self.max_snapshot_bytes < source.max_snapshot_bytes
        {
            return Err(Reject::ResourceLimit);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Reject {
    IncompatibleVersion = 1,
    IncompatibleLayout = 2,
    ResourceLimit = 3,
    FullRequired = 4,
    InvalidSequence = 5,
    Unavailable = 6,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    Hello(Hello),
    /// Snapshot único para backup; não cria assinatura incremental nem exige ACK.
    Export {
        sider_version: Bytes,
        max_record_bytes: u32,
        max_snapshot_bytes: u64,
    },
    Continue(Cursor),
    FullStart {
        cursor: Cursor,
        entries: u64,
    },
    SnapshotEntry(Mutation),
    FullEnd {
        cursor: Cursor,
        entries: u64,
        digest: u32,
    },
    Batch {
        sequence: u64,
        batch: crate::storage::ResolvedBatch,
    },
    Ack(Cursor),
    Heartbeat(Cursor),
    Reject(Reject),
    StatusRequest,
    Promote,
    Promoted(Cursor),
    Status {
        readonly: bool,
        cursor: Cursor,
        upstream_sequence: Option<u64>,
        connected: bool,
        backlog_bytes: u64,
        full_syncs: u64,
        partial_syncs: u64,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O de replicação: {0}")]
    Io(#[from] std::io::Error),
    #[error("prazo de replicação excedido")]
    Timeout,
    #[error("limite de replicação excedido")]
    Limit,
    #[error("mensagem de replicação inválida: {0}")]
    Invalid(&'static str),
    #[error("versão de replicação não suportada: {0}")]
    Version(u16),
    #[error("registro de replicação: {0}")]
    Record(#[from] format::FormatError),
}

fn write_cursor(out: &mut Vec<u8>, cursor: Cursor) {
    out.extend_from_slice(&cursor.epoch);
    out.extend_from_slice(&cursor.sequence.to_le_bytes());
}

pub fn encode(message: &Message, limits: Limits) -> Result<Bytes, Error> {
    limits.validate()?;
    // O encoder AOF verifica o tamanho antes de alocar. Seu orçamento precisa
    // caber também no frame negociado, incluindo os dois cabeçalhos.
    let record_limits = format::Limits {
        max_record_bytes: limits
            .record
            .max_record_bytes
            .min(limits.max_frame_bytes.saturating_sub(HEADER_BYTES + 12)),
        ..limits.record
    };
    let mut body = Vec::new();
    let kind = match message {
        Message::Hello(hello) => {
            if hello.sider_version.is_empty()
                || hello.sider_version.len() > 32
                || !hello.sider_version.iter().all(u8::is_ascii_graphic)
                || hello.shard_count == 0
                || hello.routing_version == 0
                || hello.record_version == 0
                || hello.max_record_bytes == 0
                || hello.max_mutations == 0
                || hello.max_snapshot_bytes == 0
            {
                return Err(Error::Invalid("handshake"));
            }
            body.push(hello.sider_version.len() as u8);
            body.extend_from_slice(&hello.sider_version);
            for value in [
                hello.record_version,
                hello.shard_count,
                hello.routing_version,
                hello.max_record_bytes,
                hello.max_mutations,
            ] {
                body.extend_from_slice(&value.to_le_bytes());
            }
            body.extend_from_slice(&hello.max_snapshot_bytes.to_le_bytes());
            body.push(u8::from(hello.cursor.is_some()));
            if let Some(cursor) = hello.cursor {
                write_cursor(&mut body, cursor);
            }
            1
        }
        Message::Continue(cursor) => {
            write_cursor(&mut body, *cursor);
            2
        }
        Message::FullStart { cursor, entries } => {
            write_cursor(&mut body, *cursor);
            body.extend_from_slice(&entries.to_le_bytes());
            3
        }
        Message::SnapshotEntry(mutation @ Mutation::Put { .. }) => {
            body = format::encode(&Record::Snapshot(mutation.clone()), record_limits)?;
            4
        }
        Message::SnapshotEntry(Mutation::Delete { .. }) => {
            return Err(Error::Invalid("remoção dentro de snapshot"));
        }
        Message::FullEnd {
            cursor,
            entries,
            digest,
        } => {
            write_cursor(&mut body, *cursor);
            body.extend_from_slice(&entries.to_le_bytes());
            body.extend_from_slice(&digest.to_le_bytes());
            5
        }
        Message::Batch { sequence, batch } => {
            body = format::encode(
                &Record::Batch {
                    sequence: *sequence,
                    batch: batch.clone(),
                },
                record_limits,
            )?;
            6
        }
        Message::Ack(cursor) => {
            write_cursor(&mut body, *cursor);
            7
        }
        Message::Heartbeat(cursor) => {
            write_cursor(&mut body, *cursor);
            8
        }
        Message::Reject(reject) => {
            body.push(*reject as u8);
            9
        }
        Message::Export {
            sider_version,
            max_record_bytes,
            max_snapshot_bytes,
        } => {
            if sider_version.is_empty()
                || sider_version.len() > 32
                || !sider_version.iter().all(u8::is_ascii_graphic)
                || *max_record_bytes == 0
                || *max_snapshot_bytes == 0
            {
                return Err(Error::Invalid("pedido de exportação"));
            }
            body.push(sider_version.len() as u8);
            body.extend_from_slice(sider_version);
            body.extend_from_slice(&max_record_bytes.to_le_bytes());
            body.extend_from_slice(&max_snapshot_bytes.to_le_bytes());
            10
        }
        Message::StatusRequest => {
            body.push(0);
            11
        }
        Message::Promote => {
            body.push(0);
            12
        }
        Message::Promoted(cursor) => {
            write_cursor(&mut body, *cursor);
            13
        }
        Message::Status {
            readonly,
            cursor,
            upstream_sequence,
            connected,
            backlog_bytes,
            full_syncs,
            partial_syncs,
        } => {
            body.push(u8::from(*readonly));
            write_cursor(&mut body, *cursor);
            body.push(u8::from(upstream_sequence.is_some()));
            body.extend_from_slice(&upstream_sequence.unwrap_or(0).to_le_bytes());
            body.push(u8::from(*connected));
            for value in [backlog_bytes, full_syncs, partial_syncs] {
                body.extend_from_slice(&value.to_le_bytes());
            }
            14
        }
    };
    let size = body.len().checked_add(HEADER_BYTES).ok_or(Error::Limit)?;
    if size > limits.max_frame_bytes {
        return Err(Error::Limit);
    }
    let len = u32::try_from(body.len()).map_err(|_| Error::Limit)?;
    let mut frame = Vec::with_capacity(size);
    frame.extend_from_slice(MAGIC);
    frame.extend_from_slice(&VERSION.to_le_bytes());
    frame.push(kind);
    frame.push(0);
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&(!len).to_le_bytes());
    let digest = format::snapshot_digest(format::checksum(&frame), &body);
    frame.extend_from_slice(&digest.to_le_bytes());
    frame.extend_from_slice(&body);
    Ok(Bytes::from(frame))
}

fn header(header: &[u8], limits: Limits) -> Result<(u8, usize), Error> {
    limits.validate()?;
    if header.len() != HEADER_BYTES || &header[..8] != MAGIC || header[11] != 0 {
        return Err(Error::Invalid("cabeçalho"));
    }
    let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
    if version != VERSION {
        return Err(Error::Version(version));
    }
    if !(1..=14).contains(&header[10]) {
        return Err(Error::Invalid("tipo de mensagem"));
    }
    let len = u32::from_le_bytes(header[12..16].try_into().unwrap());
    if !len != u32::from_le_bytes(header[16..20].try_into().unwrap()) {
        return Err(Error::Invalid("comprimento"));
    }
    let len = len as usize;
    if len == 0 || len > limits.max_frame_bytes - HEADER_BYTES {
        return Err(Error::Limit);
    }
    let valid_size = match header[10] {
        1 => len <= 86,
        2 | 7 | 8 => len == 24,
        3 => len == 32,
        5 => len == 36,
        9 => len == 1,
        10 => len <= 45,
        11 | 12 => len == 1,
        13 => len == 24,
        14 => len == 59,
        _ => true,
    };
    if !valid_size {
        return Err(Error::Invalid("tamanho do tipo de mensagem"));
    }
    Ok((header[10], len))
}

struct Fields {
    bytes: Bytes,
    position: usize,
}

impl Fields {
    fn take(&mut self, count: usize) -> Result<Bytes, Error> {
        let end = self
            .position
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(Error::Invalid("payload incompleto"))?;
        let value = self.bytes.slice(self.position..end);
        self.position = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    fn boolean(&mut self) -> Result<bool, Error> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::Invalid("flag booleana")),
        }
    }
    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(
            self.take(4)?.as_ref().try_into().unwrap(),
        ))
    }
    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(
            self.take(8)?.as_ref().try_into().unwrap(),
        ))
    }
    fn cursor(&mut self) -> Result<Cursor, Error> {
        Ok(Cursor {
            epoch: self.take(16)?.as_ref().try_into().unwrap(),
            sequence: self.u64()?,
        })
    }
}

/// Aceita exatamente um frame, sem ignorar bytes posteriores.
pub fn decode(frame: Bytes, limits: Limits) -> Result<Message, Error> {
    if frame.len() < HEADER_BYTES {
        return Err(Error::Invalid("cabeçalho incompleto"));
    }
    let (kind, len) = header(&frame[..HEADER_BYTES], limits)?;
    if frame.len() != HEADER_BYTES + len {
        return Err(Error::Invalid("comprimento do payload"));
    }
    let body = frame.slice(HEADER_BYTES..);
    let expected = u32::from_le_bytes(frame[20..24].try_into().unwrap());
    if format::snapshot_digest(format::checksum(&frame[..20]), &body) != expected {
        return Err(Error::Invalid("checksum"));
    }
    if kind == 4 || kind == 6 {
        // Um prefixo AOF forjado não pode pedir uma alocação maior que o frame
        // já recebido, mesmo quando os limites globais permitem registros grandes.
        if body.len() < 12
            || u32::from_le_bytes(body[..4].try_into().unwrap()) as usize != body.len() - 12
        {
            return Err(Error::Invalid("comprimento do registro"));
        }
        let mut input = body.as_ref();
        let record = format::read_record(&mut input, limits.record)?;
        if !input.is_empty() {
            return Err(Error::Invalid("bytes depois do registro"));
        }
        return match (kind, record) {
            (4, Next::Record(Record::Snapshot(mutation @ Mutation::Put { .. }))) => {
                Ok(Message::SnapshotEntry(mutation))
            }
            (6, Next::Record(Record::Batch { sequence, batch })) => {
                Ok(Message::Batch { sequence, batch })
            }
            _ => Err(Error::Invalid("registro incompatível ou incompleto")),
        };
    }
    let mut fields = Fields {
        bytes: body,
        position: 0,
    };
    let message = match kind {
        1 => {
            let size = fields.byte()? as usize;
            let hello = Hello {
                sider_version: fields.take(size)?,
                record_version: fields.u32()?,
                shard_count: fields.u32()?,
                routing_version: fields.u32()?,
                max_record_bytes: fields.u32()?,
                max_mutations: fields.u32()?,
                max_snapshot_bytes: fields.u64()?,
                cursor: match fields.byte()? {
                    0 => None,
                    1 => Some(fields.cursor()?),
                    _ => return Err(Error::Invalid("flag do cursor")),
                },
            };
            // Reutiliza a validação dos campos fixos sem alocar dados proporcionais ao peer.
            let message = Message::Hello(hello);
            encode(&message, limits)?;
            message
        }
        2 => Message::Continue(fields.cursor()?),
        3 => Message::FullStart {
            cursor: fields.cursor()?,
            entries: fields.u64()?,
        },
        5 => Message::FullEnd {
            cursor: fields.cursor()?,
            entries: fields.u64()?,
            digest: fields.u32()?,
        },
        7 => Message::Ack(fields.cursor()?),
        8 => Message::Heartbeat(fields.cursor()?),
        9 => Message::Reject(match fields.byte()? {
            1 => Reject::IncompatibleVersion,
            2 => Reject::IncompatibleLayout,
            3 => Reject::ResourceLimit,
            4 => Reject::FullRequired,
            5 => Reject::InvalidSequence,
            6 => Reject::Unavailable,
            _ => return Err(Error::Invalid("motivo de rejeição")),
        }),
        10 => {
            let length = fields.byte()? as usize;
            let message = Message::Export {
                sider_version: fields.take(length)?,
                max_record_bytes: fields.u32()?,
                max_snapshot_bytes: fields.u64()?,
            };
            encode(&message, limits)?;
            message
        }
        11 | 12 => {
            if fields.byte()? != 0 {
                return Err(Error::Invalid("pedido administrativo"));
            }
            if kind == 11 {
                Message::StatusRequest
            } else {
                Message::Promote
            }
        }
        13 => Message::Promoted(fields.cursor()?),
        14 => {
            let readonly = fields.boolean()?;
            let cursor = fields.cursor()?;
            let has_upstream = fields.boolean()?;
            let sequence = fields.u64()?;
            if !has_upstream && sequence != 0 {
                return Err(Error::Invalid("posição upstream ausente"));
            }
            Message::Status {
                readonly,
                cursor,
                upstream_sequence: has_upstream.then_some(sequence),
                connected: fields.boolean()?,
                backlog_bytes: fields.u64()?,
                full_syncs: fields.u64()?,
                partial_syncs: fields.u64()?,
            }
        }
        _ => return Err(Error::Invalid("tipo de mensagem")),
    };
    if fields.position != fields.bytes.len() {
        return Err(Error::Invalid("bytes depois da mensagem"));
    }
    Ok(message)
}

/// O prazo cobre o frame inteiro, incluindo um peer que nunca completa o cabeçalho.
pub async fn read(
    input: &mut (impl AsyncRead + Unpin),
    limits: Limits,
    deadline: Duration,
) -> Result<Message, Error> {
    Ok(read_with_frame(input, limits, deadline).await?.1)
}

/// Conserva os bytes validados para a assinatura do snapshot, sem recodificação.
pub async fn read_with_frame(
    input: &mut (impl AsyncRead + Unpin),
    limits: Limits,
    deadline: Duration,
) -> Result<(Bytes, Message), Error> {
    if deadline.is_zero() || tokio::time::Instant::now().checked_add(deadline).is_none() {
        return Err(Error::Limit);
    }
    limits.validate()?;
    tokio::time::timeout(deadline, async {
        let mut prefix = [0; HEADER_BYTES];
        input.read_exact(&mut prefix).await?;
        let (_, size) = header(&prefix, limits)?;
        let mut frame = vec![0; HEADER_BYTES + size];
        frame[..HEADER_BYTES].copy_from_slice(&prefix);
        input.read_exact(&mut frame[HEADER_BYTES..]).await?;
        let frame = Bytes::from(frame);
        let message = decode(frame.clone(), limits)?;
        Ok((frame, message))
    })
    .await
    .map_err(|_| Error::Timeout)?
}

pub async fn write(
    output: &mut (impl AsyncWrite + Unpin),
    message: &Message,
    limits: Limits,
    deadline: Duration,
) -> Result<(), Error> {
    if deadline.is_zero() || tokio::time::Instant::now().checked_add(deadline).is_none() {
        return Err(Error::Limit);
    }
    let frame = encode(message, limits)?;
    tokio::time::timeout(deadline, output.write_all(&frame))
        .await
        .map_err(|_| Error::Timeout)??;
    Ok(())
}
