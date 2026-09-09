//! Comandos tipados e parsing sem acesso ao armazenamento.

mod collections;
mod fpconv;
mod info;
mod parser;
mod reply;
mod score;
mod sorted_set;
pub use collections::{HashCommand, ListCommand, SetCommand};
pub use info::InfoSections;
pub use score::Score;
pub use sorted_set::SortedSetCommand;

use bytes::Bytes;
use std::time::Duration;

pub use parser::{RequestError, parse};
pub use reply::{ExecutionError, Reply};

/// Condição de existência verificada pelo worker antes de substituir o valor.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SetCondition {
    #[default]
    Always,
    Missing,
    Present,
}

/// Política temporal de SET, resolvida no instante de execução.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SetExpiry {
    #[default]
    Persistent,
    Keep,
    After(Duration),
}

/// Opções independentes de transporte para uma substituição condicional.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SetOptions {
    pub condition: SetCondition,
    pub expiry: SetExpiry,
    pub return_previous: bool,
}

/// Unidade de uma operação temporal, preservada para validar e reportar erros.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpiryUnit {
    Seconds,
    Milliseconds,
}

/// Decimal canônico compatível com o parser de inteiros Redis.
pub(crate) fn parse_decimal(value: &[u8]) -> Option<i64> {
    if value == b"0" {
        return Some(0);
    }
    let digits = value.strip_prefix(b"-").unwrap_or(value);
    if !matches!(digits.first(), Some(b'1'..=b'9')) || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(value).ok()?.parse().ok()
}

/// Comando validado, sem canais ou conhecimento do protocolo de transporte.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// Diagnóstico operacional próprio, com seleção limitada de seções.
    Info(InfoSections),
    SortedSet {
        key: Bytes,
        operation: SortedSetCommand,
    },
    SetCollection {
        key: Bytes,
        operation: SetCommand,
    },
    /// Operação sobre campos e valores binários de um hash.
    Hash {
        key: Bytes,
        operation: HashCommand,
    },
    /// Operação de lista, incluindo pops individuais e ranges por índice.
    List {
        key: Bytes,
        operation: ListCommand,
    },
    /// Responde PONG ou devolve a mensagem binária.
    Ping(Option<Bytes>),
    /// Devolve exatamente o payload.
    Echo(Bytes),
    /// Lê o valor de uma chave.
    Get {
        key: Bytes,
    },
    /// Cria ou substitui um valor persistente.
    Set {
        key: Bytes,
        value: Bytes,
    },
    /// Aplica condições, prazo e retorno do valor anterior.
    SetWithOptions {
        key: Bytes,
        value: Bytes,
        options: SetOptions,
    },
    /// Remove chaves; duplicatas são preservadas para contar apenas efeitos reais.
    Del {
        keys: Vec<Bytes>,
    },
    /// Conta cada ocorrência de uma chave existente.
    Exists {
        keys: Vec<Bytes>,
    },
    /// Incrementa um inteiro decimal i64, criando zero antes da operação se ausente.
    Incr {
        key: Bytes,
    },
    /// Decrementa um inteiro decimal i64.
    Decr {
        key: Bytes,
    },
    /// Lê valores na ordem das chaves, preservando duplicatas.
    MGet {
        keys: Vec<Bytes>,
    },
    /// Aplica um lote indivisível; o último par de uma chave prevalece.
    MSet {
        entries: Vec<(Bytes, Bytes)>,
    },
    /// Define expiração relativa em milissegundos; prazo não positivo remove a chave.
    Expire {
        key: Bytes,
        value: i64,
        unit: ExpiryUnit,
    },
    /// Retorna o prazo restante ou os sentinelas -1 (persistente) e -2 (ausente).
    Ttl {
        key: Bytes,
        milliseconds: bool,
    },
    /// Remove a expiração de uma chave existente.
    Persist {
        key: Bytes,
    },
    /// Inscreve esta conexão em canais efêmeros, fora do armazenamento.
    Subscribe {
        channels: Vec<Bytes>,
    },
    /// Remove inscrições; lista vazia remove todas as inscrições da conexão.
    Unsubscribe {
        channels: Vec<Bytes>,
    },
    /// Publica uma mensagem binária sem alterar o dataset.
    Publish {
        channel: Bytes,
        message: Bytes,
    },
    /// Inicia uma fila transacional pertencente à conexão.
    Multi,
    /// Executa a fila inteira no seu único shard.
    Exec,
    /// Descarta a fila e suas observações.
    Discard,
    /// Observa chaves até EXEC, DISCARD, UNWATCH ou encerramento.
    Watch {
        keys: Vec<Bytes>,
    },
    /// Libera as observações desta conexão.
    Unwatch,
}

impl Command {
    /// Classificação sem consultar o dataset; condições que falhariam continuam escritas.
    pub fn writes_dataset(&self) -> bool {
        match self {
            Self::Set { .. }
            | Self::SetWithOptions { .. }
            | Self::Del { .. }
            | Self::Incr { .. }
            | Self::Decr { .. }
            | Self::MSet { .. }
            | Self::Expire { .. }
            | Self::Persist { .. } => true,
            Self::Hash { operation, .. } => matches!(
                operation,
                HashCommand::Set { .. } | HashCommand::Delete { .. }
            ),
            Self::List { operation, .. } => matches!(
                operation,
                ListCommand::Push { .. } | ListCommand::Pop { .. }
            ),
            Self::SetCollection { operation, .. } => matches!(
                operation,
                SetCommand::Add { .. } | SetCommand::Remove { .. }
            ),
            Self::SortedSet { operation, .. } => matches!(
                operation,
                SortedSetCommand::Add { .. } | SortedSetCommand::Remove { .. }
            ),
            Self::Ping(_)
            | Self::Info(_)
            | Self::Echo(_)
            | Self::Get { .. }
            | Self::Exists { .. }
            | Self::MGet { .. }
            | Self::Ttl { .. }
            | Self::Subscribe { .. }
            | Self::Unsubscribe { .. }
            | Self::Publish { .. }
            | Self::Multi
            | Self::Exec
            | Self::Discard
            | Self::Watch { .. }
            | Self::Unwatch => false,
        }
    }

    /// Visita chaves na ordem original sem alocar nem confundir valores com chaves.
    pub fn visit_keys(&self, mut visit: impl FnMut(&Bytes)) {
        match self {
            Self::Ping(_)
            | Self::Info(_)
            | Self::Echo(_)
            | Self::Subscribe { .. }
            | Self::Unsubscribe { .. }
            | Self::Publish { .. } => {}
            Self::Multi | Self::Exec | Self::Discard | Self::Unwatch => {}
            Self::Watch { keys } => keys.iter().for_each(visit),
            Self::Get { key }
            | Self::Hash { key, .. }
            | Self::List { key, .. }
            | Self::SetCollection { key, .. }
            | Self::SortedSet { key, .. }
            | Self::Set { key, .. }
            | Self::SetWithOptions { key, .. }
            | Self::Incr { key }
            | Self::Decr { key }
            | Self::Expire { key, .. }
            | Self::Ttl { key, .. }
            | Self::Persist { key } => visit(key),
            Self::Del { keys } | Self::Exists { keys } | Self::MGet { keys } => {
                keys.iter().for_each(visit)
            }
            Self::MSet { entries } => entries.iter().for_each(|(key, _)| visit(key)),
        }
    }
}
