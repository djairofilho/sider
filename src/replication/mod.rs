//! Transporte interno e histórico limitado de lotes resolvidos Sider → Sider.
//!
//! A integração com papéis, snapshot e AOF pertence ao coordenador de replicação.

pub mod journal;
pub mod protocol;

/// Uma posição só identifica estado dentro da mesma época do primário.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cursor {
    pub epoch: [u8; 16],
    pub sequence: u64,
}
