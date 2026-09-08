//! Armazenamento síncrono de chaves e valores binários, sem acesso ao protocolo.

use std::collections::HashMap;

use bytes::Bytes;

use crate::command::{Command, Reply};

/// Mapa em memória com proprietário único e execução sequencial dos comandos.
///
/// Não oferece persistência, expiração ou quota de memória na versão 0.1.
/// Compartilhamento entre clientes será responsabilidade do worker proprietário.
#[derive(Default)]
pub struct Store {
    values: HashMap<Bytes, Bytes>,
}

impl Store {
    /// Cria um armazenamento vazio, usando o hasher padrão de `HashMap`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Aplica um comando validado sem suspender a execução.
    ///
    /// `GET` compartilha o conteúdo imutável de `Bytes` com a resposta. Alterar ou
    /// remover a chave depois não altera uma resposta já devolvida.
    pub fn execute(&mut self, command: Command) -> Reply {
        match command {
            Command::Ping(None) => Reply::Pong,
            Command::Ping(Some(message)) | Command::Echo(message) => Reply::Bulk(Some(message)),
            Command::Get { key } => Reply::Bulk(self.values.get(&key).cloned()),
            Command::Set { key, value } => {
                self.values.insert(key, value);
                Reply::Ok
            }
            Command::Del { keys } => {
                // Nos alvos de 32/64 bits, uma Vec<Bytes> válida possui menos de
                // i64::MAX elementos. Cada elemento causa no máximo um incremento.
                let mut removed = 0_i64;
                for key in keys {
                    if self.values.remove(&key).is_some() {
                        removed += 1;
                    }
                }
                Reply::Integer(removed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(store: &mut Store, key: &'static [u8], value: &'static [u8]) {
        assert_eq!(
            store.execute(Command::Set {
                key: Bytes::from_static(key),
                value: Bytes::from_static(value),
            }),
            Reply::Ok
        );
    }

    fn get(store: &mut Store, key: &'static [u8]) -> Reply {
        store.execute(Command::Get {
            key: Bytes::from_static(key),
        })
    }

    #[test]
    fn new_and_default_start_without_keys() {
        for mut store in [Store::new(), Store::default()] {
            assert_eq!(get(&mut store, b"missing"), Reply::Bulk(None));
            assert_eq!(get(&mut store, b""), Reply::Bulk(None));
        }
    }

    #[test]
    fn ping_and_echo_preserve_binary_messages_and_state() {
        let mut store = Store::new();
        set(&mut store, b"kept", b"value");

        assert_eq!(store.execute(Command::Ping(None)), Reply::Pong);
        for message in [Bytes::new(), Bytes::from_static(b"\xff\0\r\n\x80")] {
            assert_eq!(
                store.execute(Command::Ping(Some(message.clone()))),
                Reply::Bulk(Some(message.clone()))
            );
            assert_eq!(
                store.execute(Command::Echo(message.clone())),
                Reply::Bulk(Some(message))
            );
        }
        assert_eq!(
            get(&mut store, b"kept"),
            Reply::Bulk(Some(Bytes::from_static(b"value")))
        );
    }

    #[test]
    fn empty_key_and_empty_value_are_not_missing() {
        let mut store = Store::new();
        set(&mut store, b"", b"");
        set(&mut store, b"nonempty", b"");

        assert_eq!(get(&mut store, b""), Reply::Bulk(Some(Bytes::new())));
        assert_eq!(
            get(&mut store, b"nonempty"),
            Reply::Bulk(Some(Bytes::new()))
        );
        assert_eq!(get(&mut store, b"absent"), Reply::Bulk(None));
    }

    #[test]
    fn binary_keys_and_values_keep_every_byte() {
        let mut store = Store::new();
        set(&mut store, b"\xff\0\r\n\x80", b"\0\xff\x80\r\n");
        set(&mut store, b"\xff", b"prefix only");

        assert_eq!(
            get(&mut store, b"\xff\0\r\n\x80"),
            Reply::Bulk(Some(Bytes::from_static(b"\0\xff\x80\r\n")))
        );
        assert_eq!(
            get(&mut store, b"\xff"),
            Reply::Bulk(Some(Bytes::from_static(b"prefix only")))
        );
        assert_eq!(get(&mut store, b"\xff\0"), Reply::Bulk(None));
    }

    #[test]
    fn set_overwrites_without_changing_other_keys() {
        let mut store = Store::new();
        set(&mut store, b"key", b"first");
        set(&mut store, b"other", b"retained");
        set(&mut store, b"key", b"second");

        assert_eq!(
            get(&mut store, b"key"),
            Reply::Bulk(Some(Bytes::from_static(b"second")))
        );
        assert_eq!(
            get(&mut store, b"other"),
            Reply::Bulk(Some(Bytes::from_static(b"retained")))
        );
        set(&mut store, b"key", b"");
        assert_eq!(get(&mut store, b"key"), Reply::Bulk(Some(Bytes::new())));
    }

    #[test]
    fn key_comparison_is_case_sensitive() {
        let mut store = Store::new();
        set(&mut store, b"key", b"lower");
        set(&mut store, b"KEY", b"upper");

        assert_eq!(
            get(&mut store, b"key"),
            Reply::Bulk(Some(Bytes::from_static(b"lower")))
        );
        assert_eq!(
            get(&mut store, b"KEY"),
            Reply::Bulk(Some(Bytes::from_static(b"upper")))
        );
        assert_eq!(get(&mut store, b"Key"), Reply::Bulk(None));
    }

    #[test]
    fn del_counts_only_actual_removals_and_preserves_other_keys() {
        let mut store = Store::new();
        set(&mut store, b"a", b"first");
        set(&mut store, b"b", b"second");
        set(&mut store, b"c", b"kept");

        let keys = ["a", "a", "missing", "b", "b", "missing"]
            .into_iter()
            .map(|key| Bytes::from_static(key.as_bytes()))
            .collect();
        assert_eq!(store.execute(Command::Del { keys }), Reply::Integer(2));
        assert_eq!(get(&mut store, b"a"), Reply::Bulk(None));
        assert_eq!(get(&mut store, b"b"), Reply::Bulk(None));
        assert_eq!(
            get(&mut store, b"c"),
            Reply::Bulk(Some(Bytes::from_static(b"kept")))
        );
        assert_eq!(
            store.execute(Command::Del {
                keys: vec![Bytes::from_static(b"a"), Bytes::from_static(b"missing")],
            }),
            Reply::Integer(0)
        );
    }

    #[test]
    fn del_accepts_empty_and_binary_keys_and_counts_empty_values() {
        let mut store = Store::new();
        set(&mut store, b"", b"");
        set(&mut store, b"\xff\0\r\n", b"");

        assert_eq!(
            store.execute(Command::Del {
                keys: vec![
                    Bytes::new(),
                    Bytes::from_static(b"\xff\0\r\n"),
                    Bytes::new(),
                ],
            }),
            Reply::Integer(2)
        );
        assert_eq!(get(&mut store, b""), Reply::Bulk(None));
        assert_eq!(get(&mut store, b"\xff\0\r\n"), Reply::Bulk(None));
    }

    #[test]
    fn directly_constructed_empty_del_is_a_noop() {
        let mut store = Store::new();
        set(&mut store, b"kept", b"value");

        assert_eq!(
            store.execute(Command::Del { keys: Vec::new() }),
            Reply::Integer(0)
        );
        assert_eq!(
            get(&mut store, b"kept"),
            Reply::Bulk(Some(Bytes::from_static(b"value")))
        );
    }

    #[test]
    fn get_shares_immutable_bytes_and_reply_survives_overwrite_and_removal() {
        let mut store = Store::new();
        let value = Bytes::from(vec![0xff, 0, b'\r', b'\n', 0x80]);
        let original_pointer = value.as_ptr();
        assert_eq!(
            store.execute(Command::Set {
                key: Bytes::from_static(b"key"),
                value,
            }),
            Reply::Ok
        );

        let Reply::Bulk(Some(first)) = get(&mut store, b"key") else {
            panic!("GET deve devolver o valor armazenado");
        };
        let Reply::Bulk(Some(second)) = get(&mut store, b"key") else {
            panic!("GET repetido deve devolver o mesmo conteúdo");
        };
        assert_eq!(first.as_ptr(), original_pointer);
        assert_eq!(second.as_ptr(), original_pointer);

        set(&mut store, b"key", b"replacement");
        assert_eq!(
            store.execute(Command::Del {
                keys: vec![Bytes::from_static(b"key")],
            }),
            Reply::Integer(1)
        );
        drop(store);
        assert_eq!(first.as_ref(), &[0xff, 0, b'\r', b'\n', 0x80]);
        assert_eq!(second, first);
    }
}
