//! Registro efêmero de canais binários, independente do armazenamento e do AOF.
//!
//! O mutex protege somente metadados e envios `try_send`; nenhum socket ou await
//! ocorre sob o lock. Sua ordem define a ordem das publicações para os assinantes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use thiserror::Error;
use tokio::sync::{mpsc, watch};

use crate::resp::Frame;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Message {
    pub channel: Bytes,
    pub payload: Bytes,
}

impl Message {
    pub(crate) fn into_frame(self) -> Frame {
        Frame::Array(Some(vec![
            Frame::Bulk(Some(Bytes::from_static(b"message"))),
            Frame::Bulk(Some(self.channel)),
            Frame::Bulk(Some(self.payload)),
        ]))
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum PubSubError {
    #[error("ERR pubsub channel limit exceeded")]
    ChannelLimit,
    #[error("assinante encerrado ou fila de notificações cheia")]
    Closed,
}

#[derive(Clone, Default)]
pub(crate) struct Hub {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    next_id: u64,
    subscribers: BTreeMap<u64, Entry>,
    channels: BTreeMap<Bytes, BTreeSet<u64>>,
}

struct Entry {
    channels: BTreeSet<Bytes>,
    messages: mpsc::Sender<Message>,
    evicted: watch::Sender<bool>,
}

impl State {
    fn remove(&mut self, id: u64) {
        if let Some(entry) = self.subscribers.remove(&id) {
            entry.evicted.send_replace(true);
            for channel in entry.channels {
                if let Some(members) = self.channels.get_mut(&channel) {
                    members.remove(&id);
                    if members.is_empty() {
                        self.channels.remove(&channel);
                    }
                }
            }
        }
    }
}

impl Hub {
    /// O chamador valida os limites antes de criar o canal Tokio.
    pub(crate) fn connect(
        &self,
        max_channels: usize,
        capacity: usize,
    ) -> Result<Subscription, PubSubError> {
        let (messages, receiver) = mpsc::channel(capacity);
        let (evicted, eviction) = watch::channel(false);
        let mut state = self.state.lock().expect("mutex Pub/Sub envenenado");
        let id = state.next_id;
        state.next_id = id.checked_add(1).ok_or(PubSubError::Closed)?;
        state.subscribers.insert(
            id,
            Entry {
                channels: BTreeSet::new(),
                messages,
                evicted,
            },
        );
        Ok(Subscription {
            hub: self.clone(),
            id,
            max_channels,
            receiver,
            eviction,
        })
    }

    /// Conta filas que aceitaram a mensagem; isso não confirma leitura pelo cliente.
    /// Fila cheia remove todas as inscrições daquele cliente e sinaliza sua conexão.
    pub(crate) fn publish(&self, message: Message) -> i64 {
        let mut state = self.state.lock().expect("mutex Pub/Sub envenenado");
        let members = state
            .channels
            .get(&message.channel)
            .cloned()
            .unwrap_or_default();
        let mut accepted = 0;
        for id in members {
            let sent = state
                .subscribers
                .get(&id)
                .is_some_and(|entry| entry.messages.try_send(message.clone()).is_ok());
            if sent {
                accepted += 1;
            } else {
                state.remove(id);
            }
        }
        accepted
    }
}

/// Guard de conexão: drop, cancelamento, erro e EOF removem todas as inscrições.
pub(crate) struct Subscription {
    hub: Hub,
    id: u64,
    max_channels: usize,
    receiver: mpsc::Receiver<Message>,
    eviction: watch::Receiver<bool>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.hub
            .state
            .lock()
            .expect("mutex Pub/Sub envenenado")
            .remove(self.id);
    }
}

impl Subscription {
    pub(crate) fn active(&self) -> bool {
        self.hub
            .state
            .lock()
            .expect("mutex Pub/Sub envenenado")
            .subscribers
            .get(&self.id)
            .is_some_and(|entry| !entry.channels.is_empty())
    }

    pub(crate) fn is_evicted(&self) -> bool {
        *self.eviction.borrow()
    }

    pub(crate) async fn evicted(&mut self) {
        let _ = self.eviction.wait_for(|closed| *closed).await;
    }

    pub(crate) fn try_message(&mut self) -> Option<Message> {
        self.receiver.try_recv().ok()
    }

    pub(crate) async fn message(&mut self) -> Option<Message> {
        tokio::select! {
            biased;
            _ = self.eviction.wait_for(|closed| *closed) => None,
            message = self.receiver.recv() => message,
        }
    }

    pub(crate) fn subscribe(&mut self, channels: Vec<Bytes>) -> Result<Vec<Frame>, PubSubError> {
        let mut state = self.hub.state.lock().expect("mutex Pub/Sub envenenado");
        let entry = state.subscribers.get(&self.id).ok_or(PubSubError::Closed)?;
        let unique: BTreeSet<_> = channels
            .iter()
            .cloned()
            .chain(entry.channels.iter().cloned())
            .collect();
        if unique.len() > self.max_channels {
            return Err(PubSubError::ChannelLimit);
        }
        // Captura mensagens anteriores sob o mesmo lock da inscrição. Confirmações
        // nunca ultrapassam mensagens já aceitas na fila desta conexão.
        let mut responses = drain(&mut self.receiver);
        for channel in channels {
            let entry = state
                .subscribers
                .get_mut(&self.id)
                .ok_or(PubSubError::Closed)?;
            entry.channels.insert(channel.clone());
            let count = entry.channels.len();
            state
                .channels
                .entry(channel.clone())
                .or_default()
                .insert(self.id);
            responses.push(confirmation(b"subscribe", Some(channel), count));
        }
        Ok(responses)
    }

    pub(crate) fn unsubscribe(&mut self, channels: Vec<Bytes>) -> Result<Vec<Frame>, PubSubError> {
        let mut state = self.hub.state.lock().expect("mutex Pub/Sub envenenado");
        let entry = state.subscribers.get(&self.id).ok_or(PubSubError::Closed)?;
        let channels = if channels.is_empty() {
            entry.channels.iter().cloned().collect()
        } else {
            channels
        };
        let mut responses = drain(&mut self.receiver);
        if channels.is_empty() {
            responses.push(confirmation(b"unsubscribe", None, 0));
        }
        for channel in channels {
            let entry = state
                .subscribers
                .get_mut(&self.id)
                .ok_or(PubSubError::Closed)?;
            entry.channels.remove(&channel);
            let count = entry.channels.len();
            if let Some(members) = state.channels.get_mut(&channel) {
                members.remove(&self.id);
                if members.is_empty() {
                    state.channels.remove(&channel);
                }
            }
            responses.push(confirmation(b"unsubscribe", Some(channel), count));
        }
        Ok(responses)
    }
}

fn drain(receiver: &mut mpsc::Receiver<Message>) -> Vec<Frame> {
    let mut responses = Vec::new();
    while let Ok(message) = receiver.try_recv() {
        responses.push(message.into_frame());
    }
    responses
}

fn confirmation(kind: &'static [u8], channel: Option<Bytes>, count: usize) -> Frame {
    Frame::Array(Some(vec![
        Frame::Bulk(Some(Bytes::from_static(kind))),
        Frame::Bulk(channel),
        Frame::Integer(count as i64),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(channel: &'static [u8], payload: &'static [u8]) -> Message {
        Message {
            channel: Bytes::from_static(channel),
            payload: Bytes::from_static(payload),
        }
    }

    #[test]
    fn pubsub_binary_channels_duplicates_and_atomic_limit() {
        let hub = Hub::default();
        let mut subscriber = hub.connect(2, 4).unwrap();
        let channels = vec![Bytes::new(), Bytes::from_static(b"\xff\0")];
        assert_eq!(
            subscriber.subscribe(channels.clone()).unwrap(),
            vec![
                confirmation(b"subscribe", Some(channels[0].clone()), 1),
                confirmation(b"subscribe", Some(channels[1].clone()), 2),
            ]
        );
        assert_eq!(
            subscriber.subscribe(vec![channels[1].clone()]).unwrap(),
            vec![confirmation(b"subscribe", Some(channels[1].clone()), 2)]
        );
        assert_eq!(
            subscriber.subscribe(vec![Bytes::from_static(b"other"), channels[0].clone()]),
            Err(PubSubError::ChannelLimit)
        );
        assert_eq!(hub.publish(message(b"other", b"")), 0);
        assert_eq!(hub.publish(message(b"", b"\xff\0")), 1);
        let responses = subscriber.unsubscribe(vec![]).unwrap();
        assert_eq!(
            responses,
            vec![
                message(b"", b"\xff\0").into_frame(),
                confirmation(b"unsubscribe", Some(channels[0].clone()), 1),
                confirmation(b"unsubscribe", Some(channels[1].clone()), 0)
            ]
        );
        assert!(!subscriber.active());
        assert_eq!(
            subscriber.unsubscribe(vec![]).unwrap(),
            vec![confirmation(b"unsubscribe", None, 0)]
        );
        assert_eq!(
            subscriber
                .unsubscribe(vec![Bytes::from_static(b"missing")])
                .unwrap(),
            vec![confirmation(
                b"unsubscribe",
                Some(Bytes::from_static(b"missing")),
                0
            )]
        );
    }

    #[test]
    fn pubsub_slow_subscriber_is_removed_without_delaying_fast_subscriber() {
        let hub = Hub::default();
        let mut slow = hub.connect(2, 1).unwrap();
        let mut fast = hub.connect(2, 1).unwrap();
        for subscriber in [&mut slow, &mut fast] {
            subscriber
                .subscribe(vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")])
                .unwrap();
        }
        assert_eq!(hub.publish(message(b"a", b"one")), 2);
        assert_eq!(fast.try_message(), Some(message(b"a", b"one")));
        assert_eq!(hub.publish(message(b"a", b"two")), 1);
        assert!(slow.is_evicted());
        assert_eq!(fast.try_message(), Some(message(b"a", b"two")));
        assert_eq!(hub.publish(message(b"b", b"three")), 1);
        assert_eq!(fast.try_message(), Some(message(b"b", b"three")));
        drop(fast);
        assert_eq!(hub.publish(message(b"b", b"four")), 0);
        drop(slow);
        let state = hub.state.lock().unwrap();
        assert!(state.channels.is_empty());
        assert!(state.subscribers.is_empty());
    }

    #[test]
    fn pubsub_concurrent_publishers_have_one_order_and_no_duplicates() {
        let hub = Hub::default();
        let mut first = hub.connect(1, 64).unwrap();
        let mut second = hub.connect(1, 64).unwrap();
        for subscriber in [&mut first, &mut second] {
            subscriber
                .subscribe(vec![Bytes::from_static(b"a")])
                .unwrap();
        }
        std::thread::scope(|scope| {
            for publisher in 0..4_u8 {
                let hub = &hub;
                scope.spawn(move || {
                    for sequence in 0..16_u8 {
                        assert_eq!(
                            hub.publish(Message {
                                channel: Bytes::from_static(b"a"),
                                payload: Bytes::from(vec![publisher, sequence])
                            }),
                            2
                        );
                    }
                });
            }
        });
        let mut seen = BTreeSet::new();
        for _ in 0..64 {
            let message = first.try_message().unwrap();
            assert_eq!(second.try_message(), Some(message.clone()));
            assert!(seen.insert(message.payload));
        }
        assert_eq!(seen.len(), 64);
        assert!(first.try_message().is_none());
        assert!(second.try_message().is_none());
    }
}
