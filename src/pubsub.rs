//! Ephemeral registry of binary channels, independent of storage and AOF.
//!
//! The mutex protects only metadata and `try_send` operations; no socket or await
//! occurs under the lock. Its order defines publication order for subscribers.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use thiserror::Error;
use tokio::sync::{mpsc, watch};

use crate::command::{Command, Reply};
use crate::metrics::{Counter, Metrics};
use crate::resp::{EncodeError, Frame, RespLimits, encode};

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
    #[error("subscriber closed or notification queue full")]
    Closed,
}

#[derive(Clone)]
pub(crate) struct Hub {
    state: Arc<Mutex<State>>,
    metrics: Metrics,
}

impl Default for Hub {
    fn default() -> Self {
        Self::with_metrics(Metrics::default())
    }
}

#[derive(Default)]
struct State {
    metrics: Metrics,
    subscriptions: usize,
    active_subscribers: usize,
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
    fn gauges(&self) {
        self.metrics.pubsub(
            self.channels.len(),
            self.active_subscribers,
            self.subscriptions,
        );
    }
    fn publish(&mut self, message: Message) -> i64 {
        let members = self
            .channels
            .get(&message.channel)
            .cloned()
            .unwrap_or_default();
        let mut accepted = 0;
        for id in members {
            let sent = self
                .subscribers
                .get(&id)
                .is_some_and(|entry| entry.messages.try_send(message.clone()).is_ok());
            if sent {
                accepted += 1;
            } else {
                self.metrics.add(Counter::PubSubEvictions, 1);
                self.remove(id);
            }
        }
        self.metrics.add(Counter::PubSubDeliveries, accepted as u64);
        accepted
    }

    fn remove(&mut self, id: u64) {
        if let Some(entry) = self.subscribers.remove(&id) {
            self.subscriptions -= entry.channels.len();
            self.active_subscribers -= usize::from(!entry.channels.is_empty());
            entry.evicted.send_replace(true);
            for channel in entry.channels {
                if let Some(members) = self.channels.get_mut(&channel) {
                    members.remove(&id);
                    if members.is_empty() {
                        self.channels.remove(&channel);
                    }
                }
            }
            self.gauges();
        }
    }
}

impl Hub {
    pub(crate) fn with_metrics(metrics: Metrics) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                metrics: metrics.clone(),
                ..State::default()
            })),
            metrics,
        }
    }
    /// The caller validates limits before creating the Tokio channel.
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

    /// Counts queues that accepted the message; this does not confirm client reads.
    /// A full queue removes all subscriptions for that client and signals its connection.
    pub(crate) fn publish(&self, message: Message) -> i64 {
        self.state
            .lock()
            .expect("mutex Pub/Sub envenenado")
            .publish(message)
    }
}

/// Connection guard: drop, cancellation, error, and EOF remove all subscriptions.
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
    pub(crate) fn metrics(&self) -> &Metrics {
        &self.hub.metrics
    }
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
        let hub = self.hub.clone();
        let mut state = hub.state.lock().expect("mutex Pub/Sub envenenado");
        self.subscribe_locked(&mut state, channels, true)
    }

    fn subscribe_locked(
        &mut self,
        state: &mut State,
        channels: Vec<Bytes>,
        drain_messages: bool,
    ) -> Result<Vec<Frame>, PubSubError> {
        let entry = state.subscribers.get(&self.id).ok_or(PubSubError::Closed)?;
        let unique: BTreeSet<_> = channels
            .iter()
            .cloned()
            .chain(entry.channels.iter().cloned())
            .collect();
        if unique.len() > self.max_channels {
            return Err(PubSubError::ChannelLimit);
        }
        // Captures earlier messages under the subscription's same lock. Confirmations
        // never overtake messages already accepted in this connection's queue.
        let mut responses = if drain_messages {
            drain(&mut self.receiver)
        } else {
            Vec::new()
        };
        for channel in channels {
            let entry = state
                .subscribers
                .get_mut(&self.id)
                .ok_or(PubSubError::Closed)?;
            if entry.channels.insert(channel.clone()) {
                state.subscriptions += 1;
                state.active_subscribers += usize::from(entry.channels.len() == 1);
            }
            let count = entry.channels.len();
            state
                .channels
                .entry(channel.clone())
                .or_default()
                .insert(self.id);
            responses.push(confirmation(b"subscribe", Some(channel), count));
        }
        state.gauges();
        Ok(responses)
    }

    pub(crate) fn unsubscribe(&mut self, channels: Vec<Bytes>) -> Result<Vec<Frame>, PubSubError> {
        let hub = self.hub.clone();
        let mut state = hub.state.lock().expect("mutex Pub/Sub envenenado");
        self.unsubscribe_locked(&mut state, channels, true)
    }

    fn unsubscribe_locked(
        &mut self,
        state: &mut State,
        channels: Vec<Bytes>,
        drain_messages: bool,
    ) -> Result<Vec<Frame>, PubSubError> {
        let entry = state.subscribers.get(&self.id).ok_or(PubSubError::Closed)?;
        let channels = if channels.is_empty() {
            entry.channels.iter().cloned().collect()
        } else {
            channels
        };
        let mut responses = if drain_messages {
            drain(&mut self.receiver)
        } else {
            Vec::new()
        };
        if channels.is_empty() {
            responses.push(confirmation(b"unsubscribe", None, 0));
        }
        for channel in channels {
            let entry = state
                .subscribers
                .get_mut(&self.id)
                .ok_or(PubSubError::Closed)?;
            if entry.channels.remove(&channel) {
                state.subscriptions -= 1;
                state.active_subscribers -= usize::from(entry.channels.is_empty());
            }
            let count = entry.channels.len();
            if let Some(members) = state.channels.get_mut(&channel) {
                members.remove(&self.id);
                if members.is_empty() {
                    state.channels.remove(&channel);
                }
            }
            responses.push(confirmation(b"unsubscribe", Some(channel), count));
        }
        state.gauges();
        Ok(responses)
    }

    /// The worker calls only after accepted append. No await or socket operation occurs under the lock.
    pub(crate) fn complete_exec(
        &mut self,
        commands: Vec<Command>,
        limits: RespLimits,
        apply: impl FnOnce() -> Reply,
    ) -> Result<Bytes, EncodeError> {
        let hub = self.hub.clone();
        let mut state = hub.state.lock().expect("mutex Pub/Sub envenenado");
        let Reply::Array(replies) = apply() else {
            unreachable!("prepare_batch always produces an array")
        };
        let command_count = commands.len();
        let mut frames = Vec::new();
        for (command, reply) in commands.into_iter().zip(replies) {
            match command {
                Command::Info(sections) => {
                    frames.push(Frame::Bulk(Some(hub.metrics.render(sections))))
                }
                Command::Subscribe { channels } => {
                    match self.subscribe_locked(&mut state, channels, false) {
                        Ok(acks) => frames.extend(acks),
                        Err(error) => frames.push(Frame::Error(Bytes::from(error.to_string()))),
                    }
                }
                Command::Unsubscribe { channels } => {
                    match self.unsubscribe_locked(&mut state, channels, false) {
                        Ok(acks) => frames.extend(acks),
                        Err(error) => frames.push(Frame::Error(Bytes::from(error.to_string()))),
                    }
                }
                Command::Publish { channel, message } => {
                    let message = Message {
                        channel,
                        payload: message,
                    };
                    let mut check = BytesMut::new();
                    if encode(&message.clone().into_frame(), &mut check, limits).is_err() {
                        frames.push(Frame::Error(Bytes::from_static(
                            b"ERR pubsub message exceeds response limit",
                        )));
                    } else {
                        frames.push(Frame::Integer(state.publish(message)));
                    }
                }
                Command::Ping(payload)
                    if state
                        .subscribers
                        .get(&self.id)
                        .is_some_and(|entry| !entry.channels.is_empty()) =>
                {
                    frames.push(Frame::Array(Some(vec![
                        Frame::Bulk(Some(Bytes::from_static(b"pong"))),
                        Frame::Bulk(Some(payload.unwrap_or_default())),
                    ])));
                }
                _ => frames.push(reply.into()),
            }
        }
        // Redis delays notifications to the same connection until all EXEC replies.
        frames.extend(drain(&mut self.receiver));
        drop(state);
        for frame in &frames {
            hub.metrics.response(frame);
        }
        encode_exec(command_count, frames, limits)
    }
}

/// SUBSCRIBE can produce multiple frames for one command in Redis RESP2 EXEC.
/// First validates/allocates the whole physical aggregate; then replaces only the
/// header with the logical command count. The new header is never larger.
fn encode_exec(
    command_count: usize,
    frames: Vec<Frame>,
    limits: RespLimits,
) -> Result<Bytes, EncodeError> {
    let physical_count = frames.len();
    assert!(physical_count >= command_count);
    let mut output = BytesMut::new();
    encode(&Frame::Array(Some(frames)), &mut output, limits)?;
    let physical_header = format!("*{physical_count}\r\n");
    let logical_header = format!("*{command_count}\r\n");
    let body = output.split_off(physical_header.len());
    output.clear();
    output.extend_from_slice(logical_header.as_bytes());
    output.extend_from_slice(&body);
    Ok(output.freeze())
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
