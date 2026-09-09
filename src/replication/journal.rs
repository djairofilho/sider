//! Byte-based retention; slow sockets never wait in the publication path.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::watch;

use super::Cursor;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_bytes: usize,
    pub max_batches: usize,
    pub max_frame_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid history limit")]
    Limit,
    #[error("divergent replication epoch")]
    Epoch,
    #[error("invalid replication sequence")]
    Sequence,
    #[error("insufficient history; full synchronization required")]
    Lagged,
    #[error("history closed")]
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub cursor: Cursor,
    pub frame: Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Status {
    pub head: Cursor,
    pub oldest_sequence: Option<u64>,
    pub bytes: usize,
    pub batches: usize,
}

struct State {
    head: Cursor,
    entries: VecDeque<Entry>,
    bytes: usize,
    closed: bool,
}

struct Shared {
    state: Mutex<State>,
    limits: Limits,
    changed: watch::Sender<bool>,
}

#[derive(Clone)]
pub struct Journal(Arc<Shared>);

impl Journal {
    pub fn new(head: Cursor, limits: Limits) -> Result<Self, Error> {
        if limits.max_bytes == 0
            || limits.max_bytes > isize::MAX as usize
            || limits.max_batches == 0
            || limits.max_frame_bytes == 0
            || limits.max_frame_bytes > limits.max_bytes
        {
            return Err(Error::Limit);
        }
        let (changed, _) = watch::channel(false);
        Ok(Self(Arc::new(Shared {
            state: Mutex::new(State {
                head,
                entries: VecDeque::new(),
                bytes: 0,
                closed: false,
            }),
            limits,
            changed,
        })))
    }

    /// The writer calls this operation after recording an already encoded, validated batch.
    /// It does not perform I/O or wait for subscribers. Rejections retain head and retention.
    pub fn publish(&self, sequence: u64, frame: Bytes) -> Result<(), Error> {
        if frame.is_empty() || frame.len() > self.0.limits.max_frame_bytes {
            return Err(Error::Limit);
        }
        let mut state = self.0.state.lock().map_err(|_| Error::Closed)?;
        if state.closed {
            return Err(Error::Closed);
        }
        if state.head.sequence.checked_add(1) != Some(sequence) {
            return Err(Error::Sequence);
        }
        while state.bytes > self.0.limits.max_bytes - frame.len()
            || state.entries.len() >= self.0.limits.max_batches
        {
            let removed = state.entries.pop_front().expect("non-empty retention");
            state.bytes -= removed.frame.len();
        }
        state.head.sequence = sequence;
        let cursor = state.head;
        state.bytes += frame.len();
        state.entries.push_back(Entry { cursor, frame });
        self.0.changed.send_modify(|changed| *changed = !*changed);
        Ok(())
    }

    pub fn status(&self) -> Result<Status, Error> {
        let state = self.0.state.lock().map_err(|_| Error::Closed)?;
        if state.closed {
            return Err(Error::Closed);
        }
        Ok(Status {
            head: state.head,
            oldest_sequence: state.entries.front().map(|entry| entry.cursor.sequence),
            bytes: state.bytes,
            batches: state.entries.len(),
        })
    }

    /// `cursor` is the last position already delivered, not the next requested position.
    pub fn subscribe(&self, cursor: Cursor) -> Result<Subscriber, Error> {
        let state = self.0.state.lock().map_err(|_| Error::Closed)?;
        next(&state, cursor)?;
        Ok(Subscriber {
            journal: self.clone(),
            cursor,
            changed: self.0.changed.subscribe(),
        })
    }

    /// Closes the epoch, including subscribers that were waiting for a new batch.
    pub fn close(&self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.closed = true;
            state.entries.clear();
            state.bytes = 0;
            self.0.changed.send_modify(|changed| *changed = !*changed);
        }
    }
}

fn next(state: &State, cursor: Cursor) -> Result<Option<Entry>, Error> {
    if state.closed {
        return Err(Error::Closed);
    }
    if cursor.epoch != state.head.epoch {
        return Err(Error::Epoch);
    }
    if cursor.sequence > state.head.sequence {
        return Err(Error::Sequence);
    }
    if cursor.sequence == state.head.sequence {
        return Ok(None);
    }
    let sequence = cursor.sequence.checked_add(1).ok_or(Error::Sequence)?;
    let oldest = state.entries.front().ok_or(Error::Lagged)?.cursor.sequence;
    let index = sequence.checked_sub(oldest).ok_or(Error::Lagged)?;
    let index = usize::try_from(index).map_err(|_| Error::Lagged)?;
    state
        .entries
        .get(index)
        .cloned()
        .map(Some)
        .ok_or(Error::Lagged)
}

pub struct Subscriber {
    journal: Journal,
    cursor: Cursor,
    changed: watch::Receiver<bool>,
}

impl Subscriber {
    /// The send position advances only when the consumer receives the whole frame.
    /// Remote confirmation and write timeout belong to the transport session.
    pub async fn next(&mut self) -> Result<Entry, Error> {
        loop {
            self.changed.borrow_and_update();
            let entry = {
                let state = self.journal.0.state.lock().map_err(|_| Error::Closed)?;
                next(&state, self.cursor)?
            };
            if let Some(entry) = entry {
                self.cursor = entry.cursor;
                return Ok(entry);
            }
            self.changed.changed().await.map_err(|_| Error::Closed)?;
        }
    }
}
