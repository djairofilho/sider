//! A published decision may not reopen admission after incomplete owner application.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::worker::DbError;

#[derive(Clone)]
pub(super) struct Admission(Arc<AtomicBool>);

impl Default for Admission {
    fn default() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }
}

impl Admission {
    pub(super) fn check(&self) -> Result<(), DbError> {
        if self.0.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(DbError::Unavailable)
        }
    }

    pub(super) fn publication(&self) -> Publication {
        Publication {
            admission: self.clone(),
            complete: false,
        }
    }
}

/// Create after exclusion and before a writer submission whose outcome can be
/// uncertain. Complete only after every owner and the applied cursor are done,
/// or after a verified rejection before the durable decision. On error, panic,
/// or task cancellation, Drop closes admission before the exclusive guard drops.
pub(super) struct Publication {
    admission: Admission,
    complete: bool,
}

impl Publication {
    pub(super) fn complete(mut self) {
        self.complete = true;
    }
}

impl Drop for Publication {
    fn drop(&mut self) {
        if !self.complete {
            self.admission.0.store(false, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{Future, poll_fn};
    use std::task::Poll;

    #[test]
    fn completed_or_verified_rejected_publication_keeps_admission_open() {
        let admission = Admission::default();
        admission.publication().complete();
        assert_eq!(admission.check(), Ok(()));
    }

    #[test]
    fn publication_panic_closes_admission_before_unwinding_finishes() {
        let admission = Admission::default();
        let cloned = admission.clone();
        assert!(
            std::panic::catch_unwind(move || {
                let _publication = cloned.publication();
                panic!("injected publication failure");
            })
            .is_err()
        );
        assert_eq!(admission.check(), Err(DbError::Unavailable));
    }

    #[tokio::test]
    async fn forced_publication_cancellation_closes_admission() {
        let admission = Admission::default();
        let cloned = admission.clone();
        let (entered, entering) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _publication = cloned.publication();
            entered.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        entering.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(admission.check(), Err(DbError::Unavailable));
    }

    #[tokio::test]
    async fn failed_admission_closes_dataset_transaction_and_pubsub_command_connections() {
        use crate::resp::{Frame, RespLimits, encode};
        use bytes::{Bytes, BytesMut};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let requests: &[&[&[u8]]] = &[
            &[b"PING"],
            &[b"ECHO", b"x"],
            &[b"INFO"],
            &[b"MULTI"],
            &[b"WATCH", b"k"],
            &[b"SUBSCRIBE", b"a"],
            &[b"UNSUBSCRIBE", b"a"],
            &[b"PUBLISH", b"a", b"message"],
        ];
        for active in [false, true] {
            for request in requests {
                let (_stop, shutdown) = tokio::sync::watch::channel(false);
                let (database, _owner) = super::super::worker::channel(
                    1,
                    std::time::Duration::from_secs(5),
                    shutdown.clone(),
                )
                .unwrap();
                let hub = crate::pubsub::Hub::default();
                let mut observer = hub.connect(1, 1).unwrap();
                observer.subscribe(vec![Bytes::from_static(b"a")]).unwrap();
                let (mut client, stream) = tokio::io::duplex(1024);
                let task = tokio::spawn(crate::connection::run_with_pubsub(
                    stream,
                    crate::ServerConfig::default(),
                    database.clone(),
                    shutdown,
                    hub,
                ));
                if active {
                    client
                        .write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\na\r\n")
                        .await
                        .unwrap();
                    let mut ack = vec![0; b"*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n".len()];
                    client.read_exact(&mut ack).await.unwrap();
                }
                drop(database.admission.publication());
                let frame = Frame::Array(Some(
                    request
                        .iter()
                        .map(|argument| Frame::Bulk(Some(Bytes::copy_from_slice(argument))))
                        .collect(),
                ));
                let mut bytes = BytesMut::new();
                encode(&frame, &mut bytes, RespLimits::default()).unwrap();
                client.write_all(&bytes).await.unwrap();
                assert!(matches!(
                    task.await.unwrap(),
                    Err(crate::connection::ConnectionError::Database(
                        DbError::Unavailable
                    ))
                ));
                assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
                let mut message = Box::pin(observer.message());
                poll_fn(|cx| {
                    assert!(message.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
            }
        }
    }
}
