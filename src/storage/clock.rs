//! Relógios injetáveis; expirações em andamento dependem somente do monotônico.

use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

/// A origem de tempo permite testes sem sleeps e futura conversão de deadlines AOF.
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
    fn unix_millis(&self) -> i64;
}

/// Relógio do runtime; testes Tokio podem pausar seu componente monotônico.
#[derive(Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn unix_millis(&self) -> i64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
            Err(error) => -i64::try_from(error.duration().as_millis()).unwrap_or(i64::MAX),
        }
    }
}
