//! Injectable clocks; pending expiries depend only on the monotonic clock.

use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

/// The time source enables tests without sleeps and future AOF deadline conversion.
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
    fn unix_millis(&self) -> i64;
}

/// Runtime clock; Tokio tests can pause its monotonic component.
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
