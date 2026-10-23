//! Kafka client context that caps librdkafka's own log output.

use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use rdkafka::{
    client::ClientContext, config::RDKafkaLogLevel, consumer::ConsumerContext, error::KafkaError,
};

/// Length of one log window.
const WINDOW: Duration = Duration::from_secs(10);
/// How many librdkafka messages one window lets through.
const MESSAGES_PER_WINDOW: u32 = 5;

/// Client context for the consumers and producers of this service.
///
/// While a broker is unreachable librdkafka reports every failed connection
/// attempt, which came to hundreds of log lines per second per pod. This context
/// passes on the first few messages of each window and only counts the rest. The
/// count is logged once, when the first message after the window arrives.
pub struct ThrottledContext;

static THROTTLE: Mutex<Throttle> = Mutex::new(Throttle::new());

struct Throttle {
    window_start: Option<Instant>,
    passed: u32,
    suppressed: u64,
}

impl Throttle {
    const fn new() -> Self {
        Self {
            window_start: None,
            passed: 0,
            suppressed: 0,
        }
    }

    /// Returns whether a message may be logged, and how many messages were
    /// dropped in the window that just closed.
    fn admit(&mut self, now: Instant) -> (bool, u64) {
        let mut dropped = 0;
        if self
            .window_start
            .is_none_or(|start| now.duration_since(start) >= WINDOW)
        {
            dropped = self.suppressed;
            self.window_start = Some(now);
            self.passed = 0;
            self.suppressed = 0;
        }
        if self.passed < MESSAGES_PER_WINDOW {
            self.passed += 1;
            (true, dropped)
        } else {
            self.suppressed += 1;
            (false, dropped)
        }
    }
}

fn admit() -> bool {
    let (pass, dropped) = THROTTLE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .admit(Instant::now());
    if dropped > 0 {
        tracing::warn!(dropped, "librdkafka messages dropped by the log throttle");
    }
    pass
}

impl ClientContext for ThrottledContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        if !admit() {
            return;
        }
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => tracing::error!("librdkafka: {} {}", fac, log_message),
            RDKafkaLogLevel::Warning => tracing::warn!("librdkafka: {} {}", fac, log_message),
            RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => {
                tracing::info!("librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Debug => tracing::debug!("librdkafka: {} {}", fac, log_message),
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        if admit() {
            tracing::error!("librdkafka: {}: {}", error, reason);
        }
    }
}

impl ConsumerContext for ThrottledContext {}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{Throttle, MESSAGES_PER_WINDOW, WINDOW};

    #[test]
    fn passes_a_few_messages_per_window_and_counts_the_rest() {
        let mut throttle = Throttle::new();
        let start = Instant::now();

        for _ in 0..MESSAGES_PER_WINDOW {
            assert_eq!(throttle.admit(start), (true, 0));
        }
        assert_eq!(throttle.admit(start), (false, 0));
        assert_eq!(throttle.admit(start + Duration::from_secs(1)), (false, 0));
    }

    #[test]
    fn reports_the_dropped_count_when_a_new_window_opens() {
        let mut throttle = Throttle::new();
        let start = Instant::now();

        for _ in 0..MESSAGES_PER_WINDOW + 3 {
            throttle.admit(start);
        }

        assert_eq!(throttle.admit(start + WINDOW), (true, 3));
        assert_eq!(throttle.admit(start + WINDOW), (true, 0));
    }
}
