// rate_limiter.rs
use std::{fmt::Debug, sync::Arc};

use crate::clock::Clock;

#[derive(Clone, Debug)]
pub struct RateLimiter {
    clock: Arc<dyn Clock>,
    tokens: u32,
    capacity: u32,
    rate_per_sec: u32,
    last_sec: u64,
}

impl RateLimiter {
    /// `capacity` = max tokens (burst). `rate_per_sec` = tokens added per second.
    pub fn new(capacity: u32, rate_per_sec: u32, clock: Arc<dyn Clock>) -> Self {
        let now = clock.now();
        Self {
            clock,
            tokens: capacity,
            capacity,
            rate_per_sec,
            last_sec: now,
        }
    }

    /// Try to spend `cost` tokens. Returns true if allowed.
    /// If `cost > capacity`, we **always deny** (hard cap).
    pub fn try_acquire(&mut self, cost: u32) -> bool {
        self.refill_now();
        if cost > self.capacity {
            return false;
        }
        if self.tokens >= cost {
            self.tokens -= cost;
            true
        } else {
            false
        }
    }

    /// Refill based on current clock time.
    pub fn refill_now(&mut self) {
        let now = self.clock.now();
        self.refill_at(now);
    }

    /// Refill as if time were `sec`. Safe if time stood still.
    pub fn refill_at(&mut self, sec: u64) {
        if sec <= self.last_sec || self.rate_per_sec == 0 {
            self.last_sec = self.last_sec.max(sec);
            return;
        }
        let dt = sec - self.last_sec;
        // No floats; integer refill (floor). That’s fine at second granularity.
        let add = (dt as u64).saturating_mul(self.rate_per_sec as u64) as u32;
        if add > 0 {
            self.tokens = self.tokens.saturating_add(add).min(self.capacity);
            self.last_sec = sec;
        }
    }

    pub fn available(&self) -> u32 {
        self.tokens
    }

    pub fn capacity(&self) -> u32 {
        self.capacity
    }
}

/*──────────────────────────── tests ───────────────────────────*/
#[cfg(test)]
mod tests {
    use crate::clock::MockClock;

    use super::*;
    use std::sync::Arc;

    #[tokio::test(start_paused = true)]
    async fn starts_full() {
        let clk = Arc::new(MockClock::new(100));
        let tb = RateLimiter::new(5, 10, clk);
        assert_eq!(tb.available(), 5);
        assert_eq!(tb.capacity(), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_until_empty_then_deny() {
        let clk = Arc::new(MockClock::new(0));
        let mut tb = RateLimiter::new(3, 1, clk);
        assert!(tb.try_acquire(1));
        assert!(tb.try_acquire(1));
        assert!(tb.try_acquire(1));
        assert!(!tb.try_acquire(1)); // empty
        assert_eq!(tb.available(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn refills_by_seconds_floor_and_clamps_to_capacity() {
        let clk = Arc::new(MockClock::new(0));
        let mut tb = RateLimiter::new(5, 2, clk.clone()); // +2 tokens per second
        assert!(tb.try_acquire(5)); // drain
        assert_eq!(tb.available(), 0);

        // 0.9s later (but our clock is second-granularity), nothing changes
        clk.advance(0).await; // explicit noop; clarity
        tb.refill_now();
        assert_eq!(tb.available(), 0);

        // +1 second → +2 tokens
        clk.advance(1).await;
        tb.refill_now();
        assert_eq!(tb.available(), 2);

        // +10 seconds → +20 tokens but clamped to capacity(5)
        clk.advance(10).await;
        tb.refill_now();
        assert_eq!(tb.available(), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn cost_greater_than_capacity_is_always_denied() {
        let clk = Arc::new(MockClock::new(0));
        let mut tb = RateLimiter::new(5, 10, clk);
        assert!(!tb.try_acquire(6));
        assert_eq!(tb.available(), 5); // unchanged
    }

    #[tokio::test(start_paused = true)]
    async fn multiple_small_acquisitions() {
        let clk = Arc::new(MockClock::new(0));
        let mut tb = RateLimiter::new(4, 1, clk.clone());
        assert!(tb.try_acquire(1)); // 3
        assert!(tb.try_acquire(1)); // 2
        assert_eq!(tb.available(), 2);

        // +1s → +1 token (to 3)
        clk.advance(1).await;
        tb.refill_now();
        assert_eq!(tb.available(), 3);

        // spend 2 → 1 left
        assert!(tb.try_acquire(2));
        assert_eq!(tb.available(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn zero_rate_never_refills() {
        let clk = Arc::new(MockClock::new(0));
        let mut tb = RateLimiter::new(3, 0, clk.clone());
        assert!(tb.try_acquire(3)); // drain
        assert!(!tb.try_acquire(1)); // stays empty
        clk.advance(1000).await;
        tb.refill_now();
        assert_eq!(tb.available(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn time_does_not_go_backwards() {
        let clk = Arc::new(MockClock::new(10));
        let mut tb = RateLimiter::new(5, 5, clk.clone());
        assert!(tb.try_acquire(5));
        assert_eq!(tb.available(), 0);

        // set clock back — should be ignored
        clk.set(8).await;
        tb.refill_now();
        assert_eq!(tb.available(), 0);

        // back to >= last → refill
        clk.set(11).await;
        tb.refill_now();
        assert!(tb.available() > 0);
    }

    #[tokio::test(start_paused = true)]
    async fn burst_then_refill_exact() {
        let clk = Arc::new(MockClock::new(0));
        let mut tb = RateLimiter::new(10, 3, clk.clone());
        assert!(tb.try_acquire(10)); // drain to 0
        for _ in 0..3 {
            clk.advance(1).await;
            tb.refill_now();
        }
        // 3 seconds * 3 rps = 9 (but min(capacity, 9) = 9)
        assert_eq!(tb.available(), 9);
        assert!(tb.try_acquire(9));
        assert_eq!(tb.available(), 0);
    }
}
