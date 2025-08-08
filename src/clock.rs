use std::{fmt::Debug, sync::Arc};
use tokio::time::{Duration, Instant, advance};

pub trait Clock: Debug + Send + Sync + 'static {
    /// Monotonic seconds (maps Tokio's Instant → u64 seconds).
    fn now(&self) -> u64;
}

/*──────────────────────── internal mapper ─────────────────────*/

#[derive(Debug, Clone)]
struct InstantMapper {
    origin: Instant, // reference point
    base: u64,       // logical seconds at `origin`
}
impl InstantMapper {
    fn new(start_seconds: u64) -> Self {
        Self {
            origin: Instant::now(),
            base: start_seconds,
        }
    }
    #[inline]
    fn now(&self) -> u64 {
        self.base + self.origin.elapsed().as_secs()
    }
}

/*──────────────────────── SystemClock (prod) ──────────────────*/

#[derive(Debug, Clone)]
pub struct SystemClock {
    m: InstantMapper,
}
impl SystemClock {
    pub fn new() -> Self {
        Self {
            m: InstantMapper::new(0),
        }
    }
}
impl Clock for SystemClock {
    #[inline]
    fn now(&self) -> u64 {
        self.m.now()
    }
}

/*──────────────────────── MockClock (tests) ───────────────────*/

#[derive(Debug, Clone)]
pub struct MockClock {
    m: InstantMapper,
}
impl MockClock {
    /// Start logical time at `start_seconds`.
    pub fn new(start_seconds: u64) -> Self {
        Self {
            m: InstantMapper::new(start_seconds),
        }
    }

    /// Advance BOTH Tokio's paused clock and this logical mapping.
    /// Use only in tests with `#[tokio::test(start_paused = true)]`.
    pub async fn advance(&self, delta_secs: u64) {
        if delta_secs > 0 {
            advance(Duration::from_secs(delta_secs)).await;
        }
    }

    /// Set absolute logical seconds; advances Tokio by the delta if needed.
    pub async fn set(&self, target_secs: u64) {
        let now = self.m.now();
        if target_secs > now {
            self.advance(target_secs - now).await;
        }
    }
}
impl Clock for MockClock {
    #[inline]
    fn now(&self) -> u64 {
        self.m.now()
    }
}

/*──────────────────────────── tests ───────────────────────────*/

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, sleep};

    #[tokio::test(start_paused = true)]
    async fn system_clock_tracks_tokio_time() {
        let sys = SystemClock::new();
        let t0 = sys.now();
        assert_eq!(
            t0, 0,
            "fresh SystemClock should start at 0s in paused tests"
        );

        // Jump 3s in virtual time
        advance(Duration::from_secs(3)).await;

        let t1 = sys.now();
        assert_eq!(t1, t0 + 3, "SystemClock must advance with Tokio time");
    }

    #[tokio::test(start_paused = true)]
    async fn mock_clock_advance_and_set_move_tokio() {
        let mc = MockClock::new(10);
        assert_eq!(mc.now(), 10);

        // Advance 5s (moves Tokio time too)
        mc.advance(5).await;
        assert_eq!(mc.now(), 15);

        // Prove Tokio timers are driven: a 2s sleep completes after we advance 2s
        let sleeper = tokio::spawn(async {
            sleep(Duration::from_secs(2)).await;
            1
        });
        // Without advancing, task would hang forever in paused mode
        mc.advance(2).await;
        let v = sleeper.await.unwrap();
        assert_eq!(v, 1);

        // Set absolute time forward
        mc.set(42).await;
        assert_eq!(mc.now(), 42);
    }

    #[tokio::test(start_paused = true)]
    async fn system_and_mock_stay_in_lockstep_on_advances() {
        let sys = SystemClock::new();
        let mc = MockClock::new(5);

        let s0 = sys.now();
        let m0 = mc.now();
        assert_eq!(s0, 0);
        assert_eq!(m0, 5);

        // Advance 7s; both should move by +7
        mc.advance(7).await;

        let s1 = sys.now();
        let m1 = mc.now();
        assert_eq!(s1 - s0, 7);
        assert_eq!(m1 - m0, 7);
        assert_eq!(s1, 7);
        assert_eq!(m1, 12);
    }

    #[tokio::test(start_paused = true)]
    async fn sleeps_depend_on_tokio_advances_not_wallclock() {
        use tokio::time::{Duration, advance, sleep, timeout};

        // Spawn a sleeper
        let mut task = tokio::spawn(async {
            sleep(Duration::from_secs(60)).await;
            "done"
        });

        // Without advancing time, it must NOT finish
        assert!(timeout(Duration::from_secs(0), &mut task).await.is_err());

        // Advance virtual time; it SHOULD finish
        advance(Duration::from_secs(60)).await;
        assert_eq!(task.await.unwrap(), "done");
    }
}
