pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> u64;
}
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> u64 {
        // e.g., UNIX epoch seconds or millis
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}
