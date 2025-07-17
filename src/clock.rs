pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> u64;
}
pub struct SystemClock;
impl Clock for SystemClock {
    // Unix apoch time as secs
    fn now(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}
