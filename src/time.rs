#[cfg(target_arch = "wasm32")]
pub use web_time::{Instant, SystemTime};

#[cfg(not(target_arch = "wasm32"))]
pub use std::time::{Instant, SystemTime};
