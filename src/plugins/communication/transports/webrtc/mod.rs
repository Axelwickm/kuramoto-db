#![cfg(feature = "webrtc_transport")]

// Public API is re-exported from the selected backend.

// Native backend (non-wasm targets)
#[cfg(all(not(target_arch = "wasm32"), feature = "webrtc_native"))]
mod native;
#[cfg(all(not(target_arch = "wasm32"), feature = "webrtc_native"))]
pub use native::*;

// Wasm backend (browser)
#[cfg(all(target_arch = "wasm32", feature = "webrtc_wasm"))]
mod wasm;
#[cfg(all(target_arch = "wasm32", feature = "webrtc_wasm"))]
pub use wasm::*;

// Compile-time guards for missing backend features.
#[cfg(all(feature = "webrtc_transport", not(target_arch = "wasm32"), not(feature = "webrtc_native")))]
compile_error!("feature 'webrtc_transport' requires 'webrtc_native' on non-wasm targets");

#[cfg(all(feature = "webrtc_transport", target_arch = "wasm32", not(feature = "webrtc_wasm")))]
compile_error!("feature 'webrtc_transport' requires 'webrtc_wasm' on wasm32 targets");

