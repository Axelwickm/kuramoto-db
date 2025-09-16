#![cfg(feature = "ws_transport")]

// Native backend (non-wasm targets)
#[cfg(all(not(target_arch = "wasm32"), feature = "ws_native"))]
mod native;
#[cfg(all(not(target_arch = "wasm32"), feature = "ws_native"))]
pub use native::*;

// Wasm backend (browser)
#[cfg(all(target_arch = "wasm32", feature = "ws_wasm"))]
mod wasm;
#[cfg(all(target_arch = "wasm32", feature = "ws_wasm"))]
pub use wasm::*;

// Compile-time guards for backend selection
#[cfg(all(feature = "ws_transport", not(target_arch = "wasm32"), not(feature = "ws_native")))]
compile_error!("feature 'ws_transport' requires 'ws_native' on non-wasm targets");

#[cfg(all(feature = "ws_transport", target_arch = "wasm32", not(feature = "ws_wasm")))]
compile_error!("feature 'ws_transport' requires 'ws_wasm' on wasm32 targets");

