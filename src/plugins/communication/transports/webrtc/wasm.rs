#![cfg(all(feature = "webrtc_transport", feature = "webrtc_wasm", target_arch = "wasm32"))]

// Intentionally not implemented yet. We keep a clear compile-time failure for now.

compile_error!("webrtc_wasm backend not implemented yet — enable 'webrtc_native' on non-wasm, or contribute a wasm backend using web-sys");

