//! ConnectRPC transport over Tauri IPC.
//!
//! Carries the Connect protocol over Tauri commands and channels instead of
//! HTTP. The webview is the client; Rust hosts the services.
//!
//! Neither side reimplements the protocol. `ConnectRpcService` is already a
//! `tower::Service<http::Request>`, and on the TypeScript side
//! `@connectrpc/connect/protocol-connect` accepts any byte-level client. This
//! crate is the shuttle between them, so framing, compression negotiation,
//! trailers, and error mapping all come from the existing runtime.
//!
//! # Usage
//!
//! ```rust,ignore
//! let router = Arc::new(MyService).register(connectrpc::Router::new());
//! tauri::Builder::default()
//!     .plugin(connectrpc_tauri::serve(ConnectRpcService::new(router)))
//!     .run(tauri::generate_context!())?;
//! ```
//!
//! # Concurrency
//!
//! Every IPC command is `async` and nothing in the request path blocks the
//! runtime. The one synchronous lock guards a hash map and is never held across
//! an await, which the `await_holding_lock` lint below enforces.

// A guard held across an await would stall every other call on this app.
#![deny(clippy::await_holding_lock)]

mod body;
mod call;
mod codec;
mod deferred;
mod plugin;
mod registry;
mod scheme;

#[cfg(feature = "testing")]
pub mod testing;

pub use deferred::DeferredDispatcher;
pub use plugin::{PLUGIN_NAME, serve};
pub use registry::CallId;
pub use scheme::SCHEME;

/// Generated wire types for the transport envelopes.
///
/// Public so tests and hand-written clients can build frames; the shapes are
/// mirrored by `packages/transport` on the TypeScript side.
#[allow(clippy::match_single_binding)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/connectrpc.tauri.v1.mod.rs"));
}

/// The demo service used by the tests and the example app.
///
/// Behind a feature flag so it does not ship in production builds of the
/// transport.
#[cfg(feature = "greet-example")]
pub mod greet {
    include!(concat!(env!("OUT_DIR"), "/greet.v1.mod.rs"));
}
