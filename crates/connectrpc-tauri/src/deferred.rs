//! A dispatcher initialized after the Tauri plugin is registered.

use std::{
    fmt,
    sync::{Arc, OnceLock},
};

use bytes::Bytes;
use connectrpc::{
    CodecFormat, Dispatcher, MethodDescriptor, Payload, RequestContext,
    dispatcher::{
        RequestStream, StreamingResult, UnaryResult, unimplemented_streaming, unimplemented_unary,
    },
};

/// A ConnectRPC dispatcher that can be initialized exactly once.
///
/// Tauri requires plugins that register URI schemes to be attached to the
/// builder, but application services may depend on state created later in the
/// app setup hook. Clone this dispatcher into the plugin's service, then set
/// the real dispatcher during setup.
///
/// Calls made before initialization are reported as unimplemented.
///
/// ```rust
/// use connectrpc::{ConnectRpcService, Router};
/// use connectrpc_tauri::{DeferredDispatcher, serve};
///
/// let deferred = DeferredDispatcher::new();
/// fn plugin<R: tauri::Runtime>(
///     deferred: DeferredDispatcher<Router>,
/// ) -> tauri::plugin::TauriPlugin<R> {
///     serve(ConnectRpcService::new(deferred))
/// }
/// let _service = ConnectRpcService::new(deferred.clone());
/// assert!(deferred.set(Router::new()).is_ok());
/// ```
pub struct DeferredDispatcher<D> {
    /// Shared one-time initialization cell used by every clone.
    inner: Arc<OnceLock<D>>,
}

impl<D> DeferredDispatcher<D> {
    /// Creates an uninitialized dispatcher.
    #[must_use = "retain and initialize the dispatcher before requests arrive"]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(OnceLock::new()),
        }
    }

    /// Sets the dispatcher used for all subsequent calls.
    ///
    /// # Errors
    ///
    /// Returns `dispatcher` unchanged if this instance was already initialized.
    pub fn set(&self, dispatcher: D) -> Result<(), D> {
        self.inner.set(dispatcher)
    }

    /// Returns `true` when both handles refer to the same deferred dispatcher.
    #[inline]
    #[must_use = "the identity comparison is returned without modifying either dispatcher"]
    pub fn same_dispatcher(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<D> Clone for DeferredDispatcher<D> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<D> Default for DeferredDispatcher<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D> fmt::Debug for DeferredDispatcher<D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredDispatcher")
            .field("is_initialized", &self.inner.get().is_some())
            .finish()
    }
}

impl<D: Dispatcher> Dispatcher for DeferredDispatcher<D> {
    #[inline]
    fn lookup(&self, path: &str) -> Option<MethodDescriptor> {
        self.inner.get()?.lookup(path)
    }

    #[inline]
    fn call_unary(
        &self,
        path: &str,
        context: RequestContext,
        request: Payload,
        format: CodecFormat,
    ) -> UnaryResult {
        match self.inner.get() {
            Some(dispatcher) => dispatcher.call_unary(path, context, request, format),
            None => unimplemented_unary(path),
        }
    }

    #[inline]
    fn call_server_streaming(
        &self,
        path: &str,
        context: RequestContext,
        request: Bytes,
        format: CodecFormat,
    ) -> StreamingResult {
        match self.inner.get() {
            Some(dispatcher) => dispatcher.call_server_streaming(path, context, request, format),
            None => unimplemented_streaming(path),
        }
    }

    #[inline]
    fn call_client_streaming(
        &self,
        path: &str,
        context: RequestContext,
        requests: RequestStream,
        format: CodecFormat,
    ) -> UnaryResult {
        match self.inner.get() {
            Some(dispatcher) => dispatcher.call_client_streaming(path, context, requests, format),
            None => unimplemented_unary(path),
        }
    }

    #[inline]
    fn call_bidi_streaming(
        &self,
        path: &str,
        context: RequestContext,
        requests: RequestStream,
        format: CodecFormat,
    ) -> StreamingResult {
        match self.inner.get() {
            Some(dispatcher) => dispatcher.call_bidi_streaming(path, context, requests, format),
            None => unimplemented_streaming(path),
        }
    }
}
