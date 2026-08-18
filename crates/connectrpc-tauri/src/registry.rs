//! Tracks in-flight calls so later IPC messages can reach them.

use std::{collections::HashMap, sync::Mutex};

use bytes::Bytes;
use tokio::{sync::mpsc, task::AbortHandle};

/// Identifies one in-flight RPC. Allocated by the webview.
pub type CallId = u64;

/// State for one in-flight call.
struct Call {
    /// Feeds the request body. `None` once the client signalled end-of-stream.
    body: Option<mpsc::Sender<Bytes>>,
    /// Stops the task pumping the response back to the webview.
    ///
    /// `None` for the moment between registering the call and spawning that
    /// task; a cancel arriving in that window is honoured when the handle is
    /// attached.
    pump: Option<AbortHandle>,
    /// Set when a cancel arrived before the pump handle did.
    cancelled: bool,
}

/// A registered call awaiting its response pump.
///
/// Registering before spawning keeps `connect_rpc_send` from ever seeing an
/// unknown call id, and keeps the pump's completion cleanup from racing ahead
/// of the insert.
pub(crate) struct Registration<'a> {
    registry: &'a CallRegistry,
    id: CallId,
}

impl Registration<'_> {
    /// Attach the response pump, aborting it if a cancel already arrived.
    pub(crate) fn attach(self, pump: AbortHandle) {
        let mut calls = self.registry.lock();
        let Some(call) = calls.get_mut(&self.id) else {
            // The call already finished; nothing to abort.
            return;
        };
        if call.cancelled {
            calls.remove(&self.id);
            drop(calls);
            pump.abort();
        } else {
            call.pump = Some(pump);
        }
    }
}

/// The set of in-flight calls for one app.
///
/// Managed as Tauri state. Entries are removed when the call completes, when
/// the client cancels, or when the webview goes away and the response pump
/// fails to send.
///
/// The lock is a `std::sync::Mutex`, not an async one, deliberately: every
/// critical section is a hash lookup with no await inside, so a guard never
/// spans a suspension point and an async mutex would only add a wakeup per
/// access.
#[derive(Default)]
pub(crate) struct CallRegistry {
    // ponytail: one lock for all calls. Contention is bounded by the IPC
    // message rate, so shard only if a profile says otherwise.
    calls: Mutex<HashMap<CallId, Call>>,
}

impl CallRegistry {
    /// Register a call before its response pump exists.
    pub(crate) fn begin(&self, id: CallId, body: Option<mpsc::Sender<Bytes>>) -> Registration<'_> {
        self.lock().insert(
            id,
            Call {
                body,
                pump: None,
                cancelled: false,
            },
        );
        Registration { registry: self, id }
    }

    /// Get the body sender for a call, if it is still accepting chunks.
    pub(crate) fn body_sender(&self, id: CallId) -> Option<mpsc::Sender<Bytes>> {
        self.lock().get(&id).and_then(|c| c.body.clone())
    }

    /// Close the request body without ending the call: the response may still
    /// be streaming back.
    pub(crate) fn close_request_body(&self, id: CallId) {
        if let Some(call) = self.lock().get_mut(&id) {
            call.body = None;
        }
    }

    /// Cancel a call: stop the response pump and drop the request body sender,
    /// which unblocks a handler parked on the next request message.
    pub(crate) fn remove(&self, id: CallId) {
        let mut calls = self.lock();
        let Some(call) = calls.get_mut(&id) else {
            return;
        };
        match call.pump.take() {
            Some(pump) => {
                calls.remove(&id);
                drop(calls);
                pump.abort();
            }
            None => {
                // The pump has not been attached yet. Drop the body sender so a
                // handler parked on the request stream wakes, and let `attach`
                // abort the task once it exists.
                call.cancelled = true;
                call.body = None;
            }
        }
    }

    /// Drop the bookkeeping for a call that finished on its own.
    ///
    /// Unlike [`Self::remove`], this does not abort the pump: the pump is the
    /// caller, and aborting itself mid-cleanup would be a self-inflicted panic
    /// on some runtimes.
    pub(crate) fn forget(&self, id: CallId) {
        self.lock().remove(&id);
    }

    /// How many calls are currently tracked.
    #[cfg(feature = "testing")]
    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }

    /// A poisoned lock only means some other call's handler panicked; the map
    /// itself is still consistent, so recover rather than cascade the panic
    /// into every subsequent RPC.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<CallId, Call>> {
        self.calls.lock().unwrap_or_else(|e| e.into_inner())
    }
}
