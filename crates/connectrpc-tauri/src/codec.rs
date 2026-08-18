//! Protobuf encoding for the IPC envelopes.

use buffa::Message;
use tauri::ipc::InvokeBody;

/// Decode a command argument from a raw IPC payload.
///
/// Rejects JSON payloads: the TypeScript side always sends an `ArrayBuffer`,
/// so a JSON body means a hand-rolled caller or a version mismatch, and
/// silently accepting the number-array form would hide the cost we designed
/// the protobuf envelope to avoid.
pub(crate) fn decode<T: Message + Default>(body: &InvokeBody) -> Result<T, String> {
    let InvokeBody::Raw(bytes) = body else {
        return Err("expected a raw IPC payload; send an ArrayBuffer".to_string());
    };
    T::decode(&mut bytes.as_slice()).map_err(|e| format!("malformed transport frame: {e}"))
}

/// Encode a value for the IPC response or a channel frame.
pub(crate) fn encode<T: Message>(value: &T) -> Vec<u8> {
    value.encode_to_vec()
}
