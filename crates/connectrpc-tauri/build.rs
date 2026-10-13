/// Commands the webview may call. Tauri's ACL generates permissions from this
/// list and rejects anything not named here.
const COMMANDS: &[&str] = &["connect_rpc", "connect_rpc_send", "connect_rpc_cancel"];

// The protobuf bindings are checked in under `src/generated`, so building the
// crate needs no `protoc`. `tools/protobuf-bindings` regenerates them.
fn main() {
    tauri_plugin::Builder::new(COMMANDS).build();
}
