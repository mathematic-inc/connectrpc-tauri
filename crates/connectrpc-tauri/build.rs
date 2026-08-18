/// Commands the webview may call. Tauri's ACL generates permissions from this
/// list and rejects anything not named here.
const COMMANDS: &[&str] = &["connect_rpc", "connect_rpc_send", "connect_rpc_cancel"];

fn main() {
    connectrpc_build::Config::new()
        .files(&[
            "proto/connectrpc/tauri/v1/transport.proto",
            // Used only by the tests, but codegen runs per-crate and the extra
            // file costs nothing at runtime.
            "proto/greet/v1/greet.proto",
        ])
        .includes(&["proto"])
        .compile()
        .expect("failed to compile transport protos");

    tauri_plugin::Builder::new(COMMANDS).build();
}
