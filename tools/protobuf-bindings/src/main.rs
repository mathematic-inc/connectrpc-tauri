//! Regenerates the protobuf bindings checked in under
//! `crates/connectrpc-tauri/src/generated` from `proto/`.
//!
//! They're checked in so that building the crate needs no `protoc`. Only
//! maintainers changing a schema or the generator run this, from the
//! repository root with the pinned `protoc`:
//!
//! ```sh
//! mise exec -- cargo run --locked -p protobuf-bindings
//! ```
//!
//! It's its own crate rather than an example of `connectrpc-tauri` so that it
//! still builds when the checked-in bindings don't, such as after a generator
//! upgrade. `hk check` runs it and fails if the bindings change.

use std::{error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let proto = root.join("proto");
    connectrpc_build::Config::new()
        .files(&[
            proto.join("connectrpc/tauri/v1/transport.proto"),
            // The demo service, used by the tests and the example app behind
            // the crate's `greet-example` feature.
            proto.join("greet/v1/greet.proto"),
        ])
        .includes(&[&proto])
        .out_dir(root.join("crates/connectrpc-tauri/src/generated"))
        .emit_rerun_directives(false)
        .compile()?;
    Ok(())
}
