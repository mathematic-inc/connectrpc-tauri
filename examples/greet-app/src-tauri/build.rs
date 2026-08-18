fn main() {
    // `generate_context!` panics at compile time if `frontendDist` is missing,
    // and `dist/` is gitignored, so a clean checkout cannot even `cargo test`
    // without building the frontend first. An empty directory satisfies the
    // macro; a real `tauri build` still runs `beforeBuildCommand` and fills it.
    std::fs::create_dir_all("../dist").expect("create frontendDist directory");
    tauri_build::build();
}
