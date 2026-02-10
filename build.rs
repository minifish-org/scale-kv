fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .output_path(&out_dir)
        .file("schema/storage.capnp")
        .run()
        .unwrap();
    let output_file = std::path::Path::new(&out_dir).join("storage_capnp.rs");
    if !output_file.exists() {
        panic!("capnp output not found: {}", output_file.display());
    }
    // capnpc currently emits `dyn (::path::Type)` in a few spots, which triggers
    // `unused_parens` under `-D warnings`. Normalize generated output to keep a
    // warning-free build without adding crate-level allow attributes.
    let generated = std::fs::read_to_string(&output_file).unwrap();
    let normalized = generated.replace(
        "dyn (::capnp::private::capability::ClientHook)",
        "dyn ::capnp::private::capability::ClientHook",
    );
    std::fs::write(&output_file, normalized).unwrap();

    println!("cargo:rerun-if-changed=schema/storage.capnp");
}
