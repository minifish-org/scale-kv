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
    println!("cargo:rerun-if-changed=schema/storage.capnp");
}
