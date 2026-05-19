fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/raft_internal.proto";
    println!("cargo:rerun-if-changed={proto}");
    // Build scripts run single-threaded for this crate, so setting PROTOC is scoped to
    // the current process and safe for tonic/prost code generation.
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &["proto"])?;

    Ok(())
}
