fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/raft_internal.proto";
    println!("cargo:rerun-if-changed={proto}");
    let mut config = tonic_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        // Generate `bytes` fields as `Bytes` instead of `Vec<u8>` so decoding
        // payload-heavy RPCs slices the receive buffer instead of copying.
        .bytes(["."])
        .compile_protos_with_config(config, &[proto], &["proto"])?;

    Ok(())
}
