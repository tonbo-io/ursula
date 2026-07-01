fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/raft_internal.proto";
    println!("cargo:rerun-if-changed={proto}");
    // Durable shared protos are symlinks to ../ursula-proto/proto so they ship
    // inside the ursula-raft tarball (cargo follows symlinks on package). This
    // keeps the .proto files a single source of truth in ursula-proto while
    // letting ursula-raft compile standalone from crates.io.
    println!("cargo:rerun-if-changed=proto/types.proto");
    println!("cargo:rerun-if-changed=proto/errors.proto");
    println!("cargo:rerun-if-changed=proto/durable.proto");
    println!("cargo:rerun-if-changed=proto/snapshot.proto");
    // Build scripts run single-threaded for this crate, so setting PROTOC is scoped to
    // the current process and safe for tonic/prost code generation.
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .extern_path(".ursula.durable.v1", "::ursula_proto")
        // Keep the generated `StoredLogEntryV1::Payload` enum small: the `normal`
        // variant carries an entire `RaftGroupCommandV1` (~288 B) and would otherwise
        // make the enum 288 B regardless of variant. Boxing only affects the in-memory
        // layout used during encode/decode — the wire format is unchanged. These proto
        // values do not sit in steady-state memory, so the extra heap allocation lands
        // on the slow path (disk write, network frame) rather than the hot path.
        .boxed(".ursula.raft.v1.StoredLogEntryV1.payload.normal")
        .compile_protos(&[proto], &["proto"])?;

    Ok(())
}
