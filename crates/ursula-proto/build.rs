fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/types.proto",
        "proto/errors.proto",
        "proto/durable.proto",
        "proto/snapshot.proto",
    ];
    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    prost_build::Config::new()
        // Generate `bytes` fields as `Bytes` instead of `Vec<u8>` so decoding
        // payload-heavy commands slices the receive buffer instead of copying.
        .bytes(["."])
        .boxed(".ursula.durable.v1.SnapshotFrameV1.frame.stream")
        .type_attribute("ProducerRequestV1", "#[derive(Eq)]")
        .type_attribute(
            "ProducerRequestV1",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute("ExternalPayloadRefV1", "#[derive(Eq)]")
        .type_attribute(
            "ExternalPayloadRefV1",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute("ColdChunkRefV1", "#[derive(Eq)]")
        .type_attribute(
            "ColdChunkRefV1",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_protos(&protos, &["proto"])?;

    Ok(())
}
