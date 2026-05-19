fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/durable.proto";
    println!("cargo:rerun-if-changed={proto}");
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    prost_build::Config::new()
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
        .compile_protos(&[proto], &["proto"])?;

    Ok(())
}
