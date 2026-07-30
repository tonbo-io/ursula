use std::path::PathBuf;

use anyhow::Context;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args_os().skip(1).map(PathBuf::from);
    let input = args.next().context("missing core Wasm input path")?;
    let output = args.next().context("missing component output path")?;
    if args.next().is_some() {
        anyhow::bail!("expected exactly an input and output path");
    }
    let module = std::fs::read(&input)
        .with_context(|| format!("read core Wasm module '{}'", input.display()))?;
    let component = wit_component::ComponentEncoder::default()
        .module(&module)?
        .validate(true)
        .encode()?;
    std::fs::write(&output, component)
        .with_context(|| format!("write component '{}'", output.display()))?;
    Ok(())
}
