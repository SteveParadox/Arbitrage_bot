use std::path::PathBuf;

use anyhow::Result;
use scanner::load_triangle_config;

fn main() -> Result<()> {
    let default_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../shared/config/triangles.json");
    let path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or(default_path);

    let config = load_triangle_config(&path)?;
    println!(
        "loaded {} routes across {} unique triangles using {} required symbols",
        config.route_count,
        config.triangle_count,
        config.required_symbols().len()
    );
    Ok(())
}
