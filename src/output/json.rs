use anyhow::Result;

use crate::types::AnalysisResult;

pub fn render(results: &[AnalysisResult]) -> Result<()> {
    let json = serde_json::to_string_pretty(results)?;
    println!("{json}");
    Ok(())
}
